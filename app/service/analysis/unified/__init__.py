# unified/__init__.py
"""
统一商务标审查服务（重构版）

通过 Mixin 多重继承组装 UnifiedBusinessReviewService，保持原有功能完全一致。
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.core.document_types import DOCUMENT_TYPE_BUSINESS_BID
from app.service.analysis.compliance.consistency import ConsistencyChecker
from app.service.analysis.deviation import DeviationChecker
from app.service.analysis.compliance.integrity import IntegrityChecker
from app.service.analysis.itemized import ItemizedPricingChecker
from app.service.analysis.reasonableness import ReasonablenessChecker
from app.service.analysis.verification import VerificationChecker
from app.service.analysis.project_input_loader import ProjectAnalysisInputLoader
from app.service.postgresql_service import PostgreSQLService

from .constants import (
    RESULT_SCHEMA_VERSION,
    EXTRACTION_TABLE_SCHEMA_VERSION,
    DEFAULT_RESULT_KEY,
    BUSINESS_RESULT_KEY,
    PAGE_KEYS,
    PAGE_LIST_KEYS,
    ATTACHMENT_REF_RE,
    PAGE_REF_RE,
    BUSINESS_FILE_RE,
    TECHNICAL_FILE_RE,
    CHECK_DISPLAY_ORDER,
)
from .document_loader import DocumentLoaderMixin
from .orchestrator import OrchestratorMixin
from .extraction_tables import ExtractionTablesMixin
from .reading_guide import ReadingGuideMixin
from .result_normalizer import ResultNormalizerMixin
from .consistency_filter import ConsistencyFilterMixin
from .doc_merge_utils import DocMergeUtilsMixin
from .helpers import HelpersMixin


class UnifiedBusinessReviewService(
    DocumentLoaderMixin,
    OrchestratorMixin,
    ExtractionTablesMixin,
    ReadingGuideMixin,
    ResultNormalizerMixin,
    ConsistencyFilterMixin,
    DocMergeUtilsMixin,
    HelpersMixin,
):
    """统一商务标审查服务，编排多个审查子模块，输出标准化结果与阅读指南。"""

    # 结果与抽数表的 schema 版本号
    RESULT_SCHEMA_VERSION = RESULT_SCHEMA_VERSION
    EXTRACTION_TABLE_SCHEMA_VERSION = EXTRACTION_TABLE_SCHEMA_VERSION
    # 默认结果键名
    DEFAULT_RESULT_KEY = DEFAULT_RESULT_KEY
    BUSINESS_RESULT_KEY = BUSINESS_RESULT_KEY

    # 页面字段相关常量
    PAGE_KEYS = PAGE_KEYS
    PAGE_LIST_KEYS = PAGE_LIST_KEYS

    # 附件引用与页码引用正则
    ATTACHMENT_REF_RE = ATTACHMENT_REF_RE
    PAGE_REF_RE = PAGE_REF_RE

    # 文件名后缀识别
    BUSINESS_FILE_RE = BUSINESS_FILE_RE
    TECHNICAL_FILE_RE = TECHNICAL_FILE_RE

    # 审查项的展示顺序
    CHECK_DISPLAY_ORDER = CHECK_DISPLAY_ORDER

    def __init__(self, db_service: PostgreSQLService | None = None) -> None:
        """初始化各子检查器与数据库服务。"""
        self.db_service = db_service or PostgreSQLService()
        self.integrity_checker = IntegrityChecker()
        self.consistency_checker = ConsistencyChecker()
        self.reasonableness_checker = ReasonablenessChecker()
        self.itemized_checker = ItemizedPricingChecker()
        self.deviation_checker = DeviationChecker()
        self.verification_checker = VerificationChecker(None)

    # 对外入口
    def review_dataset(
        self,
        dataset_dir: str | Path,
        *,
        project_identifier: str | None = None,
    ) -> dict[str, Any]:
        """扫描本地目录下的招标/投标 JSON 文件，执行统一审查并返回结果。"""
        dataset = self._discover_dataset(dataset_dir)
        resolved_project_identifier = project_identifier or self._default_project_identifier(dataset["base_dir"])

        bidders: list[dict[str, Any]] = []
        for bidder in dataset["bidders"]:
            bidders.append(
                self._review_bidder(
                    tender_payload=dataset["tender"]["content"],
                    tender_meta=dataset["tender"]["meta"],
                    bidder=bidder,
                )
            )

        extraction_tables = self._build_review_extraction_tables(
            tender_payload=dataset["tender"]["content"],
            tender_meta=dataset["tender"]["meta"],
            bidder_sources=dataset["bidders"],
            bidder_reviews=bidders,
        )
        reading_guide = self._build_review_reading_guide(
            tender_meta=dataset["tender"]["meta"],
            bidders=bidders,
        )

        return {
            "schema_version": self.RESULT_SCHEMA_VERSION,
            "review_type": "unified_business_review",
            "generated_at": self._utc_now_iso(),
            "project_identifier_id": resolved_project_identifier,
            "dataset": {
                "base_dir": str(dataset["base_dir"]),
                "tender": dataset["tender"]["meta"],
                "bidders": [
                    {
                        "bidder_key": bidder["bidder_key"],
                        "business": bidder["business"]["meta"],
                        "technical": bidder["technical"]["meta"],
                    }
                    for bidder in dataset["bidders"]
                ],
                "file_count": 1 + len(dataset["bidders"]) * 2,
            },
            "reading_guide": reading_guide,
            "extraction_tables": extraction_tables,
            "function_validation": self._summarize_function_validation(bidders),
            "summary": self._summarize_review(bidders),
            "bidders": bidders,
        }

    def persist_dataset_review(
        self,
        dataset_dir: str | Path,
        *,
        project_identifier: str | None = None,
        result_key: str = DEFAULT_RESULT_KEY,
    ) -> dict[str, Any]:
        """执行数据集审查并将结果持久化至数据库。"""
        review = self.review_dataset(
            dataset_dir,
            project_identifier=project_identifier,
        )
        project = self._get_or_create_project(review["project_identifier_id"])
        result_record = self.db_service.upsert_project_result_item(
            project["identifier_id"],
            result_key,
            review,
        )
        return {
            "project": project,
            "result_key": result_key,
            "overview": self._build_response_overview(review),
            "review": review,
            "result_record": result_record,
        }

    def persist_uploaded_business_review(
        self,
        *,
        tender_file_name: str,
        tender_payload: dict[str, Any],
        tender_raw_bytes: bytes,
        business_bid_documents: list[dict[str, Any]],
        project_identifier: str | None = None,
        result_key: str = BUSINESS_RESULT_KEY,
    ) -> dict[str, Any]:
        """处理上传的招标/投标 JSON 文件，执行审查并持久化。"""
        project = self._ensure_project(project_identifier)
        review = self._review_uploaded_business_documents(
            tender_file_name=tender_file_name,
            tender_payload=tender_payload,
            tender_raw_bytes=tender_raw_bytes,
            business_bid_documents=business_bid_documents,
            project_identifier=project["identifier_id"],
        )
        result_record = self.db_service.upsert_project_result_item(
            project["identifier_id"],
            result_key,
            review,
        )
        return {
            "project": project,
            "result_key": result_key,
            "overview": self._build_response_overview(review),
            "review": review,
            "result_record": result_record,
        }

    def review_project_business_documents(
        self,
        *,
        project_identifier: str,
    ) -> dict[str, Any]:
        """从数据库中读取项目绑定的招投标文档并执行审查。"""
        payload_data = ProjectAnalysisInputLoader(self.db_service).load(
            project_identifier
        )
        if not payload_data:
            raise ValueError(f"project not found: {project_identifier}")
        return self._review_project_business_documents(
            project_identifier=project_identifier,
            payload_data=payload_data,
        )

    def review_project_deviation_documents(
        self,
        *,
        project_identifier: str,
    ) -> dict[str, Any]:
        """从数据库中读取招标、商务标、技术标并执行独立偏离表检查。"""
        payload_data = ProjectAnalysisInputLoader(self.db_service).load(
            project_identifier
        )
        if not payload_data:
            raise ValueError(f"project not found: {project_identifier}")
        return self._review_project_deviation_documents(
            project_identifier=project_identifier,
            payload_data=payload_data,
        )

    def persist_project_business_review(
        self,
        *,
        project_identifier: str,
        result_key: str = BUSINESS_RESULT_KEY,
    ) -> dict[str, Any]:
        """执行项目商务标审查并持久化结果。"""
        project = self.db_service.get_project_by_identifier(project_identifier)
        if not project:
            raise ValueError(f"project not found: {project_identifier}")

        review = self.review_project_business_documents(project_identifier=project_identifier)
        result_record = self.db_service.upsert_project_result_item(
            project_identifier,
            result_key,
            review,
        )
        clear_latest_result = getattr(
            self.db_service,
            "clear_project_manual_review_latest_result",
            None,
        )
        if callable(clear_latest_result):
            result_record = clear_latest_result(project_identifier, result_key)
        return {
            "project": project,
            "result_key": result_key,
            "overview": self._build_response_overview(review),
            "review": review,
            "result_record": result_record,
        }

    def persist_project_deviation_review(
        self,
        *,
        project_identifier: str,
        result_key: str = "deviation_check",
    ) -> dict[str, Any]:
        """执行项目偏离表检查并持久化结果。"""
        project = self.db_service.get_project_by_identifier(project_identifier)
        if not project:
            raise ValueError(f"project not found: {project_identifier}")

        review = self.review_project_deviation_documents(project_identifier=project_identifier)
        result_record = self.db_service.upsert_project_result_item(
            project_identifier,
            result_key,
            review,
        )
        return {
            "project": project,
            "result_key": result_key,
            "overview": self._build_response_overview(review),
            "review": review,
            "result_record": result_record,
        }
