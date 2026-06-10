"""
分项报价明细检查模块

通过 Mixin 多重继承组装 ItemizedPricingChecker，保持原有功能完全一致。
"""

from decimal import Decimal

from .html_parser import _TableHTMLParser
from .utils import UtilsMixin
from .document_parser import DocumentParserMixin
from .table_extractor import TableExtractorMixin
from .structured_extractor import StructuredExtractorMixin
from .arithmetic_checker import ArithmeticCheckerMixin
from .rate_mode import RateModeMixin
from .normal_mode import NormalModeMixin


class ItemizedPricingChecker(
    UtilsMixin,
    DocumentParserMixin,
    TableExtractorMixin,
    StructuredExtractorMixin,
    ArithmeticCheckerMixin,
    RateModeMixin,
    NormalModeMixin,
):
    # 报价表/总价表锚点与关键词配置（类常量）
    ITEM_SECTION_ANCHORS = (
        "分项报价表",
        "分项报价明细",
        "投标报价明细表",
        "报价明细一览表",
        "报价分类明细",
        "报价明细",
        "标价明细",
        "已标价工程量清单",
        "已标价清单",
        "供应清单",
        "货物清单",
        "工程量清单",
        "投标价格表",
        "报价表",
    )
    PRIMARY_ITEM_SECTION_ANCHORS = (
        "分项报价表",
        "分项报价明细",
        "投标报价明细表",
        "报价明细一览表",
        "报价分类明细",
        "报价明细",
        "标价明细",
        "已标价工程量清单",
        "已标价清单",
    )
    TOTAL_SECTION_ANCHORS = ("开标一览表","报价一览表","投标总价","总报价",)
    TOTAL_KEYWORDS = ("合计","总计","总价","总报价","投标总价","单价合计","金额合计","报价合计",)
    OPENING_TOTAL_KEYWORDS = ("投标总价","总报价","开标一览表","报价一览表",)
    SUBTOTAL_KEYWORDS = ("小计",)
    RATE_KEYWORDS = ("下浮率","优惠率","折扣率","折让率","下浮",)
    UNIT_KEYWORDS = ("台","套","项","个","批","次","人","年","月","日","米","吨","樘","组","m2","㎡",)
    ZERO_AMOUNT_KEYWORDS = ("包含","免费","赠送","无偿","不收费",)
    STRUCTURED_COLUMN_ALIASES = {
        "serial": ("序号", "编号"),
        "model": ("型号", "规格型号", "项目", "品名", "设备名称"),
        "description": ("说明", "名称", "内容", "参数", "配置", "描述", "服务类型", "服务项目"),
        "brand": ("品牌", "厂家", "厂商", "制造商", "生产厂家", "产地"),
        "quantity": ("数量",),
        "unit_price": ("单价", "投标单价", "报价单价", "综合单价", "含税单价"),
        "line_total": ("合计", "总价", "金额", "小计", "总额", "分项总价", "单项总价"),
    }
    MONEY_TOLERANCE = Decimal("0.10")
    PRIMARY_ITEM_SECTION_NEARBY_PAGE_GAP = 2
    LAYOUT_TABLE_START_PAGE_GAP = 2
    STRUCTURED_FALLBACK_NEARBY_PAGE_GAP = 3
    LOW_CONFIDENCE_UNRESOLVED_THRESHOLD = 3

    # 主入口方法（保留在原类中，因为它不属于某个特定 Mixin）
    def check_itemized_logic(self, text: object, tender_text: object | None = None) -> dict:
        document = self._prepare_document(text)
        item_sections = document["item_sections"]
        total_sections = document["total_sections"]
        candidate_sections = document["candidate_sections"]

        if not item_sections:
            return self._build_missing_itemized_result()

        if self._detect_downward_rate_mode(candidate_sections):
            tender_document = self._prepare_document(tender_text) if tender_text is not None else None
            return self._check_downward_rate_mode(
                candidate_sections,
                tender_document=tender_document,
                item_sections=item_sections,
            )
        return self._check_normal_mode(item_sections, total_sections, candidate_sections, document=document)

    def _build_missing_itemized_result(
        self,
        *,
        mode: str = "normal",
        summary: str | None = None,
    ) -> dict:
        """Return a hard missing result when no real itemized pricing table is found."""
        message = summary or "未识别到分项报价表，无法执行分项报价表一致性校验。"
        return {
            "itemized_table_detected": False,
            "mode": mode,
            "status": "not_detected",
            "passed": None,
            "summary": message,
            "checks": {
                "row_arithmetic": {
                    "status": "not_detected",
                    "issue_count": 0,
                    "issues": [],
                    "unresolved_count": 0,
                    "unresolved_rows": [],
                },
                "sum_consistency": {
                    "status": "not_detected",
                    "calculated_total": None,
                    "declared_total": None,
                    "difference": None,
                    "matched_total_label": None,
                },
                "duplicate_items": {
                    "status": "not_detected",
                    "issue_count": 0,
                    "issues": [],
                },
                "missing_item": {
                    "status": "missing",
                    "missing_items": [],
                    "comparison_basis": None,
                    "hints": [],
                    "hint_level": None,
                },
            },
            "evidence": {
                "analysis_basis": "not_detected",
                "structured_tables": [],
                "structured_relation_count": 0,
                "structured_relations": [],
                "structured_group_checks": [],
                "extracted_item_count": 0,
                "extracted_items": [],
                "total_candidates": [],
                "excluded_total_candidates": [],
                "unresolved_rows": [],
            },
            "manual_review": {
                "required": False,
                "recognized_total": None,
                "calculated_total": None,
                "difference": None,
                "total_candidates": [],
                "unclear_content_count": 0,
                "unclear_contents": [],
                "row_issue_count": 0,
                "row_issues": [],
                "confidence_level": "missing",
                "confidence_reasons": ["未识别到真实分项报价表。"],
            },
            "details": [message],
        }
