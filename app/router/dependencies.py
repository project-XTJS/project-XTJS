# app/router/dependencies.py
"""可复用的路由依赖。"""

from dataclasses import dataclass

from app.service.analysis import BidDocumentReviewService, DuplicateCheckService
from app.service.analysis_service import get_analysis_service
from app.service.minio_service import MinioService
from app.service.postgresql_service import PostgreSQLService


@dataclass(frozen=True)
class RecognitionOptions:
    """历史占位对象，仅用于避免改动无关路由签名。"""

    def as_kwargs(self) -> dict:
        return {}


def get_form_recognition_options() -> RecognitionOptions:
    return RecognitionOptions()


def get_query_recognition_options() -> RecognitionOptions:
    return RecognitionOptions()


def get_db_service() -> PostgreSQLService:
    return PostgreSQLService()


def get_oss_service() -> MinioService:
    return MinioService()


def get_text_analysis_service():
    return get_analysis_service()


def get_duplicate_check_service() -> DuplicateCheckService:
    return DuplicateCheckService()


def get_bid_document_review_service() -> BidDocumentReviewService:
    return BidDocumentReviewService()


def get_technical_bid_review_service() -> BidDocumentReviewService:
    return get_bid_document_review_service()
