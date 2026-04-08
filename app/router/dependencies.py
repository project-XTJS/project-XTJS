# app/router/dependencies.py
"""Reusable route dependencies."""

from dataclasses import dataclass

from app.service.analysis import DuplicateCheckService
from app.service.analysis_service import get_analysis_service
from app.service.minio_service import MinioService
from app.service.postgresql_service import PostgreSQLService


@dataclass(frozen=True)
class RecognitionOptions:
    """Legacy placeholder kept only to avoid touching unrelated route signatures."""

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
