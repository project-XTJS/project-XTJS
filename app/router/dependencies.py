# -*- coding: utf-8 -*-
"""
可复用的路由依赖。

通过 FastAPI 的 Depends 机制向路由处理函数注入各类服务实例，
包括数据库、对象存储、文本分析、重复检查、标书审查等。
"""

from dataclasses import dataclass

from app.service.analysis import BidDocumentReviewService, DuplicateCheckService
from app.service.analysis_service import get_analysis_service
from app.service.cache_service import RedisCacheService, get_cache_service as get_shared_cache_service
from app.service.minio_service import MinioService
from app.service.postgresql_service import PostgreSQLService


# 历史兼容对象，仅用于避免改动无关路由签名
@dataclass(frozen=True)
class RecognitionOptions:
    """历史占位对象，目前不携带任何实际配置。"""

    def as_kwargs(self) -> dict:
        return {}


def get_form_recognition_options() -> RecognitionOptions:
    """表单识别配置（历史占位）。"""
    return RecognitionOptions()


def get_query_recognition_options() -> RecognitionOptions:
    """查询识别配置（历史占位）。"""
    return RecognitionOptions()


# 数据库与存储服务
def get_db_service() -> PostgreSQLService:
    """获取 PostgreSQL 服务实例。"""
    return PostgreSQLService()


def get_oss_service() -> MinioService:
    """获取 MinIO 对象存储服务实例。"""
    return MinioService()


def get_cache_service() -> RedisCacheService:
    """获取生产缓存服务实例。"""
    return get_shared_cache_service()


# 分析服务
def get_text_analysis_service():
    """获取文本分析服务实例（OCR 提取、完整性、合理性等）。"""
    return get_analysis_service()


def get_duplicate_check_service() -> DuplicateCheckService:
    """获取重复检查服务实例。"""
    return DuplicateCheckService()


def get_bid_document_review_service() -> BidDocumentReviewService:
    """获取标书文档审查服务实例。"""
    return BidDocumentReviewService()


def get_technical_bid_review_service() -> BidDocumentReviewService:
    """获取技术标审查服务（当前与标书文档审查复用同一实现）。"""
    return get_bid_document_review_service()
