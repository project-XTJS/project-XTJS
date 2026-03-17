# app/router/dependencies.py
from app.service.postgresql_service import PostgreSQLService
from app.service.minio_service import MinioService
from app.service.analysis_service import get_analysis_service

def get_db_service() -> PostgreSQLService:
    """获取数据库服务实例"""
    return PostgreSQLService()

def get_oss_service() -> MinioService:
    """获取对象存储服务实例"""
    return MinioService()

def get_text_analysis_service():
    """获取文本分析服务实例"""
    return get_analysis_service()