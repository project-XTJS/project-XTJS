# 对外统一导出识别 schema 相关能力，避免业务层依赖具体文件路径。
from app.config.recognition_schema import (
    RecognitionSchemaConfig,
    build_pdf_round1_recognition_template,
    get_pdf_round1_field_catalog,
)

__all__ = [
    "RecognitionSchemaConfig",
    "build_pdf_round1_recognition_template",
    "get_pdf_round1_field_catalog",
]
