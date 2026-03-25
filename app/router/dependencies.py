# app/router/dependencies.py
"""路由依赖与可复用参数定义。"""

from dataclasses import dataclass
from typing import Literal, Optional

from fastapi import Form, Query

from app.service.analysis_service import get_analysis_service
from app.service.minio_service import MinioService
from app.service.postgresql_service import PostgreSQLService

# 统一 PDF 模式类型，避免在多个路由重复声明。
PdfMode = Literal["auto", "text", "ocr", "hybrid"]

_RECOGNITION_PPS_DESC = "是否启用 PPStructureV3；null 表示使用服务默认配置。"
_RECOGNITION_SEAL_DESC = "是否启用印章识别；null 表示使用服务默认配置。"
_RECOGNITION_SIG_DESC = "是否启用签名识别；null 表示使用服务默认配置。"
_RECOGNITION_PDF_MODE_DESC = "覆盖 PDF 抽取策略：auto、text、ocr 或 hybrid。"


@dataclass(frozen=True)
class RecognitionOptions:
    """识别相关可选参数集合，用于路由层复用。"""

    use_ppstructure_v3: Optional[bool] = None
    use_seal_recognition: Optional[bool] = None
    use_signature_recognition: Optional[bool] = None
    pdf_mode: Optional[PdfMode] = None

    def as_kwargs(self) -> dict:
        """转换为关键字参数，便于直接透传给识别服务。"""
        return {
            "use_ppstructure_v3": self.use_ppstructure_v3,
            "use_seal_recognition": self.use_seal_recognition,
            "use_signature_recognition": self.use_signature_recognition,
            "pdf_mode": self.pdf_mode,
        }


def get_form_recognition_options(
    use_ppstructure_v3: Optional[bool] = Form(default=None, description=_RECOGNITION_PPS_DESC),
    use_seal_recognition: Optional[bool] = Form(default=None, description=_RECOGNITION_SEAL_DESC),
    use_signature_recognition: Optional[bool] = Form(default=None, description=_RECOGNITION_SIG_DESC),
    pdf_mode: Optional[PdfMode] = Form(default=None, description=_RECOGNITION_PDF_MODE_DESC),
) -> RecognitionOptions:
    """从 Form 参数构造识别选项。"""
    return RecognitionOptions(
        use_ppstructure_v3=use_ppstructure_v3,
        use_seal_recognition=use_seal_recognition,
        use_signature_recognition=use_signature_recognition,
        pdf_mode=pdf_mode,
    )


def get_query_recognition_options(
    use_ppstructure_v3: Optional[bool] = Query(default=None, description=_RECOGNITION_PPS_DESC),
    use_seal_recognition: Optional[bool] = Query(default=None, description=_RECOGNITION_SEAL_DESC),
    use_signature_recognition: Optional[bool] = Query(default=None, description=_RECOGNITION_SIG_DESC),
    pdf_mode: Optional[PdfMode] = Query(default=None, description=_RECOGNITION_PDF_MODE_DESC),
) -> RecognitionOptions:
    """从 Query 参数构造识别选项。"""
    return RecognitionOptions(
        use_ppstructure_v3=use_ppstructure_v3,
        use_seal_recognition=use_seal_recognition,
        use_signature_recognition=use_signature_recognition,
        pdf_mode=pdf_mode,
    )


def get_db_service() -> PostgreSQLService:
    """获取数据库服务实例。"""
    return PostgreSQLService()


def get_oss_service() -> MinioService:
    """获取对象存储服务实例。"""
    return MinioService()


def get_text_analysis_service():
    """获取文本分析服务实例。"""
    return get_analysis_service()
