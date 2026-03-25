# app/router/dependencies.py
from dataclasses import dataclass
from typing import Literal, Optional

from fastapi import Form, Query

from app.service.analysis_service import get_analysis_service
from app.service.minio_service import MinioService
from app.service.postgresql_service import PostgreSQLService

PdfMode = Literal["auto", "text", "ocr", "hybrid"]

_RECOGNITION_PPS_DESC = "Enable PPStructureV3; null means use service default."
_RECOGNITION_SEAL_DESC = "Enable seal recognition; null means use service default."
_RECOGNITION_SIG_DESC = "Enable signature recognition; null means use service default."
_RECOGNITION_PDF_MODE_DESC = "Override PDF extraction strategy: auto, text, ocr, or hybrid."


@dataclass(frozen=True)
class RecognitionOptions:
    use_ppstructure_v3: Optional[bool] = None
    use_seal_recognition: Optional[bool] = None
    use_signature_recognition: Optional[bool] = None
    pdf_mode: Optional[PdfMode] = None

    def as_kwargs(self) -> dict:
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
    return RecognitionOptions(
        use_ppstructure_v3=use_ppstructure_v3,
        use_seal_recognition=use_seal_recognition,
        use_signature_recognition=use_signature_recognition,
        pdf_mode=pdf_mode,
    )


def get_db_service() -> PostgreSQLService:
    """获取数据库服务实例"""
    return PostgreSQLService()


def get_oss_service() -> MinioService:
    """获取对象存储服务实例"""
    return MinioService()


def get_text_analysis_service():
    """获取文本分析服务实例"""
    return get_analysis_service()
