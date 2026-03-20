# app/service/analysis_service.py
from functools import lru_cache

from app.config.settings import settings
from app.service.ocr_service import OCRService
from app.utils.text_utils import extract_file_data, preprocess_text

from app.service.analysis.integrity import IntegrityChecker
from app.service.analysis.pricing_reasonableness import ReasonablenessChecker
from app.service.analysis.itemized_pricing import ItemizedPricingChecker
from app.service.analysis.deviation import DeviationChecker
from app.service.analysis.verification import VerificationChecker


class AnalysisService:
    SUPPORTED_EXTENSIONS = ["pdf", "jpg", "jpeg", "png"]
    IMAGE_EXTENSIONS = {"jpg", "jpeg", "png"}

    def __init__(self, ocr_service: OCRService) -> None:
        self.ocr_service = ocr_service
        self.integrity = IntegrityChecker()
        self.reasonableness = ReasonablenessChecker()
        self.itemized = ItemizedPricingChecker()
        self.deviation = DeviationChecker()
        self.verification = VerificationChecker(ocr_service)

    def get_supported_extensions(self) -> list:
        return self.SUPPORTED_EXTENSIONS.copy()

    def _should_use_ocr(self, file_extension: str, raw_text: str) -> bool:
        normalized_extension = file_extension.lower().lstrip(".")
        if normalized_extension in self.IMAGE_EXTENSIONS:
            return True
        if normalized_extension == "pdf" and settings.PADDLE_OCR_FORCE_PDF_OCR:
            return True
        return len((raw_text or "").strip()) < 50

    def _has_ocr_signal(self, ocr_result: dict) -> bool:
        if not isinstance(ocr_result, dict):
            return False
        text = str(ocr_result.get("text") or "").strip()
        pages = ocr_result.get("pages") or []
        layout_blocks = ocr_result.get("layout_blocks") or []
        seals = ocr_result.get("seals") or {}
        seal_count = seals.get("count", 0)
        try:
            seal_count = int(seal_count)
        except (TypeError, ValueError):
            seal_count = 0
        return bool(text or pages or layout_blocks or seal_count > 0)

    def _compose_parser_engine(self, base_engine: str, ocr_used: bool, layout_used: bool) -> str:
        engines: list[str] = []
        if base_engine and base_engine != "unknown":
            engines.append(base_engine)
        if ocr_used:
            engines.append("PaddleOCR")
        if layout_used:
            engines.append("PPStructureV3")

        deduped: list[str] = []
        for engine in engines:
            if engine not in deduped:
                deduped.append(engine)
        return "+".join(deduped) if deduped else "unknown"

    def _compose_ocr_engine(self, ocr_used: bool, layout_used: bool) -> str:
        if layout_used and ocr_used:
            return "PaddleOCR+PPStructureV3"
        if layout_used:
            return "PPStructureV3"
        if ocr_used:
            return "PaddleOCR"
        return "none"

    def extract_text_result(self, file_path: str, file_extension: str) -> dict:
        """
        核心调度方法：获取真实的物理文件解析数据，动态返回元数据。
        """
        file_data = extract_file_data(file_path, file_extension)
        raw_text = file_data.get("content", "") or ""
        pages = file_data.get("pages", []) or []
        page_count = file_data.get("page_count", 0) or 0

        ocr_available = bool(getattr(self.ocr_service, "available", False))
        structure_available = bool(getattr(self.ocr_service, "structure_available", False))
        ocr_used = False
        layout_used = False
        seal_data = {"count": 0, "texts": []}
        layout_block_count = 0

        if self._should_use_ocr(file_extension, raw_text) and ocr_available and hasattr(self.ocr_service, "extract_all"):
            try:
                ocr_result = self.ocr_service.extract_all(file_path, file_extension)
                candidate_text = str(ocr_result.get("text") or "").strip()
                candidate_pages = ocr_result.get("pages") or []
                candidate_seals = ocr_result.get("seals") or seal_data
                layout_blocks = ocr_result.get("layout_blocks") or []
                layout_used = bool(ocr_result.get("structure_used"))
                layout_block_count = len(layout_blocks)

                if candidate_text:
                    raw_text = candidate_text
                if candidate_pages:
                    pages = candidate_pages
                    page_count = len(candidate_pages)
                elif candidate_text and not pages:
                    pages = [{"page": 1, "text": candidate_text}]
                    page_count = 1

                ocr_used = bool(ocr_result.get("ocr_applied")) or self._has_ocr_signal(ocr_result)
                if ocr_used:
                    seal_data = candidate_seals
                else:
                    layout_used = False
                    layout_block_count = 0
            except Exception as exc:
                print(f"调用 OCR 服务异常: {exc}")

        if pages:
            page_count = len(pages)
        elif raw_text.strip():
            page_count = max(page_count, 1)

        return {
            "content": raw_text,
            "text_length": len(raw_text),
            "pages": pages,
            "page_count": page_count,
            "parser_engine": self._compose_parser_engine(
                file_data.get("parser_engine", "unknown"),
                ocr_used=ocr_used,
                layout_used=layout_used,
            ),
            "source_mode": "local",
            "active_device": getattr(self.ocr_service, "active_device", "cpu"),
            "ocr_engine": self._compose_ocr_engine(ocr_used=ocr_used, layout_used=layout_used),
            "ocr_used": ocr_used,
            "ocr_available": ocr_available,
            "structure_available": structure_available,
            "layout_engine": "PPStructureV3" if layout_used else "none",
            "layout_used": layout_used,
            "layout_block_count": layout_block_count,
            "seal_enabled": settings.PADDLE_OCR_ENABLE_SEAL_RECOGNITION,
            "seal_removed": settings.PADDLE_OCR_EXCLUDE_SEAL_TEXT,
            "seal_detected": seal_data.get("count", 0) > 0,
            "seal_count": seal_data.get("count", 0),
            "seal_texts": seal_data.get("texts", []),
        }

    def run_full_analysis(self, text: str, extraction_meta: dict) -> dict:
        clean_text = preprocess_text(text)
        return {
            "integrity_result": self.integrity.check_integrity(clean_text),
            "pricing_reasonableness": self.reasonableness.check_price_reasonableness(clean_text),
            "itemized_check": self.itemized.check_itemized_logic(clean_text),
            "deviation_result": self.deviation.check_technical_deviation(clean_text),
            "verification_result": self.verification.check_seal_and_date(extraction_meta),
        }


@lru_cache(maxsize=1)
def get_analysis_service() -> AnalysisService:
    return AnalysisService(ocr_service=OCRService())
