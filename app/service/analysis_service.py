# app/service/analysis_service.py
from functools import lru_cache
import threading
from typing import Any, Literal

from app.config.settings import settings
from app.service.ocr_service import OCRService
from app.utils.text_utils import extract_file_data, preprocess_text

from app.service.analysis.integrity import IntegrityChecker
from app.service.analysis.consistency import TemplateAnalysisService
from app.service.analysis.pricing_reasonableness import ReasonablenessChecker
from app.service.analysis.itemized_pricing import ItemizedPricingChecker
from app.service.analysis.deviation import DeviationChecker
from app.service.analysis.verification import VerificationChecker


class AnalysisService:
    SUPPORTED_EXTENSIONS = ["pdf", "jpg", "jpeg", "png"]
    IMAGE_EXTENSIONS = {"jpg", "jpeg", "png"}
    MIN_PDF_TEXT_LENGTH = 120
    MIN_PDF_AVG_CHARS_PER_PAGE = 45
    MAX_PDF_EMPTY_PAGE_RATIO = 0.4
    MIN_PDF_SPARSE_PAGE_CHARS = 24

    def __init__(self, ocr_service: OCRService) -> None:
        self.ocr_service = ocr_service
        self.integrity = IntegrityChecker()
        self.consistency = TemplateAnalysisService()
        self.reasonableness = ReasonablenessChecker()
        self.itemized = ItemizedPricingChecker()
        self.deviation = DeviationChecker()
        self.verification = VerificationChecker(ocr_service)

    def get_supported_extensions(self) -> list:
        return self.SUPPORTED_EXTENSIONS.copy()

    def _normalize_pdf_mode(self, pdf_mode_override: str | None = None) -> str:
        override = str(pdf_mode_override or "").strip().lower()
        if override in {"auto", "text", "ocr", "hybrid"}:
            return override
        if settings.PADDLE_OCR_FORCE_PDF_OCR:
            return "ocr"
        mode = str(getattr(settings, "PADDLE_OCR_PDF_MODE", "auto") or "auto").strip().lower()
        if mode in {"auto", "text", "ocr", "hybrid"}:
            return mode
        return "auto"

    def _build_pdf_text_stats(self, raw_text: str, pages: list[dict]) -> dict[str, Any]:
        normalized_text = str(raw_text or "")
        total_chars = len(normalized_text.strip())
        total_pages = len(pages or [])

        page_char_total = 0
        non_empty_pages = 0
        empty_pages = 0
        sparse_pages = 0
        for page in pages or []:
            page_text = ""
            if isinstance(page, dict):
                page_text = str(page.get("text") or "")
            elif isinstance(page, str):
                page_text = page

            page_chars = len(page_text.strip())
            page_char_total += page_chars
            if page_chars > 0:
                non_empty_pages += 1
            else:
                empty_pages += 1
            if page_chars <= self.MIN_PDF_SPARSE_PAGE_CHARS:
                sparse_pages += 1

        if total_pages == 0:
            avg_chars = float(total_chars)
            empty_page_ratio = 1.0 if total_chars == 0 else 0.0
            sparse_page_ratio = empty_page_ratio
        else:
            if page_char_total == 0 and total_chars > 0:
                page_char_total = total_chars
                non_empty_pages = 1
                empty_pages = 0
            avg_chars = page_char_total / max(total_pages, 1)
            empty_page_ratio = 1.0 - (non_empty_pages / max(total_pages, 1))
            sparse_page_ratio = sparse_pages / max(total_pages, 1)

        return {
            "total_chars": total_chars,
            "total_pages": total_pages,
            "avg_chars_per_page": round(avg_chars, 2),
            "empty_page_ratio": round(empty_page_ratio, 4),
            "empty_pages": int(empty_pages),
            "sparse_page_count": int(sparse_pages),
            "sparse_page_ratio": round(sparse_page_ratio, 4),
        }

    def _decide_extraction_route(
        self,
        file_extension: str,
        raw_text: str,
        pages: list[dict],
        pdf_mode_override: str | None = None,
    ) -> dict[str, Any]:
        normalized_extension = file_extension.lower().lstrip(".")
        if normalized_extension in self.IMAGE_EXTENSIONS:
            return {
                "use_ocr": True,
                "route": "ocr",
                "reason": "image_input",
                "pdf_mode": "n/a",
                "pdf_text_stats": {},
            }

        if normalized_extension != "pdf":
            return {
                "use_ocr": False,
                "route": "native",
                "reason": "non_pdf_non_image",
                "pdf_mode": "n/a",
                "pdf_text_stats": {},
            }

        pdf_mode = self._normalize_pdf_mode(pdf_mode_override)
        text_stats = self._build_pdf_text_stats(raw_text, pages)

        if pdf_mode == "text":
            return {
                "use_ocr": False,
                "route": "pdfplumber",
                "reason": "forced_pdf_text_mode",
                "pdf_mode": pdf_mode,
                "pdf_text_stats": text_stats,
            }

        if pdf_mode == "ocr":
            return {
                "use_ocr": True,
                "route": "ocr",
                "reason": "forced_pdf_ocr_mode",
                "pdf_mode": pdf_mode,
                "pdf_text_stats": text_stats,
            }

        if pdf_mode == "hybrid":
            return {
                "use_ocr": True,
                "route": "ocr",
                "reason": "hybrid_mode_enabled",
                "pdf_mode": pdf_mode,
                "pdf_text_stats": text_stats,
            }

        if text_stats["total_chars"] < self.MIN_PDF_TEXT_LENGTH:
            return {
                "use_ocr": True,
                "route": "ocr",
                "reason": "pdf_text_too_short",
                "pdf_mode": pdf_mode,
                "pdf_text_stats": text_stats,
            }

        if text_stats["avg_chars_per_page"] < self.MIN_PDF_AVG_CHARS_PER_PAGE:
            return {
                "use_ocr": True,
                "route": "ocr",
                "reason": "pdf_avg_text_too_low",
                "pdf_mode": pdf_mode,
                "pdf_text_stats": text_stats,
            }

        if text_stats["empty_page_ratio"] > self.MAX_PDF_EMPTY_PAGE_RATIO:
            return {
                "use_ocr": True,
                "route": "ocr",
                "reason": "pdf_too_many_empty_pages",
                "pdf_mode": pdf_mode,
                "pdf_text_stats": text_stats,
            }

        if text_stats.get("empty_pages", 0) > 0:
            return {
                "use_ocr": True,
                "route": "ocr",
                "reason": "pdf_has_empty_page",
                "pdf_mode": pdf_mode,
                "pdf_text_stats": text_stats,
            }

        if text_stats.get("sparse_page_count", 0) > 0:
            return {
                "use_ocr": True,
                "route": "ocr",
                "reason": "pdf_has_sparse_page",
                "pdf_mode": pdf_mode,
                "pdf_text_stats": text_stats,
            }

        return {
            "use_ocr": False,
            "route": "pdfplumber",
            "reason": "pdf_text_sufficient",
            "pdf_mode": pdf_mode,
            "pdf_text_stats": text_stats,
        }

    def _has_ocr_signal(self, ocr_result: dict) -> bool:
        if not isinstance(ocr_result, dict):
            return False

        text = str(ocr_result.get("text") or "").strip()
        pages = ocr_result.get("pages") or []
        layout_sections = ocr_result.get("layout_sections") or ocr_result.get("layout_blocks") or []
        seals = ocr_result.get("seals") or {}
        signatures = ocr_result.get("signatures") or {}
        seal_count = seals.get("count", 0)
        signature_count = signatures.get("count", 0)
        try:
            seal_count = int(seal_count)
        except (TypeError, ValueError):
            seal_count = 0
        try:
            signature_count = int(signature_count)
        except (TypeError, ValueError):
            signature_count = 0
        covered_texts = seals.get("covered_texts") or []
        return bool(text or pages or layout_sections or seal_count > 0 or signature_count > 0 or covered_texts)

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

    def extract_text_result(
        self,
        file_path: str,
        file_extension: str,
        use_ppstructure_v3: bool | None = None,
        use_seal_recognition: bool | None = None,
        use_signature_recognition: bool | None = None,
        pdf_mode: Literal["auto", "text", "ocr", "hybrid"] | None = None,
    ) -> dict:
        """
        核心调度：先走文件结构化抽取，再根据策略决定是否 OCR/版面分析。
        """
        file_data = extract_file_data(file_path, file_extension)
        raw_text = file_data.get("content", "") or ""
        pages = file_data.get("pages", []) or []
        page_count = file_data.get("page_count", 0) or 0
        normalized_extension = file_extension.lower().lstrip(".")

        ocr_available = bool(getattr(self.ocr_service, "available", False))
        ppstructure_requested = use_ppstructure_v3
        ppstructure_enabled = bool(
            settings.PADDLE_OCR_ENABLE_STRUCTURE
            if use_ppstructure_v3 is None
            else use_ppstructure_v3
        )
        seal_recognition_enabled = bool(
            settings.PADDLE_OCR_ENABLE_SEAL_RECOGNITION
            if use_seal_recognition is None
            else use_seal_recognition
        )
        signature_recognition_enabled = bool(
            settings.PADDLE_OCR_ENABLE_SIGNATURE_RECOGNITION
            if use_signature_recognition is None
            else use_signature_recognition
        )
        if not ocr_available:
            ppstructure_enabled = False
            seal_recognition_enabled = False
            signature_recognition_enabled = False
        ocr_used = False
        layout_used = False
        layout_sections: list[dict] = []
        logical_tables: list[dict] = []
        seal_data = {"count": 0, "texts": [], "covered_texts": []}
        signature_data = {"count": 0, "texts": []}

        extraction_route = self._decide_extraction_route(
            file_extension,
            raw_text,
            pages,
            pdf_mode_override=pdf_mode,
        )

        if extraction_route["use_ocr"] and not ocr_available:
            if normalized_extension in self.IMAGE_EXTENSIONS:
                extraction_route["route"] = "ocr_unavailable"
                extraction_route["reason"] = "ocr_unavailable_for_image"
            else:
                extraction_route["route"] = "pdfplumber"
                extraction_route["reason"] = "ocr_unavailable_fallback_to_pdfplumber"

        if extraction_route["use_ocr"] and ocr_available and hasattr(self.ocr_service, "extract_all"):
            try:
                ocr_result = self.ocr_service.extract_all(
                    file_path,
                    file_extension,
                    use_structure=use_ppstructure_v3,
                    use_seal_recognition=use_seal_recognition,
                    use_signature_recognition=use_signature_recognition,
                )
                candidate_text = str(ocr_result.get("text") or "").strip()
                candidate_pages = ocr_result.get("pages") or []
                candidate_seals = ocr_result.get("seals") or seal_data
                candidate_signatures = ocr_result.get("signatures") or signature_data
                layout_sections = ocr_result.get("layout_sections") or ocr_result.get("layout_blocks") or []
                logical_tables = ocr_result.get("logical_tables") or []
                layout_used = bool(ocr_result.get("structure_used"))
                ppstructure_enabled = bool(ocr_result.get("structure_enabled", ppstructure_enabled))
                seal_recognition_enabled = bool(
                    ocr_result.get("seal_recognition_enabled", seal_recognition_enabled)
                )
                signature_recognition_enabled = bool(
                    ocr_result.get("signature_recognition_enabled", signature_recognition_enabled)
                )

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
                    signature_data = candidate_signatures
                else:
                    layout_used = False
                    layout_sections = []
                    logical_tables = []
            except Exception as exc:
                print(f"调用 OCR 服务异常: {exc}")

        if (
            not extraction_route["use_ocr"]
            and normalized_extension == "pdf"
            and ocr_available
            and ppstructure_enabled
            and hasattr(self.ocr_service, "extract_structure_layout")
        ):
            try:
                structure_result = self.ocr_service.extract_structure_layout(
                    file_path,
                    file_extension,
                    use_structure=use_ppstructure_v3,
                )
                structure_layout_sections = structure_result.get("layout_sections") or []
                if structure_layout_sections:
                    layout_sections = structure_layout_sections
                logical_tables = structure_result.get("logical_tables") or logical_tables
                layout_used = bool(structure_result.get("structure_used"))
                ppstructure_enabled = bool(
                    structure_result.get("structure_enabled", ppstructure_enabled)
                )
            except Exception as exc:
                print(f"Structure layout extraction failed: {exc}")

        if (
            not extraction_route["use_ocr"]
            and normalized_extension == "pdf"
            and ocr_available
            and getattr(settings, "PADDLE_OCR_DETECT_MARKERS_ON_TEXT_PDF", True)
            and (seal_recognition_enabled or signature_recognition_enabled)
            and hasattr(self.ocr_service, "extract_visual_markers")
        ):
            try:
                marker_result = self.ocr_service.extract_visual_markers(
                    file_path,
                    file_extension,
                    use_seal_recognition=use_seal_recognition,
                    use_signature_recognition=use_signature_recognition,
                )
                marker_seals = marker_result.get("seals") or seal_data
                marker_signatures = marker_result.get("signatures") or signature_data
                marker_covered_texts = marker_seals.get("covered_texts") or []

                marker_seal_count = marker_seals.get("count", 0)
                marker_signature_count = marker_signatures.get("count", 0)
                try:
                    marker_seal_count = int(marker_seal_count)
                except (TypeError, ValueError):
                    marker_seal_count = 0
                try:
                    marker_signature_count = int(marker_signature_count)
                except (TypeError, ValueError):
                    marker_signature_count = 0

                marker_has_signal = bool(
                    marker_seal_count > 0
                    or marker_signature_count > 0
                    or marker_covered_texts
                )
                if marker_has_signal:
                    seal_data = marker_seals
                    signature_data = marker_signatures
                    ocr_used = True
            except Exception as exc:
                print(f"Marker detection service failed: {exc}")

        if pages:
            page_count = len(pages)
        elif raw_text.strip():
            page_count = max(page_count, 1)

        parser_engine = self._compose_parser_engine(
            file_data.get("parser_engine", "unknown"),
            ocr_used=ocr_used,
            layout_used=layout_used,
        )
        ocr_engine = self._compose_ocr_engine(ocr_used=ocr_used, layout_used=layout_used)
        covered_text_items = seal_data.get("covered_texts") or []
        covered_texts: list[str] = []
        for item in covered_text_items:
            if isinstance(item, dict):
                text_value = str(item.get("text") or "").strip()
            else:
                text_value = str(item or "").strip()
            if text_value:
                covered_texts.append(text_value)
        covered_texts = list(dict.fromkeys(covered_texts))
        seal_count = seal_data.get("count", 0)
        signature_count = signature_data.get("count", 0)
        try:
            seal_count = int(seal_count)
        except (TypeError, ValueError):
            seal_count = 0
        try:
            signature_count = int(signature_count)
        except (TypeError, ValueError):
            signature_count = 0

        table_sections = [
            section
            for section in layout_sections
            if isinstance(section, dict) and str(section.get("type") or "").strip().lower() == "table"
        ]

        return {
            "content": raw_text,
            "text_length": len(raw_text),
            "pages": pages,
            "page_count": page_count,
            "parser_engine": parser_engine,
            "source_mode": "local",
            "active_device": getattr(self.ocr_service, "active_device", "cpu"),
            "ocr_engine": ocr_engine,
            "ocr_used": ocr_used,
            "layout_used": layout_used,
            "layout_sections": layout_sections,
            "layout_section_count": len(layout_sections),
            "table_sections": table_sections,
            "table_section_count": len(table_sections),
            "logical_tables": logical_tables,
            "logical_table_count": len(logical_tables),
            "seal_detected": seal_count > 0,
            "seal_count": seal_count,
            "seal_texts": seal_data.get("texts", []),
            "seal_covered_texts": covered_texts,
            "seal_covered_text_count": len(covered_texts),
            "signature_detected": signature_count > 0,
            "signature_count": signature_count,
            "signature_texts": signature_data.get("texts", []),
            "recognition_route": extraction_route["route"],
            "recognition_reason": extraction_route["reason"],
            "pdf_mode": extraction_route["pdf_mode"],
            "pdf_text_stats": extraction_route["pdf_text_stats"],
            "ppstructure_v3_requested": ppstructure_requested,
            "ppstructure_v3_enabled": ppstructure_enabled,
            "seal_recognition_enabled": seal_recognition_enabled,
            "signature_recognition_enabled": signature_recognition_enabled,
        }

    def run_full_analysis(self, text: str, extraction_meta: dict) -> dict:
        clean_text = preprocess_text(text)
        return {
            "integrity_result": self.integrity.check_integrity(clean_text),
            "pricing_reasonableness": self.reasonableness.check_price_reasonableness(clean_text),
            "itemized_check": self.itemized.check_itemized_logic(text),
            "deviation_result": self.deviation.check_technical_deviation(clean_text),
            "verification_result": self.verification.check_seal_and_date(extraction_meta),
        }


class AnalysisServiceDispatcher:
    """Dispatch OCR requests across multiple AnalysisService workers (usually one per GPU)."""

    def __init__(
        self,
        services: list[AnalysisService],
        devices: list[str],
        *,
        max_inflight_per_device: int = 1,
    ) -> None:
        if not services:
            raise ValueError("services cannot be empty")
        if len(services) != len(devices):
            raise ValueError("services/devices length mismatch")

        self._services = services
        self._devices = devices
        self._capacity = max(1, int(max_inflight_per_device))
        self._permits = [
            threading.BoundedSemaphore(value=self._capacity)
            for _ in services
        ]
        self._inflight = [0 for _ in services]
        self._state_lock = threading.Lock()
        self._rr_cursor = 0

        # Keep existing router behavior unchanged for non-OCR paths.
        primary = services[0]
        self.integrity = primary.integrity
        self.consistency = primary.consistency
        self.reasonableness = primary.reasonableness
        self.itemized = primary.itemized
        self.deviation = primary.deviation
        self.verification = primary.verification

    def get_supported_extensions(self) -> list:
        return self._services[0].get_supported_extensions()

    def run_full_analysis(self, text: str, extraction_meta: dict) -> dict:
        return self._services[0].run_full_analysis(text, extraction_meta)

    def _acquire_slot(self) -> int:
        total = len(self._services)
        while True:
            with self._state_lock:
                start = self._rr_cursor
                ordered = sorted(
                    range(total),
                    key=lambda idx: (self._inflight[idx], (idx - start) % total),
                )
                self._rr_cursor = (self._rr_cursor + 1) % total

            for idx in ordered:
                if self._permits[idx].acquire(blocking=False):
                    with self._state_lock:
                        self._inflight[idx] += 1
                    return idx

            # All workers are busy; block on the least-loaded one.
            fallback_idx = ordered[0]
            self._permits[fallback_idx].acquire()
            with self._state_lock:
                self._inflight[fallback_idx] += 1
            return fallback_idx

    def _release_slot(self, idx: int) -> None:
        with self._state_lock:
            self._inflight[idx] = max(0, self._inflight[idx] - 1)
        self._permits[idx].release()

    def extract_text_result(
        self,
        file_path: str,
        file_extension: str,
        use_ppstructure_v3: bool | None = None,
        use_seal_recognition: bool | None = None,
        use_signature_recognition: bool | None = None,
        pdf_mode: Literal["auto", "text", "ocr", "hybrid"] | None = None,
    ) -> dict:
        idx = self._acquire_slot()
        service = self._services[idx]
        device = self._devices[idx]
        if bool(getattr(settings, "PADDLE_OCR_MULTI_GPU_LOG_SCHEDULING", False)):
            print(
                f"AnalysisServiceDispatcher: route request to worker={idx}, "
                f"configured_device={device}, active_device={service.ocr_service.active_device}"
            )
        try:
            return service.extract_text_result(
                file_path,
                file_extension,
                use_ppstructure_v3=use_ppstructure_v3,
                use_seal_recognition=use_seal_recognition,
                use_signature_recognition=use_signature_recognition,
                pdf_mode=pdf_mode,
            )
        finally:
            self._release_slot(idx)


def _resolve_ocr_device_pool() -> list[str]:
    raw_pool = str(getattr(settings, "PADDLE_OCR_DEVICE_POOL", "") or "").strip()
    if not raw_pool:
        return [str(settings.PADDLE_OCR_DEVICE).strip()]

    normalized = raw_pool.replace(";", ",").replace("|", ",")
    devices: list[str] = []
    for item in normalized.split(","):
        token = item.strip()
        if not token:
            continue
        if token.isdigit():
            token = f"gpu:{token}"
        if token not in devices:
            devices.append(token)
    return devices or [str(settings.PADDLE_OCR_DEVICE).strip()]


@lru_cache(maxsize=1)
def get_analysis_service() -> AnalysisService | AnalysisServiceDispatcher:
    devices = _resolve_ocr_device_pool()
    if len(devices) <= 1:
        preferred = devices[0] if devices else None
        return AnalysisService(ocr_service=OCRService(preferred_device=preferred))

    max_inflight = max(1, int(getattr(settings, "PADDLE_OCR_MAX_INFLIGHT_PER_DEVICE", 1)))
    services: list[AnalysisService] = []
    for device in devices:
        services.append(AnalysisService(ocr_service=OCRService(preferred_device=device)))

    print(
        "AnalysisService: multi-device OCR pool initialized "
        f"(devices={devices}, max_inflight_per_device={max_inflight})"
    )
    return AnalysisServiceDispatcher(
        services=services,
        devices=devices,
        max_inflight_per_device=max_inflight,
    )
