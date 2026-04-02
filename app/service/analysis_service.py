# app/service/analysis_service.py
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import lru_cache
import os
from pathlib import Path
import subprocess
import tempfile
import threading

from app.config.settings import settings
from app.service.ocr_service import OCRService
from app.utils.text_utils import preprocess_text

from app.service.analysis.integrity import IntegrityChecker
from app.service.analysis.consistency import ConsistencyChecker
from app.service.analysis.pricing_reasonableness import ReasonablenessChecker
from app.service.analysis.itemized_pricing import ItemizedPricingChecker
from app.service.analysis.deviation import DeviationChecker
from app.service.analysis.verification import VerificationChecker


def _estimate_pdf_page_count(file_path: str) -> int:
    try:
        import pypdfium2 as pdfium

        document = pdfium.PdfDocument(file_path)
        try:
            return len(document)
        finally:
            close_method = getattr(document, "close", None)
            if callable(close_method):
                close_method()
    except Exception:
        return 0


def _split_page_ranges(total_pages: int, worker_count: int) -> list[tuple[int, int]]:
    normalized_total = max(0, int(total_pages or 0))
    normalized_workers = max(1, min(int(worker_count or 1), normalized_total or 1))
    if normalized_total <= 0:
        return []

    base_size, remainder = divmod(normalized_total, normalized_workers)
    page_ranges: list[tuple[int, int]] = []
    start = 0
    for idx in range(normalized_workers):
        chunk_size = base_size + (1 if idx < remainder else 0)
        end = start + chunk_size
        if chunk_size > 0:
            page_ranges.append((start, end))
        start = end
    return page_ranges


def _export_pdf_page_range(
    source_file_path: str,
    dest_file_path: str,
    start_page: int,
    end_page: int,
) -> None:
    import pypdfium2 as pdfium

    source_document = pdfium.PdfDocument(source_file_path)
    target_document = pdfium.PdfDocument.new()
    try:
        target_document.import_pages(source_document, pages=list(range(start_page, end_page)))
        target_document.save(dest_file_path)
    finally:
        target_document.close()
        close_method = getattr(source_document, "close", None)
        if callable(close_method):
            close_method()


class AnalysisService:
    SUPPORTED_EXTENSIONS = ["pdf", "jpg", "jpeg", "png"]

    def __init__(self, ocr_service: OCRService) -> None:
        self.ocr_service = ocr_service
        self.integrity = IntegrityChecker()
        self.consistency = ConsistencyChecker()
        self.reasonableness = ReasonablenessChecker()
        self.itemized = ItemizedPricingChecker()
        self.deviation = DeviationChecker()
        self.verification = VerificationChecker(ocr_service)

    def get_supported_extensions(self) -> list[str]:
        return self.SUPPORTED_EXTENSIONS.copy()

    def extract_text_result(
        self,
        file_path: str,
        file_extension: str,
        *,
        page_offset: int = 0,
    ) -> dict:
        normalized_extension = file_extension.lower().lstrip(".")
        if normalized_extension not in self.SUPPORTED_EXTENSIONS:
            raise ValueError(
                f"Unsupported file type: {file_extension}. "
                f"Supported types: {', '.join(self.SUPPORTED_EXTENSIONS)}."
            )

        if not bool(getattr(self.ocr_service, "available", False)):
            raise RuntimeError("PaddleOCR-VL-1.5 is unavailable.")

        ocr_result = self.ocr_service.extract_all(
            file_path,
            normalized_extension,
            page_offset=page_offset,
        )
        raw_text = str(ocr_result.get("text") or "")
        pages = ocr_result.get("pages") or []
        page_count = len(pages) if isinstance(pages, list) else 0
        layout_sections = ocr_result.get("layout_sections") or []
        logical_tables = ocr_result.get("logical_tables") or []
        seal_data = ocr_result.get("seals") or {"count": 0, "texts": []}

        try:
            seal_count = int(seal_data.get("count", 0))
        except (TypeError, ValueError):
            seal_count = 0

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
            "parser_engine": "PaddleOCR-VL-1.5",
            "source_mode": "local",
            "active_device": getattr(self.ocr_service, "active_device", "cpu"),
            "ocr_engine": "PaddleOCR-VL-1.5",
            "ocr_used": True,
            "layout_used": bool(layout_sections),
            "layout_sections": layout_sections,
            "layout_section_count": len(layout_sections),
            "table_sections": table_sections,
            "table_section_count": len(table_sections),
            "logical_tables": logical_tables,
            "logical_table_count": len(logical_tables),
            "seal_detected": seal_count > 0,
            "seal_count": seal_count,
            "seal_texts": seal_data.get("texts", []),
            "recognition_route": "paddleocr_vl",
            "recognition_reason": "vl_only_pipeline",
            "pdf_mode": "vl_only",
            "pdf_text_stats": {},
            "ppstructure_v3_requested": False,
            "ppstructure_v3_enabled": False,
            "seal_recognition_enabled": bool(settings.PADDLE_VL_USE_SEAL_RECOGNITION),
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
    """Dispatch OCR requests across multiple AnalysisService workers."""

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

        primary = services[0]
        self.integrity = primary.integrity
        self.consistency = primary.consistency
        self.reasonableness = primary.reasonableness
        self.itemized = primary.itemized
        self.deviation = primary.deviation
        self.verification = primary.verification

    def get_supported_extensions(self) -> list[str]:
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

            fallback_idx = ordered[0]
            self._permits[fallback_idx].acquire()
            with self._state_lock:
                self._inflight[fallback_idx] += 1
            return fallback_idx

    def _release_slot(self, idx: int) -> None:
        with self._state_lock:
            self._inflight[idx] = max(0, self._inflight[idx] - 1)
        self._permits[idx].release()

    def _acquire_slots(self, requested: int) -> list[int]:
        target = max(1, min(int(requested or 1), len(self._services)))
        while True:
            with self._state_lock:
                start = self._rr_cursor
                ordered = sorted(
                    range(len(self._services)),
                    key=lambda idx: (self._inflight[idx], (idx - start) % len(self._services)),
                )
                self._rr_cursor = (self._rr_cursor + target) % len(self._services)

            acquired: list[int] = []
            for idx in ordered:
                if len(acquired) >= target:
                    break
                if self._permits[idx].acquire(blocking=False):
                    with self._state_lock:
                        self._inflight[idx] += 1
                    acquired.append(idx)

            if acquired:
                return acquired

            fallback_idx = ordered[0]
            self._permits[fallback_idx].acquire()
            with self._state_lock:
                self._inflight[fallback_idx] += 1
            return [fallback_idx]

    def _release_slots(self, indices: list[int]) -> None:
        for idx in indices:
            self._release_slot(idx)

    def _merge_parallel_pdf_results(
        self,
        results: list[tuple[int, int, int, dict]],
        *,
        expected_total_pages: int,
    ) -> dict:
        ordered_results = sorted(results, key=lambda item: item[0])
        template = ordered_results[0][3]
        primary_service = self._services[ordered_results[0][2]]

        pages: list[dict] = []
        layout_sections: list[dict] = []
        seal_texts: list[str] = []
        active_devices: list[str] = []
        seal_count = 0

        for _, _, idx, payload in ordered_results:
            device_name = str(payload.get("active_device") or self._devices[idx] or "").strip()
            if device_name and device_name not in active_devices:
                active_devices.append(device_name)

            payload_pages = payload.get("pages") or []
            if isinstance(payload_pages, list):
                pages.extend(payload_pages)

            payload_layout_sections = payload.get("layout_sections") or []
            if isinstance(payload_layout_sections, list):
                layout_sections.extend(payload_layout_sections)

            for text in payload.get("seal_texts") or []:
                normalized_text = str(text or "").strip()
                if normalized_text:
                    seal_texts.append(normalized_text)

            try:
                seal_count += int(payload.get("seal_count") or 0)
            except (TypeError, ValueError):
                pass

        pages = sorted(
            pages,
            key=lambda item: int(item.get("page", 0) or 0) if isinstance(item, dict) else 0,
        )
        actual_page_numbers = [
            int(item.get("page", 0) or 0)
            for item in pages
            if isinstance(item, dict) and int(item.get("page", 0) or 0) > 0
        ]
        expected_page_numbers = list(range(1, max(0, int(expected_total_pages or 0)) + 1))
        if actual_page_numbers != expected_page_numbers:
            raise RuntimeError(
                "Page-parallel OCR produced incomplete page results "
                f"(expected={expected_page_numbers}, actual={actual_page_numbers})"
            )

        raw_text = primary_service.ocr_service._merge_text_parts(
            [str(page.get("text") or "") for page in pages if isinstance(page, dict)],
            join_char="\n",
        )
        table_sections = [
            section
            for section in layout_sections
            if isinstance(section, dict) and str(section.get("type") or "").strip().lower() == "table"
        ]
        logical_tables = (
            primary_service.ocr_service._attach_table_outputs(
                {"layout_sections": layout_sections, "logical_tables": []}
            ).get("logical_tables")
            or []
        )
        deduped_seal_texts = primary_service.ocr_service._dedupe_text_parts(seal_texts)
        merged_active_device = (
            ",".join(active_devices)
            if len(active_devices) > 1
            else (active_devices[0] if active_devices else str(template.get("active_device") or "cpu"))
        )

        return {
            "content": raw_text,
            "text_length": len(raw_text),
            "pages": pages,
            "page_count": len(pages),
            "parser_engine": template.get("parser_engine") or "PaddleOCR-VL-1.5",
            "source_mode": template.get("source_mode") or "local",
            "active_device": merged_active_device,
            "active_devices": active_devices,
            "ocr_engine": template.get("ocr_engine") or "PaddleOCR-VL-1.5",
            "ocr_used": bool(template.get("ocr_used", True)),
            "layout_used": bool(layout_sections),
            "layout_sections": layout_sections,
            "layout_section_count": len(layout_sections),
            "table_sections": table_sections,
            "table_section_count": len(table_sections),
            "logical_tables": logical_tables,
            "logical_table_count": len(logical_tables),
            "seal_detected": seal_count > 0,
            "seal_count": seal_count,
            "seal_texts": deduped_seal_texts,
            "recognition_route": template.get("recognition_route") or "paddleocr_vl",
            "recognition_reason": template.get("recognition_reason") or "vl_only_pipeline",
            "pdf_mode": template.get("pdf_mode") or "vl_only",
            "pdf_text_stats": template.get("pdf_text_stats") or {},
            "ppstructure_v3_requested": bool(template.get("ppstructure_v3_requested", False)),
            "ppstructure_v3_enabled": bool(template.get("ppstructure_v3_enabled", False)),
            "seal_recognition_enabled": bool(
                template.get("seal_recognition_enabled", settings.PADDLE_VL_USE_SEAL_RECOGNITION)
            ),
            "page_parallelized": True,
        }

    def extract_text_result(self, file_path: str, file_extension: str) -> dict:
        normalized_extension = file_extension.lower().lstrip(".")
        if normalized_extension == "pdf" and len(self._services) > 1:
            total_pages = _estimate_pdf_page_count(file_path)
            if total_pages > 1:
                requested_slots = min(total_pages, len(self._services))
                indices = self._acquire_slots(requested_slots)
                try:
                    if len(indices) > 1:
                        page_ranges = _split_page_ranges(total_pages, len(indices))
                        temp_dir = tempfile.TemporaryDirectory(
                            prefix=f"{Path(file_path).stem}_pages_",
                            dir=str(settings.OCR_RUNTIME_TEMP_DIR),
                        )
                        try:
                            assignments: list[tuple[int, int, int, str]] = []
                            for idx, (start_page, end_page) in zip(indices, page_ranges):
                                chunk_path = (
                                    Path(temp_dir.name)
                                    / f"{Path(file_path).stem}_pages_{start_page + 1}_{end_page}.pdf"
                                )
                                _export_pdf_page_range(
                                    file_path,
                                    str(chunk_path),
                                    start_page,
                                    end_page,
                                )
                                assignments.append((start_page, end_page, idx, str(chunk_path)))

                            if bool(getattr(settings, "PADDLE_OCR_MULTI_GPU_LOG_SCHEDULING", False)):
                                distribution = ", ".join(
                                    f"{self._devices[idx]}:{end_page - start_page}p"
                                    for start_page, end_page, idx, _ in assignments
                                )
                                print(
                                    "AnalysisServiceDispatcher: page-parallel PDF scheduling "
                                    f"(total_pages={total_pages}, distribution={distribution})"
                                )

                            parallel_results: list[tuple[int, int, int, dict]] = []
                            with ThreadPoolExecutor(max_workers=len(assignments)) as executor:
                                future_map = {
                                    executor.submit(
                                        self._services[idx].extract_text_result,
                                        chunk_file_path,
                                        normalized_extension,
                                        page_offset=start_page,
                                    ): (start_page, end_page, idx)
                                    for start_page, end_page, idx, chunk_file_path in assignments
                                }
                                for future in as_completed(future_map):
                                    start_page, end_page, idx = future_map[future]
                                    parallel_results.append(
                                        (start_page, end_page, idx, future.result())
                                    )

                            return self._merge_parallel_pdf_results(
                                parallel_results,
                                expected_total_pages=total_pages,
                            )
                        finally:
                            temp_dir.cleanup()
                    service = self._services[indices[0]]
                    device = self._devices[indices[0]]
                    if bool(getattr(settings, "PADDLE_OCR_MULTI_GPU_LOG_SCHEDULING", False)):
                        print(
                            f"AnalysisServiceDispatcher: route request to worker={indices[0]}, "
                            f"configured_device={device}, active_device={service.ocr_service.active_device}"
                        )
                    return service.extract_text_result(file_path, file_extension)
                finally:
                    self._release_slots(indices)

        idx = self._acquire_slot()
        try:
            service = self._services[idx]
            device = self._devices[idx]
            if bool(getattr(settings, "PADDLE_OCR_MULTI_GPU_LOG_SCHEDULING", False)):
                print(
                    f"AnalysisServiceDispatcher: route request to worker={idx}, "
                    f"configured_device={device}, active_device={service.ocr_service.active_device}"
                )
            return service.extract_text_result(file_path, file_extension)
        finally:
            self._release_slot(idx)


def _normalize_device_token(raw_value: str) -> str:
    token = str(raw_value or "").strip()
    if not token:
        return ""
    if token.isdigit():
        return f"gpu:{token}"
    return token


def _discover_visible_gpu_devices() -> list[str]:
    for env_name in ("CUDA_VISIBLE_DEVICES", "NVIDIA_VISIBLE_DEVICES"):
        raw_value = str(os.environ.get(env_name, "") or "").strip()
        if not raw_value:
            continue

        lowered = raw_value.lower()
        if lowered in {"none", "void", "no", "false"}:
            return []
        if lowered not in {"all", "auto"}:
            entries = [
                entry.strip()
                for entry in raw_value.replace(";", ",").replace("|", ",").split(",")
                if entry.strip()
            ]
            if entries:
                return [f"gpu:{idx}" for idx in range(len(entries))]

    try:
        result = subprocess.run(
            ["nvidia-smi", "--query-gpu=index", "--format=csv,noheader"],
            check=False,
            capture_output=True,
            text=True,
            timeout=3,
        )
        if result.returncode == 0:
            lines = [line.strip() for line in result.stdout.splitlines() if line.strip()]
            if lines:
                return [f"gpu:{idx}" for idx in range(len(lines))]
    except Exception:
        pass

    try:
        import paddle

        cuda_module = getattr(getattr(paddle, "device", None), "cuda", None)
        if cuda_module is not None:
            device_count = getattr(cuda_module, "device_count", None)
            if callable(device_count):
                count = int(device_count() or 0)
                if count > 0:
                    return [f"gpu:{idx}" for idx in range(count)]
    except Exception:
        pass

    return []


def _resolve_ocr_device_pool() -> list[str]:
    raw_pool = str(getattr(settings, "PADDLE_OCR_DEVICE_POOL", "") or "").strip()
    if not raw_pool:
        return [_normalize_device_token(settings.PADDLE_OCR_DEVICE)]

    if raw_pool.lower() in {"auto", "all", "visible", "visible_gpus"}:
        devices = _discover_visible_gpu_devices()
        if devices:
            return devices
        fallback_device = _normalize_device_token(settings.PADDLE_OCR_DEVICE)
        return [fallback_device] if fallback_device else ["cpu"]

    normalized = raw_pool.replace(";", ",").replace("|", ",")
    devices: list[str] = []
    for item in normalized.split(","):
        token = _normalize_device_token(item)
        if not token:
            continue
        if token not in devices:
            devices.append(token)
    if devices:
        return devices

    fallback_device = _normalize_device_token(settings.PADDLE_OCR_DEVICE)
    return [fallback_device] if fallback_device else ["cpu"]


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
