import html
import os
from pathlib import Path
import re
import threading
from typing import Any

from app.config.settings import settings
from app.service.ocr_progress import OCRProgressMonitor
from app.service.table_parser import build_logical_tables, build_table_structure


class OCRService:
    def __init__(self, preferred_device: str | None = None):
        self.available = False
        self.pipeline = None
        self.preferred_device = str(preferred_device or "").strip() or None
        self.active_device = "cpu"
        self._predictor_lock = threading.Lock()

        self._prepare_runtime_dirs()
        self._prepare_runtime_env()
        self._init_engine()

    def _describe_document(self, file_path: str, file_type: str, total_pages: int) -> str:
        file_name = Path(file_path).name
        normalized_type = str(file_type or "").strip().lower().lstrip(".") or "unknown"
        page_label = total_pages if total_pages > 0 else "unknown"
        return f"file={file_name}, type={normalized_type}, estimated_pages={page_label}"

    def _runtime_cache_dirs(self) -> tuple[str, ...]:
        runtime_root = settings.OCR_STORAGE_ROOT
        return (
            str(runtime_root / ".cache"),
            str(runtime_root / "hf-home"),
            str(runtime_root / "hf-cache"),
            str(runtime_root / "modelscope-cache"),
            str(runtime_root / "aistudio-cache"),
        )

    def _prepare_runtime_dirs(self) -> None:
        for path in (
            settings.OCR_STORAGE_ROOT,
            settings.PADDLE_PDX_CACHE_HOME,
            settings.OCR_RUNTIME_TEMP_DIR,
            *self._runtime_cache_dirs(),
        ):
            os.makedirs(path, exist_ok=True)

    def _prepare_runtime_env(self) -> None:
        runtime_tmp = str(settings.OCR_RUNTIME_TEMP_DIR)
        runtime_root = settings.OCR_STORAGE_ROOT

        os.environ["PADDLE_PDX_CACHE_HOME"] = str(settings.PADDLE_PDX_CACHE_HOME)
        os.environ["PADDLE_PDX_DISABLE_MODEL_SOURCE_CHECK"] = (
            "1" if settings.PADDLE_PDX_DISABLE_MODEL_SOURCE_CHECK else "0"
        )
        os.environ["TMPDIR"] = runtime_tmp
        os.environ["TMP"] = runtime_tmp
        os.environ["TEMP"] = runtime_tmp
        os.environ["XDG_CACHE_HOME"] = str(runtime_root / ".cache")
        os.environ["HF_HOME"] = str(runtime_root / "hf-home")
        os.environ["HUGGINGFACE_HUB_CACHE"] = str(runtime_root / "hf-cache")
        os.environ["MODELSCOPE_CACHE"] = str(runtime_root / "modelscope-cache")
        os.environ["AISTUDIO_CACHE_HOME"] = str(runtime_root / "aistudio-cache")

    def _patch_paddle_tensor_int(self) -> None:
        try:
            import numpy as np
            import paddle
        except Exception:
            return

        tensor_type = type(paddle.to_tensor([0]))
        current_int = getattr(tensor_type, "__int__", None)
        if current_int is None or getattr(current_int, "_xtjs_len1_tensor_patch", False):
            return

        # PaddleOCR-VL's processor may produce shape=[1] tensors before casting.
        def _patched_tensor_int(value: Any) -> int:
            try:
                return current_int(value)
            except TypeError:
                array = np.asarray(value)
                if getattr(array, "size", 0) == 1:
                    return int(array.reshape(-1)[0].item())
                raise

        _patched_tensor_int._xtjs_len1_tensor_patch = True
        tensor_type.__int__ = _patched_tensor_int

    def _candidate_devices(self) -> list[str]:
        primary_device = self.preferred_device or settings.PADDLE_OCR_DEVICE
        candidates = [primary_device]
        if self.preferred_device is None and settings.PADDLE_OCR_DEVICE.startswith("gpu:"):
            candidates.append("gpu")
        if settings.PADDLE_OCR_FALLBACK_TO_CPU:
            candidates.append("cpu")

        unique_candidates: list[str] = []
        for device in candidates:
            token = str(device or "").strip()
            if token and token not in unique_candidates:
                unique_candidates.append(token)
        return unique_candidates

    def _build_pipeline_kwargs(self, device: str) -> dict[str, Any]:
        return {
            "device": device,
            "pipeline_version": settings.PADDLE_VL_PIPELINE_VERSION,
            "use_doc_orientation_classify": settings.PADDLE_OCR_USE_DOC_ORIENTATION,
            "use_doc_unwarping": settings.PADDLE_OCR_USE_DOC_UNWARPING,
            "use_layout_detection": settings.PADDLE_VL_USE_LAYOUT_DETECTION,
            "use_chart_recognition": settings.PADDLE_VL_USE_CHART_RECOGNITION,
            "use_seal_recognition": settings.PADDLE_VL_USE_SEAL_RECOGNITION,
            "use_ocr_for_image_block": settings.PADDLE_VL_USE_OCR_FOR_IMAGE_BLOCK,
            "format_block_content": settings.PADDLE_VL_FORMAT_BLOCK_CONTENT,
            "merge_layout_blocks": settings.PADDLE_VL_MERGE_LAYOUT_BLOCKS,
            "use_queues": settings.PADDLE_VL_USE_QUEUES,
        }

    def _init_engine(self) -> None:
        try:
            self._patch_paddle_tensor_int()
            from paddleocr import PaddleOCRVL
        except Exception as exc:
            print(f"OCRService bootstrap failed: {exc}", flush=True)
            return

        last_error: Exception | None = None
        for device in self._candidate_devices():
            try:
                print(
                    "OCRService: model loading started "
                    f"(device={device}, pipeline_version={settings.PADDLE_VL_PIPELINE_VERSION})",
                    flush=True,
                )
                self.pipeline = PaddleOCRVL(**self._build_pipeline_kwargs(device))
                self.available = True
                self.active_device = device
                print(
                    "OCRService: model loading completed "
                    f"(device={self.active_device}, pipeline_version={settings.PADDLE_VL_PIPELINE_VERSION})",
                    flush=True,
                )
                return
            except Exception as exc:
                last_error = exc
                print(f"OCRService: model loading failed (device={device}): {exc}", flush=True)

        self.pipeline = None
        self.available = False
        print(f"OCRService bootstrap failed: {last_error}", flush=True)

    def _estimate_total_pages(self, file_path: str, file_type: str) -> int:
        normalized_type = str(file_type or "").strip().lower().lstrip(".")
        if normalized_type in {"jpg", "jpeg", "png", "bmp", "tif", "tiff"}:
            return 1
        if normalized_type != "pdf":
            return 0

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

    def _build_progress_monitor(
        self,
        *,
        file_path: str,
        file_type: str,
        total_pages: int,
    ) -> OCRProgressMonitor:
        return OCRProgressMonitor(
            file_path=file_path,
            file_type=file_type,
            device=self.active_device,
            total_pages=total_pages,
            enabled=bool(getattr(settings, "OCR_PROGRESS_ENABLED", True)),
            bar_width=int(getattr(settings, "OCR_PROGRESS_BAR_WIDTH", 24)),
            keep_recent_updates=int(getattr(settings, "OCR_PROGRESS_KEEP_RECENT_UPDATES", 12)),
            heartbeat_seconds=float(getattr(settings, "OCR_PROGRESS_HEARTBEAT_SECONDS", 2.0)),
        )

    def _run_pipeline(
        self,
        input_path: str,
        *,
        progress_monitor: OCRProgressMonitor | None = None,
        total_pages: int = 0,
    ) -> list[Any]:
        if not self.available or self.pipeline is None:
            raise RuntimeError("PaddleOCR-VL-1.5 is unavailable.")

        with self._predictor_lock:
            if progress_monitor is not None:
                progress_monitor.update(
                    stage="predict",
                    current=0,
                    total=max(total_pages, 1),
                    detail="starting pipeline.predict_iter",
                    emit=False,
                )

            results: list[Any] = []
            for index, item in enumerate(self.pipeline.predict_iter(input_path), start=1):
                results.append(item)
                if progress_monitor is not None:
                    progress_monitor.update(
                        stage="predict",
                        current=index,
                        total=max(total_pages, index, 1),
                        detail=f"page/batch {index} completed",
                        emit=False,
                    )

            if settings.PADDLE_VL_RESTRUCTURE_PAGES and len(results) > 1:
                if progress_monitor is not None:
                    progress_monitor.update(
                        stage="restructure",
                        current=0,
                        total=1,
                        detail="restructure pages",
                        emit=False,
                    )
                results = list(
                    self.pipeline.restructure_pages(
                        results,
                        merge_tables=True,
                        relevel_titles=True,
                        concatenate_pages=False,
                    )
                )
                if progress_monitor is not None:
                    progress_monitor.update(
                        stage="restructure",
                        current=1,
                        total=1,
                        detail="restructure completed",
                        emit=False,
                    )
            return results

    def _to_builtin(self, value: Any) -> Any:
        if value is None or isinstance(value, (str, int, float, bool)):
            return value

        json_value = getattr(value, "json", None)
        if json_value is not None:
            try:
                payload = value.json
            except Exception:
                payload = None
            if isinstance(payload, dict) and "res" in payload:
                return self._to_builtin(payload["res"])

        if isinstance(value, dict):
            return {key: self._to_builtin(item) for key, item in value.items()}
        if isinstance(value, (list, tuple, set)):
            return [self._to_builtin(item) for item in value]
        if hasattr(value, "tolist"):
            try:
                return value.tolist()
            except Exception:
                pass
        if hasattr(value, "item"):
            try:
                return value.item()
            except Exception:
                pass
        return value

    def _dedupe_text_parts(self, parts: list[str]) -> list[str]:
        deduped: list[str] = []
        seen = set()
        for item in parts:
            normalized = str(item or "").strip()
            if not normalized:
                continue
            key = re.sub(r"\s+", " ", normalized)
            if key not in seen:
                seen.add(key)
                deduped.append(normalized)
        return deduped

    def _merge_text_parts(self, parts: list[str], *, join_char: str = "\n") -> str:
        filtered = self._dedupe_text_parts(parts)
        return join_char.join(filtered).strip()

    def _normalize_section_text(self, text: Any, *, preserve_lines: bool = False) -> str:
        normalized = html.unescape(str(text or ""))
        if preserve_lines:
            normalized = re.sub(r"\r\n?", "\n", normalized)
            normalized = re.sub(r"<br\s*/?>", "\n", normalized, flags=re.IGNORECASE)
            normalized = re.sub(
                r"</?(table|thead|tbody|tfoot|tr|p|div|section|article)[^>]*>",
                "\n",
                normalized,
                flags=re.IGNORECASE,
            )
            normalized = re.sub(r"</?(td|th)[^>]*>", "\t", normalized, flags=re.IGNORECASE)
            normalized = re.sub(r"<[^>]+>", " ", normalized)
            normalized = re.sub(r"[^\S\n\t]+", " ", normalized)
            normalized = re.sub(r" *\t *", "\t", normalized)
            normalized = re.sub(r"[ \t]*\n[ \t]*", "\n", normalized)
            normalized = re.sub(r"\n{3,}", "\n\n", normalized)

            lines: list[str] = []
            for line in normalized.splitlines():
                cells = [re.sub(r" {2,}", " ", cell).strip() for cell in line.split("\t")]
                cleaned = "\t".join(cells).strip()
                if cleaned:
                    lines.append(cleaned)
            return "\n".join(lines)

        normalized = re.sub(r"<[^>]+>", " ", normalized)
        normalized = re.sub(r"\s+", " ", normalized).strip()
        return normalized

    def _normalize_bbox(self, value: Any) -> Any:
        builtin_value = self._to_builtin(value)
        if builtin_value in (None, ""):
            return None
        return builtin_value

    def _bbox_anchor(self, bbox: Any) -> tuple[float, float]:
        builtin_bbox = self._to_builtin(bbox)
        if builtin_bbox is None:
            return (1e9, 1e9)
        if isinstance(builtin_bbox, (list, tuple)):
            if len(builtin_bbox) >= 2 and all(isinstance(item, (int, float)) for item in builtin_bbox[:2]):
                return (float(builtin_bbox[0]), float(builtin_bbox[1]))
            if builtin_bbox and all(
                isinstance(item, (list, tuple))
                and len(item) >= 2
                and isinstance(item[0], (int, float))
                and isinstance(item[1], (int, float))
                for item in builtin_bbox
            ):
                x_values = [float(item[0]) for item in builtin_bbox]
                y_values = [float(item[1]) for item in builtin_bbox]
                return (min(x_values), min(y_values))
        return (1e9, 1e9)

    def _bbox_to_xywh(self, bbox: Any) -> list[int] | None:
        builtin_bbox = self._to_builtin(bbox)
        if builtin_bbox is None:
            return None

        if (
            isinstance(builtin_bbox, (list, tuple))
            and len(builtin_bbox) >= 4
            and all(isinstance(item, (int, float)) for item in builtin_bbox[:4])
        ):
            x1, y1, x2, y2 = [int(round(float(item))) for item in builtin_bbox[:4]]
            width = max(1, x2 - x1)
            height = max(1, y2 - y1)
            return [x1, y1, width, height]

        if (
            isinstance(builtin_bbox, (list, tuple))
            and builtin_bbox
            and all(
                isinstance(item, (list, tuple))
                and len(item) >= 2
                and isinstance(item[0], (int, float))
                and isinstance(item[1], (int, float))
                for item in builtin_bbox
            )
        ):
            xs = [float(item[0]) for item in builtin_bbox]
            ys = [float(item[1]) for item in builtin_bbox]
            x1 = int(round(min(xs)))
            y1 = int(round(min(ys)))
            x2 = int(round(max(xs)))
            y2 = int(round(max(ys)))
            return [x1, y1, max(1, x2 - x1), max(1, y2 - y1)]

        return None

    def _normalize_layout_type(self, value: str) -> str:
        normalized = str(value or "").strip().lower()
        if not normalized:
            return "text"
        if "seal" in normalized:
            return "seal"
        if "table" in normalized:
            return "table"
        if any(token in normalized for token in ("title", "header", "heading")):
            return "heading"
        if any(token in normalized for token in ("figure", "image", "chart", "photo")):
            return "figure"
        return "text"

    def _extract_text_value(self, value: Any) -> str:
        builtin_value = self._to_builtin(value)
        if isinstance(builtin_value, str):
            return self._normalize_section_text(builtin_value, preserve_lines=True)
        if isinstance(builtin_value, list):
            return self._merge_text_parts(
                [self._extract_text_value(item) for item in builtin_value],
                join_char="\n",
            )
        if isinstance(builtin_value, dict):
            parts: list[str] = []
            for key in ("block_content", "markdown", "text", "html", "content", "caption"):
                candidate = builtin_value.get(key)
                if candidate:
                    parts.append(self._extract_text_value(candidate))
            return self._merge_text_parts(parts, join_char="\n")
        return ""

    def _page_number_from_payload(self, payload: dict[str, Any], fallback_page_no: int) -> int:
        raw_page_index = payload.get("page_index")
        if isinstance(raw_page_index, int):
            return raw_page_index + 1 if raw_page_index >= 0 else fallback_page_no
        return fallback_page_no

    def _build_table_section(self, page_no: int, block: dict[str, Any]) -> dict[str, Any] | None:
        raw_text = str(block.get("text") or "")
        normalized_raw_text = self._normalize_section_text(raw_text, preserve_lines=True)
        if len(normalized_raw_text) < 2:
            return None

        html_parts = [raw_text] if "<table" in raw_text.lower() else []
        section: dict[str, Any] = {
            "page": page_no,
            "type": "table",
            "text": normalized_raw_text,
        }

        bbox = self._normalize_bbox(block.get("bbox"))
        if bbox is not None:
            section["bbox"] = bbox
        if normalized_raw_text:
            section["raw_text"] = normalized_raw_text
        if html_parts:
            section["html"] = "\n\n".join(html_parts)

        table_structure = build_table_structure(
            html_parts=html_parts,
            raw_text=normalized_raw_text,
        )
        if table_structure is not None:
            section["table_structure"] = table_structure
        return section

    def _extract_layout_blocks(self, page_payload: dict[str, Any], page_no: int) -> list[dict[str, Any]]:
        blocks: list[dict[str, Any]] = []
        parsing_res_list = page_payload.get("parsing_res_list") or []
        for item_index, item in enumerate(parsing_res_list):
            built_item = self._to_builtin(item)
            if not isinstance(built_item, dict):
                continue

            label = str(
                built_item.get("block_label")
                or built_item.get("label")
                or built_item.get("type")
                or "text"
            ).strip()
            block_order = built_item.get("block_order")
            try:
                order = int(block_order) if block_order is not None else item_index + 1
            except (TypeError, ValueError):
                order = item_index + 1

            blocks.append(
                {
                    "page": page_no,
                    "label": label,
                    "type": self._normalize_layout_type(label),
                    "text": self._extract_text_value(built_item),
                    "bbox": self._normalize_bbox(
                        built_item.get("block_bbox")
                        or built_item.get("bbox")
                        or built_item.get("box")
                    ),
                    "_order": order,
                    "_raw": built_item,
                }
            )
        return blocks

    def _simplify_layout_sections(self, layout_blocks: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not layout_blocks:
            return []

        sorted_blocks = sorted(
            layout_blocks,
            key=lambda item: (
                int(item.get("page", 0) or 0),
                int(item.get("_order", 0) or 0),
                self._bbox_anchor(item.get("bbox"))[1],
                self._bbox_anchor(item.get("bbox"))[0],
            ),
        )

        sections: list[dict[str, Any]] = []
        seen = set()
        for block in sorted_blocks:
            section_type = str(block.get("type") or "text")
            if section_type not in {"heading", "text", "table", "seal"}:
                continue

            page_no = int(block.get("page", 0) or 0)
            if page_no <= 0:
                continue

            if section_type == "table":
                section = self._build_table_section(page_no, block)
                if section is None:
                    continue
            else:
                section_text = self._normalize_section_text(block.get("text") or "")
                if len(section_text) < 2:
                    continue
                section = {
                    "page": page_no,
                    "type": section_type,
                    "text": section_text,
                }
                bbox = self._normalize_bbox(block.get("bbox"))
                if bbox is not None:
                    section["bbox"] = bbox

            section_signature = (
                page_no,
                section["type"],
                section["text"],
                str(section.get("bbox")),
            )
            if section_signature in seen:
                continue
            seen.add(section_signature)
            sections.append(section)

        return sections

    def _extract_page_seals(self, page_payload: dict[str, Any], page_no: int) -> dict[str, Any]:
        seal_info = {"count": 0, "texts": [], "locations": []}
        parsing_res_list = page_payload.get("parsing_res_list") or []
        for item in parsing_res_list:
            built_item = self._to_builtin(item)
            if not isinstance(built_item, dict):
                continue

            label = str(
                built_item.get("block_label")
                or built_item.get("label")
                or built_item.get("type")
                or ""
            ).strip().lower()
            if "seal" not in label:
                continue

            text = self._normalize_section_text(
                built_item.get("block_content") or built_item.get("content") or ""
            )
            bbox = self._bbox_to_xywh(
                built_item.get("block_bbox")
                or built_item.get("bbox")
                or built_item.get("box")
            )

            seal_info["count"] += 1
            if text:
                seal_info["texts"].append(text)
            if bbox is not None:
                seal_info["locations"].append({"page": page_no, "box": bbox})

        seal_info["texts"] = self._dedupe_text_parts(seal_info["texts"])
        return seal_info

    def _extract_page_text(
        self,
        page_sections: list[dict[str, Any]],
        page_payload: dict[str, Any],
    ) -> str:
        section_text = self._merge_text_parts(
            [
                str(section.get("text") or "")
                for section in page_sections
                if str(section.get("type") or "") in {"heading", "text", "table", "seal"}
            ],
            join_char="\n",
        )
        if section_text:
            return section_text

        fallback_parts: list[str] = []
        for item in page_payload.get("parsing_res_list") or []:
            built_item = self._to_builtin(item)
            if not isinstance(built_item, dict):
                continue
            label = str(
                built_item.get("block_label")
                or built_item.get("label")
                or built_item.get("type")
                or ""
            ).strip().lower()
            if label in {"image", "chart"}:
                continue
            candidate = self._normalize_section_text(
                built_item.get("block_content") or built_item.get("content") or "",
                preserve_lines=True,
            )
            if candidate:
                fallback_parts.append(candidate)
        return self._merge_text_parts(fallback_parts, join_char="\n")

    def _attach_table_outputs(self, result: dict[str, Any] | None) -> dict[str, Any]:
        payload = dict(result or {})
        layout_sections = payload.get("layout_sections") or []
        if not isinstance(layout_sections, list):
            payload["logical_tables"] = []
            return payload

        payload["logical_tables"] = build_logical_tables(layout_sections)
        return payload

    def extract_all(self, file_path: str, file_type: str = "pdf") -> dict[str, Any]:
        total_pages = self._estimate_total_pages(file_path, file_type)
        print(
            "OCRService: OCR inference started "
            f"({self._describe_document(file_path, file_type, total_pages)}, device={self.active_device})",
            flush=True,
        )
        progress_monitor = self._build_progress_monitor(
            file_path=file_path,
            file_type=file_type,
            total_pages=total_pages,
        )
        progress_monitor.start()

        try:
            results = self._run_pipeline(
                file_path,
                progress_monitor=progress_monitor,
                total_pages=total_pages,
            )
            if not results:
                raise RuntimeError("PaddleOCR-VL-1.5 returned no results.")

            total_result_pages = max(total_pages, len(results))
            pages: list[dict[str, Any]] = []
            layout_sections: list[dict[str, Any]] = []
            all_seal_texts: list[str] = []
            all_seal_locations: list[dict[str, Any]] = []
            progress_monitor.update(
                stage="postprocess",
                current=0,
                total=max(total_result_pages, 1),
                detail="normalizing OCR results",
                emit=False,
            )

            for fallback_page_no, result in enumerate(results, start=1):
                page_payload = self._to_builtin(result)
                if not isinstance(page_payload, dict):
                    continue

                page_no = self._page_number_from_payload(page_payload, fallback_page_no)
                page_blocks = self._extract_layout_blocks(page_payload, page_no)
                page_sections = self._simplify_layout_sections(page_blocks)
                page_text = self._extract_page_text(page_sections, page_payload)
                page_seals = self._extract_page_seals(page_payload, page_no)

                pages.append({"page": page_no, "text": page_text})
                layout_sections.extend(page_sections)
                all_seal_texts.extend(page_seals["texts"])
                all_seal_locations.extend(page_seals["locations"])

                progress_monitor.update(
                    stage="postprocess",
                    current=fallback_page_no,
                    total=max(total_result_pages, fallback_page_no, 1),
                    detail=f"parsed page {page_no}",
                    emit=False,
                )

            full_text = self._merge_text_parts(
                [str(page.get("text") or "") for page in pages],
                join_char="\n",
            )

            payload = {
                "text": full_text,
                "pages": pages,
                "seals": {
                    "count": len(all_seal_locations),
                    "texts": self._dedupe_text_parts(all_seal_texts),
                    "locations": all_seal_locations,
                },
                "layout_sections": layout_sections,
                "logical_tables": [],
                "ocr_applied": True,
                "structure_used": bool(layout_sections),
                "structure_enabled": bool(settings.PADDLE_VL_USE_LAYOUT_DETECTION),
                "seal_recognition_enabled": bool(settings.PADDLE_VL_USE_SEAL_RECOGNITION),
                "engine": "PaddleOCR-VL-1.5",
            }

            progress_monitor.update(
                stage="tables",
                current=0,
                total=1,
                detail="building logical tables",
                emit=False,
            )
            payload = self._attach_table_outputs(payload)
            progress_monitor.update(
                stage="tables",
                current=1,
                total=1,
                detail="logical tables ready",
                emit=False,
            )

            progress_monitor.finish(success=True)
            return payload
        except Exception as exc:
            progress_summary = progress_monitor.finish(success=False, error_message=str(exc))
            if isinstance(exc, RuntimeError) and str(exc):
                raise
            raise RuntimeError(
                f"OCR failed after {progress_summary.get('total_elapsed_seconds', 0)}s: {exc}"
            ) from exc
