import html
import os
from pathlib import Path
import re
import shutil
import threading
import uuid
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from app.config.settings import settings
from app.service.ocr_progress import OCRProgressMonitor
from app.service.table_parser import build_logical_tables, build_table_structure


class OCRService:
    RUNNING_HEADER_HEADING_RE = re.compile(
        r"^第[一二三四五六七八九十0-9]+章|^[一二三四五六七八九十]+、|^[（(][一二三四五六七八九十A-Za-z0-9]+[)）]|^\d+\s*[)）.．、]|^(附件|附表|附录)\s*\d+(?:-\d+)?"
    )

    SIGNATURE_ANCHOR_TOKENS = ("签字", "签章", "签名", "手签")
    SIGNATURE_PLACEHOLDER_TOKENS = (
        "手写签字",
        "签字",
        "签名",
        "手写",
        "字或盖章",
        "签字或盖章",
        "盖章",
    )
    SIGNATURE_CANDIDATE_BLOCKED_WORDS = (
        "签字",
        "签章",
        "签名",
        "盖章",
        "授权",
        "代表",
        "法定",
        "日期",
        "公司",
        "有限公司",
        "项目",
        "投标",
        "科技",
        "电磁",
    )
    SIGNATURE_CANDIDATE_BLOCKED_CHARS = set("签章盖日期公司项目科技设备授权代表投标电磁")

    DEFAULT_SIGNATURE_PLACEHOLDER_TEXT = "已签字"

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
            "use_signature_recognition": settings.PADDLE_VL_USE_SIGNATURE_RECOGNITION,
            "use_ocr_for_image_block": settings.PADDLE_VL_USE_OCR_FOR_IMAGE_BLOCK,
            "format_block_content": settings.PADDLE_VL_FORMAT_BLOCK_CONTENT,
            "merge_layout_blocks": settings.PADDLE_VL_MERGE_LAYOUT_BLOCKS,
            "use_queues": settings.PADDLE_VL_USE_QUEUES,
        }

    def _extract_unknown_argument(self, exc: Exception) -> str | None:
        match = re.search(r"Unknown argument:\s*([A-Za-z0-9_]+)", str(exc or ""))
        return match.group(1) if match else None

    def _instantiate_pipeline(self, pipeline_cls: Any, device: str) -> tuple[Any, list[str]]:
        kwargs = dict(self._build_pipeline_kwargs(device))
        disabled_args: list[str] = []

        while True:
            try:
                return pipeline_cls(**kwargs), disabled_args
            except Exception as exc:
                unknown_arg = self._extract_unknown_argument(exc)
                if not unknown_arg or unknown_arg not in kwargs:
                    raise
                kwargs.pop(unknown_arg, None)
                disabled_args.append(unknown_arg)
                print(
                    "OCRService: retrying model load without unsupported argument "
                    f"{unknown_arg!r} (device={device})",
                    flush=True,
                )

    def _init_engine(self) -> None:
        if os.name == "nt":
            try:
                # On Windows, importing torch first can avoid a DLL search-order issue
                # triggered when paddleocr/modelscope pulls torch in later.
                import torch  # noqa: F401
            except Exception as exc:
                print(
                    "OCRService: optional torch preload skipped on Windows "
                    f"(reason: {exc})",
                    flush=True,
                )

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
                self.pipeline, disabled_args = self._instantiate_pipeline(PaddleOCRVL, device)
                self.available = True
                self.active_device = device
                if disabled_args:
                    print(
                        "OCRService: model loading completed with compatibility fallback "
                        f"(device={self.active_device}, disabled_args={disabled_args})",
                        flush=True,
                    )
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

    def _stage_input_for_pipeline(self, file_path: str) -> tuple[str, Path | None]:
        source = Path(file_path)
        try:
            source_str = str(source)
            source_str.encode("ascii")
            return source_str, None
        except UnicodeEncodeError:
            pass

        staging_dir = Path(settings.OCR_RUNTIME_TEMP_DIR) / "input-staging"
        staging_dir.mkdir(parents=True, exist_ok=True)
        suffix = source.suffix or ".bin"
        staged_path = staging_dir / f"{uuid.uuid4().hex}{suffix}"
        shutil.copy2(source, staged_path)
        return str(staged_path), staged_path

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
                        detail=f"page/batch {index} predicted",
                        emit=True,
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

    def _signature_placeholder_text(self) -> str:
        configured = self._normalize_section_text(
            getattr(settings, "OCR_SIGNATURE_PLACEHOLDER_TEXT", self.DEFAULT_SIGNATURE_PLACEHOLDER_TEXT)
        )
        return configured or self.DEFAULT_SIGNATURE_PLACEHOLDER_TEXT

    def _xywh_to_bbox(self, bbox: list[int] | None) -> list[int] | None:
        if bbox is None or len(bbox) < 4:
            return None
        left, top, width, height = [int(round(float(item))) for item in bbox[:4]]
        return [left, top, left + max(1, width), top + max(1, height)]

    def _clip_xywh_to_page(
        self,
        bbox: list[int] | None,
        page_size: tuple[int, int] | None,
    ) -> list[int] | None:
        if bbox is None or len(bbox) < 4:
            return bbox
        if page_size is None:
            return [int(round(float(item))) for item in bbox[:4]]

        page_width, page_height = page_size
        if page_width <= 0 or page_height <= 0:
            return [int(round(float(item))) for item in bbox[:4]]

        left, top, width, height = [int(round(float(item))) for item in bbox[:4]]
        left = max(0, min(left, page_width - 1))
        top = max(0, min(top, page_height - 1))
        width = max(1, min(width, page_width - left))
        height = max(1, min(height, page_height - top))
        return [left, top, width, height]

    def _page_payload_size(self, page_payload: dict[str, Any]) -> tuple[int, int] | None:
        try:
            width = int(round(float(page_payload.get("width") or 0)))
            height = int(round(float(page_payload.get("height") or 0)))
        except (TypeError, ValueError):
            return None

        if width <= 0 or height <= 0:
            return None
        return (width, height)

    def _load_pdf_page_sizes(self, file_path: str, file_type: str) -> dict[int, tuple[float, float]]:
        normalized_type = str(file_type or "").strip().lower().lstrip(".")
        if normalized_type != "pdf":
            return {}

        try:
            import pypdfium2 as pdfium
        except Exception:
            return {}

        document = pdfium.PdfDocument(file_path)
        page_sizes: dict[int, tuple[float, float]] = {}
        try:
            for page_index in range(len(document)):
                page = document[page_index]
                try:
                    width = float(page.get_width())
                    height = float(page.get_height())
                except Exception:
                    try:
                        width, height = page.get_size()
                        width = float(width)
                        height = float(height)
                    except Exception:
                        continue
                if width > 0 and height > 0:
                    page_sizes[page_index + 1] = (width, height)
                close_page = getattr(page, "close", None)
                if callable(close_page):
                    close_page()
        finally:
            close_document = getattr(document, "close", None)
            if callable(close_document):
                close_document()

        return page_sizes

    def _scale_bbox_to_pdf(
        self,
        bbox: Any,
        ocr_image_size: tuple[int, int] | None,
        pdf_page_size: tuple[float, float] | None,
    ) -> Any:
        builtin_bbox = self._to_builtin(bbox)
        if builtin_bbox is None or ocr_image_size is None or pdf_page_size is None:
            return self._normalize_bbox(builtin_bbox)

        image_width, image_height = ocr_image_size
        pdf_width, pdf_height = pdf_page_size
        if image_width <= 0 or image_height <= 0 or pdf_width <= 0 or pdf_height <= 0:
            return self._normalize_bbox(builtin_bbox)

        scale_x = float(pdf_width) / float(image_width)
        scale_y = float(pdf_height) / float(image_height)

        if (
            isinstance(builtin_bbox, (list, tuple))
            and len(builtin_bbox) >= 4
            and all(isinstance(item, (int, float)) for item in builtin_bbox[:4])
        ):
            x1, y1, x2, y2 = [float(item) for item in builtin_bbox[:4]]
            return [
                int(round(x1 * scale_x)),
                int(round(y1 * scale_y)),
                int(round(x2 * scale_x)),
                int(round(y2 * scale_y)),
            ]

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
            return [
                [
                    int(round(float(item[0]) * scale_x)),
                    int(round(float(item[1]) * scale_y)),
                ]
                for item in builtin_bbox
            ]

        return self._normalize_bbox(builtin_bbox)

    def _scale_xywh_to_pdf(
        self,
        bbox: list[int] | None,
        ocr_image_size: tuple[int, int] | None,
        pdf_page_size: tuple[float, float] | None,
    ) -> list[int] | None:
        if bbox is None or len(bbox) < 4 or ocr_image_size is None or pdf_page_size is None:
            return bbox

        image_width, image_height = ocr_image_size
        pdf_width, pdf_height = pdf_page_size
        if image_width <= 0 or image_height <= 0 or pdf_width <= 0 or pdf_height <= 0:
            return bbox

        scale_x = float(pdf_width) / float(image_width)
        scale_y = float(pdf_height) / float(image_height)
        left, top, width, height = [float(item) for item in bbox[:4]]
        return [
            int(round(left * scale_x)),
            int(round(top * scale_y)),
            max(1, int(round(width * scale_x))),
            max(1, int(round(height * scale_y))),
        ]

    def _page_coordinate_context(
        self,
        page_payload: dict[str, Any],
        page_no: int,
        pdf_page_sizes: dict[int, tuple[float, float]] | None,
    ) -> dict[str, Any]:
        ocr_image_size = self._page_payload_size(page_payload)
        pdf_page_size = (pdf_page_sizes or {}).get(page_no)
        bbox_coordinate_space = "pdf" if ocr_image_size and pdf_page_size else "ocr_image"
        return {
            "bbox_coordinate_space": bbox_coordinate_space,
            "ocr_image_size": ocr_image_size,
            "pdf_page_size": pdf_page_size,
        }

    def _project_section_for_output(
        self,
        section: dict[str, Any],
        coordinate_context: dict[str, Any],
    ) -> dict[str, Any]:
        projected = dict(section)
        bbox = self._normalize_bbox(section.get("bbox"))
        if bbox is None:
            return projected

        projected["bbox_ocr"] = bbox
        projected["bbox"] = self._scale_bbox_to_pdf(
            bbox,
            coordinate_context.get("ocr_image_size"),
            coordinate_context.get("pdf_page_size"),
        )
        return projected

    def _project_detection_info_for_output(
        self,
        info: dict[str, Any],
        coordinate_context: dict[str, Any],
    ) -> dict[str, Any]:
        projected = {
            "count": int(info.get("count", 0) or 0),
            "texts": list(info.get("texts") or []),
            "locations": [],
        }
        for item in info.get("locations") or []:
            if not isinstance(item, dict):
                continue
            projected_item = dict(item)
            box = self._to_builtin(item.get("box"))
            if isinstance(box, list) and len(box) >= 4:
                projected_item["box_ocr"] = [int(round(float(value))) for value in box[:4]]
                projected_item["box"] = self._scale_xywh_to_pdf(
                    projected_item["box_ocr"],
                    coordinate_context.get("ocr_image_size"),
                    coordinate_context.get("pdf_page_size"),
                )
            projected["locations"].append(projected_item)

        projected["texts"] = self._dedupe_text_parts(projected["texts"])
        return projected

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

    def _bbox_signature_key(self, bbox: Any) -> tuple[int, int, int, int] | None:
        xywh = self._bbox_to_xywh(bbox)
        return tuple(xywh) if xywh is not None else None

    def _normalize_layout_type(self, value: str) -> str:
        normalized = str(value or "").strip().lower()
        if not normalized:
            return "text"
        if "signature" in normalized:
            return "signature"
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
        native_table = self._extract_native_table_payload(block, page_no)
        if native_table:
            section["native_table"] = native_table

        table_structure = build_table_structure(
            html_parts=html_parts,
            raw_text=normalized_raw_text,
        )
        if table_structure is not None:
            section["table_structure"] = table_structure
        return section

    def _extract_native_table_payload(self, block: dict[str, Any], page_no: int) -> dict[str, Any] | None:
        raw_payload = self._to_builtin(block.get("_raw"))
        if not isinstance(raw_payload, dict):
            raw_payload = {}

        native_payload = dict(raw_payload)
        if page_no > 0 and not isinstance(native_payload.get("page"), int):
            native_payload["page"] = page_no

        bbox = self._normalize_bbox(
            native_payload.get("block_bbox")
            or native_payload.get("bbox")
            or native_payload.get("box")
            or block.get("bbox")
        )
        if bbox is not None and not any(key in native_payload for key in ("block_bbox", "bbox", "box")):
            native_payload["bbox"] = bbox

        if not native_payload:
            return None
        return native_payload

    def _collect_native_tables(self, layout_sections: list[dict[str, Any]]) -> list[dict[str, Any]]:
        native_tables: list[dict[str, Any]] = []
        for section in layout_sections:
            if not isinstance(section, dict):
                continue
            if str(section.get("type") or "").strip().lower() != "table":
                continue
            native_table = section.get("native_table")
            if isinstance(native_table, dict):
                native_tables.append(dict(native_table))
        return native_tables

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
                self._bbox_anchor(item.get("bbox"))[1],
                self._bbox_anchor(item.get("bbox"))[0],
                int(item.get("_order", 0) or 0),
            ),
        )

        sections: list[dict[str, Any]] = []
        seen = set()
        for block in sorted_blocks:
            section_type = str(block.get("type") or "text")
            if section_type not in {"heading", "text", "table", "seal", "signature"}:
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
                if section_type == "signature":
                    if not section_text and self._normalize_bbox(block.get("bbox")) is None:
                        continue
                elif len(section_text) < 2:
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

    def _is_signature_placeholder_text(self, text: Any) -> bool:
        normalized = self._normalize_section_text(text)
        if not normalized:
            return False

        compact = re.sub(r"\s+", "", normalized)
        compact = re.sub(r"[：:_＿\-—.·•()\[\]（）【】]", "", compact)
        if compact in self.SIGNATURE_PLACEHOLDER_TOKENS:
            return True
        return compact.endswith("签字") and len(compact) <= 6

    def _normalize_signature_candidate_text(self, text: Any) -> str:
        normalized = self._normalize_section_text(text)
        if not normalized:
            return ""

        compact = re.sub(r"\s+", "", normalized)
        compact = re.sub(r"[_＿:：()\[\]（）【】\-—.·•]", "", compact)
        if not compact or len(compact) < 2 or len(compact) > 12:
            return ""
        if any(token in compact for token in self.SIGNATURE_CANDIDATE_BLOCKED_WORDS):
            return ""
        if re.search(r"\d", compact):
            return ""

        cleaned = re.sub(r"[^A-Za-z\u4e00-\u9fff]", "", compact)
        if not cleaned or len(cleaned) < 2 or len(cleaned) > 8:
            return ""
        if any(char in self.SIGNATURE_CANDIDATE_BLOCKED_CHARS for char in cleaned):
            return ""
        if re.fullmatch(r"[\u4e00-\u9fff]{2,4}", cleaned):
            return cleaned
        if re.fullmatch(r"[A-Za-z]{2,20}", cleaned):
            return cleaned
        if re.fullmatch(r"[\u4e00-\u9fffA-Za-z]{2,6}", cleaned):
            return cleaned
        return ""

    def _is_signature_anchor_text(self, text: Any) -> bool:
        compact = re.sub(r"\s+", "", self._normalize_section_text(text))
        return bool(compact and any(token in compact for token in self.SIGNATURE_ANCHOR_TOKENS))

    def _boxes_are_close(
        self,
        left_bbox: list[int] | None,
        right_bbox: list[int] | None,
        *,
        max_dx: int = 260,
        max_dy: int = 120,
    ) -> bool:
        if left_bbox is None or right_bbox is None:
            return False

        left_x, left_y, left_w, left_h = left_bbox
        right_x, right_y, right_w, right_h = right_bbox
        left_right = left_x + left_w
        right_right = right_x + right_w
        left_bottom = left_y + left_h

        same_band = abs(left_y - right_y) <= max(max(left_h, right_h), 28)
        horizontal_overlap = right_right >= left_x - 80 and right_x <= left_right + max_dx
        below = 0 <= right_y - left_bottom <= max_dy and right_right >= left_x - 80 and right_x <= left_right + max_dx
        return (same_band and horizontal_overlap) or below

    def _bbox_distance(self, source_bbox: list[int] | None, target_bbox: list[int] | None) -> int:
        if source_bbox is None or target_bbox is None:
            return 10**9

        source_x, source_y, source_w, source_h = source_bbox
        target_x, target_y, target_w, target_h = target_bbox
        source_center_x = source_x + source_w / 2
        source_center_y = source_y + source_h / 2
        target_center_x = target_x + target_w / 2
        target_center_y = target_y + target_h / 2

        return int(abs(source_center_x - target_center_x) + abs(source_center_y - target_center_y))

    def _resolve_signature_section_text(
        self,
        signature_section: dict[str, Any],
        page_blocks: list[dict[str, Any]],
    ) -> str:
        raw_text = self._normalize_section_text(signature_section.get("text") or "")
        if raw_text and not self._is_signature_placeholder_text(raw_text):
            return raw_text

        signature_bbox = self._bbox_to_xywh(signature_section.get("bbox"))
        best_text = ""
        best_score: int | None = None

        for block in page_blocks:
            block_type = str(block.get("type") or "").strip().lower()
            if block_type not in {"text", "figure"}:
                continue

            candidate_text = self._normalize_signature_candidate_text(block.get("text") or "")
            if not candidate_text:
                continue

            candidate_bbox = self._bbox_to_xywh(block.get("bbox"))
            if signature_bbox is not None and candidate_bbox is not None:
                if not self._boxes_are_close(signature_bbox, candidate_bbox, max_dx=320, max_dy=160):
                    continue

            score = self._bbox_distance(signature_bbox, candidate_bbox)
            if block_type == "figure":
                score -= 16

            if best_score is None or score < best_score:
                best_score = score
                best_text = candidate_text

        return best_text or raw_text

    def _merge_signature_into_anchor(
        self,
        signature_section: dict[str, Any],
        page_sections: list[dict[str, Any]],
    ) -> None:
        signature_text = self._normalize_section_text(signature_section.get("text") or "")
        if not signature_text:
            return

        signature_bbox = self._bbox_to_xywh(signature_section.get("bbox"))
        signature_page = int(signature_section.get("page", 0) or 0)
        best_anchor: dict[str, Any] | None = None
        best_score: int | None = None

        for section in page_sections:
            if section is signature_section:
                continue
            if int(section.get("page", 0) or 0) != signature_page:
                continue
            if str(section.get("type") or "").strip().lower() not in {"heading", "text"}:
                continue

            anchor_text = self._normalize_section_text(section.get("text") or "")
            if not self._is_signature_anchor_text(anchor_text):
                continue

            anchor_bbox = self._bbox_to_xywh(section.get("bbox"))
            if signature_bbox is not None and anchor_bbox is not None:
                if not self._boxes_are_close(anchor_bbox, signature_bbox, max_dx=420, max_dy=180):
                    continue

            score = self._bbox_distance(anchor_bbox, signature_bbox)
            if best_score is None or score < best_score:
                best_score = score
                best_anchor = section

        if best_anchor is None:
            return

        anchor_text = self._normalize_section_text(best_anchor.get("text") or "")
        anchor_compact = re.sub(r"\s+", "", anchor_text)
        signature_compact = re.sub(r"\s+", "", signature_text)
        if signature_compact and signature_compact in anchor_compact:
            signature_section["_merged"] = True
            return

        merged_text = re.sub(r"[_＿]{2,}$", "", anchor_text).rstrip()
        if re.search(r"[：:]\s*$", merged_text):
            merged_text = f"{merged_text}{signature_text}"
        elif any(token in merged_text for token in self.SIGNATURE_ANCHOR_TOKENS):
            merged_text = f"{merged_text.rstrip('：: ')}：{signature_text}"
        else:
            return

        best_anchor["text"] = merged_text
        signature_section["_merged"] = True

    def _enrich_page_signature_sections(
        self,
        page_sections: list[dict[str, Any]],
        page_blocks: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        if not page_sections:
            return page_sections

        for section in page_sections:
            if str(section.get("type") or "").strip().lower() != "signature":
                continue

            resolved_text = self._resolve_signature_section_text(section, page_blocks)
            if resolved_text:
                section["text"] = resolved_text
            self._merge_signature_into_anchor(section, page_sections)

        return page_sections

    def _include_section_in_page_text(self, section: dict[str, Any]) -> bool:
        if section.get("_merged"):
            return False
        return str(section.get("type") or "") in {"heading", "text", "table", "seal", "signature"}

    def _is_signature_placeholder_text(self, text: Any) -> bool:
        normalized = self._normalize_section_text(text)
        if not normalized:
            return False

        compact = re.sub(r"\s+", "", normalized)
        compact = re.sub(r"[：:_＿\-\u2014.·•()\[\]（）【】]", "", compact)
        if compact == re.sub(r"\s+", "", self._signature_placeholder_text()):
            return True
        if compact in self.SIGNATURE_PLACEHOLDER_TOKENS:
            return True
        return compact.endswith("签字") and len(compact) <= 6

    def _is_signature_anchor_text(self, text: Any) -> bool:
        compact = re.sub(r"\s+", "", self._normalize_section_text(text))
        if not compact:
            return False
        return bool(
            re.search(
                r"(签字或盖章|签章或盖章|签名或盖章|签字|签章|签名|手签)[\)）】】]*([：:]|$)",
                compact,
            )
        )

    def _is_table_signature_anchor_text(self, text: Any) -> bool:
        compact = re.sub(r"\s+", "", self._normalize_section_text(text))
        if not compact:
            return False
        return bool(
            re.search(
                r"(法定代表人(?:或其)?(?:委托代理人|授权代理人|授权代表)?|委托代理人|授权代理人|授权代表)[^：:\n]{0,20}[：:]",
                compact,
            )
        )

    def _find_signature_anchor_match(
        self,
        text: Any,
    ) -> tuple[str, int, int, str, str] | None:
        normalized = self._normalize_section_text(text)
        if not normalized:
            return None

        patterns = (
            re.compile(
                r"(?P<prefix>(?:签字或盖章|签章或盖章|签名或盖章|签字|签章|签名|手签)[\)）】】]*[：:])\s*(?P<value>[^)）\]】>\n]{0,24})"
            ),
            re.compile(
                r"(?P<prefix>(?:法定代表人(?:或其)?(?:委托代理人|授权代理人|授权代表)?|委托代理人|授权代理人|授权代表)[^：:\n]{0,20}[：:])\s*(?P<value>[^)）\]】>\n]{0,24})"
            ),
        )

        best_match: tuple[str, int, int, str, str] | None = None
        for pattern in patterns:
            for match in pattern.finditer(normalized):
                best_match = (
                    normalized,
                    match.start(),
                    match.end(),
                    str(match.group("prefix") or ""),
                    self._normalize_section_text(match.group("value") or ""),
                )
        return best_match

    def _extract_signature_anchor_value(self, text: Any) -> str:
        match = self._find_signature_anchor_match(text)
        if match is None:
            return ""

        _, _, _, _, value = match
        return re.sub(r"^[_＿\-\u2014~.·•\s]+|[_＿\-\u2014~.·•\s]+$", "", value)

    def _strip_signature_anchor_value(self, text: Any) -> str:
        match = self._find_signature_anchor_match(text)
        normalized = self._normalize_section_text(text)
        if match is None or not normalized:
            return normalized

        _, start, _, prefix, _ = match
        return f"{normalized[:start]}{prefix}"

    def _build_signature_anchor_text(self, text: Any) -> str:
        match = self._find_signature_anchor_match(text)
        placeholder = self._signature_placeholder_text()
        if match is None:
            prefix = self._strip_signature_anchor_value(text)
            if not prefix:
                return placeholder
            if prefix.endswith(("：", ":")):
                return self._clean_signature_anchor_text(f"{prefix}{placeholder}")
            return self._clean_signature_anchor_text(f"{prefix.rstrip('：: ')}：{placeholder}")

        normalized, start, end, prefix, _ = match
        if not prefix:
            return placeholder
        return self._clean_signature_anchor_text(f"{normalized[:start]}{prefix}{placeholder}{normalized[end:]}")

    def _clean_signature_anchor_text(self, text: Any) -> str:
        normalized = self._normalize_section_text(text)
        if not normalized:
            return ""

        placeholder = re.escape(self._signature_placeholder_text())
        normalized = normalized.replace("$", "")
        normalized = re.sub(rf"({placeholder})\s*([（(]\s*(?:签字或盖章|签章或盖章|签名或盖章|签字|签章|签名)\s*[)）])", r"\1\2", normalized)
        normalized = re.sub(
            r"([（(]\s*(?:签字或盖章|签章或盖章|签名或盖章|签字|签章|签名)\s*[)）])\s*[A-Za-z\u4e00-\u9fff]{1,4}(?=(?:\s|$|日期|20\d{2}年|\d{4}年))",
            r"\1",
            normalized,
        )
        normalized = re.sub(
            rf"({placeholder})\s*[)）]?\s*[A-Za-z\u4e00-\u9fff]{{1,4}}(?=(?:\s|$|日期|20\d{{2}}年|\d{{4}}年))",
            r"\1",
            normalized,
        )
        normalized = re.sub(
            rf"({placeholder})\s*[)）](?=(?:\s|$|日期|20\d{{2}}年|\d{{4}}年))",
            r"\1",
            normalized,
        )
        normalized = re.sub(r"\s{2,}", " ", normalized).strip()
        return normalized

    def _estimate_signature_bbox_from_anchor(
        self,
        anchor_bbox: Any,
        page_image_size: tuple[int, int] | None,
    ) -> list[int] | None:
        anchor_xywh = self._bbox_to_xywh(anchor_bbox)
        if anchor_xywh is None:
            return None

        left, top, width, height = anchor_xywh
        estimated_bbox = [
            left + int(width * 0.56),
            top - int(height * 0.35),
            max(132, int(width * 0.26)),
            max(72, int(height * 1.9)),
        ]
        return self._clip_xywh_to_page(estimated_bbox, page_image_size)

    def _estimate_signature_bbox_from_text_anchor(
        self,
        anchor_section: dict[str, Any],
        page_sections: list[dict[str, Any]],
        page_blocks: list[dict[str, Any]],
        page_image_size: tuple[int, int] | None,
    ) -> list[int] | None:
        anchor_xywh = self._bbox_to_xywh(anchor_section.get("bbox"))
        if anchor_xywh is None:
            return self._estimate_signature_bbox_from_anchor(anchor_section.get("bbox"), page_image_size)

        nearest_seal_bbox = self._find_nearest_seal_bbox(anchor_xywh, page_sections, page_blocks)
        if nearest_seal_bbox is not None:
            seal_distance = self._bbox_distance(anchor_xywh, nearest_seal_bbox)
            if seal_distance <= max(280, anchor_xywh[2] + nearest_seal_bbox[2]):
                anchor_left, anchor_top, anchor_width, anchor_height = anchor_xywh
                seal_left, seal_top, seal_width, seal_height = nearest_seal_bbox
                estimated_bbox = [
                    max(0, min(anchor_left + int(anchor_width * 0.34), seal_left - int(seal_width * 0.72))),
                    max(0, min(anchor_top, seal_top) - int(max(anchor_height, seal_height) * 0.28)),
                    max(108, int(max(anchor_width * 0.20, seal_width * 2.1))),
                    max(72, int(max(anchor_height, seal_height) * 1.45)),
                ]
                return self._clip_xywh_to_page(estimated_bbox, page_image_size)

        return self._estimate_signature_bbox_from_anchor(anchor_section.get("bbox"), page_image_size)

    def _find_nearest_seal_bbox(
        self,
        reference_bbox: list[int] | None,
        page_sections: list[dict[str, Any]],
        page_blocks: list[dict[str, Any]],
    ) -> list[int] | None:
        if reference_bbox is None:
            return None

        best_bbox: list[int] | None = None
        best_score: int | None = None
        sources = [*page_sections, *page_blocks]
        for item in sources:
            if str(item.get("type") or "").strip().lower() != "seal":
                continue
            bbox = self._bbox_to_xywh(item.get("bbox"))
            if bbox is None:
                continue
            score = self._bbox_distance(reference_bbox, bbox)
            if best_score is None or score < best_score:
                best_score = score
                best_bbox = bbox
        return best_bbox

    def _estimate_signature_bbox_from_table_anchor(
        self,
        anchor_section: dict[str, Any],
        page_sections: list[dict[str, Any]],
        page_blocks: list[dict[str, Any]],
        page_image_size: tuple[int, int] | None,
    ) -> list[int] | None:
        table_bbox = self._bbox_to_xywh(anchor_section.get("bbox"))
        if table_bbox is None:
            return None

        left, top, width, height = table_bbox
        nearest_seal_bbox = self._find_nearest_seal_bbox(table_bbox, page_sections, page_blocks)
        if nearest_seal_bbox is not None:
            seal_left, seal_top, seal_width, seal_height = nearest_seal_bbox
            estimated_bbox = [
                max(left + int(width * 0.46), seal_left + int(seal_width * 0.85)),
                max(top + int(height * 0.72), seal_top - int(seal_height * 0.18)),
                max(144, int(width * 0.22)),
                max(72, int(seal_height * 0.9)),
            ]
            return self._clip_xywh_to_page(estimated_bbox, page_image_size)

        estimated_bbox = [
            left + int(width * 0.62),
            top + int(height * 0.78),
            max(144, int(width * 0.22)),
            max(72, int(height * 0.12)),
        ]
        return self._clip_xywh_to_page(estimated_bbox, page_image_size)

    def _signature_anchor_reference_bbox(
        self,
        anchor_section: dict[str, Any],
        page_sections: list[dict[str, Any]],
        page_blocks: list[dict[str, Any]],
        page_image_size: tuple[int, int] | None,
    ) -> list[int] | None:
        section_type = str(anchor_section.get("type") or "").strip().lower()
        if section_type == "table":
            return self._estimate_signature_bbox_from_table_anchor(
                anchor_section,
                page_sections,
                page_blocks,
                page_image_size,
            )
        return self._estimate_signature_bbox_from_text_anchor(
            anchor_section,
            page_sections,
            page_blocks,
            page_image_size,
        ) or self._bbox_to_xywh(anchor_section.get("bbox"))

    def _anchor_has_signature_evidence(
        self,
        anchor_section: dict[str, Any],
        page_sections: list[dict[str, Any]],
        page_blocks: list[dict[str, Any]],
        page_image_size: tuple[int, int] | None,
    ) -> bool:
        anchor_value = self._extract_signature_anchor_value(anchor_section.get("text") or "")
        compact_value = re.sub(r"\s+", "", anchor_value)
        if compact_value and not self._is_signature_placeholder_text(compact_value):
            cleaned = re.sub(r"[_＿\-\u2014~.·•\s]", "", compact_value)
            if cleaned:
                return True

        anchor_bbox = self._signature_anchor_reference_bbox(
            anchor_section,
            page_sections,
            page_blocks,
            page_image_size,
        )
        estimated_bbox = anchor_bbox

        for section in page_sections:
            if section is anchor_section:
                continue
            section_type = str(section.get("type") or "").strip().lower()
            if section_type not in {"seal", "signature"}:
                continue

            section_bbox = self._bbox_to_xywh(section.get("bbox"))
            if section_type == "signature" and anchor_bbox is not None and section_bbox is not None:
                if self._boxes_are_close(anchor_bbox, section_bbox, max_dx=420, max_dy=180):
                    return True
            if section_type == "seal" and estimated_bbox is not None and section_bbox is not None:
                if self._boxes_are_close(estimated_bbox, section_bbox, max_dx=180, max_dy=180):
                    return True

        for block in page_blocks:
            block_type = str(block.get("type") or "").strip().lower()
            if block_type not in {"seal", "signature"}:
                continue

            block_bbox = self._bbox_to_xywh(block.get("bbox"))
            if block_type == "signature" and anchor_bbox is not None and block_bbox is not None:
                if self._boxes_are_close(anchor_bbox, block_bbox, max_dx=420, max_dy=180):
                    return True
            if block_type == "seal" and estimated_bbox is not None and block_bbox is not None:
                if self._boxes_are_close(estimated_bbox, block_bbox, max_dx=180, max_dy=180):
                    return True

        return False

    def _match_signatures_to_anchors(
        self,
        signatures: list[dict[str, Any]],
        anchors: list[dict[str, Any]],
    ) -> list[tuple[int, int]]:
        if not signatures or not anchors:
            return []

        candidate_pairs: list[tuple[int, int, int, int]] = []
        for signature_index, signature in enumerate(signatures):
            signature_bbox = self._bbox_to_xywh(signature.get("bbox"))
            for anchor_index, anchor in enumerate(anchors):
                anchor_bbox = self._bbox_to_xywh(anchor.get("_signature_anchor_bbox") or anchor.get("bbox"))
                if anchor_bbox is not None and signature_bbox is not None:
                    if not self._boxes_are_close(anchor_bbox, signature_bbox, max_dx=420, max_dy=180):
                        continue
                candidate_pairs.append(
                    (
                        self._bbox_distance(anchor_bbox, signature_bbox),
                        abs(signature_index - anchor_index),
                        anchor_index,
                        signature_index,
                    )
                )

        matches: list[tuple[int, int]] = []
        matched_signature_indexes: set[int] = set()
        matched_anchor_indexes: set[int] = set()
        for _, _, anchor_index, signature_index in sorted(candidate_pairs):
            if signature_index in matched_signature_indexes or anchor_index in matched_anchor_indexes:
                continue
            matched_signature_indexes.add(signature_index)
            matched_anchor_indexes.add(anchor_index)
            matches.append((signature_index, anchor_index))

        remaining_signature_indexes = [
            index for index in range(len(signatures)) if index not in matched_signature_indexes
        ]
        remaining_anchor_indexes = [
            index for index in range(len(anchors)) if index not in matched_anchor_indexes
        ]
        for signature_index, anchor_index in zip(remaining_signature_indexes, remaining_anchor_indexes):
            matches.append((signature_index, anchor_index))

        return sorted(matches, key=lambda item: item[1])

    def _enrich_page_signature_sections(
        self,
        page_sections: list[dict[str, Any]],
        page_blocks: list[dict[str, Any]],
        page_image_size: tuple[int, int] | None,
    ) -> list[dict[str, Any]]:
        if not page_sections and not page_blocks:
            return page_sections

        sections = page_sections
        placeholder = self._signature_placeholder_text()
        existing_signature_keys = {
            self._bbox_signature_key(section.get("bbox"))
            for section in sections
            if str(section.get("type") or "").strip().lower() == "signature"
        }
        inferred_page_no = 0
        for section in sections:
            inferred_page_no = int(section.get("page", 0) or 0)
            if inferred_page_no > 0:
                break

        for block in page_blocks:
            if str(block.get("type") or "").strip().lower() != "signature":
                continue
            bbox = self._normalize_bbox(block.get("bbox"))
            signature_key = self._bbox_signature_key(bbox)
            if signature_key in existing_signature_keys:
                continue

            synthetic_section = {
                "page": int(block.get("page", 0) or inferred_page_no or 0),
                "type": "signature",
                "text": placeholder,
            }
            if bbox is not None:
                synthetic_section["bbox"] = bbox
            sections.append(synthetic_section)
            existing_signature_keys.add(signature_key)

        signature_sections: list[dict[str, Any]] = []
        anchor_sections: list[dict[str, Any]] = []
        for section in sections:
            section_type = str(section.get("type") or "").strip().lower()
            if section_type == "signature":
                section["text"] = placeholder
                section.pop("_merged", None)
                signature_sections.append(section)
                continue

            if section_type in {"heading", "text", "table"}:
                section["text"] = self._normalize_section_text(section.get("text") or "")
                is_anchor = self._is_signature_anchor_text(section.get("text") or "")
                is_anchor = is_anchor or self._is_table_signature_anchor_text(section.get("text") or "")
                if is_anchor:
                    section["_signature_anchor_bbox"] = self._xywh_to_bbox(
                        self._signature_anchor_reference_bbox(
                            section,
                            sections,
                            page_blocks,
                            page_image_size,
                        )
                    )
                    anchor_sections.append(section)

        matched_pairs = self._match_signatures_to_anchors(signature_sections, anchor_sections)
        matched_anchor_indexes = {anchor_index for _, anchor_index in matched_pairs}
        for anchor_index, anchor_section in enumerate(anchor_sections):
            if anchor_index in matched_anchor_indexes:
                continue
            if not self._anchor_has_signature_evidence(
                anchor_section,
                sections,
                page_blocks,
                page_image_size,
            ):
                continue

            inferred_bbox = self._xywh_to_bbox(
                self._signature_anchor_reference_bbox(
                    anchor_section,
                    sections,
                    page_blocks,
                    page_image_size,
                )
            )
            synthetic_section = {
                "page": int(anchor_section.get("page", 0) or inferred_page_no or 0),
                "type": "signature",
                "text": placeholder,
                "_synthetic": True,
            }
            if inferred_bbox is not None:
                synthetic_section["bbox"] = inferred_bbox
            sections.append(synthetic_section)
            signature_sections.append(synthetic_section)

        for signature_index, anchor_index in self._match_signatures_to_anchors(
            signature_sections,
            anchor_sections,
        ):
            signature_section = signature_sections[signature_index]
            anchor_section = anchor_sections[anchor_index]
            anchor_section["text"] = self._build_signature_anchor_text(anchor_section.get("text") or "")
            target_signature_bbox = self._bbox_to_xywh(anchor_section.get("_signature_anchor_bbox"))
            current_signature_bbox = self._bbox_to_xywh(signature_section.get("bbox"))
            if target_signature_bbox is not None:
                page_width = int((page_image_size or (0, 0))[0] or 0)
                replace_bbox = current_signature_bbox is None
                if current_signature_bbox is not None:
                    replace_bbox = (
                        current_signature_bbox[2] > max(target_signature_bbox[2] * 2, 220)
                        or current_signature_bbox[3] > max(target_signature_bbox[3] * 2, 160)
                        or (page_width > 0 and current_signature_bbox[2] >= int(page_width * 0.72))
                    )
                if replace_bbox:
                    signature_section["bbox"] = self._xywh_to_bbox(target_signature_bbox)
            signature_section["_merged"] = True
            if str(anchor_section.get("type") or "").strip().lower() == "table":
                for key in ("raw_text", "html"):
                    if key in anchor_section:
                        anchor_section[key] = self._build_signature_anchor_text(anchor_section.get(key) or "")
                native_table = anchor_section.get("native_table")
                if isinstance(native_table, dict):
                    for native_key in ("block_content", "content", "text", "html"):
                        if native_key in native_table:
                            native_table[native_key] = self._build_signature_anchor_text(
                                native_table.get(native_key) or ""
                            )

        sections.sort(
            key=lambda item: (
                int(item.get("page", 0) or 0),
                self._bbox_anchor(item.get("bbox"))[1],
                self._bbox_anchor(item.get("bbox"))[0],
                int(item.get("_order", 0) or 0),
            ),
        )
        return sections

    def _include_section_in_page_text(self, section: dict[str, Any]) -> bool:
        if section.get("_merged"):
            return False
        return str(section.get("type") or "") in {"heading", "text", "table", "seal", "signature"}

    def _normalize_running_header_signature(self, text: Any) -> str:
        normalized = self._normalize_section_text(text)
        normalized = re.sub(r"\s+", "", normalized)
        return re.sub(r"[^\u4e00-\u9fffA-Za-z0-9]", "", normalized)

    def _is_running_header_candidate(self, section: dict[str, Any], page_extents: dict[int, int]) -> bool:
        if str(section.get("type") or "") != "heading":
            return False

        text = self._normalize_section_text(section.get("text") or "")
        if not text or self.RUNNING_HEADER_HEADING_RE.match(text):
            return False

        signature = self._normalize_running_header_signature(text)
        if not signature or len(signature) > 30:
            return False

        bbox = self._bbox_to_xywh(section.get("bbox"))
        if bbox is not None:
            page_no = int(section.get("page", 0) or 0)
            page_extent = page_extents.get(page_no)
            if page_extent:
                top_ratio = bbox[1] / max(page_extent, 1)
                if top_ratio <= 0.18:
                    return True

        return "招标文件" in text or "投标文件" in text

    def _strip_running_headers(self, sections: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not sections:
            return []

        page_extents: dict[int, int] = {}
        for section in sections:
            page_no = int(section.get("page", 0) or 0)
            bbox = self._bbox_to_xywh(section.get("bbox"))
            if page_no <= 0 or bbox is None:
                continue
            page_extents[page_no] = max(page_extents.get(page_no, 0), bbox[1] + bbox[3])

        candidate_pages: dict[str, set[int]] = defaultdict(set)
        for section in sections:
            if not self._is_running_header_candidate(section, page_extents):
                continue
            signature = self._normalize_running_header_signature(section.get("text") or "")
            page_no = int(section.get("page", 0) or 0)
            if signature and page_no > 0:
                candidate_pages[signature].add(page_no)

        repeated_signatures = {
            signature
            for signature, pages in candidate_pages.items()
            if len(pages) >= 3
        }
        if not repeated_signatures:
            return sections

        filtered: list[dict[str, Any]] = []
        for section in sections:
            signature = self._normalize_running_header_signature(section.get("text") or "")
            if signature in repeated_signatures and self._is_running_header_candidate(section, page_extents):
                continue
            filtered.append(section)
        return filtered

    def _rebuild_pages_from_sections(self, sections: list[dict[str, Any]], page_numbers: list[int]) -> list[dict[str, Any]]:
        by_page: dict[int, list[dict[str, Any]]] = defaultdict(list)
        for section in sections:
            page_no = int(section.get("page", 0) or 0)
            if page_no > 0:
                by_page[page_no].append(section)

        pages: list[dict[str, Any]] = []
        for page_no in page_numbers:
            page_sections = by_page.get(page_no, [])
            page_text = self._merge_text_parts(
                [
                    str(section.get("text") or "")
                    for section in page_sections
                    if self._include_section_in_page_text(section)
                ],
                join_char="\n",
            )
            pages.append({"page": page_no, "text": page_text})
        return pages

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

    def _extract_page_signatures(
        self,
        page_blocks: list[dict[str, Any]],
        page_sections: list[dict[str, Any]],
        page_no: int,
    ) -> dict[str, Any]:
        signature_info = {"count": 0, "texts": [], "locations": []}
        seen_bbox_keys: set[tuple[int, int, int, int] | None] = set()

        for section in page_sections:
            if str(section.get("type") or "").strip().lower() != "signature":
                continue
            signature_info["count"] += 1
            bbox_key = self._bbox_signature_key(section.get("bbox"))
            seen_bbox_keys.add(bbox_key)
            text = self._normalize_section_text(section.get("text") or "")
            bbox = self._bbox_to_xywh(section.get("bbox"))
            if text:
                signature_info["texts"].append(text)
            if bbox is not None:
                signature_info["locations"].append({"page": page_no, "box": bbox})

        for block in page_blocks:
            if str(block.get("type") or "").strip().lower() != "signature":
                continue

            bbox_key = self._bbox_signature_key(block.get("bbox"))
            if bbox_key in seen_bbox_keys:
                continue

            signature_info["count"] += 1
            text = self._normalize_section_text(block.get("text") or "")
            bbox = self._bbox_to_xywh(block.get("bbox"))
            if text:
                signature_info["texts"].append(text)
            if bbox is not None:
                signature_info["locations"].append({"page": page_no, "box": bbox})

        signature_info["texts"] = self._dedupe_text_parts(signature_info["texts"])
        return signature_info

    def _extract_page_text(
        self,
        page_sections: list[dict[str, Any]],
        page_payload: dict[str, Any],
    ) -> str:
        section_text = self._merge_text_parts(
            [
                str(section.get("text") or "")
                for section in page_sections
                if self._include_section_in_page_text(section)
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
            payload["native_tables"] = []
            return payload

        payload["native_tables"] = self._collect_native_tables(layout_sections)
        payload["logical_tables"] = build_logical_tables(layout_sections)
        return payload

    def _resolve_postprocess_workers(self, total_pages: int) -> int:
        if total_pages <= 1:
            return 1

        configured = int(getattr(settings, "OCR_POSTPROCESS_MAX_WORKERS", 0) or 0)
        if configured <= 0:
            configured = min(4, os.cpu_count() or 1)
        return max(1, min(configured, total_pages))

    def _postprocess_page_payload(
        self,
        page_payload: dict[str, Any],
        fallback_page_no: int,
        pdf_page_sizes: dict[int, tuple[float, float]] | None,
    ) -> dict[str, Any]:
        page_no = self._page_number_from_payload(page_payload, fallback_page_no)
        coordinate_context = self._page_coordinate_context(page_payload, page_no, pdf_page_sizes)
        page_blocks = self._extract_layout_blocks(page_payload, page_no)
        page_sections = self._simplify_layout_sections(page_blocks)
        page_sections = self._enrich_page_signature_sections(
            page_sections,
            page_blocks,
            coordinate_context.get("ocr_image_size"),
        )
        page_text = self._extract_page_text(page_sections, page_payload)
        page_seals = self._project_detection_info_for_output(
            self._extract_page_seals(page_payload, page_no),
            coordinate_context,
        )
        page_signatures = self._project_detection_info_for_output(
            self._extract_page_signatures(page_blocks, page_sections, page_no),
            coordinate_context,
        )
        page_sections_output = [
            self._project_section_for_output(section, coordinate_context)
            for section in page_sections
        ]

        return {
            "fallback_page_no": fallback_page_no,
            "page_no": page_no,
            "page_text": page_text,
            "page_sections": page_sections_output,
            "page_seals": page_seals,
            "page_signatures": page_signatures,
            "coordinate_context": coordinate_context,
        }

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
        pipeline_input_path, staged_path = self._stage_input_for_pipeline(file_path)
        pdf_page_sizes = self._load_pdf_page_sizes(file_path, file_type)

        try:
            results = self._run_pipeline(
                pipeline_input_path,
                progress_monitor=progress_monitor,
                total_pages=total_pages,
            )
            if not results:
                raise RuntimeError("PaddleOCR-VL-1.5 returned no results.")

            total_result_pages = max(total_pages, len(results))
            pages: list[dict[str, Any]] = []
            layout_sections: list[dict[str, Any]] = []
            all_seal_count = 0
            all_seal_texts: list[str] = []
            all_seal_locations: list[dict[str, Any]] = []
            all_signature_count = 0
            all_signature_texts: list[str] = []
            all_signature_locations: list[dict[str, Any]] = []
            progress_monitor.update(
                stage="postprocess",
                current=0,
                total=max(total_result_pages, 1),
                detail="normalizing OCR results",
                emit=True,
            )

            page_payloads: list[tuple[int, dict[str, Any]]] = []
            for fallback_page_no, result in enumerate(results, start=1):
                page_payload = self._to_builtin(result)
                if isinstance(page_payload, dict):
                    page_payloads.append((fallback_page_no, page_payload))

            processed_pages: list[dict[str, Any]] = []
            worker_count = self._resolve_postprocess_workers(len(page_payloads))
            if worker_count > 1:
                with ThreadPoolExecutor(
                    max_workers=worker_count,
                    thread_name_prefix="ocr-postprocess",
                ) as executor:
                    futures = {
                        executor.submit(
                            self._postprocess_page_payload,
                            page_payload,
                            fallback_page_no,
                            pdf_page_sizes,
                        ): fallback_page_no
                        for fallback_page_no, page_payload in page_payloads
                    }
                    for completed_count, future in enumerate(as_completed(futures), start=1):
                        page_result = future.result()
                        processed_pages.append(page_result)
                        progress_monitor.update(
                            stage="postprocess",
                            current=completed_count,
                            total=max(total_result_pages, completed_count, 1),
                            detail=f"parsed page {page_result['page_no']}",
                            emit=True,
                    )
            else:
                for completed_count, (fallback_page_no, page_payload) in enumerate(page_payloads, start=1):
                    page_result = self._postprocess_page_payload(
                        page_payload,
                        fallback_page_no,
                        pdf_page_sizes,
                    )
                    processed_pages.append(page_result)
                    progress_monitor.update(
                        stage="postprocess",
                        current=completed_count,
                        total=max(total_result_pages, completed_count, 1),
                        detail=f"parsed page {page_result['page_no']}",
                        emit=True,
                    )

            processed_pages.sort(
                key=lambda item: (
                    int(item.get("page_no", 0) or 0),
                    int(item.get("fallback_page_no", 0) or 0),
                )
            )
            for item in processed_pages:
                pages.append({"page": item["page_no"], "text": item["page_text"]})
                layout_sections.extend(item["page_sections"])
                all_seal_count += int(item["page_seals"].get("count", 0) or 0)
                all_seal_texts.extend(item["page_seals"]["texts"])
                all_seal_locations.extend(item["page_seals"]["locations"])
                all_signature_count += int(item["page_signatures"].get("count", 0) or 0)
                all_signature_texts.extend(item["page_signatures"]["texts"])
                all_signature_locations.extend(item["page_signatures"]["locations"])

            layout_sections = self._strip_running_headers(layout_sections)
            page_numbers = [int(page.get("page", 0) or 0) for page in pages if int(page.get("page", 0) or 0) > 0]
            pages = self._rebuild_pages_from_sections(layout_sections, page_numbers)

            full_text = self._merge_text_parts(
                [str(page.get("text") or "") for page in pages],
                join_char="\n",
            )

            payload = {
                "text": full_text,
                "pages": pages,
                "seals": {
                    "count": all_seal_count,
                    "texts": self._dedupe_text_parts(all_seal_texts),
                    "locations": all_seal_locations,
                },
                "signatures": {
                    "count": all_signature_count,
                    "texts": self._dedupe_text_parts(all_signature_texts),
                    "locations": all_signature_locations,
                },
                "layout_sections": layout_sections,
                "native_tables": [],
                "logical_tables": [],
                "bbox_coordinate_space": "pdf" if pdf_page_sizes else "ocr_image",
                "bbox_source_coordinate_space": "ocr_image",
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
        finally:
            if staged_path is not None:
                try:
                    staged_path.unlink(missing_ok=True)
                except Exception:
                    pass
