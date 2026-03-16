import os
import inspect
import sys
from collections.abc import Iterable, Mapping
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, List, Optional

from app.config.ocr import OCRConfig


class OCRService:
    def __init__(self):
        # Compatible with PaddleOCR 3.x: initialize OCR and PP-StructureV3 separately.
        self.ocr = None
        self.structure = None
        self.structure_seal_enabled = False
        self.available = False
        self.active_device = OCRConfig.DEVICE
        self.init_errors: List[str] = []
        self._prepare_runtime_storage()
        self._sanitize_import_path()
        if OCRConfig.DISABLE_MODEL_SOURCE_CHECK:
            os.environ.setdefault("PADDLE_PDX_DISABLE_MODEL_SOURCE_CHECK", "True")
        os.environ.setdefault("PADDLE_PDX_CACHE_HOME", str(OCRConfig.PDX_CACHE_HOME))

        try:
            from paddleocr import PaddleOCR, PPStructureV3
        except ImportError as exc:
            self._record_error("ImportError", exc)
            return

        for device in self._build_device_candidates():
            self.active_device = device
            self._apply_runtime_flags(device)

            candidate_ocr = None
            candidate_structure = None
            candidate_structure_seal_enabled = False

            try:
                candidate_ocr = PaddleOCR(
                    lang=OCRConfig.LANG,
                    ocr_version=OCRConfig.OCR_VERSION,
                    use_doc_orientation_classify=OCRConfig.USE_DOC_ORIENTATION,
                    use_doc_unwarping=OCRConfig.USE_DOC_UNWARPING,
                    use_textline_orientation=OCRConfig.USE_TEXTLINE_ORIENTATION,
                    device=device,
                    enable_hpi=OCRConfig.ENABLE_HPI,
                )
                print(f"PaddleOCR 3.x initialized successfully on {device}")
            except Exception as exc:
                self._record_error(f"Error initializing PaddleOCR on {device}", exc)

            if OCRConfig.ENABLE_STRUCTURE:
                try:
                    structure_kwargs = {
                        "lang": OCRConfig.LANG,
                        "ocr_version": OCRConfig.OCR_VERSION,
                        "use_doc_orientation_classify": OCRConfig.USE_DOC_ORIENTATION,
                        "use_doc_unwarping": OCRConfig.USE_DOC_UNWARPING,
                        "use_textline_orientation": OCRConfig.USE_TEXTLINE_ORIENTATION,
                        "use_seal_recognition": False,
                        "use_table_recognition": OCRConfig.STRUCTURE_USE_TABLE,
                        "use_formula_recognition": OCRConfig.STRUCTURE_USE_FORMULA,
                        "device": device,
                        "enable_hpi": OCRConfig.ENABLE_HPI,
                    }
                    if self._supports_kwarg(PPStructureV3.__init__, "format_block_content"):
                        structure_kwargs["format_block_content"] = False

                    if OCRConfig.ENABLE_SEAL_RECOGNITION:
                        try:
                            candidate_structure = PPStructureV3(
                                **{**structure_kwargs, "use_seal_recognition": True}
                            )
                            candidate_structure_seal_enabled = True
                            print(
                                f"PP-StructureV3 initialized successfully on {device} with seal recognition"
                            )
                        except Exception as exc:
                            self._record_error(
                                f"Error initializing PP-StructureV3 with seal recognition on {device}",
                                exc,
                            )

                    if candidate_structure is None:
                        candidate_structure = PPStructureV3(**structure_kwargs)
                        print(f"PP-StructureV3 initialized successfully on {device}")
                except Exception as exc:
                    self._record_error(
                        f"Error initializing PP-StructureV3 on {device}", exc
                    )

            if candidate_ocr is not None or candidate_structure is not None:
                self.ocr = candidate_ocr
                self.structure = candidate_structure
                self.structure_seal_enabled = candidate_structure_seal_enabled
                self.available = True
                if device != OCRConfig.DEVICE:
                    print(
                        f"OCR device fallback applied: requested={OCRConfig.DEVICE}, active={device}"
                    )
                break

        self.available = self.ocr is not None or self.structure is not None

    def _record_error(self, prefix: str, exc: Exception) -> None:
        message = f"{prefix}: {exc}"
        self.init_errors.append(message)
        print(message)

    def _sanitize_import_path(self) -> None:
        # Keep only current interpreter's site-packages so the runtime follows
        # the selected IDE interpreter (e.g., PythonProject) consistently.
        runtime_prefix = Path(sys.prefix).resolve()
        sanitized: List[str] = []

        for raw_path in sys.path:
            if not raw_path:
                sanitized.append(raw_path)
                continue

            try:
                resolved = Path(raw_path).resolve()
            except Exception:
                sanitized.append(raw_path)
                continue

            path_text = str(resolved).lower()
            is_site_packages = "site-packages" in path_text
            belongs_to_runtime = (
                resolved == runtime_prefix or runtime_prefix in resolved.parents
            )
            if is_site_packages:
                if not belongs_to_runtime:
                    continue

            sanitized.append(raw_path)

        sys.path[:] = sanitized

    def _build_device_candidates(self) -> List[str]:
        primary = OCRConfig.DEVICE.strip() if OCRConfig.DEVICE else "cpu"
        candidates = [primary]

        if self._is_gpu_device(primary):
            try:
                import paddle

                if not paddle.is_compiled_with_cuda():
                    message = (
                        f"GPU device requested ({primary}) but PaddlePaddle has no CUDA support; fallback to CPU."
                    )
                    self.init_errors.append(message)
                    print(message)
                    return ["cpu"]
            except Exception as exc:
                self._record_error("Failed to check CUDA support; fallback to CPU", exc)
                return ["cpu"]

            if OCRConfig.FALLBACK_TO_CPU:
                candidates.append("cpu")

        return candidates

    @staticmethod
    def _is_gpu_device(device: str) -> bool:
        return device.strip().lower().startswith("gpu")

    @staticmethod
    def _supports_kwarg(callable_obj: Any, kwarg_name: str) -> bool:
        try:
            signature = inspect.signature(callable_obj)
        except Exception:
            return False
        return kwarg_name in signature.parameters

    def _apply_runtime_flags(self, device: str) -> None:
        if self._is_gpu_device(device):
            os.environ.pop("FLAGS_use_mkldnn", None)
            return

        if OCRConfig.DISABLE_MKLDNN_ON_CPU:
            # Workaround for oneDNN/PIR runtime incompatibility on some CPU environments.
            os.environ.setdefault("FLAGS_use_mkldnn", "0")

    def _prepare_runtime_storage(self) -> None:
        OCRConfig.STORAGE_ROOT.mkdir(parents=True, exist_ok=True)
        OCRConfig.PDX_CACHE_HOME.mkdir(parents=True, exist_ok=True)
        OCRConfig.RUNTIME_TEMP_DIR.mkdir(parents=True, exist_ok=True)
        os.environ["PADDLE_PDX_CACHE_HOME"] = str(OCRConfig.PDX_CACHE_HOME)
        os.environ["TMP"] = str(OCRConfig.RUNTIME_TEMP_DIR)
        os.environ["TEMP"] = str(OCRConfig.RUNTIME_TEMP_DIR)
        os.environ["TMPDIR"] = str(OCRConfig.RUNTIME_TEMP_DIR)

    def recognize_text(self, image_path: str) -> str:
        """Recognize text from a single image."""
        if self.ocr is None:
            return self._not_available_message()

        try:
            result = self.ocr.predict(image_path)
            return self._collect_text(result)
        except Exception as exc:
            return f"Error during text recognition: {exc}"

    def recognize_image(self, image_path: str) -> str:
        """Recognize text from a document image, preferring PP-StructureV3."""
        result = self.recognize_image_result(image_path)
        return result["content"]

    def recognize_pdf(self, pdf_path: str) -> str:
        """Recognize text from a PDF, preferring PP-StructureV3 when available."""
        result = self.recognize_pdf_result(pdf_path)
        return result["content"]

    def recognize_image_result(self, image_path: str) -> dict[str, Any]:
        """Recognize an image and return text plus runtime metadata."""
        if not self.available:
            return self._build_failure_result(self._not_available_message())

        if self.structure is not None:
            try:
                structured_result = self._predict_with_structure(image_path)
                structured_text, seal_texts = self._collect_structure_text_and_seals(
                    structured_result
                )
                if structured_text.strip() or seal_texts:
                    return self._build_success_result(
                        content=structured_text,
                        ocr_engine="PP-StructureV3",
                        seal_texts=seal_texts,
                    )
            except Exception as exc:
                self._record_error(
                    "Error during structured image recognition, fallback to OCR", exc
                )

        if self.ocr is not None:
            try:
                return self._build_success_result(
                    content=self._collect_text(self.ocr.predict(image_path)),
                    ocr_engine="PaddleOCR 3.x",
                )
            except Exception as exc:
                self._record_error("Error during image OCR recognition", exc)

        return self._build_failure_result(self._not_available_message())

    def recognize_pdf_result(self, pdf_path: str) -> dict[str, Any]:
        """Recognize a PDF and return text plus runtime metadata."""
        if not self.available:
            return self._build_failure_result(self._not_available_message())

        try:
            if self.structure is not None:
                try:
                    structured_result = self._predict_with_structure(pdf_path)
                    structured_text, seal_texts = self._collect_structure_text_and_seals(
                        structured_result
                    )
                    if structured_text.strip() or seal_texts:
                        return self._build_success_result(
                            content=structured_text,
                            ocr_engine="PP-StructureV3",
                            seal_texts=seal_texts,
                        )
                except Exception as exc:
                    self._record_error(
                        "Error during structured PDF recognition, fallback to OCR", exc
                    )

            if self.ocr is not None:
                return self._build_success_result(
                    content=self._recognize_pdf_with_ocr(pdf_path),
                    ocr_engine="PaddleOCR 3.x",
                )

            return self._build_failure_result(self._not_available_message())
        except Exception as exc:
            return self._build_failure_result(f"Error during PDF recognition: {exc}")

    def recognize_bytes(self, image_bytes: bytes) -> str:
        """Recognize text from image bytes."""
        if not self.available:
            return self._not_available_message()

        temp_image_path: Optional[str] = None
        try:
            import cv2
            import numpy as np

            nparr = np.frombuffer(image_bytes, np.uint8)
            image = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
            if image is None:
                return "Error during byte stream recognition: invalid image bytes."

            with NamedTemporaryFile(delete=False, suffix=".png") as temp_file:
                temp_image_path = temp_file.name
            cv2.imwrite(temp_image_path, image)
            return self.recognize_image(temp_image_path)
        except Exception as exc:
            return f"Error during byte stream recognition: {exc}"
        finally:
            if temp_image_path and os.path.exists(temp_image_path):
                os.unlink(temp_image_path)

    def is_available(self) -> bool:
        """Check whether OCR service is available."""
        return self.available

    def _not_available_message(self) -> str:
        if self.available:
            return "PaddleOCR is available."
        base = "PaddleOCR is not available."
        if not self.init_errors:
            return f"{base} Please check if PaddleOCR is installed correctly."
        reason = " | ".join(self.init_errors)
        return f"{base} {reason}"

    def _build_success_result(
        self,
        *,
        content: str,
        ocr_engine: str,
        seal_texts: Optional[List[str]] = None,
    ) -> dict[str, Any]:
        normalized_seal_texts = self._dedupe_texts(seal_texts or [])
        return {
            "success": True,
            "content": content.strip(),
            "ocr_engine": ocr_engine,
            "active_device": self.active_device,
            "seal_enabled": self.structure_seal_enabled,
            "seal_removed": OCRConfig.EXCLUDE_SEAL_TEXT and bool(normalized_seal_texts),
            "seal_detected": bool(normalized_seal_texts),
            "seal_count": len(normalized_seal_texts),
            "seal_texts": normalized_seal_texts,
        }

    def _build_failure_result(self, message: str) -> dict[str, Any]:
        return {
            "success": False,
            "content": message,
            "ocr_engine": "",
            "active_device": self.active_device,
            "seal_enabled": self.structure_seal_enabled,
            "seal_removed": False,
            "seal_detected": False,
            "seal_count": 0,
            "seal_texts": [],
        }

    def _recognize_pdf_with_structure(self, pdf_path: str) -> str:
        if self.structure is None:
            return ""

        result = self._predict_with_structure(pdf_path)
        structured_text, _ = self._collect_structure_text_and_seals(result)
        return structured_text

    def _recognize_pdf_with_ocr(self, pdf_path: str) -> str:
        import fitz

        page_texts: List[str] = []

        with fitz.open(pdf_path) as doc:
            for page_num in range(len(doc)):
                page = doc.load_page(page_num)
                pix = page.get_pixmap()

                temp_image_path: Optional[str] = None
                try:
                    with NamedTemporaryFile(delete=False, suffix=".png") as temp_file:
                        temp_image_path = temp_file.name
                    pix.save(temp_image_path)
                    page_text = self.recognize_text(temp_image_path).strip()
                    if page_text:
                        page_texts.append(page_text)
                finally:
                    if temp_image_path and os.path.exists(temp_image_path):
                        os.unlink(temp_image_path)

        return "\n".join(page_texts).strip()

    def _predict_with_structure(self, input_path: str) -> Any:
        if self.structure is None:
            return []

        predict_kwargs = {
            "use_seal_recognition": self.structure_seal_enabled,
            "use_table_recognition": OCRConfig.STRUCTURE_USE_TABLE,
            "use_formula_recognition": OCRConfig.STRUCTURE_USE_FORMULA,
        }
        if self._supports_kwarg(self.structure.predict, "format_block_content"):
            predict_kwargs["format_block_content"] = False
        return self.structure.predict(input_path, **predict_kwargs)

    def _collect_structure_text_and_seals(
        self, results: Any
    ) -> tuple[str, list[str]]:
        lines: List[str] = []
        seal_texts: List[str] = []
        self._collect_structure_lines(results, lines, seal_texts)
        return (
            "\n".join(line for line in lines if line).strip(),
            self._dedupe_texts(seal_texts),
        )

    def _collect_structure_lines(
        self, node: Any, lines: List[str], seal_texts: List[str]
    ) -> None:
        if node is None:
            return

        if isinstance(node, str):
            text = node.strip()
            if text:
                lines.append(text)
            return

        if isinstance(node, bytes):
            text = node.decode("utf-8", errors="ignore").strip()
            if text:
                lines.append(text)
            return

        block_label = self._stringify_text(
            self._get_field(node, "block_label") or self._get_field(node, "label")
        ).lower()
        if block_label == "seal":
            self._collect_seal_texts(node, seal_texts)
            return

        parsing_res_list = self._get_field(node, "parsing_res_list")
        if isinstance(parsing_res_list, list):
            for block in parsing_res_list:
                self._collect_structure_lines(block, lines, seal_texts)
            return

        seal_res_list = self._get_field(node, "seal_res_list")
        if isinstance(seal_res_list, list):
            for seal_res in seal_res_list:
                self._collect_seal_texts(seal_res, seal_texts)

        block_content = self._extract_block_content(node)
        if block_content:
            lines.append(block_content)
            return

        rec_texts = self._get_field(node, "rec_texts")
        if isinstance(rec_texts, list):
            found_rec_text = False
            for item in rec_texts:
                text = self._stringify_text(item)
                if text:
                    lines.append(text)
                    found_rec_text = True
            if found_rec_text:
                return

        overall_ocr_res = self._get_field(node, "overall_ocr_res")
        if overall_ocr_res is not None:
            before_len = len(lines)
            self._collect_structure_lines(overall_ocr_res, lines, seal_texts)
            if len(lines) > before_len:
                return

        markdown_texts = self._get_field(node, "markdown_texts")
        if isinstance(markdown_texts, str) and markdown_texts.strip():
            lines.append(markdown_texts.strip())
            return

        rec_text = self._stringify_text(self._get_field(node, "rec_text"))
        if rec_text:
            lines.append(rec_text)
            return

        plain_text = self._stringify_text(self._get_field(node, "text"))
        if plain_text:
            lines.append(plain_text)
            return

        if isinstance(node, Mapping):
            for item in node.values():
                self._collect_structure_lines(item, lines, seal_texts)
            return

        if isinstance(node, Iterable):
            for item in node:
                self._collect_structure_lines(item, lines, seal_texts)

    def _collect_seal_texts(self, node: Any, seal_texts: List[str]) -> None:
        if node is None:
            return

        block_content = self._extract_block_content(node)
        if block_content:
            seal_texts.append(block_content)

        rec_texts = self._get_field(node, "rec_texts")
        if isinstance(rec_texts, list):
            for item in rec_texts:
                text = self._stringify_text(item)
                if text:
                    seal_texts.append(text)

        rec_text = self._stringify_text(self._get_field(node, "rec_text"))
        if rec_text:
            seal_texts.append(rec_text)

        overall_ocr_res = self._get_field(node, "overall_ocr_res")
        if overall_ocr_res is not None:
            self._collect_seal_texts(overall_ocr_res, seal_texts)

        if isinstance(node, Mapping):
            for item in node.values():
                self._collect_seal_texts(item, seal_texts)
            return

        if isinstance(node, Iterable) and not isinstance(node, (str, bytes)):
            for item in node:
                self._collect_seal_texts(item, seal_texts)

    @staticmethod
    def _extract_block_content(node: Any) -> str:
        if node is None:
            return ""
        for field_name in ("block_content", "content", "markdown_texts"):
            value = OCRService._get_field(node, field_name)
            if isinstance(value, str) and value.strip():
                return value.strip()
            text = OCRService._stringify_text(value)
            if text:
                return text
        return ""

    @staticmethod
    def _dedupe_texts(texts: List[str]) -> List[str]:
        seen = set()
        ordered: List[str] = []
        for item in texts:
            normalized = item.strip()
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            ordered.append(normalized)
        return ordered

    def _collect_text(self, results: Any) -> str:
        lines: List[str] = []
        self._collect_lines(results, lines)
        return "\n".join(line for line in lines if line).strip()

    def _collect_lines(self, node: Any, lines: List[str]) -> None:
        if node is None:
            return

        if isinstance(node, str):
            text = node.strip()
            if text:
                lines.append(text)
            return

        if isinstance(node, bytes):
            text = node.decode("utf-8", errors="ignore").strip()
            if text:
                lines.append(text)
            return

        legacy_text = self._extract_legacy_line_text(node)
        if legacy_text:
            lines.append(legacy_text)
            return

        rec_texts = self._get_field(node, "rec_texts")
        if isinstance(rec_texts, list):
            found_rec_text = False
            for item in rec_texts:
                text = self._stringify_text(item)
                if text:
                    lines.append(text)
                    found_rec_text = True
            if found_rec_text:
                return

        parsing_res_list = self._get_field(node, "parsing_res_list")
        if isinstance(parsing_res_list, list):
            found_parsing_text = False
            for block in parsing_res_list:
                block_text = self._get_field(block, "block_content")
                if not block_text:
                    block_text = self._get_field(block, "content")
                text = self._stringify_text(block_text)
                if text:
                    lines.append(text)
                    found_parsing_text = True
            if found_parsing_text:
                return

        overall_ocr_res = self._get_field(node, "overall_ocr_res")
        if overall_ocr_res is not None:
            before_len = len(lines)
            self._collect_lines(overall_ocr_res, lines)
            if len(lines) > before_len:
                return

        markdown_texts = self._get_field(node, "markdown_texts")
        if isinstance(markdown_texts, str) and markdown_texts.strip():
            lines.append(markdown_texts.strip())
            return

        rec_text = self._get_field(node, "rec_text")
        text = self._stringify_text(rec_text)
        if text:
            lines.append(text)
            return

        plain_text = self._get_field(node, "text")
        text = self._stringify_text(plain_text)
        if text:
            lines.append(text)
            return

        if isinstance(node, Mapping):
            for item in node.values():
                self._collect_lines(item, lines)
            return

        if isinstance(node, Iterable):
            for item in node:
                self._collect_lines(item, lines)
            return

    @staticmethod
    def _extract_legacy_line_text(node: Any) -> str:
        if not isinstance(node, (list, tuple)) or len(node) != 2:
            return ""

        candidate = node[1]
        if not isinstance(candidate, (list, tuple)) or not candidate:
            return ""

        text = candidate[0]
        return text.strip() if isinstance(text, str) else ""

    @staticmethod
    def _stringify_text(value: Any) -> str:
        if isinstance(value, str):
            return value.strip()
        if isinstance(value, (list, tuple)) and value:
            first_item = value[0]
            return first_item.strip() if isinstance(first_item, str) else ""
        return ""

    @staticmethod
    def _get_field(node: Any, field_name: str) -> Any:
        if isinstance(node, Mapping):
            return node.get(field_name)

        try:
            if field_name in node:
                return node[field_name]
        except Exception:
            pass

        return getattr(node, field_name, None)
