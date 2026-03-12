import os
import sys
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, List, Optional

from app.config.ocr import OCRConfig


class OCRService:
    def __init__(self):
        # Compatible with PaddleOCR 3.x: initialize OCR and PP-StructureV3 separately.
        self.ocr = None
        self.structure = None
        self.available = False
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

        try:
            self.ocr = PaddleOCR(
                lang=OCRConfig.LANG,
                ocr_version=OCRConfig.OCR_VERSION,
                use_doc_orientation_classify=False,
                use_doc_unwarping=False,
                use_textline_orientation=False,
                device=OCRConfig.DEVICE,
                enable_hpi=OCRConfig.ENABLE_HPI,
            )
            print("PaddleOCR 3.x initialized successfully")
        except Exception as exc:
            self._record_error("Error initializing PaddleOCR", exc)

        try:
            self.structure = PPStructureV3(
                lang=OCRConfig.LANG,
                ocr_version=OCRConfig.OCR_VERSION,
                use_doc_orientation_classify=OCRConfig.USE_DOC_ORIENTATION,
                use_doc_unwarping=OCRConfig.USE_DOC_UNWARPING,
                use_textline_orientation=OCRConfig.USE_TEXTLINE_ORIENTATION,
                use_table_recognition=OCRConfig.STRUCTURE_USE_TABLE,
                use_formula_recognition=OCRConfig.STRUCTURE_USE_FORMULA,
                format_block_content=False,
                device=OCRConfig.DEVICE,
                enable_hpi=OCRConfig.ENABLE_HPI,
            )
            print("PP-StructureV3 initialized successfully")
        except Exception as exc:
            self._record_error("Error initializing PP-StructureV3", exc)

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

    def _prepare_runtime_storage(self) -> None:
        OCRConfig.STORAGE_ROOT.mkdir(parents=True, exist_ok=True)
        OCRConfig.PDX_CACHE_HOME.mkdir(parents=True, exist_ok=True)
        OCRConfig.RUNTIME_TEMP_DIR.mkdir(parents=True, exist_ok=True)
        os.environ["PADDLE_PDX_CACHE_HOME"] = str(OCRConfig.PDX_CACHE_HOME)
        os.environ["TMP"] = str(OCRConfig.RUNTIME_TEMP_DIR)
        os.environ["TEMP"] = str(OCRConfig.RUNTIME_TEMP_DIR)

    def recognize_text(self, image_path: str) -> str:
        """Recognize text from a single image."""
        if self.ocr is None:
            return self._not_available_message()

        try:
            result = self.ocr.predict(image_path)
            return self._collect_text(result)
        except Exception as exc:
            return f"Error during text recognition: {exc}"

    def recognize_pdf(self, pdf_path: str) -> str:
        """Recognize text from a PDF, preferring PP-StructureV3 when available."""
        if not self.available:
            return self._not_available_message()

        try:
            if self.structure is not None:
                structured_text = self._recognize_pdf_with_structure(pdf_path)
                if structured_text.strip():
                    return structured_text

            if self.ocr is not None:
                return self._recognize_pdf_with_ocr(pdf_path)

            return self._not_available_message()
        except Exception as exc:
            return f"Error during PDF recognition: {exc}"

    def recognize_bytes(self, image_bytes: bytes) -> str:
        """Recognize text from image bytes."""
        if self.ocr is None:
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
            return self.recognize_text(temp_image_path)
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

    def _recognize_pdf_with_structure(self, pdf_path: str) -> str:
        if self.structure is None:
            return ""

        result = self.structure.predict(
            pdf_path,
            use_table_recognition=OCRConfig.STRUCTURE_USE_TABLE,
            use_formula_recognition=OCRConfig.STRUCTURE_USE_FORMULA,
            format_block_content=False,
        )
        return self._collect_text(result)

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

    def _collect_text(self, results: Any) -> str:
        lines: List[str] = []
        self._collect_lines(results, lines)
        return "\n".join(line for line in lines if line).strip()

    def _collect_lines(self, node: Any, lines: List[str]) -> None:
        if node is None:
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

        if isinstance(node, (list, tuple)):
            for item in node:
                self._collect_lines(item, lines)
            return

        if isinstance(node, dict):
            for item in node.values():
                self._collect_lines(item, lines)

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
        if isinstance(node, dict):
            return node.get(field_name)

        try:
            if field_name in node:
                return node[field_name]
        except Exception:
            pass

        return getattr(node, field_name, None)
