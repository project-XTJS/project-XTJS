import os
import re
from typing import Any

import cv2
import fitz  # PyMuPDF
import numpy as np
from tqdm import tqdm

from app.config.settings import settings


class OCRService:
    def __init__(self):
        self.available = False
        self.ocr = None
        self.structure = None
        self.structure_available = False
        self.active_device = "cpu"
        self.seal_dir = "output_seals"
        if not os.path.exists(self.seal_dir):
            os.makedirs(self.seal_dir)

        self._prepare_runtime_dirs()
        self._prepare_runtime_env()
        self._init_engines()

    def _prepare_runtime_dirs(self) -> None:
        for path in (
            settings.OCR_STORAGE_ROOT,
            settings.PADDLE_PDX_CACHE_HOME,
            settings.OCR_RUNTIME_TEMP_DIR,
        ):
            os.makedirs(path, exist_ok=True)

    def _prepare_runtime_env(self) -> None:
        os.environ.setdefault("PADDLE_PDX_CACHE_HOME", str(settings.PADDLE_PDX_CACHE_HOME))
        os.environ.setdefault(
            "PADDLE_PDX_DISABLE_MODEL_SOURCE_CHECK",
            "1" if settings.PADDLE_PDX_DISABLE_MODEL_SOURCE_CHECK else "0",
        )
        os.environ.setdefault("TMPDIR", str(settings.OCR_RUNTIME_TEMP_DIR))

    def _candidate_devices(self) -> list[str]:
        candidates = [settings.PADDLE_OCR_DEVICE]
        if settings.PADDLE_OCR_DEVICE.startswith("gpu:"):
            candidates.append("gpu")
        if settings.PADDLE_OCR_FALLBACK_TO_CPU:
            candidates.append("cpu")

        unique_candidates: list[str] = []
        for device in candidates:
            if device and device not in unique_candidates:
                unique_candidates.append(device)
        return unique_candidates

    def _build_ocr_kwargs(self, device: str) -> dict[str, Any]:
        return {
            "lang": settings.PADDLE_OCR_LANG,
            "device": device,
            "use_doc_orientation_classify": settings.PADDLE_OCR_USE_DOC_ORIENTATION,
            "use_doc_unwarping": settings.PADDLE_OCR_USE_DOC_UNWARPING,
            "use_textline_orientation": settings.PADDLE_OCR_USE_TEXTLINE_ORIENTATION,
        }

    def _build_structure_kwargs(self, device: str) -> dict[str, Any]:
        return {
            "device": device,
            "use_doc_orientation_classify": settings.PADDLE_OCR_USE_DOC_ORIENTATION,
            "use_doc_unwarping": settings.PADDLE_OCR_USE_DOC_UNWARPING,
            "use_textline_orientation": settings.PADDLE_OCR_USE_TEXTLINE_ORIENTATION,
        }

    def _iter_ocr_kwargs_candidates(self, device: str) -> list[dict[str, Any]]:
        base_kwargs = self._build_ocr_kwargs(device)
        candidates = [
            {
                **base_kwargs,
                "ocr_version": settings.PADDLE_OCR_VERSION,
                "enable_hpi": settings.PADDLE_OCR_ENABLE_HPI,
            },
            {
                **base_kwargs,
                "ocr_version": settings.PADDLE_OCR_VERSION,
            },
            base_kwargs,
        ]

        deduped: list[dict[str, Any]] = []
        seen = set()
        for kwargs in candidates:
            signature = tuple(sorted(kwargs.items()))
            if signature not in seen:
                deduped.append(kwargs)
                seen.add(signature)
        return deduped

    def _iter_structure_kwargs_candidates(self, device: str) -> list[dict[str, Any]]:
        base_kwargs = self._build_structure_kwargs(device)
        candidates = [
            {
                **base_kwargs,
                "use_table_recognition": settings.PADDLE_STRUCTURE_USE_TABLE,
                "use_formula_recognition": settings.PADDLE_STRUCTURE_USE_FORMULA,
            },
            base_kwargs,
        ]

        deduped: list[dict[str, Any]] = []
        seen = set()
        for kwargs in candidates:
            signature = tuple(sorted(kwargs.items()))
            if signature not in seen:
                deduped.append(kwargs)
                seen.add(signature)
        return deduped

    def _init_engines(self) -> None:
        try:
            from paddleocr import PaddleOCR
        except Exception as exc:
            print(f"OCRService 加载失败: {exc}")
            return

        last_error: Exception | None = None
        for device in self._candidate_devices():
            for kwargs in self._iter_ocr_kwargs_candidates(device):
                try:
                    self.ocr = PaddleOCR(**kwargs)
                    self.available = True
                    self.active_device = device
                    print(
                        "OCRService: PaddleOCR 加载成功"
                        f" (device={self.active_device}, lang={settings.PADDLE_OCR_LANG}, version={settings.PADDLE_OCR_VERSION})"
                    )
                    print(f"印章截图将保存至: {self.seal_dir}")
                    break
                except Exception as exc:
                    last_error = exc
            if self.available:
                break
            print(f"OCRService: PaddleOCR 初始化失败 (device={device}): {last_error}")

        if not self.available:
            print(f"OCRService 加载失败: {last_error}")
            return

        if settings.PADDLE_OCR_ENABLE_STRUCTURE:
            self._init_structure_engine()

    def _init_structure_engine(self) -> None:
        try:
            from paddleocr import PPStructureV3
        except Exception as exc:
            self.structure = None
            self.structure_available = False
            print(f"OCRService: PPStructureV3 未启用，回退为通用 OCR 流程: {exc}")
            return

        last_error: Exception | None = None
        for kwargs in self._iter_structure_kwargs_candidates(self.active_device):
            try:
                self.structure = PPStructureV3(**kwargs)
                self.structure_available = True
                print(
                    "OCRService: PPStructureV3 加载成功"
                    f" (device={self.active_device})"
                )
                return
            except Exception as exc:
                last_error = exc

        self.structure = None
        self.structure_available = False
        print(f"OCRService: PPStructureV3 未启用，回退为通用 OCR 流程: {last_error}")

    def _empty_result(self) -> dict:
        return {
            "text": "",
            "pages": [],
            "seals": {"count": 0, "texts": [], "locations": []},
            "layout_blocks": [],
            "ocr_applied": False,
            "structure_used": False,
        }

    def extract_all(self, file_path: str, file_type: str = "pdf") -> dict:
        if not self.available:
            return self._empty_result()

        ext = file_type.lower().lstrip(".")
        if ext == "pdf":
            return self._recognize_pdf(file_path)
        return self._recognize_image(file_path)

    def _run_predictor(self, predictor: Any, image: np.ndarray, predictor_name: str) -> list:
        if predictor is None:
            return []
        try:
            return list(predictor.predict(image))
        except Exception as exc:
            print(f"{predictor_name} 推理失败: {exc}")
            return []

    def _extract_text_from_result(self, ocr_res: list, join_char: str = "\n") -> str:
        if not ocr_res:
            return ""

        texts: list[str] = []
        try:
            for item in ocr_res:
                if isinstance(item, dict) and "rec_texts" in item:
                    texts.extend(str(text).strip() for text in item["rec_texts"] if str(text).strip())
                elif hasattr(item, "rec_texts"):
                    texts.extend(str(text).strip() for text in getattr(item, "rec_texts") if str(text).strip())
                elif isinstance(item, list):
                    for line in item:
                        if isinstance(line, list) and len(line) == 2 and isinstance(line[1], tuple):
                            text = str(line[1][0]).strip()
                            if text:
                                texts.append(text)
        except Exception as exc:
            print(f"OCR 结果解析警告: {exc}")

        return join_char.join(texts)

    def _to_builtin(self, value: Any, depth: int = 0) -> Any:
        if depth > 6:
            return None
        if value is None or isinstance(value, (str, int, float, bool)):
            return value
        if isinstance(value, dict):
            return {str(key): self._to_builtin(item, depth + 1) for key, item in value.items()}
        if isinstance(value, (list, tuple, set)):
            return [self._to_builtin(item, depth + 1) for item in value]
        if hasattr(value, "tolist"):
            try:
                return value.tolist()
            except Exception:
                pass
        if hasattr(value, "__dict__"):
            return {
                key: self._to_builtin(item, depth + 1)
                for key, item in vars(value).items()
                if not key.startswith("_")
            }
        return str(value)

    def _merge_text_parts(self, parts: list[str], join_char: str = "\n") -> str:
        merged: list[str] = []
        seen = set()
        for part in parts:
            normalized = re.sub(r"\n{3,}", "\n\n", str(part or "").strip())
            if normalized and normalized not in seen:
                merged.append(normalized)
                seen.add(normalized)
        return join_char.join(merged)

    def _extract_text_value(self, value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, str):
            return value.strip()
        if isinstance(value, dict):
            text_parts: list[str] = []
            for key in ("block_content", "markdown", "text", "html", "content", "caption"):
                candidate = self._extract_text_value(value.get(key))
                if candidate:
                    text_parts.append(candidate)
            if "rec_texts" in value:
                text_parts.append(
                    self._merge_text_parts([str(item).strip() for item in value["rec_texts"] if str(item).strip()])
                )
            if "texts" in value:
                text_parts.append(self._extract_text_value(value["texts"]))
            return self._merge_text_parts(text_parts)
        if isinstance(value, (list, tuple, set)):
            return self._merge_text_parts([self._extract_text_value(item) for item in value])
        builtin_value = self._to_builtin(value)
        if builtin_value is value:
            return ""
        return self._extract_text_value(builtin_value)

    def _normalize_bbox(self, value: Any) -> Any:
        builtin_value = self._to_builtin(value)
        if builtin_value in (None, ""):
            return None
        return builtin_value

    def _extract_layout_blocks(self, structure_result: list, page_no: int) -> list[dict]:
        built_result = self._to_builtin(structure_result)
        if not built_result:
            return []

        blocks: list[dict] = []
        seen = set()
        label_keys = ("block_label", "label", "type", "category")
        bbox_keys = ("coordinate", "bbox", "box", "region_box", "points")
        child_keys = (
            "parsing_res_list",
            "table_res_list",
            "layout_det_res",
            "overall_ocr_res",
            "blocks",
            "items",
            "regions",
            "sub_blocks",
            "children",
            "tables",
            "res",
        )

        def walk(node: Any) -> None:
            if isinstance(node, list):
                for item in node:
                    walk(item)
                return
            if not isinstance(node, dict):
                return

            label = ""
            for key in label_keys:
                candidate = node.get(key)
                if isinstance(candidate, str) and candidate.strip():
                    label = candidate.strip()
                    break

            text = self._extract_text_value(node)
            bbox = None
            for key in bbox_keys:
                if key in node:
                    bbox = self._normalize_bbox(node.get(key))
                    if bbox is not None:
                        break

            is_layout_block = bool(label or bbox is not None or "block_content" in node)
            if is_layout_block and text:
                signature = (label, text, str(bbox))
                if signature not in seen:
                    blocks.append(
                        {
                            "page": page_no,
                            "type": label or "text",
                            "text": text,
                            "bbox": bbox,
                        }
                    )
                    seen.add(signature)

            for key in child_keys:
                if key in node:
                    walk(node.get(key))

            for key, value in node.items():
                if key in child_keys or key in label_keys or key in bbox_keys:
                    continue
                if key in {"block_content", "markdown", "text", "html", "content", "caption", "rec_texts", "texts"}:
                    continue
                if isinstance(value, (dict, list)):
                    walk(value)

        walk(built_result)
        return blocks

    def _extract_structure_text(self, structure_result: list) -> str:
        return self._extract_text_value(self._to_builtin(structure_result))

    def _combine_page_text(self, layout_text: str, ocr_text: str) -> str:
        return self._merge_text_parts([layout_text, ocr_text])

    def _run_structure_layout(self, image: np.ndarray, page_no: int) -> tuple[str, list[dict], bool]:
        if not self.structure_available or self.structure is None:
            return "", [], False

        structure_result = self._run_predictor(self.structure, image, "PPStructureV3")
        if not structure_result:
            return "", [], False

        layout_blocks = self._extract_layout_blocks(structure_result, page_no)
        layout_text = self._merge_text_parts([block["text"] for block in layout_blocks])
        if not layout_text:
            layout_text = self._extract_structure_text(structure_result)

        return layout_text, layout_blocks, bool(layout_text or layout_blocks)

    def _remove_seal_texts(self, text: str, seal_texts: list[str]) -> str:
        if not text or not seal_texts or not settings.PADDLE_OCR_EXCLUDE_SEAL_TEXT:
            return text

        cleaned_text = text
        for seal_text in seal_texts:
            normalized = str(seal_text or "").strip()
            if len(normalized) >= 2:
                cleaned_text = cleaned_text.replace(normalized, "")
        return re.sub(r"\n{3,}", "\n\n", cleaned_text).strip()

    def _detect_seals(self, img_bgr: np.ndarray, page_no: int = 1) -> dict:
        seal_info = {"count": 0, "texts": [], "locations": []}
        if img_bgr is None or not settings.PADDLE_OCR_ENABLE_SEAL_RECOGNITION:
            return seal_info

        hsv = cv2.cvtColor(img_bgr, cv2.COLOR_BGR2HSV)
        lower_red1, upper_red1 = np.array([0, 43, 46]), np.array([10, 255, 255])
        lower_red2, upper_red2 = np.array([156, 43, 46]), np.array([180, 255, 255])
        mask = cv2.add(
            cv2.inRange(hsv, lower_red1, upper_red1),
            cv2.inRange(hsv, lower_red2, upper_red2),
        )

        kernel = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (5, 5))
        mask = cv2.dilate(mask, kernel, iterations=2)
        contours, _ = cv2.findContours(mask, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)

        seal_idx = 0
        for contour in contours:
            area = cv2.contourArea(contour)
            if area <= 1200:
                continue

            x, y, w, h = cv2.boundingRect(contour)
            aspect_ratio = float(w) / h if h else 0.0
            if not 0.5 < aspect_ratio < 2.0:
                continue

            seal_idx += 1
            box = [int(x), int(y), int(w), int(h)]
            seal_crop = img_bgr[y : y + h, x : x + w]
            save_path = os.path.join(self.seal_dir, f"seal_P{page_no}_{seal_idx}.png")
            cv2.imwrite(save_path, seal_crop)

            resized_crop = cv2.resize(seal_crop, None, fx=2.0, fy=2.0, interpolation=cv2.INTER_CUBIC)
            crop_result = self._run_predictor(self.ocr, resized_crop, "Seal OCR")
            raw_text = self._extract_text_from_result(crop_result, join_char="")
            clean_text = re.sub(r"[〇一二三四五六七八九十月年\d\-\.：:（）\(\)]", "", raw_text)
            if len(clean_text) > 2:
                seal_info["texts"].append(clean_text)

            seal_info["count"] += 1
            seal_info["locations"].append(box)

        seal_info["texts"] = list(dict.fromkeys(seal_info["texts"]))
        return seal_info

    def _render_pdf_page(self, page) -> np.ndarray:
        pix = page.get_pixmap(matrix=fitz.Matrix(2, 2))
        img = np.frombuffer(pix.samples, dtype=np.uint8).reshape(pix.h, pix.w, pix.n)
        if pix.n == 4:
            return cv2.cvtColor(img, cv2.COLOR_RGBA2BGR)
        if pix.n == 3:
            return cv2.cvtColor(img, cv2.COLOR_RGB2BGR)
        return cv2.cvtColor(img, cv2.COLOR_GRAY2BGR)

    def _recognize_pdf(self, pdf_path: str) -> dict:
        pages_data: list[dict] = []
        full_text: list[str] = []
        layout_blocks: list[dict] = []
        all_seals = {"count": 0, "texts": [], "locations": []}
        ocr_applied = False
        structure_used = False

        doc = fitz.open(pdf_path)
        try:
            total_pages = len(doc)
            pbar = tqdm(range(total_pages), desc="解析中", unit="页")
            for page_index in pbar:
                page_no = page_index + 1
                image = self._render_pdf_page(doc[page_index])

                ocr_result = self._run_predictor(self.ocr, image, "PaddleOCR")
                ocr_text = self._extract_text_from_result(ocr_result, join_char="\n")
                layout_text, page_layout_blocks, page_structure_used = self._run_structure_layout(image, page_no)
                seal_result = self._detect_seals(image, page_no=page_no)

                page_text = self._combine_page_text(layout_text, ocr_text)
                page_text = self._remove_seal_texts(page_text, seal_result["texts"])

                if seal_result["count"] > 0:
                    all_seals["count"] += seal_result["count"]
                    all_seals["texts"].extend(seal_result["texts"])
                    for box in seal_result["locations"]:
                        all_seals["locations"].append({"page": page_no, "box": box})

                if page_layout_blocks:
                    layout_blocks.extend(page_layout_blocks)

                pages_data.append({"page": page_no, "text": page_text})
                full_text.append(page_text)
                ocr_applied = ocr_applied or bool(ocr_text.strip() or layout_text.strip() or seal_result["count"])
                structure_used = structure_used or page_structure_used
                pbar.set_postfix({"印章数": all_seals["count"]})
        finally:
            doc.close()

        all_seals["texts"] = list(dict.fromkeys(all_seals["texts"]))
        return {
            "text": self._merge_text_parts(full_text),
            "pages": pages_data,
            "seals": all_seals,
            "layout_blocks": layout_blocks,
            "ocr_applied": ocr_applied,
            "structure_used": structure_used,
        }

    def _recognize_image(self, img_path: str) -> dict:
        image = cv2.imread(img_path)
        if image is None:
            return self._empty_result()

        ocr_result = self._run_predictor(self.ocr, image, "PaddleOCR")
        ocr_text = self._extract_text_from_result(ocr_result, join_char="\n")
        layout_text, layout_blocks, structure_used = self._run_structure_layout(image, page_no=1)
        seal_result = self._detect_seals(image, page_no=1)

        page_text = self._combine_page_text(layout_text, ocr_text)
        page_text = self._remove_seal_texts(page_text, seal_result["texts"])
        formatted_locations = [{"page": 1, "box": box} for box in seal_result["locations"]]

        return {
            "text": page_text,
            "pages": [{"page": 1, "text": page_text}],
            "seals": {
                "count": seal_result["count"],
                "texts": seal_result["texts"],
                "locations": formatted_locations,
            },
            "layout_blocks": layout_blocks,
            "ocr_applied": bool(ocr_text.strip() or layout_text.strip() or seal_result["count"]),
            "structure_used": structure_used,
        }
