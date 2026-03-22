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
        self.signature_dir = "output_signatures"
        if not os.path.exists(self.seal_dir):
            os.makedirs(self.seal_dir)
        if not os.path.exists(self.signature_dir):
            os.makedirs(self.signature_dir)

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
            print(f"OCRService bootstrap failed: {exc}")
            return

        last_error: Exception | None = None
        for device in self._candidate_devices():
            for kwargs in self._iter_ocr_kwargs_candidates(device):
                try:
                    self.ocr = PaddleOCR(**kwargs)
                    self.available = True
                    self.active_device = device
                    print(
                        "OCRService: PaddleOCR initialized "
                        f"(device={self.active_device}, lang={settings.PADDLE_OCR_LANG}, version={settings.PADDLE_OCR_VERSION})"
                    )
                    print(f"OCRService: seal crops will be saved to {self.seal_dir}")
                    break
                except Exception as exc:
                    last_error = exc
            if self.available:
                break
            print(f"OCRService: PaddleOCR init failed (device={device}): {last_error}")

        if not self.available:
            print(f"OCRService bootstrap failed: {last_error}")
            return

        if settings.PADDLE_OCR_ENABLE_STRUCTURE:
            self._init_structure_engine()

    def _init_structure_engine(self) -> None:
        try:
            from paddleocr import PPStructureV3
        except Exception as exc:
            self.structure = None
            self.structure_available = False
            print(f"OCRService: PPStructureV3 disabled, fallback to OCR-only pipeline: {exc}")
            return

        last_error: Exception | None = None
        for kwargs in self._iter_structure_kwargs_candidates(self.active_device):
            try:
                self.structure = PPStructureV3(**kwargs)
                self.structure_available = True
                print(f"OCRService: PPStructureV3 initialized (device={self.active_device})")
                return
            except Exception as exc:
                last_error = exc

        self.structure = None
        self.structure_available = False
        print(f"OCRService: PPStructureV3 disabled, fallback to OCR-only pipeline: {last_error}")

    def _empty_result(self) -> dict:
        return {
            "text": "",
            "pages": [],
            "seals": {"count": 0, "texts": [], "locations": [], "covered_texts": []},
            "signatures": {"count": 0, "texts": [], "locations": []},
            "layout_sections": [],
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

    def _empty_marker_result(self) -> dict:
        return {
            "seals": {"count": 0, "texts": [], "locations": [], "covered_texts": []},
            "signatures": {"count": 0, "texts": [], "locations": []},
            "marker_applied": False,
        }

    def _finalize_marker_result(self, seals: dict, signatures: dict) -> tuple[dict, dict]:
        seals["texts"] = list(dict.fromkeys(seals.get("texts", [])))
        dedup_covered_texts: list[dict] = []
        covered_seen = set()
        for item in seals.get("covered_texts", []):
            signature = (item.get("page"), str(item.get("box")), item.get("text"))
            if signature in covered_seen or not item.get("text"):
                continue
            covered_seen.add(signature)
            dedup_covered_texts.append(item)
        seals["covered_texts"] = dedup_covered_texts
        signatures["texts"] = list(dict.fromkeys(signatures.get("texts", [])))
        return seals, signatures

    def extract_visual_markers(self, file_path: str, file_type: str = "pdf") -> dict:
        if not self.available:
            return self._empty_marker_result()

        ext = file_type.lower().lstrip(".")
        all_seals = {"count": 0, "texts": [], "locations": [], "covered_texts": []}
        all_signatures = {"count": 0, "texts": [], "locations": []}
        marker_applied = False

        if ext == "pdf":
            doc = fitz.open(file_path)
            try:
                total_pages = len(doc)
                for page_index in range(total_pages):
                    page_no = page_index + 1
                    image = self._render_pdf_page(doc[page_index])
                    seal_result = self._detect_seals(image, page_no=page_no)
                    signature_result = self._detect_handwritten_signatures(image, page_no=page_no)

                    if seal_result["count"] > 0:
                        all_seals["count"] += seal_result["count"]
                        all_seals["texts"].extend(seal_result["texts"])
                        for box in seal_result["locations"]:
                            all_seals["locations"].append({"page": page_no, "box": box})
                        for covered in seal_result.get("covered_texts", []):
                            if not isinstance(covered, dict):
                                continue
                            all_seals["covered_texts"].append(
                                {
                                    "page": int(covered.get("page", page_no)),
                                    "box": covered.get("box", []),
                                    "text": str(covered.get("text") or "").strip(),
                                }
                            )

                    if signature_result["count"] > 0:
                        all_signatures["count"] += signature_result["count"]
                        all_signatures["texts"].extend(signature_result["texts"])
                        for box in signature_result["locations"]:
                            all_signatures["locations"].append({"page": page_no, "box": box})

                    marker_applied = marker_applied or bool(
                        seal_result["count"] or seal_result.get("covered_texts") or signature_result["count"]
                    )
            finally:
                doc.close()
        else:
            image = cv2.imread(file_path)
            if image is None:
                return self._empty_marker_result()

            seal_result = self._detect_seals(image, page_no=1)
            signature_result = self._detect_handwritten_signatures(image, page_no=1)

            all_seals["count"] = seal_result["count"]
            all_seals["texts"] = seal_result["texts"]
            all_seals["locations"] = [{"page": 1, "box": box} for box in seal_result["locations"]]
            all_seals["covered_texts"] = seal_result.get("covered_texts", [])

            all_signatures["count"] = signature_result["count"]
            all_signatures["texts"] = signature_result["texts"]
            all_signatures["locations"] = [{"page": 1, "box": box} for box in signature_result["locations"]]

            marker_applied = bool(
                seal_result["count"] or seal_result.get("covered_texts") or signature_result["count"]
            )

        finalized_seals, finalized_signatures = self._finalize_marker_result(all_seals, all_signatures)
        return {
            "seals": finalized_seals,
            "signatures": finalized_signatures,
            "marker_applied": marker_applied,
        }

    def _run_predictor(self, predictor: Any, image: np.ndarray, predictor_name: str) -> list:
        if predictor is None:
            return []
        try:
            return list(predictor.predict(image))
        except Exception as exc:
            print(f"{predictor_name} inference failed: {exc}")
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
            print(f"OCR result parsing warning: {exc}")

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

    def _bbox_anchor(self, bbox: Any) -> tuple[float, float]:
        builtin_bbox = self._to_builtin(bbox)
        if builtin_bbox is None:
            return (1e9, 1e9)
        if isinstance(builtin_bbox, (list, tuple)):
            # [x1, y1, x2, y2]
            if len(builtin_bbox) >= 2 and all(isinstance(item, (int, float)) for item in builtin_bbox[:2]):
                return (float(builtin_bbox[0]), float(builtin_bbox[1]))
            # [[x, y], ...]
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

    def _normalize_layout_type(self, value: str) -> str:
        normalized = str(value or "").strip().lower()
        if not normalized:
            return "text"
        if any(token in normalized for token in ("title", "header", "heading")):
            return "heading"
        if "table" in normalized:
            return "table"
        if any(token in normalized for token in ("figure", "image", "chart", "photo")):
            return "figure"
        return "text"

    def _normalize_section_text(self, text: str) -> str:
        normalized = re.sub(r"\s+", " ", str(text or "")).strip()
        if not normalized:
            return ""
        # Avoid persisting markdown/html fragments directly.
        normalized = re.sub(r"<[^>]+>", " ", normalized)
        normalized = re.sub(r"\s+", " ", normalized).strip()
        return normalized

    def _clip_section_text(self, text: str, max_chars: int = 240) -> str:
        if len(text) <= max_chars:
            return text
        return f"{text[:max_chars].rstrip()}..."

    def _simplify_layout_sections(self, layout_blocks: list[dict]) -> list[dict]:
        if not layout_blocks:
            return []

        sorted_blocks = sorted(
            layout_blocks,
            key=lambda item: (
                int(item.get("page", 0) or 0),
                float(item.get("_order_y", 1e9) or 1e9),
                float(item.get("_order_x", 1e9) or 1e9),
            ),
        )

        sections: list[dict] = []
        seen = set()
        page_section_count: dict[int, int] = {}
        max_sections_per_page = 60

        for block in sorted_blocks:
            page_no = int(block.get("page", 0) or 0)
            if page_no <= 0:
                continue
            if page_section_count.get(page_no, 0) >= max_sections_per_page:
                continue

            section_type = self._normalize_layout_type(str(block.get("type") or "text"))
            section_text = self._normalize_section_text(block.get("text") or "")
            if len(section_text) < 2:
                continue
            section_text = self._clip_section_text(section_text)

            signature = (page_no, section_type, section_text)
            if signature in seen:
                continue
            seen.add(signature)
            sections.append({"page": page_no, "type": section_type, "text": section_text})
            page_section_count[page_no] = page_section_count.get(page_no, 0) + 1

        return sections

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
                    x_anchor, y_anchor = self._bbox_anchor(bbox)
                    blocks.append(
                        {
                            "page": page_no,
                            "type": label or "text",
                            "text": text,
                            "bbox": bbox,
                            "_order_x": x_anchor,
                            "_order_y": y_anchor,
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
        layout_sections = self._simplify_layout_sections(layout_blocks)
        layout_text = self._merge_text_parts([section["text"] for section in layout_sections])
        if not layout_text:
            layout_text = self._extract_structure_text(structure_result)

        return layout_text, layout_sections, bool(layout_text or layout_sections)

    def _remove_seal_texts(self, text: str, seal_texts: list[str]) -> str:
        if not text or not seal_texts or not settings.PADDLE_OCR_EXCLUDE_SEAL_TEXT:
            return text

        cleaned_text = text
        for seal_text in seal_texts:
            normalized = str(seal_text or "").strip()
            if len(normalized) >= 2:
                cleaned_text = cleaned_text.replace(normalized, "")
        return re.sub(r"\n{3,}", "\n\n", cleaned_text).strip()

    def _sanitize_recognized_text(self, text: str, *, min_len: int = 2, max_len: int = 80) -> str:
        normalized = re.sub(r"\s+", "", str(text or ""))
        normalized = re.sub(r"[^0-9A-Za-z\u4e00-\u9fa5]", "", normalized)
        if len(normalized) < min_len:
            return ""
        if len(normalized) > max_len:
            return normalized[:max_len]
        return normalized

    def _expand_box(self, box: list[int], img_shape: tuple[int, int, int], pad: int = 12) -> tuple[int, int, int, int]:
        x, y, w, h = box
        img_h, img_w = img_shape[:2]
        x1 = max(0, x - pad)
        y1 = max(0, y - pad)
        x2 = min(img_w, x + w + pad)
        y2 = min(img_h, y + h + pad)
        return x1, y1, x2, y2

    def _recover_text_from_seal_region(
        self,
        img_bgr: np.ndarray,
        seal_box: list[int],
        full_seal_mask: np.ndarray,
    ) -> str:
        if img_bgr is None or self.ocr is None:
            return ""

        x1, y1, x2, y2 = self._expand_box(seal_box, img_bgr.shape, pad=14)
        if x2 <= x1 or y2 <= y1:
            return ""

        roi = img_bgr[y1:y2, x1:x2]
        roi_mask = full_seal_mask[y1:y2, x1:x2]
        if roi.size == 0 or roi_mask.size == 0:
            return ""

        inpainted = cv2.inpaint(roi, roi_mask, 3, cv2.INPAINT_TELEA)

        b_channel, g_channel, _ = cv2.split(roi)
        red_suppressed_gray = cv2.max(b_channel, g_channel)
        red_suppressed = cv2.cvtColor(red_suppressed_gray, cv2.COLOR_GRAY2BGR)

        gray = cv2.cvtColor(inpainted, cv2.COLOR_BGR2GRAY)
        enhanced_binary = cv2.adaptiveThreshold(
            gray,
            255,
            cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
            cv2.THRESH_BINARY,
            31,
            11,
        )
        enhanced = cv2.cvtColor(enhanced_binary, cv2.COLOR_GRAY2BGR)

        recovered_candidates: list[str] = []
        for candidate in (inpainted, red_suppressed, enhanced):
            resized = cv2.resize(candidate, None, fx=2.0, fy=2.0, interpolation=cv2.INTER_CUBIC)
            candidate_result = self._run_predictor(self.ocr, resized, "Seal Covered OCR")
            raw_text = self._extract_text_from_result(candidate_result, join_char="")
            cleaned = self._sanitize_recognized_text(raw_text, min_len=2, max_len=40)
            if cleaned:
                recovered_candidates.append(cleaned)

        return self._merge_text_parts(recovered_candidates, join_char=" ")

    def _is_box_close(self, box_a: list[int], box_b: list[int], x_gap: int, y_gap: int) -> bool:
        ax1, ay1, aw, ah = box_a
        bx1, by1, bw, bh = box_b
        ax2, ay2 = ax1 + aw, ay1 + ah
        bx2, by2 = bx1 + bw, by1 + bh
        return not (
            ax2 + x_gap < bx1
            or bx2 + x_gap < ax1
            or ay2 + y_gap < by1
            or by2 + y_gap < ay1
        )

    def _merge_nearby_boxes(self, boxes: list[list[int]], x_gap: int = 24, y_gap: int = 20) -> list[list[int]]:
        if not boxes:
            return []
        merged: list[list[int]] = []
        for box in sorted(boxes, key=lambda item: (item[1], item[0])):
            merged_into_existing = False
            for current in merged:
                if self._is_box_close(current, box, x_gap=x_gap, y_gap=y_gap):
                    cx, cy, cw, ch = current
                    bx, by, bw, bh = box
                    x1 = min(cx, bx)
                    y1 = min(cy, by)
                    x2 = max(cx + cw, bx + bw)
                    y2 = max(cy + ch, by + bh)
                    current[0] = x1
                    current[1] = y1
                    current[2] = x2 - x1
                    current[3] = y2 - y1
                    merged_into_existing = True
                    break
            if not merged_into_existing:
                merged.append(box.copy())
        return merged

    def _detect_handwritten_signatures(self, img_bgr: np.ndarray, page_no: int = 1) -> dict:
        signature_info = {"count": 0, "texts": [], "locations": []}
        if img_bgr is None or not getattr(settings, "PADDLE_OCR_ENABLE_SIGNATURE_RECOGNITION", True):
            return signature_info

        img_h, img_w = img_bgr.shape[:2]
        if img_h < 20 or img_w < 20:
            return signature_info

        hsv = cv2.cvtColor(img_bgr, cv2.COLOR_BGR2HSV)
        gray = cv2.cvtColor(img_bgr, cv2.COLOR_BGR2GRAY)

        blue_mask = cv2.inRange(hsv, np.array([90, 40, 20]), np.array([140, 255, 255]))
        dark_mask = cv2.inRange(gray, 0, 130)
        stroke_mask = cv2.bitwise_or(blue_mask, dark_mask)

        roi_top = int(img_h * 0.35)
        bottom_mask = stroke_mask[roi_top:, :]
        kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (5, 3))
        bottom_mask = cv2.morphologyEx(bottom_mask, cv2.MORPH_CLOSE, kernel, iterations=1)
        bottom_mask = cv2.erode(bottom_mask, np.ones((2, 2), np.uint8), iterations=1)

        contours, _ = cv2.findContours(bottom_mask, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)

        candidate_boxes: list[list[int]] = []
        for contour in contours:
            area = cv2.contourArea(contour)
            if area < 280 or area > 15000:
                continue
            x, y, w, h = cv2.boundingRect(contour)
            aspect_ratio = float(w) / max(float(h), 1.0)
            fill_ratio = float(area) / max(float(w * h), 1.0)
            if w < 50 or h < 12:
                continue
            if not 1.4 < aspect_ratio < 14.0:
                continue
            if not 0.02 < fill_ratio < 0.65:
                continue

            candidate_boxes.append([int(x), int(y + roi_top), int(w), int(h)])

        merged_boxes = self._merge_nearby_boxes(candidate_boxes, x_gap=28, y_gap=22)
        merged_boxes = sorted(merged_boxes, key=lambda item: item[1], reverse=True)[:8]

        signature_idx = 0
        for box in merged_boxes:
            x, y, w, h = box
            signature_idx += 1
            x1, y1, x2, y2 = self._expand_box(box, img_bgr.shape, pad=10)
            signature_crop = img_bgr[y1:y2, x1:x2]
            if signature_crop.size == 0:
                continue

            save_path = os.path.join(self.signature_dir, f"signature_P{page_no}_{signature_idx}.png")
            cv2.imwrite(save_path, signature_crop)

            gray_crop = cv2.cvtColor(signature_crop, cv2.COLOR_BGR2GRAY)
            binary_crop = cv2.adaptiveThreshold(
                gray_crop,
                255,
                cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
                cv2.THRESH_BINARY,
                35,
                15,
            )
            binary_bgr = cv2.cvtColor(binary_crop, cv2.COLOR_GRAY2BGR)

            signature_text_candidates: list[str] = []
            for candidate in (signature_crop, binary_bgr):
                resized = cv2.resize(candidate, None, fx=2.0, fy=2.0, interpolation=cv2.INTER_CUBIC)
                candidate_result = self._run_predictor(self.ocr, resized, "Signature OCR")
                raw_text = self._extract_text_from_result(candidate_result, join_char="")
                clean_text = self._sanitize_recognized_text(raw_text, min_len=2, max_len=20)
                if clean_text:
                    signature_text_candidates.append(clean_text)

            merged_text = self._merge_text_parts(signature_text_candidates, join_char=" ")
            if merged_text:
                signature_info["texts"].append(merged_text)

            signature_info["count"] += 1
            signature_info["locations"].append([x, y, w, h])

        signature_info["texts"] = list(dict.fromkeys(signature_info["texts"]))
        return signature_info

    def _append_recovered_texts(self, page_text: str, recovered_items: list[dict]) -> str:
        recovered_text = self._merge_text_parts(
            [str(item.get("text") or "").strip() for item in recovered_items if isinstance(item, dict)],
            join_char="\n",
        )
        if not recovered_text:
            return page_text
        return self._merge_text_parts([page_text, recovered_text], join_char="\n")

    def _detect_seals(self, img_bgr: np.ndarray, page_no: int = 1) -> dict:
        seal_info = {"count": 0, "texts": [], "locations": [], "covered_texts": []}
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
            clean_text = self._sanitize_recognized_text(raw_text, min_len=2, max_len=30)
            if clean_text:
                seal_info["texts"].append(clean_text)

            recovered_text = self._recover_text_from_seal_region(img_bgr, box, mask)
            if recovered_text:
                seal_info["covered_texts"].append({"page": page_no, "box": box, "text": recovered_text})

            seal_info["count"] += 1
            seal_info["locations"].append(box)

        seal_info["texts"] = list(dict.fromkeys(seal_info["texts"]))
        dedup_covered_texts: list[dict] = []
        covered_seen = set()
        for item in seal_info["covered_texts"]:
            signature = (item.get("page"), item.get("text"), str(item.get("box")))
            if signature in covered_seen:
                continue
            covered_seen.add(signature)
            dedup_covered_texts.append(item)
        seal_info["covered_texts"] = dedup_covered_texts
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
        layout_sections: list[dict] = []
        all_seals = {"count": 0, "texts": [], "locations": [], "covered_texts": []}
        all_signatures = {"count": 0, "texts": [], "locations": []}
        ocr_applied = False
        structure_used = False

        doc = fitz.open(pdf_path)
        try:
            total_pages = len(doc)
            pbar = tqdm(range(total_pages), desc="OCR processing", unit="page")
            for page_index in pbar:
                page_no = page_index + 1
                image = self._render_pdf_page(doc[page_index])

                ocr_result = self._run_predictor(self.ocr, image, "PaddleOCR")
                ocr_text = self._extract_text_from_result(ocr_result, join_char="\n")
                layout_text, page_layout_sections, page_structure_used = self._run_structure_layout(image, page_no)
                seal_result = self._detect_seals(image, page_no=page_no)
                signature_result = self._detect_handwritten_signatures(image, page_no=page_no)

                page_text = self._combine_page_text(layout_text, ocr_text)
                page_text = self._remove_seal_texts(page_text, seal_result["texts"])
                page_text = self._append_recovered_texts(page_text, seal_result.get("covered_texts", []))

                if seal_result["count"] > 0:
                    all_seals["count"] += seal_result["count"]
                    all_seals["texts"].extend(seal_result["texts"])
                    for box in seal_result["locations"]:
                        all_seals["locations"].append({"page": page_no, "box": box})
                    for covered in seal_result.get("covered_texts", []):
                        if not isinstance(covered, dict):
                            continue
                        all_seals["covered_texts"].append(
                            {
                                "page": int(covered.get("page", page_no)),
                                "box": covered.get("box", []),
                                "text": str(covered.get("text") or "").strip(),
                            }
                        )

                if signature_result["count"] > 0:
                    all_signatures["count"] += signature_result["count"]
                    all_signatures["texts"].extend(signature_result["texts"])
                    for box in signature_result["locations"]:
                        all_signatures["locations"].append({"page": page_no, "box": box})

                if page_layout_sections:
                    layout_sections.extend(page_layout_sections)

                pages_data.append({"page": page_no, "text": page_text})
                full_text.append(page_text)
                ocr_applied = ocr_applied or bool(
                    ocr_text.strip()
                    or layout_text.strip()
                    or seal_result["count"]
                    or seal_result.get("covered_texts")
                    or signature_result["count"]
                )
                structure_used = structure_used or page_structure_used
                pbar.set_postfix({"seal_count": all_seals["count"], "signature_count": all_signatures["count"]})
        finally:
            doc.close()

        all_seals["texts"] = list(dict.fromkeys(all_seals["texts"]))
        dedup_covered_texts: list[dict] = []
        covered_seen = set()
        for item in all_seals["covered_texts"]:
            signature = (item.get("page"), str(item.get("box")), item.get("text"))
            if signature in covered_seen or not item.get("text"):
                continue
            covered_seen.add(signature)
            dedup_covered_texts.append(item)
        all_seals["covered_texts"] = dedup_covered_texts
        all_signatures["texts"] = list(dict.fromkeys(all_signatures["texts"]))
        return {
            "text": self._merge_text_parts(full_text),
            "pages": pages_data,
            "seals": all_seals,
            "signatures": all_signatures,
            "layout_sections": layout_sections,
            "ocr_applied": ocr_applied,
            "structure_used": structure_used,
        }

    def _recognize_image(self, img_path: str) -> dict:
        image = cv2.imread(img_path)
        if image is None:
            return self._empty_result()

        ocr_result = self._run_predictor(self.ocr, image, "PaddleOCR")
        ocr_text = self._extract_text_from_result(ocr_result, join_char="\n")
        layout_text, layout_sections, structure_used = self._run_structure_layout(image, page_no=1)
        seal_result = self._detect_seals(image, page_no=1)
        signature_result = self._detect_handwritten_signatures(image, page_no=1)

        page_text = self._combine_page_text(layout_text, ocr_text)
        page_text = self._remove_seal_texts(page_text, seal_result["texts"])
        page_text = self._append_recovered_texts(page_text, seal_result.get("covered_texts", []))
        formatted_locations = [{"page": 1, "box": box} for box in seal_result["locations"]]
        formatted_signature_locations = [{"page": 1, "box": box} for box in signature_result["locations"]]

        return {
            "text": page_text,
            "pages": [{"page": 1, "text": page_text}],
            "seals": {
                "count": seal_result["count"],
                "texts": seal_result["texts"],
                "locations": formatted_locations,
                "covered_texts": seal_result.get("covered_texts", []),
            },
            "signatures": {
                "count": signature_result["count"],
                "texts": list(dict.fromkeys(signature_result["texts"])),
                "locations": formatted_signature_locations,
            },
            "layout_sections": layout_sections,
            "ocr_applied": bool(
                ocr_text.strip()
                or layout_text.strip()
                or seal_result["count"]
                or seal_result.get("covered_texts")
                or signature_result["count"]
            ),
            "structure_used": structure_used,
        }
