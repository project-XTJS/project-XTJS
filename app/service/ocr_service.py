import html
import os
import queue
import re
import threading
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
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
        self._structure_init_attempted = False
        self._predictor_lock = threading.Lock()
        self.active_device = "cpu"
        self.seal_dir = "output_seals"
        self.signature_dir = "output_signatures"
        if not os.path.exists(self.seal_dir):
            os.makedirs(self.seal_dir)
        if not os.path.exists(self.signature_dir):
            os.makedirs(self.signature_dir)
        self.face_detector = self._load_face_detector()

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

    def _load_face_detector(self):
        cv2_data = getattr(cv2, "data", None)
        cascade_dir = getattr(cv2_data, "haarcascades", "") if cv2_data is not None else ""
        cascade_path = os.path.join(cascade_dir, "haarcascade_frontalface_default.xml")
        if not cascade_path or not os.path.exists(cascade_path):
            return None
        detector = cv2.CascadeClassifier(cascade_path)
        if detector is None or detector.empty():
            return None
        return detector

    def _detect_faces(self, img_bgr: np.ndarray) -> list[list[int]]:
        if self.face_detector is None or img_bgr is None or img_bgr.size == 0:
            return []
        gray = cv2.cvtColor(img_bgr, cv2.COLOR_BGR2GRAY)
        gray = cv2.equalizeHist(gray)
        min_side = max(24, int(min(gray.shape[:2]) * 0.05))
        try:
            faces = self.face_detector.detectMultiScale(
                gray,
                scaleFactor=1.12,
                minNeighbors=5,
                minSize=(min_side, min_side),
            )
        except Exception:
            return []

        results: list[list[int]] = []
        for x, y, w, h in faces:
            if w <= 0 or h <= 0:
                continue
            results.append([int(x), int(y), int(w), int(h)])
        return results

    def _estimate_ring_contrast(self, red_mask: np.ndarray) -> float:
        if red_mask is None or red_mask.size == 0:
            return 0.0
        h, w = red_mask.shape[:2]
        min_side = min(h, w)
        if min_side < 18:
            return 0.0

        band = max(2, int(min_side * 0.12))
        outer = np.zeros((h, w), dtype=np.uint8)
        outer[:band, :] = 255
        outer[h - band :, :] = 255
        outer[:, :band] = 255
        outer[:, w - band :] = 255
        outer_area = float(max(cv2.countNonZero(outer), 1))
        outer_red = float(cv2.countNonZero(cv2.bitwise_and(red_mask, outer))) / outer_area

        if h <= (band * 2) or w <= (band * 2):
            return outer_red

        inner = np.zeros((h, w), dtype=np.uint8)
        inner[band : h - band, band : w - band] = 255
        inner_area = float(max(cv2.countNonZero(inner), 1))
        inner_red = float(cv2.countNonZero(cv2.bitwise_and(red_mask, inner))) / inner_area
        return outer_red - inner_red

    def _build_candidate_profile(self, roi_bgr: np.ndarray, roi_red_mask: np.ndarray) -> dict[str, float]:
        if roi_bgr is None or roi_bgr.size == 0 or roi_red_mask is None or roi_red_mask.size == 0:
            return {
                "red_ratio": 0.0,
                "yellow_ratio": 0.0,
                "green_ratio": 0.0,
                "blue_ratio": 0.0,
                "skin_ratio": 0.0,
                "ring_contrast": 0.0,
                "edge_ratio": 0.0,
                "center_red_ratio": 0.0,
            }

        h, w = roi_red_mask.shape[:2]
        area = float(max(h * w, 1))

        hsv = cv2.cvtColor(roi_bgr, cv2.COLOR_BGR2HSV)
        yellow_mask = cv2.inRange(hsv, np.array([15, 60, 60]), np.array([42, 255, 255]))
        green_mask = cv2.inRange(hsv, np.array([35, 40, 40]), np.array([90, 255, 255]))
        blue_mask = cv2.inRange(hsv, np.array([90, 40, 40]), np.array([130, 255, 255]))

        ycrcb = cv2.cvtColor(roi_bgr, cv2.COLOR_BGR2YCrCb)
        skin_mask = cv2.inRange(ycrcb, np.array([0, 133, 77]), np.array([255, 173, 127]))

        gray = cv2.cvtColor(roi_bgr, cv2.COLOR_BGR2GRAY)
        edge_mask = cv2.Canny(gray, 80, 180)
        center_mask = np.zeros((h, w), dtype=np.uint8)
        center_pad_x = max(1, int(w * 0.30))
        center_pad_y = max(1, int(h * 0.30))
        if (w - center_pad_x) > center_pad_x and (h - center_pad_y) > center_pad_y:
            center_mask[center_pad_y : h - center_pad_y, center_pad_x : w - center_pad_x] = 255
        else:
            center_mask[:, :] = 255
        center_area = float(max(cv2.countNonZero(center_mask), 1))
        center_red_ratio = float(cv2.countNonZero(cv2.bitwise_and(roi_red_mask, center_mask))) / center_area

        return {
            "red_ratio": float(cv2.countNonZero(roi_red_mask)) / area,
            "yellow_ratio": float(cv2.countNonZero(yellow_mask)) / area,
            "green_ratio": float(cv2.countNonZero(green_mask)) / area,
            "blue_ratio": float(cv2.countNonZero(blue_mask)) / area,
            "skin_ratio": float(cv2.countNonZero(skin_mask)) / area,
            "ring_contrast": self._estimate_ring_contrast(roi_red_mask),
            "edge_ratio": float(cv2.countNonZero(edge_mask)) / area,
            "center_red_ratio": center_red_ratio,
        }

    def _is_national_emblem_like(self, profile: dict[str, float]) -> bool:
        red_ratio = float(profile.get("red_ratio", 0.0))
        yellow_ratio = float(profile.get("yellow_ratio", 0.0))
        ring_contrast = float(profile.get("ring_contrast", 0.0))
        edge_ratio = float(profile.get("edge_ratio", 0.0))
        return (
            red_ratio >= 0.12
            and yellow_ratio >= 0.08
            and ring_contrast <= 0.08
            and edge_ratio >= 0.02
        )

    def _has_round_star_stamp_evidence(
        self,
        profile: dict[str, float],
        *,
        merged_aspect: float,
        merged_red_density: float,
    ) -> bool:
        ring_contrast = float(profile.get("ring_contrast", 0.0))
        edge_ratio = float(profile.get("edge_ratio", 0.0))
        center_red_ratio = float(profile.get("center_red_ratio", 0.0))
        yellow_ratio = float(profile.get("yellow_ratio", 0.0))
        green_ratio = float(profile.get("green_ratio", 0.0))
        blue_ratio = float(profile.get("blue_ratio", 0.0))
        return (
            0.58 <= merged_aspect <= 1.74
            and merged_red_density >= 0.026
            and ring_contrast >= 0.055
            and center_red_ratio >= 0.030
            and edge_ratio <= 0.23
            and yellow_ratio <= 0.22
            and green_ratio <= 0.12
            and blue_ratio <= 0.12
        )

    def _box_overlap_on_min_area(self, box_a: list[int], box_b: list[int]) -> float:
        inter_area = self._box_intersection_area(box_a, box_b)
        if inter_area <= 0:
            return 0.0
        area_a = max(int(box_a[2]) * int(box_a[3]), 1)
        area_b = max(int(box_b[2]) * int(box_b[3]), 1)
        return float(inter_area) / float(min(area_a, area_b))

    def _is_face_like_candidate(self, box: list[int], profile: dict[str, float], face_boxes: list[list[int]]) -> bool:
        for face_box in face_boxes:
            if self._box_overlap_on_min_area(box, face_box) >= 0.30:
                return True
        skin_ratio = float(profile.get("skin_ratio", 0.0))
        red_ratio = float(profile.get("red_ratio", 0.0))
        ring_contrast = float(profile.get("ring_contrast", 0.0))
        edge_ratio = float(profile.get("edge_ratio", 0.0))
        if skin_ratio >= 0.22 and ring_contrast < 0.10 and edge_ratio < 0.16 and red_ratio <= 0.60:
            return True
        if skin_ratio >= 0.35 and ring_contrast < 0.14 and red_ratio <= 0.70:
            return True
        return False

    def _is_reliable_marker_text(self, seal_text: str, covered_text: str) -> bool:
        merged = self._merge_text_parts([seal_text, covered_text], join_char="")
        if not merged:
            return False
        has_strong_hint = self._contains_strong_seal_hint_text(seal_text, covered_text)
        identity_document_like = self._contains_identity_document_text(seal_text, covered_text)
        if identity_document_like and not has_strong_hint:
            return False
        if has_strong_hint:
            return True
        chinese_chars = re.findall(r"[\u4e00-\u9fa5]", merged)
        if len(chinese_chars) >= 4 and any(
            token in merged
            for token in ("\u516c\u53f8", "\u6709\u9650", "\u4e13\u7528", "\u5408\u540c", "\u8d22\u52a1", "\u76d6\u7ae0")
        ):
            return True
        return False

    def _contains_strong_seal_hint_text(self, seal_text: str, covered_text: str) -> bool:
        merged = self._merge_text_parts([seal_text, covered_text], join_char="")
        if not merged:
            return False
        strong_tokens = (
            "\u516c\u7ae0",
            "\u5370\u7ae0",
            "\u4e13\u7528\u7ae0",
            "\u5408\u540c\u7ae0",
            "\u8d22\u52a1\u7ae0",
            "\u53d1\u7968\u7ae0",
            "\u9a91\u7f1d\u7ae0",
            "\u76d6\u7ae0",
            "\u7b7e\u7ae0",
            "\u6cd5\u4eba\u7ae0",
            "\u7535\u5b50\u7ae0",
        )
        if any(token in merged for token in strong_tokens):
            return True
        return bool(re.search(r"(\u4e13\u7528|\u5408\u540c|\u8d22\u52a1|\u53d1\u7968|\u6295\u6807).{0,2}\u7ae0", merged))

    def _contains_seal_hint_text(self, seal_text: str, covered_text: str) -> bool:
        merged = self._merge_text_parts([seal_text, covered_text], join_char="")
        if not merged:
            return False
        if self._contains_strong_seal_hint_text(seal_text, covered_text):
            return True
        hint_tokens = (
            "\u516c\u53f8",
            "\u4e13\u7528",
            "\u5408\u540c",
            "\u8d22\u52a1",
            "\u53d1\u7968",
            "\u6295\u6807",
        )
        return any(token in merged for token in hint_tokens)

    def _contains_identity_document_text(self, seal_text: str, covered_text: str) -> bool:
        merged = self._merge_text_parts([seal_text, covered_text], join_char="")
        if not merged:
            return False
        identity_tokens = (
            "\u5c45\u6c11\u8eab\u4efd\u8bc1",
            "\u8eab\u4efd\u8bc1",
            "\u4e2d\u534e\u4eba\u6c11\u5171\u548c\u56fd",
            "\u516c\u5b89\u5c40",
            "\u7b7e\u53d1\u673a\u5173",
            "\u6709\u6548\u671f\u9650",
            "\u516c\u6c11\u8eab\u4efd\u53f7\u7801",
            "\u673a\u52a8\u8f66\u9a7e\u9a76\u8bc1",
            "\u9a7e\u9a76\u8bc1",
            "\u884c\u9a76\u8bc1",
            "\u62a4\u7167",
            "\u6e2f\u6fb3\u901a\u884c\u8bc1",
            "\u5f80\u6765\u6e2f\u6fb3\u901a\u884c\u8bc1",
            "\u5c45\u4f4f\u8bc1",
            "\u793e\u4fdd\u5361",
            "\u5b9e\u4e60\u671f",
            "\u8bc1\u4ef6\u53f7",
            "\u53d1\u8bc1\u673a\u5173",
        )
        if any(token in merged for token in identity_tokens):
            return True
        if "\u8bc1" in merged and any(
            token in merged
            for token in ("\u8eab\u4efd", "\u9a7e\u9a76", "\u884c\u9a76", "\u516c\u5b89", "\u53d1\u8bc1", "\u6709\u6548\u671f")
        ):
            return True
        return False

    def _is_certificate_title_text(self, seal_text: str, covered_text: str) -> bool:
        merged = self._merge_text_parts([seal_text, covered_text], join_char="")
        if not merged:
            return False
        certificate_tokens = (
            "\u6bd5\u4e1a\u8bc1\u4e66",
            "\u5b66\u4f4d\u8bc1\u4e66",
            "\u8363\u8a89\u8bc1\u4e66",
            "\u83b7\u5956\u8bc1\u4e66",
            "\u8d44\u683c\u8bc1\u4e66",
            "\u8bc1\u4e66",
            "\u5c45\u6c11\u8eab\u4efd\u8bc1",
            "\u8eab\u4efd\u8bc1",
            "\u9a7e\u9a76\u8bc1",
            "\u884c\u9a76\u8bc1",
            "\u62a4\u7167",
            "\u6e2f\u6fb3\u901a\u884c\u8bc1",
            "\u5c45\u4f4f\u8bc1",
            "\u793e\u4fdd\u5361",
        )
        return any(token in merged for token in certificate_tokens)

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
        self._structure_init_attempted = True
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

    def _resolve_structure_enabled(self, use_structure: bool | None = None) -> bool:
        desired = bool(settings.PADDLE_OCR_ENABLE_STRUCTURE if use_structure is None else use_structure)
        if not desired:
            return False

        if (not self.structure_available or self.structure is None) and not self._structure_init_attempted:
            self._init_structure_engine()

        return bool(self.structure_available and self.structure is not None)

    def _resolve_marker_enabled(
        self,
        use_seal_recognition: bool | None = None,
        use_signature_recognition: bool | None = None,
    ) -> tuple[bool, bool]:
        seal_enabled = bool(
            settings.PADDLE_OCR_ENABLE_SEAL_RECOGNITION
            if use_seal_recognition is None
            else use_seal_recognition
        )
        signature_enabled = bool(
            getattr(settings, "PADDLE_OCR_ENABLE_SIGNATURE_RECOGNITION", True)
            if use_signature_recognition is None
            else use_signature_recognition
        )
        return seal_enabled, signature_enabled

    def _resolve_pipeline_enabled(self, total_pages: int) -> bool:
        if total_pages < 1:
            return False
        enabled = bool(getattr(settings, "PADDLE_OCR_ENABLE_PIPELINE_PARALLEL", True))
        try:
            min_pages = int(getattr(settings, "PADDLE_OCR_PIPELINE_MIN_PAGES", 2))
        except Exception:
            min_pages = 2
        return enabled and total_pages >= max(2, min_pages)

    def _resolve_pipeline_queue_size(self) -> int:
        try:
            queue_size = int(getattr(settings, "PADDLE_OCR_PIPELINE_QUEUE_SIZE", 4))
        except Exception:
            queue_size = 4
        return max(1, min(queue_size, 64))

    def _resolve_pipeline_render_workers(self, total_pages: int) -> int:
        try:
            workers = int(getattr(settings, "PADDLE_OCR_PIPELINE_RENDER_WORKERS", 2))
        except Exception:
            workers = 2
        cpu_cap = max(1, (os.cpu_count() or 4) // 2)
        return max(1, min(workers, total_pages, cpu_cap))

    def _resolve_pipeline_post_workers(self, total_pages: int) -> int:
        try:
            workers = int(getattr(settings, "PADDLE_OCR_PIPELINE_POST_WORKERS", 2))
        except Exception:
            workers = 2
        cpu_cap = max(1, os.cpu_count() or 4)
        return max(1, min(workers, total_pages, cpu_cap))

    def _should_log_pipeline_metrics(self) -> bool:
        return bool(getattr(settings, "PADDLE_OCR_PIPELINE_LOG_METRICS", True))

    def _empty_result(self) -> dict:
        return {
            "text": "",
            "pages": [],
            "seals": {"count": 0, "texts": [], "locations": [], "covered_texts": []},
            "signatures": {"count": 0, "texts": [], "locations": []},
            "layout_sections": [],
            "ocr_applied": False,
            "structure_used": False,
            "structure_enabled": False,
            "seal_recognition_enabled": False,
            "signature_recognition_enabled": False,
        }

    def extract_all(
        self,
        file_path: str,
        file_type: str = "pdf",
        use_structure: bool | None = None,
        use_seal_recognition: bool | None = None,
        use_signature_recognition: bool | None = None,
    ) -> dict:
        if not self.available:
            return self._empty_result()

        ext = file_type.lower().lstrip(".")
        structure_enabled = self._resolve_structure_enabled(use_structure=use_structure)
        seal_enabled, signature_enabled = self._resolve_marker_enabled(
            use_seal_recognition=use_seal_recognition,
            use_signature_recognition=use_signature_recognition,
        )
        if ext == "pdf":
            return self._recognize_pdf(
                file_path,
                structure_enabled=structure_enabled,
                seal_enabled=seal_enabled,
                signature_enabled=signature_enabled,
            )
        return self._recognize_image(
            file_path,
            structure_enabled=structure_enabled,
            seal_enabled=seal_enabled,
            signature_enabled=signature_enabled,
        )

    def _empty_layout_result(self) -> dict:
        return {
            "layout_sections": [],
            "structure_used": False,
            "structure_enabled": False,
            "layout_text": "",
        }

    def extract_structure_layout(
        self,
        file_path: str,
        file_type: str = "pdf",
        use_structure: bool | None = None,
    ) -> dict:
        if not self.available:
            return self._empty_layout_result()

        structure_enabled = self._resolve_structure_enabled(use_structure=use_structure)
        if not structure_enabled:
            return self._empty_layout_result()

        ext = file_type.lower().lstrip(".")
        layout_sections: list[dict] = []
        layout_text_parts: list[str] = []
        structure_used = False

        if ext == "pdf":
            doc = fitz.open(file_path)
            try:
                total_pages = len(doc)
                for page_index in range(total_pages):
                    page_no = page_index + 1
                    image = self._render_pdf_page(doc[page_index])
                    page_text, page_sections, page_structure_used = self._run_structure_layout(
                        image,
                        page_no,
                        structure_enabled=structure_enabled,
                    )
                    if page_text.strip():
                        layout_text_parts.append(page_text)
                    if page_sections:
                        layout_sections.extend(page_sections)
                    structure_used = structure_used or page_structure_used
            finally:
                doc.close()
        else:
            image = cv2.imread(file_path)
            if image is None:
                empty_result = self._empty_layout_result()
                empty_result["structure_enabled"] = structure_enabled
                return empty_result

            page_text, page_sections, page_structure_used = self._run_structure_layout(
                image,
                page_no=1,
                structure_enabled=structure_enabled,
            )
            if page_text.strip():
                layout_text_parts.append(page_text)
            if page_sections:
                layout_sections.extend(page_sections)
            structure_used = page_structure_used

        return {
            "layout_sections": layout_sections,
            "structure_used": structure_used,
            "structure_enabled": structure_enabled,
            "layout_text": self._merge_text_parts(layout_text_parts),
        }

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

    def extract_visual_markers(
        self,
        file_path: str,
        file_type: str = "pdf",
        use_seal_recognition: bool | None = None,
        use_signature_recognition: bool | None = None,
    ) -> dict:
        if not self.available:
            return self._empty_marker_result()

        seal_enabled, signature_enabled = self._resolve_marker_enabled(
            use_seal_recognition=use_seal_recognition,
            use_signature_recognition=use_signature_recognition,
        )
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
                    seal_result = self._detect_seals(image, page_no=page_no, enabled=seal_enabled)
                    signature_result = self._detect_handwritten_signatures(
                        image,
                        page_no=page_no,
                        enabled=signature_enabled,
                    )

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

            seal_result = self._detect_seals(image, page_no=1, enabled=seal_enabled)
            signature_result = self._detect_handwritten_signatures(
                image,
                page_no=1,
                enabled=signature_enabled,
            )

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
            with self._predictor_lock:
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

    def _dedupe_text_parts(self, parts: list[str]) -> list[str]:
        deduped: list[str] = []
        seen = set()
        for part in parts:
            normalized = str(part or "").strip()
            if normalized and normalized not in seen:
                deduped.append(normalized)
                seen.add(normalized)
        return deduped

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

    def _collect_nested_field_values(
        self,
        value: Any,
        field_names: tuple[str, ...],
        *,
        keep_markup: bool = False,
    ) -> list[str]:
        collected: list[str] = []

        def walk(node: Any, depth: int = 0) -> None:
            if node is None or depth > 6:
                return
            if isinstance(node, dict):
                for key, item in node.items():
                    if key in field_names:
                        if keep_markup and isinstance(item, str):
                            candidate = item.strip()
                        else:
                            candidate = self._extract_text_value(item)
                        if candidate:
                            collected.append(candidate)
                    if isinstance(item, (dict, list, tuple, set)):
                        walk(item, depth + 1)
                return
            if isinstance(node, (list, tuple, set)):
                for item in node:
                    walk(item, depth + 1)

        walk(self._to_builtin(value))
        return self._dedupe_text_parts(collected)

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

    def _normalize_section_text(self, text: str, *, preserve_lines: bool = False) -> str:
        normalized = html.unescape(str(text or ""))
        if preserve_lines:
            normalized = re.sub(r"\r\n?", "\n", normalized)
            normalized = re.sub(r"<br\s*/?>", "\n", normalized, flags=re.IGNORECASE)
            normalized = re.sub(r"</?(table|thead|tbody|tfoot|tr|p|div|section|article)[^>]*>", "\n", normalized, flags=re.IGNORECASE)
            normalized = re.sub(r"</?(td|th)[^>]*>", "\t", normalized, flags=re.IGNORECASE)
            normalized = re.sub(r"<[^>]+>", " ", normalized)
            normalized = re.sub(r"[^\S\n\t]+", " ", normalized)
            normalized = re.sub(r" *\t *", "\t", normalized)
            normalized = re.sub(r"[ \t]*\n[ \t]*", "\n", normalized)
            normalized = re.sub(r"\n{3,}", "\n\n", normalized)
            lines = [re.sub(r"[ \t]+", " ", line).strip() for line in normalized.splitlines()]
            return "\n".join(line for line in lines if line)

        normalized = re.sub(r"\s+", " ", normalized).strip()
        if not normalized:
            return ""
        normalized = re.sub(r"<[^>]+>", " ", normalized)
        normalized = re.sub(r"\s+", " ", normalized).strip()
        return normalized

    def _build_table_section(self, page_no: int, block: dict) -> dict | None:
        markdown_parts = self._dedupe_text_parts(block.get("table_markdown_parts") or [])
        html_parts = self._dedupe_text_parts(block.get("table_html_parts") or [])
        cell_texts = self._dedupe_text_parts(block.get("table_cell_texts") or [])
        raw_text = str(block.get("text") or "")

        full_text_candidates = [
            self._merge_text_parts(
                [self._normalize_section_text(item, preserve_lines=True) for item in markdown_parts],
                join_char="\n\n",
            ),
            self._merge_text_parts(
                [self._normalize_section_text(item, preserve_lines=True) for item in html_parts],
                join_char="\n\n",
            ),
            self._normalize_section_text(raw_text, preserve_lines=True),
            self._merge_text_parts(
                [self._normalize_section_text(item, preserve_lines=True) for item in cell_texts],
                join_char="\n",
            ),
        ]
        section_text = next((candidate for candidate in full_text_candidates if candidate), "")
        if len(section_text) < 2:
            return None

        section = {"page": page_no, "type": "table", "text": section_text}
        normalized_raw_text = self._normalize_section_text(raw_text, preserve_lines=True)
        if normalized_raw_text:
            section["raw_text"] = normalized_raw_text
        if markdown_parts:
            section["markdown"] = "\n\n".join(markdown_parts)
        if html_parts:
            section["html"] = "\n\n".join(html_parts)
        if cell_texts:
            section["cell_texts"] = cell_texts
        return section

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

            signature = (page_no, section["type"], section["text"])
            if signature in seen:
                continue
            seen.add(signature)
            sections.append(section)
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

            section_type = self._normalize_layout_type(label or "text")
            text = self._extract_text_value(node)
            bbox = None
            for key in bbox_keys:
                if key in node:
                    bbox = self._normalize_bbox(node.get(key))
                    if bbox is not None:
                        break

            table_markdown_parts: list[str] = []
            table_html_parts: list[str] = []
            table_cell_texts: list[str] = []
            if section_type == "table":
                table_markdown_parts = self._collect_nested_field_values(node, ("markdown",), keep_markup=True)
                table_html_parts = self._collect_nested_field_values(node, ("html", "pred_html"), keep_markup=True)
                table_cell_texts = self._collect_nested_field_values(node, ("rec_texts",))

            has_table_content = bool(table_markdown_parts or table_html_parts or table_cell_texts)
            is_layout_block = bool(label or bbox is not None or "block_content" in node)
            if is_layout_block and (text or has_table_content):
                signature_text = (
                    text
                    or "\n".join(table_cell_texts)
                    or "\n\n".join(table_markdown_parts)
                    or "\n\n".join(table_html_parts)
                )
                signature = (label or section_type, signature_text, str(bbox))
                if signature not in seen:
                    x_anchor, y_anchor = self._bbox_anchor(bbox)
                    block = {
                        "page": page_no,
                        "type": label or section_type or "text",
                        "text": text,
                        "bbox": bbox,
                        "_order_x": x_anchor,
                        "_order_y": y_anchor,
                    }
                    if table_markdown_parts:
                        block["table_markdown_parts"] = table_markdown_parts
                    if table_html_parts:
                        block["table_html_parts"] = table_html_parts
                    if table_cell_texts:
                        block["table_cell_texts"] = table_cell_texts
                    blocks.append(block)
                    seen.add(signature)

            for key in child_keys:
                if key in node:
                    walk(node.get(key))

            for key, value in node.items():
                if key in child_keys or key in label_keys or key in bbox_keys:
                    continue
                if key in {"block_content", "markdown", "text", "html", "pred_html", "content", "caption", "rec_texts", "texts"}:
                    continue
                if isinstance(value, (dict, list)):
                    walk(value)

        walk(built_result)
        return blocks

    def _extract_structure_text(self, structure_result: list) -> str:
        return self._extract_text_value(self._to_builtin(structure_result))

    def _combine_page_text(self, layout_text: str, ocr_text: str) -> str:
        return self._merge_text_parts([layout_text, ocr_text])

    def _run_structure_layout(
        self,
        image: np.ndarray,
        page_no: int,
        *,
        structure_enabled: bool,
    ) -> tuple[str, list[dict], bool]:
        if not structure_enabled or not self.structure_available or self.structure is None:
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
        seal_hint_tokens = ("公章", "印章", "专用章", "财务章", "合同章", "发票章", "签章", "签字")
        for seal_text in seal_texts:
            normalized = str(seal_text or "").strip()
            if len(normalized) < 2:
                continue
            # Only strip short and stamp-like snippets to avoid deleting real body text.
            if len(normalized) > 14:
                continue
            if any(company_token in normalized for company_token in ("有限公司", "有限责任公司", "股份有限公司", "集团有限公司")):
                continue
            if not any(token in normalized for token in seal_hint_tokens):
                continue
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

    def _build_seal_recovery_boxes(
        self,
        seal_box: list[int],
        img_shape: tuple[int, int, int],
    ) -> list[tuple[int, int, int, int]]:
        x, y, w, h = seal_box
        img_h, img_w = img_shape[:2]
        if w <= 0 or h <= 0:
            return []

        boxes: list[tuple[int, int, int, int]] = []
        boxes.append(self._expand_box(seal_box, img_shape, pad=14))

        context_x_pad = max(24, int(w * 0.85))
        context_y_pad = max(16, int(h * 0.35))
        boxes.append(
            (
                max(0, x - context_x_pad),
                max(0, y - context_y_pad),
                min(img_w, x + w + context_x_pad),
                min(img_h, y + h + context_y_pad),
            )
        )

        deduped: list[tuple[int, int, int, int]] = []
        seen = set()
        for box in boxes:
            if box[2] <= box[0] or box[3] <= box[1]:
                continue
            if box in seen:
                continue
            seen.add(box)
            deduped.append(box)
        return deduped

    def _recover_text_from_seal_region(
        self,
        img_bgr: np.ndarray,
        seal_box: list[int],
        full_seal_mask: np.ndarray,
    ) -> str:
        if img_bgr is None or self.ocr is None:
            return ""

        scored_candidates: list[tuple[float, str]] = []
        recovery_boxes = self._build_seal_recovery_boxes(seal_box, img_bgr.shape)
        if not recovery_boxes:
            return ""

        for x1, y1, x2, y2 in recovery_boxes:
            roi = img_bgr[y1:y2, x1:x2]
            roi_mask = full_seal_mask[y1:y2, x1:x2]
            if roi.size == 0 or roi_mask.size == 0:
                continue

            inpainted = cv2.inpaint(roi, roi_mask, 3, cv2.INPAINT_TELEA)

            b_channel, g_channel, _ = cv2.split(roi)
            red_suppressed_gray = cv2.max(b_channel, g_channel)
            red_suppressed = cv2.cvtColor(red_suppressed_gray, cv2.COLOR_GRAY2BGR)

            clahe = cv2.createCLAHE(clipLimit=2.2, tileGridSize=(8, 8))
            contrast_gray = clahe.apply(red_suppressed_gray)
            contrast_bgr = cv2.cvtColor(contrast_gray, cv2.COLOR_GRAY2BGR)

            min_side = max(3, min(int(contrast_gray.shape[0]), int(contrast_gray.shape[1])))
            adaptive_block = max(15, min(41, (min_side // 2) * 2 - 1))
            if adaptive_block % 2 == 0:
                adaptive_block += 1
            if adaptive_block <= 1:
                adaptive_block = 3

            enhanced_binary = cv2.adaptiveThreshold(
                contrast_gray,
                255,
                cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
                cv2.THRESH_BINARY,
                adaptive_block,
                11,
            )
            _, otsu_binary = cv2.threshold(
                contrast_gray,
                0,
                255,
                cv2.THRESH_BINARY + cv2.THRESH_OTSU,
            )
            enhanced = cv2.cvtColor(enhanced_binary, cv2.COLOR_GRAY2BGR)
            otsu = cv2.cvtColor(otsu_binary, cv2.COLOR_GRAY2BGR)

            for candidate in (inpainted, red_suppressed, contrast_bgr, enhanced, otsu):
                scale = 2.0 if max(candidate.shape[:2]) > 220 else 2.6
                resized = cv2.resize(candidate, None, fx=scale, fy=scale, interpolation=cv2.INTER_CUBIC)
                candidate_result = self._run_predictor(self.ocr, resized, "Seal Covered OCR")
                raw_text = self._extract_text_from_result(candidate_result, join_char="")
                cleaned = self._sanitize_recognized_text(raw_text, min_len=2, max_len=48)
                if not cleaned:
                    continue

                chinese_count = len(re.findall(r"[\u4e00-\u9fa5]", cleaned))
                digit_count = len(re.findall(r"\d", cleaned))
                if len(cleaned) > 42 and chinese_count < 4:
                    continue
                if chinese_count == 0 and len(cleaned) >= 12:
                    continue

                score = float(chinese_count * 4 + min(len(cleaned), 30) - max(0, len(cleaned) - 36))
                if digit_count >= 10 and chinese_count <= 2:
                    score -= 8.0
                if any(
                    token in cleaned
                    for token in ("公司", "盖章", "日期", "签字", "代表", "电话", "地址", "传真", "投标", "授权")
                ):
                    score += 10.0
                scored_candidates.append((score, cleaned))

        if not scored_candidates:
            return ""

        recovered_candidates: list[str] = []
        for _, text_value in sorted(scored_candidates, key=lambda item: item[0], reverse=True):
            if any(text_value == existing or text_value in existing or existing in text_value for existing in recovered_candidates):
                continue
            recovered_candidates.append(text_value)
            if len(recovered_candidates) >= 1:
                break

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

    def _merge_box_pair(self, box_a: list[int], box_b: list[int]) -> list[int]:
        ax, ay, aw, ah = box_a
        bx, by, bw, bh = box_b
        x1 = min(ax, bx)
        y1 = min(ay, by)
        x2 = max(ax + aw, bx + bw)
        y2 = max(ay + ah, by + bh)
        return [int(x1), int(y1), int(x2 - x1), int(y2 - y1)]

    def _box_intersection_area(self, box_a: list[int], box_b: list[int]) -> int:
        ax, ay, aw, ah = box_a
        bx, by, bw, bh = box_b
        x_left = max(ax, bx)
        y_top = max(ay, by)
        x_right = min(ax + aw, bx + bw)
        y_bottom = min(ay + ah, by + bh)
        if x_right <= x_left or y_bottom <= y_top:
            return 0
        return int((x_right - x_left) * (y_bottom - y_top))

    def _should_merge_seal_boxes(self, box_a: list[int], box_b: list[int]) -> bool:
        inter_area = self._box_intersection_area(box_a, box_b)
        if inter_area > 0:
            return True

        aw, ah = box_a[2], box_a[3]
        bw, bh = box_b[2], box_b[3]
        min_dim = max(8, int(min(aw, ah, bw, bh)))
        adaptive_gap = min(72, max(12, int(min_dim * 0.85)))
        if not self._is_box_close(box_a, box_b, x_gap=adaptive_gap, y_gap=adaptive_gap):
            return False

        acx, acy = box_a[0] + aw / 2.0, box_a[1] + ah / 2.0
        bcx, bcy = box_b[0] + bw / 2.0, box_b[1] + bh / 2.0
        center_distance = float(np.hypot(acx - bcx, acy - bcy))
        center_threshold = 0.95 * (max(aw, ah) + max(bw, bh))
        if center_distance > center_threshold:
            return False

        merged = self._merge_box_pair(box_a, box_b)
        merged_area = merged[2] * merged[3]
        area_sum = max(aw * ah + bw * bh, 1)
        # Prevent merging clearly separated seals.
        if merged_area > area_sum * 4.2:
            return False
        return True

    def _merge_fragmented_seal_boxes(self, boxes: list[list[int]]) -> list[list[int]]:
        if not boxes:
            return []

        merged = self._merge_nearby_boxes(boxes, x_gap=16, y_gap=16)
        changed = True
        while changed:
            changed = False
            next_boxes: list[list[int]] = []
            consumed = [False] * len(merged)
            for i, base in enumerate(merged):
                if consumed[i]:
                    continue
                current = base.copy()
                for j in range(i + 1, len(merged)):
                    if consumed[j]:
                        continue
                    if self._should_merge_seal_boxes(current, merged[j]):
                        current = self._merge_box_pair(current, merged[j])
                        consumed[j] = True
                        changed = True
                next_boxes.append(current)
            merged = next_boxes

        deduped: list[list[int]] = []
        seen = set()
        for box in merged:
            signature = tuple(int(v) for v in box)
            if signature in seen:
                continue
            seen.add(signature)
            deduped.append([int(v) for v in box])
        return deduped

    def _merge_many_boxes(self, boxes: list[list[int]]) -> list[int]:
        if not boxes:
            return [0, 0, 0, 0]
        x1 = min(int(box[0]) for box in boxes)
        y1 = min(int(box[1]) for box in boxes)
        x2 = max(int(box[0] + box[2]) for box in boxes)
        y2 = max(int(box[1] + box[3]) for box in boxes)
        return [x1, y1, max(0, x2 - x1), max(0, y2 - y1)]

    def _box_center(self, box: list[int]) -> tuple[float, float]:
        return (float(box[0]) + float(box[2]) / 2.0, float(box[1]) + float(box[3]) / 2.0)

    def _refine_seal_box_with_mask(
        self,
        box: list[int],
        full_mask: np.ndarray,
        img_shape: tuple[int, int, int],
    ) -> list[int]:
        if full_mask is None or full_mask.size == 0:
            return [int(v) for v in box]

        refined = [int(v) for v in box]
        img_h, img_w = img_shape[:2]
        if refined[2] <= 0 or refined[3] <= 0:
            return refined

        for _ in range(2):
            max_side = max(refined[2], refined[3])
            pad = max(16, min(int(max_side * 1.15), int(min(img_h, img_w) * 0.14)))
            x1, y1, x2, y2 = self._expand_box(refined, img_shape, pad=pad)
            roi = full_mask[y1:y2, x1:x2]
            if roi.size == 0:
                break

            binary = (roi > 0).astype(np.uint8)
            num_labels, _, stats, _ = cv2.connectedComponentsWithStats(binary, connectivity=8)
            if num_labels <= 1:
                break

            merge_candidates: list[list[int]] = [refined]
            proximity_gap = max(10, int(max_side * 0.75))
            base_center_x, base_center_y = self._box_center(refined)
            min_component_area = max(18, int(max_side * 0.10))

            for label in range(1, num_labels):
                area = int(stats[label, cv2.CC_STAT_AREA])
                if area < min_component_area:
                    continue

                cx = int(stats[label, cv2.CC_STAT_LEFT])
                cy = int(stats[label, cv2.CC_STAT_TOP])
                cw = int(stats[label, cv2.CC_STAT_WIDTH])
                ch = int(stats[label, cv2.CC_STAT_HEIGHT])
                if cw <= 0 or ch <= 0:
                    continue
                component_box = [x1 + cx, y1 + cy, cw, ch]
                if component_box[2] >= int(img_w * 0.95) or component_box[3] >= int(img_h * 0.95):
                    continue

                if self._box_intersection_area(refined, component_box) > 0 or self._is_box_close(
                    refined,
                    component_box,
                    x_gap=proximity_gap,
                    y_gap=proximity_gap,
                ):
                    merge_candidates.append(component_box)
                    continue

                comp_center_x, comp_center_y = self._box_center(component_box)
                center_distance = float(np.hypot(base_center_x - comp_center_x, base_center_y - comp_center_y))
                center_threshold = 1.05 * (max_side + max(component_box[2], component_box[3]))
                if center_distance <= center_threshold:
                    merge_candidates.append(component_box)

            if len(merge_candidates) <= 1:
                break

            merged_box = self._merge_many_boxes(merge_candidates)
            merged_w, merged_h = int(merged_box[2]), int(merged_box[3])
            if merged_w <= 0 or merged_h <= 0:
                break

            merged_area = float(merged_w * merged_h)
            current_area = float(max(refined[2] * refined[3], 1))
            growth_ratio = merged_area / current_area
            merged_aspect = float(merged_w) / float(max(merged_h, 1))

            if growth_ratio <= 1.08:
                break
            if growth_ratio > 8.5 and max_side >= 48:
                break
            if not 0.30 <= merged_aspect <= 3.20 and growth_ratio > 2.0:
                break

            refined = merged_box

        return [int(v) for v in refined]

    def _detect_handwritten_signatures(self, img_bgr: np.ndarray, page_no: int = 1, *, enabled: bool) -> dict:
        signature_info = {"count": 0, "texts": [], "locations": []}
        if img_bgr is None or not enabled:
            return signature_info

        img_h, img_w = img_bgr.shape[:2]
        if img_h < 20 or img_w < 20:
            return signature_info

        hsv = cv2.cvtColor(img_bgr, cv2.COLOR_BGR2HSV)
        gray = cv2.cvtColor(img_bgr, cv2.COLOR_BGR2GRAY)

        blue_mask = cv2.inRange(hsv, np.array([90, 40, 20]), np.array([140, 255, 255]))
        dark_mask = cv2.inRange(gray, 0, 130)
        stroke_mask = cv2.bitwise_or(blue_mask, dark_mask)

        roi_top = int(img_h * 0.32)
        bottom_mask = stroke_mask[roi_top:, :]
        kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (5, 3))
        bottom_mask = cv2.morphologyEx(bottom_mask, cv2.MORPH_CLOSE, kernel, iterations=1)
        bottom_mask = cv2.erode(bottom_mask, np.ones((2, 2), np.uint8), iterations=1)

        contours, _ = cv2.findContours(bottom_mask, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)

        candidate_boxes: list[list[int]] = []
        for contour in contours:
            area = cv2.contourArea(contour)
            if area < 220 or area > 18000:
                continue
            x, y, w, h = cv2.boundingRect(contour)
            aspect_ratio = float(w) / max(float(h), 1.0)
            fill_ratio = float(area) / max(float(w * h), 1.0)
            if w < 44 or h < 10:
                continue
            if not 1.1 < aspect_ratio < 15.0:
                continue
            if not 0.018 < fill_ratio < 0.68:
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

    def _detect_seals(self, img_bgr: np.ndarray, page_no: int = 1, *, enabled: bool) -> dict:
        seal_info = {"count": 0, "texts": [], "locations": [], "covered_texts": []}
        if img_bgr is None or not enabled:
            return seal_info

        img_h, img_w = img_bgr.shape[:2]
        image_area = float(max(img_h * img_w, 1))
        hsv = cv2.cvtColor(img_bgr, cv2.COLOR_BGR2HSV)
        lower_red1, upper_red1 = np.array([0, 28, 28]), np.array([12, 255, 255])
        lower_red2, upper_red2 = np.array([150, 28, 28]), np.array([180, 255, 255])
        mask = cv2.add(
            cv2.inRange(hsv, lower_red1, upper_red1),
            cv2.inRange(hsv, lower_red2, upper_red2),
        )

        kernel = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (5, 5))
        mask = cv2.medianBlur(mask, 5)
        # Keep thin circular stamp rings while still connecting fragmented red pixels.
        mask = cv2.morphologyEx(mask, cv2.MORPH_CLOSE, kernel, iterations=2)
        bridge_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (3, 9))
        mask = cv2.morphologyEx(mask, cv2.MORPH_CLOSE, bridge_kernel, iterations=1)
        mask = cv2.dilate(mask, kernel, iterations=1)
        contours, _ = cv2.findContours(mask, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        face_boxes = self._detect_faces(img_bgr)

        candidate_boxes: list[list[int]] = []
        for contour in contours:
            area = cv2.contourArea(contour)
            if area <= 400:
                continue

            area_ratio = area / image_area
            if area_ratio > 0.35:
                continue

            perimeter = cv2.arcLength(contour, True)
            if perimeter <= 0:
                continue
            circularity = float((4.0 * np.pi * area) / (perimeter * perimeter))
            if circularity < 0.16:
                continue

            x, y, w, h = cv2.boundingRect(contour)
            if w <= 0 or h <= 0:
                continue
            # Hard-stop for page-level false positives.
            if w >= int(img_w * 0.92) or h >= int(img_h * 0.92):
                continue
            if (w * h) >= int(image_area * 0.55):
                continue
            aspect_ratio = float(w) / h if h else 0.0
            if not 0.5 <= aspect_ratio <= 2.0:
                continue

            bbox_ratio = float(w * h) / image_area
            if bbox_ratio > 0.45:
                continue

            touching_border = x <= 2 or y <= 2 or (x + w) >= (img_w - 2) or (y + h) >= (img_h - 2)
            if touching_border and bbox_ratio > 0.22:
                continue

            (_, _), radius = cv2.minEnclosingCircle(contour)
            if radius < 10:
                continue
            circle_area = float(np.pi * radius * radius)
            if circle_area <= 0:
                continue
            fill_ratio = area / circle_area
            if not 0.12 <= fill_ratio <= 1.20:
                continue

            roi_mask = mask[y : y + h, x : x + w]
            red_density = float(cv2.countNonZero(roi_mask)) / float(max(w * h, 1))
            if red_density < 0.018:
                continue

            candidate_boxes.append([int(x), int(y), int(w), int(h)])

        merged_boxes = self._merge_fragmented_seal_boxes(candidate_boxes)
        refined_boxes = [
            self._refine_seal_box_with_mask(box, mask, img_bgr.shape)
            for box in merged_boxes
            if isinstance(box, list) and len(box) == 4
        ]
        merged_boxes = self._merge_fragmented_seal_boxes(refined_boxes)
        merged_boxes = sorted(merged_boxes, key=lambda item: (item[1], item[0]))

        seal_idx = 0
        for box in merged_boxes:
            x, y, w, h = box
            if w <= 0 or h <= 0:
                continue
            merged_aspect = float(w) / float(max(h, 1))
            if not 0.52 <= merged_aspect <= 1.92:
                continue
            min_side_threshold = max(24, int(min(img_h, img_w) * 0.018))
            if min(w, h) < min_side_threshold:
                continue
            bbox_ratio = float(w * h) / image_area
            if w >= int(img_w * 0.92) or h >= int(img_h * 0.92):
                continue
            if bbox_ratio > 0.55:
                continue
            touching_border = x <= 2 or y <= 2 or (x + w) >= (img_w - 2) or (y + h) >= (img_h - 2)
            if touching_border and bbox_ratio > 0.28:
                continue

            roi_mask = mask[y : y + h, x : x + w]
            merged_red_density = float(cv2.countNonZero(roi_mask)) / float(max(w * h, 1))
            if merged_red_density < 0.012:
                continue

            seal_crop = img_bgr[y : y + h, x : x + w]
            if seal_crop.size == 0:
                continue
            candidate_profile = self._build_candidate_profile(seal_crop, roi_mask)
            face_like = self._is_face_like_candidate(box, candidate_profile, face_boxes)
            emblem_like = self._is_national_emblem_like(candidate_profile)

            resized_crop = cv2.resize(seal_crop, None, fx=2.0, fy=2.0, interpolation=cv2.INTER_CUBIC)
            crop_result = self._run_predictor(self.ocr, resized_crop, "Seal OCR")
            raw_text = self._extract_text_from_result(crop_result, join_char="")
            clean_text = self._sanitize_recognized_text(raw_text, min_len=2, max_len=30)

            recovered_text = self._recover_text_from_seal_region(img_bgr, box, mask)
            has_seal_hint = self._contains_seal_hint_text(clean_text, recovered_text)
            has_strong_seal_hint = self._contains_strong_seal_hint_text(clean_text, recovered_text)
            identity_document_like = self._contains_identity_document_text(clean_text, recovered_text)
            ring_contrast = float(candidate_profile.get("ring_contrast", 0.0))
            edge_ratio = float(candidate_profile.get("edge_ratio", 0.0))
            yellow_ratio = float(candidate_profile.get("yellow_ratio", 0.0))
            green_ratio = float(candidate_profile.get("green_ratio", 0.0))
            blue_ratio = float(candidate_profile.get("blue_ratio", 0.0))
            skin_ratio = float(candidate_profile.get("skin_ratio", 0.0))
            weak_stamp_shape = ring_contrast < 0.05 and merged_red_density < 0.055
            clutter_like = edge_ratio > 0.17 and ring_contrast < 0.07
            colorful_background = (green_ratio > 0.05 or blue_ratio > 0.05) and merged_red_density < 0.16
            certificate_like = self._is_certificate_title_text(clean_text, recovered_text)
            strong_round_stamp = (
                ring_contrast >= 0.10
                and merged_red_density >= 0.045
                and 0.62 <= merged_aspect <= 1.62
                and edge_ratio <= 0.16
                and yellow_ratio <= 0.12
                and green_ratio <= 0.06
                and blue_ratio <= 0.06
            )
            strong_dense_stamp = (
                merged_red_density >= 0.18
                and edge_ratio <= 0.18
                and skin_ratio < 0.24
                and yellow_ratio <= 0.14
                and green_ratio <= 0.04
                and blue_ratio <= 0.04
            )
            round_star_stamp = self._has_round_star_stamp_evidence(
                candidate_profile,
                merged_aspect=merged_aspect,
                merged_red_density=merged_red_density,
            )
            has_visual_stamp_evidence = strong_round_stamp or strong_dense_stamp or round_star_stamp
            if (weak_stamp_shape or clutter_like) and not has_seal_hint and not has_visual_stamp_evidence:
                continue
            if colorful_background and not has_strong_seal_hint and not has_visual_stamp_evidence:
                continue
            if identity_document_like and not has_strong_seal_hint:
                continue
            if certificate_like and not has_strong_seal_hint and not has_visual_stamp_evidence:
                continue
            if not has_seal_hint and not has_visual_stamp_evidence:
                continue
            reliable_marker_text = self._is_reliable_marker_text(clean_text, recovered_text)
            small_fragment_threshold = max(36, int(min(img_h, img_w) * 0.022))
            if min(w, h) < small_fragment_threshold and not has_seal_hint and not reliable_marker_text:
                continue
            if face_like and not has_seal_hint:
                continue
            if identity_document_like and emblem_like and not has_strong_seal_hint:
                continue
            if emblem_like and not reliable_marker_text:
                continue

            seal_idx += 1
            save_path = os.path.join(self.seal_dir, f"seal_P{page_no}_{seal_idx}.png")
            cv2.imwrite(save_path, seal_crop)
            if clean_text:
                seal_info["texts"].append(clean_text)
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

    def _recognize_pdf(
        self,
        pdf_path: str,
        *,
        structure_enabled: bool,
        seal_enabled: bool,
        signature_enabled: bool,
    ) -> dict:
        doc = fitz.open(pdf_path)
        try:
            total_pages = len(doc)
        finally:
            doc.close()

        if self._resolve_pipeline_enabled(total_pages):
            return self._recognize_pdf_pipeline(
                pdf_path,
                total_pages=total_pages,
                structure_enabled=structure_enabled,
                seal_enabled=seal_enabled,
                signature_enabled=signature_enabled,
            )

        return self._recognize_pdf_serial(
            pdf_path,
            total_pages=total_pages,
            structure_enabled=structure_enabled,
            seal_enabled=seal_enabled,
            signature_enabled=signature_enabled,
        )

    def _run_pdf_stage_b(
        self,
        image: np.ndarray,
        page_no: int,
        *,
        structure_enabled: bool,
    ) -> dict:
        timings_ms = {"ocr_ms": 0.0, "layout_ms": 0.0}

        start = time.perf_counter()
        ocr_result = self._run_predictor(self.ocr, image, "PaddleOCR")
        ocr_text = self._extract_text_from_result(ocr_result, join_char="\n")
        timings_ms["ocr_ms"] = (time.perf_counter() - start) * 1000.0

        start = time.perf_counter()
        layout_text, layout_sections, structure_used = self._run_structure_layout(
            image,
            page_no,
            structure_enabled=structure_enabled,
        )
        timings_ms["layout_ms"] = (time.perf_counter() - start) * 1000.0

        return {
            "page_no": page_no,
            "image": image,
            "ocr_text": ocr_text,
            "layout_text": layout_text,
            "layout_sections": layout_sections,
            "structure_used": structure_used,
            "text_signal": bool(ocr_text.strip() or layout_text.strip()),
            "timings_ms": timings_ms,
        }

    def _run_pdf_stage_c(
        self,
        stage_b_payload: dict,
        *,
        seal_enabled: bool,
        signature_enabled: bool,
    ) -> dict:
        page_no = int(stage_b_payload.get("page_no", 0))
        image = stage_b_payload.get("image")
        ocr_text = str(stage_b_payload.get("ocr_text") or "")
        layout_text = str(stage_b_payload.get("layout_text") or "")
        layout_sections = stage_b_payload.get("layout_sections") or []
        structure_used = bool(stage_b_payload.get("structure_used"))

        timings_ms = {"seal_ms": 0.0, "signature_ms": 0.0, "merge_ms": 0.0}

        start = time.perf_counter()
        seal_result = self._detect_seals(image, page_no=page_no, enabled=seal_enabled)
        timings_ms["seal_ms"] = (time.perf_counter() - start) * 1000.0

        start = time.perf_counter()
        signature_result = self._detect_handwritten_signatures(
            image,
            page_no=page_no,
            enabled=signature_enabled,
        )
        timings_ms["signature_ms"] = (time.perf_counter() - start) * 1000.0

        start = time.perf_counter()
        page_text = self._combine_page_text(layout_text, ocr_text)
        page_text = self._remove_seal_texts(page_text, seal_result["texts"])
        page_text = self._append_recovered_texts(page_text, seal_result.get("covered_texts", []))
        timings_ms["merge_ms"] = (time.perf_counter() - start) * 1000.0

        text_signal = bool(stage_b_payload.get("text_signal"))
        return {
            "page_no": page_no,
            "page_text": page_text,
            "layout_sections": layout_sections,
            "seal_result": seal_result,
            "signature_result": signature_result,
            "structure_used": structure_used,
            "ocr_applied": bool(
                text_signal
                or seal_result["count"]
                or seal_result.get("covered_texts")
                or signature_result["count"]
            ),
            "timings_ms": timings_ms,
        }

    def _build_pdf_result_from_page_results(
        self,
        page_results: list[dict],
        *,
        structure_enabled: bool,
        seal_enabled: bool,
        signature_enabled: bool,
    ) -> dict:
        pages_data: list[dict] = []
        full_text: list[str] = []
        layout_sections: list[dict] = []
        all_seals = {"count": 0, "texts": [], "locations": [], "covered_texts": []}
        all_signatures = {"count": 0, "texts": [], "locations": []}
        ocr_applied = False
        structure_used = False

        ordered_results = sorted(page_results, key=lambda item: int(item.get("page_no", 0)))
        for page_result in ordered_results:
            page_no = int(page_result.get("page_no", 0))
            if page_no <= 0:
                continue

            page_text = str(page_result.get("page_text") or "")
            page_layout_sections = page_result.get("layout_sections") or []
            seal_result = page_result.get("seal_result") or {"count": 0, "texts": [], "locations": [], "covered_texts": []}
            signature_result = page_result.get("signature_result") or {"count": 0, "texts": [], "locations": []}

            if page_layout_sections:
                layout_sections.extend(page_layout_sections)

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

            pages_data.append({"page": page_no, "text": page_text})
            full_text.append(page_text)
            ocr_applied = ocr_applied or bool(page_result.get("ocr_applied"))
            structure_used = structure_used or bool(page_result.get("structure_used"))

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
            "structure_enabled": structure_enabled,
            "seal_recognition_enabled": seal_enabled,
            "signature_recognition_enabled": signature_enabled,
        }

    def _log_pdf_stage_metrics(
        self,
        *,
        mode: str,
        total_pages: int,
        total_ms: float,
        stage_metrics: dict[str, float],
        render_workers: int | None = None,
        post_workers: int | None = None,
        queue_size: int | None = None,
    ) -> None:
        if not self._should_log_pipeline_metrics():
            return

        avg_ms = total_ms / max(total_pages, 1)
        extra = ""
        if render_workers is not None and post_workers is not None and queue_size is not None:
            extra = f", render_workers={render_workers}, post_workers={post_workers}, queue={queue_size}"
        print(
            "OCRService: PDF OCR "
            f"mode={mode}, pages={total_pages}{extra}, total={total_ms:.1f}ms, "
            f"render={stage_metrics.get('render_ms', 0.0):.1f}ms, "
            f"ocr={stage_metrics.get('ocr_ms', 0.0):.1f}ms, "
            f"layout={stage_metrics.get('layout_ms', 0.0):.1f}ms, "
            f"seal={stage_metrics.get('seal_ms', 0.0):.1f}ms, "
            f"signature={stage_metrics.get('signature_ms', 0.0):.1f}ms, "
            f"merge={stage_metrics.get('merge_ms', 0.0):.1f}ms, "
            f"avg={avg_ms:.1f}ms/page"
        )

    def _recognize_pdf_serial(
        self,
        pdf_path: str,
        *,
        total_pages: int,
        structure_enabled: bool,
        seal_enabled: bool,
        signature_enabled: bool,
    ) -> dict:
        stage_metrics = {
            "render_ms": 0.0,
            "ocr_ms": 0.0,
            "layout_ms": 0.0,
            "seal_ms": 0.0,
            "signature_ms": 0.0,
            "merge_ms": 0.0,
        }
        page_results: list[dict] = []
        running_seal_count = 0
        running_signature_count = 0
        started_all = time.perf_counter()

        doc = fitz.open(pdf_path)
        try:
            pbar = tqdm(range(total_pages), desc="OCR processing", unit="page")
            for page_index in pbar:
                page_no = page_index + 1
                start = time.perf_counter()
                image = self._render_pdf_page(doc[page_index])
                stage_metrics["render_ms"] += (time.perf_counter() - start) * 1000.0

                stage_b_payload = self._run_pdf_stage_b(
                    image,
                    page_no,
                    structure_enabled=structure_enabled,
                )
                stage_metrics["ocr_ms"] += float(stage_b_payload["timings_ms"].get("ocr_ms", 0.0))
                stage_metrics["layout_ms"] += float(stage_b_payload["timings_ms"].get("layout_ms", 0.0))

                page_result = self._run_pdf_stage_c(
                    stage_b_payload,
                    seal_enabled=seal_enabled,
                    signature_enabled=signature_enabled,
                )
                page_results.append(page_result)

                stage_metrics["seal_ms"] += float(page_result["timings_ms"].get("seal_ms", 0.0))
                stage_metrics["signature_ms"] += float(page_result["timings_ms"].get("signature_ms", 0.0))
                stage_metrics["merge_ms"] += float(page_result["timings_ms"].get("merge_ms", 0.0))

                running_seal_count += int(page_result.get("seal_result", {}).get("count", 0))
                running_signature_count += int(page_result.get("signature_result", {}).get("count", 0))
                pbar.set_postfix({"seal_count": running_seal_count, "signature_count": running_signature_count})
        finally:
            doc.close()

        result = self._build_pdf_result_from_page_results(
            page_results,
            structure_enabled=structure_enabled,
            seal_enabled=seal_enabled,
            signature_enabled=signature_enabled,
        )
        self._log_pdf_stage_metrics(
            mode="serial",
            total_pages=total_pages,
            total_ms=(time.perf_counter() - started_all) * 1000.0,
            stage_metrics=stage_metrics,
        )
        return result

    def _recognize_pdf_pipeline(
        self,
        pdf_path: str,
        *,
        total_pages: int,
        structure_enabled: bool,
        seal_enabled: bool,
        signature_enabled: bool,
    ) -> dict:
        queue_size = self._resolve_pipeline_queue_size()
        render_workers = self._resolve_pipeline_render_workers(total_pages)
        post_workers = self._resolve_pipeline_post_workers(total_pages)

        stage_metrics = {
            "render_ms": 0.0,
            "ocr_ms": 0.0,
            "layout_ms": 0.0,
            "seal_ms": 0.0,
            "signature_ms": 0.0,
            "merge_ms": 0.0,
        }
        page_results: list[dict] = []
        running_seal_count = 0
        running_signature_count = 0

        render_queue: queue.Queue = queue.Queue(maxsize=queue_size)
        page_index_queue: queue.Queue = queue.Queue()
        for page_index in range(total_pages):
            page_index_queue.put(page_index)

        render_error: dict[str, Exception | None] = {"error": None}
        render_error_lock = threading.Lock()
        stop_event = threading.Event()

        def render_worker() -> None:
            local_doc = None
            try:
                local_doc = fitz.open(pdf_path)
                while not stop_event.is_set():
                    try:
                        page_index = page_index_queue.get_nowait()
                    except queue.Empty:
                        break

                    start = time.perf_counter()
                    image = self._render_pdf_page(local_doc[page_index])
                    render_ms = (time.perf_counter() - start) * 1000.0
                    item = {"page_index": page_index, "image": image, "render_ms": render_ms}

                    while not stop_event.is_set():
                        try:
                            render_queue.put(item, timeout=0.2)
                            break
                        except queue.Full:
                            continue
            except Exception as exc:
                with render_error_lock:
                    if render_error["error"] is None:
                        render_error["error"] = exc
                stop_event.set()
            finally:
                if local_doc is not None:
                    try:
                        local_doc.close()
                    except Exception:
                        pass
                while not stop_event.is_set():
                    try:
                        render_queue.put({"_render_done": True}, timeout=0.2)
                        break
                    except queue.Full:
                        continue

        render_threads = [
            threading.Thread(target=render_worker, name=f"ocr-render-{idx + 1}", daemon=True)
            for idx in range(render_workers)
        ]
        for thread in render_threads:
            thread.start()

        started_all = time.perf_counter()
        pending_futures: set[Any] = set()
        completed_pages = 0
        processed_stage_b = 0
        render_done_count = 0

        pbar = tqdm(total=total_pages, desc="OCR processing", unit="page")

        def collect_completed_futures(*, block: bool) -> None:
            nonlocal completed_pages, running_seal_count, running_signature_count
            if not pending_futures:
                return
            timeout = None if block else 0
            done, _ = wait(pending_futures, timeout=timeout, return_when=FIRST_COMPLETED)
            for future in done:
                pending_futures.remove(future)
                page_result = future.result()
                page_results.append(page_result)
                stage_metrics["seal_ms"] += float(page_result["timings_ms"].get("seal_ms", 0.0))
                stage_metrics["signature_ms"] += float(page_result["timings_ms"].get("signature_ms", 0.0))
                stage_metrics["merge_ms"] += float(page_result["timings_ms"].get("merge_ms", 0.0))
                running_seal_count += int(page_result.get("seal_result", {}).get("count", 0))
                running_signature_count += int(page_result.get("signature_result", {}).get("count", 0))
                completed_pages += 1
                pbar.update(1)
                pbar.set_postfix({"seal_count": running_seal_count, "signature_count": running_signature_count})

        try:
            with ThreadPoolExecutor(max_workers=post_workers, thread_name_prefix="ocr-post") as post_pool:
                while processed_stage_b < total_pages:
                    collect_completed_futures(block=False)

                    with render_error_lock:
                        render_exc = render_error["error"]
                    if render_exc is not None and render_done_count >= render_workers:
                        raise RuntimeError(f"PDF render pipeline failed: {render_exc}") from render_exc

                    try:
                        item = render_queue.get(timeout=0.2)
                    except queue.Empty:
                        if render_done_count >= render_workers and processed_stage_b < total_pages:
                            raise RuntimeError(
                                "PDF render pipeline ended early "
                                f"(processed={processed_stage_b}, total={total_pages})"
                            )
                        continue

                    if item.get("_render_done"):
                        render_done_count += 1
                        continue

                    page_no = int(item["page_index"]) + 1
                    stage_metrics["render_ms"] += float(item.get("render_ms", 0.0))

                    stage_b_payload = self._run_pdf_stage_b(
                        item["image"],
                        page_no,
                        structure_enabled=structure_enabled,
                    )
                    stage_metrics["ocr_ms"] += float(stage_b_payload["timings_ms"].get("ocr_ms", 0.0))
                    stage_metrics["layout_ms"] += float(stage_b_payload["timings_ms"].get("layout_ms", 0.0))
                    processed_stage_b += 1

                    max_pending = max(queue_size, post_workers)
                    while len(pending_futures) >= max_pending:
                        collect_completed_futures(block=True)

                    future = post_pool.submit(
                        self._run_pdf_stage_c,
                        stage_b_payload,
                        seal_enabled=seal_enabled,
                        signature_enabled=signature_enabled,
                    )
                    pending_futures.add(future)

                while pending_futures:
                    collect_completed_futures(block=True)
        finally:
            stop_event.set()
            for thread in render_threads:
                thread.join(timeout=5.0)
            pbar.close()

        if completed_pages != total_pages:
            raise RuntimeError(
                "PDF post-processing pipeline ended early "
                f"(completed={completed_pages}, total={total_pages})"
            )

        result = self._build_pdf_result_from_page_results(
            page_results,
            structure_enabled=structure_enabled,
            seal_enabled=seal_enabled,
            signature_enabled=signature_enabled,
        )
        self._log_pdf_stage_metrics(
            mode="pipeline",
            total_pages=total_pages,
            total_ms=(time.perf_counter() - started_all) * 1000.0,
            stage_metrics=stage_metrics,
            render_workers=render_workers,
            post_workers=post_workers,
            queue_size=queue_size,
        )
        return result

    def _recognize_image(
        self,
        img_path: str,
        *,
        structure_enabled: bool,
        seal_enabled: bool,
        signature_enabled: bool,
    ) -> dict:
        image = cv2.imread(img_path)
        if image is None:
            return self._empty_result()

        ocr_result = self._run_predictor(self.ocr, image, "PaddleOCR")
        ocr_text = self._extract_text_from_result(ocr_result, join_char="\n")
        layout_text, layout_sections, structure_used = self._run_structure_layout(
            image,
            page_no=1,
            structure_enabled=structure_enabled,
        )
        seal_result = self._detect_seals(image, page_no=1, enabled=seal_enabled)
        signature_result = self._detect_handwritten_signatures(
            image,
            page_no=1,
            enabled=signature_enabled,
        )

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
            "structure_enabled": structure_enabled,
            "seal_recognition_enabled": seal_enabled,
            "signature_recognition_enabled": signature_enabled,
        }
