"""Review-time signature-presence evidence from source PDF pixels.

This module never performs text OCR and never mutates stored OCR payloads.
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
import threading
import time
from pathlib import Path
from typing import Any

from app.config.settings import settings
from .compliance.template_pdf_evidence import _page_lock, source_pdf_bytes


EVIDENCE_KEY = "_signature_presence_evidence"
VERSION = "signature-presence-v4"
_model_lock = threading.Lock()
_model_bundle: tuple[Any, Any, str] | None = None
logger = logging.getLogger(__name__)

_SIGNATURE_MARKERS = ("签字或盖章", "签字", "签章", "签名", "手签")
_ROLE_MARKERS = ("法定代表人", "授权代表", "授权委托人", "委托代理人", "被授权人", "代表人", "项目经理")
_PLACEHOLDERS = {"", "已签字", "已盖章", "已签章", "未识别", "无法解析识别内容"}


def _container(payload: dict[str, Any]) -> dict[str, Any]:
    data = payload.get("data")
    return data if isinstance(data, dict) else payload


def _xyxy(section: dict[str, Any]) -> list[float] | None:
    raw = section.get("bbox") or section.get("box")
    if not isinstance(raw, (list, tuple)) or len(raw) < 4:
        return None
    try:
        a, b, c, d = (float(value) for value in raw[:4])
    except (TypeError, ValueError):
        return None
    if section.get("bbox_format") == "xywh":
        return [a, b, a + c, b + d]
    return [min(a, c), min(b, d), max(a, c), max(b, d)]


def _inline_signature_value(text: Any) -> str | None:
    value = str(text or "").strip()
    if not value or not any(marker in value for marker in _SIGNATURE_MARKERS):
        return None
    tail = re.split(r"[：:]", value)[-1] if ("：" in value or ":" in value) else re.split(
        r"(?:签字或盖章|签字|签章|签名|手签)", value, maxsplit=1
    )[-1]
    tail = re.sub(r"(?:签字或盖章|签字|签章|签名|手签|盖章)", "", tail)
    tail = re.sub(r"[（）()【】\[\]_:：,，;；.。\-—/\\\s_]+", "", tail)
    if tail in _PLACEHOLDERS or re.search(r"\d", tail):
        return None
    if re.fullmatch(r"[\u4e00-\u9fa5]", tail) and tail not in set("签字章盖名无空年月日"):
        return tail
    if re.fullmatch(r"[\u4e00-\u9fa5]{2,6}|[A-Za-z]{2,20}", tail):
        return tail
    return None


def _value_region_left(field: dict[str, Any]) -> float | None:
    """Estimate where filled content begins, excluding the printed field label."""
    text = str(field.get("field_text") or "").strip()
    box = field.get("field_box") or []
    if len(box) < 4:
        return None
    if not re.search(r"[：:]", text):
        # A table heading such as “法定代表人签名” is not itself a bounded
        # fill-in region and must not be sent to the handwriting detector.
        return None
    label = re.split(r"[：:]", text, maxsplit=1)[0] + "："
    compact_label = re.sub(r"\s+", "", label)
    x0, y0, x1, y1 = (float(value) for value in box[:4])
    character_width = min(12.5, max(8.0, (y1 - y0) * 0.62))
    estimated = x0 + len(compact_label) * character_width + 4.0
    return min(max(estimated, x0), max(x1 - 20.0, x0))


def _field_regions(payload: dict[str, Any]) -> list[dict[str, Any]]:
    data = _container(payload)
    top_space = str(data.get("bbox_coordinate_space") or "").lower()
    result: list[dict[str, Any]] = []
    seen: set[tuple[int, tuple[int, ...]]] = set()
    for section in data.get("layout_sections") or []:
        if not isinstance(section, dict):
            continue
        text = str(section.get("text") or section.get("raw_text") or "").strip()
        if not text or not any(marker in text for marker in _SIGNATURE_MARKERS):
            continue
        if not any(role in text for role in _ROLE_MARKERS):
            continue
        if _inline_signature_value(text) is not None:
            continue
        page = section.get("page")
        space = str(section.get("coordinate_system") or top_space).lower()
        box = _xyxy(section)
        if not isinstance(page, int) or box is None or "pdf" not in space:
            continue
        key = (page, tuple(round(value) for value in box))
        if key in seen:
            continue
        seen.add(key)
        result.append({"page": page, "field_box": box, "field_text": text})
    return result


def _model() -> tuple[Any, Any, str]:
    global _model_bundle
    with _model_lock:
        if _model_bundle is not None:
            return _model_bundle
        model_path = Path(settings.SIGNATURE_PRESENCE_MODEL_PATH)
        if not model_path.exists():
            raise FileNotFoundError(f"签名检测模型未预置：{model_path}")
        from transformers import AutoImageProcessor, AutoModelForObjectDetection
        processor = AutoImageProcessor.from_pretrained(str(model_path), local_files_only=True)
        model = AutoModelForObjectDetection.from_pretrained(str(model_path), local_files_only=True)
        device = str(settings.SIGNATURE_PRESENCE_DEVICE or "cpu")
        model.to(device)
        model.eval()
        digest = hashlib.sha256()
        for path in sorted(model_path.rglob("*")):
            if path.is_file() and path.name in {"config.json", "model.safetensors", "pytorch_model.bin"}:
                digest.update(path.name.encode())
                with path.open("rb") as handle:
                    for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                        digest.update(chunk)
        model_id = f"{model_path.name}:{digest.hexdigest()[:16]}"
        _model_bundle = (processor, model, model_id)
        return _model_bundle


def _detect_crop(image: Any, threshold: float) -> tuple[list[dict[str, Any]], str]:
    import torch
    processor, model, model_id = _model()
    device = str(settings.SIGNATURE_PRESENCE_DEVICE or "cpu")
    inputs = processor(images=image, return_tensors="pt")
    inputs = {key: value.to(device) for key, value in inputs.items()}
    with torch.inference_mode():
        outputs = model(**inputs)
    target = torch.tensor([[image.height, image.width]], device=outputs.logits.device)
    parsed = processor.post_process_object_detection(
        outputs, threshold=threshold, target_sizes=target
    )[0]
    detections: list[dict[str, Any]] = []
    labels = getattr(model.config, "id2label", {}) or {}
    for score, label, box in zip(parsed["scores"], parsed["labels"], parsed["boxes"]):
        label_text = str(labels.get(int(label), label)).lower()
        if "signature" not in label_text:
            continue
        detections.append({
            "confidence": round(float(score), 6),
            "crop_box": [round(float(value), 3) for value in box.tolist()],
        })
    return detections, model_id


def _suppress_red_seal(image: Any) -> Any:
    """Remove red stamp pixels before signature detection.

    The detector must establish black handwriting presence; a company seal may
    overlap the field but must never become personal-signature evidence.
    """
    import numpy as np
    from PIL import Image

    pixels = np.asarray(image).copy()
    red = (
        (pixels[:, :, 0] > 100)
        & ((pixels[:, :, 0].astype(int) - pixels[:, :, 1].astype(int)) > 24)
        & ((pixels[:, :, 0].astype(int) - pixels[:, :, 2].astype(int)) > 24)
    )
    pixels[red] = 255
    return Image.fromarray(pixels)


def _cache_root() -> Path:
    return Path(settings.BUSINESS_REVIEW_EVIDENCE_CACHE_ROOT) / "signature-presence"


def signature_presence_for(
    payload: dict[str, Any],
    required_fields: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    if required_fields is None:
        fields = _field_regions(payload)
    else:
        fields = [
            dict(field)
            for field in required_fields
            if isinstance(field, dict)
            and isinstance(field.get("page"), int)
            and isinstance(field.get("field_box"), list)
            and len(field["field_box"]) >= 4
            and _inline_signature_value(field.get("field_text")) is None
        ]
    fields = [field for field in fields if _value_region_left(field) is not None]
    evidence: dict[str, Any] = {
        "version": VERSION,
        "fields": [],
        "model_available": False,
        "processing": "red_seal_suppression_then_signature_object_detection",
    }
    if not settings.SIGNATURE_PRESENCE_ENABLED or not fields:
        return evidence
    started = time.monotonic()
    try:
        pdf_data = source_pdf_bytes(payload)
    except Exception as exc:
        evidence["error"] = "source_pdf_unavailable"
        logger.warning("signature presence source unavailable: %s", exc)
        return evidence
    content_digest = hashlib.sha256(pdf_data).hexdigest()
    try:
        _, _, model_id = _model()
        evidence["model_available"] = True
        evidence["model"] = model_id
    except Exception as exc:
        evidence["error"] = "model_unavailable"
        logger.warning("signature presence model unavailable: %s", exc)
        return evidence

    import fitz
    from PIL import Image
    cache_root = _cache_root()
    cache_root.mkdir(parents=True, exist_ok=True)
    dpi = int(settings.SIGNATURE_PRESENCE_RENDER_DPI)
    scale = dpi / 72.0
    threshold = float(settings.SIGNATURE_PRESENCE_THRESHOLD)
    deadline = started + int(settings.SIGNATURE_PRESENCE_TIMEOUT_SECONDS)
    with fitz.open(stream=pdf_data, filetype="pdf") as pdf:
        for field in fields:
            if time.monotonic() >= deadline:
                evidence["fields"].append({**field, "status": "unclear", "reason_code": "signature_detection_budget_exceeded"})
                continue
            page_number = field["page"]
            if not 1 <= page_number <= len(pdf):
                evidence["fields"].append({**field, "status": "unclear", "reason_code": "signature_page_out_of_range"})
                continue
            cache_payload = {
                "version": VERSION, "digest": content_digest, "page": page_number,
                "field_box": field["field_box"], "model": model_id, "dpi": dpi,
                "threshold": threshold,
            }
            cache_key = hashlib.sha256(json.dumps(cache_payload, sort_keys=True).encode()).hexdigest()
            cache_path = cache_root / f"{cache_key}.json"
            lock_path = cache_root / f"{cache_key}.lock"
            with _page_lock(lock_path):
                if cache_path.exists():
                    try:
                        evidence["fields"].append(json.loads(cache_path.read_text(encoding="utf-8")))
                        continue
                    except (OSError, ValueError, TypeError):
                        pass
                page = pdf[page_number - 1]
                x0, y0, x1, y1 = field["field_box"]
                value_left = _value_region_left(field)
                if value_left is None:
                    continue
                final: dict[str, Any] | None = None
                for padding_x, padding_y in ((18.0, 14.0), (48.0, 32.0)):
                    clip = fitz.Rect(
                        max(0.0, value_left - 2.0), max(0.0, y0 - padding_y),
                        min(page.rect.width, x1 + padding_x), min(page.rect.height, y1 + padding_y),
                    )
                    pix = page.get_pixmap(matrix=fitz.Matrix(scale, scale), clip=clip, alpha=False)
                    image = Image.frombytes("RGB", (pix.width, pix.height), pix.samples)
                    try:
                        detections, _ = _detect_crop(_suppress_red_seal(image), threshold)
                    except Exception as exc:
                        logger.warning("signature presence inference failed page=%s: %s", page_number, exc)
                        final = {**field, "status": "unclear", "reason_code": "signature_detector_failed", "model": model_id}
                        break
                    projected: list[dict[str, Any]] = []
                    for item in detections:
                        bx0, by0, bx1, by1 = item["crop_box"]
                        touches_crop_edge = min(bx0, by0, image.width - bx1, image.height - by1) <= 3
                        pdf_box = [
                            clip.x0 + bx0 / scale,
                            clip.y0 + by0 / scale,
                            max((bx1 - bx0) / scale, 0.0),
                            max((by1 - by0) / scale, 0.0),
                        ]
                        # A padded crop may contain handwriting from an adjacent
                        # role row.  Keep only detections whose centre belongs to
                        # the original OCR field.  This makes every image result
                        # field-scoped and prevents one signature from satisfying
                        # two required roles on the same page.
                        centre_x = pdf_box[0] + pdf_box[2] / 2
                        centre_y = pdf_box[1] + pdf_box[3] / 2
                        if not (
                            value_left - 4.0 <= centre_x <= x1 + 18.0
                            and y0 - 10.0 <= centre_y <= y1 + 10.0
                        ):
                            continue
                        # Reject detections that span several neighbouring rows.
                        # DETR can otherwise return a large box around a seal and
                        # both role lines, which is not evidence for this field.
                        if pdf_box[3] > max((y1 - y0) * 2.5, 56.0):
                            continue
                        projected.append({
                            "page": page_number,
                            "box": [round(value, 3) for value in pdf_box],
                            "confidence": item["confidence"],
                            "source": "signature_image_detector",
                            "model": model_id,
                            "field_box": [
                                round(x0, 3), round(y0, 3),
                                round(max(x1 - x0, 0.0), 3),
                                round(max(y1 - y0, 0.0), 3),
                            ],
                            "coordinate_system": "pdf_points",
                            "processing": "red_seal_suppressed",
                            "_crop_edge": touches_crop_edge,
                        })
                    if projected:
                        projected = [max(projected, key=lambda value: value["confidence"])]
                    clipped = bool(projected and projected[0].pop("_crop_edge", False))
                    if not projected and padding_x < 48.0:
                        continue
                    if projected and clipped and padding_x < 48.0:
                        continue
                    if projected and clipped:
                        final = {**field, "status": "unclear", "reason_code": "signature_crop_truncated", "detections": projected, "model": model_id}
                    elif projected:
                        final = {**field, "status": "detected", "reason_code": "signature_image_evidence_confirmed", "detections": projected, "model": model_id}
                    else:
                        final = {**field, "status": "absent", "reason_code": "signature_not_detected", "detections": [], "model": model_id}
                    break
                if final is None:
                    final = {**field, "status": "unclear", "reason_code": "signature_detector_failed", "model": model_id}
                if final["status"] in {"detected", "absent"}:
                    temporary = cache_path.with_suffix(f".{threading.get_ident()}.tmp")
                    temporary.write_text(json.dumps(final, ensure_ascii=False), encoding="utf-8")
                    temporary.replace(cache_path)
                evidence["fields"].append(final)
    evidence["elapsed_seconds"] = round(time.monotonic() - started, 3)
    return evidence
