# -*- coding: utf-8 -*-
"""Helpers for frontend document highlighting locations."""
from __future__ import annotations

import json
from typing import Any


COORDINATE_SYSTEM = "pdf_point"


def normalize_page(value: Any) -> int | None:
    """Return the first positive page number found in a scalar or container."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value > 0 else None
    if isinstance(value, float):
        return int(value) if value.is_integer() and value > 0 else None
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.isdigit():
            page = int(stripped)
            return page if page > 0 else None
        return None
    if isinstance(value, dict):
        for key in (
            "source_page",
            "page",
            "pages",
            "page_refs",
            "section_pages",
            "response_page",
            "requirement_page",
            "start_page",
        ):
            page = normalize_page(value.get(key))
            if page:
                return page
        return None
    if isinstance(value, (list, tuple, set)):
        pages = [page for item in value if (page := normalize_page(item))]
        return min(pages) if pages else None
    return None


def normalize_bbox(value: Any) -> list[float] | None:
    """Normalize common OCR bbox formats to [x0, y0, x1, y1]."""
    if value is None:
        return None

    if isinstance(value, dict):
        direct_keys = ("x0", "y0", "x1", "y1")
        if all(key in value for key in direct_keys):
            return _rect_from_values([value.get(key) for key in direct_keys])
        size_keys = ("x", "y", "width", "height")
        if all(key in value for key in size_keys):
            return _rect_from_xywh(
                value.get("x"),
                value.get("y"),
                value.get("width"),
                value.get("height"),
            )
        for key in ("bbox", "bbox_ocr", "box", "rect"):
            nested = normalize_bbox(value.get(key))
            if nested:
                return nested
        return None

    if not isinstance(value, (list, tuple)):
        return None

    if len(value) >= 4 and all(isinstance(item, (int, float)) for item in value[:4]):
        x0, y0, third, fourth = [float(item) for item in value[:4]]
        if third >= x0 and fourth >= y0:
            return _clean_rect([x0, y0, third, fourth])
        if third >= 0 and fourth >= 0:
            return _clean_rect([x0, y0, x0 + third, y0 + fourth])
        return _clean_rect([min(x0, third), min(y0, fourth), max(x0, third), max(y0, fourth)])

    if value and all(
        isinstance(item, (list, tuple))
        and len(item) >= 2
        and all(isinstance(part, (int, float)) for part in item[:2])
        for item in value
    ):
        xs = [float(item[0]) for item in value]
        ys = [float(item[1]) for item in value]
        return _clean_rect([min(xs), min(ys), max(xs), max(ys)])

    return None


def make_location(
    *,
    document_identifier_id: Any = None,
    file_name: Any = None,
    document_role: Any = None,
    document_type: Any = None,
    page: Any = None,
    bbox: Any = None,
    text: Any = None,
    coordinate_system: str = COORDINATE_SYSTEM,
) -> dict[str, Any] | None:
    """Build a standard frontend location object."""
    doc_id = str(document_identifier_id or "").strip()
    file = str(file_name or "").strip()
    role = str(document_role or "").strip()
    doc_type = str(document_type or "").strip()
    page_number = normalize_page(page)
    normalized_bbox = normalize_bbox(bbox)
    text_value = _normalize_text(text)
    if not any((doc_id, file, role, doc_type, page_number, normalized_bbox, text_value)):
        return None
    location = {
        "document_identifier_id": doc_id,
        "file_name": file,
        "page": page_number,
        "bbox": normalized_bbox,
        "text": text_value,
        "coordinate_system": coordinate_system,
    }
    if role:
        location["document_role"] = role
    if doc_type:
        location["document_type"] = doc_type
    return location


def normalize_locations(
    locations: Any,
    *,
    defaults: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Normalize a list of locations and fill missing document defaults."""
    normalized: list[dict[str, Any]] = []
    if isinstance(locations, dict):
        raw_locations = [locations]
    elif isinstance(locations, list):
        raw_locations = locations
    else:
        raw_locations = []

    defaults = defaults or {}
    for item in raw_locations:
        if not isinstance(item, dict):
            continue
        location = make_location(
            document_identifier_id=(
                item.get("document_identifier_id")
                or item.get("document_id")
                or item.get("identifier_id")
                or defaults.get("document_identifier_id")
            ),
            file_name=item.get("file_name") or item.get("document_name") or defaults.get("file_name"),
            document_role=(
                item.get("document_role")
                or item.get("document")
                or item.get("role")
                or defaults.get("document_role")
                or defaults.get("role")
            ),
            document_type=item.get("document_type") or defaults.get("document_type"),
            page=(
                item.get("source_page")
                or item.get("page")
                or item.get("pages")
                or defaults.get("page")
            ),
            bbox=item.get("bbox") or item.get("bbox_ocr") or item.get("box") or defaults.get("bbox"),
            text=(
                item.get("text")
                or item.get("matched_text")
                or item.get("preview")
                or item.get("label")
                or defaults.get("text")
            ),
            coordinate_system=str(item.get("coordinate_system") or defaults.get("coordinate_system") or COORDINATE_SYSTEM),
        )
        append_location(normalized, location)
    return normalized


def collect_locations(*values: Any) -> list[dict[str, Any]]:
    """Collect standard locations from arbitrary nested values."""
    collected: list[dict[str, Any]] = []

    def visit(value: Any) -> None:
        if isinstance(value, list):
            for item in value:
                visit(item)
            return
        if not isinstance(value, dict):
            return

        nested_locations = normalize_locations(value.get("locations"))
        for location in nested_locations:
            append_location(collected, location)
        if nested_locations:
            return

        direct = make_location(
            document_identifier_id=(
                value.get("document_identifier_id")
                or value.get("document_id")
                or value.get("identifier_id")
            ),
            file_name=value.get("file_name") or value.get("document_name"),
            document_role=value.get("document_role") or value.get("document") or value.get("role"),
            document_type=value.get("document_type"),
            page=value.get("source_page") or value.get("page") or value.get("pages"),
            bbox=value.get("bbox") or value.get("bbox_ocr") or value.get("box"),
            text=(
                value.get("matched_text")
                or value.get("text")
                or value.get("preview")
                or value.get("label")
                or value.get("left_preview")
                or value.get("right_preview")
            ),
        )
        append_location(collected, direct)

    for raw in values:
        visit(raw)
    return collected


def append_location(locations: list[dict[str, Any]], location: dict[str, Any] | None) -> None:
    """Append a location only once."""
    if not location:
        return
    key = json.dumps(location, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    for existing in locations:
        existing_key = json.dumps(existing, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        if existing_key == key:
            return
    locations.append(location)


def _rect_from_values(values: list[Any]) -> list[float] | None:
    try:
        return _clean_rect([float(item) for item in values[:4]])
    except (TypeError, ValueError):
        return None


def _rect_from_xywh(x: Any, y: Any, width: Any, height: Any) -> list[float] | None:
    try:
        x0 = float(x)
        y0 = float(y)
        w = float(width)
        h = float(height)
    except (TypeError, ValueError):
        return None
    return _clean_rect([x0, y0, x0 + w, y0 + h])


def _clean_rect(values: list[float]) -> list[float] | None:
    if len(values) < 4:
        return None
    x0, y0, x1, y1 = values[:4]
    if x1 < x0:
        x0, x1 = x1, x0
    if y1 < y0:
        y0, y1 = y1, y0
    if x1 <= x0 or y1 <= y0:
        return None
    return [round(float(x0), 2), round(float(y0), 2), round(float(x1), 2), round(float(y1), 2)]


def _normalize_text(value: Any) -> str:
    text = "" if value is None else str(value)
    return " ".join(text.split())
