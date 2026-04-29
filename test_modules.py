import hashlib
import html
import json
import os
import re
from pathlib import Path
from typing import Any

from app.core.document_types import (
    DOCUMENT_TYPE_BUSINESS_BID,
    DOCUMENT_TYPE_TENDER,
    DOCUMENT_TYPE_TECHNICAL_BID,
)
from app.service.analysis.bid_document_review import BidDocumentReviewService
from app.service.analysis.consistency import ConsistencyChecker, DocumentProcessor
from app.service.analysis.deviation import DeviationChecker
from app.service.analysis.duplicate_check import DuplicateCheckService
from app.service.analysis.integrity import IntegrityChecker
from app.service.analysis.itemized_pricing import ItemizedPricingChecker
from app.service.analysis.pricing_reasonableness import ReasonablenessChecker
from app.service.analysis.template_extractor import TemplateExtractor
from app.service.analysis.verification import VerificationChecker
from app.service.analysis.visualizer import ReportVisualizer


SAMPLE_PRESETS: dict[str, dict[str, Any]] = {
    "export_tax": {
        "tender": Path("./ocr_results/出口退税/招标文件.json"),
        "business": [
            Path("./ocr_results/出口退税/征盛商务标.json"),
            Path("./ocr_results/出口退税/智税商务标.json"),
            Path("./ocr_results/出口退税/链坤商务标.json"),
        ],
        "technical": [],
        "output": Path("./test_reports/出口退税"),
        "source_roots": [
            Path(r"D:\Desktop\测试文件\出口退税"),
        ],
    },
    "medicine": {
        "tender": Path("./ocr_results/药品JSON识别结果/招标.JSON"),
        "business": [
            Path("./ocr_results/药品JSON识别结果/宏银商务标.JSON"),
            Path("./ocr_results/药品JSON识别结果/戎元商务标.JSON"),
            Path("./ocr_results/药品JSON识别结果/舒源商务标.JSON"),
        ],
        "technical": [
            Path("./ocr_results/药品JSON识别结果/宏银技术标.JSON"),
            Path("./ocr_results/药品JSON识别结果/戎元技术标.JSON"),
            Path("./ocr_results/药品JSON识别结果/舒源技术标.JSON"),
        ],
        "output": Path("./test_reports/药品JSON识别结果"),
        "source_roots": [
            Path(r"D:\Desktop\测试文件\药品"),
        ],
    },
}


def _load_sample_config() -> dict[str, Any]:
    sample_key = os.getenv("XTJS_SAMPLE", "export_tax").strip().lower() or "export_tax"
    return SAMPLE_PRESETS.get(sample_key, SAMPLE_PRESETS["export_tax"])


_SAMPLE_CONFIG = _load_sample_config()
TENDER_PATH: Path = _SAMPLE_CONFIG["tender"]
BUSINESS_BID_FILES: list[Path] = list(_SAMPLE_CONFIG["business"])
TECHNICAL_BID_FILES: list[Path] = list(_SAMPLE_CONFIG["technical"])
OUTPUT_DIR: Path = _SAMPLE_CONFIG["output"]
SOURCE_SEARCH_ROOTS = [
    Path("./ocr_results"),
    Path("./source_documents"),
    Path("./uploads"),
    Path("./data"),
    *[Path(item) for item in _SAMPLE_CONFIG.get("source_roots", [])],
]
SOURCE_SEARCH_EXCLUDE_DIRS = {
    ".git",
    ".venv",
    ".uv-cache",
    ".ocr_runtime",
    ".ocr_runtime_bench",
    "ocr_runtime_probe",
    "test_reports",
    "__pycache__",
}
_SOURCE_FILE_INDEX: dict[str, list[Path]] | None = None
_SOURCE_PAGE_COUNT_CACHE: dict[str, int | None] = {}


def load_json_file(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as file:
        return json.load(file)


def unwrap_document_payload(payload: dict[str, Any]) -> dict[str, Any]:
    data = payload.get("data")
    return data if isinstance(data, dict) else payload


def _derive_source_tokens(json_path: Path) -> list[str]:
    stem = json_path.stem
    tokens = [stem]
    for suffix in ["商务标", "技术标", "投标文件", "投标", "商务", "技术", "招标文件", "招标"]:
        if stem.endswith(suffix):
            trimmed = stem[: -len(suffix)].strip()
            if trimmed:
                tokens.append(trimmed)
    normalized: list[str] = []
    seen: set[str] = set()
    for token in tokens:
        token = str(token or "").strip()
        if not token:
            continue
        key = token.lower()
        if key in seen:
            continue
        seen.add(key)
        normalized.append(token)
    return normalized


def _iter_source_search_roots() -> list[Path]:
    roots: list[Path] = []
    env_value = os.getenv("XTJS_SOURCE_ROOTS", "").strip()
    for item in env_value.split(";"):
        item = item.strip()
        if not item:
            continue
        roots.append(Path(item))
    roots.extend(SOURCE_SEARCH_ROOTS)
    roots.append(Path("."))

    normalized: list[Path] = []
    seen: set[str] = set()
    for root in roots:
        try:
            resolved = root.resolve()
        except Exception:
            continue
        key = str(resolved).lower()
        if key in seen or not resolved.exists() or not resolved.is_dir():
            continue
        seen.add(key)
        normalized.append(resolved)
    return normalized


def _build_source_file_index() -> dict[str, list[Path]]:
    global _SOURCE_FILE_INDEX
    if _SOURCE_FILE_INDEX is not None:
        return _SOURCE_FILE_INDEX

    index: dict[str, list[Path]] = {}
    for root in _iter_source_search_roots():
        for current_root, dir_names, file_names in os.walk(root):
            dir_names[:] = [
                name for name in dir_names
                if name not in SOURCE_SEARCH_EXCLUDE_DIRS and not name.startswith(".")
            ]
            for file_name in file_names:
                suffix = Path(file_name).suffix.lower()
                if suffix not in {".pdf", ".png", ".jpg", ".jpeg"}:
                    continue
                path = Path(current_root) / file_name
                index.setdefault(file_name.lower(), []).append(path)
    _SOURCE_FILE_INDEX = index
    return index


def _source_page_count(path: Path) -> int | None:
    cache_key = str(path.resolve()).lower()
    if cache_key in _SOURCE_PAGE_COUNT_CACHE:
        return _SOURCE_PAGE_COUNT_CACHE[cache_key]

    page_count: int | None = None
    suffix = path.suffix.lower()
    try:
        if suffix == ".pdf":
            import fitz

            pdf = fitz.open(path)
            try:
                page_count = int(pdf.page_count)
            finally:
                pdf.close()
        elif suffix in {".png", ".jpg", ".jpeg"}:
            page_count = 1
    except Exception:
        page_count = None

    _SOURCE_PAGE_COUNT_CACHE[cache_key] = page_count
    return page_count


def _pick_best_source_match(json_path: Path, payload: dict[str, Any], matches: list[Path]) -> Path | None:
    if not matches:
        return None
    if len(matches) == 1:
        return matches[0]

    document = unwrap_document_payload(payload)
    expected_pages = document.get("page_count") if isinstance(document.get("page_count"), int) else None
    filename = str(document.get("filename") or "").strip()
    expected_name = Path(filename).name.lower() if filename else ""
    tokens = _derive_source_tokens(json_path)

    ranked: list[tuple[int, int, int, str, Path]] = []
    for path in matches:
        resolved = path.resolve()
        path_text = resolved.as_posix().lower()
        parent_name = resolved.parent.name.lower()
        score = 0
        page_match = 0

        if expected_name and resolved.name.lower() == expected_name:
            score += 20
        for token in tokens:
            token_lower = token.lower()
            if token_lower == parent_name:
                score += 60
            elif token_lower in path_text:
                score += 30

        actual_pages = _source_page_count(resolved)
        if expected_pages is not None and actual_pages == expected_pages:
            score += 80
            page_match = 1

        ranked.append((score, page_match, -len(path_text), path_text, resolved))

    ranked.sort(reverse=True)
    return ranked[0][-1]


def find_source_document(json_path: Path, payload: dict[str, Any]) -> Path | None:
    document = unwrap_document_payload(payload)
    candidates: list[Path] = []

    filename = str(document.get("filename") or "").strip()
    if filename:
        filename_path = Path(filename)
        if filename_path.is_absolute() and filename_path.exists():
            return filename_path
        candidates.append(json_path.parent / filename_path.name)
        stem = filename_path.stem
    else:
        stem = json_path.stem

    for ext in [".pdf", ".PDF", ".png", ".PNG", ".jpg", ".JPG", ".jpeg", ".JPEG"]:
        candidates.append(json_path.parent / f"{stem}{ext}")

    found_matches: list[Path] = []
    seen_matches: set[str] = set()

    def push_match(path: Path | None) -> None:
        if path is None:
            return
        try:
            resolved = path.resolve()
        except Exception:
            resolved = path
        key = str(resolved).lower()
        if key in seen_matches:
            return
        seen_matches.add(key)
        found_matches.append(resolved)

    sibling_map = {path.name.lower(): path for path in json_path.parent.iterdir() if path.is_file()}
    for candidate in candidates:
        if candidate.exists():
            push_match(candidate)
        matched = sibling_map.get(candidate.name.lower())
        if matched is not None:
            push_match(matched)

    source_index = _build_source_file_index()
    for candidate in candidates:
        matches = source_index.get(candidate.name.lower()) or []
        for match in matches:
            push_match(match)

    return _pick_best_source_match(json_path, payload, found_matches)


def _load_preview_font():
    from PIL import ImageFont

    font_candidates = [
        "C:/Windows/Fonts/msyh.ttc",
        "C:/Windows/Fonts/msyhbd.ttc",
        "C:/Windows/Fonts/simhei.ttf",
    ]
    for candidate in font_candidates:
        path = Path(candidate)
        if not path.exists():
            continue
        try:
            return ImageFont.truetype(str(path), 16)
        except Exception:
            continue
    return ImageFont.load_default()


def build_synthetic_preview_assets(
    payload: dict[str, Any],
    output_dir: Path,
    *,
    prefix: str,
    title: str,
) -> dict[str, Any]:
    from PIL import Image, ImageDraw

    document = unwrap_document_payload(payload)
    output_dir.mkdir(parents=True, exist_ok=True)
    page_count = int(document.get("page_count") or 1)
    layout_sections = document.get("layout_sections") or []
    native_tables = document.get("native_tables") or []
    seal_locations = ((document.get("seal") or {}).get("locations") or [])
    signature_locations = ((document.get("signature") or {}).get("locations") or [])
    font = _load_preview_font()

    page_items: dict[int, list[dict[str, Any]]] = {page: [] for page in range(1, page_count + 1)}

    def add_item(page: int | None, bbox: Any, kind: str, label: str) -> None:
        if not isinstance(page, int):
            return
        if not isinstance(bbox, (list, tuple)) or len(bbox) < 4:
            return
        coords = []
        for item in bbox[:4]:
            if not isinstance(item, (int, float)):
                return
            coords.append(int(round(float(item))))
        x0, y0, x1, y1 = coords
        if x1 < x0 or y1 < y0:
            if x1 > 0 and y1 > 0:
                x1 = x0 + x1
                y1 = y0 + y1
        coords = [min(x0, x1), min(y0, y1), max(x0, x1), max(y0, y1)]
        page_items.setdefault(page, []).append(
            {
                "bbox": coords,
                "kind": kind,
                "label": label,
            }
        )

    for index, section in enumerate(layout_sections, start=1):
        add_item(
            section.get("page"),
            section.get("bbox") or section.get("bbox_ocr"),
            str(section.get("type") or "text"),
            f"S{index}",
        )
    for index, table in enumerate(native_tables, start=1):
        add_item(
            table.get("page"),
            table.get("block_bbox"),
            "table",
            f"T{index}",
        )
    for index, location in enumerate(seal_locations, start=1):
        add_item(location.get("page"), location.get("box") or location.get("bbox"), "seal", f"Seal {index}")
    for index, location in enumerate(signature_locations, start=1):
        add_item(location.get("page"), location.get("box") or location.get("bbox"), "signature", f"Sign {index}")

    color_map = {
        "heading": ((58, 145, 255), (58, 145, 255, 36)),
        "table": ((37, 99, 235), (37, 99, 235, 28)),
        "seal": ((220, 38, 38), (220, 38, 38, 34)),
        "signature": ((245, 158, 11), (245, 158, 11, 34)),
        "text": ((107, 114, 128), (107, 114, 128, 18)),
    }

    pages: dict[str, Any] = {}
    for page in range(1, page_count + 1):
        items = page_items.get(page) or []
        max_x = max([item["bbox"][2] for item in items], default=560)
        max_y = max([item["bbox"][3] for item in items], default=820)
        width = max(620, min(max_x + 40, 1800))
        height = max(880, min(max_y + 40, 2400))

        image = Image.new("RGBA", (width, height), (255, 255, 255, 255))
        draw = ImageDraw.Draw(image, "RGBA")
        draw.rectangle((8, 8, width - 8, height - 8), outline=(210, 214, 220, 255), width=2)
        draw.text((18, 16), f"{title} P{page}", fill=(55, 65, 81, 255), font=font)

        for item in items:
            bbox = item["bbox"]
            kind = item["kind"]
            outline, fill = color_map.get(kind, color_map["text"])
            draw.rectangle(tuple(bbox), outline=outline + (255,), fill=fill, width=2)
            label = item["label"]
            label_y = bbox[1] - 18 if bbox[1] > 24 else bbox[1] + 4
            draw.text((bbox[0] + 4, label_y), label, fill=outline + (255,), font=font)

        image_path = output_dir / f"page_{page}.png"
        image.convert("RGB").save(image_path)
        pages[str(page)] = {
            "image_url": image_path.resolve().as_uri(),
            "width": width,
            "height": height,
        }

    return {
        "title": title,
        "source_kind": "synthetic",
        "source_missing": True,
        "pages": pages,
    }


def build_file_preview_assets(
    payload: dict[str, Any],
    json_path: Path,
    output_dir: Path,
    *,
    prefix: str,
    title: str,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    source_path = find_source_document(json_path, payload)
    if source_path is None:
        return build_synthetic_preview_assets(payload, output_dir, prefix=prefix, title=title)

    suffix = source_path.suffix.lower()
    if suffix == ".pdf":
        try:
            import fitz

            pdf = fitz.open(source_path)
            pages: dict[str, Any] = {}
            try:
                for page_index in range(pdf.page_count):
                    page = pdf.load_page(page_index)
                    rect = page.rect
                    pix = page.get_pixmap(matrix=fitz.Matrix(1.8, 1.8), alpha=False)
                    image_path = output_dir / f"page_{page_index + 1}.png"
                    pix.save(str(image_path))
                    pages[str(page_index + 1)] = {
                        "image_url": image_path.resolve().as_uri(),
                        "width": float(rect.width),
                        "height": float(rect.height),
                    }
            finally:
                pdf.close()
            return {
                "title": title,
                "source_kind": "pdf",
                "source_file": str(source_path),
                "source_url": source_path.resolve().as_uri(),
                "pages": pages,
            }
        except Exception:
            return build_synthetic_preview_assets(payload, output_dir, prefix=prefix, title=title)

    if suffix in {".png", ".jpg", ".jpeg"}:
        from PIL import Image

        with Image.open(source_path) as image:
            rgb_image = image.convert("RGB")
            image_path = output_dir / "page_1.png"
            rgb_image.save(image_path)
            return {
                "title": title,
                "source_kind": "image",
                "source_file": str(source_path),
                "source_url": source_path.resolve().as_uri(),
                "pages": {
                    "1": {
                        "image_url": image_path.resolve().as_uri(),
                        "width": int(rgb_image.width),
                        "height": int(rgb_image.height),
                    }
                },
            }

    return build_synthetic_preview_assets(payload, output_dir, prefix=prefix, title=title)


def build_document_preview_config(
    *,
    tender_json: dict[str, Any],
    tender_json_path: Path,
    bidder_json: dict[str, Any],
    bidder_json_path: Path,
    output_dir: Path,
) -> dict[str, Any]:
    preview_root = output_dir
    bidder_preview = build_file_preview_assets(
        bidder_json,
        bidder_json_path,
        preview_root / "bidder",
        prefix="bidder",
        title=f"投标文件：{unwrap_document_payload(bidder_json).get('filename') or bidder_json_path.stem}",
    )
    tender_preview = build_file_preview_assets(
        tender_json,
        tender_json_path,
        preview_root / "tender",
        prefix="tender",
        title=f"招标文件：{unwrap_document_payload(tender_json).get('filename') or tender_json_path.stem}",
    )
    return {
        "documents": {
            "bidder": bidder_preview,
            "tender": tender_preview,
        }
    }


def make_identifier(path: Path, role: str) -> str:
    digest = hashlib.sha1(f"{role}:{path.resolve()}".encode("utf-8")).hexdigest()[:12]
    return f"local_{role}_{digest}"


def derive_bidder_key(path: Path) -> str:
    stem = path.stem
    stem = re.sub(r"(商务标|技术标|投标文件|投标|商务|技术)$", "", stem)
    stem = re.sub(r"[\s._\-（）()]+$", "", stem)
    return stem.strip() or path.stem


def role_label(role: str) -> str:
    mapping = {
        DOCUMENT_TYPE_TENDER: "招标文件",
        DOCUMENT_TYPE_BUSINESS_BID: "商务标",
        DOCUMENT_TYPE_TECHNICAL_BID: "技术标",
    }
    return mapping.get(role, role)


def build_document_record(
    *,
    path: Path,
    role: str,
    content: dict[str, Any],
    relation_index: int,
    tender_path: Path | None,
    tender_json: dict[str, Any] | None,
    tender_identifier: str | None,
) -> dict[str, Any]:
    bidder_key = None if role == DOCUMENT_TYPE_TENDER else derive_bidder_key(path)
    return {
        "relation_id": f"local_relation_{relation_index}",
        "relation_role": role,
        "document_id": make_identifier(path, role),
        "identifier_id": make_identifier(path, role),
        "document_type": role,
        "file_name": path.name,
        "file_url": str(path),
        "bidder_key": bidder_key,
        "extracted": True,
        "content": content,
        "tender_identifier_id": tender_identifier,
        "tender_document_type": DOCUMENT_TYPE_TENDER if tender_json is not None else None,
        "tender_file_name": tender_path.name if tender_path is not None else None,
        "tender_file_url": str(tender_path) if tender_path is not None else None,
        "tender_extracted": True if tender_json is not None else None,
        "tender_content": tender_json,
    }  

def build_bidder_infos(paths: list[Path]) -> list[dict[str, Any]]:
    raw_infos: list[dict[str, str]] = []
    for path in paths:
        try:
            payload = load_json_file(path)
            display_name = str(unwrap_document_payload(payload).get("filename") or path.stem)
        except Exception:
            display_name = path.stem
        raw_infos.append(
            {"display_name": display_name, "stem": path.stem}
        )
    duplicate_names = {
        item["display_name"]
        for item in raw_infos
        if sum(1 for other in raw_infos if other["display_name"] == item["display_name"]) > 1
    }
    infos: list[dict[str, Any]] = []
    for item in raw_infos:
        display_name = item["display_name"]
        if display_name in duplicate_names and display_name != item["stem"]:
            display_name = f"{item['stem']}（{display_name}）"
        infos.append(
            {
                "name": display_name,
                "filename": item["stem"],
                "url": f"report_{item['stem']}.html",
            }
        )
    return infos


def build_project_records(
    *,
    tender_path: Path,
    business_bid_files: list[Path],
    technical_bid_files: list[Path],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    tender_json = load_json_file(tender_path)
    tender_identifier = make_identifier(tender_path, DOCUMENT_TYPE_TENDER)
    records: list[dict[str, Any]] = []
    relation_index = 1

    for path in business_bid_files:
        payload = load_json_file(path)
        records.append(
            build_document_record(
                path=path,
                role=DOCUMENT_TYPE_BUSINESS_BID,
                content=payload,
                relation_index=relation_index,
                tender_path=tender_path,
                tender_json=tender_json,
                tender_identifier=tender_identifier,
            )
        )
        relation_index += 1

    for path in technical_bid_files:
        payload = load_json_file(path)
        records.append(
            build_document_record(
                path=path,
                role=DOCUMENT_TYPE_TECHNICAL_BID,
                content=payload,
                relation_index=relation_index,
                tender_path=tender_path,
                tender_json=tender_json,
                tender_identifier=tender_identifier,
            )
        )
        relation_index += 1

    return tender_json, records


def build_source_lookup(
    *,
    tender_path: Path,
    tender_json: dict[str, Any],
    business_bid_files: list[Path],
    technical_bid_files: list[Path],
) -> dict[str, dict[str, Any]]:
    lookup: dict[str, dict[str, Any]] = {}

    def register(path: Path, payload: dict[str, Any], role: str) -> None:
        document = unwrap_document_payload(payload)
        source_path = find_source_document(path, payload)
        source_url = source_path.resolve().as_uri() if source_path else ""
        source_suffix = source_path.suffix.lower() if source_path else ""
        lookup[path.name] = {
            "json_path": str(path),
            "json_name": path.name,
            "role": role,
            "display_name": str(document.get("filename") or path.stem),
            "page_count": int(document.get("page_count") or 0),
            "source_path": str(source_path) if source_path else "",
            "source_url": source_url,
            "source_kind": (
                "pdf" if source_suffix == ".pdf"
                else "image" if source_suffix in {".png", ".jpg", ".jpeg"}
                else "missing"
            ),
        }

    register(tender_path, tender_json, DOCUMENT_TYPE_TENDER)
    for path in business_bid_files:
        register(path, load_json_file(path), DOCUMENT_TYPE_BUSINESS_BID)
    for path in technical_bid_files:
        register(path, load_json_file(path), DOCUMENT_TYPE_TECHNICAL_BID)
    return lookup


def run_project_checks(
    *,
    project_identifier: str,
    document_records: list[dict[str, Any]],
) -> dict[str, Any]:
    duplicate_service = DuplicateCheckService()
    review_service = BidDocumentReviewService()

    business_duplicate = duplicate_service.check_project_documents(
        project_identifier=project_identifier,
        project={"identifier_id": project_identifier},
        document_records=document_records,
        document_types=[DOCUMENT_TYPE_BUSINESS_BID],
        max_pairs_per_type=50,
    )
    technical_duplicate = duplicate_service.check_project_documents(
        project_identifier=project_identifier,
        project={"identifier_id": project_identifier},
        document_records=document_records,
        document_types=[DOCUMENT_TYPE_TECHNICAL_BID],
        max_pairs_per_type=50,
    )

    requested_types = [DOCUMENT_TYPE_BUSINESS_BID]
    if any(record.get("document_type") == DOCUMENT_TYPE_TECHNICAL_BID for record in document_records):
        requested_types.append(DOCUMENT_TYPE_TECHNICAL_BID)

    bid_review = review_service.check_project_documents(
        project_identifier=project_identifier,
        project={"identifier_id": project_identifier},
        document_records=document_records,
        document_types=requested_types,
    )

    return {
        "project_identifier": project_identifier,
        "business_duplicate_check": business_duplicate,
        "technical_duplicate_check": technical_duplicate,
        "bid_document_review": bid_review,
    }


def render_duplicate_rows(
    result: dict[str, Any],
    doc_type: str,
    *,
    source_lookup: dict[str, dict[str, Any]],
    issue_pages: dict[str, str],
    current_files: set[str] | None = None,
) -> str:
    items = list(iter_duplicate_items(result, doc_type, current_files=current_files))
    if not items:
        return "<tr><td colspan='8'>未发现相关可疑对</td></tr>"

    page_key = "business_duplicates" if doc_type == DOCUMENT_TYPE_BUSINESS_BID else "technical_duplicates"
    detail_page = issue_pages.get(page_key, "")
    rows: list[str] = []
    for item in items:
        metrics = item.get("metrics") or {}
        risk_level = str(item.get("risk_level") or "-")
        left_file = str(item.get("left_file_name") or "")
        right_file = str(item.get("right_file_name") or "")
        left_pages = collect_duplicate_pages(item, "left")
        right_pages = collect_duplicate_pages(item, "right")
        detail_anchor = duplicate_pair_anchor(doc_type, item)
        detail_href = issue_page_href(detail_page, detail_anchor)
        rows.append(
            f"<tr class='{severity_css_class(risk_level)}'>"
            f"<td>{html.escape(risk_level)}</td>"
            f"<td>{html.escape(str(item.get('exact_match_score') or 0))}</td>"
            f"<td>{build_source_doc_cell_html(source_lookup, left_file, left_pages)}</td>"
            f"<td>{build_source_doc_cell_html(source_lookup, right_file, right_pages)}</td>"
            f"<td>{html.escape(str(metrics.get('exact_section_count') or 0))}</td>"
            f"<td>{html.escape(str(metrics.get('exact_block_count') or 0))}</td>"
            f"<td>{html.escape(str(metrics.get('exact_table_count') or 0))}</td>"
            f"<td>{build_issue_detail_link(detail_href, '查看全部证据')}</td>"
            "</tr>"
        )
    return "\n".join(rows)


def render_typo_rows(
    result: dict[str, Any],
    *,
    source_lookup: dict[str, dict[str, Any]],
    issue_pages: dict[str, str],
    current_files: set[str] | None = None,
) -> str:
    items = list(iter_typo_items(result, current_files=current_files))
    if not items:
        return "<tr><td colspan='7'>未发现错别字候选</td></tr>"

    detail_page = issue_pages.get("typos", "")
    rows: list[str] = []
    for role, file_name, item in items:
        page = normalize_pages(item.get("page"))
        detail_anchor = typo_issue_anchor(role, file_name, item)
        detail_href = issue_page_href(detail_page, detail_anchor)
        rows.append(
            f"<tr class='{severity_css_class('warning')}'>"
            f"<td>{html.escape(role_label(role))}</td>"
            f"<td>{build_source_doc_cell_html(source_lookup, file_name, page)}</td>"
            f"<td>{build_source_page_links_html(source_lookup, file_name, page)}</td>"
            f"<td><mark>{html.escape(str(item.get('matched_text') or '-'))}</mark></td>"
            f"<td>{html.escape(str(item.get('suggestion') or '-'))}</td>"
            f"<td>{highlight_text_html(str(item.get('text') or '-'), str(item.get('matched_text') or ''))}</td>"
            f"<td>{build_issue_detail_link(detail_href, '查看问题页')}</td>"
            "</tr>"
        )
    return "\n".join(rows)


def render_personnel_rows(
    result: dict[str, Any],
    *,
    source_lookup: dict[str, dict[str, Any]],
    issue_pages: dict[str, str],
    current_files: set[str] | None = None,
) -> str:
    items = list(iter_personnel_reuse_items(result, current_files=current_files))
    if not items:
        return "<tr><td colspan='6'>未发现一人多用</td></tr>"

    detail_page = issue_pages.get("personnel", "")
    rows: list[str] = []
    for role, item in items:
        detail_anchor = personnel_issue_anchor(role, item)
        detail_href = issue_page_href(detail_page, detail_anchor)
        occurrences = list(item.get("items") or [])
        occurrence_html = "<br>".join(
            build_source_doc_cell_html(
                source_lookup,
                str(entry.get("file_name") or ""),
                normalize_pages(entry.get("page")),
            )
            for entry in occurrences
        ) or "-"
        rows.append(
            f"<tr class='{severity_css_class(str(item.get('risk_level') or 'warning'))}'>"
            f"<td>{html.escape(role_label(role))}</td>"
            f"<td><mark>{html.escape(str(item.get('name') or '-'))}</mark></td>"
            f"<td>{html.escape(str(item.get('risk_level') or '-'))}</td>"
            f"<td>{html.escape(str(item.get('document_count') or 0))}</td>"
            f"<td>{occurrence_html}</td>"
            f"<td>{build_issue_detail_link(detail_href, '查看问题页')}</td>"
            "</tr>"
        )
    return "\n".join(rows)


def normalize_pages(*values: Any) -> list[int]:
    pages: set[int] = set()

    def visit(value: Any) -> None:
        if value is None:
            return
        if isinstance(value, int):
            if value > 0:
                pages.add(value)
            return
        if isinstance(value, (list, tuple, set)):
            for item in value:
                visit(item)
            return
        if isinstance(value, str):
            for token in re.findall(r"\d+", value):
                page = int(token)
                if page > 0:
                    pages.add(page)

    for current in values:
        visit(current)
    return sorted(pages)


def coalesce_page_ranges(pages: list[int]) -> list[tuple[int, int]]:
    if not pages:
        return []
    merged: list[tuple[int, int]] = []
    start = end = pages[0]
    for page in pages[1:]:
        if page == end + 1:
            end = page
            continue
        merged.append((start, end))
        start = end = page
    merged.append((start, end))
    return merged


def make_stable_token(*parts: Any) -> str:
    joined = "|".join(str(part) for part in parts)
    return hashlib.sha1(joined.encode("utf-8")).hexdigest()[:12]


def append_page_fragment(url: str, page: int | None) -> str:
    if not url:
        return ""
    base = url.split("#", 1)[0]
    if page and page > 0:
        return f"{base}#page={page}"
    return base


def severity_css_class(value: str) -> str:
    normalized = value.strip().lower()
    if normalized in {"high", "critical", "error", "严重"}:
        return "issue-row issue-severity-high"
    if normalized in {"medium", "warning", "warn", "中", "中等"}:
        return "issue-row issue-severity-medium"
    return "issue-row issue-severity-low"


def build_issue_detail_link(href: str, label: str) -> str:
    if not href:
        return "<span class='issue-muted'>-</span>"
    return (
        f"<a class='issue-link issue-detail-link' href='{html.escape(href)}' "
        f"target='_blank' rel='noreferrer'>{html.escape(label)}</a>"
    )


def build_source_file_link_html(
    source_lookup: dict[str, dict[str, Any]],
    file_name: str,
    *,
    label: str | None = None,
    page: int | None = None,
) -> str:
    entry = source_lookup.get(file_name) or {}
    display_name = label or str(entry.get("display_name") or file_name or "-")
    source_url = str(entry.get("source_url") or "")
    if not source_url:
        return f"<span>{html.escape(display_name)}</span>"
    return (
        f"<a class='issue-link issue-file-link' href='{html.escape(append_page_fragment(source_url, page))}' "
        f"target='_blank' rel='noreferrer'>{html.escape(display_name)}</a>"
    )


def build_source_page_links_html(
    source_lookup: dict[str, dict[str, Any]],
    file_name: str,
    pages: list[int],
) -> str:
    if not pages:
        return "<span class='issue-muted'>页码待补充</span>"

    entry = source_lookup.get(file_name) or {}
    source_url = str(entry.get("source_url") or "")
    fragments: list[str] = []
    for start, end in coalesce_page_ranges(pages):
        label = f"P{start}" if start == end else f"P{start}-P{end}"
        if source_url:
            fragments.append(
                f"<a class='issue-link issue-page-link' href='{html.escape(append_page_fragment(source_url, start))}' "
                f"target='_blank' rel='noreferrer'>{html.escape(label)}</a>"
            )
        else:
            fragments.append(f"<span>{html.escape(label)}</span>")
    return "<span class='issue-page-links'>" + " ".join(fragments) + "</span>"


def build_source_doc_cell_html(
    source_lookup: dict[str, dict[str, Any]],
    file_name: str,
    pages: list[int],
) -> str:
    entry = source_lookup.get(file_name) or {}
    display_name = str(entry.get("display_name") or file_name or "-")
    json_name = str(entry.get("json_name") or file_name or "")
    first_page = pages[0] if pages else None
    parts = [
        "<div class='issue-doc-cell'>",
        f"<div>{build_source_file_link_html(source_lookup, file_name, label=display_name, page=first_page)}</div>",
    ]
    if json_name and json_name != display_name:
        parts.append(f"<div class='issue-subtext'>{html.escape(json_name)}</div>")
    parts.append(f"<div>{build_source_page_links_html(source_lookup, file_name, pages)}</div>")
    parts.append("</div>")
    return "".join(parts)


def trim_text(value: str, limit: int = 140) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    if len(text) <= limit:
        return text
    return text[: limit - 1].rstrip() + "…"


def highlight_text_html(text: str, needle: str) -> str:
    raw_text = str(text or "-")
    keyword = str(needle or "").strip()
    if not keyword:
        return html.escape(raw_text)
    index = raw_text.find(keyword)
    if index < 0:
        return html.escape(raw_text)
    before = raw_text[:index]
    matched = raw_text[index:index + len(keyword)]
    after = raw_text[index + len(keyword):]
    return f"{html.escape(before)}<mark>{html.escape(matched)}</mark>{html.escape(after)}"


def duplicate_pair_anchor(doc_type: str, item: dict[str, Any]) -> str:
    return f"duplicate-{doc_type}-{make_stable_token(item.get('left_file_name'), item.get('right_file_name'))}"


def typo_issue_anchor(role: str, file_name: str, item: dict[str, Any]) -> str:
    return (
        f"typo-{make_stable_token(role, file_name, item.get('page'), item.get('matched_text'), item.get('suggestion'))}"
    )


def personnel_issue_anchor(role: str, item: dict[str, Any]) -> str:
    occurrence_keys = [
        f"{entry.get('file_name')}:{entry.get('page')}:{entry.get('name')}"
        for entry in (item.get("items") or [])
    ]
    return f"personnel-{make_stable_token(role, item.get('name'), *sorted(occurrence_keys))}"


def issue_page_href(page: str, anchor: str | None = None) -> str:
    if not page:
        return ""
    return f"{page}#{anchor}" if anchor else page


def collect_duplicate_pages(item: dict[str, Any], side: str) -> list[int]:
    collected: list[int] = []
    page_key = f"{side}_page"
    pages_key = f"{side}_pages"
    for section in item.get("duplicate_sections") or []:
        collected.extend(normalize_pages(section.get(pages_key)))
    for table in item.get("duplicate_tables") or []:
        collected.extend(normalize_pages(table.get(pages_key)))
    for block in item.get("duplicate_blocks") or []:
        collected.extend(normalize_pages(block.get(page_key), block.get("page")))
    return sorted(set(collected))


def iter_duplicate_items(
    result: dict[str, Any],
    doc_type: str,
    *,
    current_files: set[str] | None = None,
) -> list[dict[str, Any]]:
    group = ((result.get("groups") or {}).get(doc_type) or {})
    items = [
        item
        for item in (group.get("items") or [])
        if is_duplicate_issue(item)
    ]
    if not current_files:
        return items
    return [
        item
        for item in items
        if str(item.get("left_file_name") or "") in current_files
        or str(item.get("right_file_name") or "") in current_files
    ]


def is_duplicate_issue(item: dict[str, Any]) -> bool:
    if bool(item.get("suspicious")) or bool(item.get("exact_duplicate")):
        return True
    risk_level = str(item.get("risk_level") or "").strip().lower()
    if risk_level and risk_level not in {"none", "low", "-"}:
        return True
    try:
        score = float(item.get("exact_match_score") or 0)
    except (TypeError, ValueError):
        score = 0.0
    return score >= 0.8


def iter_typo_items(
    result: dict[str, Any],
    *,
    current_files: set[str] | None = None,
) -> list[tuple[str, str, dict[str, Any]]]:
    rows: list[tuple[str, str, dict[str, Any]]] = []
    for role, group in (result.get("groups") or {}).items():
        typo = group.get("typo_check") or {}
        for document in typo.get("documents") or []:
            file_name = str(document.get("file_name") or "")
            if current_files and file_name not in current_files:
                continue
            for item in document.get("items") or []:
                rows.append((str(role), file_name, item))
    return rows


def iter_personnel_reuse_items(
    result: dict[str, Any],
    *,
    current_files: set[str] | None = None,
) -> list[tuple[str, dict[str, Any]]]:
    rows: list[tuple[str, dict[str, Any]]] = []
    for role, group in (result.get("groups") or {}).items():
        reuse = group.get("personnel_reuse_check") or {}
        for item in (reuse.get("items") or reuse.get("reused_names") or []):
            related_files = {
                str(entry.get("file_name") or "")
                for entry in (item.get("items") or [])
            }
            if current_files and not (related_files & current_files):
                continue
            rows.append((str(role), item))
    return rows


def render_duplicate_evidence_sections(
    item: dict[str, Any],
    *,
    source_lookup: dict[str, dict[str, Any]],
) -> str:
    left_file = str(item.get("left_file_name") or "")
    right_file = str(item.get("right_file_name") or "")
    blocks = item.get("duplicate_blocks") or []
    sections = item.get("duplicate_sections") or []
    tables = item.get("duplicate_tables") or []
    parts: list[str] = []

    if sections:
        entries = []
        for section in sections:
            left_pages = normalize_pages(section.get("left_pages"))
            right_pages = normalize_pages(section.get("right_pages"))
            entries.append(
                "<li>"
                f"<div><strong>文件A：</strong>{build_source_page_links_html(source_lookup, left_file, left_pages)}</div>"
                f"<div class='issue-preview'>{html.escape(trim_text(str(section.get('left_preview') or section.get('left_title') or '-')))}</div>"
                f"<div><strong>文件B：</strong>{build_source_page_links_html(source_lookup, right_file, right_pages)}</div>"
                f"<div class='issue-preview'>{html.escape(trim_text(str(section.get('right_preview') or section.get('right_title') or '-')))}</div>"
                "</li>"
            )
        parts.append(f"<details open><summary>重复段落证据（{len(sections)}）</summary><ul class='issue-evidence-list'>{''.join(entries)}</ul></details>")

    if blocks:
        entries = []
        for block in blocks:
            left_pages = normalize_pages(block.get("left_page"), block.get("page"))
            right_pages = normalize_pages(block.get("right_page"), block.get("page"))
            entries.append(
                "<li>"
                f"<div><strong>文件A：</strong>{build_source_page_links_html(source_lookup, left_file, left_pages)}</div>"
                f"<div><strong>文件B：</strong>{build_source_page_links_html(source_lookup, right_file, right_pages)}</div>"
                f"<div class='issue-preview'><mark>{html.escape(trim_text(str(block.get('text') or '-')))}</mark></div>"
                "</li>"
            )
        parts.append(f"<details open><summary>重复句证据（{len(blocks)}）</summary><ul class='issue-evidence-list'>{''.join(entries)}</ul></details>")

    if tables:
        entries = []
        for table in tables:
            left_pages = normalize_pages(table.get("left_pages"))
            right_pages = normalize_pages(table.get("right_pages"))
            sample_rows = table.get("sample_rows") or []
            sample_text = trim_text(json.dumps(sample_rows, ensure_ascii=False), 220) if sample_rows else "-"
            entries.append(
                "<li>"
                f"<div><strong>文件A：</strong>{build_source_page_links_html(source_lookup, left_file, left_pages)}</div>"
                f"<div><strong>文件B：</strong>{build_source_page_links_html(source_lookup, right_file, right_pages)}</div>"
                f"<div class='issue-preview'>{html.escape(sample_text)}</div>"
                "</li>"
            )
        parts.append(f"<details open><summary>重复表格证据（{len(tables)}）</summary><ul class='issue-evidence-list'>{''.join(entries)}</ul></details>")

    return "".join(parts) or "<p class='issue-muted'>当前未返回更细的重复证据。</p>"


def build_duplicate_issue_detail_html(
    *,
    project_identifier: str,
    title: str,
    result: dict[str, Any],
    doc_type: str,
    source_lookup: dict[str, dict[str, Any]],
) -> str:
    items = list(iter_duplicate_items(result, doc_type))
    if not items:
        body = "<p class='issue-empty'>未发现相关可疑对。</p>"
    else:
        cards: list[str] = []
        for item in items:
            metrics = item.get("metrics") or {}
            risk_level = str(item.get("risk_level") or "-")
            left_file = str(item.get("left_file_name") or "")
            right_file = str(item.get("right_file_name") or "")
            left_pages = collect_duplicate_pages(item, "left")
            right_pages = collect_duplicate_pages(item, "right")
            cards.append(
                f"""
                <article id="{html.escape(duplicate_pair_anchor(doc_type, item))}" class="issue-card {severity_css_class(risk_level)}">
                  <div class="issue-card-header">
                    <div>
                      <h2>{html.escape(left_file)} <> {html.escape(right_file)}</h2>
                      <p class="issue-meta">风险：{html.escape(risk_level)} ｜ 分数：{html.escape(str(item.get('exact_match_score') or 0))}</p>
                    </div>
                    <div class="issue-metrics">
                      <span>重复段 {html.escape(str(metrics.get('exact_section_count') or 0))}</span>
                      <span>重复句 {html.escape(str(metrics.get('exact_block_count') or 0))}</span>
                      <span>重复表 {html.escape(str(metrics.get('exact_table_count') or 0))}</span>
                    </div>
                  </div>
                  <div class="issue-doc-grid">
                    <div>{build_source_doc_cell_html(source_lookup, left_file, left_pages)}</div>
                    <div>{build_source_doc_cell_html(source_lookup, right_file, right_pages)}</div>
                  </div>
                  {render_duplicate_evidence_sections(item, source_lookup=source_lookup)}
                </article>
                """
            )
        body = "".join(cards)
    return build_issue_page_shell(project_identifier=project_identifier, title=title, body=body)


def build_typo_issue_detail_html(
    *,
    project_identifier: str,
    result: dict[str, Any],
    source_lookup: dict[str, dict[str, Any]],
) -> str:
    items = list(iter_typo_items(result))
    if not items:
        body = "<p class='issue-empty'>未发现错别字候选。</p>"
    else:
        cards: list[str] = []
        for role, file_name, item in items:
            pages = normalize_pages(item.get("page"))
            cards.append(
                f"""
                <article id="{html.escape(typo_issue_anchor(role, file_name, item))}" class="issue-card issue-row issue-severity-medium">
                  <div class="issue-card-header">
                    <div>
                      <h2><mark>{html.escape(str(item.get("matched_text") or "-"))}</mark> -> {html.escape(str(item.get("suggestion") or "-"))}</h2>
                      <p class="issue-meta">{html.escape(role_label(role))}</p>
                    </div>
                  </div>
                  <div>{build_source_doc_cell_html(source_lookup, file_name, pages)}</div>
                  <div class="issue-preview issue-block">{highlight_text_html(str(item.get("text") or "-"), str(item.get("matched_text") or ""))}</div>
                </article>
                """
            )
        body = "".join(cards)
    return build_issue_page_shell(project_identifier=project_identifier, title="错别字问题总览", body=body)


def build_personnel_issue_detail_html(
    *,
    project_identifier: str,
    result: dict[str, Any],
    source_lookup: dict[str, dict[str, Any]],
) -> str:
    items = list(iter_personnel_reuse_items(result))
    if not items:
        body = "<p class='issue-empty'>未发现一人多用。</p>"
    else:
        cards: list[str] = []
        for role, item in items:
            occurrences = []
            for entry in item.get("items") or []:
                file_name = str(entry.get("file_name") or "")
                pages = normalize_pages(entry.get("page"))
                occurrences.append(
                    "<li>"
                    f"{build_source_doc_cell_html(source_lookup, file_name, pages)}"
                    f"<div class='issue-preview'>{html.escape(trim_text(str(entry.get('text') or '-')))}</div>"
                    "</li>"
                )
            cards.append(
                f"""
                <article id="{html.escape(personnel_issue_anchor(role, item))}" class="issue-card {severity_css_class(str(item.get('risk_level') or 'warning'))}">
                  <div class="issue-card-header">
                    <div>
                      <h2><mark>{html.escape(str(item.get("name") or "-"))}</mark></h2>
                      <p class="issue-meta">{html.escape(role_label(role))} ｜ 风险：{html.escape(str(item.get("risk_level") or "-"))} ｜ 涉及文件：{html.escape(str(item.get("document_count") or 0))}</p>
                    </div>
                  </div>
                  <ul class="issue-evidence-list">{''.join(occurrences)}</ul>
                </article>
                """
            )
        body = "".join(cards)
    return build_issue_page_shell(project_identifier=project_identifier, title="一人多用问题总览", body=body)


def build_issue_page_shell(*, project_identifier: str, title: str, body: str) -> str:
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{html.escape(title)}</title>
  <style>
    body {{ margin: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; background: #f6f7f4; color: #1c1c1c; }}
    main {{ max-width: 1320px; margin: 0 auto; padding: 28px 32px 40px; }}
    h1 {{ margin: 0 0 8px; font-size: 28px; }}
    .meta {{ color: #5f6862; margin-bottom: 18px; }}
    .toolbar {{ display: flex; gap: 12px; flex-wrap: wrap; margin-bottom: 18px; }}
    .toolbar a {{ color: #0b57d0; text-decoration: none; font-weight: 600; }}
    .issue-card {{ background: #fff; border: 1px solid #d9ded8; border-radius: 10px; padding: 16px 18px; margin-bottom: 16px; }}
    .issue-card-header {{ display: flex; justify-content: space-between; gap: 16px; align-items: flex-start; }}
    .issue-card h2 {{ margin: 0 0 6px; font-size: 20px; }}
    .issue-meta {{ margin: 0; color: #5f6862; }}
    .issue-doc-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 12px; margin: 14px 0; }}
    .issue-doc-cell {{ background: #f8faf7; border: 1px solid #d9ded8; border-radius: 8px; padding: 10px 12px; }}
    .issue-subtext {{ margin-top: 4px; color: #5f6862; font-size: 12px; }}
    .issue-link {{ color: #0b57d0; text-decoration: none; }}
    .issue-link:hover {{ text-decoration: underline; }}
    .issue-page-links {{ display: inline-flex; gap: 8px; flex-wrap: wrap; margin-top: 8px; }}
    .issue-evidence-list {{ margin: 12px 0 0; padding-left: 18px; }}
    .issue-evidence-list li + li {{ margin-top: 12px; }}
    .issue-preview {{ margin-top: 8px; line-height: 1.6; }}
    .issue-block {{ background: #fff7dd; border-radius: 8px; padding: 10px 12px; }}
    .issue-metrics {{ display: flex; flex-wrap: wrap; gap: 10px; color: #5f6862; font-size: 13px; }}
    .issue-empty, .issue-muted {{ color: #5f6862; }}
    .issue-row.issue-severity-high {{ border-left: 6px solid #c62828; background: #fff5f5; }}
    .issue-row.issue-severity-medium {{ border-left: 6px solid #f9a825; background: #fff8e1; }}
    .issue-row.issue-severity-low {{ border-left: 6px solid #2e7d32; background: #f6fbf6; }}
    mark {{ background: #ffe082; padding: 0 2px; }}
    details {{ margin-top: 12px; }}
    summary {{ cursor: pointer; font-weight: 700; }}
  </style>
</head>
<body>
  <main>
    <h1>{html.escape(title)}</h1>
    <div class="meta">{html.escape(project_identifier)}</div>
    <div class="toolbar">
      <a href="project_review_summary.html">返回项目总览</a>
    </div>
    {body}
  </main>
</body>
</html>"""


def write_project_issue_pages(
    *,
    output_dir: Path,
    project_identifier: str,
    project_results: dict[str, Any],
    source_lookup: dict[str, dict[str, Any]],
) -> dict[str, str]:
    page_map = {
        "business_duplicates": "project_issue_business_duplicates.html",
        "technical_duplicates": "project_issue_technical_duplicates.html",
        "typos": "project_issue_typos.html",
        "personnel": "project_issue_personnel.html",
    }
    contents = {
        "business_duplicates": build_duplicate_issue_detail_html(
            project_identifier=project_identifier,
            title="商务标查重问题总览",
            result=project_results.get("business_duplicate_check") or {},
            doc_type=DOCUMENT_TYPE_BUSINESS_BID,
            source_lookup=source_lookup,
        ),
        "technical_duplicates": build_duplicate_issue_detail_html(
            project_identifier=project_identifier,
            title="技术标查重问题总览",
            result=project_results.get("technical_duplicate_check") or {},
            doc_type=DOCUMENT_TYPE_TECHNICAL_BID,
            source_lookup=source_lookup,
        ),
        "typos": build_typo_issue_detail_html(
            project_identifier=project_identifier,
            result=project_results.get("bid_document_review") or {},
            source_lookup=source_lookup,
        ),
        "personnel": build_personnel_issue_detail_html(
            project_identifier=project_identifier,
            result=project_results.get("bid_document_review") or {},
            source_lookup=source_lookup,
        ),
    }
    for key, file_name in page_map.items():
        path = output_dir / file_name
        path.write_text(contents[key], encoding="utf-8")
    return page_map


def build_project_review_section(
    project_results: dict[str, Any],
    *,
    source_lookup: dict[str, dict[str, Any]],
    issue_pages: dict[str, str],
    current_business_file: str | None = None,
) -> str:
    business_duplicate = project_results.get("business_duplicate_check") or {}
    technical_duplicate = project_results.get("technical_duplicate_check") or {}
    bid_review = project_results.get("bid_document_review") or {}
    bid_summary = bid_review.get("summary") or {}
    business_summary = business_duplicate.get("summary") or {}
    technical_summary = technical_duplicate.get("summary") or {}
    current_files = {current_business_file} if current_business_file else None
    scope_text = current_business_file or "全项目"

    return f"""
    <style>
      .project-review-addon {{
        margin: 32px auto 0;
        padding: 24px;
        border: 1px solid #d9ded8;
        border-radius: 12px;
        background: #fbfcfa;
      }}
      .project-review-addon h2 {{
        margin: 0 0 10px;
      }}
      .project-review-addon h3 {{
        margin: 26px 0 10px;
      }}
      .project-review-grid {{
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
        gap: 12px;
      }}
      .project-review-card {{
        background: #fff;
        border: 1px solid #d9ded8;
        border-radius: 8px;
        padding: 12px 14px;
      }}
      .project-review-card .label {{
        color: #5f6862;
        font-size: 12px;
      }}
      .project-review-card .value {{
        margin-top: 6px;
        font-size: 22px;
        font-weight: 700;
      }}
      .project-review-scope {{
        margin: 10px 0 0;
        color: #5f6862;
        font-size: 13px;
      }}
      .project-review-addon table {{
        width: 100%;
        border-collapse: collapse;
        background: #fff;
        border: 1px solid #d9ded8;
      }}
      .project-review-addon th,
      .project-review-addon td {{
        padding: 9px 10px;
        border-bottom: 1px solid #e6e8e4;
        text-align: left;
        vertical-align: top;
        font-size: 13px;
      }}
      .project-review-addon th {{
        background: #eef1ed;
        font-weight: 700;
      }}
      .project-review-addon .project-review-links {{
        display: flex;
        gap: 12px;
        flex-wrap: wrap;
        margin: 16px 0 4px;
      }}
      .project-review-addon .project-review-links a,
      .project-review-addon .issue-link {{
        color: #0b57d0;
        text-decoration: none;
      }}
      .project-review-addon .project-review-links a:hover,
      .project-review-addon .issue-link:hover {{
        text-decoration: underline;
      }}
      .project-review-addon .issue-doc-cell {{
        min-width: 220px;
      }}
      .project-review-addon .issue-page-links {{
        display: inline-flex;
        gap: 8px;
        flex-wrap: wrap;
        margin-top: 8px;
      }}
      .project-review-addon .issue-subtext {{
        margin-top: 4px;
        color: #5f6862;
        font-size: 12px;
      }}
      .project-review-addon .issue-muted {{
        color: #5f6862;
      }}
      .project-review-addon .issue-severity-high {{
        background: #fff2f2;
      }}
      .project-review-addon .issue-severity-medium {{
        background: #fff9e6;
      }}
      .project-review-addon .issue-severity-low {{
        background: #f8fbf7;
      }}
      .project-review-addon mark {{
        background: #ffe082;
        padding: 0 2px;
      }}
    </style>
    <section class="project-review-addon">
      <h2>项目级补充审查</h2>
      <div class="project-review-grid">
        <div class="project-review-card"><div class="label">商务标可疑对</div><div class="value">{html.escape(str(business_summary.get("suspicious_pair_count") or 0))}</div></div>
        <div class="project-review-card"><div class="label">技术标可疑对</div><div class="value">{html.escape(str(technical_summary.get("suspicious_pair_count") or 0))}</div></div>
        <div class="project-review-card"><div class="label">错别字候选</div><div class="value">{html.escape(str(bid_summary.get("typo_issue_count") or 0))}</div></div>
        <div class="project-review-card"><div class="label">一人多用姓名</div><div class="value">{html.escape(str(bid_summary.get("reused_name_count") or 0))}</div></div>
      </div>
      <p class="project-review-scope">当前视角：{html.escape(scope_text)}</p>
      <div class="project-review-links">
        <a href="{html.escape(issue_pages.get('business_duplicates') or '')}" target="_blank" rel="noreferrer">全部商务标查重问题</a>
        <a href="{html.escape(issue_pages.get('technical_duplicates') or '')}" target="_blank" rel="noreferrer">全部技术标查重问题</a>
        <a href="{html.escape(issue_pages.get('typos') or '')}" target="_blank" rel="noreferrer">全部错别字问题</a>
        <a href="{html.escape(issue_pages.get('personnel') or '')}" target="_blank" rel="noreferrer">全部一人多用问题</a>
      </div>

      <h3>商务标查重（当前文件相关）</h3>
      <table>
        <thead>
          <tr><th>风险</th><th>分数</th><th>文件 A</th><th>文件 B</th><th>重复段</th><th>重复句</th><th>重复表</th><th>证据</th></tr>
        </thead>
        <tbody>{render_duplicate_rows(business_duplicate, DOCUMENT_TYPE_BUSINESS_BID, source_lookup=source_lookup, issue_pages=issue_pages, current_files=current_files)}</tbody>
      </table>

      <h3>技术标查重（全项目）</h3>
      <table>
        <thead>
          <tr><th>风险</th><th>分数</th><th>文件 A</th><th>文件 B</th><th>重复段</th><th>重复句</th><th>重复表</th><th>证据</th></tr>
        </thead>
        <tbody>{render_duplicate_rows(technical_duplicate, DOCUMENT_TYPE_TECHNICAL_BID, source_lookup=source_lookup, issue_pages=issue_pages)}</tbody>
      </table>

      <h3>一人多用（当前文件相关）</h3>
      <table>
        <thead>
          <tr><th>文档类型</th><th>姓名</th><th>风险</th><th>涉及文件数</th><th>文件与页码</th><th>详情</th></tr>
        </thead>
        <tbody>{render_personnel_rows(bid_review, source_lookup=source_lookup, issue_pages=issue_pages, current_files=current_files)}</tbody>
      </table>

      <h3>错别字识别（当前文件相关）</h3>
      <table>
        <thead>
          <tr><th>文档类型</th><th>文件</th><th>页码</th><th>原字</th><th>建议</th><th>原文</th><th>详情</th></tr>
        </thead>
        <tbody>{render_typo_rows(bid_review, source_lookup=source_lookup, issue_pages=issue_pages, current_files=current_files)}</tbody>
      </table>
    </section>
    """


def build_project_summary_html(
    *,
    project_identifier: str,
    business_infos: list[dict[str, Any]],
    project_results: dict[str, Any],
    source_lookup: dict[str, dict[str, Any]],
    issue_pages: dict[str, str],
) -> str:
    link_items = "\n".join(
        f"<li><a href='{html.escape(str(item['url']))}'>{html.escape(str(item['name']))}</a></li>"
        for item in business_infos
    )
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>项目级审查总览</title>
  <style>
    body {{ margin: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; background: #f7f7f4; color: #1c1c1c; }}
    main {{ padding: 28px 36px 40px; max-width: 1320px; margin: 0 auto; }}
    h1 {{ margin: 0 0 8px; font-size: 28px; }}
    .meta {{ color: #5f6862; margin-bottom: 18px; }}
    ul {{ background: #fff; border: 1px solid #d9ded8; border-radius: 8px; padding: 14px 28px; }}
    li + li {{ margin-top: 8px; }}
  </style>
</head>
<body>
  <main>
    <h1>项目级审查总览</h1>
    <div class="meta">{html.escape(project_identifier)}</div>
    <h2>单文件报告</h2>
    <ul>{link_items}</ul>
    {build_project_review_section(project_results, source_lookup=source_lookup, issue_pages=issue_pages)}
  </main>
</body>
</html>"""


def inject_project_review_section(html_report: str, extra_section: str) -> str:
    marker = "</body>"
    if marker in html_report:
        return html_report.replace(marker, f"{extra_section}\n{marker}", 1)
    return f"{html_report}\n{extra_section}"


def generate_report_for_bidder(
    tender_json: dict[str, Any],
    tender_json_path: Path,
    bidder_json_path: Path,
    output_html_path: Path,
    all_bidder_infos: list[dict[str, Any]],
    project_results: dict[str, Any],
    source_lookup: dict[str, dict[str, Any]],
    issue_pages: dict[str, str],
) -> None:
    print(f"正在处理投标文件: {bidder_json_path}")
    bidder_json = load_json_file(bidder_json_path)

    integrity_checker = IntegrityChecker()
    integrity_report = integrity_checker.check_integrity(tender_json, bidder_json)

    cons_checker = ConsistencyChecker()
    consistency_report = cons_checker.compare_raw_data(tender_json, bidder_json)

    dev_checker = DeviationChecker()
    deviation_report = dev_checker.check_technical_deviation(tender_json, bidder_json)

    price_checker = ItemizedPricingChecker()
    pricing_report = price_checker.check_itemized_logic(bidder_json, tender_text=tender_json)

    reason_checker = ReasonablenessChecker()
    limit_report = reason_checker.check_bid_price_against_tender_limit(tender_json, bidder_json)
    compliance_report = reason_checker.check_price_compliance(bidder_json)
    reasonableness_final = [limit_report, compliance_report]

    verification_checker = VerificationChecker(None)
    verification_report = verification_checker.check_seal_and_date(tender_json, bidder_json)

    templates = TemplateExtractor.extract_consistency_templates(tender_json)
    model_segments = [{"title": item["title"], "text": "\n".join(item["content"])} for item in templates]
    bidder_segments = DocumentProcessor.segment_document(bidder_json, templates, is_test_file=True)

    switcher_info = {
        "current_file": bidder_json_path.name,
        "files": all_bidder_infos,
    }

    visualizer = ReportVisualizer()
    detail_dir_name = f"details_{bidder_json_path.stem}"
    detail_dir_path = output_html_path.parent / detail_dir_name
    preview_config = build_document_preview_config(
        tender_json=tender_json,
        tender_json_path=tender_json_path,
        bidder_json=bidder_json,
        bidder_json_path=bidder_json_path,
        output_dir=output_html_path.parent / f"locator_assets_{bidder_json_path.stem}",
    )
    html_report = visualizer.generate_html(
        integrity_report=integrity_report,
        consistency_report=consistency_report,
        test_segments=bidder_segments,
        model_segments=model_segments,
        deviation_report=deviation_report,
        pricing_report=pricing_report,
        reasonableness_report=reasonableness_final,
        verification_report=verification_report,
        file_switcher_info=switcher_info,
        detail_dir=str(detail_dir_path),
        detail_href_prefix=detail_dir_name,
        document_preview_config=preview_config,
    )

    project_review_section = build_project_review_section(
        project_results,
        source_lookup=source_lookup,
        issue_pages=issue_pages,
        current_business_file=bidder_json_path.name,
    )
    html_report = inject_project_review_section(html_report, project_review_section)

    with output_html_path.open("w", encoding="utf-8") as file:
        file.write(html_report)
    print(f"  已生成报告: {output_html_path}")


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    if not TENDER_PATH.exists():
        print(f"错误: 招标文件不存在: {TENDER_PATH}")
        return

    missing_files = [path for path in BUSINESS_BID_FILES + TECHNICAL_BID_FILES if not path.exists()]
    if missing_files:
        print("错误: 以下投标文件不存在：")
        for path in missing_files:
            print(f"  - {path}")
        return

    tender_json, document_records = build_project_records(
        tender_path=TENDER_PATH,
        business_bid_files=BUSINESS_BID_FILES,
        technical_bid_files=TECHNICAL_BID_FILES,
    )
    business_infos = build_bidder_infos(BUSINESS_BID_FILES)
    project_identifier = OUTPUT_DIR.name or TENDER_PATH.parent.name
    project_results = run_project_checks(
        project_identifier=project_identifier,
        document_records=document_records,
    )
    source_lookup = build_source_lookup(
        tender_path=TENDER_PATH,
        tender_json=tender_json,
        business_bid_files=BUSINESS_BID_FILES,
        technical_bid_files=TECHNICAL_BID_FILES,
    )
    issue_pages = write_project_issue_pages(
        output_dir=OUTPUT_DIR,
        project_identifier=project_identifier,
        project_results=project_results,
        source_lookup=source_lookup,
    )

    summary_html = build_project_summary_html(
        project_identifier=project_identifier,
        business_infos=business_infos,
        project_results=project_results,
        source_lookup=source_lookup,
        issue_pages=issue_pages,
    )
    summary_path = OUTPUT_DIR / "project_review_summary.html"
    with summary_path.open("w", encoding="utf-8") as file:
        file.write(summary_html)

    for bidder_path in BUSINESS_BID_FILES:
        infos_for_this: list[dict[str, Any]] = []
        for item in business_infos:
            copied = item.copy()
            copied["active"] = (item["filename"] == bidder_path.stem)
            infos_for_this.append(copied)

        output_html = OUTPUT_DIR / f"report_{bidder_path.stem}.html"
        generate_report_for_bidder(
            tender_json=tender_json,
            tender_json_path=TENDER_PATH,
            bidder_json_path=bidder_path,
            output_html_path=output_html,
            all_bidder_infos=infos_for_this,
            project_results=project_results,
            source_lookup=source_lookup,
            issue_pages=issue_pages,
        )

    business_summary = (project_results.get("business_duplicate_check") or {}).get("summary") or {}
    technical_summary = (project_results.get("technical_duplicate_check") or {}).get("summary") or {}
    bid_summary = (project_results.get("bid_document_review") or {}).get("summary") or {}

    print("\n全部报告生成完成。")
    print(f"输出目录: {OUTPUT_DIR}")
    print(f"项目总览: {summary_path}")
    print(f"商务标查重可疑对: {business_summary.get('suspicious_pair_count') or 0}")
    print(f"技术标查重可疑对: {technical_summary.get('suspicious_pair_count') or 0}")
    print(f"错别字候选: {bid_summary.get('typo_issue_count') or 0}")
    print(f"一人多用姓名: {bid_summary.get('reused_name_count') or 0}")


if __name__ == "__main__":
    main()
