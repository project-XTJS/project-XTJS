import hashlib
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

TENDER_PATH = Path("./ocr_results/船体广告/船体广告招标.json")
BUSINESS_BID_FILES = [
    Path("./ocr_results/船体广告/翡翠公主号-投标文件（商务标）1108.json"),
    Path("./ocr_results/船体广告/上海浦江游览船体广告位采购项目-善元-商务标.json"),
    Path("./ocr_results/船体广告/亚元-商务标.json"),
    Path("./ocr_results/船体广告/阳生文化1109-商务标.json"),
]
TECHNICAL_BID_FILES = [
    Path("./ocr_results/船体广告/翡翠公主号-投标文件（技术标）1108.json"),
    Path("./ocr_results/船体广告/上海浦江游览船体广告位采购项目-善元-技术标.json"),
    Path("./ocr_results/船体广告/亚元-技术标.json"),
    Path("./ocr_results/船体广告/阳生文化1109-技术标.json"),
]
OUTPUT_DIR = Path("./test_reports/船体广告")
EXTRA_SOURCE_ROOTS = [
    Path(r"D:\Desktop\测试文件\船体广告"),
]
ACTIVE_TEST_NAME = str(OUTPUT_DIR.name or TENDER_PATH.parent.name or TENDER_PATH.stem)

SOURCE_SEARCH_ROOTS = [
    Path("./ocr_results"),
    Path("./source_documents"),
    Path("./uploads"),
    Path("./data"),
    *[Path(item) for item in EXTRA_SOURCE_ROOTS],
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


def generate_report_for_bidder(
    visualizer: ReportVisualizer,
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

    project_review_section = visualizer.build_project_review_section(
        project_results,
        source_lookup=source_lookup,
        issue_pages=issue_pages,
        current_business_file=bidder_json_path.name,
    )
    html_report = visualizer.inject_project_review_section(html_report, project_review_section)

    with output_html_path.open("w", encoding="utf-8") as file:
        file.write(html_report)
    print(f"  已生成报告: {output_html_path}")


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    visualizer = ReportVisualizer()
    print(f"当前测试配置: {ACTIVE_TEST_NAME}")
    print(f"招标文件: {TENDER_PATH}")

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
    issue_pages = visualizer.write_project_issue_pages(
        output_dir=OUTPUT_DIR,
        project_identifier=project_identifier,
        project_results=project_results,
        source_lookup=source_lookup,
    )

    summary_html = visualizer.build_project_summary_html(
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
            visualizer=visualizer,
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
