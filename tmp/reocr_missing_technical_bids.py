from __future__ import annotations

from pathlib import Path

from fastapi.encoders import jsonable_encoder
from psycopg2.extras import Json, RealDictCursor

from app.core.document_types import DOCUMENT_TYPE_TECHNICAL_BID
from app.service.analysis.duplicate_check import DuplicateCheckService
from app.service.ocr_service import OCRService
from app.service.postgresql_service import PostgreSQLService


PROJECT_IDENTIFIER = "船体广告"
SOURCE_DIR = Path(r"D:\Desktop\测试文件\2-投标文件")
TARGET_FILE_NAMES = (
    "阳生文化技术标.pdf",
    "善元技术标.pdf",
)


def _is_missing_ocr_content(document: dict) -> bool:
    content = document.get("content") or {}
    data = content.get("data") if isinstance(content.get("data"), dict) else content
    text_length = int(data.get("text_length") or 0)
    layout_sections = list(data.get("layout_sections") or [])
    native_tables = list(data.get("native_tables") or [])
    return text_length <= 0 and not layout_sections and not native_tables


def _update_document_content(
    db: PostgreSQLService,
    *,
    identifier_id: str,
    recognition_content: dict,
) -> None:
    with db._get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                """
                UPDATE xtjs_documents
                SET content = %s, extracted = TRUE, update_time = CURRENT_TIMESTAMP
                WHERE identifier_id = %s AND deleted = FALSE
                RETURNING identifier_id, file_name, extracted, update_time
                """,
                (Json(jsonable_encoder(recognition_content)), identifier_id),
            )
            updated = cursor.fetchone()
            if not updated:
                raise RuntimeError(f"Document not found while updating OCR content: {identifier_id}")


def _recognize_pdf(ocr_service: OCRService, source_path: Path) -> dict:
    ocr_result = ocr_service.extract_all(str(source_path), "pdf")
    raw_text = str(ocr_result.get("text") or "")
    pages = ocr_result.get("pages") or []
    layout_sections = ocr_result.get("layout_sections") or []
    native_tables = ocr_result.get("native_tables") or []
    logical_tables = ocr_result.get("logical_tables") or []
    seal_data = ocr_result.get("seals") or {"count": 0, "texts": []}
    signature_data = ocr_result.get("signatures") or {"count": 0, "texts": []}

    try:
        seal_count = int(seal_data.get("count", 0))
    except (TypeError, ValueError):
        seal_count = 0
    try:
        signature_count = int(signature_data.get("count", 0))
    except (TypeError, ValueError):
        signature_count = 0

    table_sections = [
        section
        for section in layout_sections
        if isinstance(section, dict) and str(section.get("type") or "").strip().lower() == "table"
    ]

    return {
        "filename": source_path.name,
        "text_length": len(raw_text),
        "page_count": len(pages) if isinstance(pages, list) else 0,
        "parser_engine": "PaddleOCR-VL-1.5",
        "source_mode": "local",
        "active_device": getattr(ocr_service, "active_device", "cpu"),
        "ocr_engine": "PaddleOCR-VL-1.5",
        "ocr_used": True,
        "layout_used": bool(layout_sections),
        "layout_sections": layout_sections,
        "layout_section_count": len(layout_sections),
        "table_sections": table_sections,
        "table_section_count": len(table_sections),
        "native_tables": native_tables,
        "native_table_count": len(native_tables),
        "logical_tables": logical_tables,
        "logical_table_count": len(logical_tables),
        "seal_detected": seal_count > 0,
        "seal_count": seal_count,
        "seal_texts": seal_data.get("texts", []),
        "seal_locations": seal_data.get("locations", []),
        "signature_detected": signature_count > 0,
        "signature_count": signature_count,
        "signature_texts": signature_data.get("texts", []),
        "signature_locations": signature_data.get("locations", []),
        "bbox_coordinate_space": ocr_result.get("bbox_coordinate_space", "ocr_image"),
        "bbox_source_coordinate_space": ocr_result.get("bbox_source_coordinate_space", "ocr_image"),
        "recognition_route": "paddleocr_vl",
        "recognition_reason": "vl_only_pipeline",
        "pdf_mode": "vl_only",
        "pdf_text_stats": {},
        "ppstructure_v3_requested": False,
        "ppstructure_v3_enabled": False,
        "seal_recognition_enabled": True,
    }


def main() -> None:
    db = PostgreSQLService()
    ocr_service = OCRService()
    if not bool(getattr(ocr_service, "available", False)):
        raise RuntimeError("OCRService is unavailable in the current environment.")
    duplicate_service = DuplicateCheckService()

    repaired: list[str] = []
    checked: list[str] = []

    for file_name in TARGET_FILE_NAMES:
        source_path = SOURCE_DIR / file_name
        if not source_path.exists():
            raise FileNotFoundError(f"Source PDF not found: {source_path}")

        documents = db.list_documents(limit=200, offset=0, keyword=file_name).get("items") or []
        document = next(
            (
                item
                for item in documents
                if item.get("document_type") == DOCUMENT_TYPE_TECHNICAL_BID
                and item.get("file_name") == file_name
            ),
            None,
        )
        if not document:
            raise RuntimeError(f"Technical bid document not found in DB: {file_name}")

        checked.append(file_name)
        if not _is_missing_ocr_content(document):
            print(f"[skip] OCR content already present: {file_name}")
            continue

        print(f"[ocr] Re-running OCR for: {file_name}")
        recognition_content = _recognize_pdf(ocr_service, source_path)

        _update_document_content(
            db,
            identifier_id=str(document["identifier_id"]),
            recognition_content=recognition_content,
        )
        repaired.append(file_name)
        print(
            f"[done] {file_name}: text_length={recognition_content.get('text_length')} "
            f"layout_sections={len(recognition_content.get('layout_sections') or [])} "
            f"native_tables={len(recognition_content.get('native_tables') or [])}"
        )

    payload = db.get_project_documents_for_duplicate_check(PROJECT_IDENTIFIER)
    if not payload:
        raise RuntimeError(f"Project not found: {PROJECT_IDENTIFIER}")

    result = duplicate_service.check_project_documents(
        project_identifier=PROJECT_IDENTIFIER,
        project=payload.get("project"),
        document_records=payload.get("documents") or [],
        document_types=[DOCUMENT_TYPE_TECHNICAL_BID],
        max_evidence_sections=20,
        max_pairs_per_type=0,
    )
    db.upsert_project_result_item(
        project_identifier_id=PROJECT_IDENTIFIER,
        result_key="technical_bid_duplicate_check",
        result_value=result,
    )

    technical_group = ((result.get("groups") or {}).get(DOCUMENT_TYPE_TECHNICAL_BID) or {})
    print("---")
    print(f"checked={checked}")
    print(f"repaired={repaired}")
    print(f"document_count={technical_group.get('document_count')}")
    print(f"pair_count={technical_group.get('pair_count')}")
    print(f"suspicious_pair_count={technical_group.get('suspicious_pair_count')}")
    for item in technical_group.get("items") or []:
        metrics = item.get("metrics") or {}
        print(
            "pair="
            f"{item.get('left_file_name')} <> {item.get('right_file_name')} "
            f"risk={item.get('risk_level')} score={item.get('match_score')} "
            f"exact=({metrics.get('exact_section_count')}, {metrics.get('exact_block_count')}, {metrics.get('exact_table_count')}, {metrics.get('exact_image_count')}) "
            f"similar=({metrics.get('similar_section_count')}, {metrics.get('similar_block_count')}, {metrics.get('similar_table_count')})"
        )


if __name__ == "__main__":
    main()
