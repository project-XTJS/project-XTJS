"""Text analysis routes for OCR extraction and rule-based analysis."""

import html
import json
import os
import re
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile
from starlette.concurrency import run_in_threadpool

from app.router.dependencies import get_text_analysis_service
from app.schemas.analysis import TextAnalysisRequest
from app.schemas.recognition import build_analyze_file_metadata
from app.utils.text_utils import cleanup_temp_file, preprocess_text, save_temp_file

router = APIRouter()


def _clean_inline_text(value: Any) -> str:
    normalized = html.unescape(str(value or ""))
    normalized = normalized.replace("\u3000", " ")
    normalized = normalized.replace("\xa0", " ")
    normalized = re.sub(r"\s+", " ", normalized).strip()
    if not normalized:
        return ""
    normalized = re.sub(r"(?<=[\u4e00-\u9fff])\s+(?=[\u4e00-\u9fff])", "", normalized)
    normalized = re.sub(r"\s+([,.;:!?%])", r"\1", normalized)
    normalized = re.sub(r"\s+([，。；：！？、）】》])", r"\1", normalized)
    normalized = re.sub(r"([（【《])\s+", r"\1", normalized)
    return normalized.strip()


def _is_noise_line(line: str) -> bool:
    stripped = str(line or "").strip()
    if not stripped:
        return True
    if len(stripped) >= 2 and not re.search(r"[0-9A-Za-z\u4e00-\u9fff]", stripped):
        return True
    return False


def _build_clean_text(raw_value: Any) -> str:
    normalized = html.unescape(str(raw_value or ""))
    normalized = normalized.replace("\u3000", " ")
    normalized = normalized.replace("\xa0", " ")
    normalized = re.sub(r"\r\n?", "\n", normalized)
    normalized = normalized.replace("\t", " ")

    parts: list[str] = []
    for raw_line in normalized.splitlines():
        line = _clean_inline_text(raw_line)
        if not line or _is_noise_line(line):
            continue
        parts.append(line)

    return _clean_inline_text(" ".join(parts))


def _build_public_sections(sections: list[dict] | None) -> list[dict]:
    public_sections: list[dict] = []
    seen = set()

    for section in sections or []:
        if not isinstance(section, dict):
            continue
        text = _build_clean_text(section.get("raw_text") or section.get("text"))
        if not text:
            continue
        page = section.get("page")
        section_type = str(section.get("type") or "text").strip().lower() or "text"
        item = {"type": section_type, "text": text}
        if isinstance(page, int) and page > 0:
            item["page"] = page
        signature = (item.get("page"), section_type, text)
        if signature in seen:
            continue
        seen.add(signature)
        public_sections.append(item)

    return public_sections


def _build_public_logical_tables(logical_tables: list[dict] | None) -> list[dict]:
    public_tables: list[dict] = []
    for table in logical_tables or []:
        if not isinstance(table, dict):
            continue

        rows = table.get("rows") or []
        records = table.get("records") or []
        headers = table.get("headers") or []

        public_tables.append(
            {
                "id": str(table.get("id") or "").strip(),
                "pages": [
                    int(page)
                    for page in (table.get("pages") or [])
                    if isinstance(page, int) and page > 0
                ],
                "column_count": int(table.get("column_count") or 0),
                "header_row_count": int(table.get("header_row_count") or 0),
                "headers": [_clean_inline_text(item) for item in headers],
                "rows": [
                    [_clean_inline_text(cell) for cell in row]
                    for row in rows
                    if isinstance(row, list)
                ],
                "records": [
                    {
                        str(key): _clean_inline_text(value)
                        for key, value in record.items()
                    }
                    for record in records
                    if isinstance(record, dict)
                ],
                "continued": bool(table.get("continued")),
                "row_count": int(table.get("row_count") or 0),
                "data_row_count": int(table.get("data_row_count") or 0),
            }
        )
    return public_tables


def _build_analyze_file_response(
    upload: UploadFile,
    *,
    content: bytes,
    file_extension: str,
    extraction_result: dict,
) -> dict:
    metadata = build_analyze_file_metadata(
        filename=upload.filename,
        file_type=file_extension,
        file_size=len(content),
        page_count=extraction_result["page_count"],
        mime_type=upload.content_type or "",
        text_length=extraction_result["text_length"],
        parser_engine=extraction_result["parser_engine"],
        source_mode=extraction_result["source_mode"],
        ocr_engine=extraction_result["ocr_engine"],
        ocr_used=extraction_result["ocr_used"],
        layout_used=extraction_result["layout_used"],
        layout_section_count=extraction_result["layout_section_count"],
        recognition_route=extraction_result["recognition_route"],
        recognition_reason=extraction_result["recognition_reason"],
        pdf_mode=extraction_result["pdf_mode"],
        active_device=extraction_result["active_device"],
        seal_detected=extraction_result["seal_detected"],
        seal_count=extraction_result["seal_count"],
        ppstructure_v3_requested=extraction_result["ppstructure_v3_requested"],
        ppstructure_v3_enabled=extraction_result["ppstructure_v3_enabled"],
        seal_recognition_enabled=extraction_result["seal_recognition_enabled"],
    )
    public_layout_sections = _build_public_sections(extraction_result["layout_sections"])
    public_table_sections = [
        section for section in public_layout_sections if section.get("type") == "table"
    ]
    public_logical_tables = _build_public_logical_tables(extraction_result["logical_tables"])

    return {
        "filename": upload.filename,
        "file_type": file_extension,
        "file_size": len(content),
        "text_length": extraction_result["text_length"],
        "page_count": extraction_result["page_count"],
        "layout_sections": public_layout_sections,
        "table_sections": public_table_sections,
        "logical_tables": public_logical_tables,
        "recognition": {
            "route": extraction_result["recognition_route"],
            "parser_engine": extraction_result["parser_engine"],
            "ocr_engine": extraction_result["ocr_engine"],
            "ocr_used": extraction_result["ocr_used"],
            "layout_used": extraction_result["layout_used"],
        },
        "seal": {
            "detected": extraction_result["seal_detected"],
            "count": extraction_result["seal_count"],
            "texts": extraction_result["seal_texts"],
        },
        "metadata": metadata,
    }


def _coerce_source_path(raw_value: Any) -> Path | None:
    if raw_value is None:
        return None

    path_text = str(raw_value).strip().strip('"').strip("'")
    if not path_text:
        return None
    if not os.path.isabs(path_text):
        raise HTTPException(
            status_code=400,
            detail="source_paths_json must contain absolute file paths.",
        )
    return Path(path_text)


def _parse_source_paths_json(raw_value: str | None, expected_count: int) -> list[Path | None]:
    if expected_count <= 0:
        return []

    if raw_value is None or not str(raw_value).strip():
        return [None] * expected_count

    raw_text = str(raw_value).strip()
    try:
        parsed = json.loads(raw_text)
    except json.JSONDecodeError:
        if expected_count == 1:
            return [_coerce_source_path(raw_text)]
        raise HTTPException(
            status_code=400,
            detail="source_paths_json must be a JSON array aligned with uploaded files.",
        ) from None

    if isinstance(parsed, list):
        if len(parsed) != expected_count:
            raise HTTPException(
                status_code=400,
                detail="source_paths_json length must match the number of uploaded files.",
            )
        return [_coerce_source_path(item) for item in parsed]

    if expected_count == 1:
        return [_coerce_source_path(parsed)]

    raise HTTPException(
        status_code=400,
        detail="source_paths_json must be a JSON array when uploading multiple files.",
    )


def _resolve_source_path(upload: UploadFile, explicit_source_path: Path | None) -> Path | None:
    if explicit_source_path is not None:
        return explicit_source_path

    filename = str(upload.filename or "").strip()
    if not filename or "fakepath" in filename.lower():
        return None
    if not os.path.isabs(filename):
        return None
    return Path(filename)


def _build_save_result(
    status: str,
    *,
    json_path: Path | None = None,
    message: str | None = None,
) -> dict:
    result = {
        "status": status,
        "json_path": str(json_path) if json_path is not None else None,
    }
    if message:
        result["message"] = message
    return result


def _save_analyze_file_json(
    payload: dict,
    *,
    source_path: Path | None,
    enabled: bool,
) -> dict:
    if not enabled:
        return _build_save_result(
            "disabled",
            message="JSON persistence is disabled for this request.",
        )

    if source_path is None:
        return _build_save_result(
            "skipped",
            message="Source path is unavailable, so the analyzed JSON was not saved.",
        )

    target_path = source_path.with_suffix(".json")
    save_result = _build_save_result(
        "saved",
        json_path=target_path,
        message="Analyzed JSON saved beside the source file.",
    )
    serialized_payload = dict(payload)
    serialized_payload["save_result"] = save_result

    try:
        target_path.write_text(
            json.dumps(serialized_payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception as exc:
        return _build_save_result(
            "failed",
            json_path=target_path,
            message=str(exc),
        )

    return save_result


async def _analyze_single_upload(
    upload: UploadFile,
    *,
    analysis_service: Any,
    explicit_source_path: Path | None,
    save_json_to_source: bool,
) -> dict:
    allowed_extensions = set(analysis_service.get_supported_extensions())
    filename = str(upload.filename or "")
    file_extension = os.path.splitext(filename)[1].lower().lstrip(".")

    if file_extension not in allowed_extensions:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Unsupported file type: {file_extension}. "
                f"Supported types: {', '.join(sorted(allowed_extensions))}."
            ),
        )

    content = await upload.read()
    temp_file_path = save_temp_file(content, f".{file_extension}")

    try:
        extraction_result = await run_in_threadpool(
            analysis_service.extract_text_result,
            temp_file_path,
            file_extension,
        )
        payload = _build_analyze_file_response(
            upload,
            content=content,
            file_extension=file_extension,
            extraction_result=extraction_result,
        )
        payload["save_result"] = _save_analyze_file_json(
            payload,
            source_path=_resolve_source_path(upload, explicit_source_path),
            enabled=save_json_to_source,
        )
        return payload
    finally:
        cleanup_temp_file(temp_file_path)


@router.post("/analyze-file", summary="文档解析（抽取文本）")
async def analyze_file(
    file: list[UploadFile] = File(...),
    source_paths_json: str | None = Form(
        default=None,
        description=(
            "Optional JSON string or JSON array aligned with the uploaded files. "
            "Each path is used to save the analyzed JSON beside the source file."
        ),
    ),
    save_json_to_source: bool = Form(
        default=True,
        description=(
            "Whether to save each analyzed JSON beside the source file when a source "
            "path is available."
        ),
    ),
    analysis_service=Depends(get_text_analysis_service),
):
    """Analyze one or more uploaded files and optionally save the JSON beside each source."""
    uploads = [upload for upload in file if upload is not None]
    if not uploads:
        raise HTTPException(status_code=400, detail="No files were uploaded.")

    source_paths = _parse_source_paths_json(source_paths_json, len(uploads))

    if len(uploads) == 1:
        try:
            return await _analyze_single_upload(
                uploads[0],
                analysis_service=analysis_service,
                explicit_source_path=source_paths[0],
                save_json_to_source=save_json_to_source,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except RuntimeError as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc

    items: list[dict] = []
    success_count = 0

    for upload, source_path in zip(uploads, source_paths):
        try:
            result = await _analyze_single_upload(
                upload,
                analysis_service=analysis_service,
                explicit_source_path=source_path,
                save_json_to_source=save_json_to_source,
            )
        except ValueError as exc:
            items.append(
                {
                    "filename": str(upload.filename or ""),
                    "status": "failed",
                    "error": str(exc),
                }
            )
            continue
        except RuntimeError as exc:
            items.append(
                {
                    "filename": str(upload.filename or ""),
                    "status": "failed",
                    "error": str(exc),
                }
            )
            continue
        except HTTPException as exc:
            detail = exc.detail
            if not isinstance(detail, str):
                detail = json.dumps(detail, ensure_ascii=False)
            items.append(
                {
                    "filename": str(upload.filename or ""),
                    "status": "failed",
                    "error": detail,
                }
            )
            continue

        items.append(
            {
                "filename": str(upload.filename or ""),
                "status": "success",
                "result": result,
            }
        )
        success_count += 1

    failed_count = len(items) - success_count
    if failed_count == 0:
        overall_status = "success"
    elif success_count == 0:
        overall_status = "failed"
    else:
        overall_status = "partial_success"

    return {
        "status": overall_status,
        "total": len(uploads),
        "success": success_count,
        "failed": failed_count,
        "items": items,
    }


@router.post("/run", summary="统一文本分析接口")
async def run_text_analysis(
    payload: TextAnalysisRequest,
    analysis_service=Depends(get_text_analysis_service),
):
    """Dispatch text to the requested rule-based analysis module."""
    raw_text = payload.text or ""
    text = preprocess_text(raw_text)

    if payload.task_type == "integrity_check":
        return analysis_service.integrity.check_integrity(text)
    if payload.task_type == "pricing_reason":
        return analysis_service.reasonableness.check_price_reasonableness(text)
    if payload.task_type == "itemized_pricing":
        return analysis_service.itemized.check_itemized_logic(raw_text)
    if payload.task_type == "deviation_check":
        return analysis_service.deviation.check_technical_deviation(text)
    if payload.task_type == "full_analysis":
        return analysis_service.run_full_analysis(text, extraction_meta={})

    raise HTTPException(status_code=400, detail=f"Unsupported task type: {payload.task_type}")
