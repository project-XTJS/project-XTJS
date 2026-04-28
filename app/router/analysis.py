"""OCR 抽取与规则分析路由。"""

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
from app.service.table_parser import build_logical_tables, build_table_structure
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
        bbox = section.get("bbox") or section.get("box")
        if isinstance(bbox, (list, tuple)) and bbox:
            item["bbox"] = _build_public_native_table_value(list(bbox))
        bbox_ocr = section.get("bbox_ocr")
        if isinstance(bbox_ocr, (list, tuple)) and bbox_ocr:
            item["bbox_ocr"] = _build_public_native_table_value(list(bbox_ocr))
        signature = (item.get("page"), section_type, text, str(item.get("bbox")))
        if signature in seen:
            continue
        seen.add(signature)
        public_sections.append(item)

    return public_sections


def _build_public_native_table_value(value: Any) -> Any:
    if value is None or isinstance(value, (int, float, bool)):
        return value
    if isinstance(value, str):
        normalized = html.unescape(value)
        normalized = normalized.replace("\u3000", " ")
        normalized = normalized.replace("\xa0", " ")
        normalized = normalized.replace("\r\n", "\n").replace("\r", "\n")
        return normalized.strip()
    if isinstance(value, list):
        return [_build_public_native_table_value(item) for item in value]
    if isinstance(value, dict):
        return {
            str(key): _build_public_native_table_value(item)
            for key, item in value.items()
        }
    return html.unescape(str(value))


def _build_public_logical_tables(tables: list[dict] | None) -> list[dict]:
    public_tables: list[dict] = []
    for table in tables or []:
        if not isinstance(table, dict):
            continue
        public_tables.append(_build_public_native_table_value(table))
    return public_tables


def _rebuild_logical_tables_from_native(native_tables: list[dict] | None) -> list[dict]:
    layout_sections: list[dict] = []
    for table in native_tables or []:
        if not isinstance(table, dict):
            continue
        block_content = str(table.get("block_content") or "").strip()
        if "<table" not in block_content.lower():
            continue
        table_structure = build_table_structure(html_parts=[block_content], raw_text="")
        if not isinstance(table_structure, dict):
            continue
        layout_sections.append(
            {
                "type": "table",
                "page": table.get("page"),
                "raw_text": block_content,
                "html": block_content,
                "table_structure": table_structure,
            }
        )
    if not layout_sections:
        return []
    return build_logical_tables(layout_sections)


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
    rebuilt_logical_tables = _rebuild_logical_tables_from_native(extraction_result.get("native_tables"))
    public_native_tables = _build_public_logical_tables(extraction_result.get("native_tables"))
    public_logical_tables = _build_public_logical_tables(
        rebuilt_logical_tables or extraction_result.get("logical_tables") or extraction_result.get("native_tables")
    )

    return {
        "filename": upload.filename,
        "file_type": file_extension,
        "file_size": len(content),
        "text_length": extraction_result["text_length"],
        "page_count": extraction_result["page_count"],
        "layout_sections": public_layout_sections,
        "logical_tables": public_logical_tables,
        "native_tables": public_native_tables,
        "recognition": {
            "route": extraction_result["recognition_route"],
            "parser_engine": extraction_result["parser_engine"],
            "ocr_engine": extraction_result["ocr_engine"],
            "ocr_used": extraction_result["ocr_used"],
            "layout_used": extraction_result["layout_used"],
            "bbox_coordinate_space": extraction_result.get("bbox_coordinate_space", "ocr_image"),
            "bbox_source_coordinate_space": extraction_result.get("bbox_source_coordinate_space", "ocr_image"),
        },
        "seal": {
            "detected": extraction_result["seal_detected"],
            "count": extraction_result["seal_count"],
            "texts": extraction_result["seal_texts"],
            "locations": _build_public_native_table_value(extraction_result.get("seal_locations") or []),
        },
        "signature": {
            "detected": extraction_result["signature_detected"],
            "count": extraction_result["signature_count"],
            "texts": extraction_result["signature_texts"],
            "locations": _build_public_native_table_value(extraction_result.get("signature_locations") or []),
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
            detail="source_paths_json 必须填写绝对路径。",
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
            detail="source_paths_json 必须是与上传文件一一对应的 JSON 数组。",
        ) from None

    if isinstance(parsed, list):
        if len(parsed) != expected_count:
            raise HTTPException(
                status_code=400,
                detail="source_paths_json 的长度必须与上传文件数量一致。",
            )
        return [_coerce_source_path(item) for item in parsed]

    if expected_count == 1:
        return [_coerce_source_path(parsed)]

    raise HTTPException(
        status_code=400,
        detail="上传多个文件时，source_paths_json 必须是 JSON 数组。",
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
            message="当前请求未启用 JSON 保存。",
        )

    if source_path is None:
        return _build_save_result(
            "skipped",
            message="未提供可用的源文件路径，因此未保存解析后的 JSON。",
        )

    target_path = source_path.with_suffix(".json")
    save_result = _build_save_result(
        "saved",
        json_path=target_path,
        message="解析后的 JSON 已保存到源文件同目录。",
    )
    serialized_payload = dict(payload)

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
                f"不支持的文件类型：{file_extension}。"
                f"支持的类型：{', '.join(sorted(allowed_extensions))}。"
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
        _save_analyze_file_json(
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
            "可选的 JSON 字符串或 JSON 数组，需要与上传文件一一对应。"
            "每个路径都会用于将解析后的 JSON 保存到对应源文件同目录。"
        ),
    ),
    save_json_to_source: bool = Form(
        default=True,
        description=(
            "当存在可用源文件路径时，是否将每个解析结果 JSON 保存到源文件同目录。"
        ),
    ),
    analysis_service=Depends(get_text_analysis_service),
):
    """解析一个或多个上传文件，并可选择将 JSON 保存到源文件同目录。"""
    uploads = [upload for upload in file if upload is not None]
    if not uploads:
        raise HTTPException(status_code=400, detail="未上传任何文件。")

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
    """将文本分发到指定的规则分析模块。"""
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

    raise HTTPException(status_code=400, detail=f"不支持的任务类型：{payload.task_type}")
