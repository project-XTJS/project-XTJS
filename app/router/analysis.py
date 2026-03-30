"""文本分析路由：负责文件解析与规则分析分发。"""

import html
import os
import re
from typing import Any

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile
from starlette.concurrency import run_in_threadpool

from app.router.dependencies import (
    get_text_analysis_service,
)
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


@router.post("/analyze-file", summary="文档解析（抽取文本）")
async def analyze_file(
    file: UploadFile = File(...),
    analysis_service=Depends(get_text_analysis_service),
):
    """上传单个文件并返回识别结果。"""
    allowed_extensions = set(analysis_service.get_supported_extensions())
    file_extension = os.path.splitext(file.filename)[1].lower().lstrip(".")

    # 先做扩展名校验，避免无效文件进入耗时识别流程。
    if file_extension not in allowed_extensions:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Unsupported file type: {file_extension}. "
                f"Supported types: {', '.join(sorted(allowed_extensions))}."
            ),
        )

    content = await file.read()
    temp_file_path = save_temp_file(content, f".{file_extension}")

    try:
        # 识别调用放入线程池，避免阻塞事件循环。
        extraction_result = await run_in_threadpool(
            analysis_service.extract_text_result,
            temp_file_path,
            file_extension,
        )
        metadata = build_analyze_file_metadata(
            filename=file.filename,
            file_type=file_extension,
            file_size=len(content),
            page_count=extraction_result["page_count"],
            mime_type=file.content_type or "",
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
            "filename": file.filename,
            "file_type": file_extension,
            "file_size": len(content),
            "text_length": extraction_result["text_length"],
            "page_count": extraction_result["page_count"],
            "signature_trace_present": extraction_result["signature_trace_present"],
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
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    finally:
        # 无论成功失败都清理临时文件。
        cleanup_temp_file(temp_file_path)


@router.post("/run", summary="统一文本分析接口")
async def run_text_analysis(
    payload: TextAnalysisRequest,
    analysis_service=Depends(get_text_analysis_service),
):
    """按 task_type 分发到对应分析模块。"""
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
