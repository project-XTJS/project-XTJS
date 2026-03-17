"""文本分析路由：统一承接文档解析与规则分析能力。"""

import os
from fastapi import APIRouter, File, Form, HTTPException, UploadFile, Depends

from app.schemas.recognition import build_analyze_file_metadata
from app.schemas.analysis import TextAnalysisRequest
from app.utils.text_utils import cleanup_temp_file, preprocess_text, save_temp_file
from app.router.dependencies import get_text_analysis_service

router = APIRouter()

@router.post("/analyze-file", summary="文档解析（抽取文本）")
async def analyze_file(
    file: UploadFile = File(...),
    analysis_service = Depends(get_text_analysis_service)
):
    """上传单个文档并返回抽取后的正文内容。"""
    allowed_extensions = set(analysis_service.get_supported_extensions())
    file_extension = os.path.splitext(file.filename)[1].lower().lstrip(".")
    
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
        extraction_result = analysis_service.extract_text_result(temp_file_path, file_extension)
        text = extraction_result["content"]
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
            ocr_available=extraction_result["ocr_available"],
            active_device=extraction_result["active_device"],
            seal_enabled=extraction_result["seal_enabled"],
            seal_removed=extraction_result["seal_removed"],
            seal_detected=extraction_result["seal_detected"],
            seal_count=extraction_result["seal_count"],
            seal_texts=extraction_result["seal_texts"],
        )
        # 返回核心字典
        return {
            "filename": file.filename,
            "file_type": file_extension,
            "file_size": len(content),
            "content": text,
            "pages": extraction_result["pages"],
            "page_count": extraction_result["page_count"],
            "parser_engine": extraction_result["parser_engine"],
            "source_mode": extraction_result["source_mode"],
            "ocr_engine": extraction_result["ocr_engine"],
            "ocr_used": extraction_result["ocr_used"],
            "active_device": extraction_result["active_device"],
            "seal_enabled": extraction_result["seal_enabled"],
            "seal_removed": extraction_result["seal_removed"],
            "seal_detected": extraction_result["seal_detected"],
            "seal_count": extraction_result["seal_count"],
            "seal_texts": extraction_result["seal_texts"],
            "metadata": metadata,
        }
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    finally:
        cleanup_temp_file(temp_file_path)

@router.post("/run", summary="统一文本分析接口")
async def run_text_analysis(
    payload: TextAnalysisRequest,
    analysis_service = Depends(get_text_analysis_service)
):
    """按 task_type 分发到统一分析服务。"""
    text = preprocess_text(payload.text)

    if payload.task_type == "business_format":
        result = analysis_service.check_business_format(text)
    elif payload.task_type == "business_sections":
        result = analysis_service.validate_business_sections(text)
    elif payload.task_type == "technical_content":
        result = analysis_service.check_technical_content(text)
    elif payload.task_type == "extract_parameters":
        result = {"status": "success", "parameters": analysis_service.extract_parameters(text)}
    else:
        raise HTTPException(status_code=400, detail=f"Unsupported task type: {payload.task_type}")

    return result