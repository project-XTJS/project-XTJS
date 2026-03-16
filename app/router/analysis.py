"""文本分析路由：统一承接文档解析与规则分析能力。"""

import os

from fastapi import APIRouter, File, HTTPException, UploadFile

from app.model.analysis import TextAnalysisRequest
from app.service.analysis_service import get_analysis_service
from app.utils.text_utils import cleanup_temp_file, preprocess_text, save_temp_file

router = APIRouter()


def _get_analysis_service():
    return get_analysis_service()


@router.post("/analyze-file", summary="文档解析（抽取文本）")
async def analyze_file(file: UploadFile = File(...)):
    """上传单个文档并返回抽取后的正文内容。"""
    analysis_service = _get_analysis_service()
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
    # 统一落本地临时文件，便于 PDF/Word/OCR 流程复用。
    temp_file_path = save_temp_file(content, f".{file_extension}")
    try:
        extraction_result = analysis_service.extract_text_result(temp_file_path, file_extension)
        text = extraction_result["content"]
        return {
            "code": 200,
            "message": "analyze success",
            "data": {
                "filename": file.filename,
                "file_type": file_extension,
                "file_size": len(content),
                "content": text,
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
            },
        }
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    finally:
        # 无论成功失败都清理临时文件，避免磁盘堆积。
        cleanup_temp_file(temp_file_path)


@router.post("/run", summary="统一文本分析接口")
async def run_text_analysis(payload: TextAnalysisRequest):
    """按 task_type 分发到统一分析服务。"""
    analysis_service = _get_analysis_service()
    text = preprocess_text(payload.text)

    # 任务路由集中在此，减少前端对多个 API 的耦合。
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

    return {"code": 200, "message": "ok", "data": result}
