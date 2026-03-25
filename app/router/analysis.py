"""Text analysis routes."""

import os

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile
from starlette.concurrency import run_in_threadpool

from app.router.dependencies import (
    RecognitionOptions,
    get_query_recognition_options,
    get_text_analysis_service,
)
from app.schemas.analysis import TextAnalysisRequest
from app.schemas.recognition import build_analyze_file_metadata
from app.utils.text_utils import cleanup_temp_file, preprocess_text, save_temp_file

router = APIRouter()


@router.post("/analyze-file", summary="Analyze document and extract text")
async def analyze_file(
    file: UploadFile = File(...),
    recognition_options: RecognitionOptions = Depends(get_query_recognition_options),
    analysis_service=Depends(get_text_analysis_service),
):
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
        extraction_result = await run_in_threadpool(
            analysis_service.extract_text_result,
            temp_file_path,
            file_extension,
            recognition_options.use_ppstructure_v3,
            recognition_options.use_seal_recognition,
            recognition_options.use_signature_recognition,
            recognition_options.pdf_mode,
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
            seal_covered_text_count=extraction_result["seal_covered_text_count"],
            signature_detected=extraction_result["signature_detected"],
            signature_count=extraction_result["signature_count"],
            ppstructure_v3_requested=extraction_result["ppstructure_v3_requested"],
            ppstructure_v3_enabled=extraction_result["ppstructure_v3_enabled"],
            seal_recognition_enabled=extraction_result["seal_recognition_enabled"],
            signature_recognition_enabled=extraction_result["signature_recognition_enabled"],
        )

        return {
            "filename": file.filename,
            "file_type": file_extension,
            "file_size": len(content),
            "text_length": extraction_result["text_length"],
            "page_count": extraction_result["page_count"],
            "layout_sections": extraction_result["layout_sections"],
            "table_sections": extraction_result["table_sections"],
            "recognition": {
                "route": extraction_result["recognition_route"],
                "reason": extraction_result["recognition_reason"],
                "pdf_mode": extraction_result["pdf_mode"],
                "pdf_text_stats": extraction_result["pdf_text_stats"],
                "parser_engine": extraction_result["parser_engine"],
                "ocr_engine": extraction_result["ocr_engine"],
                "ocr_used": extraction_result["ocr_used"],
                "layout_used": extraction_result["layout_used"],
                "layout_section_count": extraction_result["layout_section_count"],
                "table_section_count": extraction_result["table_section_count"],
                "active_device": extraction_result["active_device"],
                "ppstructure_v3_requested": extraction_result["ppstructure_v3_requested"],
                "ppstructure_v3_enabled": extraction_result["ppstructure_v3_enabled"],
                "seal_recognition_enabled": extraction_result["seal_recognition_enabled"],
                "signature_recognition_enabled": extraction_result["signature_recognition_enabled"],
            },
            "seal": {
                "detected": extraction_result["seal_detected"],
                "count": extraction_result["seal_count"],
                "texts": extraction_result["seal_texts"],
                "covered_texts": extraction_result["seal_covered_texts"],
            },
            "signature": {
                "detected": extraction_result["signature_detected"],
                "count": extraction_result["signature_count"],
                "texts": extraction_result["signature_texts"],
            },
            "metadata": metadata,
        }
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    finally:
        cleanup_temp_file(temp_file_path)


@router.post("/run", summary="Run text analysis")
async def run_text_analysis(
    payload: TextAnalysisRequest,
    analysis_service=Depends(get_text_analysis_service),
):
    text = preprocess_text(payload.text)

    if payload.task_type == "integrity_check":
        return analysis_service.integrity.check_integrity(text)
    if payload.task_type == "pricing_reason":
        return analysis_service.reasonableness.check_price_reasonableness(text)
    if payload.task_type == "itemized_pricing":
        return analysis_service.itemized.check_itemized_logic(text)
    if payload.task_type == "deviation_check":
        return analysis_service.deviation.check_technical_deviation(text)
    if payload.task_type == "full_analysis":
        return analysis_service.run_full_analysis(text, extraction_meta={})

    raise HTTPException(status_code=400, detail=f"Unsupported task type: {payload.task_type}")
