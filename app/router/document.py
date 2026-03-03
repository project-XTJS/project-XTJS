from fastapi import APIRouter, UploadFile, File, HTTPException
import os
from app.service.document_service import DocumentService
from app.service.ocr_service import OCRService

router = APIRouter()

@router.post("/analyze")
async def analyze_document(file: UploadFile = File(...)):
    """上传并分析文档"""
    # 验证文件类型
    allowed_extensions = {"pdf", "docx", "doc"}
    file_extension = os.path.splitext(file.filename)[1].lower().lstrip('.')
    
    if file_extension not in allowed_extensions:
        raise HTTPException(
            status_code=400,
            detail="Unsupported file type. Please upload a PDF or Word file."
        )
    
    # 读取文件内容
    content = await file.read()
    
    # 保存临时文件
    temp_file_path = DocumentService.save_temp_file(content, f".{file_extension}")
    
    try:
        # 提取文本
        text = DocumentService.extract_text(temp_file_path, file_extension)
        
        # 如果是扫描PDF，使用OCR
        if file_extension == "pdf" and not text:
            ocr_service = OCRService()
            text = ocr_service.recognize_pdf(temp_file_path)
        
        # 预处理文本
        processed_text = DocumentService.preprocess_text(text)
        
        # 分析结果
        analysis_result = {
            "text_length": len(processed_text),
            "word_count": len(processed_text.split()),
            "sample_content": processed_text[:500] + "..." if len(processed_text) > 500 else processed_text
        }
        
        return {
            "filename": file.filename,
            "file_type": file_extension,
            "file_size": f"{len(content)} bytes",
            "content": processed_text,
            "analysis": analysis_result,
            "message": "Document analyzed successfully"
        }
    finally:
        # 清理临时文件
        DocumentService.cleanup_temp_file(temp_file_path)
