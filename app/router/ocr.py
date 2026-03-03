from fastapi import APIRouter, UploadFile, File, HTTPException
import os
from app.service.ocr_service import OCRService
from app.service.document_service import DocumentService

router = APIRouter()

@router.post("/recognize")
async def recognize_text(file: UploadFile = File(...)):
    """识别图片中的文本"""
    # 验证文件类型
    allowed_extensions = {"jpg", "jpeg", "png", "bmp"}
    file_extension = os.path.splitext(file.filename)[1].lower().lstrip('.')
    
    if file_extension not in allowed_extensions:
        raise HTTPException(
            status_code=400,
            detail="Unsupported file type. Please upload an image file (jpg, jpeg, png, bmp)."
        )
    
    # 读取文件内容
    content = await file.read()
    
    # 初始化OCR服务
    ocr_service = OCRService()
    
    # 识别文本
    text = ocr_service.recognize_bytes(content)
    
    return {
        "filename": file.filename,
        "file_type": file_extension,
        "content": text,
        "message": "Text recognized successfully"
    }

@router.post("/pdf_to_text")
async def pdf_to_text(file: UploadFile = File(...)):
    """将PDF文件转换为文本（使用OCR）"""
    # 验证文件类型
    file_extension = os.path.splitext(file.filename)[1].lower().lstrip('.')
    
    if file_extension != "pdf":
        raise HTTPException(
            status_code=400,
            detail="Unsupported file type. Please upload a PDF file."
        )
    
    # 读取文件内容
    content = await file.read()
    
    # 保存临时文件
    temp_file_path = DocumentService.save_temp_file(content, ".pdf")
    
    try:
        # 初始化OCR服务
        ocr_service = OCRService()
        
        # 识别文本
        text = ocr_service.recognize_pdf(temp_file_path)
        
        return {
            "filename": file.filename,
            "file_type": file_extension,
            "content": text,
            "message": "PDF converted to text successfully"
        }
    finally:
        # 清理临时文件
        DocumentService.cleanup_temp_file(temp_file_path)
