from fastapi import APIRouter, UploadFile, File, HTTPException
import os
from app.service.document_service import DocumentService
from app.service.business_format.service import BusinessFormatService

router = APIRouter()

@router.post("/check")
async def check_business_format(file: UploadFile = File(...)):
    """商务标格式审查"""
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
        
        # 预处理文本
        processed_text = DocumentService.preprocess_text(text)
        
        # 执行格式检查
        result = BusinessFormatService.check_format(processed_text)
        
        return {
            "filename": file.filename,
            "file_type": file_extension,
            "result": result,
            "message": "Business format check completed successfully"
        }
    finally:
        # 清理临时文件
        DocumentService.cleanup_temp_file(temp_file_path)

@router.post("/validate-sections")
async def validate_business_sections(file: UploadFile = File(...)):
    """验证商务标章节内容"""
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
        
        # 预处理文本
        processed_text = DocumentService.preprocess_text(text)
        
        # 执行章节验证
        result = BusinessFormatService.validate_sections(processed_text)
        
        return {
            "filename": file.filename,
            "file_type": file_extension,
            "result": result,
            "message": "Business sections validation completed successfully"
        }
    finally:
        # 清理临时文件
        DocumentService.cleanup_temp_file(temp_file_path)