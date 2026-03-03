from fastapi import APIRouter, UploadFile, File, HTTPException
import os
from app.service.document_service import DocumentService
from app.service.technical_duplication.service import TechnicalDuplicationService

router = APIRouter()

@router.post("/check")
async def check_technical_duplication(file: UploadFile = File(...)):
    """技术标查重"""
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
        
        # 执行查重检查（这里使用空的历史文本列表，实际应用中应该从数据库获取）
        result = TechnicalDuplicationService.check_duplication(processed_text, [])
        
        return {
            "filename": file.filename,
            "file_type": file_extension,
            "result": result,
            "message": "Technical duplication check completed successfully"
        }
    finally:
        # 清理临时文件
        DocumentService.cleanup_temp_file(temp_file_path)

@router.post("/content-check")
async def check_technical_content(file: UploadFile = File(...)):
    """技术标内容检查"""
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
        
        # 执行内容检查
        result = TechnicalDuplicationService.check_technical_content(processed_text)
        
        return {
            "filename": file.filename,
            "file_type": file_extension,
            "result": result,
            "message": "Technical content check completed successfully"
        }
    finally:
        # 清理临时文件
        DocumentService.cleanup_temp_file(temp_file_path)

@router.post("/extract-params")
async def extract_technical_parameters(file: UploadFile = File(...)):
    """提取技术参数"""
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
        
        # 提取技术参数
        params = TechnicalDuplicationService.extract_technical_parameters(processed_text)
        
        return {
            "filename": file.filename,
            "file_type": file_extension,
            "parameters": params,
            "message": "Technical parameters extracted successfully"
        }
    finally:
        # 清理临时文件
        DocumentService.cleanup_temp_file(temp_file_path)