import os
import tempfile
from typing import Dict, List, Any
from pdfplumber import open as open_pdf
from docx import Document

class DocumentService:
    @staticmethod
    def extract_text_from_pdf(file_path: str) -> str:
        """从PDF文件中提取文本"""
        text = ""
        with open_pdf(file_path) as pdf:
            for page in pdf.pages:
                text += page.extract_text() or ""
        return text

    @staticmethod
    def extract_text_from_docx(file_path: str) -> str:
        """从Word文件中提取文本"""
        doc = Document(file_path)
        text = ""
        for paragraph in doc.paragraphs:
            text += paragraph.text + "\n"
        return text

    @staticmethod
    def extract_text(file_path: str, file_type: str) -> str:
        """根据文件类型提取文本"""
        if file_type == "pdf":
            return DocumentService.extract_text_from_pdf(file_path)
        elif file_type in ["docx", "doc"]:
            return DocumentService.extract_text_from_docx(file_path)
        else:
            return ""

    @staticmethod
    def preprocess_text(text: str) -> str:
        """预处理文本，去除无意义词和空白"""
        # 简单的文本预处理
        text = text.strip()
        # 去除多余的空白字符
        import re
        text = re.sub(r'\s+', ' ', text)
        return text

    @staticmethod
    def save_temp_file(content: bytes, suffix: str) -> str:
        """保存临时文件"""
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as temp_file:
            temp_file.write(content)
            return temp_file.name

    @staticmethod
    def cleanup_temp_file(file_path: str):
        """清理临时文件"""
        if os.path.exists(file_path):
            os.unlink(file_path)
