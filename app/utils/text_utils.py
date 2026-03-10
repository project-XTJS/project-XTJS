import os
import re
import tempfile
from typing import List

from docx import Document
from pdfplumber import open as open_pdf


def preprocess_text(text: str) -> str:
    """文本预处理：去首尾空白并压缩连续空白字符。"""
    return re.sub(r"\s+", " ", text.strip())


def extract_text_from_pdf(file_path: str) -> str:
    """从 PDF 中抽取可直接识别的文本层内容。"""
    text = ""
    with open_pdf(file_path) as pdf:
        for page in pdf.pages:
            text += page.extract_text() or ""
    return text


def extract_text_from_docx(file_path: str) -> str:
    """从 docx 文档中按段落抽取文本。"""
    doc = Document(file_path)
    return "\n".join(paragraph.text for paragraph in doc.paragraphs)


def extract_text(file_path: str, file_type: str) -> str:
    """根据文件类型分发到对应解析器。"""
    if file_type == "pdf":
        return extract_text_from_pdf(file_path)
    if file_type == "docx":
        return extract_text_from_docx(file_path)
    if file_type == "doc":
        raise ValueError("Legacy .doc is not supported. Please upload .docx instead.")
    raise ValueError(f"Unsupported file type: {file_type}")


def split_text(text: str, chunk_size: int = 1000, overlap: int = 150) -> List[str]:
    """
    文本切片：按固定窗口分段并保留重叠区，便于后续相似度/向量检索。
    """
    cleaned = preprocess_text(text)
    if not cleaned:
        return []

    if chunk_size <= 0:
        raise ValueError("chunk_size must be greater than 0")
    if overlap < 0 or overlap >= chunk_size:
        raise ValueError("overlap must be >= 0 and < chunk_size")

    chunks: List[str] = []
    start = 0
    while start < len(cleaned):
        # 逐段推进，并保留 overlap 长度上下文。
        end = min(start + chunk_size, len(cleaned))
        chunks.append(cleaned[start:end])
        if end == len(cleaned):
            break
        start = end - overlap
    return chunks


def save_temp_file(content: bytes, suffix: str) -> str:
    """将上传字节保存为临时文件并返回路径。"""
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as temp_file:
        temp_file.write(content)
        return temp_file.name


def cleanup_temp_file(file_path: str) -> None:
    """删除临时文件（存在时）。"""
    if os.path.exists(file_path):
        os.unlink(file_path)
