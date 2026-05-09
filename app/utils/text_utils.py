# -*- coding: utf-8 -*-
"""
文本处理工具模块。

提供文本预处理、分块、临时文件创建与清理等通用功能。
"""

import os
import re
import tempfile
from typing import List


def preprocess_text(text: str) -> str:
    """将文本中的连续空白字符压缩为单个空格，并去除首尾空白。"""
    if not text:
        return ""
    return re.sub(r"\s+", " ", text.strip())


def split_text(text: str, chunk_size: int = 1000, overlap: int = 150) -> List[str]:
    """
    将文本按固定大小分块，相邻块之间存在重叠区域。

    参数：
        text: 待分块的文本
        chunk_size: 每块的最大字符数
        overlap: 相邻块之间的重叠字符数

    返回：
        文本块列表
    """
    cleaned = preprocess_text(text)
    if not cleaned:
        return []

    if chunk_size <= 0:
        raise ValueError("chunk_size must be greater than 0")
    if overlap < 0 or overlap >= chunk_size:
        raise ValueError("overlap must be >= 0 and < chunk_size")

    chunks: List[str] = []
    text_length = len(cleaned)
    start = 0

    while start < text_length:
        end = min(start + chunk_size, text_length)
        chunks.append(cleaned[start:end])
        if end == text_length:
            break
        start = end - overlap

    return chunks


def save_temp_file(content: bytes, suffix: str) -> str:
    """将字节内容保存到临时文件，返回文件路径。"""
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as temp_file:
        temp_file.write(content)
        return temp_file.name


def cleanup_temp_file(file_path: str) -> None:
    """删除指定的临时文件，失败时仅打印警告。"""
    if not file_path:
        return
    try:
        if os.path.exists(file_path):
            os.unlink(file_path)
    except Exception as exc:
        print(f"Warning: Failed to cleanup temp file {file_path}: {exc}")