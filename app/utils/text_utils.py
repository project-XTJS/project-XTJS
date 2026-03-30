import os
import re
import tempfile
from typing import List


def preprocess_text(text: str) -> str:
    """Normalize whitespace for downstream rule-based analysis."""
    if not text:
        return ""
    return re.sub(r"\s+", " ", text.strip())


def split_text(text: str, chunk_size: int = 1000, overlap: int = 150) -> List[str]:
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
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as temp_file:
        temp_file.write(content)
        return temp_file.name


def cleanup_temp_file(file_path: str) -> None:
    if not file_path:
        return
    try:
        if os.path.exists(file_path):
            os.unlink(file_path)
    except Exception as exc:
        print(f"Warning: Failed to cleanup temp file {file_path}: {exc}")
