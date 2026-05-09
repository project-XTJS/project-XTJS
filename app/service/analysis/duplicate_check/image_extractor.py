# -*- coding: utf-8 -*-
"""
图片提取与哈希（支持 PDF 和光栅图片）
"""
import hashlib
import io
from typing import Any

from app.service.minio_service import MinioService


def extract_document_images(
    record: dict[str, Any],
    *,
    role: str,
    minio_service: MinioService,
) -> list[dict[str, Any]]:
    """从文档记录中提取图片条目（仅技术标支持）。"""
    from app.core.document_types import DOCUMENT_TYPE_TECHNICAL_BID

    if role != DOCUMENT_TYPE_TECHNICAL_BID:
        return []

    file_url = str(record.get("file_url") or "").strip()
    if not file_url:
        return []

    # 图片 hash 缓存由 service 层管理，这里每次重新提取
    try:
        bucket_name, object_name = MinioService.bucket_and_object_from_file_url(file_url)
        file_bytes, content_type = minio_service.get_object_bytes(
            object_name, bucket_name=bucket_name,
        )
        images = _extract_image_entries_from_file_bytes(
            file_bytes,
            file_name=str(record.get("file_name") or object_name),
            content_type=content_type,
        )
    except Exception:
        images = []

    return images


def _extract_image_entries_from_file_bytes(
    file_bytes: bytes,
    *,
    file_name: str,
    content_type: str,
) -> list[dict[str, Any]]:
    """根据文件字节和类型提取图片条目。"""
    normalized_name = str(file_name or "").strip().lower()
    normalized_type = str(content_type or "").strip().lower()
    if normalized_name.endswith(".pdf") or "pdf" in normalized_type:
        return _extract_pdf_image_entries(file_bytes)
    return _extract_raster_image_entries(file_bytes)


def _extract_pdf_image_entries(file_bytes: bytes) -> list[dict[str, Any]]:
    """从 PDF 文件中提取所有图片并计算哈希。"""
    try:
        import fitz
    except Exception:
        return []

    entries_by_hash: dict[str, dict[str, Any]] = {}
    try:
        document = fitz.open(stream=file_bytes, filetype="pdf")
    except Exception:
        return []

    try:
        for page_index in range(document.page_count):
            page = document.load_page(page_index)
            seen_page_hashes: set[str] = set()
            for image_meta in page.get_images(full=True):
                xref = int(image_meta[0])
                try:
                    extracted = document.extract_image(xref)
                except Exception:
                    continue
                image_bytes = extracted.get("image")
                if not image_bytes:
                    continue
                image_entry = _build_image_entry(image_bytes=image_bytes, page=page_index + 1)
                if image_entry is None:
                    continue
                image_hash = str(image_entry.get("exact_hash") or "")
                if not image_hash or image_hash in seen_page_hashes:
                    continue
                seen_page_hashes.add(image_hash)
                existing = entries_by_hash.get(image_hash)
                if existing is None:
                    entries_by_hash[image_hash] = image_entry
                else:
                    merged_pages = sorted(
                        set(existing.get("pages") or []) | set(image_entry.get("pages") or [])
                    )
                    existing["pages"] = merged_pages
    finally:
        document.close()

    return sorted(entries_by_hash.values(), key=lambda item: (item.get("pages") or [10**9])[0])


def _extract_raster_image_entries(file_bytes: bytes) -> list[dict[str, Any]]:
    """将光栅图片（如 PNG/JPG）作为单张图片提取。"""
    image_entry = _build_image_entry(image_bytes=file_bytes, page=1)
    return [image_entry] if image_entry is not None else []


def _build_image_entry(
    *,
    image_bytes: bytes,
    page: int,
) -> dict[str, Any] | None:
    """根据图片字节构建图片条目，计算 SHA256 哈希。"""
    try:
        from PIL import Image
    except Exception:
        return None

    try:
        with Image.open(io.BytesIO(image_bytes)) as image:
            rgb = image.convert("RGB")
            width, height = rgb.size
            if width < 80 or height < 80 or (width * height) < 20000:
                return None
            pixel_bytes = rgb.tobytes()
    except Exception:
        return None

    exact_hash = hashlib.sha256(
        f"{width}x{height}|rgb|".encode("utf-8") + pixel_bytes
    ).hexdigest()
    return {
        "pages": [int(page)],
        "width": int(width),
        "height": int(height),
        "exact_hash": exact_hash,
    }