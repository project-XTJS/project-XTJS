import io
import uuid
from datetime import timedelta
from pathlib import Path
from typing import Optional
from urllib.parse import unquote, urlparse

from fastapi import UploadFile

from app.config.minio import ALLOWED_EXTENSIONS, MAX_FILE_SIZE, MinioConfig, minio_client


def validate_file_extension(filename: Optional[str]) -> str:
    if not filename:
        raise ValueError("filename is required")

    extension = Path(filename).suffix.lower().lstrip(".")
    if extension not in ALLOWED_EXTENSIONS:
        raise ValueError(f"unsupported file type: {extension}")
    return extension


def generate_object_name(filename: str) -> str:
    extension = Path(filename).suffix.lower()
    return f"documents/{uuid.uuid4().hex}{extension}"


def ensure_bucket() -> None:
    if not minio_client.bucket_exists(MinioConfig.BUCKET_NAME):
        minio_client.make_bucket(MinioConfig.BUCKET_NAME)


def upload_bytes(file_bytes: bytes, filename: str, content_type: Optional[str]) -> str:
    validate_file_extension(filename)
    if len(file_bytes) > MAX_FILE_SIZE:
        raise ValueError(f"file size exceeds {MAX_FILE_SIZE} bytes")

    ensure_bucket()
    object_name = generate_object_name(filename)
    minio_client.put_object(
        bucket_name=MinioConfig.BUCKET_NAME,
        object_name=object_name,
        data=io.BytesIO(file_bytes),
        length=len(file_bytes),
        content_type=content_type or "application/octet-stream",
    )
    return minio_client.presigned_get_object(
        bucket_name=MinioConfig.BUCKET_NAME,
        object_name=object_name,
        expires=timedelta(days=MinioConfig.PRESIGNED_EXPIRES_DAYS),
    )


async def upload_document_file(file: UploadFile) -> dict[str, str]:
    file_bytes = await file.read()
    file_url = upload_bytes(file_bytes, file.filename or "", file.content_type)
    return {
        "file_name": file.filename or "",
        "file_url": file_url,
    }


def delete_file_by_url(file_url: str) -> None:
    parsed = urlparse(file_url)
    path = unquote(parsed.path.lstrip("/"))
    if not path:
        return

    parts = path.split("/", 1)
    if len(parts) != 2:
        return

    bucket_name, object_name = parts
    if bucket_name != MinioConfig.BUCKET_NAME:
        return

    minio_client.remove_object(bucket_name, object_name)
