import os
import logging
from datetime import datetime, timedelta
from urllib.parse import parse_qs, urlparse
from uuid import uuid4

from fastapi import UploadFile
from minio import Minio
from minio.error import S3Error

from app.config.minio import MinioConfig

logger = logging.getLogger(__name__)


class MinioService:
    """Service layer for MinIO object operations."""

    def __init__(self) -> None:
        self.client = Minio(
            endpoint=MinioConfig.ENDPOINT,
            access_key=MinioConfig.ACCESS_KEY,
            secret_key=MinioConfig.SECRET_KEY,
            secure=MinioConfig.SECURE,
        )
        self.bucket_name = MinioConfig.BUCKET_NAME

    def _audit(
        self,
        action: str,
        status: str,
        object_name: str | None = None,
        detail: str | None = None,
    ) -> None:
        logger.info(
            "minio_audit action=%s status=%s bucket=%s object_name=%s endpoint=%s detail=%s",
            action,
            status,
            self.bucket_name,
            object_name or "",
            MinioConfig.ENDPOINT,
            detail or "",
        )

    @staticmethod
    def _get_file_size(file: UploadFile) -> int:
        try:
            file_obj = file.file
            file_obj.seek(0, os.SEEK_END)
            size = file_obj.tell()
            file_obj.seek(0)
            return size
        except Exception:
            return 0

    def validate_upload_file(self, file: UploadFile) -> None:
        size = self._get_file_size(file)
        if size <= 0 or size > MinioConfig.MAX_FILE_SIZE:
            max_mb = MinioConfig.MAX_FILE_SIZE // 1024 // 1024
            raise ValueError(f"File size must be between 1B and {max_mb}MB")

        if not file.filename:
            raise ValueError("File name cannot be empty")

        extension = os.path.splitext(file.filename)[1].lower().lstrip(".")
        if extension not in MinioConfig.ALLOWED_EXTENSIONS:
            raise ValueError(f"Unsupported file type: {extension}")

    @staticmethod
    def generate_object_name(filename: str, object_name: str | None = None) -> str:
        if object_name:
            return object_name
        basename = os.path.basename(filename or "file")
        name, ext = os.path.splitext(basename)
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")
        unique_suffix = uuid4().hex[:8]
        return f"{name}_{timestamp}_{unique_suffix}{ext}"

    @staticmethod
    def build_file_url(object_name: str, bucket_name: str | None = None) -> str:
        if not object_name or not object_name.strip():
            raise ValueError("Object name cannot be empty")

        resolved_bucket_name = (bucket_name or MinioConfig.BUCKET_NAME).strip()
        if not resolved_bucket_name:
            raise ValueError("Bucket name cannot be empty")

        normalized_object_name = object_name.lstrip("/")
        return f"minio://{resolved_bucket_name}/{normalized_object_name}"

    @staticmethod
    def is_presigned_url(file_url: str) -> bool:
        if not file_url:
            return False

        parsed = urlparse(file_url)
        if parsed.scheme not in {"http", "https"}:
            return False

        query = parse_qs(parsed.query)
        signature_fields = {
            "X-Amz-Signature",
            "X-Amz-Algorithm",
            "X-Amz-Credential",
            "X-Amz-Expires",
        }
        return any(field in query for field in signature_fields)

    def _object_exists(self, object_name: str) -> bool:
        try:
            self.client.stat_object(self.bucket_name, object_name)
            return True
        except S3Error as exc:
            if exc.code in {"NoSuchKey", "NoSuchObject"}:
                return False
            raise RuntimeError(f"Failed to check object existence: {exc}") from exc
        except Exception as exc:
            raise RuntimeError(f"Error while checking object existence: {exc}") from exc

    def _resolve_upload_object_name(self, filename: str, object_name: str | None) -> str:
        if object_name:
            if self._object_exists(object_name):
                raise ValueError(f"Object already exists: {object_name}")
            return object_name

        for _ in range(5):
            generated = self.generate_object_name(filename)
            if not self._object_exists(generated):
                return generated
        raise RuntimeError("Failed to generate a unique object name")

    def ensure_bucket(self) -> None:
        if not self.bucket_name or not self.bucket_name.strip():
            raise RuntimeError("Fixed MinIO bucket name is empty")

        try:
            exists = self.client.bucket_exists(self.bucket_name)
            if exists:
                self._audit(action="ensure_bucket", status="exists")
                return

            self.client.make_bucket(self.bucket_name)
            self._audit(action="ensure_bucket", status="created")
        except S3Error as exc:
            self._audit(
                action="ensure_bucket",
                status="failed",
                detail=f"{type(exc).__name__}: {exc}",
            )
            raise RuntimeError(f"Failed to check/create MinIO bucket: {exc}") from exc
        except Exception as exc:
            self._audit(
                action="ensure_bucket",
                status="failed",
                detail=f"{type(exc).__name__}: {exc}",
            )
            raise RuntimeError(f"MinIO bucket operation error: {exc}") from exc

    def upload_file(self, file: UploadFile, object_name: str | None = None) -> dict:
        self.validate_upload_file(file)
        size = self._get_file_size(file)
        if size <= 0:
            raise ValueError("Failed to read upload file size")

        try:
            self.ensure_bucket()
            object_name = self._resolve_upload_object_name(file.filename, object_name)
            self.client.put_object(
                bucket_name=self.bucket_name,
                object_name=object_name,
                data=file.file,
                length=size,
                content_type=file.content_type,
            )
            presigned_url = self.get_presigned_url(object_name)
            file_url = self.build_file_url(object_name, self.bucket_name)
            self._audit(action="upload_file", status="success", object_name=object_name)
            return {
                "object_name": object_name,
                "bucket_name": self.bucket_name,
                "file_url": file_url,
                "presigned_url": presigned_url,
                "size": size,
            }
        except ValueError:
            raise
        except RuntimeError:
            raise
        except S3Error as exc:
            self._audit(
                action="upload_file",
                status="failed",
                object_name=object_name,
                detail=f"{type(exc).__name__}: {exc}",
            )
            raise RuntimeError(f"MinIO upload failed: {exc}") from exc
        except Exception as exc:
            self._audit(
                action="upload_file",
                status="failed",
                object_name=object_name,
                detail=f"{type(exc).__name__}: {exc}",
            )
            raise RuntimeError(f"MinIO upload error: {exc}") from exc

    def get_presigned_url(self, object_name: str) -> str:
        if not object_name or not object_name.strip():
            raise ValueError("Object name cannot be empty")

        try:
            self.ensure_bucket()
            presigned_url = self.client.presigned_get_object(
                self.bucket_name,
                object_name,
                expires=timedelta(days=MinioConfig.URL_EXPIRE_DAYS),
            )
            self._audit(action="get_presigned_url", status="success", object_name=object_name)
            return presigned_url
        except S3Error as exc:
            self._audit(
                action="get_presigned_url",
                status="failed",
                object_name=object_name,
                detail=f"{type(exc).__name__}: {exc}",
            )
            raise RuntimeError(f"Failed to generate presigned URL: {exc}") from exc
        except Exception as exc:
            self._audit(
                action="get_presigned_url",
                status="failed",
                object_name=object_name,
                detail=f"{type(exc).__name__}: {exc}",
            )
            raise RuntimeError(f"Error while generating presigned URL: {exc}") from exc

    def delete_file(self, object_name: str) -> None:
        if not object_name or not object_name.strip():
            raise ValueError("Object name cannot be empty")

        try:
            self.ensure_bucket()
            self.client.remove_object(self.bucket_name, object_name)
            self._audit(action="delete_file", status="success", object_name=object_name)
        except S3Error as exc:
            if exc.code in {"NoSuchKey", "NoSuchObject"}:
                self._audit(
                    action="delete_file",
                    status="not_found",
                    object_name=object_name,
                    detail=f"{type(exc).__name__}: {exc}",
                )
                raise ValueError(f"Object not found: {object_name}") from exc
            self._audit(
                action="delete_file",
                status="failed",
                object_name=object_name,
                detail=f"{type(exc).__name__}: {exc}",
            )
            raise RuntimeError(f"MinIO delete failed: {exc}") from exc
        except Exception as exc:
            self._audit(
                action="delete_file",
                status="failed",
                object_name=object_name,
                detail=f"{type(exc).__name__}: {exc}",
            )
            raise RuntimeError(f"MinIO delete error: {exc}") from exc

    @staticmethod
    def object_name_from_presigned_url(file_url: str) -> str:
        _, object_name = MinioService.bucket_and_object_from_presigned_url(file_url)
        return object_name

    @staticmethod
    def bucket_name_from_presigned_url(file_url: str) -> str:
        bucket_name, _ = MinioService.bucket_and_object_from_presigned_url(file_url)
        return bucket_name

    @staticmethod
    def bucket_and_object_from_presigned_url(file_url: str) -> tuple[str, str]:
        parsed = urlparse(file_url)
        if parsed.scheme not in {"http", "https"}:
            raise ValueError("Invalid MinIO presigned URL")

        path = parsed.path.lstrip("/")
        parts = path.split("/", 1)
        if len(parts) != 2 or not parts[0] or not parts[1]:
            raise ValueError("Invalid MinIO presigned URL: missing bucket/object")
        return parts[0], parts[1]

    @staticmethod
    def object_name_from_file_url(file_url: str) -> str:
        _, object_name = MinioService.bucket_and_object_from_file_url(file_url)
        return object_name

    @staticmethod
    def bucket_name_from_file_url(file_url: str) -> str:
        bucket_name, _ = MinioService.bucket_and_object_from_file_url(file_url)
        return bucket_name

    @staticmethod
    def bucket_and_object_from_file_url(file_url: str) -> tuple[str, str]:
        parsed = urlparse(file_url)
        if parsed.scheme != "minio":
            raise ValueError("Invalid MinIO storage URL")

        bucket_name = parsed.netloc.strip()
        if not bucket_name:
            raise ValueError("Invalid MinIO storage URL: missing bucket")

        object_name = parsed.path.lstrip("/")
        if not object_name:
            raise ValueError("Invalid MinIO storage URL: missing object name")
        return bucket_name, object_name
