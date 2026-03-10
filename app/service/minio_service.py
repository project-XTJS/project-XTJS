import os
from datetime import datetime, timedelta
from urllib.parse import urlparse

from fastapi import UploadFile
from minio import Minio
from minio.error import S3Error

from app.config.minio import MinioConfig


class MinioService:
    """MinIO 业务服务：负责上传、删除、对象名处理。"""

    def __init__(self) -> None:
        # 客户端在服务初始化时创建，供路由层复用。
        self.client = Minio(
            endpoint=MinioConfig.ENDPOINT,
            access_key=MinioConfig.ACCESS_KEY,
            secret_key=MinioConfig.SECRET_KEY,
            secure=MinioConfig.SECURE,
        )

    @staticmethod
    def _get_file_size(file: UploadFile) -> int:
        """通过文件指针计算上传文件大小。"""
        try:
            file_obj = file.file
            file_obj.seek(0, os.SEEK_END)
            size = file_obj.tell()
            file_obj.seek(0)
            return size
        except Exception:
            return 0

    def validate_upload_file(self, file: UploadFile) -> None:
        """统一校验文件大小、文件名与后缀合法性。"""
        size = self._get_file_size(file)
        if size <= 0 or size > MinioConfig.MAX_FILE_SIZE:
            raise ValueError(
                f"File size must be between 1B and {MinioConfig.MAX_FILE_SIZE // 1024 // 1024}MB."
            )

        if not file.filename:
            raise ValueError("Filename is required.")

        extension = os.path.splitext(file.filename)[1].lower().lstrip(".")
        if extension not in MinioConfig.ALLOWED_EXTENSIONS:
            raise ValueError(f"Unsupported file type: {extension}")

    @staticmethod
    def generate_object_name(filename: str, object_name: str | None = None) -> str:
        """生成对象名；未指定时以时间戳避免重名。"""
        if object_name:
            return object_name
        name, ext = os.path.splitext(filename)
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        return f"{name}_{timestamp}{ext}"

    def ensure_bucket(self) -> None:
        """确保业务桶存在，不存在时自动创建。"""
        if not self.client.bucket_exists(MinioConfig.BUCKET_NAME):
            self.client.make_bucket(MinioConfig.BUCKET_NAME)

    def upload_file(self, file: UploadFile, object_name: str | None = None) -> dict:
        """上传文件并返回对象名、预签名 URL、文件大小。"""
        self.validate_upload_file(file)
        object_name = self.generate_object_name(file.filename, object_name)
        size = self._get_file_size(file)
        if size <= 0:
            raise ValueError("Unable to read upload file size.")

        try:
            self.ensure_bucket()
            self.client.put_object(
                bucket_name=MinioConfig.BUCKET_NAME,
                object_name=object_name,
                data=file.file,
                length=size,
                content_type=file.content_type,
            )
            file_url = self.client.presigned_get_object(
                MinioConfig.BUCKET_NAME,
                object_name,
                expires=timedelta(days=MinioConfig.URL_EXPIRE_DAYS),
            )
            return {"object_name": object_name, "file_url": file_url, "size": size}
        except S3Error as exc:
            raise RuntimeError(f"MinIO upload failed: {exc}") from exc

    def delete_file(self, object_name: str) -> None:
        """按对象名删除文件。"""
        try:
            self.client.remove_object(MinioConfig.BUCKET_NAME, object_name)
        except S3Error as exc:
            raise RuntimeError(f"MinIO delete failed: {exc}") from exc

    @staticmethod
    def object_name_from_presigned_url(file_url: str) -> str:
        """从预签名 URL 反解对象名，供删除流程使用。"""
        parsed = urlparse(file_url)
        path = parsed.path.lstrip("/")
        parts = path.split("/", 1)
        if len(parts) != 2:
            raise ValueError("Invalid MinIO file url.")
        return parts[1]
