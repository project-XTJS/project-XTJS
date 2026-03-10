import os

from minio import Minio


class MinioConfig:
    ENDPOINT = os.getenv("MINIO_ENDPOINT", "127.0.0.1:9000")
    ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME", "tendering-files")
    SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"
    PRESIGNED_EXPIRES_DAYS = int(os.getenv("MINIO_PRESIGNED_EXPIRES_DAYS", "7"))


ALLOWED_EXTENSIONS = {"pdf", "docx", "doc"}
MAX_FILE_SIZE = 500 * 1024 * 1024


minio_client = Minio(
    endpoint=MinioConfig.ENDPOINT,
    access_key=MinioConfig.ACCESS_KEY,
    secret_key=MinioConfig.SECRET_KEY,
    secure=MinioConfig.SECURE,
)
