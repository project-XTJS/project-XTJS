import os
from dotenv import load_dotenv

load_dotenv()


class MinioConfig:
    ENDPOINT = os.getenv("MINIO_ENDPOINT", "127.0.0.1:9000")
    ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME", "update_file")
    SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"
    URL_EXPIRE_DAYS = int(os.getenv("MINIO_PRESIGNED_EXPIRES_DAYS", "7"))
    MAX_FILE_SIZE = int(os.getenv("MINIO_MAX_FILE_SIZE", str(500 * 1024 * 1024)))
    ALLOWED_EXTENSIONS = {
        ext.strip().lower()
        for ext in os.getenv("MINIO_ALLOWED_EXTENSIONS", "pdf,docx,doc").split(",")
        if ext.strip()
    }
