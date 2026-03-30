# -*- coding: utf-8 -*-
from pathlib import Path
from typing import Set

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_ROOT = Path(__file__).resolve().parents[2]


class Settings(BaseSettings):
    DATABASE_URL: str = "postgresql://admin:password@localhost:5432/xtjs_db"

    MINIO_ENDPOINT: str = "127.0.0.1:9000"
    MINIO_ACCESS_KEY: str = "minioadmin"
    MINIO_SECRET_KEY: str = "minioadmin"
    MINIO_BUCKET_NAME: str = "update-file"
    MINIO_SECURE: bool = False
    MINIO_PRESIGNED_EXPIRES_DAYS: int = 7
    MINIO_MAX_FILE_SIZE: int = 500 * 1024 * 1024
    MINIO_ALLOWED_EXTENSIONS_STR: str = Field(
        default="pdf,png,jpg,jpeg,bmp,tif,tiff",
        validation_alias="MINIO_ALLOWED_EXTENSIONS",
    )

    @property
    def minio_allowed_extensions(self) -> Set[str]:
        return {
            ext.strip().lower()
            for ext in self.MINIO_ALLOWED_EXTENSIONS_STR.split(",")
            if ext.strip()
        }

    OCR_STORAGE_ROOT: Path = PROJECT_ROOT / ".ocr_runtime"
    PADDLE_PDX_CACHE_HOME: Path | None = None
    OCR_RUNTIME_TEMP_DIR: Path | None = None
    PADDLE_PDX_DISABLE_MODEL_SOURCE_CHECK: bool = True

    PADDLE_OCR_DEVICE: str = "gpu:0"
    PADDLE_OCR_DEVICE_POOL: str = " "
    PADDLE_OCR_MAX_INFLIGHT_PER_DEVICE: int = 1
    PADDLE_OCR_MULTI_GPU_LOG_SCHEDULING: bool = False
    PADDLE_OCR_FALLBACK_TO_CPU: bool = True
    PADDLE_OCR_USE_DOC_ORIENTATION: bool = True
    PADDLE_OCR_USE_DOC_UNWARPING: bool = True

    PADDLE_VL_PIPELINE_VERSION: str = "v1.5"
    PADDLE_VL_USE_LAYOUT_DETECTION: bool = True
    PADDLE_VL_USE_CHART_RECOGNITION: bool = False
    PADDLE_VL_USE_SEAL_RECOGNITION: bool = True
    PADDLE_VL_USE_OCR_FOR_IMAGE_BLOCK: bool = True
    PADDLE_VL_FORMAT_BLOCK_CONTENT: bool = False
    PADDLE_VL_MERGE_LAYOUT_BLOCKS: bool = False
    PADDLE_VL_USE_QUEUES: bool = False
    PADDLE_VL_RESTRUCTURE_PAGES: bool = True

    OCR_PROGRESS_ENABLED: bool = True
    OCR_PROGRESS_BAR_WIDTH: int = 24
    OCR_PROGRESS_KEEP_RECENT_UPDATES: int = 12
    OCR_PROGRESS_HEARTBEAT_SECONDS: float = 30.0

    PROJECT_BATCH_MIN_BID_GROUPS: int = 1
    PROJECT_BATCH_MAX_BID_GROUPS: int = 0
    PROJECT_BATCH_MIN_BID_FILES: int = 1
    PROJECT_BATCH_MAX_BID_FILES: int = 0

    CELERY_BROKER_URL: str = "redis://localhost:6379/0"
    CELERY_RESULT_BACKEND: str = "redis://localhost:6379/0"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


settings = Settings()

if not settings.PADDLE_PDX_CACHE_HOME:
    settings.PADDLE_PDX_CACHE_HOME = settings.OCR_STORAGE_ROOT / "paddlex-cache"
if not settings.OCR_RUNTIME_TEMP_DIR:
    settings.OCR_RUNTIME_TEMP_DIR = settings.OCR_STORAGE_ROOT / "runtime-tmp"
