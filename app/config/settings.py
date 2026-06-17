# -*- coding: utf-8 -*-
import os
from pathlib import Path
from typing import Set

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _default_ocr_storage_root() -> Path:
    local_appdata = str(os.getenv("LOCALAPPDATA", "") or "").strip()
    if local_appdata:
        return Path(local_appdata) / "XTJS" / "ocr_runtime"

    xdg_cache_home = str(os.getenv("XDG_CACHE_HOME", "") or "").strip()
    if xdg_cache_home:
        return Path(xdg_cache_home) / "xtjs" / "ocr_runtime"

    return Path.home() / ".cache" / "xtjs" / "ocr_runtime"


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

    OCR_STORAGE_ROOT: Path = Field(default_factory=_default_ocr_storage_root)
    PADDLE_PDX_CACHE_HOME: Path | None = None
    OCR_RUNTIME_TEMP_DIR: Path | None = None
    PADDLE_PDX_DISABLE_MODEL_SOURCE_CHECK: bool = True

    PADDLE_OCR_DEVICE: str = "gpu:0"
    PADDLE_OCR_DEVICE_POOL: str = "auto"
    PADDLE_OCR_MAX_INFLIGHT_PER_DEVICE: int = 2
    PADDLE_OCR_MULTI_GPU_LOG_SCHEDULING: bool = False
    PADDLE_OCR_FALLBACK_TO_CPU: bool = True
    PADDLE_OCR_USE_DOC_ORIENTATION: bool = True
    PADDLE_OCR_USE_DOC_UNWARPING: bool = True

    PADDLE_VL_PIPELINE_VERSION: str = "v1.5"
    PADDLE_VL_USE_LAYOUT_DETECTION: bool = True
    PADDLE_VL_USE_CHART_RECOGNITION: bool = False
    PADDLE_VL_USE_SEAL_RECOGNITION: bool = True
    PADDLE_VL_USE_SIGNATURE_RECOGNITION: bool = True
    PADDLE_VL_USE_OCR_FOR_IMAGE_BLOCK: bool = True
    PADDLE_VL_FORMAT_BLOCK_CONTENT: bool = False
    PADDLE_VL_MERGE_LAYOUT_BLOCKS: bool = False
    PADDLE_VL_USE_QUEUES: bool = False
    PADDLE_VL_RESTRUCTURE_PAGES: bool = True

    OCR_PROGRESS_ENABLED: bool = True
    OCR_PROGRESS_BAR_WIDTH: int = 24
    OCR_PROGRESS_KEEP_RECENT_UPDATES: int = 12
    OCR_PROGRESS_HEARTBEAT_SECONDS: float = 10.0
    OCR_POSTPROCESS_MAX_WORKERS: int = 0
    OCR_SIGNATURE_PLACEHOLDER_TEXT: str = "已签字"

    TYPO_ERNIE_CSC_MODEL_NAME: str = "ernie-csc"
    TYPO_ERNIE_CSC_DEVICE: str = "gpu:0"
    TYPO_ERNIE_CSC_MAX_SEQ_LEN: int = 128
    TYPO_ERNIE_CSC_BATCH_SIZE: int = 32
    TYPO_ERNIE_CSC_TASK_PATH: str | None = None
    TYPO_CHECK_VISIBLE: bool = True

    CONSISTENCY_EMBEDDING_MODEL_NAME: str = "BAAI/bge-small-zh-v1.5"
    CONSISTENCY_EMBEDDING_MODEL_REVISION: str = (
        "7999e1d3359715c523056ef9478215996d62a620"
    )
    CONSISTENCY_EMBEDDING_MODEL_PATH: Path = (
        PROJECT_ROOT / "models" / "bge-small-zh-v1.5"
    )
    CONSISTENCY_EMBEDDING_DEVICE: str = "cpu"
    CONSISTENCY_EMBEDDING_BATCH_SIZE: int = 32
    CONSISTENCY_EMBEDDING_MAX_LENGTH: int = 512
    CONSISTENCY_TITLE_MATCH_THRESHOLD: float = 0.78
    CONSISTENCY_TITLE_UNMATCHED_THRESHOLD: float = 0.68
    CONSISTENCY_PARAGRAPH_MATCH_THRESHOLD: float = 0.80
    CONSISTENCY_PARAGRAPH_UNMATCHED_THRESHOLD: float = 0.70
    CONSISTENCY_MATCH_MARGIN: float = 0.05
    CONSISTENCY_TEXT_PASS_THRESHOLD: float = 0.93
    CONSISTENCY_DETERMINISTIC_MISSING_MAX_LEXICAL: float = 0.30

    PROJECT_BATCH_MIN_BID_GROUPS: int = 1
    PROJECT_BATCH_MAX_BID_GROUPS: int = 0
    PROJECT_BATCH_MIN_BID_FILES: int = 1
    PROJECT_BATCH_MAX_BID_FILES: int = 0

    CELERY_BROKER_URL: str = "redis://localhost:6379/0"
    CELERY_RESULT_BACKEND: str = "redis://localhost:6379/0"

    # —— 认证与账号安全 ——
    # 生产环境必须在 .env 中用强随机长串覆盖此默认值，切勿沿用默认。
    JWT_SECRET_KEY: str = "CHANGE_ME_IN_ENV_use_a_long_random_secret"
    JWT_ALGORITHM: str = "HS256"
    JWT_ACCESS_TOKEN_EXPIRE_MINUTES: int = 120
    AUTH_MAX_FAILED_ATTEMPTS: int = 5
    AUTH_LOCK_MINUTES: int = 15
    AUTH_PASSWORD_MIN_LENGTH: int = 8
    # 启动时自动创建初始管理员；密码留空则不创建。
    AUTH_INITIAL_ADMIN_USERNAME: str = "admin"
    AUTH_INITIAL_ADMIN_PASSWORD: str = ""

    XTJS_CACHE_ENABLED: bool = True
    XTJS_CACHE_REQUIRED: bool = True
    XTJS_CACHE_REDIS_URL: str = "redis://localhost:6379/1"
    XTJS_CACHE_KEY_PREFIX: str = "xtjs"
    XTJS_CACHE_PROJECT_LIST_TTL_SECONDS: int = 30
    XTJS_CACHE_PROJECT_DETAIL_TTL_SECONDS: int = 120
    XTJS_CACHE_PROJECT_RESULTS_TTL_SECONDS: int = 300
    XTJS_CACHE_OCR_STATUS_TTL_SECONDS: int = 3
    XTJS_CACHE_PREVIEW_META_TTL_SECONDS: int = 7 * 24 * 60 * 60
    XTJS_CACHE_PREVIEW_OBJECT_PREFIX: str = "cache/previews"
    XTJS_CACHE_SCAN_BATCH_SIZE: int = 500

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
