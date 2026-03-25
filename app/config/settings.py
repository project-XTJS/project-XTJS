# -*- coding: utf-8 -*-
from pathlib import Path
from typing import Set

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

# 项目根目录
PROJECT_ROOT = Path(__file__).resolve().parents[2]


class Settings(BaseSettings):
    """全局统一配置中心。"""

    # 1. 数据库配置（PostgreSQL）
    DATABASE_URL: str = "postgresql://admin:password@localhost:5432/xtjs_db"

    # 2. 对象存储配置（MinIO）
    MINIO_ENDPOINT: str = "127.0.0.1:9000"
    MINIO_ACCESS_KEY: str = "minioadmin"
    MINIO_SECRET_KEY: str = "minioadmin"
    MINIO_BUCKET_NAME: str = "update-file"  # 固定业务 bucket
    MINIO_SECURE: bool = False  # Pydantic 会自动处理 "true"/"false" 字符串
    MINIO_PRESIGNED_EXPIRES_DAYS: int = 7
    MINIO_MAX_FILE_SIZE: int = 500 * 1024 * 1024

    # 用逗号分隔字符串接收环境变量，再通过属性转换为 Set
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

    # 3. OCR 配置
    OCR_STORAGE_ROOT: Path = PROJECT_ROOT / ".ocr_runtime"

    # 运行时动态路径，实例化后补默认值
    PADDLE_PDX_CACHE_HOME: Path | None = None
    OCR_RUNTIME_TEMP_DIR: Path | None = None

    PADDLE_PDX_DISABLE_MODEL_SOURCE_CHECK: bool = True
    PADDLE_OCR_DEVICE: str = "gpu:0"
    # 多设备池，例如 "gpu:0,gpu:1,gpu:2"
    PADDLE_OCR_DEVICE_POOL: str = " "
    # 每个设备最大并发文档数；默认 1 更稳
    PADDLE_OCR_MAX_INFLIGHT_PER_DEVICE: int = 1
    PADDLE_OCR_MULTI_GPU_LOG_SCHEDULING: bool = False
    PADDLE_OCR_FALLBACK_TO_CPU: bool = True
    PADDLE_OCR_DISABLE_MKLDNN: bool = True
    PADDLE_OCR_LANG: str = "ch"
    PADDLE_OCR_VERSION: str = "PP-OCRv5"
    PADDLE_OCR_ENABLE_HPI: bool = False
    PADDLE_OCR_ENABLE_STRUCTURE: bool = True
    PADDLE_OCR_ENABLE_SEAL_RECOGNITION: bool = True
    PADDLE_OCR_ENABLE_SIGNATURE_RECOGNITION: bool = True
    PADDLE_OCR_EXCLUDE_SEAL_TEXT: bool = True
    PADDLE_OCR_FORCE_PDF_OCR: bool = False
    # PDF 抽取策略：auto | text | ocr | hybrid
    PADDLE_OCR_PDF_MODE: str = "auto"
    # 文本可直接抽取时，是否仍进行印章/签字检测
    PADDLE_OCR_DETECT_MARKERS_ON_TEXT_PDF: bool = True
    # PDF 单文档流水线并行参数（Stage A/B/C）
    PADDLE_OCR_ENABLE_PIPELINE_PARALLEL: bool = True
    PADDLE_OCR_PIPELINE_MIN_PAGES: int = 2
    PADDLE_OCR_PIPELINE_RENDER_WORKERS: int = 2
    PADDLE_OCR_PIPELINE_POST_WORKERS: int = 2
    PADDLE_OCR_PIPELINE_QUEUE_SIZE: int = 4
    PADDLE_OCR_PIPELINE_LOG_METRICS: bool = True
    PADDLE_OCR_LOG_PROGRESS: bool = True
    PADDLE_OCR_PROGRESS_LOG_INTERVAL_SECONDS: float = 2.0

    PADDLE_OCR_USE_DOC_ORIENTATION: bool = True
    PADDLE_OCR_USE_DOC_UNWARPING: bool = True
    PADDLE_OCR_USE_TEXTLINE_ORIENTATION: bool = True

    PADDLE_STRUCTURE_USE_TABLE: bool = True
    PADDLE_STRUCTURE_USE_FORMULA: bool = False

    # 4. 项目批量识别配置
    # 最少投标文件数，默认 1
    PROJECT_BATCH_MIN_BID_FILES: int = 1
    # 最多投标文件数；<=0 表示不限制上限
    PROJECT_BATCH_MAX_BID_FILES: int = 0

    # 5. Pydantic 行为配置
    model_config = SettingsConfigDict(
        env_file=".env",  # 自动加载项目根目录下的 .env
        env_file_encoding="utf-8",
        extra="ignore",  # 忽略未在模型中声明的环境变量
    )

    # 6. Celery & Redis 配置
    CELERY_BROKER_URL: str = "redis://localhost:6379/0"
    CELERY_RESULT_BACKEND: str = "redis://localhost:6379/0"


# 全局配置单例
settings = Settings()

# 处理依赖其他路径的动态默认值
if not settings.PADDLE_PDX_CACHE_HOME:
    settings.PADDLE_PDX_CACHE_HOME = settings.OCR_STORAGE_ROOT / "paddlex-cache"
if not settings.OCR_RUNTIME_TEMP_DIR:
    settings.OCR_RUNTIME_TEMP_DIR = settings.OCR_STORAGE_ROOT / "runtime-tmp"
