# -*- coding: utf-8 -*-
"""
应用全局配置模块

基于 pydantic-settings 从环境变量 / .env 文件加载配置，并暴露单一 settings 实例。
"""

import os
from pathlib import Path
from typing import Set

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

# 项目根目录
PROJECT_ROOT = Path(__file__).resolve().parents[2]     

# 辅助函数
def _default_ocr_storage_root() -> Path:
    """
    根据操作系统环境变量确定 OCR 存储（模型缓存、临时文件等）的默认根目录。
    Windows : %LOCALAPPDATA%/XTJS/ocr_runtime
    Linux   : $XDG_CACHE_HOME/xtjs/ocr_runtime 或 ~/.cache/xtjs/ocr_runtime
    """
    # 优先使用 Windows 的 LOCALAPPDATA
    local_appdata = os.getenv("LOCALAPPDATA", "").strip()
    if local_appdata:
        return Path(local_appdata) / "XTJS" / "ocr_runtime"

    # 其次使用 Linux 的 XDG_CACHE_HOME
    xdg_cache_home = os.getenv("XDG_CACHE_HOME", "").strip()
    if xdg_cache_home:
        return Path(xdg_cache_home) / "xtjs" / "ocr_runtime"

    # 最终回退到 ~/.cache
    return Path.home() / ".cache" / "xtjs" / "ocr_runtime"


# 配置类
class Settings(BaseSettings):
    """应用配置，所有字段可通过环境变量或 .env 文件覆盖。"""

    # 数据库配置
    DATABASE_URL: str = "postgresql://admin:password@localhost:5432/xtjs_db"

    # MinIO 配置
    MINIO_ENDPOINT: str = "127.0.0.1:9000"
    MINIO_ACCESS_KEY: str = "minioadmin"
    MINIO_SECRET_KEY: str = "minioadmin"
    MINIO_BUCKET_NAME: str = "update-file"
    MINIO_SECURE: bool = False
    MINIO_PRESIGNED_EXPIRES_DAYS: int = 7
    MINIO_MAX_FILE_SIZE: int = 500 * 1024 * 1024    # 500 MB
    MINIO_ALLOWED_EXTENSIONS_STR: str = Field(
        default="pdf,png,jpg,jpeg,bmp,tif,tiff",
        validation_alias="MINIO_ALLOWED_EXTENSIONS",
    )

    @property
    def minio_allowed_extensions(self) -> Set[str]:
        """将逗号分隔的字符串解析为小写扩展名集合。"""
        return {
            ext.strip().lower()
            for ext in self.MINIO_ALLOWED_EXTENSIONS_STR.split(",")
            if ext.strip()
        }

    # OCR 存储路径
    OCR_STORAGE_ROOT: Path = Field(default_factory=_default_ocr_storage_root)
    PADDLE_PDX_CACHE_HOME: Path | None = None
    OCR_RUNTIME_TEMP_DIR: Path | None = None
    # 跳过 PaddleX 模型源校验（加快加载速度）
    PADDLE_PDX_DISABLE_MODEL_SOURCE_CHECK: bool = True

    # Paddle OCR 设备与并发
    PADDLE_OCR_DEVICE: str = "gpu:0"                # 推理设备，如 "cpu"、"gpu:0"
    PADDLE_OCR_DEVICE_POOL: str = "auto"            # 设备池策略，auto 表示自动选择
    PADDLE_OCR_MAX_INFLIGHT_PER_DEVICE: int = 1     # 单设备最大并行任务数
    PADDLE_OCR_MULTI_GPU_LOG_SCHEDULING: bool = False
    PADDLE_OCR_FALLBACK_TO_CPU: bool = True         # GPU 不可用时是否回退到 CPU
    PADDLE_OCR_USE_DOC_ORIENTATION: bool = True     # 启用文档方向分类
    PADDLE_OCR_USE_DOC_UNWARPING: bool = True       # 启用文档扭曲矫正

    # Paddle 视觉管线（版面/印章/签名等）
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

    # OCR 进度与后处理
    OCR_PROGRESS_ENABLED: bool = True
    OCR_PROGRESS_BAR_WIDTH: int = 24
    OCR_PROGRESS_KEEP_RECENT_UPDATES: int = 12
    OCR_PROGRESS_HEARTBEAT_SECONDS: float = 10.0
    OCR_POSTPROCESS_MAX_WORKERS: int = 0               # 0 表示自动
    OCR_SIGNATURE_PLACEHOLDER_TEXT: str = "已签字"      

    # 项目批处理参数
    PROJECT_BATCH_MIN_BID_GROUPS: int = 1
    PROJECT_BATCH_MAX_BID_GROUPS: int = 0              # 0 表示不限制
    PROJECT_BATCH_MIN_BID_FILES: int = 1
    PROJECT_BATCH_MAX_BID_FILES: int = 0

    # Celery 异步任务
    CELERY_BROKER_URL: str = "redis://localhost:6379/0"
    CELERY_RESULT_BACKEND: str = "redis://localhost:6379/0"

    # pydantic-settings 配置
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",             # 忽略未定义的环境变量，避免启动报错
    )


# 全局配置实例
settings = Settings()

# 如果未通过环境变量显式指定 PaddleX 缓存目录 / 临时目录，则自动设置为 OCR_STORAGE_ROOT 下的子目录
if not settings.PADDLE_PDX_CACHE_HOME:
    settings.PADDLE_PDX_CACHE_HOME = settings.OCR_STORAGE_ROOT / "paddlex-cache"
if not settings.OCR_RUNTIME_TEMP_DIR:
    settings.OCR_RUNTIME_TEMP_DIR = settings.OCR_STORAGE_ROOT / "runtime-tmp"