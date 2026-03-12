import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


def _to_bool(value: str, default: bool) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


class OCRConfig:
    PROJECT_ROOT = Path(__file__).resolve().parents[2]
    STORAGE_ROOT = Path(
        os.getenv("OCR_STORAGE_ROOT", str(PROJECT_ROOT / ".ocr_runtime"))
    )
    PDX_CACHE_HOME = Path(
        os.getenv("PADDLE_PDX_CACHE_HOME", str(STORAGE_ROOT / "paddlex-cache"))
    )
    RUNTIME_TEMP_DIR = Path(
        os.getenv("OCR_RUNTIME_TEMP_DIR", str(STORAGE_ROOT / "runtime-tmp"))
    )

    DISABLE_MODEL_SOURCE_CHECK = _to_bool(
        os.getenv("PADDLE_PDX_DISABLE_MODEL_SOURCE_CHECK"), True
    )
    DEVICE = os.getenv("PADDLE_OCR_DEVICE", "cpu")
    FALLBACK_TO_CPU = _to_bool(os.getenv("PADDLE_OCR_FALLBACK_TO_CPU"), True)
    DISABLE_MKLDNN_ON_CPU = _to_bool(os.getenv("PADDLE_OCR_DISABLE_MKLDNN"), True)
    LANG = os.getenv("PADDLE_OCR_LANG", "ch")
    OCR_VERSION = os.getenv("PADDLE_OCR_VERSION", "PP-OCRv5")
    ENABLE_HPI = _to_bool(os.getenv("PADDLE_OCR_ENABLE_HPI"), False)
    ENABLE_STRUCTURE = _to_bool(os.getenv("PADDLE_OCR_ENABLE_STRUCTURE"), True)

    USE_DOC_ORIENTATION = _to_bool(os.getenv("PADDLE_OCR_USE_DOC_ORIENTATION"), True)
    USE_DOC_UNWARPING = _to_bool(os.getenv("PADDLE_OCR_USE_DOC_UNWARPING"), True)
    USE_TEXTLINE_ORIENTATION = _to_bool(
        os.getenv("PADDLE_OCR_USE_TEXTLINE_ORIENTATION"), True
    )

    STRUCTURE_USE_TABLE = _to_bool(os.getenv("PADDLE_STRUCTURE_USE_TABLE"), True)
    STRUCTURE_USE_FORMULA = _to_bool(os.getenv("PADDLE_STRUCTURE_USE_FORMULA"), False)
