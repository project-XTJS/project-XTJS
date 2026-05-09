# -*- coding: utf-8 -*-
"""
项目与文档 CRUD 路由。

提供项目、文档、关系、分析结果的增删改查接口，
包含文档预览（含高亮）、重复检查、形式审查、人员复用检查、错别字检查等功能。
"""

import base64
import json
import io
import os
import hashlib
import re
import tempfile
import time
from collections import OrderedDict
from threading import Lock
from typing import Any, Literal, Optional
from urllib.parse import quote

from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, UploadFile
from fastapi.responses import JSONResponse, RedirectResponse
from psycopg2 import Error as PsycopgError
from starlette.concurrency import run_in_threadpool

from app.core.document_types import (
    DOCUMENT_TYPE_BUSINESS_BID,
    DOCUMENT_TYPE_TECHNICAL_BID,
    DocumentType,
)
from app.router.dependencies import (
    RecognitionOptions,
    get_bid_document_review_service,
    get_db_service,
    get_duplicate_check_service,
    get_form_recognition_options,
    get_oss_service,
    get_text_analysis_service,
)
from app.router.uploaded_json_support import (
    build_uploaded_project_document_records,
    load_uploaded_bid_json_documents,
    persist_uploaded_json_project_documents,
    read_uploaded_json_file,
)
from app.schemas.postgresql import (
    DuplicateCheckScope,
    DocumentUpdateRequest,
    IdentifierBatchDeleteRequest,
    ProjectBindDocumentsRequest,
    ProjectCreateRequest,
    ProjectDuplicateCheckRequest,
    ProjectResultUpdateRequest,
    ProjectResultUpsertRequest,
    ProjectRelationUpdateRequest,
    RelationBatchDeleteRequest,
    ProjectUpdateRequest,
)
from app.service.analysis import BidDocumentReviewService, DuplicateCheckService
from app.service.analysis.duplicate_merge import (
    DOC_TYPE_BY_MERGED_RESULT_KEY,
    MERGED_RESULT_KEY_BY_DOC_TYPE,
    RAW_RESULT_KEY_BY_DOC_TYPE,
    build_duplicate_merge_results,
)
from app.service.analysis.unified import UnifiedBusinessReviewService
from app.service.document_ingest_service import normalize_file_url, upload_extract_and_create_document
from app.service.minio_service import MinioService
from app.service.postgresql_service import PostgreSQLService

router = APIRouter()

# 预览缓存容量与过期时间（可通过环境变量调整）
_PREVIEW_CACHE_MAX_ITEMS = max(8, min(int(os.getenv("XTJS_PREVIEW_CACHE_MAX_ITEMS", "64")), 512))
_PREVIEW_CACHE_TTL_SECONDS = max(30, int(os.getenv("XTJS_PREVIEW_CACHE_TTL_SECONDS", "900")))
_DOCUMENT_PREVIEW_CACHE: "OrderedDict[tuple[str, str, int], tuple[float, dict]]" = OrderedDict()
_DOCUMENT_PREVIEW_CACHE_LOCK = Lock()
# 用于从高亮短语中提取有效 token 的正则
_HIGHLIGHT_TOKEN_PATTERN = re.compile(r"[\u4e00-\u9fffA-Za-z0-9]+")


# 将查重范围枚举转换为文档类型列表
def _document_types_from_scope(scope: DuplicateCheckScope) -> Optional[list[str]]:
    if scope == DuplicateCheckScope.ALL:
        return None
    return [scope.value]


# 统一分页参数解析（兼容 page/limit 两种方式）
def _resolve_pagination(
    *,
    page: int,
    page_size: int,
    limit: Optional[int] = None,
    offset: Optional[int] = None,
) -> tuple[int, int]:
    if limit is not None or offset is not None:
        normalized_limit = max(1, min(int(limit or page_size), 200))
        normalized_offset = max(0, int(offset or 0))
        return normalized_limit, normalized_offset
    normalized_page_size = max(1, min(int(page_size), 200))
    normalized_page = max(1, int(page))
    return normalized_page_size, (normalized_page - 1) * normalized_page_size


# 根据文件名/URL 后缀判断文档源类型（pdf 或 image）
def _document_source_kind(document: dict) -> str:
    file_name = str(document.get("file_name") or "").strip()
    file_url = str(document.get("file_url") or "").strip()
    target = file_name or file_url
    suffix = os.path.splitext(target)[1].lower()
    if suffix == ".pdf":
        return "pdf"
    if suffix in {".png", ".jpg", ".jpeg"}:
        return "image"
    return "unknown"


# 从文档记录中解析 MinIO 的 bucket 和 object 名称
def _resolve_document_source_object(document: dict) -> tuple[str, str, str]:
    file_url = str(document.get("file_url") or "").strip()
    if not file_url:
        raise ValueError("document file_url is empty")

    if file_url.startswith("minio://"):
        bucket_name, object_name = MinioService.bucket_and_object_from_file_url(file_url)
    elif MinioService.is_presigned_url(file_url):
        bucket_name, object_name = MinioService.bucket_and_object_from_presigned_url(file_url)
    else:
        raise ValueError("document source file is not stored in MinIO")

    file_name = str(document.get("file_name") or object_name or "document").strip() or "document"
    return bucket_name, object_name, file_name


# 从 MinIO 下载文档原始字节
def _load_document_source_bytes(
    *,
    document: dict,
    oss_service: MinioService,
) -> tuple[bytes, str, str]:
    bucket_name, object_name, _file_name = _resolve_document_source_object(document)
    data, content_type = oss_service.get_object_bytes(object_name, bucket_name)
    return data, content_type, object_name


# 构建预览缓存的唯一键（基于文档标识、版本、页码和高亮参数）
def _preview_cache_key(document: dict, page: int, variant: str = "") -> tuple[str, str, int, str]:
    identifier_id = str(document.get("identifier_id") or "").strip()
    version = str(document.get("update_time") or document.get("file_url") or "").strip()
    return identifier_id, version, int(page), variant


# 清理预览缓存中过期的条目
def _preview_cache_prune(now: float) -> None:
    expired_keys = [
        key for key, (created_at, _payload) in _DOCUMENT_PREVIEW_CACHE.items()
        if now - created_at > _PREVIEW_CACHE_TTL_SECONDS
    ]
    for key in expired_keys:
        _DOCUMENT_PREVIEW_CACHE.pop(key, None)


# 从缓存获取预览数据（线程安全）
def _preview_cache_get(document: dict, page: int, variant: str = "") -> Optional[dict]:
    cache_key = _preview_cache_key(document, page, variant)
    now = time.monotonic()
    with _DOCUMENT_PREVIEW_CACHE_LOCK:
        _preview_cache_prune(now)
        cached = _DOCUMENT_PREVIEW_CACHE.get(cache_key)
        if not cached:
            return None
        created_at, payload = cached
        if now - created_at > _PREVIEW_CACHE_TTL_SECONDS:
            _DOCUMENT_PREVIEW_CACHE.pop(cache_key, None)
            return None
        _DOCUMENT_PREVIEW_CACHE.move_to_end(cache_key)  # LRU 提升
        return dict(payload)


# 将预览数据写入缓存（线程安全，并控制总量）
def _preview_cache_set(document: dict, page: int, payload: dict, variant: str = "") -> None:
    cache_key = _preview_cache_key(document, page, variant)
    now = time.monotonic()
    with _DOCUMENT_PREVIEW_CACHE_LOCK:
        _preview_cache_prune(now)
        _DOCUMENT_PREVIEW_CACHE[cache_key] = (now, dict(payload))
        _DOCUMENT_PREVIEW_CACHE.move_to_end(cache_key)
        while len(_DOCUMENT_PREVIEW_CACHE) > _PREVIEW_CACHE_MAX_ITEMS:
            _DOCUMENT_PREVIEW_CACHE.popitem(last=False)


# 将 PDF/图片的字节渲染为带 base64 的预览数据（支持高亮）
def _preview_payload_from_source(
    *,
    file_bytes: bytes,
    source_kind: str,
    page: int,
    highlight_phrases: Optional[list[str]] = None,
    highlight_bbox: Optional[list[float]] = None,
    highlight_rects: Optional[list[list[float]]] = None,
) -> dict:
    if page <= 0:
        raise ValueError("page must be greater than 0")

    if source_kind == "pdf":
        import fitz

        pdf = fitz.open(stream=file_bytes, filetype="pdf")
        try:
            page_count = int(pdf.page_count)
            if page > page_count:
                raise ValueError(f"page {page} is out of range, max page is {page_count}")
            pdf_page = pdf.load_page(page - 1)
            rect = pdf_page.rect
            # 优先用文本定位高亮
            text_rects = _apply_pdf_text_highlights(
                pdf_page,
                highlight_phrases=highlight_phrases or [],
                highlight_bbox=highlight_bbox,
            )
            direct_rects = []
            if not text_rects:
                # 文本高亮失败则用 OCR 精修后的矩形框
                refined_rects = _refine_highlight_rects_via_ocr(
                    pdf_page,
                    highlight_rects=highlight_rects or [],
                    highlight_phrases=highlight_phrases or [],
                )
                direct_rects = _apply_pdf_rect_highlights(
                    pdf_page,
                    highlight_rects=refined_rects,
                )
            pix = pdf_page.get_pixmap(matrix=fitz.Matrix(1.8, 1.8), alpha=False)
            image_bytes = pix.tobytes("png")
            return {
                "page": page,
                "page_count": page_count,
                "width": float(rect.width),
                "height": float(rect.height),
                "image_data_url": (
                    "data:image/png;base64,"
                    + base64.b64encode(image_bytes).decode("ascii")
                ),
                "source_kind": "pdf",
                "highlight_rect_count": len(text_rects) + len(direct_rects),
                "highlight_applied": bool(text_rects or direct_rects),
            }
        finally:
            pdf.close()

    if source_kind == "image":
        if page != 1:
            raise ValueError("image document only supports page 1 preview")
        from PIL import Image

        with Image.open(io.BytesIO(file_bytes)) as image:
            rgb = image.convert("RGB")
            buffer = io.BytesIO()
            rgb.save(buffer, format="PNG")
            return {
                "page": 1,
                "page_count": 1,
                "width": int(rgb.width),
                "height": int(rgb.height),
                "image_data_url": (
                    "data:image/png;base64,"
                    + base64.b64encode(buffer.getvalue()).decode("ascii")
                ),
                "source_kind": "image",
            }

    raise ValueError("document source kind does not support preview")


# 归一化高亮关键词列表（去噪、去重、限制长度）
def _normalize_preview_highlight_phrases(raw_values: Optional[list[str]]) -> list[str]:
    normalized: list[str] = []
    for value in raw_values or []:
        text = re.sub(r"\s+", " ", str(value or "")).strip()
        if not text:
            continue
        compact = re.sub(r"[^\w\u4e00-\u9fff]+", "", text, flags=re.UNICODE)
        if len(compact) < 2:
            continue
        if compact.isdigit():
            continue
        if text not in normalized:
            normalized.append(text[:240])
        if len(normalized) >= 12:
            break
    return normalized


# 归一化单个高亮边界框（逗号分隔的四个数字）
def _normalize_preview_highlight_bbox(raw_bbox: Optional[str]) -> Optional[list[float]]:
    if not raw_bbox:
        return None
    parts = [segment.strip() for segment in str(raw_bbox).split(",")]
    values: list[float] = []
    for part in parts[:4]:
        try:
            values.append(float(part))
        except (TypeError, ValueError):
            return None
    if len(values) < 4:
        return None
    x0, y0, x1, y1 = values[:4]
    if x1 <= x0 or y1 <= y0:
        return None
    return [x0, y0, x1, y1]


# 生成高亮变体签名（用于缓存键，区分不同高亮参数组合）
def _preview_variant_signature(
    highlight_phrases: Optional[list[str]],
    highlight_bbox: Optional[list[float]],
    highlight_rects: Optional[list[list[float]]] = None,
) -> str:
    phrases = _normalize_preview_highlight_phrases(highlight_phrases)
    bbox = highlight_bbox or []
    rects = highlight_rects or []
    if not phrases and not bbox and not rects:
        return ""
    payload = json.dumps(
        {
            "phrases": phrases,
            "bbox": [round(float(value), 2) for value in bbox],
            "rects": [
                [round(float(value), 2) for value in rect[:4]]
                for rect in rects
                if isinstance(rect, (list, tuple)) and len(rect) >= 4
            ],
        },
        ensure_ascii=False,
        sort_keys=True,
    )
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


# 从短语中提取用于匹配的 token（去标点、小写、排序）
def _highlight_tokens_from_phrases(phrases: list[str]) -> list[str]:
    tokens: list[str] = []
    for phrase in phrases:
        for raw_token in _HIGHLIGHT_TOKEN_PATTERN.findall(str(phrase or "")):
            token = re.sub(r"[^\w\u4e00-\u9fff]+", "", raw_token, flags=re.UNICODE).lower()
            if len(token) < 2:
                continue
            if token.isdigit():
                continue
            if token not in tokens:
                tokens.append(token)
    tokens.sort(key=len, reverse=True)  # 长 token 优先匹配
    return tokens[:48]


# 判断 PDF 中的单词是否匹配任一高亮 token
def _word_matches_highlight_token(word_text: str, tokens: list[str]) -> bool:
    normalized = re.sub(r"[^\w\u4e00-\u9fff]+", "", str(word_text or ""), flags=re.UNICODE).lower()
    if len(normalized) < 2:
        return False
    for token in tokens:
        if normalized == token:
            return True
        if len(token) >= 4 and normalized in token:
            return True
        if len(normalized) >= 4 and token in normalized:
            return True
    return False


# 收集 PDF 页内需要高亮的矩形（基于文本词级定位）
def _collect_pdf_highlight_rects(pdf_page, *, highlight_phrases: list[str], highlight_bbox: Optional[list[float]]):
    import fitz

    tokens = _highlight_tokens_from_phrases(highlight_phrases)
    if not tokens:
        return []

    bbox_rect = fitz.Rect(highlight_bbox) if highlight_bbox else None

    def iter_matches(restrict_bbox: bool):
        matched = []
        for word in pdf_page.get_text("words", sort=True):
            if len(word) < 8:
                continue
            rect = fitz.Rect(word[:4])
            if restrict_bbox and bbox_rect is not None:
                overlap = rect & bbox_rect
                if overlap.is_empty:
                    continue
                if overlap.get_area() <= 0:
                    continue
            if not _word_matches_highlight_token(word[4], tokens):
                continue
            matched.append(
                {
                    "rect": rect,
                    "block": int(word[5]),
                    "line": int(word[6]),
                    "word": int(word[7]),
                }
            )
        return matched

    # 先在指定 bbox 内匹配，若无效再全文匹配
    matches = iter_matches(restrict_bbox=True)
    if not matches and bbox_rect is not None:
        matches = iter_matches(restrict_bbox=False)

    if not matches:
        return []

    # 合并同行相邻单词的矩形
    merged: list[fitz.Rect] = []
    current_rect = None
    current_key = None
    current_word_index = None

    for item in matches:
        rect = item["rect"]
        key = (item["block"], item["line"])
        word_index = item["word"]
        if current_rect is None:
            current_rect = fitz.Rect(rect)
            current_key = key
            current_word_index = word_index
            continue
        gap = rect.x0 - current_rect.x1
        same_line = key == current_key
        if same_line and word_index <= (current_word_index or 0) + 2 and gap <= max(18.0, rect.height * 1.2):
            current_rect |= rect
            current_word_index = word_index
            continue
        merged.append(current_rect)
        current_rect = fitz.Rect(rect)
        current_key = key
        current_word_index = word_index

    if current_rect is not None:
        merged.append(current_rect)
    return merged


# 在 PDF 页面上绘制文本匹配的高亮矩形
def _apply_pdf_text_highlights(pdf_page, *, highlight_phrases: list[str], highlight_bbox: Optional[list[float]]):
    rects = _collect_pdf_highlight_rects(
        pdf_page,
        highlight_phrases=_normalize_preview_highlight_phrases(highlight_phrases),
        highlight_bbox=highlight_bbox,
    )
    if not rects:
        return []
    for rect in rects:
        pdf_page.draw_rect(
            rect,
            color=(1.0, 0.91, 0.2),
            fill=(1.0, 0.91, 0.2),
            width=0.3,
            fill_opacity=0.28,
            overlay=True,
        )
    return rects


# 归一化 JSON 格式的高亮矩形数组
def _normalize_preview_highlight_rects(raw_value: Optional[str]) -> list[list[float]]:
    if not raw_value:
        return []
    try:
        parsed = json.loads(str(raw_value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return []
    rects: list[list[float]] = []
    for item in parsed if isinstance(parsed, list) else []:
        if not isinstance(item, (list, tuple)) or len(item) < 4:
            continue
        values = []
        try:
            values = [float(item[index]) for index in range(4)]
        except (TypeError, ValueError):
            continue
        x0, y0, x1, y1 = values
        if x1 <= x0 or y1 <= y0:
            continue
        rects.append([x0, y0, x1, y1])
    return rects[:24]


# 将外部传来的矩形坐标转换到 PDF 页面坐标系（处理缩放）
def _coerce_rect_to_pdf_page_space(pdf_page, rect_values: list[float]) -> Optional[list[float]]:
    import fitz

    if not isinstance(rect_values, (list, tuple)) or len(rect_values) < 4:
        return None
    try:
        x0, y0, x1, y1 = [float(rect_values[index]) for index in range(4)]
    except (TypeError, ValueError):
        return None
    if x1 <= x0 or y1 <= y0:
        return None

    page_rect = fitz.Rect(pdf_page.rect)
    page_width = max(float(page_rect.width), 1.0)
    page_height = max(float(page_rect.height), 1.0)

    # 自动猜测缩放比例
    scale = 1.0
    max_ratio = max(x1 / page_width, y1 / page_height)
    if max_ratio > 1.05:
        for candidate in (1.5, 2.0, 3.0, 4.0):
            if (x1 / candidate) <= page_width * 1.05 and (y1 / candidate) <= page_height * 1.05:
                scale = candidate
                break
        else:
            scale = max_ratio

    x0 /= scale
    y0 /= scale
    x1 /= scale
    y1 /= scale

    # 裁剪到页面范围内
    x0 = min(max(x0, 0.0), page_width)
    y0 = min(max(y0, 0.0), page_height)
    x1 = min(max(x1, 0.0), page_width)
    y1 = min(max(y1, 0.0), page_height)

    if x1 <= x0 or y1 <= y0:
        return None
    return [x0, y0, x1, y1]


# 在 PDF 页面上绘制用户指定的矩形高亮
def _apply_pdf_rect_highlights(pdf_page, *, highlight_rects: list[list[float]]):
    import fitz

    rects = []
    for item in highlight_rects or []:
        coerced = _coerce_rect_to_pdf_page_space(pdf_page, item)
        if not coerced:
            continue
        rect = fitz.Rect(coerced)
        if rect.is_empty or rect.get_area() <= 0:
            continue
        pdf_page.draw_rect(
            rect,
            color=(1.0, 0.93, 0.3),
            fill=(1.0, 0.93, 0.3),
            width=0.2,
            fill_opacity=0.22,
            overlay=True,
        )
        rects.append(rect)
    return rects


# 获取可用的 OCR 服务实例（用于预览时的 OCR 辅助定位）
def _get_preview_ocr_service():
    try:
        analysis_service = get_text_analysis_service()
    except Exception:
        return None

    direct = getattr(analysis_service, "ocr_service", None)
    if direct is not None and bool(getattr(direct, "available", False)):
        return direct

    services = getattr(analysis_service, "_services", None)
    if isinstance(services, list):
        for service in services:
            ocr_service = getattr(service, "ocr_service", None)
            if ocr_service is not None and bool(getattr(ocr_service, "available", False)):
                return ocr_service
    return None


# 紧凑化文本（去除所有非字母数字汉字的字符）
def _compact_highlight_text(text: str) -> str:
    return re.sub(r"[^\w\u4e00-\u9fff]+", "", str(text or ""), flags=re.UNICODE).lower()


# 在文本中查找短语的紧凑匹配范围（用于 OCR 结果中定位高亮短语）
def _find_compact_substring_ranges(text: str, phrase: str) -> list[tuple[int, int]]:
    source = str(text or "")
    target = str(phrase or "")
    compact_source_chars: list[str] = []
    compact_to_source_index: list[int] = []
    for index, char in enumerate(source):
        if re.match(r"[\w\u4e00-\u9fff]", char, flags=re.UNICODE):
            compact_source_chars.append(char.lower())
            compact_to_source_index.append(index)
    compact_source = "".join(compact_source_chars)
    compact_target = _compact_highlight_text(target)
    if not compact_source or not compact_target:
        return []

    ranges: list[tuple[int, int]] = []
    search_from = 0
    while search_from < len(compact_source):
        found_at = compact_source.find(compact_target, search_from)
        if found_at < 0:
            break
        end_at = found_at + len(compact_target)
        ranges.append((found_at, end_at))
        search_from = max(found_at + 1, end_at)
    return ranges


# 在 OCR 区段内按词语匹配生成子矩形（用于 OCR 精修高亮）
def _section_subrects_for_phrases(section_text: str, section_bbox: list[float], phrases: list[str]) -> list[list[float]]:
    if not section_text or not phrases:
        return []
    x0, y0, x1, y1 = section_bbox[:4]
    width = max(float(x1) - float(x0), 1.0)
    compact_length = len(_compact_highlight_text(section_text))
    if compact_length <= 0:
        return []

    rects: list[list[float]] = []
    seen = set()
    for phrase in phrases:
        for start_at, end_at in _find_compact_substring_ranges(section_text, phrase):
            left = x0 + width * (start_at / compact_length)
            right = x0 + width * (end_at / compact_length)
            key = (round(left, 1), round(right, 1), round(y0, 1), round(y1, 1))
            if key in seen:
                continue
            seen.add(key)
            rects.append([left, y0, max(left + 2.0, right), y1])
    return rects


# 当文本高亮失败时，利用 OCR 对 PDF 页面截图进行识别，并精修高亮区域
def _refine_highlight_rects_via_ocr(pdf_page, *, highlight_rects: list[list[float]], highlight_phrases: list[str]) -> list[list[float]]:
    ocr_service = _get_preview_ocr_service()
    if ocr_service is None:
        return highlight_rects

    import fitz

    scale = 2.0
    refined: list[list[float]] = []
    phrases = _normalize_preview_highlight_phrases(highlight_phrases)
    if not phrases:
        return highlight_rects
    if len(highlight_rects or []) > 1:
        return highlight_rects

    for rect_values in highlight_rects or []:
        coerced = _coerce_rect_to_pdf_page_space(pdf_page, rect_values)
        if not coerced:
            continue
        rect = fitz.Rect(coerced)
        if rect.is_empty or rect.get_area() <= 0:
            continue
        clip = fitz.Rect(rect)
        clip.x0 = max(0, clip.x0 - 2)
        clip.y0 = max(0, clip.y0 - 2)
        clip.x1 = min(float(pdf_page.rect.width), clip.x1 + 2)
        clip.y1 = min(float(pdf_page.rect.height), clip.y1 + 2)
        if clip.x1 <= clip.x0 or clip.y1 <= clip.y0:
            refined.append(coerced)
            continue
        try:
            pix = pdf_page.get_pixmap(matrix=fitz.Matrix(scale, scale), clip=clip, alpha=False)
            pix_bytes = pix.tobytes("png")
        except Exception:
            refined.append(coerced)
            continue
        with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as temp_file:
            temp_file.write(pix_bytes)
            temp_path = temp_file.name
        try:
            ocr_payload = ocr_service.extract_all(temp_path, "png")
        except Exception:
            ocr_payload = {}
        finally:
            try:
                os.unlink(temp_path)
            except OSError:
                pass

        sections = []
        if isinstance(ocr_payload, dict):
            sections = list(ocr_payload.get("layout_sections") or [])
        matched_any = False
        for section in sections:
            section_text = str(section.get("text") or section.get("raw_text") or "").strip()
            bbox = section.get("bbox") or section.get("bbox_ocr") or section.get("box")
            if not section_text or not isinstance(bbox, (list, tuple)) or len(bbox) < 4:
                continue
            local_bbox = [float(bbox[index]) for index in range(4)]
            rects = _section_subrects_for_phrases(section_text, local_bbox, phrases)
            for local_rect in rects:
                refined.append(
                    [
                        clip.x0 + (local_rect[0] / scale),
                        clip.y0 + (local_rect[1] / scale),
                        clip.x0 + (local_rect[2] / scale),
                        clip.y0 + (local_rect[3] / scale),
                    ]
                )
                matched_any = True
        if not matched_any:
            refined.append([clip.x0, clip.y0, clip.x1, clip.y1])

    return refined or highlight_rects


# 执行项目重复检查，并持久化结果和合并聚类
def _run_project_duplicate_check(
    *,
    identifier_id: str,
    document_types: Optional[list[str]],
    max_evidence_sections: int,
    max_pairs_per_type: int,
    result_key: str,
    db_service: PostgreSQLService,
    duplicate_check_service: DuplicateCheckService,
):
    payload_data = db_service.get_project_documents_for_duplicate_check(identifier_id)
    if not payload_data:
        raise HTTPException(status_code=404, detail="项目不存在")

    duplicate_result = duplicate_check_service.check_project_documents(
        project_identifier=identifier_id,
        project=payload_data["project"],
        document_records=payload_data["documents"],
        document_types=document_types,
        max_evidence_sections=max_evidence_sections,
        max_pairs_per_type=max_pairs_per_type,
    )
    db_service.upsert_project_result_item(
        project_identifier_id=identifier_id,
        result_key=result_key,
        result_value=duplicate_result,
    )
    _persist_duplicate_merge_results(
        db_service=db_service,
        project_identifier=identifier_id,
        source_result_key=result_key,
        raw_result=duplicate_result,
    )
    return duplicate_result


# 执行投标文件整体审查并持久化结果
def _run_project_bid_document_review(
    *,
    identifier_id: str,
    document_types: Optional[list[str]],
    result_key: str,
    db_service: PostgreSQLService,
    bid_document_review_service: BidDocumentReviewService,
):
    payload_data = db_service.get_project_documents_for_duplicate_check(identifier_id)
    if not payload_data:
        raise HTTPException(status_code=404, detail="项目不存在")

    review_result = bid_document_review_service.check_project_documents(
        project_identifier=identifier_id,
        project=payload_data["project"],
        document_records=payload_data["documents"],
        document_types=document_types,
    )
    db_service.upsert_project_result_item(
        project_identifier_id=identifier_id,
        result_key=result_key,
        result_value=review_result,
    )
    return review_result


# 从审查结果中提取“一人多用”分析视图
def _build_personnel_reuse_result(review_result: dict) -> dict:
    groups = {}
    total_document_count = 0
    total_skipped_document_count = 0
    total_personnel_count = 0
    total_reused_name_count = 0

    for role, group in (review_result.get("groups") or {}).items():
        summary = group.get("summary") or {}
        personnel_reuse_check = group.get("personnel_reuse_check") or {}
        group_document_count = int(summary.get("document_count") or 0)
        group_skipped_count = int(summary.get("skipped_document_count") or 0)
        group_personnel_count = int(summary.get("personnel_count") or 0)
        group_reused_name_count = int(summary.get("reused_name_count") or 0)

        groups[role] = {
            "documents": group.get("documents") or [],
            "skipped_documents": group.get("skipped_documents") or [],
            "personnel_reuse_check": personnel_reuse_check,
            "summary": {
                "document_count": group_document_count,
                "skipped_document_count": group_skipped_count,
                "personnel_count": group_personnel_count,
                "reused_name_count": group_reused_name_count,
                "suspicious": bool(group_reused_name_count),
            },
        }

        total_document_count += group_document_count
        total_skipped_document_count += group_skipped_count
        total_personnel_count += group_personnel_count
        total_reused_name_count += group_reused_name_count

    config = review_result.get("config") or {}
    return {
        "project": review_result.get("project"),
        "config": {
            "document_types": config.get("document_types") or [],
            "personnel_reuse_scope": config.get("personnel_reuse_scope"),
            "personnel_table_extraction_engine": config.get("personnel_table_extraction_engine"),
            "personnel_text_extraction_engine": config.get("personnel_text_extraction_engine"),
            "business_bid_personnel_scope": config.get("business_bid_personnel_scope"),
            "technical_bid_personnel_scope": config.get("technical_bid_personnel_scope"),
        },
        "groups": groups,
        "summary": {
            "requested_document_types": config.get("document_types") or [],
            "document_count": total_document_count,
            "skipped_document_count": total_skipped_document_count,
            "personnel_count": total_personnel_count,
            "reused_name_count": total_reused_name_count,
            "suspicious": bool(total_reused_name_count),
        },
    }


# 从审查结果中提取“错别字检查”分析视图
def _build_typo_check_result(review_result: dict) -> dict:
    groups = {}
    total_document_count = 0
    total_skipped_document_count = 0
    total_typo_issue_count = 0
    total_shared_typo_issue_count = 0
    total_suspicious_typo_document_count = 0

    for role, group in (review_result.get("groups") or {}).items():
        summary = group.get("summary") or {}
        typo_check = group.get("typo_check") or {}
        group_document_count = int(summary.get("document_count") or 0)
        group_skipped_count = int(summary.get("skipped_document_count") or 0)
        group_typo_issue_count = int(summary.get("typo_issue_count") or 0)
        group_shared_typo_issue_count = int(summary.get("shared_typo_issue_count") or 0)
        group_suspicious_document_count = int(summary.get("suspicious_typo_document_count") or 0)

        groups[role] = {
            "documents": group.get("documents") or [],
            "skipped_documents": group.get("skipped_documents") or [],
            "typo_check": typo_check,
            "summary": {
                "document_count": group_document_count,
                "skipped_document_count": group_skipped_count,
                "typo_issue_count": group_typo_issue_count,
                "shared_typo_issue_count": group_shared_typo_issue_count,
                "suspicious_typo_document_count": group_suspicious_document_count,
                "suspicious": bool(group_typo_issue_count),
            },
        }

        total_document_count += group_document_count
        total_skipped_document_count += group_skipped_count
        total_typo_issue_count += group_typo_issue_count
        total_shared_typo_issue_count += group_shared_typo_issue_count
        total_suspicious_typo_document_count += group_suspicious_document_count

    config = review_result.get("config") or {}
    result = {
        "project": review_result.get("project"),
        "config": {
            "document_types": config.get("document_types") or [],
            "typo_detection_engine": config.get("typo_detection_engine"),
            "typo_model_name": config.get("typo_model_name"),
            "typo_model_threshold": config.get("typo_model_threshold"),
            "typo_engine_statuses": config.get("typo_engine_statuses") or [],
            "typo_model_load_error": config.get("typo_model_load_error"),
            "typo_stopword_dictionary_enabled": config.get("typo_stopword_dictionary_enabled"),
        },
        "groups": groups,
        "summary": {
            "requested_document_types": config.get("document_types") or [],
            "document_count": total_document_count,
            "skipped_document_count": total_skipped_document_count,
            "typo_issue_count": total_typo_issue_count,
            "shared_typo_issue_count": total_shared_typo_issue_count,
            "suspicious_typo_document_count": total_suspicious_typo_document_count,
            "suspicious": bool(total_typo_issue_count),
        },
    }
    return result


# 项目人员复用检查（组合调用审查+构建视图）
def _run_project_personnel_reuse_check(
    *,
    identifier_id: str,
    db_service: PostgreSQLService,
    bid_document_review_service: BidDocumentReviewService,
) -> dict:
    review_result = _run_project_bid_document_review(
        identifier_id=identifier_id,
        document_types=[DuplicateCheckScope.BUSINESS_BID.value],
        result_key="bid_document_review",
        db_service=db_service,
        bid_document_review_service=bid_document_review_service,
    )
    personnel_result = _build_personnel_reuse_result(review_result)
    db_service.upsert_project_result_item(
        project_identifier_id=identifier_id,
        result_key="personnel_reuse_check",
        result_value=personnel_result,
    )
    return personnel_result


# 项目错别字检查（组合调用审查+构建视图）
def _run_project_typo_check(
    *,
    identifier_id: str,
    db_service: PostgreSQLService,
    bid_document_review_service: BidDocumentReviewService,
) -> dict:
    review_result = _run_project_bid_document_review(
        identifier_id=identifier_id,
        document_types=None,
        result_key="bid_document_review",
        db_service=db_service,
        bid_document_review_service=bid_document_review_service,
    )
    typo_result = _build_typo_check_result(review_result)
    db_service.upsert_project_result_item(
        project_identifier_id=identifier_id,
        result_key="typo_check",
        result_value=typo_result,
    )
    return typo_result


# 构建项目快照（仅含标识）
def _project_snapshot(project_identifier: str) -> dict:
    return {"identifier_id": project_identifier}


# 为项目生成相关 API 链接
def _project_api_links(project_identifier: str) -> dict[str, str]:
    quoted_identifier = quote(project_identifier, safe="")
    return {
        "detail_url": f"/api/postgresql/projects/{quoted_identifier}",
        "results_url": f"/api/postgresql/projects/{quoted_identifier}/results",
        "merged_results_url": f"/api/postgresql/projects/{quoted_identifier}/merged-results",
        "visualization_url": f"/api/postgresql/projects/{quoted_identifier}/visualization-data",
    }


# 为文档生成相关 API 链接
def _document_api_links(document_identifier: str) -> dict[str, str]:
    quoted_identifier = quote(document_identifier, safe="")
    return {
        "detail_url": f"/api/postgresql/documents/{quoted_identifier}",
        "source_url": f"/api/postgresql/documents/{quoted_identifier}/source",
        "preview_url_template": f"/api/postgresql/documents/{quoted_identifier}/preview/pages/{{page}}",
    }


# 从项目详情的关系中收集所有文档标识
def _collect_project_document_identifiers(project_detail: dict[str, Any]) -> list[str]:
    identifiers: list[str] = []
    seen: set[str] = set()
    for relation in project_detail.get("relations") or []:
        for field_name in (
            "tender_identifier_id",
            "business_bid_identifier_id",
            "technical_bid_identifier_id",
        ):
            identifier = str(relation.get(field_name) or "").strip()
            if not identifier or identifier in seen:
                continue
            seen.add(identifier)
            identifiers.append(identifier)
    return identifiers


# 提取结果记录的元信息（轻量版）
def _build_result_record_meta(result_record: Optional[dict[str, Any]]) -> Optional[dict[str, Any]]:
    if not isinstance(result_record, dict) or not result_record:
        return None
    return {
        "id": result_record.get("id"),
        "project_identifier_id": result_record.get("project_identifier_id"),
        "create_time": result_record.get("create_time"),
        "update_time": result_record.get("update_time"),
    }


# 压缩显示结果（移除原始的查重合并键）
def _compact_display_results_for_response(display_results: dict[str, Any]) -> dict[str, Any]:
    compact_results = dict(display_results)
    compact_results.pop("duplicate_check", None)
    for merged_key in MERGED_RESULT_KEY_BY_DOC_TYPE.values():
        compact_results.pop(merged_key, None)
    return compact_results


# 组装项目可视化数据（前端大屏用）
def _build_project_visualization_payload(
    *,
    identifier_id: str,
    project_detail: dict[str, Any],
    project_result: Optional[dict[str, Any]],
    db_service: PostgreSQLService,
    include_document_content: bool = False,
    include_raw_results: bool = False,
    include_result_record: bool = False,
) -> dict[str, Any]:
    refreshed_result_record, raw_results, display_results = _build_project_display_results(
        identifier_id=identifier_id,
        result_record=project_result,
        db_service=db_service,
    )
    document_identifiers = _collect_project_document_identifiers(project_detail)
    document_map: dict[str, dict[str, Any]] = {}
    for document_identifier in document_identifiers:
        document = db_service.get_document_by_identifier(document_identifier)
        if not document:
            continue
        document_payload = dict(document)
        if not include_document_content:
            document_payload.pop("content", None)
        document_payload["links"] = _document_api_links(document_identifier)
        document_map[document_identifier] = document_payload

    relation_items: list[dict[str, Any]] = []
    for relation in project_detail.get("relations") or []:
        relation_payload = dict(relation)
        relation_payload["documents"] = {}
        for role_name, field_name in (
            ("tender", "tender_identifier_id"),
            ("business_bid", "business_bid_identifier_id"),
            ("technical_bid", "technical_bid_identifier_id"),
        ):
            document_identifier = str(relation.get(field_name) or "").strip()
            if not document_identifier:
                continue
            document_payload = document_map.get(document_identifier)
            if document_payload:
                relation_payload["documents"][role_name] = document_payload
        relation_items.append(relation_payload)

    compact_display_results = _compact_display_results_for_response(display_results)
    payload = {
        "project": project_detail.get("project") or {"identifier_id": identifier_id},
        "project_links": _project_api_links(identifier_id),
        "relations": relation_items,
        "documents": list(document_map.values()),
        "result_record_meta": _build_result_record_meta(refreshed_result_record or project_result),
        "results": compact_display_results,
        "available_result_keys": sorted(compact_display_results.keys()),
    }
    if include_result_record:
        payload["result_record"] = refreshed_result_record or project_result
    if include_raw_results:
        payload["raw_results"] = raw_results
        payload["raw_available_result_keys"] = sorted(raw_results.keys())
    return payload


# 统一持久化上传的 JSON 文件，并返回项目信息与文档记录
async def _persist_uploaded_analysis_documents(
    *,
    tender_json_file: UploadFile,
    business_bid_json_files: Optional[list[UploadFile]],
    technical_bid_json_files: Optional[list[UploadFile]],
    project_identifier: Optional[str],
    db_service: PostgreSQLService,
) -> tuple[dict, list[dict]]:
    tender_document = await read_uploaded_json_file(
        tender_json_file,
        field_name="tender_json_file",
    )
    business_documents = await load_uploaded_bid_json_documents(
        business_bid_json_files,
        field_name="business_bid_json_files",
        role=DOCUMENT_TYPE_BUSINESS_BID,
    )
    technical_documents = await load_uploaded_bid_json_documents(
        technical_bid_json_files,
        field_name="technical_bid_json_files",
        role=DOCUMENT_TYPE_TECHNICAL_BID,
    )
    persisted_documents = await persist_uploaded_json_project_documents(
        db_service=db_service,
        tender_document=tender_document,
        business_bid_documents=business_documents,
        technical_bid_documents=technical_documents,
        project_identifier=project_identifier,
    )
    document_records = build_uploaded_project_document_records(persisted_documents)
    return persisted_documents, document_records


# 将分析结果写入项目结果表
def _persist_uploaded_result(
    *,
    db_service: PostgreSQLService,
    project_identifier: str,
    result_key: str,
    result_value: dict,
) -> dict:
    return db_service.upsert_project_result_item(
        project_identifier_id=project_identifier,
        result_key=result_key,
        result_value=result_value,
    )


# 持久化查重合并结果（多个 key）
def _persist_duplicate_merge_results(
    *,
    db_service: PostgreSQLService,
    project_identifier: str,
    source_result_key: str,
    raw_result: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    merged_results = build_duplicate_merge_results(
        raw_result=raw_result,
        source_result_key=source_result_key,
    )
    for merged_key, merged_value in merged_results.items():
        db_service.upsert_project_result_item(
            project_identifier_id=project_identifier,
            result_key=merged_key,
            result_value=merged_value,
        )
    return merged_results


# 查找合并结果对应的原始结果键和值
def _resolve_merged_result_source(
    *,
    merged_result_key: str,
    results: dict[str, Any],
) -> tuple[Optional[str], Optional[dict[str, Any]]]:
    doc_type = DOC_TYPE_BY_MERGED_RESULT_KEY.get(merged_result_key)
    if not doc_type:
        return None, None
    preferred_raw_key = RAW_RESULT_KEY_BY_DOC_TYPE.get(doc_type)
    if preferred_raw_key and isinstance(results.get(preferred_raw_key), dict):
        return preferred_raw_key, results.get(preferred_raw_key)
    combined_result = results.get("duplicate_check")
    if isinstance(combined_result, dict):
        return "duplicate_check", combined_result
    return None, None


# 按需加载或重建项目的合并查重结果
def _load_or_build_project_merged_results(
    *,
    identifier_id: str,
    result_record: Optional[dict[str, Any]],
    db_service: PostgreSQLService,
    requested_keys: Optional[list[str]] = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    results = dict((result_record or {}).get("result") or {})
    target_keys = requested_keys or sorted(
        {
            merged_key
            for merged_key in MERGED_RESULT_KEY_BY_DOC_TYPE.values()
            if (
                merged_key in results
                or _resolve_merged_result_source(merged_result_key=merged_key, results=results)[0]
            )
        }
    )

    merged_payloads: dict[str, Any] = {}
    changed = False
    for merged_key in target_keys:
        existing = results.get(merged_key)
        if isinstance(existing, dict) and existing:
            merged_payloads[merged_key] = existing
            continue

        source_result_key, source_result = _resolve_merged_result_source(
            merged_result_key=merged_key,
            results=results,
        )
        if not source_result_key or not isinstance(source_result, dict):
            continue

        built_results = build_duplicate_merge_results(
            raw_result=source_result,
            source_result_key=source_result_key,
        )
        built_payload = built_results.get(merged_key)
        if not isinstance(built_payload, dict):
            continue

        results[merged_key] = built_payload
        merged_payloads[merged_key] = built_payload
        db_service.upsert_project_result_item(
            project_identifier_id=identifier_id,
            result_key=merged_key,
            result_value=built_payload,
        )
        changed = True

    refreshed_result_record = result_record
    if changed:
        refreshed_result_record = db_service.get_project_result(identifier_id)
        results = dict((refreshed_result_record or {}).get("result") or {})
        merged_payloads = {
            key: value
            for key, value in ((key, results.get(key)) for key in target_keys)
            if isinstance(value, dict)
        }

    return refreshed_result_record or result_record or {}, merged_payloads


# 构建项目展示用结果（含合并查重的替换）
def _build_project_display_results(
    *,
    identifier_id: str,
    result_record: Optional[dict[str, Any]],
    db_service: PostgreSQLService,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    refreshed_record, merged_results = _load_or_build_project_merged_results(
        identifier_id=identifier_id,
        result_record=result_record,
        db_service=db_service,
    )
    raw_results = dict((refreshed_record or result_record or {}).get("result") or {})
    display_results = dict(raw_results)

    merged_key_aliases = {
        "business_bid_duplicate_check": MERGED_RESULT_KEY_BY_DOC_TYPE.get(DOCUMENT_TYPE_BUSINESS_BID),
        "technical_bid_duplicate_check": MERGED_RESULT_KEY_BY_DOC_TYPE.get(DOCUMENT_TYPE_TECHNICAL_BID),
    }
    for raw_key, merged_key in merged_key_aliases.items():
        if not merged_key:
            continue
        merged_payload = merged_results.get(merged_key)
        if isinstance(merged_payload, dict) and merged_payload:
            display_results[raw_key] = merged_payload
            display_results[merged_key] = merged_payload

    if "duplicate_check" in display_results:
        display_results["duplicate_check"] = {
            "view_mode": "merged",
            "business_bid_duplicate_check": display_results.get("business_bid_duplicate_check"),
            "technical_bid_duplicate_check": display_results.get("technical_bid_duplicate_check"),
        }

    return refreshed_record or result_record or {}, raw_results, display_results


# ======================== 路由定义 ========================

# 项目 CRUD
@router.post("/projects", summary="创建项目")
async def create_project(
    payload: ProjectCreateRequest,
    db_service: PostgreSQLService = Depends(get_db_service),
):
    """创建新项目，若项目标识已存在则返回 409。"""
    try:
        return db_service.create_project(
            identifier_id=payload.identifier_id,
        )
    except PsycopgError as exc:
        if getattr(exc, "pgcode", None) == "23505":
            raise HTTPException(status_code=409, detail="项目标识已存在") from exc
        raise HTTPException(status_code=500, detail=f"数据库错误：{exc}") from exc


@router.get("/projects", summary="查询项目列表")
async def list_projects(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=200),
    limit: Optional[int] = Query(default=None, ge=1, le=200),
    offset: Optional[int] = Query(default=None, ge=0),
    keyword: Optional[str] = Query(default=None, description="按项目标识模糊搜索"),
    db_service: PostgreSQLService = Depends(get_db_service),
):
    """分页查询项目列表，支持关键字搜索。"""
    try:
        resolved_limit, resolved_offset = _resolve_pagination(
            page=page,
            page_size=page_size,
            limit=limit,
            offset=offset,
        )
        return db_service.list_projects(
            limit=resolved_limit,
            offset=resolved_offset,
            keyword=keyword,
        )
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"数据库错误：{exc}") from exc


@router.post("/projects/batch-delete", summary="批量删除项目")
async def batch_delete_projects(
    payload: IdentifierBatchDeleteRequest,
    db_service: PostgreSQLService = Depends(get_db_service),
):
    """软删除指定标识集合中的项目。"""
    try:
        deleted_count = db_service.soft_delete_projects(payload.identifier_ids)
        return {
            "requested_count": len(payload.identifier_ids),
            "deleted_count": deleted_count,
        }
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"数据库错误：{exc}") from exc


@router.get("/projects/{identifier_id}", summary="查询项目详情")
async def get_project_detail(
    identifier_id: str,
    db_service: PostgreSQLService = Depends(get_db_service),
):
    """按项目标识返回项目详情（含绑定关系）。"""
    try:
        detail = db_service.get_project_detail(identifier_id)
        if not detail:
            raise HTTPException(status_code=404, detail="项目不存在")
        return detail
    except HTTPException:
        raise
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"数据库错误：{exc}") from exc


@router.put("/projects/{identifier_id}", summary="更新项目标识")
async def update_project(
    identifier_id: str,
    payload: ProjectUpdateRequest,
    db_service: PostgreSQLService = Depends(get_db_service),
):
    """修改项目标识为新的标识字符串。"""
    try:
        updated = db_service.update_project(
            identifier_id=identifier_id,
            new_identifier_id=payload.new_identifier_id,
        )
        if not updated:
            raise HTTPException(status_code=404, detail="项目不存在")
        return updated
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except PsycopgError as exc:
        if getattr(exc, "pgcode", None) == "23505":
            raise HTTPException(status_code=409, detail="项目标识已存在") from exc
        raise HTTPException(status_code=500, detail=f"数据库错误：{exc}") from exc


@router.delete("/projects/{identifier_id}", summary="删除项目")
async def delete_project(
    identifier_id: str,
    db_service: PostgreSQLService = Depends(get_db_service),
):
    """软删除项目。"""
    try:
        deleted = db_service.soft_delete_project(identifier_id)
        if not deleted:
            raise HTTPException(status_code=404, detail="项目不存在")
        return {"status": "deleted"}
    except HTTPException:
        raise
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"数据库错误：{exc}") from exc


# 项目分析结果
@router.get("/projects/{identifier_id}/results", summary="查询项目分析结果")
async def get_project_results(
    identifier_id: str,
    view: Literal["display", "raw"] = Query(
        default="display",
        description="display=默认返回 merge 后的展示结果，raw=返回原始结果",
    ),
    include_raw_results: bool = Query(default=False),
    include_result_record: bool = Query(default=False),
    db_service: PostgreSQLService = Depends(get_db_service),
):
    """获取项目的分析结果，支持展示视图和原始视图，可附加返回原始数据。"""
    try:
        project = db_service.get_project_by_identifier(identifier_id)
        if not project:
            raise HTTPException(status_code=404, detail="project not found")

        result_record = db_service.get_project_result(identifier_id)
        refreshed_record = result_record or {}
        raw_results = dict((result_record or {}).get("result") or {})
        display_results = raw_results
        if view == "display" or include_raw_results:
            refreshed_record, raw_results, display_results = _build_project_display_results(
                identifier_id=identifier_id,
                result_record=result_record,
                db_service=db_service,
            )

        selected_results = (
            _compact_display_results_for_response(display_results)
            if view == "display"
            else raw_results
        )
        payload = {
            "project": project,
            "view": view,
            "result_record_meta": _build_result_record_meta(refreshed_record or result_record),
            "results": selected_results,
            "available_result_keys": sorted(selected_results.keys()),
        }
        if include_result_record:
            payload["result_record"] = refreshed_record or result_record
        if include_raw_results:
            payload["raw_results"] = raw_results
            payload["raw_available_result_keys"] = sorted(raw_results.keys())
        return payload
    except HTTPException:
        raise
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"database error: {exc}") from exc


@router.get("/projects/{identifier_id}/results/{result_key}", summary="查询项目单项分析结果")
async def get_project_result_item(
    identifier_id: str,
    result_key: str,
    view: Literal["display", "raw"] = Query(default="display"),
    include_result_record: bool = Query(default=False),
    db_service: PostgreSQLService = Depends(get_db_service),
):
    """获取项目下特定键的分析结果。"""
    try:
        project = db_service.get_project_by_identifier(identifier_id)
        if not project:
            raise HTTPException(status_code=404, detail="project not found")

        result_record = db_service.get_project_result(identifier_id)
        refreshed_record, raw_results, display_results = _build_project_display_results(
            identifier_id=identifier_id,
            result_record=result_record,
            db_service=db_service,
        )
        selected_results = display_results if view == "display" else raw_results
        if result_key not in selected_results:
            raise HTTPException(status_code=404, detail="result_key not found")

        payload = {
            "result_record_meta": _build_result_record_meta(refreshed_record or result_record),
            "project": project,
            "result_key": result_key,
            "view": view,
            "result": selected_results[result_key],
            "available_result_keys": sorted(selected_results.keys()),
        }
        if include_result_record:
            payload["result_record"] = refreshed_record or result_record
        return payload
    except HTTPException:
        raise
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"database error: {exc}") from exc


@router.get("/projects/{identifier_id}/merged-results", summary="查询项目查重合并结果")
async def get_project_merged_results(
    identifier_id: str,
    result_key: Optional[str] = Query(default=None),
    include_result_record: bool = Query(default=False),
    db_service: PostgreSQLService = Depends(get_db_service),
):
    """获取查重后的合并结果（聚类视图）。"""
    try:
        project = db_service.get_project_by_identifier(identifier_id)
        if not project:
            raise HTTPException(status_code=404, detail="project not found")

        requested_keys: Optional[list[str]] = None
        if result_key is not None:
            normalized_key = str(result_key or "").strip()
            if normalized_key not in DOC_TYPE_BY_MERGED_RESULT_KEY:
                raise HTTPException(status_code=400, detail="unsupported merged result key")
            requested_keys = [normalized_key]

        result_record = db_service.get_project_result(identifier_id)
        refreshed_record, merged_results = _load_or_build_project_merged_results(
            identifier_id=identifier_id,
            result_record=result_record,
            db_service=db_service,
            requested_keys=requested_keys,
        )
        if requested_keys and requested_keys[0] not in merged_results:
            raise HTTPException(status_code=404, detail="merged result not found")

        payload = {
            "project": project,
            "result_record_meta": _build_result_record_meta(refreshed_record or result_record),
            "results": merged_results,
            "available_result_keys": sorted(merged_results.keys()),
        }
        if include_result_record:
            payload["result_record"] = refreshed_record or result_record
        return payload
    except HTTPException:
        raise
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"database error: {exc}") from exc


@router.get("/projects/{identifier_id}/visualization-data", summary="查询项目可视化聚合数据")
async def get_project_visualization_data(
    identifier_id: str,
    include_document_content: bool = Query(default=False),
    include_raw_results: bool = Query(default=False),
    include_result_record: bool = Query(default=False),
    db_service: PostgreSQLService = Depends(get_db_service),
):
    """返回前端可视化所需的聚合数据（项目、文档、关系、结果）。"""
    try:
        project_detail = db_service.get_project_detail(identifier_id)
        if not project_detail:
            raise HTTPException(status_code=404, detail="project not found")

        project_result = db_service.get_project_result(identifier_id)
        return _build_project_visualization_payload(
            identifier_id=identifier_id,
            project_detail=project_detail,
            project_result=project_result,
            db_service=db_service,
            include_document_content=include_document_content,
            include_raw_results=include_raw_results,
            include_result_record=include_result_record,
        )
    except HTTPException:
        raise
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"database error: {exc}") from exc


# 全局结果管理（不限定项目）
@router.get("/results", summary="查询结果表列表")
async def list_results(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=200),
    limit: Optional[int] = Query(default=None, ge=1, le=200),
    offset: Optional[int] = Query(default=None, ge=0),
    keyword: Optional[str] = Query(default=None),
    db_service: PostgreSQLService = Depends(get_db_service),
):
    """分页查看所有项目的结果记录。"""
    try:
        resolved_limit, resolved_offset = _resolve_pagination(
            page=page,
            page_size=page_size,
            limit=limit,
            offset=offset,
        )
        return db_service.list_project_results(
            limit=resolved_limit,
            offset=resolved_offset,
            keyword=keyword,
        )
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"database error: {exc}") from exc


@router.post("/results", summary="创建或覆盖项目结果")
async def create_or_replace_result(
    payload: ProjectResultUpsertRequest,
    db_service: PostgreSQLService = Depends(get_db_service),
):
    """创建或完全替换某个项目的结果数据。"""
    try:
        return db_service.create_or_replace_project_result(
            project_identifier_id=payload.project_identifier_id,
            result=payload.result,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"database error: {exc}") from exc


@router.get("/results/{project_identifier_id}", summary="查询单个项目结果")
async def get_result_record(
    project_identifier_id: str,
    db_service: PostgreSQLService = Depends(get_db_service),
):
    """按项目标识获取结果记录。"""
    try:
        result_record = db_service.get_project_result(project_identifier_id)
        if not result_record:
            raise HTTPException(status_code=404, detail="result record not found")
        return result_record
    except HTTPException:
        raise
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"database error: {exc}") from exc


@router.put("/results/{project_identifier_id}", summary="更新单个项目结果")
async def update_result_record(
    project_identifier_id: str,
    payload: ProjectResultUpdateRequest,
    db_service: PostgreSQLService = Depends(get_db_service),
):
    """更新指定项目的结果数据（覆盖）。"""
    try:
        return db_service.create_or_replace_project_result(
            project_identifier_id=project_identifier_id,
            result=payload.result,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"database error: {exc}") from exc


@router.delete("/results/{project_identifier_id}", summary="删除单个项目结果")
async def delete_result_record(
    project_identifier_id: str,
    db_service: PostgreSQLService = Depends(get_db_service),
):
    """删除某个项目的所有分析结果。"""
    try:
        deleted = db_service.delete_project_result(project_identifier_id)
        if not deleted:
            raise HTTPException(status_code=404, detail="result record not found")
        return {"status": "deleted"}
    except HTTPException:
        raise
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"database error: {exc}") from exc


@router.post("/results/batch-delete", summary="批量删除项目结果")
async def batch_delete_result_records(
    payload: IdentifierBatchDeleteRequest,
    db_service: PostgreSQLService = Depends(get_db_service),
):
    """批量删除多个项目的结果记录。"""
    try:
        deleted_count = db_service.delete_project_results(payload.identifier_ids)
        return {
            "requested_count": len(payload.identifier_ids),
            "deleted_count": deleted_count,
        }
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"database error: {exc}") from exc


# 业务检查接口（查重、形式审查、人员复用、错别字）
@router.post("/projects/duplicate-check", summary="项目商务标/技术标查重")
async def project_duplicate_check(
    identifier_id: str = Query(...),
    document_scope: DuplicateCheckScope = Query(default=DuplicateCheckScope.ALL),
    max_evidence_sections: int = Query(default=5, ge=1, le=20),
    max_pairs_per_type: int = Query(default=0, ge=0, le=500),
    db_service: PostgreSQLService = Depends(get_db_service),
    duplicate_check_service: DuplicateCheckService = Depends(get_duplicate_check_service),
):
    """执行项目的商务标/技术标内容查重，结果持久化。"""
    try:
        return await run_in_threadpool(
            _run_project_duplicate_check,
            identifier_id=identifier_id,
            document_types=_document_types_from_scope(document_scope),
            max_evidence_sections=max_evidence_sections,
            max_pairs_per_type=max_pairs_per_type,
            result_key="duplicate_check",
            db_service=db_service,
            duplicate_check_service=duplicate_check_service,
        )
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"数据库错误：{exc}") from exc


@router.post("/projects/business-bid-format-review", summary="项目商务标形式审查")
async def project_business_bid_format_review(
    identifier_id: str = Query(...),
    db_service: PostgreSQLService = Depends(get_db_service),
):
    """对项目中的商务标进行格式合规性检查。"""
    review_service = UnifiedBusinessReviewService(db_service=db_service)
    try:
        return await run_in_threadpool(
            review_service.persist_project_business_review,
            project_identifier=identifier_id,
            result_key=UnifiedBusinessReviewService.BUSINESS_RESULT_KEY,
        )
    except ValueError as exc:
        if "project not found" in str(exc).lower():
            raise HTTPException(status_code=404, detail="项目不存在") from exc
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"数据库错误：{exc}") from exc


@router.post("/projects/business-bid-duplicate-check", summary="项目商务标内容查重")
async def project_business_bid_duplicate_check(
    identifier_id: str = Query(...),
    max_evidence_sections: int = Query(default=5, ge=1, le=20),
    max_pairs_per_type: int = Query(default=0, ge=0, le=500),
    db_service: PostgreSQLService = Depends(get_db_service),
    duplicate_check_service: DuplicateCheckService = Depends(get_duplicate_check_service),
):
    """仅对商务标进行内容查重。"""
    try:
        return await run_in_threadpool(
            _run_project_duplicate_check,
            identifier_id=identifier_id,
            document_types=[DuplicateCheckScope.BUSINESS_BID.value],
            max_evidence_sections=max_evidence_sections,
            max_pairs_per_type=max_pairs_per_type,
            result_key="business_bid_duplicate_check",
            db_service=db_service,
            duplicate_check_service=duplicate_check_service,
        )
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"数据库错误：{exc}") from exc


@router.post("/projects/business-bid-duplicate-check/upload-json", summary="上传 OCR JSON 并执行商务标内容查重")
async def upload_business_bid_duplicate_check(
    tender_json_file: UploadFile = File(...),
    business_bid_json_files: list[UploadFile] = File(...),
    technical_bid_json_files: Optional[list[UploadFile]] = File(default=None),
    project_identifier: Optional[str] = Form(default=None),
    max_evidence_sections: int = Form(default=5, ge=1, le=20),
    max_pairs_per_type: int = Form(default=0, ge=0, le=500),
    db_service: PostgreSQLService = Depends(get_db_service),
    duplicate_check_service: DuplicateCheckService = Depends(get_duplicate_check_service),
):
    """上传招投标 OCR JSON 文件，直接执行商务标内容查重。"""
    uploads = [upload for upload in business_bid_json_files if upload is not None]
    if not uploads:
        raise HTTPException(status_code=400, detail="business_bid_json_files 不能为空。")

    try:
        persisted_documents, document_records = await _persist_uploaded_analysis_documents(
            tender_json_file=tender_json_file,
            business_bid_json_files=uploads,
            technical_bid_json_files=technical_bid_json_files,
            project_identifier=project_identifier,
            db_service=db_service,
        )
        resolved_project_identifier = persisted_documents["project"]["identifier_id"]
        duplicate_result = await run_in_threadpool(
            duplicate_check_service.check_project_documents,
            project_identifier=resolved_project_identifier,
            project=_project_snapshot(resolved_project_identifier),
            document_records=document_records,
            document_types=[DuplicateCheckScope.BUSINESS_BID.value],
            max_evidence_sections=max_evidence_sections,
            max_pairs_per_type=max_pairs_per_type,
        )
        result_record = await run_in_threadpool(
            _persist_uploaded_result,
            db_service=db_service,
            project_identifier=resolved_project_identifier,
            result_key="business_bid_duplicate_check",
            result_value=duplicate_result,
        )
        merged_results = await run_in_threadpool(
            _persist_duplicate_merge_results,
            db_service=db_service,
            project_identifier=resolved_project_identifier,
            source_result_key="business_bid_duplicate_check",
            raw_result=duplicate_result,
        )
        return {
            "project": persisted_documents["project"],
            "result_key": "business_bid_duplicate_check",
            "result": duplicate_result,
            "result_record": result_record,
            "merged_results": merged_results,
            "document_binding": persisted_documents["binding"],
        }
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"数据库错误：{exc}") from exc


@router.post("/projects/technical-bid-duplicate-check", summary="项目技术标内容查重")
async def project_technical_bid_duplicate_check(
    identifier_id: str = Query(...),
    max_evidence_sections: int = Query(default=5, ge=1, le=20),
    max_pairs_per_type: int = Query(default=0, ge=0, le=500),
    db_service: PostgreSQLService = Depends(get_db_service),
    duplicate_check_service: DuplicateCheckService = Depends(get_duplicate_check_service),
):
    """仅对技术标进行内容查重。"""
    try:
        return await run_in_threadpool(
            _run_project_duplicate_check,
            identifier_id=identifier_id,
            document_types=[DuplicateCheckScope.TECHNICAL_BID.value],
            max_evidence_sections=max_evidence_sections,
            max_pairs_per_type=max_pairs_per_type,
            result_key="technical_bid_duplicate_check",
            db_service=db_service,
            duplicate_check_service=duplicate_check_service,
        )
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"数据库错误：{exc}") from exc


@router.post("/projects/technical-bid-duplicate-check/upload-json", summary="上传 OCR JSON 并执行技术标内容查重")
async def upload_technical_bid_duplicate_check(
    tender_json_file: UploadFile = File(...),
    technical_bid_json_files: list[UploadFile] = File(...),
    business_bid_json_files: Optional[list[UploadFile]] = File(default=None),
    project_identifier: Optional[str] = Form(default=None),
    max_evidence_sections: int = Form(default=5, ge=1, le=20),
    max_pairs_per_type: int = Form(default=0, ge=0, le=500),
    db_service: PostgreSQLService = Depends(get_db_service),
    duplicate_check_service: DuplicateCheckService = Depends(get_duplicate_check_service),
):
    """上传招投标 OCR JSON 文件，直接执行技术标内容查重。"""
    uploads = [upload for upload in technical_bid_json_files if upload is not None]
    if not uploads:
        raise HTTPException(status_code=400, detail="technical_bid_json_files 不能为空。")

    try:
        persisted_documents, document_records = await _persist_uploaded_analysis_documents(
            tender_json_file=tender_json_file,
            business_bid_json_files=business_bid_json_files,
            technical_bid_json_files=uploads,
            project_identifier=project_identifier,
            db_service=db_service,
        )
        resolved_project_identifier = persisted_documents["project"]["identifier_id"]
        duplicate_result = await run_in_threadpool(
            duplicate_check_service.check_project_documents,
            project_identifier=resolved_project_identifier,
            project=_project_snapshot(resolved_project_identifier),
            document_records=document_records,
            document_types=[DuplicateCheckScope.TECHNICAL_BID.value],
            max_evidence_sections=max_evidence_sections,
            max_pairs_per_type=max_pairs_per_type,
        )
        result_record = await run_in_threadpool(
            _persist_uploaded_result,
            db_service=db_service,
            project_identifier=resolved_project_identifier,
            result_key="technical_bid_duplicate_check",
            result_value=duplicate_result,
        )
        merged_results = await run_in_threadpool(
            _persist_duplicate_merge_results,
            db_service=db_service,
            project_identifier=resolved_project_identifier,
            source_result_key="technical_bid_duplicate_check",
            raw_result=duplicate_result,
        )
        return {
            "project": persisted_documents["project"],
            "result_key": "technical_bid_duplicate_check",
            "result": duplicate_result,
            "result_record": result_record,
            "merged_results": merged_results,
            "document_binding": persisted_documents["binding"],
        }
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"数据库错误：{exc}") from exc


@router.post("/projects/personnel-reuse-check", summary="项目一人多用检查")
async def project_personnel_reuse_check(
    identifier_id: str = Query(...),
    db_service: PostgreSQLService = Depends(get_db_service),
    bid_document_review_service: BidDocumentReviewService = Depends(get_bid_document_review_service),
):
    """检查商务标中是否存在同一人员出现在多家投标单位的情况。"""
    try:
        return await run_in_threadpool(
            _run_project_personnel_reuse_check,
            identifier_id=identifier_id,
            db_service=db_service,
            bid_document_review_service=bid_document_review_service,
        )
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"数据库错误：{exc}") from exc


@router.post("/projects/personnel-reuse-check/upload-json", summary="上传 OCR JSON 并执行一人多用检查")
async def upload_personnel_reuse_check(
    tender_json_file: UploadFile = File(...),
    business_bid_json_files: list[UploadFile] = File(...),
    technical_bid_json_files: Optional[list[UploadFile]] = File(default=None),
    project_identifier: Optional[str] = Form(default=None),
    db_service: PostgreSQLService = Depends(get_db_service),
    bid_document_review_service: BidDocumentReviewService = Depends(get_bid_document_review_service),
):
    """上传招投标 OCR JSON 文件，直接执行人员复用检查。"""
    uploads = [upload for upload in business_bid_json_files if upload is not None]
    if not uploads:
        raise HTTPException(status_code=400, detail="business_bid_json_files 不能为空。")

    try:
        persisted_documents, document_records = await _persist_uploaded_analysis_documents(
            tender_json_file=tender_json_file,
            business_bid_json_files=uploads,
            technical_bid_json_files=technical_bid_json_files,
            project_identifier=project_identifier,
            db_service=db_service,
        )
        resolved_project_identifier = persisted_documents["project"]["identifier_id"]
        review_result = await run_in_threadpool(
            bid_document_review_service.check_project_documents,
            project_identifier=resolved_project_identifier,
            project=_project_snapshot(resolved_project_identifier),
            document_records=document_records,
            document_types=[DuplicateCheckScope.BUSINESS_BID.value],
        )
        personnel_result = _build_personnel_reuse_result(review_result)
        result_record = await run_in_threadpool(
            _persist_uploaded_result,
            db_service=db_service,
            project_identifier=resolved_project_identifier,
            result_key="personnel_reuse_check",
            result_value=personnel_result,
        )
        await run_in_threadpool(
            _persist_uploaded_result,
            db_service=db_service,
            project_identifier=resolved_project_identifier,
            result_key="bid_document_review",
            result_value=review_result,
        )
        return {
            "project": persisted_documents["project"],
            "result_key": "personnel_reuse_check",
            "result": personnel_result,
            "result_record": result_record,
            "document_binding": persisted_documents["binding"],
        }
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"数据库错误：{exc}") from exc


@router.post("/projects/typo-check", summary="项目错别字检查")
async def project_typo_check(
    identifier_id: str = Query(...),
    db_service: PostgreSQLService = Depends(get_db_service),
    bid_document_review_service: BidDocumentReviewService = Depends(get_bid_document_review_service),
):
    """对项目中的文档进行错别字扫描。"""
    try:
        return await run_in_threadpool(
            _run_project_typo_check,
            identifier_id=identifier_id,
            db_service=db_service,
            bid_document_review_service=bid_document_review_service,
        )
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"数据库错误：{exc}") from exc


@router.post("/projects/typo-check/upload-json", summary="上传 OCR JSON 并执行错别字检查")
async def upload_typo_check(
    tender_json_file: UploadFile = File(...),
    business_bid_json_files: Optional[list[UploadFile]] = File(default=None),
    technical_bid_json_files: Optional[list[UploadFile]] = File(default=None),
    project_identifier: Optional[str] = Form(default=None),
    db_service: PostgreSQLService = Depends(get_db_service),
    bid_document_review_service: BidDocumentReviewService = Depends(get_bid_document_review_service),
):
    """上传招投标 OCR JSON 文件，直接执行错别字检查。"""
    business_uploads = [upload for upload in (business_bid_json_files or []) if upload is not None]
    technical_uploads = [upload for upload in (technical_bid_json_files or []) if upload is not None]
    if not business_uploads and not technical_uploads:
        raise HTTPException(status_code=400, detail="至少需要上传一份商务标或技术标文件。")

    try:
        persisted_documents, document_records = await _persist_uploaded_analysis_documents(
            tender_json_file=tender_json_file,
            business_bid_json_files=business_uploads,
            technical_bid_json_files=technical_uploads,
            project_identifier=project_identifier,
            db_service=db_service,
        )
        resolved_project_identifier = persisted_documents["project"]["identifier_id"]
        review_result = await run_in_threadpool(
            bid_document_review_service.check_project_documents,
            project_identifier=resolved_project_identifier,
            project=_project_snapshot(resolved_project_identifier),
            document_records=document_records,
            document_types=None,
        )
        typo_result = _build_typo_check_result(review_result)
        result_record = await run_in_threadpool(
            _persist_uploaded_result,
            db_service=db_service,
            project_identifier=resolved_project_identifier,
            result_key="typo_check",
            result_value=typo_result,
        )
        await run_in_threadpool(
            _persist_uploaded_result,
            db_service=db_service,
            project_identifier=resolved_project_identifier,
            result_key="bid_document_review",
            result_value=review_result,
        )
        return {
            "project": persisted_documents["project"],
            "result_key": "typo_check",
            "result": typo_result,
            "result_record": result_record,
            "document_binding": persisted_documents["binding"],
        }
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"数据库错误：{exc}") from exc


@router.post("/projects/bid-document-review", summary="项目投标文件审查")
async def project_bid_document_review(
    identifier_id: str = Query(...),
    document_scope: DuplicateCheckScope = Query(default=DuplicateCheckScope.ALL),
    db_service: PostgreSQLService = Depends(get_db_service),
    bid_document_review_service: BidDocumentReviewService = Depends(get_bid_document_review_service),
):
    """综合审查投标文件（可指定商务标/技术标范围）。"""
    try:
        return await run_in_threadpool(
            _run_project_bid_document_review,
            identifier_id=identifier_id,
            document_types=_document_types_from_scope(document_scope),
            result_key="bid_document_review",
            db_service=db_service,
            bid_document_review_service=bid_document_review_service,
        )
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"数据库错误：{exc}") from exc


# 旧版兼容路由（不在 Swagger 文档中展示）
@router.post("/projects/technical-bid-review", summary="旧版项目投标文件审查", include_in_schema=False)
async def project_technical_bid_review_legacy(
    identifier_id: str = Query(...),
    document_scope: DuplicateCheckScope = Query(default=DuplicateCheckScope.ALL),
    db_service: PostgreSQLService = Depends(get_db_service),
    bid_document_review_service: BidDocumentReviewService = Depends(get_bid_document_review_service),
):
    return await run_in_threadpool(
        _run_project_bid_document_review,
        identifier_id=identifier_id,
        document_types=_document_types_from_scope(document_scope),
        result_key="bid_document_review",
        db_service=db_service,
        bid_document_review_service=bid_document_review_service,
    )


@router.post("/projects/{identifier_id}/duplicate-check", summary="项目商务标/技术标查重", include_in_schema=False)
async def project_duplicate_check_legacy(
    identifier_id: str,
    payload: Optional[ProjectDuplicateCheckRequest] = None,
    db_service: PostgreSQLService = Depends(get_db_service),
    duplicate_check_service: DuplicateCheckService = Depends(get_duplicate_check_service),
):
    request_payload = payload or ProjectDuplicateCheckRequest()
    return await run_in_threadpool(
        _run_project_duplicate_check,
        identifier_id=identifier_id,
        document_types=request_payload.document_types,
        max_evidence_sections=request_payload.max_evidence_sections,
        max_pairs_per_type=request_payload.max_pairs_per_type,
        result_key="duplicate_check",
        db_service=db_service,
        duplicate_check_service=duplicate_check_service,
    )


# 文档与绑定关系管理
@router.post("/projects/{identifier_id}/bind-documents", summary="绑定招标/商务标/技术标文件")
async def bind_project_documents(
    identifier_id: str,
    payload: ProjectBindDocumentsRequest,
    db_service: PostgreSQLService = Depends(get_db_service),
):
    """将三个文档标识绑定到项目下形成一个关系记录。"""
    try:
        return db_service.bind_project_documents(
            identifier_id,
            payload.tender_document_identifier,
            payload.business_bid_document_identifier,
            payload.technical_bid_document_identifier,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"数据库错误：{exc}") from exc


@router.get("/relations", summary="查询项目文件绑定列表")
async def list_relations(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=200),
    limit: Optional[int] = Query(default=None, ge=1, le=200),
    offset: Optional[int] = Query(default=None, ge=0),
    keyword: Optional[str] = Query(default=None),
    project_identifier: Optional[str] = Query(default=None),
    db_service: PostgreSQLService = Depends(get_db_service),
):
    """分页查询项目文档绑定关系。"""
    try:
        resolved_limit, resolved_offset = _resolve_pagination(
            page=page,
            page_size=page_size,
            limit=limit,
            offset=offset,
        )
        return db_service.list_relations(
            limit=resolved_limit,
            offset=resolved_offset,
            keyword=keyword,
            project_identifier=project_identifier,
        )
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"数据库错误：{exc}") from exc


@router.get("/relations/{relation_id}", summary="查询关联详情")
async def get_relation_detail(
    relation_id: int,
    db_service: PostgreSQLService = Depends(get_db_service),
):
    """按 ID 获取单条绑定关系。"""
    try:
        relation = db_service.get_relation_by_id(relation_id)
        if not relation:
            raise HTTPException(status_code=404, detail="关联不存在")
        return relation
    except HTTPException:
        raise
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"数据库错误：{exc}") from exc


@router.put("/relations/{relation_id}", summary="更新关联")
async def update_relation(
    relation_id: int,
    payload: ProjectRelationUpdateRequest,
    db_service: PostgreSQLService = Depends(get_db_service),
):
    """修改已有的文档绑定关系。"""
    try:
        updated = db_service.update_relation(
            relation_id=relation_id,
            tender_document_identifier=payload.tender_document_identifier,
            business_bid_document_identifier=payload.business_bid_document_identifier,
            technical_bid_document_identifier=payload.technical_bid_document_identifier,
        )
        if not updated:
            raise HTTPException(status_code=404, detail="关联不存在")
        return updated
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"数据库错误：{exc}") from exc


@router.delete("/relations/{relation_id}", summary="删除关联")
async def delete_relation(
    relation_id: int,
    db_service: PostgreSQLService = Depends(get_db_service),
):
    """删除一条文档绑定关系。"""
    try:
        deleted = db_service.delete_relation(relation_id)
        if not deleted:
            raise HTTPException(status_code=404, detail="关联不存在")
        return {"status": "deleted"}
    except HTTPException:
        raise
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"数据库错误：{exc}") from exc


@router.post("/relations/batch-delete", summary="批量删除关联")
async def batch_delete_relations(
    payload: RelationBatchDeleteRequest,
    db_service: PostgreSQLService = Depends(get_db_service),
):
    """批量删除绑定关系。"""
    try:
        deleted_count = db_service.delete_relations(payload.relation_ids)
        return {
            "requested_count": len(payload.relation_ids),
            "deleted_count": deleted_count,
        }
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"数据库错误：{exc}") from exc


# 文档 CUD 与预览
@router.post("/documents", summary="上传并创建文档")
async def create_document(
    file: UploadFile = File(...),
    document_type: DocumentType = Form(...),
    identifier_id: Optional[str] = Form(default=None),
    document_name: Optional[str] = Form(default=None),
    object_name: Optional[str] = Form(default=None),
    recognition_options: RecognitionOptions = Depends(get_form_recognition_options),
    db_service: PostgreSQLService = Depends(get_db_service),
    oss_service: MinioService = Depends(get_oss_service),
    analysis_service=Depends(get_text_analysis_service),
):
    """上传文件并触发 OCR 提取，创建文档记录。"""
    result = await upload_extract_and_create_document(
        file=file,
        document_type=document_type,
        db_service=db_service,
        oss_service=oss_service,
        analysis_service=analysis_service,
        identifier_id=identifier_id,
        document_name=document_name,
        object_name=object_name,
        **recognition_options.as_kwargs(),
        raise_http_exception=True,
    )
    return {
        "document": result["document"],
        "upload": result["upload"],
    }


@router.get("/documents", summary="查询文档列表")
async def list_documents(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=200),
    limit: Optional[int] = Query(default=None, ge=1, le=200),
    offset: Optional[int] = Query(default=None, ge=0),
    keyword: Optional[str] = Query(default=None),
    document_type: Optional[str] = Query(default=None),
    extracted: Optional[bool] = Query(default=None),
    db_service: PostgreSQLService = Depends(get_db_service),
):
    """分页查询文档列表，支持按类型、提取状态过滤。"""
    try:
        resolved_limit, resolved_offset = _resolve_pagination(
            page=page,
            page_size=page_size,
            limit=limit,
            offset=offset,
        )
        return db_service.list_documents(
            limit=resolved_limit,
            offset=resolved_offset,
            keyword=keyword,
            document_type=document_type,
            extracted=extracted,
        )
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"数据库错误：{exc}") from exc


@router.post("/documents/batch-delete", summary="批量删除文档")
async def batch_delete_documents(
    payload: IdentifierBatchDeleteRequest,
    db_service: PostgreSQLService = Depends(get_db_service),
):
    """软删除一批文档。"""
    try:
        deleted_count = db_service.soft_delete_documents(payload.identifier_ids)
        return {
            "requested_count": len(payload.identifier_ids),
            "deleted_count": deleted_count,
        }
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"数据库错误：{exc}") from exc


@router.get("/documents/{identifier_id}", summary="查询文档")
async def get_document(
    identifier_id: str,
    db_service: PostgreSQLService = Depends(get_db_service),
):
    """按标识获取单个文档详情。"""
    try:
        document = db_service.get_document_by_identifier(identifier_id)
        if not document:
            raise HTTPException(status_code=404, detail="文档不存在")
        return document
    except HTTPException:
        raise
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"数据库错误：{exc}") from exc


@router.put("/documents/{identifier_id}", summary="更新文档")
async def update_document(
    identifier_id: str,
    payload: DocumentUpdateRequest,
    db_service: PostgreSQLService = Depends(get_db_service),
):
    """更新文档的文件名或文件 URL。"""
    try:
        normalized_file_url = (
            normalize_file_url(payload.file_url) if payload.file_url is not None else None
        )
        updated = db_service.update_document(
            identifier_id=identifier_id,
            file_name=payload.file_name,
            file_url=normalized_file_url,
        )
        if not updated:
            raise HTTPException(status_code=404, detail="文档不存在")
        return updated
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"数据库错误：{exc}") from exc


@router.delete("/documents/{identifier_id}", summary="删除文档")
async def delete_document(
    identifier_id: str,
    db_service: PostgreSQLService = Depends(get_db_service),
):
    """软删除文档。"""
    try:
        deleted = db_service.soft_delete_document(identifier_id)
        if not deleted:
            raise HTTPException(status_code=404, detail="文档不存在")
        return {"status": "deleted"}
    except HTTPException:
        raise
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"数据库错误：{exc}") from exc


@router.get("/documents/{identifier_id}/source", summary="获取文档源文件")
async def get_document_source(
    identifier_id: str,
    page: Optional[int] = Query(default=None, ge=1, description="可选页码，仅 PDF 源文件支持跳转"),
    db_service: PostgreSQLService = Depends(get_db_service),
    oss_service: MinioService = Depends(get_oss_service),
):
    """重定向到文档在 MinIO 中的预签名下载 URL，支持 PDF 页码锚点。"""
    try:
        document = db_service.get_document_by_identifier(identifier_id)
        if not document:
            raise HTTPException(status_code=404, detail="document not found")

        bucket_name, object_name, _file_name = _resolve_document_source_object(document)
        presigned_url = oss_service.get_presigned_url(object_name, bucket_name)
        if page and _document_source_kind(document) == "pdf":
            presigned_url = f"{presigned_url}#page={page}"
        return RedirectResponse(url=presigned_url, status_code=307)
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"database error: {exc}") from exc


@router.get("/documents/{identifier_id}/preview/pages/{page}", summary="获取文档页面预览")
async def get_document_page_preview(
    identifier_id: str,
    page: int,
    highlight: Optional[list[str]] = Query(default=None, description="需要高亮的关键词，可传多个"),
    highlight_bbox: Optional[str] = Query(default=None, description="单个高亮框，格式为逗号分隔四个数字"),
    highlight_rects: Optional[str] = Query(default=None, description="多个高亮框的 JSON 数组字符串"),
    db_service: PostgreSQLService = Depends(get_db_service),
    oss_service: MinioService = Depends(get_oss_service),
):
    """返回文档指定页的 base64 预览图，支持文本/区域高亮。结果会被缓存。"""
    try:
        document = db_service.get_document_by_identifier(identifier_id)
        if not document:
            raise HTTPException(status_code=404, detail="document not found")

        normalized_highlight_phrases = _normalize_preview_highlight_phrases(highlight)
        normalized_highlight_bbox = _normalize_preview_highlight_bbox(highlight_bbox)
        normalized_highlight_rects = _normalize_preview_highlight_rects(highlight_rects)
        preview_variant = _preview_variant_signature(
            normalized_highlight_phrases,
            normalized_highlight_bbox,
            normalized_highlight_rects,
        )

        cached_payload = _preview_cache_get(document, page, preview_variant)
        if cached_payload is not None:
            return JSONResponse(cached_payload)

        source_kind = _document_source_kind(document)
        file_bytes, _content_type, _object_name = _load_document_source_bytes(
            document=document,
            oss_service=oss_service,
        )
        try:
            payload = _preview_payload_from_source(
                file_bytes=file_bytes,
                source_kind=source_kind,
                page=page,
                highlight_phrases=normalized_highlight_phrases,
                highlight_bbox=normalized_highlight_bbox,
                highlight_rects=normalized_highlight_rects,
            )
        except Exception:
            # 高亮渲染失败时回退到无高亮预览
            payload = _preview_payload_from_source(
                file_bytes=file_bytes,
                source_kind=source_kind,
                page=page,
                highlight_phrases=[],
                highlight_bbox=None,
                highlight_rects=[],
            )
            payload["highlight_applied"] = False
            payload["highlight_fallback"] = True
        payload["document_identifier"] = identifier_id
        payload["file_name"] = str(document.get("file_name") or "")
        payload["source_url"] = f"/api/postgresql/documents/{identifier_id}/source"
        _preview_cache_set(document, page, payload, preview_variant)
        return JSONResponse(payload)
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    except PsycopgError as exc:
        raise HTTPException(status_code=500, detail=f"database error: {exc}") from exc    
