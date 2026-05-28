# -*- coding: utf-8 -*-
"""
文本规范化与实用工具（纯函数，无状态）
"""
import hashlib
import html
import re
from difflib import SequenceMatcher
from typing import Any

from .constants import (
    SPLIT_LINE_PATTERN,
    SENTENCE_BOUNDARY_PATTERN,
    MIN_SENTENCE_COMPACT_LENGTH,
    BUSINESS_SIMILARITY_MIN_KEY_LENGTH,
    COMMON_DUPLICATE_HEADER_TOKENS,
    COMMON_DUPLICATE_REQUIREMENT_TOKENS,
    COMMON_DUPLICATE_TEMPLATE_PATTERNS,
    PAGE_NUMBER_PATTERN,
)


def normalize_plain_text(value: Any) -> str:
    """基础文本规范化：反转义、统一空格和换行。"""
    text = html.unescape(str(value or ""))
    text = text.replace("\u3000", " ").replace("\xa0", " ")
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t\f\v]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def compact_raw_text(text: str) -> str:
    """将文本中所有空白字符去除，用于精确哈希比较。"""
    normalized = normalize_plain_text(text)
    return re.sub(r"\s+", "", normalized)


def hash_text(text: str) -> str:
    """计算文本的 SHA256 哈希（UTF-8 编码）。"""
    return hashlib.sha256(str(text or "").encode("utf-8")).hexdigest()


def clip(text: str, max_chars: int) -> str:
    """将文本截断到指定长度，超长部分用省略号表示。"""
    if max_chars <= 0:
        return ""
    if len(text) <= max_chars:
        return text
    return f"{text[:max_chars]}..."


def split_sentences(text: str) -> list[str]:
    """按句子边界分割文本并去重。"""
    normalized = normalize_plain_text(text)
    if not normalized:
        return []

    sentences: list[str] = []
    for line in SPLIT_LINE_PATTERN.split(normalized):
        line = line.strip()
        if not line:
            continue

        parts = SENTENCE_BOUNDARY_PATTERN.split(line)
        buffer = ""
        for part in parts:
            fragment = str(part or "").strip()
            if not fragment:
                continue
            buffer = f"{buffer}{fragment}".strip()
            if SENTENCE_BOUNDARY_PATTERN.search(fragment):
                sentences.append(buffer)
                buffer = ""
        if buffer:
            sentences.append(buffer)

    deduped: list[str] = []
    seen = set()
    for sentence in sentences:
        normalized_sentence = normalize_plain_text(sentence)
        if not normalized_sentence:
            continue
        key = compact_raw_text(normalized_sentence)
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(normalized_sentence)
    return deduped


def similarity_ratio(left: str, right: str) -> float:
    """计算两段文本的相似度（0~1）。"""
    if not left or not right:
        return 0.0
    if left == right:
        return 1.0
    return SequenceMatcher(None, left, right).ratio()


def business_similarity_key(value: Any) -> str:
    """将文本转换为适合相似度比较的规范化键（替换页码、数字等）。"""
    text = _strip_scope_serial_prefix(normalize_plain_text(value))
    if not text:
        return ""
    text = re.sub(r"第\s*\d+\s*页", " <PAGE> ", text, flags=re.IGNORECASE)
    text = re.sub(r"(?:P|p)\s*\d+(?:\s*-\s*(?:P|p)?\s*\d+)?", " <PAGE> ", text)
    text = re.sub(r"[¥￥]?\d[\d,，.．]*", " <NUM> ", text)
    text = re.sub(r"[()（）【】\[\]{}<>《》:：;；,，、/\\|]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip().lower()
    text = re.sub(r"(?:<num>\s*){2,}", "<num> ", text)
    text = re.sub(r"(?:<page>\s*){2,}", "<page> ", text)
    return text.strip()


def _strip_scope_serial_prefix(text: str) -> str:
    """移除文本开头的序号前缀。"""
    normalized = normalize_plain_text(text)
    if not normalized:
        return ""
    stripped = re.sub(
        r"^\s*(?:[(（]?\d{1,4}[)）]?[.、:：]?|[一二三四五六七八九十百千]+[、.．])\s+",
        "",
        normalized,
    )
    return stripped.strip()


def is_noise_block(text: str, section_type: str) -> bool:
    """判断文本块是否为噪声（页码、目录、过短等）。"""
    compact = compact_raw_text(text)
    if not compact:
        return True
    if PAGE_NUMBER_PATTERN.fullmatch(compact):
        return True
    # SectionClassifier.RE_TOC 引用留在 block_extractor 中处理
    if section_type != "heading" and len(compact) < 4:
        return True
    return False
