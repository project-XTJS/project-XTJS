# -*- coding: utf-8 -*-
"""
招标模板排除（利用招标文件哈希集和占位符屏蔽固定内容）
"""
import re
from typing import Any

from .text_utils import normalize_plain_text, compact_raw_text


def exclude_template_content(
    ordered_blocks: list[dict[str, Any]],
    table_entries: list[dict[str, Any]],
    *,
    template_context: dict[str, Any] | None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """根据招标模板上下文移除重复的固定文案区块和表格。"""
    if template_context is None:
        return ordered_blocks, table_entries

    block_hashes = set(template_context.get("block_hashes") or [])
    table_hashes = set(template_context.get("table_hashes") or [])
    placeholder_patterns = list(template_context.get("placeholder_patterns") or [])

    filtered_blocks = [
        block
        for block in ordered_blocks
        if str(block.get("exact_hash") or "") not in block_hashes
        and not _matches_template_placeholder(block, placeholder_patterns)
    ]
    filtered_tables = [
        table
        for table in table_entries
        if str(table.get("exact_hash") or "") not in table_hashes
    ]
    return filtered_blocks, filtered_tables


def _build_template_placeholder_patterns(
    blocks: list[dict[str, Any]],
) -> list[re.Pattern[str]]:
    """从招标文本中生成带占位符的正则模式，用于识别模板化语句。"""
    patterns: list[re.Pattern[str]] = []
    seen = set()
    for block in blocks:
        pattern = _template_placeholder_pattern(str(block.get("text") or ""))
        if pattern is None:
            continue
        key = pattern.pattern
        if key in seen:
            continue
        seen.add(key)
        patterns.append(pattern)
    return patterns


def _template_placeholder_pattern(text: str) -> re.Pattern[str] | None:
    """为包含下划线、省略号、格式占位符的文本生成正则模式。"""
    normalized = normalize_plain_text(text)
    changed = False
    format_tokens = re.findall(r"[（(][^）)]{0,80}格式[^）)]*[）)]", normalized)
    pattern_source = normalized
    for index, token in enumerate(format_tokens):
        pattern_source = pattern_source.replace(token, f"__FMT_TOKEN_{index}__", 1)
    escaped = re.escape(pattern_source)

    if re.search(r"_{2,}|…{2,}|\.{3,}", normalized):
        escaped = re.sub(r"_{2,}", ".+?", escaped)
        escaped = re.sub(r"…{2,}", ".+?", escaped)
        escaped = re.sub(r"(?:\\\.){3,}", ".+?", escaped)
        changed = True

    for index, token in enumerate(format_tokens):
        escaped = escaped.replace(re.escape(f"__FMT_TOKEN_{index}__"), rf"(?:{re.escape(token)})?")
        changed = True

    if not changed:
        return None

    escaped = escaped.replace(r"\ ", r"\s*")
    return re.compile(rf"^{escaped}$", re.IGNORECASE)


def _matches_template_placeholder(
    block: dict[str, Any],
    patterns: list[re.Pattern[str]],
) -> bool:
    """判断文本块是否完全匹配模板占位符模式。"""
    if not patterns:
        return False
    text = normalize_plain_text(block.get("text") or "")
    return any(pattern.fullmatch(text) for pattern in patterns)