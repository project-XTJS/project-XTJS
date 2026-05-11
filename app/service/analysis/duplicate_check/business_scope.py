# -*- coding: utf-8 -*-
"""
商务标查重范围提取（分项报价+偏离表）
"""
import re
from typing import Any

from app.service.analysis.itemized import ItemizedPricingChecker
from app.service.analysis.deviation import DeviationChecker

from .text_utils import (
    normalize_plain_text,
    compact_raw_text,
    hash_text,
)
from .constants import (
    SPLIT_LINE_PATTERN,
    COMMON_DUPLICATE_HEADER_TOKENS,
    COMMON_DUPLICATE_REQUIREMENT_TOKENS,
    DEVIATION_RESPONSE_TOKENS,
    COMMON_DUPLICATE_TEMPLATE_PATTERNS,
)


def extract_business_duplicate_segments(
    payload: dict[str, Any],
    itemized_checker: ItemizedPricingChecker,
    deviation_checker: DeviationChecker,
    star_requirement_context: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """从商务标中提取分项报价和偏离表相关段落作为查重范围。"""
    segments: list[dict[str, Any]] = []

    # 分项报价部分
    itemized_document = itemized_checker._prepare_document(payload)
    for section in itemized_document.get("item_sections") or []:
        segment = _segment_from_itemized_section(section)
        if segment is not None:
            segments.append(segment)

    # 偏离表部分
    deviation_payload = deviation_checker._coerce_payload(payload)
    deviation_sections = deviation_checker._extract_bid_deviation_sections(deviation_payload)
    row_segments = _segments_from_deviation_rows(
        deviation_sections,
        deviation_checker=deviation_checker,
        star_requirement_context=star_requirement_context,
    )
    segments.extend(row_segments)

    # 补充未被行覆盖的偏离表章节
    covered_page_keys = {
        tuple(int(page) for page in (segment.get("pages") or []) if isinstance(page, int))
        for segment in row_segments
    }
    for section in (deviation_sections.get("business") or []) + (deviation_sections.get("technical") or []):
        section_pages = tuple(
            int(page)
            for page in ([section.get("page")] if isinstance(section.get("page"), int) else [])
            if isinstance(page, int)
        )
        if section_pages and section_pages in covered_page_keys:
            continue
        segment = _segment_from_deviation_section(
            section,
            deviation_checker=deviation_checker,
            star_requirement_context=star_requirement_context,
        )
        if segment is not None:
            segments.append(segment)

    deduped = _dedupe_scoped_segments(segments)
    deduped.sort(key=_scoped_segment_sort_key)
    return deduped


def _segments_from_deviation_rows(
    deviation_sections: dict[str, Any],
    *,
    deviation_checker: DeviationChecker,
    star_requirement_context: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    """从已解析的偏离行中构建查重段落。"""
    section_pages = {
        int(section.get("page"))
        for section in (deviation_sections.get("business") or []) + (deviation_sections.get("technical") or [])
        if isinstance(section, dict) and isinstance(section.get("page"), int)
    }
    grouped: dict[tuple[str, int], list[str]] = {}

    for row in deviation_sections.get("rows") or []:
        if not isinstance(row, dict):
            continue
        page = row.get("page")
        if not isinstance(page, int):
            continue
        title = str(row.get("title") or "").strip() or "偏离表"
        if "偏离" not in title and page not in section_pages:
            continue

        requirement = normalize_plain_text(row.get("requirement_text") or "")
        response = normalize_plain_text(row.get("response_text") or "")
        deviation = normalize_plain_text(row.get("deviation_text") or "")
        star_matched = _matches_star_requirement(
            requirement,
            deviation_checker=deviation_checker,
            star_requirement_context=star_requirement_context,
        )
        if not _is_deviation_duplicate_row(requirement, response, deviation):
            continue
        joined = " | ".join(
            part
            for part in (
                "" if star_matched else requirement,
                response,
                deviation,
            )
            if part
        ).strip()
        if len(compact_raw_text(joined)) < 6:
            continue

        grouped.setdefault((title, page), []).append(joined)

    segments: list[dict[str, Any]] = []
    for (title, page), lines in grouped.items():
        deduped_lines: list[str] = []
        seen = set()
        for line in lines:
            key = compact_raw_text(line)
            if not key or key in seen:
                continue
            seen.add(key)
            deduped_lines.append(line)
        if not deduped_lines:
            continue
        segments.append(
            {
                "title": title,
                "pages": [page],
                "kind": "table",
                "source": "deviation_table",
                "preserve_common_lines": True,
                "lines": deduped_lines,
            }
        )
    return segments


def _is_deviation_duplicate_row(
    requirement: str,
    response: str,
    deviation: str,
) -> bool:
    """判断偏离行是否具有重复检查意义。"""
    compact_requirement = compact_raw_text(requirement)
    compact_response = compact_raw_text(response)
    compact_deviation = compact_raw_text(deviation)
    joined = f"{compact_requirement}{compact_response}{compact_deviation}"
    if not joined:
        return False

    header_hits = sum(1 for token in COMMON_DUPLICATE_HEADER_TOKENS if token in joined)
    if header_hits >= 4:
        return False

    if compact_requirement and compact_requirement == compact_response and len(compact_requirement) >= 12:
        return False

    if compact_deviation:
        return True

    if not compact_response:
        return False

    if any(token in compact_response for token in ("响应", "相同", "满足", "符合", "偏离", "详见")):
        return True

    return False


def _segment_from_itemized_section(section: dict[str, Any]) -> dict[str, Any] | None:
    """将分项报价区段标准化为查重段落。"""
    lines = _normalize_scope_lines(section.get("lines") or [])
    if not lines:
        return None

    raw_pages = section.get("pages")
    pages = [page for page in raw_pages if isinstance(page, int)] if isinstance(raw_pages, list) else []
    if not pages and isinstance(section.get("page"), int):
        pages = [int(section["page"])]

    return {
        "title": str(section.get("anchor") or "分项报价表").strip() or "分项报价表",
        "pages": pages or [1],
        "kind": "table",
        "source": "itemized_pricing",
        "lines": lines,
    }


def _segment_from_deviation_section(
    section: dict[str, Any],
    *,
    deviation_checker: DeviationChecker,
    star_requirement_context: dict[str, Any] | None,
) -> dict[str, Any] | None:
    """将偏离表区段标准化为查重段落。"""
    raw_lines = section.get("lines")
    if not isinstance(raw_lines, list) or not raw_lines:
        raw_lines = SPLIT_LINE_PATTERN.split(str(section.get("text") or ""))
    lines = _normalize_scope_lines(
        _trim_deviation_section_lines(raw_lines),
        preserve_common_lines=True,
    )
    lines = [line for line in lines if _is_deviation_response_line(line)]
    lines = [
        _strip_star_requirement_content(
            line,
            deviation_checker=deviation_checker,
            star_requirement_context=star_requirement_context,
        )
        for line in lines
    ]
    lines = [line for line in lines if compact_raw_text(line)]
    if not lines:
        return None

    pages: list[int] = []
    line_items = section.get("line_items")
    if isinstance(line_items, list):
        for item in line_items:
            if isinstance(item, dict) and isinstance(item.get("page"), int):
                page = int(item["page"])
                if page not in pages:
                    pages.append(page)
    if not pages and isinstance(section.get("page"), int):
        pages.append(int(section["page"]))

    title = str(section.get("title") or "").strip() or "偏离表"
    return {
        "title": title,
        "pages": pages or [1],
        "kind": "table",
        "source": "deviation_table",
        "preserve_common_lines": True,
        "lines": lines,
    }


def _strip_star_requirement_content(
    line: str,
    *,
    deviation_checker: DeviationChecker,
    star_requirement_context: dict[str, Any] | None,
) -> str:
    """对偏离表原始行做轻量删减：命中招标★要求时，尽量只保留响应/偏离侧文本。"""
    text = normalize_plain_text(line)
    if not text:
        return ""
    if not _matches_star_requirement(
        text,
        deviation_checker=deviation_checker,
        star_requirement_context=star_requirement_context,
    ):
        return text

    for token in ("投标文件的响应", "投标响应", "响应内容", "响应", "应答", "回复", "偏离说明", "偏离"):
        index = text.find(token)
        if index >= 0:
            trimmed = normalize_plain_text(text[index:])
            if compact_raw_text(trimmed):
                return trimmed
    return ""


def _trim_deviation_section_lines(values: list[Any]) -> list[str]:
    """截断偏离表章节中超出边界的行。"""
    trimmed: list[str] = []
    for raw_value in values:
        text = normalize_plain_text(raw_value)
        if not text:
            continue
        if trimmed and _is_deviation_scope_boundary(text):
            break
        trimmed.append(text)
    return trimmed


def _is_deviation_scope_boundary(text: str) -> bool:
    """识别是否到达偏离表范围的边界。"""
    compact = compact_raw_text(text)
    if not compact:
        return False
    if "偏离" in compact:
        return False
    if re.match(r"^(附件|附表|附录)\s*[0-9一二三四五六七八九十]+", text):
        return True
    return any(
        token in compact
        for token in (
            "基本情况表",
            "资格证明",
            "资信证明",
            "业绩证明",
            "类似项目",
            "开标一览表",
            "报价一览表",
        )
    )


def _normalize_scope_lines(
    values: list[Any],
    *,
    preserve_common_lines: bool = False,
) -> list[str]:
    """对范围内的文本行进行规范化并去重。"""
    normalized: list[str] = []
    seen = set()
    for value in values:
        text = normalize_plain_text(value)
        if not text:
            continue
        text = _strip_scope_serial_prefix(text)
        if not text:
            continue
        if not preserve_common_lines and _is_common_duplicate_scope_line(text):
            continue
        key = compact_raw_text(text)
        if not key or key in seen:
            continue
        seen.add(key)
        normalized.append(text)
    return normalized


def _matches_star_requirement(
    requirement: str,
    *,
    deviation_checker: DeviationChecker,
    star_requirement_context: dict[str, Any] | None,
) -> bool:
    """判断偏离表要求列是否命中招标文件中提取出的★强制要求。"""
    items = list((star_requirement_context or {}).get("items") or [])
    if not items:
        return False

    normalized_requirement = _normalize_requirement_for_star_match(
        requirement,
        deviation_checker=deviation_checker,
    )
    if len(normalized_requirement) < 4:
        return False

    for item in items:
        star_norm = str(item.get("normalized_requirement") or "").strip()
        if not star_norm:
            continue
        if normalized_requirement == star_norm:
            return True
        if len(normalized_requirement) >= 8 and (
            normalized_requirement in star_norm or star_norm in normalized_requirement
        ):
            return True

        fragment_hits = 0
        for fragment in item.get("fragments") or []:
            fragment_key = str(fragment or "").strip()
            if fragment_key and fragment_key in normalized_requirement:
                fragment_hits += 1
        if fragment_hits >= 2:
            return True
    return False


def _normalize_requirement_for_star_match(
    requirement: str,
    *,
    deviation_checker: DeviationChecker,
) -> str:
    """复用偏离检查器的要求清洗与归一化逻辑，保持★要求匹配口径一致。"""
    cleaned = deviation_checker._clean_req(requirement)
    return deviation_checker._norm(cleaned)


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


def _is_common_duplicate_scope_line(text: str) -> bool:
    """判断是否为应忽略的公共模板行（如表头、固定提示语等）。"""
    compact = compact_raw_text(text)
    if not compact:
        return True

    for pattern in COMMON_DUPLICATE_TEMPLATE_PATTERNS:
        if pattern.search(text) or pattern.search(compact):
            return True

    token_hits = sum(1 for token in COMMON_DUPLICATE_HEADER_TOKENS if token in compact)
    if compact in {
        "投标文件的响应情况",
        "投标文件的响应",
        "响应情况",
        "偏离说明",
        "对应材料投标文件所在页",
    }:
        return True
    if "序号" in compact and token_hits >= 4:
        return True
    if token_hits >= 5 and len(compact) <= 80:
        return True
    if compact.endswith("偏离表") and len(compact) <= 30:
        return True

    if "无偏离" in compact and ("与招标文件" in compact or "与采购文件" in compact):
        return True
    if "与招标文件条款相同" in compact or "与采购文件条款相同" in compact:
        return True

    if any(token in compact for token in COMMON_DUPLICATE_REQUIREMENT_TOKENS):
        return True

    if 4 <= len(compact) <= 32 and "项目" in compact:
        return True

    if re.fullmatch(r"[（(]?\d+[）)]?[\u4e00-\u9fa5]{0,8}[;；。]?", compact):
        return True
    return False


def _is_deviation_response_line(text: str) -> bool:
    """判断文本行是否为偏离表中的具体响应行。"""
    compact = compact_raw_text(text)
    if not compact:
        return False
    if any(token in compact for token in DEVIATION_RESPONSE_TOKENS):
        return True
    if re.search(r"(?:^|[^A-Za-z])P\d+", compact, re.IGNORECASE):
        return True
    if re.search(r"第\d+页", compact):
        return True
    return False


def _dedupe_scoped_segments(segments: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """对查重段落进行去重。"""
    deduped: list[dict[str, Any]] = []
    seen = set()
    for segment in segments:
        joined = "\n".join(segment.get("lines") or [])
        key = compact_raw_text(
            f"{segment.get('source') or ''}\n{segment.get('title') or ''}\n{joined}"
        )
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(segment)
    return deduped


def _scoped_segment_sort_key(segment: dict[str, Any]) -> tuple[int, int, str]:
    """定义查重段落的排序键。"""
    pages = [page for page in (segment.get("pages") or []) if isinstance(page, int)]
    first_page = min(pages) if pages else 1
    source = str(segment.get("source") or "")
    source_rank = 0 if source == "itemized_pricing" else 1
    title = str(segment.get("title") or "")
    return (first_page, source_rank, title)
