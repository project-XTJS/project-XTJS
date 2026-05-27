# -*- coding: utf-8 -*-
"""
商务标查重范围提取（分项报价+偏离表）
"""
import re
from typing import Any

from app.service.analysis.itemized import ItemizedPricingChecker
from app.service.analysis.deviation import DeviationChecker
from app.service.analysis.location_utils import normalize_bbox

from .text_utils import (
    normalize_plain_text,
    compact_raw_text,
    hash_text,
    similarity_ratio,
)
from .constants import (
    SPLIT_LINE_PATTERN,
    COMMON_DUPLICATE_HEADER_TOKENS,
    COMMON_DUPLICATE_REQUIREMENT_TOKENS,
    DEVIATION_RESPONSE_TOKENS,
    COMMON_DUPLICATE_TEMPLATE_PATTERNS,
)

GENERIC_DEVIATION_RESPONSE_TOKENS = (
    "与招标文件条款相同",
    "与采购文件条款相同",
    "与招标文件一致",
    "与采购文件一致",
    "全部响应",
    "全部满足",
    "全部符合",
    "全部无偏离",
    "所有响应",
    "所有满足",
    "所有符合",
    "所有无偏离",
    "无偏离",
    "未偏离",
    "没有偏离",
    "完全响应",
    "完全满足",
    "响应",
    "满足",
    "符合",
    "详见",
)

DEVIATION_STATUS_ONLY_TOKENS = (
    "无偏离",
    "未偏离",
    "正偏离",
    "负偏离",
    "响应",
    "认可",
    "符合",
    "满足",
    "相同",
    "一致",
)

GENERIC_DUPLICATE_COMPARE_TOKENS = (
    "我方",
    "我司",
    "我单位",
    "我公司",
    "本公司",
    "本单位",
    "投标人",
    "投标文件的响应",
    "投标响应",
    "响应内容",
    "响应",
    "应答",
    "回复",
    "认可",
    "无偏离",
    "未偏离",
    "正偏离",
    "负偏离",
    "偏离说明",
    "偏离",
    "详见",
)


def _merge_segment_bboxes(values: list[list[float] | None]) -> list[float] | None:
    boxes = [box for box in values if isinstance(box, list) and len(box) >= 4]
    if not boxes:
        return None
    return [
        round(min(float(box[0]) for box in boxes), 2),
        round(min(float(box[1]) for box in boxes), 2),
        round(max(float(box[2]) for box in boxes), 2),
        round(max(float(box[3]) for box in boxes), 2),
    ]


def extract_business_duplicate_segments(
    payload: dict[str, Any],
    itemized_checker: ItemizedPricingChecker,
    deviation_checker: DeviationChecker,
    star_requirement_context: dict[str, Any] | None = None,
    deviation_template_context: dict[str, Any] | None = None,
    itemized_template_context: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """从商务标中提取分项报价和偏离表相关段落作为查重范围。"""
    segments: list[dict[str, Any]] = []

    # 分项报价先按招标模板剔除表头/固定行，再保留剩余可比内容。
    itemized_document = itemized_checker._prepare_document(payload)
    for section in itemized_document.get("item_sections") or []:
        segment = _segment_from_itemized_section(
            section,
            itemized_template_context=itemized_template_context,
        )
        if segment is not None:
            segments.append(segment)

    # 偏离表先按招标模板剔除要求列/模板行，再保留响应内容。
    deviation_payload = deviation_checker._coerce_payload(payload)
    deviation_sections = deviation_checker._extract_bid_deviation_sections(deviation_payload)
    row_segments = _segments_from_deviation_rows(
        deviation_sections,
        deviation_checker=deviation_checker,
        star_requirement_context=star_requirement_context,
        deviation_template_context=deviation_template_context,
    )
    segments.extend(row_segments)

    # 补充未被行覆盖的偏离表章节
    covered_page_keys = {
        tuple(int(page) for page in (segment.get("pages") or []) if isinstance(page, int))
        for segment in row_segments
    }
    for section in _iter_business_bid_deviation_sections(deviation_sections):
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
            deviation_template_context=deviation_template_context,
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
    deviation_template_context: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    """从已解析的偏离行中构建查重段落。"""
    section_pages = {
        int(section.get("page"))
        for section in (deviation_sections.get("business") or []) + (deviation_sections.get("technical") or [])
        if isinstance(section, dict) and isinstance(section.get("page"), int)
    }
    grouped: dict[tuple[str, int], list[dict[str, Any]]] = {}

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
        if _is_requirement_echo_duplicate_row(requirement, response, deviation):
            continue
        star_matched = _matches_star_requirement(
            requirement,
            deviation_checker=deviation_checker,
            star_requirement_context=star_requirement_context,
        )
        template_matched = _matches_tender_template_requirement(
            requirement,
            deviation_checker=deviation_checker,
            deviation_template_context=deviation_template_context,
        )
        if not _is_deviation_duplicate_row(requirement, response, deviation):
            continue
        joined = " | ".join(
            part
            for part in (
                "" if (star_matched or template_matched) else requirement,
                response,
                deviation,
            )
            if part
        ).strip()
        joined = _strip_tender_template_line_content(
            joined,
            deviation_checker=deviation_checker,
            deviation_template_context=deviation_template_context,
        )
        if len(compact_raw_text(joined)) < 6:
            continue

        grouped.setdefault((title, page), []).append(
            {
                "text": joined,
                "bbox": normalize_bbox(row.get("bbox") or row.get("box")),
            }
        )

    segments: list[dict[str, Any]] = []
    for (title, page), items in grouped.items():
        deduped_lines: list[str] = []
        bboxes: list[list[float] | None] = []
        seen = set()
        for item in items:
            line = str(item.get("text") or "")
            key = compact_raw_text(line)
            if not key or key in seen:
                continue
            seen.add(key)
            deduped_lines.append(line)
            bboxes.append(normalize_bbox(item.get("bbox")))
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
                "bbox": _merge_segment_bboxes(bboxes),
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


def _iter_business_bid_deviation_sections(deviation_sections: dict[str, Any]) -> list[dict[str, Any]]:
    """商务标中的商务/技术偏离表都纳入查重候选，由后续规则过滤纯模板和纯回声内容。"""
    sections: list[dict[str, Any]] = []
    seen = set()
    for group_name in ("business", "technical"):
        for section in deviation_sections.get(group_name) or []:
            if not isinstance(section, dict):
                continue
            key = (
                group_name,
                str(section.get("title") or "").strip(),
                int(section.get("page") or 0) if isinstance(section.get("page"), int) else 0,
            )
            if key in seen:
                continue
            seen.add(key)
            sections.append(section)
    return sections


def _is_technical_deviation_title(title: str) -> bool:
    """识别标题是否为技术偏离表。"""
    compact_title = compact_raw_text(title)
    if not compact_title:
        return False
    return "技术" in compact_title and "商务" not in compact_title


def _normalize_duplicate_compare_text(text: str) -> str:
    """对偏离行文本做轻量归一，便于判断是否只是原条款复述。"""
    normalized = normalize_plain_text(text)
    if not normalized:
        return ""
    normalized = re.sub(r"\bP\d{1,4}(?:\s*[-~]\s*P?\d{1,4})?\b", "", normalized, flags=re.IGNORECASE)
    normalized = re.sub(r"第\d+页", "", normalized)
    for token in GENERIC_DUPLICATE_COMPARE_TOKENS:
        normalized = normalized.replace(token, "")
    normalized = re.sub(r"[|｜/:：;；,，.。\-_\s]+", "", normalized)
    return normalized


def _is_deviation_status_only(text: str) -> bool:
    """识别仅由“无偏离/正偏离/认可”等状态词组成的文本。"""
    compact = compact_raw_text(text)
    if not compact:
        return True
    residue = compact
    for token in DEVIATION_STATUS_ONLY_TOKENS:
        compact_token = compact_raw_text(token)
        if compact_token:
            residue = residue.replace(compact_token, "")
    residue = re.sub(r"[|｜/:：;；,，.。\-_\s]+", "", residue)
    return not residue


def _is_generic_deviation_similarity_line(text: str) -> bool:
    """识别只适合精确比对、不适合相似度比对的通用偏离响应行。"""
    return _is_generic_deviation_response_only_text(text)


def _is_requirement_echo_duplicate_row(
    requirement: str,
    response: str,
    deviation: str,
) -> bool:
    """识别仅复述原条款的偏离行，避免“无偏离/认可原条款”成为误报证据。"""
    compact_requirement = compact_raw_text(requirement)
    compact_response = compact_raw_text(response)
    generic_deviation = _is_deviation_status_only(deviation) or _is_generic_deviation_response_only_text(deviation)

    if compact_requirement and not compact_response and generic_deviation:
        return True

    normalized_requirement = _normalize_duplicate_compare_text(requirement)
    normalized_response = _normalize_duplicate_compare_text(response)
    if not normalized_requirement or not normalized_response:
        return False

    if normalized_requirement in normalized_response:
        residue = normalized_response.replace(normalized_requirement, "")
        if len(residue) <= 8 and generic_deviation:
            return True

    if (
        len(normalized_requirement) >= 12
        and similarity_ratio(normalized_requirement, normalized_response) >= 0.92
        and generic_deviation
    ):
        return True
    return False


def _segment_from_itemized_section(
    section: dict[str, Any],
    *,
    itemized_template_context: dict[str, Any] | None,
) -> dict[str, Any] | None:
    """将分项报价区段标准化为查重段落。"""
    lines = _normalize_scope_lines(section.get("lines") or [])
    # 逐行剔除与招标分项报价模板重合的固定内容。
    lines = [
        line
        for line in (
            _strip_itemized_template_line_content(
                line,
                itemized_template_context=itemized_template_context,
            )
            for line in lines
        )
        if compact_raw_text(line)
    ]
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
        "bbox": normalize_bbox(section.get("bbox") or section.get("box")),
        "lines": lines,
    }


def _segment_from_deviation_section(
    section: dict[str, Any],
    *,
    deviation_checker: DeviationChecker,
    star_requirement_context: dict[str, Any] | None,
    deviation_template_context: dict[str, Any] | None,
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
    cleaned_lines: list[str] = []
    for line in lines:
        # 先剔★要求，再剔招标偏离表模板，最后只保留真正可比的响应文本。
        line = _strip_star_requirement_content(
            line,
            deviation_checker=deviation_checker,
            star_requirement_context=star_requirement_context,
        )
        line = _strip_tender_template_line_content(
            line,
            deviation_checker=deviation_checker,
            deviation_template_context=deviation_template_context,
        )
        if _is_common_duplicate_scope_line(line):
            continue
        if not compact_raw_text(line):
            continue
        cleaned_lines.append(line)
    lines = cleaned_lines
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
    bboxes = [normalize_bbox(section.get("bbox") or section.get("box"))]
    if isinstance(line_items, list):
        bboxes.extend(
            normalize_bbox(item.get("bbox") or item.get("box"))
            for item in line_items
            if isinstance(item, dict)
        )
    return {
        "title": title,
        "pages": pages or [1],
        "kind": "table",
        "source": "deviation_table",
        "preserve_common_lines": True,
        "bbox": _merge_segment_bboxes(bboxes),
        "lines": lines,
    }


def _strip_itemized_template_line_content(
    line: str,
    *,
    itemized_template_context: dict[str, Any] | None,
) -> str:
    """根据招标文件分项报价表模板剔除投标文件里的模板行。"""
    text = normalize_plain_text(line)
    if not text:
        return ""
    if itemized_template_context is None:
        return text
    if _matches_itemized_template_line(
        text,
        itemized_template_context=itemized_template_context,
    ):
        return ""
    return text


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


def _strip_tender_template_line_content(
    line: str,
    *,
    deviation_checker: DeviationChecker,
    deviation_template_context: dict[str, Any] | None,
) -> str:
    """根据招标文件偏离表模板剔除投标文件行里的模板内容。"""
    text = normalize_plain_text(line)
    if not text:
        return ""
    if _matches_tender_template_line(
        text,
        deviation_checker=deviation_checker,
        deviation_template_context=deviation_template_context,
    ):
        return ""

    parts = [
        normalize_plain_text(part)
        for part in re.split(r"\s*[|｜]\s*", text)
        if normalize_plain_text(part)
    ]
    if len(parts) < 2:
        return text

    # 管道分隔的偏离表行按列清洗，只保留非模板列。
    kept_parts = [
        part
        for part in parts
        if not _matches_tender_template_line(
            part,
            deviation_checker=deviation_checker,
            deviation_template_context=deviation_template_context,
        )
        and not _matches_tender_template_requirement(
            part,
            deviation_checker=deviation_checker,
            deviation_template_context=deviation_template_context,
        )
    ]
    if not kept_parts:
        return ""
    if len(kept_parts) == len(parts):
        return text
    return " | ".join(kept_parts)


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


def _matches_tender_template_requirement(
    requirement: str,
    *,
    deviation_checker: DeviationChecker,
    deviation_template_context: dict[str, Any] | None,
) -> bool:
    """判断偏离表要求列是否命中招标文件里的模板要求项。"""
    items = list((deviation_template_context or {}).get("requirement_items") or [])
    if not items:
        return False

    normalized_requirement = _normalize_requirement_for_star_match(
        requirement,
        deviation_checker=deviation_checker,
    )
    if len(normalized_requirement) < 4:
        return False

    for item in items:
        template_norm = str(item.get("normalized_requirement") or "").strip()
        if not template_norm:
            continue
        if normalized_requirement == template_norm:
            return True
        if len(normalized_requirement) >= 8 and (
            normalized_requirement in template_norm or template_norm in normalized_requirement
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


def _matches_tender_template_line(
    text: str,
    *,
    deviation_checker: DeviationChecker,
    deviation_template_context: dict[str, Any] | None,
) -> bool:
    """判断整行文本是否属于招标文件偏离表模板。"""
    if deviation_template_context is None:
        return False

    normalized_text = normalize_plain_text(text)
    compact_text = compact_raw_text(normalized_text)
    if not compact_text:
        return False
    if compact_text in set(deviation_template_context.get("line_keys") or []):
        return True

    normalized_line = deviation_checker._norm(normalized_text)
    for item in deviation_template_context.get("line_items") or []:
        template_compact = str(item.get("compact") or "").strip()
        template_norm = str(item.get("normalized") or "").strip()
        if not template_compact or not template_norm:
            continue
        if compact_text == template_compact or normalized_line == template_norm:
            return True
        if len(normalized_line) >= 8 and (
            normalized_line in template_norm or template_norm in normalized_line
        ):
            return True
    return False


def _matches_itemized_template_line(
    text: str,
    *,
    itemized_template_context: dict[str, Any] | None,
) -> bool:
    """判断分项报价表行是否命中招标文件里的模板行。"""
    if itemized_template_context is None:
        return False
    normalized_text = normalize_plain_text(text)
    compact_text = compact_raw_text(normalized_text)
    if not compact_text:
        return False
    if compact_text in set(itemized_template_context.get("line_keys") or []):
        return True
    for item in itemized_template_context.get("line_items") or []:
        template_compact = str(item.get("compact") or "").strip()
        if not template_compact:
            continue
        if compact_text == template_compact:
            return True
        if len(compact_text) >= 8 and (
            compact_text in template_compact or template_compact in compact_text
        ):
            return True
    return False


def _is_generic_deviation_response_only_text(text: str) -> bool:
    """识别仅由通用响应话术组成的空洞偏离表文本。"""
    compact = compact_raw_text(text)
    if not compact:
        return True

    normalized = compact
    has_generic_token = False
    for token in GENERIC_DEVIATION_RESPONSE_TOKENS:
        compact_token = compact_raw_text(token)
        if compact_token and compact_token in normalized:
            normalized = normalized.replace(compact_token, "")
            has_generic_token = True

    normalized = re.sub(r"(?:^|[^a-z])p\d+(?:[-~]p?\d+)?", "", normalized, flags=re.IGNORECASE)
    normalized = re.sub(r"第\d+页", "", normalized)
    normalized = re.sub(r"\d{1,4}", "", normalized)
    normalized = re.sub(r"[|｜/:：;；,，.。\-_\s]+", "", normalized)
    return has_generic_token and not normalized


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
    deduped_by_key: dict[str, dict[str, Any]] = {}
    order: list[str] = []
    for segment in segments:
        joined = "\n".join(segment.get("lines") or [])
        key = compact_raw_text(
            f"{segment.get('source') or ''}\n{segment.get('title') or ''}\n{joined}"
        )
        if not key:
            continue
        existing = deduped_by_key.get(key)
        if existing is None:
            deduped_by_key[key] = segment
            order.append(key)
            continue
        if not existing.get("bbox") and segment.get("bbox"):
            deduped_by_key[key] = segment
    return [deduped_by_key[key] for key in order]


def _scoped_segment_sort_key(segment: dict[str, Any]) -> tuple[int, int, str]:
    """定义查重段落的排序键。"""
    pages = [page for page in (segment.get("pages") or []) if isinstance(page, int)]
    first_page = min(pages) if pages else 1
    source = str(segment.get("source") or "")
    source_rank = 0 if source == "itemized_pricing" else 1
    title = str(segment.get("title") or "")
    return (first_page, source_rank, title)
