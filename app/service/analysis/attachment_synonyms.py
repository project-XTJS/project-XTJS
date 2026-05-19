from __future__ import annotations

from typing import Iterable


ATTACHMENT_TITLE_SYNONYMS: dict[str, list[str]] = {
    "投标保证书": [
        "投标声明函",
        "投标人声明函",
        "投标人响应声明函",
        "投标响应声明函",
    ],
    "开标一览表": [
        "报价一览表",
    ],
    "分项报价表": [
        "报价明细",
        "报价明细一览表",
        "标价明细",
        "报价分类明细",
    ],
    "商务条款偏离表": [
        "商务条款偏离表格",
        "商务偏离表",
        "合同条款偏离表",
    ],
    "技术条款偏离表": [
        "技术需求偏离表",
        "技术需求偏离表格",
        "技术偏离表",
        "技术应答偏离表",
    ],
    "投标人基本情况表": [
        "投标人基本情况简介",
        "供应商基本情况简介",
        "投标方情况介绍",
        "投标人响应方情况介绍",
    ],
    "类似项目业绩清单": [
        "类似成功案例的业绩证明",
        "类似成功案例业绩",
        "类似项目业绩表",
        "业绩一览表",
    ],
    "投标人承诺声明函": [
        "投标人承诺函",
        "供应商承诺函",
        "供应商承诺声明函",
        "投标方承诺函",
        "投标人响应方承诺函",
        "承诺函",
    ],
    "不参与围标串标承诺书": [
        "不围标串标承诺书",
        "围标串标承诺书",
        "无不良竞争承诺函",
    ],
    "保证金缴纳凭证": [
        "投标保证金凭证",
        "投标保证金截图",
        "投标保证金转账凭证",
        "投标保证金缴纳证明",
        "保证金截图",
    ],
    "财务状况及税收、社会保障资金缴纳情况声明函": [
        "财务状况及税收和社会保障资金缴纳情况声明函",
        "财务状况及税收、社会保障资金缴纳情况、没有重大违法记录声明函",
        "财务状况及税收和社会保障资金缴纳情况、没有重大违法记录声明函",
        "没有重大违法记录声明函",
    ],
}


def normalize_attachment_title_token(text: str) -> str:
    return "".join(
        ch
        for ch in str(text or "").strip()
        if ch.isalnum() or "\u4e00" <= ch <= "\u9fff"
    )


def _matches_alias(target_norm: str, candidate_norm: str) -> bool:
    if not target_norm or not candidate_norm:
        return False
    return (
        target_norm == candidate_norm
        or target_norm in candidate_norm
        or candidate_norm in target_norm
    )


def _dedupe_titles(values: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for item in values:
        value = str(item or "").strip()
        if value and value not in seen:
            seen.add(value)
            result.append(value)
    return result


def _build_group_index() -> dict[str, list[str]]:
    index: dict[str, list[str]] = {}
    for canonical, aliases in ATTACHMENT_TITLE_SYNONYMS.items():
        group = _dedupe_titles([canonical, *aliases])
        normalized_group = [normalize_attachment_title_token(item) for item in group]
        for item, normalized in zip(group, normalized_group):
            if not normalized:
                continue
            index[normalized] = group
            for other in normalized_group:
                if other and _matches_alias(normalized, other):
                    index.setdefault(other, group)
    return index


ATTACHMENT_TITLE_GROUP_INDEX = _build_group_index()


def attachment_title_group(title: str) -> list[str]:
    normalized = normalize_attachment_title_token(title)
    if not normalized:
        return [str(title or "").strip()]
    direct = ATTACHMENT_TITLE_GROUP_INDEX.get(normalized)
    if direct:
        return direct
    for key, group in ATTACHMENT_TITLE_GROUP_INDEX.items():
        if _matches_alias(normalized, key):
            return group
    return [str(title or "").strip()]


def canonicalize_attachment_title(title: str) -> str:
    group = attachment_title_group(title)
    return group[0] if group else str(title or "").strip()


def attachment_title_variants(title: str) -> list[str]:
    return attachment_title_group(title)


def all_attachment_synonym_titles() -> Iterable[str]:
    for canonical, aliases in ATTACHMENT_TITLE_SYNONYMS.items():
        yield canonical
        yield from aliases
