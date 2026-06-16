from __future__ import annotations

import re
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
        "商务响应表",
        "合同条款偏离表",
    ],
    "技术条款偏离表": [
        "技术需求偏离表",
        "技术需求偏离表格",
        "技术偏离表",
        "技术响应表",
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
    "被授权人社保缴纳证明": [
        "授权代表社保缴纳证明",
        "授权代表社保缴纳记录",
        "授权代表社会保险缴费记录",
        "授权代表社会保险个人权益记录",
        "被授权人社保缴纳记录",
        "被授权人社会保险缴费记录",
        "被授权人社会保险个人权益记录",
        "社保缴纳证明",
        "社保缴纳记录",
        "社保证明",
        "社保证明材料",
        "社会保险缴纳证明",
        "社会保险缴费证明",
        "社会保险缴费记录",
        "社会保险个人权益记录",
        "个人权益记录",
        "个人参保证明",
        "参保证明",
        "劳动合同证明",
        "劳动合同",
        "劳动合同书",
        "聘用合同",
        "退休证",
    ],
    "法定代表人资格证明书": [
        "法定代表人证明",
        "法定代表人证明书",
        "法定代表人身份证明",
        "法定代表人证明及法定代表人授权委托书",
        "法定代表人/单位负责人证明书及身份证",
        "法定代表人或单位负责人证明书及身份证",
        "单位负责人证明书",
        "单位负责人身份证明",
    ],
    "法定代表人授权委托书": [
        "授权委托书",
        "法人授权委托书",
        "法定代表人/单位负责人授权委托书及被授权人身份证",
        "法定代表人或单位负责人授权委托书及被授权人身份证",
    ],
    "拟派项目经理有效的注册建造师、安全生产考核证书": [
        "拟派项目经理和项目经理有效的注册建造师、安全生产考核证书",
        "拟派项目经理和项目经理有效的注册建造师、安全生产考核证书、社保证明文件",
        "拟派项目经理和项目经理有效的注册建造师、安全生产考核证书、项目负责人的近三个月任一个月的社保证明文件",
        "拟派项目经理和项目经理有效的注册建造师、安全生产考核证书、项目负责人的近三个月任一一个月的社保证明文件",
        "拟派项目经理有效的注册建造师、安全生产考核证书，社保证明材料",
    ],
    "\u9879\u76ee\u7ba1\u7406\u673a\u6784\u4eba\u5458\u60c5\u51b5\u8868": [
        "\u9879\u76ee\u7ba1\u7406\u673a\u6784\u4eba\u5458\u7ec4\u6210\u8868",
    ],
}


PACKAGE_OPTION_TOKEN_RE = (
    r"(?:[\u25a1\u25a0\u2610\u2611\u2713\u2714]?\s*"
    r"\u5305\u4ef6(?:[\u4e00-\u9fff]+|\d+|[A-Za-z]+))"
)
PACKAGE_OPTION_GROUP_RE = re.compile(
    r"[\(\uFF08\[\u3010]\s*"
    + PACKAGE_OPTION_TOKEN_RE
    + r"(?:\s*(?:/|\uFF0F|\u3001|,|\uFF0C)\s*"
    + PACKAGE_OPTION_TOKEN_RE
    + r")*\s*[\)\uFF09\]\u3011]"
)


def strip_attachment_title_parenthetical_noise(title: str) -> str:
    """Remove common package-option suffixes like （包件一/包件二） from titles."""
    value = str(title or "").strip()
    previous = None
    while value != previous:
        previous = value
        value = PACKAGE_OPTION_GROUP_RE.sub("", value)
        value = re.sub(r"\s+", " ", value).strip()
    return value


def normalize_attachment_title_token(text: str) -> str:
    text = strip_attachment_title_parenthetical_noise(text)
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
    cleaned = strip_attachment_title_parenthetical_noise(title)
    normalized = normalize_attachment_title_token(cleaned)
    if not normalized:
        return [cleaned]
    direct = ATTACHMENT_TITLE_GROUP_INDEX.get(normalized)
    if direct:
        return direct
    for key, group in ATTACHMENT_TITLE_GROUP_INDEX.items():
        if _matches_alias(normalized, key):
            return group
    return [cleaned]


def canonicalize_attachment_title(title: str) -> str:
    cleaned = strip_attachment_title_parenthetical_noise(title)
    group = attachment_title_group(cleaned)
    return group[0] if group else cleaned


def attachment_title_variants(title: str) -> list[str]:
    return attachment_title_group(strip_attachment_title_parenthetical_noise(title))


def all_attachment_synonym_titles() -> Iterable[str]:
    for canonical, aliases in ATTACHMENT_TITLE_SYNONYMS.items():  
        yield canonical
        yield from aliases
