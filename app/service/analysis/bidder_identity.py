"""Resolve bidder identity from explicit first-OCR fields.

The cover page remains authoritative.  Some response documents start with a
table of contents, though, so a missing cover field may be recovered from the
same complete organization value repeated in explicit signing fields on at
least two physical pages.  Seal detector text is never used as identity.
"""
import re
from difflib import SequenceMatcher
from html import unescape

RULE_VERSION = "explicit-bidder-fields-v3"
ANCHORS = (
    "投标人",
    "投标单位",
    "单位名称",
    "参选人",
    "供应商",
    "供应商名称",
    "供应商全称",
    "响应单位",
    "磋商响应单位",
)
FIELD_LABEL = "|".join(sorted((re.escape(item) for item in ANCHORS), key=len, reverse=True))
# OCR may omit the colon after an explicit ``供应商（加盖公章）`` field.  The
# parenthesised seal hint still gives a bounded field label; a bare label without
# either the hint or a colon remains ineligible.
FIELD = re.compile(
    rf"^(?P<label>{FIELD_LABEL})(?:(?P<seal_hint>[（(]\s*(?:名称\s*)?(?:加\s*盖\s*)?公\s*章\s*[）)])\s*[：:]?|\s*[：:])"
)
ORG_SUFFIX = r"(?:有限责任公司|股份有限公司|集团有限公司|有限公司|公司|报社|事务所|研究院|研究所|大学|学校|中学|中心|合作社|协会|委员会|银行)"
ORG = re.compile(r"^[A-Za-z0-9\u4e00-\u9fff（）()·&.\-]{2,100}?" + ORG_SUFFIX)
GENERIC = {"餐饮管理", "餐饮", "科技", "智能科技", "实业发展", "实业", "电子商务", "商贸", "贸易", "服务", "上海", "深圳"}
PUBLIC_SEAL = re.compile(r"[（(]\s*(?:名称\s*)?(?:加\s*盖\s*)?公\s*章\s*[）)]")
NEXT_FIELD = re.compile(r"(?:联系人|联系电话|电话|地址|日期|法定代表人|授权代表|项目名称|项目编号|签字|签章|盖章|投标人|投标单位|单位名称|参选人|供应商(?:名称|全称)?|(?:磋商)?响应单位)\s*[：:]")


def comparison_key(text):
    return re.sub(r"[^0-9A-Za-z\u4e00-\u9fff]", "", str(text or ""))


def name_value(text):
    """Read a bounded organization value; also used for exact seal comparison."""
    text = re.sub(r"\s+", "", PUBLIC_SEAL.sub("", str(text or ""))).strip("：:|；;,，")
    text = FIELD.sub("", text, count=1)
    text = NEXT_FIELD.split(text, maxsplit=1)[0].split("|", 1)[0]
    match = ORG.match(text)
    if not match:
        return None
    value = match.group(0)
    branch = re.match(r"^[A-Za-z0-9\u4e00-\u9fff]{0,24}?(?:分公司|分行|支行|营业部|分院|分所)", text[match.end():])
    if branch:
        value += branch.group(0)
    if value.count("(") != value.count(")") or value.count("（") != value.count("）"):
        return None
    if any(word in value for word in ("填写", "名称", "示例", "附件", "目录", "法定代表人", "注册资本", "登记机关", "统一社会信用代码", "盖章", "公章")):
        return None
    return value


def sufficient_name(text):
    value = name_value(text)
    if not value:
        return False
    stem = re.sub(ORG_SUFFIX + r"$", "", comparison_key(value))
    return len(stem) >= 2 and stem not in GENERIC


def company_score(name, seal):
    candidate = name_value(seal)
    left, right = comparison_key(name), comparison_key(candidate or seal)
    if not left or not right:
        return 0.0
    if left == right and sufficient_name(name) and sufficient_name(candidate):
        return 1.0
    return min(SequenceMatcher(None, left, right).ratio(), 0.999)


def _field_lines(section):
    lines = section.get("lines")
    if isinstance(lines, list) and lines:
        return [(str(x.get("text") or ""), x.get("bbox")) if isinstance(x, dict) else (str(x), None) for x in lines]
    text = re.sub(r"</(?:tr|p|div)>", "\n", str(section.get("text") or ""), flags=re.I)
    text = re.sub(r"</t[dh]>", " | ", text, flags=re.I)
    text = unescape(re.sub(r"<[^>]*>", "", text))
    return [(line.strip(), None) for line in text.splitlines()]


def _result(candidates, reason=None, resolved_reason="explicit_homepage_field", **extra):
    valid = [c for c in candidates if sufficient_name(c["name"])]
    keys = {comparison_key(c["name"]) for c in valid}
    resolved = len(keys) == 1 and reason is None
    return {"rule_version": RULE_VERSION, "status": "resolved" if resolved else "pending",
            "name": valid[0]["name"] if resolved else None,
            "reason": reason or (resolved_reason if resolved else "conflicting_bidder_fields" if len(keys) > 1 else "homepage_field_not_found"),
            "candidates": candidates, **extra}


def _field_candidates(sections, *, require_seal_hint=False, require_box=False):
    candidates = []
    for index, section in enumerate(sections):
        lines = _field_lines(section)
        for position, (line, box) in enumerate(lines):
            compact = re.sub(r"\s+", "", line)
            # Tables preserve cell boundaries; the value may be in the next cell.
            cells = compact.split("|")
            for cell_index, cell in enumerate(cells):
                match = FIELD.match(cell)
                if not match:
                    continue
                if require_seal_hint and not match.group("seal_hint"):
                    continue
                raw = cell[match.end():]
                if not raw and cell_index + 1 < len(cells):
                    raw = cells[cell_index + 1]
                value = name_value(raw)
                if not value and not NEXT_FIELD.search(raw):
                    joined = raw
                    # Only consecutive lines within this OCR block, before a blank/field.
                    for continuation, _ in lines[position + 1:position + 4]:
                        extra = re.sub(r"\s+", "", continuation)
                        if not extra or ":" in extra or "：" in extra or "|" in extra or re.search(r"[。；;]|20\d{2}年", extra):
                            break
                        joined += extra
                        value = name_value(joined)
                        if value:
                            break
                if not value and not raw and index + 1 < len(sections):
                    nxt = sections[index + 1]
                    a, b = section.get("bbox"), nxt.get("bbox")
                    if a and b and len(a) == len(b) == 4 and nxt.get("type") == "text":
                        # Verification sections use xywh; require a close aligned field value.
                        aligned = abs(a[0] - b[0]) <= max(a[2], b[2]) and 0 <= b[1] - a[1] <= max(2 * a[3], 36)
                        if aligned and not NEXT_FIELD.search(str(nxt.get("text") or "")):
                            value = name_value(nxt.get("text"))
                if value:
                    evidence_box = box or section.get("bbox")
                    if require_box and not (isinstance(evidence_box, (list, tuple)) and len(evidence_box) == 4):
                        continue
                    candidates.append({
                        "name": value,
                        "page": section.get("page"),
                        "field": match.group("label"),
                        "text": line,
                        "bbox": evidence_box,
                        "source_block_index": section.get("index", index),
                    })
    return candidates


def identify(container, sections):
    """Prefer page 1, then repeated explicit signing fields; never metadata/seals."""
    homepage = [s for s in sections if s.get("page") == 1 and s.get("type") not in {"seal", "signature"}]
    homepage_candidates = _field_candidates(homepage)
    if homepage_candidates:
        return _result(homepage_candidates)

    document_sections = [
        s for s in sections
        if s.get("page") != 1 and s.get("type") not in {"seal", "signature"}
    ]
    document_candidates = _field_candidates(
        document_sections,
        require_seal_hint=True,
        require_box=True,
    )
    pages_by_name = {}
    for candidate in document_candidates:
        if not sufficient_name(candidate.get("name")):
            continue
        pages_by_name.setdefault(comparison_key(candidate["name"]), set()).add(candidate.get("page"))
    repeated_names = {
        key for key, pages in pages_by_name.items()
        if len({page for page in pages if isinstance(page, int) and page > 0}) >= 2
    }
    repeated_candidates = [
        candidate for candidate in document_candidates
        if comparison_key(candidate.get("name")) in repeated_names
    ]
    ignored_candidates = [
        candidate for candidate in document_candidates
        if comparison_key(candidate.get("name")) not in repeated_names
    ]
    if repeated_candidates:
        return _result(
            repeated_candidates,
            resolved_reason="repeated_explicit_document_fields",
            ignored_candidates=ignored_candidates,
        )
    return _result([], ignored_candidates=ignored_candidates)


def combine(*identities):
    """Business and technical are equal sources; conflicting covers stay pending."""
    candidates = [dict(c, document_role=role) for role, identity in identities for c in identity.get("candidates", [])]
    conflict = any(identity.get("reason") == "conflicting_bidder_fields" for _, identity in identities)
    return _result(
        candidates,
        "conflicting_bidder_fields" if conflict else None,
        resolved_reason="consistent_explicit_bidder_fields",
    )
