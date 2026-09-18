"""Homepage-only bidder fields. No OCR corrections or identity fallbacks."""
import re
from difflib import SequenceMatcher
from html import unescape

RULE_VERSION = "homepage-fields-v1"
ANCHORS = ("投标人", "投标单位", "单位名称", "参选人")
FIELD = re.compile(r"^(?:" + "|".join(ANCHORS) + r")[：:]")
ORG_SUFFIX = r"(?:有限责任公司|股份有限公司|集团有限公司|有限公司|公司|报社|事务所|研究院|研究所|大学|学校|中学|中心|合作社|协会|委员会|银行)"
ORG = re.compile(r"^[A-Za-z0-9\u4e00-\u9fff（）()·&.\-]{2,100}?" + ORG_SUFFIX)
GENERIC = {"餐饮管理", "餐饮", "科技", "智能科技", "实业发展", "实业", "电子商务", "商贸", "贸易", "服务", "上海", "深圳"}
PUBLIC_SEAL = re.compile(r"[（(]\s*公\s*章\s*[）)]")
NEXT_FIELD = re.compile(r"(?:联系人|联系电话|电话|地址|日期|法定代表人|授权代表|项目名称|项目编号|签字|签章|盖章|投标人|投标单位|单位名称|参选人)\s*[：:]")


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


def _result(candidates, reason=None):
    valid = [c for c in candidates if sufficient_name(c["name"])]
    keys = {comparison_key(c["name"]) for c in valid}
    resolved = len(keys) == 1 and reason is None
    return {"rule_version": RULE_VERSION, "status": "resolved" if resolved else "pending",
            "name": valid[0]["name"] if resolved else None,
            "reason": reason or ("explicit_homepage_field" if resolved else "conflicting_bidder_fields" if len(keys) > 1 else "homepage_field_not_found"),
            "candidates": candidates}


def identify(container, sections):
    """Only page 1 and four explicit colon fields; never inspect metadata."""
    candidates = []
    homepage = [s for s in sections if s.get("page") == 1 and s.get("type") not in {"seal", "signature"}]
    for index, section in enumerate(homepage):
        lines = _field_lines(section)
        for position, (line, box) in enumerate(lines):
            compact = re.sub(r"\s+", "", line)
            # Tables preserve cell boundaries; the value may be in the next cell.
            cells = compact.split("|")
            for cell_index, cell in enumerate(cells):
                match = FIELD.match(cell)
                if not match:
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
                if not value and not raw and index + 1 < len(homepage):
                    nxt = homepage[index + 1]
                    a, b = section.get("bbox"), nxt.get("bbox")
                    if a and b and len(a) == len(b) == 4 and nxt.get("type") == "text":
                        # Verification sections use xywh; require a close aligned field value.
                        aligned = abs(a[0] - b[0]) <= max(a[2], b[2]) and 0 <= b[1] - a[1] <= max(2 * a[3], 36)
                        if aligned and not NEXT_FIELD.search(str(nxt.get("text") or "")):
                            value = name_value(nxt.get("text"))
                if value:
                    candidates.append({"name": value, "page": 1, "field": match.group(0)[:-1], "text": line, "bbox": box or section.get("bbox"), "source_block_index": section.get("index", index)})
    return _result(candidates)


def combine(*identities):
    """Business and technical are equal sources; conflicting covers stay pending."""
    candidates = [dict(c, document_role=role) for role, identity in identities for c in identity.get("candidates", [])]
    conflict = any(identity.get("reason") == "conflicting_bidder_fields" for _, identity in identities)
    return _result(candidates, "conflicting_bidder_fields" if conflict else None)
