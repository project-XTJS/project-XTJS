"""Exact fixed-template comparison primitives.

Similarity is deliberately absent from this module.  Callers may use a fuzzy
score to select a likely paragraph, but a selected paragraph only passes when
all fixed text is identical and all differences are bounded by slots declared
by the tender template.
"""

from __future__ import annotations

from dataclasses import dataclass
import difflib
import re
from typing import Any


LEGACY_VERSION = "fixed-template-exact-v3"
V31_VERSION = "fixed-template-exact-v3.1"
VERSION = "fixed-template-exact-v3.2"
SLOT_TOKEN_RE = re.compile(r"\uFFF0(?P<index>\d+)\uFFF1")
MARKUP_RE = re.compile(r"\\underline\s*\{|<u(?:\s[^>]*)?>", re.I)
LITERAL_BLANK_RE = re.compile(r"[_＿]{2,}|(?:\.{3,}|…{2,}|·{3,})")
BRACKET_SLOT_RE = re.compile(r"[（(【\[]\s*(?P<label>[^（）()【】\[\]\n]{1,40})\s*[）)】\]]")
INLINE_FIELD_RE = re.compile(
    r"[（(]?(?P<label>小写|大写|项目名称|项目编号|投标人名称|参选人名称|供应商名称|"
    r"单位名称(?:[（(](?:盖公章|加盖公章)[）)])?|供应商全称(?:[（(]加盖公章[）)])?|供应商地址|行业类型|"
    r"姓名|职务|地址|金额|日期|电话|手机|邮编|在册人数|成立日期|"
    r"注册资本|实收资本|经营范围|专业人员分类及人数|联系人|电子邮箱|电子邮件|传真|"
    r"其他需要说明的情况|其他情况|法定代表人(?:[（(]?或其授权代表[）)]?|或授权委托人)?"
    r"(?:[（(]?签字或盖章[）)]?)?|授权代表(?:[（(]?签字或盖章[）)]?)?|"
    r"授权委托人(?:[（(]?签字或盖章[）)]?)?|被授权人(?:[（(]?签字或盖章[）)]?)?|"
    r"参选人(?:[（(]?加盖公章[）)]?)?|签字或盖章)[）)]?\s*[:：]"
)

SLOT_HINTS = (
    "请填写", "填写", "项目名称", "项目编号", "招标编号", "采购编号",
    "采购人", "采购代理机构", "招标人", "投标人", "参选人",
    "投标人名称", "参选人名称", "供应商名称", "公司名称", "单位名称", "姓名", "职务",
    "地址", "金额", "报价", "日期", "年月日", "电话", "手机", "身份证号码",
)
TRAILING_FIELD_HINTS = SLOT_HINTS + ("签字", "签名", "盖章", "公章", "年龄", "性别", "邮编")
OBLIGATION_HINTS = ("必须", "不得", "应当", "承诺", "保证", "同意", "遵守", "承担")


@dataclass(frozen=True)
class Slot:
    index: int
    label: str
    source: str
    start: int
    end: int


@dataclass(frozen=True)
class TemplatePattern:
    original_text: str
    display_text: str
    pattern_text: str
    fixed_segments: tuple[str, ...]
    slots: tuple[Slot, ...]
    issues: tuple[str, ...]


def _brace_end(text: str, start: int) -> int | None:
    depth = 0
    for index in range(start, len(text)):
        if index and text[index - 1] == "\\":
            continue
        if text[index] == "{":
            depth += 1
        elif text[index] == "}":
            depth -= 1
            if depth == 0:
                return index + 1
    return None


def _markup_end(text: str, match: re.Match[str]) -> int | None:
    if match.group().startswith("\\"):
        return _brace_end(text, match.end() - 1)
    depth = 1
    for tag in re.finditer(r"</?u(?:\s[^>]*)?>", text[match.end():], re.I):
        depth += -1 if tag.group().startswith("</") else 1
        if depth == 0:
            return match.end() + tag.end()
    return None


def _markup_content(raw: str) -> str:
    if raw.lstrip().startswith("\\underline"):
        opening = raw.find("{")
        value = raw[opening + 1:-1] if opening >= 0 and raw.endswith("}") else raw
    else:
        value = re.sub(r"^<u(?:\s[^>]*)?>|</u>$", "", raw, flags=re.I)
    previous = None
    while value != previous:
        previous = value
        value = re.sub(r"^\s*\\text\s*\{(?P<value>.*)\}\s*$", r"\g<value>", value, flags=re.S)
    return value.replace("\\_", "_").strip()


def is_explicit_slot_content(value: Any) -> bool:
    """Only template-authored blank or filling prompts create a slot.

    Underline emphasis around ordinary prose is fixed content.  In particular,
    adding an underline in a bid cannot create a slot because this function is
    only applied to the tender pattern.
    """
    text = str(value or "").strip()
    compact = re.sub(r"\s+", "", text)
    if not compact or not re.sub(r"[_＿.·…\-—－]", "", compact):
        return True
    if len(compact) > 40 or any(marker in compact for marker in OBLIGATION_HINTS):
        return False
    return any(marker in compact for marker in SLOT_HINTS)


def _layout_normalize(value: Any) -> str:
    """Ignore layout whitespace while retaining a real single-space boundary."""
    text = str(value or "").replace("\r\n", "\n").replace("\r", "\n")
    text = text.replace("\u00a0", " ").replace("\u3000", " ")
    text = re.sub(r"[\t\f\v ]+", " ", text)
    text = re.sub(r" *\n+ *", " ", text)
    text = re.sub(r"^(\s*(?:附件\s*)?\d+(?:[-－]\d+)*(?:[.．、)）]))\s+", r"\1", text)
    text = re.sub(r"^(\s*[（(]\d+(?:[-－]\d+)*[）)])\s+", r"\1", text)
    # PDF text blocks frequently insert visual spacing between CJK glyphs.
    # Preserve spaces between Latin words/numbers, where a space can be text.
    cjk_or_punctuation = r"\u3400-\u9fff0-9，。；：、！？）》】”’/%％"
    previous = None
    while text != previous:
        previous = text
        text = re.sub(rf"(?<=[{cjk_or_punctuation}]) (?=[{cjk_or_punctuation}])", "", text)
    # OCR/PDF layout extraction often emits a visual gap where Chinese text
    # touches an ASCII acronym or number.  It is not a word separator: retain
    # spaces inside an ASCII phrase (for example ``AI PC``), but remove only
    # the CJK/ASCII boundary gap.
    text = re.sub(r"(?<=[\u3400-\u9fff]) +(?=[A-Za-z0-9])", "", text)
    text = re.sub(r"(?<=[A-Za-z0-9]) +(?=[\u3400-\u9fff])", "", text)
    # Spaces adjacent to Chinese punctuation are layout gaps. Preserve spaces
    # between Latin words/numbers, where they can be substantive text.
    text = re.sub(r"(?<=[，。；：、！？（）《》【】“”‘’]) +", "", text)
    text = re.sub(r" +(?=[，。；：、！？（）《》【】“”‘’])", "", text)
    text = re.sub(r"^(\s*[ivxlcdm]+[.．、)）])\s+", r"\1", text, flags=re.I)
    return text.strip()


def plain_text(value: Any) -> str:
    """Remove rendering markup but retain its textual content exactly."""
    text = str(value or "")
    result: list[str] = []
    cursor = 0
    while (match := MARKUP_RE.search(text, cursor)) is not None:
        result.append(text[cursor:match.start()])
        end = _markup_end(text, match)
        if end is None:
            result.append(text[match.start():match.end()])
            cursor = match.end()
            continue
        result.append(_markup_content(text[match.start():end]))
        cursor = end
    result.append(text[cursor:])
    rendered = "".join(result)
    rendered = re.sub(r"\\text\s*\{([^{}]*)\}", r"\1", rendered)
    rendered = rendered.replace("$", "")
    return _layout_normalize(rendered)


def _v32_layout_spacing(text: str) -> str:
    value = str(text or "")
    list_marker = r"(?:[（(]?\d+(?:[-－]\d+)*[）).．、]|[（(][一二三四五六七八九十百零]+[）)])"
    value = re.sub(rf"(?<=[：:；;。！？!?])\s+(?={list_marker})", "", value)
    value = re.sub(rf"({list_marker})\s+(?=[A-Za-z\u3400-\u9fff])", r"\1", value)
    return value


def _plain_text_v32(value: Any) -> str:
    return _v32_layout_spacing(plain_text(value))


def _build_pattern(value: Any, *, preserve_numbered_field_prefixes: bool) -> TemplatePattern:
    original = str(value or "")
    pieces: list[str] = []
    slots: list[Slot] = []
    issues: list[str] = []
    cursor = 0

    def add_slot(label: str, source: str, start: int, end: int) -> None:
        index = len(slots)
        pieces.append(f"\uFFF0{index}\uFFF1")
        slots.append(Slot(index, label or f"填写区{index + 1}", source, start, end))

    while (match := MARKUP_RE.search(original, cursor)) is not None:
        pieces.append(original[cursor:match.start()])
        end = _markup_end(original, match)
        if end is None:
            issues.append("下划线标记未闭合，填写范围无法确定")
            pieces.append(original[match.start():match.end()])
            cursor = match.end()
            continue
        content = _markup_content(original[match.start():end])
        if is_explicit_slot_content(content):
            add_slot(content, "template_underline_prompt", match.start(), end)
        else:
            pieces.append(content)
        cursor = end
    pieces.append(original[cursor:])
    intermediate = "".join(pieces)

    inline_pieces: list[str] = []
    inline_cursor = 0
    inline_matches = list(INLINE_FIELD_RE.finditer(intermediate))
    for position, match in enumerate(inline_matches):
        next_match = inline_matches[position + 1] if position + 1 < len(inline_matches) else None
        value_end = next_match.start() if next_match is not None else len(intermediate)
        between = intermediate[match.end():value_end]
        # A semicolon, full stop or line break closes a labelled field.  Keep
        # that delimiter as fixed text while allowing the value immediately
        # before it to vary (including an empty exemplar in the template).
        delimiter = re.search(r"[；;。\n]", between)
        if delimiter is not None:
            value_end = match.end() + delimiter.start()
            between = between[:delimiter.start()]
        fixed_anchors = (
            *OBLIGATION_HINTS,
            "（有专业", "(有专业", "（盖章）", "(盖章)", "（加盖公章）", "(加盖公章)",
            "后附：", "后附:", "说明：", "说明:",
        )
        obligation_positions = [between.find(marker) for marker in fixed_anchors if marker in between]
        if obligation_positions:
            fixed_start = min(obligation_positions)
            value_end = match.end() + fixed_start
            between = between[:fixed_start]
        # A populated exemplar between two explicit field labels is still a
        # fill region.  It is removed rather than left behind as fixed text.
        # Sentence boundaries are never swallowed by this rule.
        if (
            (not between and next_match is not None)
            or len(between) > 40
            or any(ch in between for ch in "。；;\n")
        ):
            continue
        inline_pieces.append(intermediate[inline_cursor:match.end()])
        index = len(slots)
        inline_pieces.append(f"\uFFF0{index}\uFFF1")
        slots.append(Slot(index, match.group("label"), "explicit_field_label", match.end(), match.end()))
        # Units remain fixed even when a template provides an example value.
        suffix = ""
        if match.group("label") in {"小写", "大写", "金额"}:
            unit = re.search(r"(?P<unit>(?:人民币)?(?:亿元|万元|元|角|分|%|％))\s*$", between)
            if unit:
                suffix = unit.group("unit")
        if preserve_numbered_field_prefixes and next_match is not None:
            numbered = re.search(
                r"\s*(?P<number>[（(][一二三四五六七八九十百零\d]+[）)])\s*$",
                between,
            )
            if numbered:
                suffix += numbered.group("number")
        inline_pieces.append(suffix)
        inline_cursor = value_end
    if inline_pieces:
        inline_pieces.append(intermediate[inline_cursor:])
        intermediate = "".join(inline_pieces)

    if preserve_numbered_field_prefixes:
        bare_field = re.compile(
            r"(?P<label>性别|年龄|身份证号码|职务|姓名|地址|邮编|电话|手机)"
            r"(?P<blank>[ \t\u3000]+)(?=[，,；;])"
        )
        bare_pieces: list[str] = []
        bare_cursor = 0
        for match in bare_field.finditer(intermediate):
            bare_pieces.append(intermediate[bare_cursor:match.end("label")])
            index = len(slots)
            bare_pieces.append(f"\uFFF0{index}\uFFF1")
            slots.append(Slot(
                index,
                match.group("label"),
                "explicit_blank_after_label",
                match.start("blank"),
                match.end("blank"),
            ))
            bare_cursor = match.end("blank")
        if bare_pieces:
            bare_pieces.append(intermediate[bare_cursor:])
            intermediate = "".join(bare_pieces)

    # Literal blanks and explicit bracket prompts are template evidence too.
    rebuilt: list[str] = []
    cursor = 0
    combined = re.compile(f"(?:{LITERAL_BLANK_RE.pattern})|(?:{BRACKET_SLOT_RE.pattern})")
    for match in combined.finditer(intermediate):
        rebuilt.append(intermediate[cursor:match.start()])
        label_match = BRACKET_SLOT_RE.fullmatch(match.group())
        if label_match and not is_explicit_slot_content(label_match.group("label")):
            rebuilt.append(match.group())
            cursor = match.end()
            continue
        label = label_match.group("label") if label_match else "空白填写线"
        index = len(slots)
        rebuilt.append(f"\uFFF0{index}\uFFF1")
        slots.append(Slot(index, label, "template_blank", match.start(), match.end()))
        cursor = match.end()
    rebuilt.append(intermediate[cursor:])
    pattern_text = _layout_normalize("".join(rebuilt))
    if preserve_numbered_field_prefixes:
        pattern_text = _v32_layout_spacing(pattern_text)
    salutation = re.match(r"^(致\uFFF0\d+\uFFF1[：:])", pattern_text)
    if salutation:
        index = len(slots)
        token = f"\uFFF0{index}\uFFF1"
        pattern_text = pattern_text[:salutation.end()] + token + pattern_text[salutation.end():]
        slots.append(Slot(index, "抬头单位", "salutation_value", salutation.end(), salutation.end()))
    if not slots and pattern_text.endswith(("：", ":")) and len(pattern_text) <= 60:
        compact = re.sub(r"\s+", "", pattern_text)
        if compact not in {"注：", "注:", "说明：", "说明:"} and (
            any(marker in compact for marker in TRAILING_FIELD_HINTS)
            or not any(marker in compact for marker in OBLIGATION_HINTS)
        ):
            index = len(slots)
            pattern_text += f"\uFFF0{index}\uFFF1"
            slots.append(Slot(index, compact.rstrip("：:"), "explicit_field_label", len(original), len(original)))

    # Slot discovery happens in more than one pass (markup, inline labels,
    # literal blanks).  Renumber the final tokens in their actual reading order
    # so captures and fill-region evidence always refer to the same slot.
    slot_by_index = {slot.index: slot for slot in slots}
    ordered_slots: list[Slot] = []

    def renumber(match: re.Match[str]) -> str:
        old_index = int(match.group("index"))
        slot = slot_by_index[old_index]
        new_index = len(ordered_slots)
        ordered_slots.append(Slot(
            new_index,
            slot.label,
            slot.source,
            slot.start,
            slot.end,
        ))
        return f"\uFFF0{new_index}\uFFF1"

    pattern_text = SLOT_TOKEN_RE.sub(renumber, pattern_text)
    pattern_text = re.sub(r" *(\uFFF0\d+\uFFF1) *", r"\1", pattern_text)
    slots = ordered_slots
    fixed = tuple(SLOT_TOKEN_RE.split(pattern_text)[::2])
    display = SLOT_TOKEN_RE.sub(lambda m: f"〔填写区{int(m.group('index')) + 1}〕", pattern_text)
    return TemplatePattern(original, display, pattern_text, fixed, tuple(slots), tuple(issues))


def build_pattern_v31(value: Any) -> TemplatePattern:
    """Frozen v3.1 template parser used by the rollback path."""
    return _build_pattern(value, preserve_numbered_field_prefixes=False)


def build_pattern(value: Any) -> TemplatePattern:
    return _build_pattern(value, preserve_numbered_field_prefixes=True)


def _align_slots(pattern: TemplatePattern, candidate: str) -> tuple[str, list[str]]:
    """Return pass/unclear/fail and captures for a bounded ordered slot map."""
    text = plain_text(candidate)
    fixed = list(pattern.fixed_segments)
    if not pattern.slots:
        return ("pass" if text == plain_text(pattern.pattern_text) else "fail"), []

    # Exact bounded regular expression.  Slots cannot cross paragraph boundaries
    # because layout normalization has already converted an evidence-backed wrap
    # into one ordinary space.
    regex_parts: list[str] = []
    for index, segment in enumerate(fixed):
        regex_parts.append(re.escape(segment))
        if index < len(pattern.slots):
            regex_parts.append("(.*?)")
    regex = re.compile("".join(regex_parts), re.S)
    match = regex.fullmatch(text)
    if match:
        return "pass", list(match.groups())

    # More than one possible bounded placement means the fill range is not safe.
    search_regex = re.compile("".join(regex_parts), re.S)
    placements = list(search_regex.finditer(text))
    if len(placements) > 1:
        return "unclear", []
    return "fail", []


def compare_pattern_legacy(pattern: TemplatePattern, candidate: Any) -> dict[str, Any]:
    candidate_text = plain_text(candidate)
    if pattern.issues:
        return {
            "status": "unclear",
            "template_text": pattern.display_text,
            "bid_text": candidate_text,
            "captures": [],
            "issues": list(pattern.issues),
            "differences": [],
        }
    status, captures = _align_slots(pattern, candidate_text)
    differences = [] if status == "pass" else character_differences(
        fixed_text(pattern), candidate_text
    )
    return {
        "status": status,
        "template_text": pattern.display_text,
        "bid_text": candidate_text,
        "captures": captures,
        "issues": [],
        "differences": differences,
    }


def _slot_fixed_offsets(pattern: TemplatePattern) -> set[int]:
    """Return insertion offsets created by declared template slots.

    Offsets are measured in the fixed-only text used for the character diff.
    Candidate insertions at these exact offsets are field values and therefore
    are not fixed-content changes.
    """
    offsets: set[int] = set()
    offset = 0
    for index, segment in enumerate(pattern.fixed_segments):
        offset += len(plain_text(segment))
        if index < len(pattern.slots):
            offsets.add(offset)
            following = plain_text(pattern.fixed_segments[index + 1])
            punctuation = re.match(r"^[：:，,；;]\s*", following)
            if punctuation:
                offsets.add(offset + len(punctuation.group()))
    return offsets


def character_differences_for_pattern(
    pattern: TemplatePattern,
    candidate: Any,
) -> list[dict[str, Any]]:
    """Diff fixed text while excluding values in declared filling slots."""
    differences = character_differences(fixed_text(pattern), candidate)
    slot_offsets = _slot_fixed_offsets(pattern)
    return [
        difference
        for difference in differences
        if not (
            difference.get("type") == "insert"
            and int((difference.get("template_range") or {}).get("start", -1))
            == int((difference.get("template_range") or {}).get("end", -2))
            and int((difference.get("template_range") or {}).get("start", -1))
            in slot_offsets
        )
    ]


def compare_pattern_v31(pattern: TemplatePattern, candidate: Any) -> dict[str, Any]:
    """Frozen v3.1 comparison path used by the rollback switch."""
    candidate_text = plain_text(candidate)
    if pattern.issues:
        return {
            "status": "unclear",
            "template_text": pattern.display_text,
            "bid_text": candidate_text,
            "captures": [],
            "issues": list(pattern.issues),
            "differences": [],
        }
    status, captures = _align_slots(pattern, candidate_text)
    differences = [] if status == "pass" else character_differences_for_pattern(
        pattern,
        candidate_text,
    )
    return {
        "status": status,
        "template_text": pattern.display_text,
        "bid_text": candidate_text,
        "captures": captures,
        "issues": [],
        "differences": differences,
    }


def _field_label(value: str) -> str:
    label = re.sub(r"[（(].*?[）)]", "", str(value or ""))
    return label.strip().rstrip("：:")


def _field_matches(text: str, label: str, start: int) -> list[re.Match[str]]:
    value = _field_label(label)
    if not value:
        return []
    prefix = (
        r"(?:[（(]?(?:\d+(?:[-－]\d+)*|[一二三四五六七八九十百零]+)"
        r"[）).．、]?\s*)?"
    )
    return list(re.finditer(rf"{prefix}{re.escape(value)}\s*[:：]", text[start:]))


def _anchor_candidates(segment: str, *, from_start: bool) -> list[str]:
    value = plain_text(segment)
    if not value:
        return []
    candidates = [value]
    stripped = value.lstrip() if from_start else value.rstrip()
    if stripped != value:
        candidates.append(stripped)
    lengths = (48, 32, 20, 12, 8, 4)
    for length in lengths:
        if len(stripped) < length:
            continue
        candidates.append(stripped[:length] if from_start else stripped[-length:])
    # Prefer anchors with actual words.  A punctuation-only anchor is useful
    # only as part of a longer exact fragment and must not bound a fill alone.
    return list(dict.fromkeys(
        item for item in candidates
        if item and (len(item) >= 4 or re.search(r"[A-Za-z0-9\u3400-\u9fff]", item))
    ))


def _find_left_anchor(text: str, segment: str, start: int) -> tuple[int | None, bool]:
    hits: list[tuple[int, int]] = []
    for anchor in _anchor_candidates(segment, from_start=False):
        position = text.find(anchor, start)
        if position >= 0:
            hits.append((position + len(anchor), len(anchor)))
    if not hits:
        return None, False
    longest = max(length for _, length in hits)
    endpoints = sorted({end for end, length in hits if length == longest})
    return endpoints[0], len(endpoints) == 1


def _find_right_anchor(
    text: str,
    segment: str,
    start: int,
    next_slot: Slot | None,
) -> tuple[int | None, bool]:
    hits: list[tuple[int, int]] = []
    if next_slot is not None:
        for match in _field_matches(text, next_slot.label, start):
            hits.append((start + match.start(), 1000))
    for anchor in _anchor_candidates(segment, from_start=True):
        position = text.find(anchor, start)
        if position >= 0:
            hits.append((position, len(anchor)))
    value = plain_text(segment)
    if value and value[0] in "；;。！!？？,，":
        position = text.find(value[0], start)
        if position >= 0:
            hits.append((position, 1))
    if not hits:
        return None, False
    longest = max(length for _, length in hits)
    positions = sorted({position for position, length in hits if length == longest})
    return positions[0], len(positions) == 1


def _candidate_fill_ranges(
    pattern: TemplatePattern,
    candidate_text: str,
) -> tuple[list[dict[str, Any]], list[str]]:
    """Locate only template-declared fill values in the candidate.

    The mapping is deliberately bounded by fixed labels/anchors.  A bid-side
    underline or a low similarity score can never create or widen a range.
    """
    ranges: list[dict[str, Any]] = []
    issues: list[str] = []
    cursor = 0
    fixed = [plain_text(value) for value in pattern.fixed_segments]
    for index, slot in enumerate(pattern.slots):
        left = fixed[index]
        right = fixed[index + 1]
        next_slot = pattern.slots[index + 1] if index + 1 < len(pattern.slots) else None
        start: int | None = None
        unique_left = True
        if slot.source in {"explicit_field_label", "salutation_value"}:
            matches = _field_matches(candidate_text, slot.label, cursor)
            if matches:
                absolute = [(cursor + match.start(), cursor + match.end()) for match in matches]
                start = absolute[0][1]
                unique_left = len(absolute) == 1
        if start is None:
            if not left:
                start = cursor
            else:
                start, unique_left = _find_left_anchor(candidate_text, left, cursor)
        if start is None:
            issues.append(f"填写区“{slot.label}”缺少可确认的前置固定锚点")
            continue

        end, unique_right = _find_right_anchor(candidate_text, right, start, next_slot)
        if end is None:
            if (
                index == len(pattern.slots) - 1
                and slot.source == "explicit_field_label"
                and any(marker in right for marker in OBLIGATION_HINTS)
            ):
                obligation_positions = [
                    candidate_text.find(marker, start)
                    for marker in OBLIGATION_HINTS
                    if candidate_text.find(marker, start) >= 0
                ]
                end = min(obligation_positions) if obligation_positions else len(candidate_text)
                unique_right = True
            if index == len(pattern.slots) - 1 and not right and (
                slot.source in {"explicit_field_label", "salutation_value"}
                or bool(left)
            ):
                end = len(candidate_text)
                unique_right = True
            elif end is None:
                issues.append(f"填写区“{slot.label}”缺少可确认的后置固定锚点")
                continue
        if end < start:
            issues.append(f"填写区“{slot.label}”的固定锚点顺序冲突")
            continue
        if not unique_left or not unique_right:
            issues.append(f"填写区“{slot.label}”存在多个同等位置")
            continue
        if ranges and start < int(ranges[-1]["candidate_range"]["end"]):
            issues.append(f"填写区“{slot.label}”与上一填写区重叠")
            continue
        ranges.append({
            "index": slot.index,
            "label": slot.label,
            "source": slot.source,
            "template_range": {"start": slot.start, "end": slot.end},
            "candidate_range": {"start": start, "end": end},
            "value": candidate_text[start:end],
        })
        cursor = end
    if len(ranges) != len(pattern.slots) and not issues:
        issues.append("填写区域未能完整映射")
    return ranges, issues


def _remove_candidate_ranges(
    text: str,
    ranges: list[dict[str, Any]],
) -> tuple[str, list[int], list[dict[str, Any]]]:
    removed = sorted(
        (
            int(item["candidate_range"]["start"]),
            int(item["candidate_range"]["end"]),
        )
        for item in ranges
    )
    result: list[str] = []
    index_map: list[int] = []
    cursor = 0
    for start, end in removed:
        for index in range(cursor, start):
            result.append(text[index])
            index_map.append(index)
        cursor = end
    for index in range(cursor, len(text)):
        result.append(text[index])
        index_map.append(index)

    spans: list[dict[str, Any]] = []
    if index_map:
        compare_start = 0
        original_start = index_map[0]
        previous = index_map[0]
        for compare_index, original_index in enumerate(index_map[1:], start=1):
            if original_index != previous + 1:
                spans.append({
                    "comparison_range": {"start": compare_start, "end": compare_index},
                    "original_range": {"start": original_start, "end": previous + 1},
                })
                compare_start = compare_index
                original_start = original_index
            previous = original_index
        spans.append({
            "comparison_range": {"start": compare_start, "end": len(index_map)},
            "original_range": {"start": original_start, "end": previous + 1},
        })
    return "".join(result), index_map, spans


def _original_offset(index_map: list[int], offset: int, original_length: int) -> int:
    if not index_map:
        return 0 if offset <= 0 else original_length
    if offset <= 0:
        return index_map[0]
    if offset >= len(index_map):
        return original_length
    return index_map[offset]


def _remap_differences(
    differences: list[dict[str, Any]],
    *,
    candidate_text: str,
    index_map: list[int],
) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for difference in differences:
        value = dict(difference)
        compare_range = dict(value.get("bid_range") or {})
        start = int(compare_range.get("start") or 0)
        end = int(compare_range.get("end") or start)
        original_start = _original_offset(index_map, start, len(candidate_text))
        if end > start and index_map:
            original_end = index_map[min(end, len(index_map)) - 1] + 1
        else:
            original_end = original_start
        value["comparison_bid_range"] = compare_range
        value["bid_range"] = {"start": original_start, "end": original_end}
        if end > start:
            value["bid_text"] = candidate_text[original_start:original_end]
        result.append(value)
    return result


def compare_pattern(pattern: TemplatePattern, candidate: Any) -> dict[str, Any]:
    """Compare v3.2 fixed content after bounded fill-region projection."""
    candidate_text = _plain_text_v32(candidate)
    if pattern.issues:
        return {
            "status": "unclear",
            "template_text": pattern.display_text,
            "bid_text": candidate_text,
            "comparison_bid_text": candidate_text,
            "captures": [],
            "fillable_ranges": [],
            "text_map": [],
            "issues": list(pattern.issues),
            "differences": [],
        }
    status, captures = _align_slots(pattern, candidate_text)
    if status == "pass":
        return {
            "status": "pass",
            "template_text": pattern.display_text,
            "bid_text": candidate_text,
            "comparison_bid_text": fixed_text(pattern),
            "captures": captures,
            "fillable_ranges": [],
            "text_map": [],
            "issues": [],
            "differences": [],
        }
    if not pattern.slots:
        return {
            "status": "fail",
            "template_text": pattern.display_text,
            "bid_text": candidate_text,
            "comparison_bid_text": candidate_text,
            "captures": [],
            "fillable_ranges": [],
            "text_map": [{
                "comparison_range": {"start": 0, "end": len(candidate_text)},
                "original_range": {"start": 0, "end": len(candidate_text)},
            }] if candidate_text else [],
            "issues": [],
            "differences": character_differences(fixed_text(pattern), candidate_text),
        }

    ranges, issues = _candidate_fill_ranges(pattern, candidate_text)
    if issues:
        return {
            "status": "unclear",
            "template_text": pattern.display_text,
            "bid_text": candidate_text,
            "comparison_bid_text": candidate_text,
            "captures": [item["value"] for item in ranges],
            "fillable_ranges": ranges,
            "text_map": [],
            "issues": issues,
            "differences": [],
        }
    projected, index_map, text_map = _remove_candidate_ranges(candidate_text, ranges)
    differences = _remap_differences(
        character_differences(fixed_text(pattern), projected),
        candidate_text=candidate_text,
        index_map=index_map,
    )
    return {
        "status": "pass" if not differences else "fail",
        "template_text": pattern.display_text,
        "bid_text": candidate_text,
        "comparison_bid_text": projected,
        "captures": [item["value"] for item in ranges],
        "fillable_ranges": ranges,
        "text_map": text_map,
        "issues": [],
        "differences": differences,
    }


def fixed_text(pattern: TemplatePattern) -> str:
    """Text used for a failed diff; declared fill values are not invented."""
    return SLOT_TOKEN_RE.sub("", pattern.pattern_text)


def character_differences(template_text: Any, bid_text: Any) -> list[dict[str, Any]]:
    left = plain_text(template_text)
    right = plain_text(bid_text)
    matcher = difflib.SequenceMatcher(None, left, right, autojunk=False)
    differences: list[dict[str, Any]] = []
    for tag, left_start, left_end, right_start, right_end in matcher.get_opcodes():
        if tag == "equal":
            continue
        diff_type = {"replace": "replace", "delete": "delete", "insert": "insert"}[tag]
        differences.append({
            "type": diff_type,
            "template_text": left[left_start:left_end],
            "bid_text": right[right_start:right_end],
            "template_range": {"start": left_start, "end": left_end},
            "bid_range": {"start": right_start, "end": right_end},
        })

    # Report a relocation when the same non-trivial fixed text is deleted and
    # inserted elsewhere.  Keep both ranges for auditability.
    deleted = [item for item in differences if item["type"] == "delete" and len(item["template_text"]) >= 2]
    inserted = [item for item in differences if item["type"] == "insert" and len(item["bid_text"]) >= 2]
    used: set[int] = set()
    moved: list[dict[str, Any]] = []
    for delete in deleted:
        for index, insert in enumerate(inserted):
            if index in used or delete["template_text"] != insert["bid_text"]:
                continue
            used.add(index)
            moved.append({
                "type": "move",
                "template_text": delete["template_text"],
                "bid_text": insert["bid_text"],
                "template_range": delete["template_range"],
                "bid_range": insert["bid_range"],
            })
            break
    return differences + moved


def with_locations(
    differences: list[dict[str, Any]],
    *,
    template_locations: list[dict[str, Any]],
    bid_locations: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Attach real block/line/cell boxes without fabricating character boxes."""
    template_precision = _location_precision(template_locations)
    bid_precision = _location_precision(bid_locations)
    return [
        {
            **item,
            "template_locations": list(template_locations),
            "bid_locations": list(bid_locations),
            "template_location_precision": template_precision,
            "bid_location_precision": bid_precision,
            "coordinate_system": "pdf_points",
        }
        for item in differences
    ]


def _location_precision(locations: list[dict[str, Any]]) -> str:
    values = {str(item.get("location_precision") or item.get("type") or "block").lower() for item in locations}
    if any(value in {"character", "char"} for value in values):
        return "character"
    if any(value in {"word", "line", "cell", "table_cell"} for value in values):
        return "cell" if any("cell" in value for value in values) else "line"
    return "block"
