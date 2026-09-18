"""Evidence-based template text projection; never changes stored OCR text.

Offsets always refer to the input string. Only explicit underline spans may be
removed: dates, numbers, parentheses and organization names are not blanks.
"""
from __future__ import annotations

import re
from typing import Any

VERSION = "underline-spans-v2"
EVIDENCE_KEY = "_template_underline_evidence"


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


def markup_spans(text: str) -> tuple[list[dict[str, Any]], list[str]]:
    """Parse balanced LaTeX/HTML underline markup, including nested commands."""
    spans: list[dict[str, Any]] = []
    issues: list[str] = []
    cursor = 0
    pattern = re.compile(r"\\underline\s*\{|<u(?:\s[^>]*)?>", re.I)
    while (match := pattern.search(text, cursor)) is not None:
        if match.group().startswith("\\"):
            end = _brace_end(text, match.end() - 1)
        else:
            depth, end = 1, None
            for tag in re.finditer(r"</?u(?:\s[^>]*)?>", text[match.end():], re.I):
                depth += -1 if tag.group().startswith("</") else 1
                if depth == 0:
                    end = match.end() + tag.end()
                    break
        if end is None:
            issues.append("下划线标记未闭合，无法确定排除范围")
            cursor = match.end()
            continue
        spans.append({"start": match.start(), "end": end, "text": text[match.start():end],
                      "source": "ocr_underline_markup"})
        cursor = end
    # Empty literal underline glyphs contain no semantic value.
    spans.extend({"start": m.start(), "end": m.end(), "text": m.group(),
                  "source": "empty_underline"} for m in re.finditer(r"[_＿]{2,}", text)
                 if not any(s["start"] <= m.start() < s["end"] for s in spans))
    return spans, issues


def compact_offsets(text: str) -> tuple[str, list[int]]:
    pairs = [(ch.lower(), index) for index, ch in enumerate(text) if ch.isalnum()]
    return "".join(ch for ch, _ in pairs), [index for _, index in pairs]


def _overlap(a: list, b: list) -> bool:
    return len(a) == 4 and len(b) == 4 and min(a[2], b[2]) > max(a[0], b[0]) and min(a[3], b[3]) > max(a[1], b[1])


def project_text(text: Any, *, evidence: dict | None = None, pages: list[int] | None = None,
                 locations: list[dict] | None = None, require_physical: bool = False, require_scope: bool = False) -> dict:
    """Return a comparison-only text and auditable exclusion spans.

    A physical span must have a unique exact text alignment in this attachment.
    Repeated or unmappable text never triggers a guess or a raw-text fallback.
    """
    original = str(text or "")
    spans, issues = markup_spans(original)
    marked = list(original)
    for span in spans:
        marked[span["start"]:span["end"]] = " " * (span["end"] - span["start"])
    unmarked = "".join(marked)
    compact, offsets = compact_offsets(unmarked)
    page_map = (evidence or {}).get("pages") or {}
    requested = sorted(set(int(p) for p in (pages or []) if p))
    physical: list[dict] = []
    for page in requested:
        entry = page_map.get(str(page), page_map.get(page))
        if not isinstance(entry, dict):
            if require_physical:
                issues.append(f"第{page}页缺少原件横线定位证据")
            continue
        if entry.get("status") != "ready":
            issues.extend(entry.get("issues") or [f"第{page}页横线范围待确认"])
        for span in entry.get("spans") or []:
            page_locations = [loc for loc in (locations or []) if loc.get("page") == page and loc.get("bbox")]
            if require_scope and not page_locations:
                issues.append(f"第{page}页附件坐标不确定，无法确认横线归属")
                continue
            if page_locations and span.get("bbox") and not any(_overlap(loc["bbox"], span["bbox"]) for loc in page_locations):
                continue
            physical.append({**span, "page": page})
    if require_physical and not requested:
        issues.append("附件页码不确定，无法核验原件横线范围")
    seen = set()
    for span in physical:
        key = (span.get("page"), tuple(span.get("bbox") or []), span.get("text"))
        if key in seen:
            continue
        seen.add(key)
        target, _ = compact_offsets(str(span.get("text") or ""))
        if not target:
            continue
        starts = [m.start() for m in re.finditer(re.escape(target), compact)]
        if not starts:
            # Markup already excluded this exact phrase; this is corroboration.
            if any(target in compact_offsets(str(s.get("text") or ""))[0] for s in spans):
                continue
            # Text elsewhere on the page is outside this particular attachment.
            if locations:
                issues.append(f"第{span['page']}页下划线文字无法与附件原文精确对齐")
            continue
        if len(starts) != 1:
            issues.append(f"第{span['page']}页下划线文字存在多个候选位置")
            continue
        start, end = offsets[starts[0]], offsets[starts[0] + len(target) - 1] + 1
        spans.append({**span, "start": start, "end": end, "text": original[start:end]})
    for span in spans:
        # Preserve paragraph boundaries even when an underline spans lines.
        for index in range(span["start"], span["end"]):
            if marked[index] not in "\r\n":
                marked[index] = " "
    return {"version": VERSION, "status": "unclear" if issues else "ready",
            "text": "".join(marked), "excluded_spans": spans,
            "issues": list(dict.fromkeys(issues))}
