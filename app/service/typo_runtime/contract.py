"""Validated typo candidates and conservative word-level acceptance rules."""

from __future__ import annotations

import json
import re
from difflib import SequenceMatcher
from functools import lru_cache
from pathlib import Path
from typing import Any


VERSION = "duplicate-typo-macbert-cec3-v3"
LEGACY_VERSION = "duplicate-typo-macbert-v2"
DEFAULT_AUTO_MIN_PROBABILITY = 0.90
DEFAULT_AUTO_MIN_PROBABILITY_RATIO = 20.0
_LEXICON_PATH = Path(__file__).with_name("word_rules.json")
_CHINESE_CHARACTER = re.compile(r"[\u4e00-\u9fff]")


class TypoUnavailable(RuntimeError):
    pass


@lru_cache(maxsize=8)
def _read_word_rules(path: str) -> dict[str, Any]:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise TypoUnavailable("错别字词语规则不可用") from exc
    if not isinstance(payload, dict) or not str(payload.get("version") or "").strip():
        raise TypoUnavailable("错别字词语规则格式无效")
    corrections = payload.get("approved_corrections")
    protected = payload.get("protected_terms")
    if not isinstance(corrections, list) or not isinstance(protected, list):
        raise TypoUnavailable("错别字词语规则格式无效")
    return payload


def word_rules(path: str | Path | None = None) -> dict[str, Any]:
    """Load a validated rule file; runtime callers use the packaged default."""
    return _read_word_rules(str(Path(path or _LEXICON_PATH).resolve()))


def word_rule_version(rules: dict[str, Any] | None = None) -> str | None:
    # v3 never consults the historical hand-maintained word rules.
    return str(rules["version"]) if rules is not None else None


def protected_spans(text: str) -> list[tuple[int, int]]:
    patterns = [
        r"https?://\S+|[\w.+-]+@[\w.-]+",
        r"[A-Za-z0-9][A-Za-z0-9_.%％/\-]*",
        r"(?:姓名|联系人|法定代表人|投标人|投标单位|参选人|单位名称|公司名称|项目名称|型号|规格|账号)[：:]\s*[^\n；;，,。|]{1,100}",
        r"[\u4e00-\u9fffA-Za-z（）()·]{2,60}(?:有限责任公司|股份有限公司|有限公司)",
    ]
    return [(match.start(), match.end()) for pattern in patterns for match in re.finditer(pattern, text)]


def _overlaps(start: int, end: int, spans: list[tuple[int, int]]) -> bool:
    return any(start < span_end and end > span_start for span_start, span_end in spans)


def _protected_term_at(
    text: str,
    start: int,
    end: int,
    rules: dict[str, Any],
) -> str | None:
    for value in rules.get("protected_terms") or []:
        term = str(value or "").strip()
        if not term:
            continue
        for match in re.finditer(re.escape(term), text):
            if match.start() <= start and end <= match.end():
                return term
    return None


def _approved_word_edit(
    text: str,
    start: int,
    end: int,
    replacement: str,
    rules: dict[str, Any],
) -> dict[str, Any] | None:
    matches: list[dict[str, Any]] = []
    for value in rules.get("approved_corrections") or []:
        if not isinstance(value, dict):
            continue
        source = str(value.get("source") or "")
        target = str(value.get("target") or "")
        if not source or len(source) != len(target):
            continue
        differences = [index for index, pair in enumerate(zip(source, target)) if pair[0] != pair[1]]
        if len(differences) != 1:
            continue
        difference = differences[0]
        if target[difference] != replacement:
            continue
        for occurrence in re.finditer(re.escape(source), text):
            conditions = value.get("conditions") or {}
            if not isinstance(conditions, dict):
                continue
            left = text[:occurrence.start()]
            right = text[occurrence.end():]
            required_left = [
                str(item) for item in conditions.get("required_left") or [] if str(item)
            ]
            required_right = [
                str(item) for item in conditions.get("required_right") or [] if str(item)
            ]
            forbidden_left = [
                str(item) for item in conditions.get("forbidden_left") or [] if str(item)
            ]
            forbidden_right = [
                str(item) for item in conditions.get("forbidden_right") or [] if str(item)
            ]
            if required_left and not any(left.endswith(item) for item in required_left):
                continue
            if required_right and not any(right.startswith(item) for item in required_right):
                continue
            if any(left.endswith(item) for item in forbidden_left):
                continue
            if any(right.startswith(item) for item in forbidden_right):
                continue
            if occurrence.start() + difference == start and end == start + 1:
                matches.append(
                    {
                        "word_start": occurrence.start(),
                        "word_end": occurrence.end(),
                        "original_word": source,
                        "replacement_word": target,
                        "rule_id": str(value.get("id") or f"{source}->{target}"),
                    }
                )
    if not matches:
        return None
    matches.sort(key=lambda item: len(item["original_word"]), reverse=True)
    return matches[0]


def validate_candidates(text: str, payload: Any, *, allow_subunit_ratio: bool = False) -> list[dict[str, Any]]:
    """Validate character offsets and probabilities returned by the model worker."""
    if not isinstance(payload, dict) or not isinstance(payload.get("candidates"), list):
        raise TypoUnavailable("纠错输出缺少字符候选")
    found: list[dict[str, Any]] = []
    seen: set[tuple[int, int, str]] = set()
    protected = protected_spans(text)
    for raw in payload["candidates"]:
        if not isinstance(raw, dict):
            raise TypoUnavailable("纠错字符候选格式无效")
        start, end = raw.get("start"), raw.get("end")
        original, replacement = raw.get("original"), raw.get("replacement")
        if (
            not isinstance(start, int)
            or not isinstance(end, int)
            or end != start + 1
            or not 0 <= start < end <= len(text)
            or not isinstance(original, str)
            or not isinstance(replacement, str)
            or len(original) != 1
            or len(replacement) != 1
            or text[start:end] != original
            or original == replacement
            or not _CHINESE_CHARACTER.fullmatch(original)
            or not _CHINESE_CHARACTER.fullmatch(replacement)
        ):
            raise TypoUnavailable("纠错字符候选位置无效")
        try:
            probability = float(raw.get("candidate_probability"))
            source_probability = float(raw.get("source_probability"))
            probability_ratio = float(raw.get("probability_ratio"))
        except (TypeError, ValueError) as exc:
            raise TypoUnavailable("纠错字符候选概率无效") from exc
        if not 0.0 <= probability <= 1.0 or not 0.0 <= source_probability <= 1.0 or probability_ratio < (0.0 if allow_subunit_ratio else 1.0):
            raise TypoUnavailable("纠错字符候选概率无效")
        key = (start, end, replacement)
        if key in seen:
            continue
        seen.add(key)
        found.append(
            {
                "start": start,
                "end": end,
                "position": start,
                "original": original,
                "replacement": replacement,
                "candidate_probability": probability,
                "source_probability": source_probability,
                "probability_ratio": probability_ratio,
                "protected_span": _overlaps(start, end, protected),
            }
        )
    return sorted(found, key=lambda item: (item["start"], item["end"], item["replacement"]))


def classify_candidates(
    text: str,
    candidates: list[dict[str, Any]],
    *,
    auto_min_probability: float = DEFAULT_AUTO_MIN_PROBABILITY,
    auto_min_probability_ratio: float = DEFAULT_AUTO_MIN_PROBABILITY_RATIO,
    rules: dict[str, Any] | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Split model candidates into confirmed word edits and review-only candidates."""
    active_rules = rules or word_rules()
    confirmed: list[dict[str, Any]] = []
    review: list[dict[str, Any]] = []
    for candidate in candidates:
        start, end = int(candidate["start"]), int(candidate["end"])
        if candidate.get("protected_span") or _protected_term_at(
            text, start, end, active_rules
        ):
            continue
        word_edit = _approved_word_edit(
            text,
            start,
            end,
            str(candidate["replacement"]),
            active_rules,
        )
        context_start = max(0, start - 12)
        context_end = min(len(text), end + 12)
        item = {
            **candidate,
            "matched_text": candidate["original"],
            "raw_matched_text": candidate["original"],
            "suggestion": candidate["replacement"],
            "highlight_text": candidate["original"],
            "error_type": "substitution",
            "display_text": text[context_start:context_end],
            "context_start": context_start,
            "context_end": context_end,
            "rule_version": LEGACY_VERSION,
            "word_rule_version": word_rule_version(active_rules),
        }
        if word_edit:
            item.update(word_edit)
            item["matched_text"] = word_edit["original_word"]
            item["suggestion"] = word_edit["replacement_word"]
        else:
            item.update(
                {
                    "word_start": start,
                    "word_end": end,
                    "original_word": candidate["original"],
                    "replacement_word": candidate["replacement"],
                    "verification_status": "review",
                    "review_reason": "unverified_word",
                }
            )
            review.append(item)
            continue
        if (
            float(candidate["candidate_probability"]) < auto_min_probability
            or float(candidate["probability_ratio"]) < auto_min_probability_ratio
        ):
            item.update(
                {
                    "verification_status": "review",
                    "review_reason": "below_auto_accept_threshold",
                }
            )
            review.append(item)
            continue
        item.update({"verification_status": "confirmed", "review_reason": None})
        confirmed.append(item)
    return confirmed, review


def classify_dual_candidates(
    text: str,
    candidates: list[dict[str, Any]],
    corrected: str | None,
    roundtrip: str | None,
    *,
    auto_min_probability: float = DEFAULT_AUTO_MIN_PROBABILITY,
    auto_min_probability_ratio: float = DEFAULT_AUTO_MIN_PROBABILITY_RATIO,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Accept only exact MacBERT/CEC3 agreement on valid word substitutions."""
    eligible = [
        item for item in candidates
        if not item.get("protected_span")
        and float(item["candidate_probability"]) >= auto_min_probability
        and float(item["probability_ratio"]) >= auto_min_probability_ratio
    ]
    counts = {
        "candidate_count": len(candidates),
        "eligible_count": len(eligible),
        "hidden_count": len(candidates),
    }
    if not eligible:
        return [], counts
    if not isinstance(corrected, str) or not corrected or not isinstance(roundtrip, str):
        raise TypoUnavailable("CEC3 纠错输出无效")
    if corrected != roundtrip or len(corrected) != len(text) or corrected == text:
        return [], counts

    actual = {
        (index, index + 1, before, after)
        for index, (before, after) in enumerate(zip(text, corrected))
        if before != after
    }
    nominated = {
        (int(item["start"]), int(item["end"]), item["original"], item["replacement"])
        for item in eligible
    }
    if actual != nominated or any(
        not _CHINESE_CHARACTER.fullmatch(before)
        or not _CHINESE_CHARACTER.fullmatch(after)
        for _, _, before, after in actual
    ):
        return [], counts

    try:
        import jieba
    except ImportError as exc:
        raise TypoUnavailable("Jieba 通用词典不可用") from exc
    jieba.initialize()
    tokens = list(jieba.tokenize(corrected, mode="default"))
    source_tokens = list(jieba.tokenize(text, mode="default"))
    confirmed: list[dict[str, Any]] = []
    for candidate in eligible:
        start = candidate["start"]
        matches = [
            (token, word_start, word_end)
            for token, word_start, word_end in tokens
            if word_start <= start < word_end
        ]
        if len(matches) != 1:
            return [], counts
        target_word, word_start, word_end = matches[0]
        original_word = text[word_start:word_end]
        source_matches = [
            (source_start, source_end)
            for _, source_start, source_end in source_tokens
            if source_start <= start < source_end
        ]
        if (
            not 2 <= word_end - word_start <= 8
            or not re.fullmatch(r"[\u4e00-\u9fff]{2,8}", target_word)
            or not jieba.dt.FREQ.get(target_word, 0)
            or jieba.dt.FREQ.get(original_word, 0)
            or source_matches != [(word_start, word_end)]
            or sum(a != b for a, b in zip(original_word, target_word)) != 1
            or original_word[start - word_start] != candidate["original"]
            or target_word[start - word_start] != candidate["replacement"]
        ):
            return [], counts
        context_start = max(0, start - 12)
        context_end = min(len(text), start + 13)
        confirmed.append({
            **candidate,
            "matched_text": original_word,
            "raw_matched_text": candidate["original"],
            "suggestion": target_word,
            "highlight_text": candidate["original"],
            "error_type": "substitution",
            "display_text": text[context_start:context_end],
            "context_start": context_start,
            "context_end": context_end,
            "word_start": word_start,
            "word_end": word_end,
            "original_word": original_word,
            "replacement_word": target_word,
            "verification_status": "confirmed",
            "verification_method": "macbert_cec3_roundtrip",
            "rule_version": VERSION,
            "word_rule_version": None,
        })
    counts["hidden_count"] = len(candidates) - len(confirmed)
    return confirmed, counts


def validate_edits(text: str, payload: Any) -> list[dict[str, Any]]:
    """Legacy whole-text edit validator retained for old callers and fixtures."""
    if not isinstance(payload, dict) or not isinstance(payload.get("edits"), list):
        raise TypoUnavailable("纠错输出格式不完整")
    found: list[dict[str, Any]] = []
    seen: set[tuple[int, int, str]] = set()
    protected = protected_spans(text)
    for raw in payload["edits"]:
        if not isinstance(raw, dict):
            raise TypoUnavailable("纠错修改项无效")
        old, new = raw.get("original"), raw.get("replacement")
        if not isinstance(old, str) or not isinstance(new, str) or not 0 < len(old) <= 600 or len(new) > 600:
            raise TypoUnavailable("纠错输出格式无效")
        if old == new:
            continue
        starts = [match.start() for match in re.finditer(re.escape(old), text)]
        if len(starts) != 1:
            raise TypoUnavailable("纠错原文位置缺失或不唯一")
        edits = [operation for operation in SequenceMatcher(None, old, new, autojunk=False).get_opcodes() if operation[0] != "equal"]
        if len(edits) > 6 or sum(max(c - b, e - d) for _, b, c, d, e in edits) > 8:
            raise TypoUnavailable("纠错输出包含大范围改写，未采纳")
        for _, a, b, c, d in edits:
            source, target = old[a:b], new[c:d]
            if max(len(source), len(target)) > 4:
                raise TypoUnavailable("纠错输出超出字词范围")
            start, end = starts[0] + a, starts[0] + b
            if not re.fullmatch(r"[\u4e00-\u9fff]*", source + target):
                continue
            if _overlaps(start, end, protected):
                continue
            if start == end:
                if start > 0:
                    start -= 1
                    source = text[start:end]
                    target = source + target
                elif end < len(text):
                    end += 1
                    source = text[start:end]
                    target = target + source
                else:
                    raise TypoUnavailable("无法定位补字位置")
            key = (start, end, target)
            if key in seen:
                continue
            if any(start < item["end"] and end > item["start"] for item in found):
                raise TypoUnavailable("纠错修改范围冲突")
            seen.add(key)
            context = text[max(0, start - 12):min(len(text), end + 12)]
            found.append(
                {
                    "start": start,
                    "end": end,
                    "position": start,
                    "original": source,
                    "matched_text": source,
                    "raw_matched_text": source,
                    "suggestion": target,
                    "replacement": target,
                    "error_type": "missing" if len(target) > len(source) else "extra" if len(target) < len(source) else "substitution",
                    "display_text": context,
                    "highlight_text": source,
                    "context": text,
                    "rule_version": LEGACY_VERSION,
                }
            )
    return sorted(found, key=lambda item: item["start"])


def chunks(text: str, limit: int = 240, overlap: int = 24):
    start = 0
    while start < len(text):
        end = min(len(text), start + limit)
        if end < len(text):
            marks = [match.end() for match in re.finditer(r"[。；！？\n]", text[start:end])]
            if marks and marks[-1] >= limit // 2:
                end = start + marks[-1]
        yield start, text[start:end]
        if end == len(text):
            break
        start = max(start + 1, end - overlap)
