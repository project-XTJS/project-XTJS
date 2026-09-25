"""Experimental v4 per-position verification; never loads hand-written word rules."""

from __future__ import annotations

import re
from difflib import SequenceMatcher
from typing import Any

VERSION = "duplicate-typo-macbert-cec3-v4"
HANZI = re.compile(r"[\u4e00-\u9fff]")


def budget_candidates(candidates: list[dict[str, Any]], limit: int = 8):
    eligible = [item for item in candidates if not item.get("protected_span")]
    eligible.sort(key=lambda item: (-float(item["probability_ratio"]), -float(item["candidate_probability"]), item["start"]))
    return eligible[:limit], max(0, len(eligible) - limit)


def _maps(before: str, after: str):
    """Map equal-length replacements positionally; never guess across shifts."""
    old_to_new: dict[int, int] = {}
    new_to_old: dict[int, int] = {}
    for tag, a, b, c, d in SequenceMatcher(None, before, after, autojunk=False).get_opcodes():
        if tag == "equal" or (tag == "replace" and b - a == d - c):
            for old, new in zip(range(a, b), range(c, d)):
                old_to_new[old] = new
                new_to_old[new] = old
    return old_to_new, new_to_old


def classify_v4(
    text: str,
    candidates: list[dict[str, Any]],
    corrected: str,
    roundtrip: str | None,
    *,
    supported_threshold: float,
    unsupported_threshold: float,
    reasons: dict[tuple[int, str], str] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Confirm independently supported edits; other CEC3 edits do not poison a sentence."""
    import jieba

    jieba.initialize()
    if not isinstance(corrected, str) or not isinstance(roundtrip, str):
        raise ValueError("invalid_cec3_output")
    old_to_new, new_to_old = _maps(text, corrected)
    corrected_to_roundtrip, _ = _maps(corrected, roundtrip)
    target_tokens = list(jieba.tokenize(corrected, mode="default"))
    counts = {
        "candidate_count": len(candidates), "eligible_count": len(candidates),
        "confirmed_count": 0, "hidden_count": len(candidates),
        "verifier_rejected_count": 0, "cec3_unsupported_count": 0,
        "word_invalid_count": 0, "position_invalid_count": 0,
    }
    confirmed = []
    by_position: dict[int, list[dict[str, Any]]] = {}
    for item in candidates:
        by_position.setdefault(int(item["start"]), []).append(item)
        if reasons is not None:
            reasons[(int(item["start"]), str(item["replacement"]))] = "cec3_unsupported"
    for start, proposals in by_position.items():
        def mark(items, reason):
            if reasons is not None:
                for candidate in items:
                    reasons[(int(candidate["start"]), str(candidate["replacement"]))] = reason
        target_pos = old_to_new.get(start)
        if target_pos is None or new_to_old.get(target_pos) != start:
            counts["position_invalid_count"] += len(proposals)
            mark(proposals, "position_invalid")
            continue
        cec3_character = corrected[target_pos]
        if cec3_character != text[start] and not HANZI.fullmatch(cec3_character):
            counts["position_invalid_count"] += len(proposals)
            mark(proposals, "position_invalid")
            continue
        if cec3_character != text[start]:
            proposals = [item for item in proposals if item["replacement"] == cec3_character]
            if not proposals:
                counts["cec3_unsupported_count"] += len(by_position[start])
                continue
            second_pos = corrected_to_roundtrip.get(target_pos)
            if second_pos is None or roundtrip[second_pos] != cec3_character:
                counts["cec3_unsupported_count"] += len(proposals)
                mark(proposals, "cec3_unsupported")
                continue
            method = "macbert_verifier_cec3"
            threshold = supported_threshold
        else:
            method = "macbert_verifier_only"
            threshold = unsupported_threshold
            mark(proposals, "verifier_rejected")
            # An unchanged CEC3 result is not evidence for choosing among two
            # plausible alternatives. Require a unique highest-score proposal.
            proposals = sorted(proposals, key=lambda item: float(item["verifier_score"]), reverse=True)
            if len(proposals) > 1 and float(proposals[0]["verifier_score"]) == float(proposals[1]["verifier_score"]):
                counts["verifier_rejected_count"] += len(proposals)
                continue
            proposals = proposals[:1]
        item = proposals[0]
        if float(item["verifier_score"]) < threshold:
            counts["verifier_rejected_count"] += len(proposals)
            mark(proposals, "verifier_rejected")
            continue
        proposed_text = corrected if method == "macbert_verifier_cec3" else text[:start] + item["replacement"] + text[start + 1:]
        proposed_pos = target_pos if method == "macbert_verifier_cec3" else start
        tokens = target_tokens if method == "macbert_verifier_cec3" else list(jieba.tokenize(proposed_text, mode="default"))
        matches = [(word, a, b) for word, a, b in tokens if a <= proposed_pos < b]
        if len(matches) != 1:
            counts["word_invalid_count"] += 1
            mark([item], "word_invalid")
            continue
        target_word, new_start, new_end = matches[0]
        if method == "macbert_verifier_cec3":
            mapped = [new_to_old.get(index) for index in range(new_start, new_end)]
            if any(value is None for value in mapped) or mapped != list(range(mapped[0], mapped[0] + len(mapped))):
                counts["position_invalid_count"] += 1
                mark([item], "position_invalid")
                continue
            word_start, word_end = mapped[0], mapped[-1] + 1
        else:
            word_start, word_end = new_start, new_end
        original_word = text[word_start:word_end]
        if (
            not 2 <= len(target_word) <= 8
            or not re.fullmatch(r"[\u4e00-\u9fff]{2,8}", target_word)
            or not jieba.dt.FREQ.get(target_word, 0)
            or len(original_word) != len(target_word)
            or sum(a != b for a, b in zip(original_word, target_word)) != 1
            or target_word[start - word_start] != item["replacement"]
        ):
            counts["word_invalid_count"] += 1
            mark([item], "word_invalid")
            continue
        context_start, context_end = max(0, start - 12), min(len(text), start + 13)
        confirmed.append({
            **item, "matched_text": original_word, "raw_matched_text": item["original"],
            "suggestion": target_word, "highlight_text": item["original"],
            "error_type": "substitution", "display_text": text[context_start:context_end],
            "context_start": context_start, "context_end": context_end,
            "word_start": word_start, "word_end": word_end,
            "original_word": original_word, "replacement_word": target_word,
            "verification_status": "confirmed", "verification_method": method,
            "rule_version": VERSION, "word_rule_version": None,
        })
        mark([item], "eligible_for_calibration")
    counts["confirmed_count"] = len(confirmed)
    counts["hidden_count"] = len(candidates) - len(confirmed)
    return confirmed, counts
