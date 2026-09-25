"""Original-first, spelling-only verification for the v5 pipeline."""

from __future__ import annotations

import math
import re
from difflib import SequenceMatcher
from functools import lru_cache
from pathlib import Path
from typing import Any

VERSION = "duplicate-typo-original-detector-v5"
HANZI = re.compile(r"[\u4e00-\u9fff]")
WORD = re.compile(r"[\u4e00-\u9fff]{2,8}")


def _maps(before: str, after: str):
    """Map unchanged spans and equal-length replacements, never guess across shifts."""
    old_to_new: dict[int, int] = {}
    new_to_old: dict[int, int] = {}
    for tag, a, b, c, d in SequenceMatcher(None, before, after, autojunk=False).get_opcodes():
        if tag == "equal" or (tag == "replace" and b - a == d - c):
            for old, new in zip(range(a, b), range(c, d)):
                old_to_new[old] = new
                new_to_old[new] = old
    return old_to_new, new_to_old


def _stable_maps(before: str, after: str):
    """Keep only code-point mappings identical under forward/reverse alignment."""
    forward, _ = _maps(before, after)
    reverse, _ = _maps(before[::-1], after[::-1])
    stable = {
        old: new for old, new in forward.items()
        if reverse.get(len(before) - old - 1) == len(after) - new - 1
    }
    return stable, {new: old for old, new in stable.items()}


class ShapeSoundGate:
    """Deterministic pronunciation or fixed-font glyph evidence; never a typo detector."""

    def __init__(self, font_path: str | Path, *, glyph_threshold: float):
        from PIL import ImageFont

        self.font_path = Path(font_path)
        if not self.font_path.is_file() or not 0 <= glyph_threshold <= 1:
            raise ValueError("v5_font_or_glyph_threshold_invalid")
        self.font = ImageFont.truetype(str(self.font_path), 72)
        self.glyph_threshold = float(glyph_threshold)

    @lru_cache(maxsize=8192)
    def _glyph(self, character: str) -> tuple[int, ...] | None:
        from PIL import Image, ImageDraw

        bounds = self.font.getbbox(character)
        if not bounds or bounds[2] <= bounds[0] or bounds[3] <= bounds[1]:
            return None
        image = Image.new("L", (bounds[2] - bounds[0], bounds[3] - bounds[1]), 0)
        ImageDraw.Draw(image).text((-bounds[0], -bounds[1]), character, font=self.font, fill=255)
        bbox = image.getbbox()
        if not bbox:
            return None
        image = image.crop(bbox)
        image.thumbnail((56, 56), Image.Resampling.LANCZOS)
        canvas = Image.new("L", (64, 64), 0)
        canvas.paste(image, ((64 - image.width) // 2, (64 - image.height) // 2))
        return tuple(canvas.get_flattened_data())

    def evidence(self, original: str, replacement: str) -> dict[str, Any] | None:
        from pypinyin import Style, pinyin

        if not HANZI.fullmatch(original) or not HANZI.fullmatch(replacement):
            return None
        source_readings = pinyin(original, style=Style.NORMAL, heteronym=True, errors="ignore")
        target_readings = pinyin(replacement, style=Style.NORMAL, heteronym=True, errors="ignore")
        source_sounds = set(source_readings[0]) if source_readings else set()
        target_sounds = set(target_readings[0]) if target_readings else set()
        same_sound = bool(source_sounds & target_sounds)
        a, b = self._glyph(original), self._glyph(replacement)
        similarity = None
        if a is not None and b is not None:
            numerator = sum(x * y for x, y in zip(a, b))
            denominator = math.sqrt(sum(x * x for x in a) * sum(y * y for y in b))
            similarity = numerator / denominator if denominator else None
        similar_shape = similarity is not None and similarity >= self.glyph_threshold
        if not same_sound and not similar_shape:
            return None
        return {
            "similarity_type": "both" if same_sound and similar_shape else "pinyin" if same_sound else "glyph",
            "glyph_similarity": similarity,
        }


def select_candidates(candidates: list[dict[str, Any]], detector_scores: list[float | None], *, threshold: float, limit: int = 8, source_text: str | None = None):
    """Only detector-positive positions enter correction; count rejected/over-budget items."""
    selected = []
    rejected = 0
    source_valid_rejected = 0
    known_source_positions = set()
    if source_text is not None:
        import jieba

        jieba.initialize()
        for token, begin, end in jieba.tokenize(source_text, mode="default"):
            if len(token) >= 2 and jieba.dt.FREQ.get(token, 0):
                known_source_positions.update(range(begin, end))
    for candidate in candidates:
        start = int(candidate["start"])
        score = detector_scores[start] if start < len(detector_scores) else None
        if candidate.get("protected_span") or score is None or score < threshold:
            rejected += 1
            continue
        if start in known_source_positions:
            source_valid_rejected += 1
            continue
        selected.append({**candidate, "detector_score": float(score)})
    selected.sort(key=lambda item: (-float(item["probability_ratio"]), -float(item["candidate_probability"]), item["start"]))
    return selected[:limit], rejected, max(0, len(selected) - limit), source_valid_rejected


def classify_v5(
    text: str,
    candidates: list[dict[str, Any]],
    corrected: str,
    roundtrip: str,
    *,
    gate: ShapeSoundGate,
    reasons: dict[tuple[int, str], str] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Confirm one-character typo only after independent original and correction evidence."""
    import jieba

    jieba.initialize()
    if not isinstance(corrected, str) or not isinstance(roundtrip, str):
        raise ValueError("invalid_cec3_output")
    old_to_new, new_to_old = _stable_maps(text, corrected)
    corrected_to_roundtrip, _ = _stable_maps(corrected, roundtrip)
    tokens = list(jieba.tokenize(corrected, mode="default"))
    counts = {key: 0 for key in (
        "source_valid_rejected_count", "word_invalid_count", "similarity_rejected_count",
        "cec3_unsupported_count", "position_invalid_count",
    )}
    by_position: dict[int, list[dict[str, Any]]] = {}
    for item in candidates:
        by_position.setdefault(int(item["start"]), []).append(item)
    confirmed = []
    for start, proposals in by_position.items():
        def mark(items, reason):
            if reasons is not None:
                for proposal in items:
                    reasons[(int(proposal["start"]), str(proposal["replacement"]))] = reason

        target_pos = old_to_new.get(start)
        if target_pos is None or new_to_old.get(target_pos) != start:
            counts["position_invalid_count"] += len(proposals)
            mark(proposals, "position_invalid")
            continue
        replacement = corrected[target_pos]
        matches = [item for item in proposals if item["replacement"] == replacement]
        if replacement == text[start] or not HANZI.fullmatch(replacement) or len(matches) != 1:
            counts["cec3_unsupported_count"] += len(proposals)
            mark(proposals, "cec3_unsupported")
            continue
        second_pos = corrected_to_roundtrip.get(target_pos)
        if second_pos is None or roundtrip[second_pos] != replacement:
            counts["cec3_unsupported_count"] += len(proposals)
            mark(proposals, "cec3_unsupported")
            continue
        item = matches[0]
        mark([proposal for proposal in proposals if proposal is not item], "cec3_unsupported")
        containing = [(word, a, b) for word, a, b in tokens if a <= target_pos < b]
        if len(containing) != 1:
            counts["word_invalid_count"] += 1
            mark([item], "word_invalid")
            continue
        target_word, new_start, new_end = containing[0]
        mapped = [new_to_old.get(index) for index in range(new_start, new_end)]
        if not mapped or any(value is None for value in mapped) or mapped != list(range(mapped[0], mapped[0] + len(mapped))):
            counts["position_invalid_count"] += 1
            mark([item], "position_invalid")
            continue
        word_start, word_end = mapped[0], mapped[-1] + 1
        source_word = text[word_start:word_end]
        if (
            not WORD.fullmatch(target_word)
            or not jieba.dt.FREQ.get(target_word, 0)
            or len(source_word) != len(target_word)
            or sum(a != b for a, b in zip(source_word, target_word)) != 1
            or source_word[start - word_start] != item["original"]
            or target_word[start - word_start] != replacement
        ):
            counts["word_invalid_count"] += 1
            mark([item], "word_invalid")
            continue
        if jieba.dt.FREQ.get(source_word, 0):
            counts["source_valid_rejected_count"] += 1
            mark([item], "source_valid")
            continue
        similarity = gate.evidence(item["original"], replacement)
        if similarity is None:
            counts["similarity_rejected_count"] += 1
            mark([item], "similarity_rejected")
            continue
        context_start, context_end = max(0, start - 12), min(len(text), start + 13)
        confirmed.append({
            **item, **similarity,
            "matched_text": source_word, "raw_matched_text": item["original"],
            "suggestion": target_word, "highlight_text": item["original"],
            "error_type": "substitution", "display_text": text[context_start:context_end],
            "context_start": context_start, "context_end": context_end,
            "word_start": word_start, "word_end": word_end,
            "original_word": source_word, "replacement_word": target_word,
            "verification_status": "confirmed", "verification_method": "original_detector_macbert_cec3",
            "rule_version": VERSION, "word_rule_version": None,
        })
        mark([item], "confirmed")
    return confirmed, counts
