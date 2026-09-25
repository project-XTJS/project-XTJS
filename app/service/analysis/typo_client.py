"""Local typo candidates for text that already belongs to duplicate evidence."""

from __future__ import annotations

import hashlib
import json
import urllib.request
from difflib import SequenceMatcher
from typing import Any

from app.config.settings import settings
from app.service.typo_runtime.contract import TypoUnavailable, VERSION, chunks


class DuplicateTypoService:
    def check_text_snippets_for_typos(self, snippets):
        confirmed: list[dict[str, Any]] = []
        review: list[dict[str, Any]] = []
        counts = {key: 0 for key in (
            "candidate_count", "eligible_count", "hidden_count", "budget_skipped_count",
            "verifier_rejected_count", "cec3_unsupported_count", "word_invalid_count",
            "position_invalid_count", "detector_rejected_count", "source_valid_rejected_count",
            "similarity_rejected_count",
        )}
        for snippet in snippets:
            text = str(snippet.get("text") or "")
            seen: set[tuple[str, int, int, str]] = set()
            for offset, part in self._snippet_chunks(text, snippet):
                request = urllib.request.Request(
                    settings.TYPO_SERVICE_URL.rstrip("/") + "/check",
                    data=json.dumps({"text": part}, ensure_ascii=False).encode(),
                    headers={"Content-Type": "application/json"},
                )
                try:
                    with urllib.request.urlopen(
                        request, timeout=settings.TYPO_CLIENT_TIMEOUT_SECONDS
                    ) as response:
                        payload = json.loads(response.read())
                except (OSError, ValueError) as exc:
                    raise TypoUnavailable("错别字检查未完成：模型暂不可用，请重试") from exc
                if payload.get("status") != "completed":
                    raise TypoUnavailable("错别字检查未完成")
                counts["rule_version"] = payload.get("rule_version")
                counts["verifier_model"] = payload.get("verifier_model")
                counts["detector_model"] = payload.get("detector_model")
                for key in (
                    "candidate_count", "eligible_count", "hidden_count", "budget_skipped_count",
                    "verifier_rejected_count", "cec3_unsupported_count", "word_invalid_count",
                    "position_invalid_count", "detector_rejected_count", "source_valid_rejected_count",
                    "similarity_rejected_count",
                ):
                    counts[key] += int(payload.get(key) or 0)
                for bucket_name in ("issues", "review_candidates"):
                    for raw_issue in payload.get(bucket_name) or []:
                        item = self._project_issue(
                            raw_issue,
                            offset=offset,
                            source_text=text,
                            snippet=snippet,
                            payload=payload,
                        )
                        output_bucket = bucket_name
                        if bucket_name == "issues" and not item.get("source_location_reliable"):
                            counts["hidden_count"] += 1
                            continue
                        if bucket_name == "review_candidates":
                            counts["hidden_count"] += 1
                            continue
                        key = (
                            output_bucket,
                            item["word_start"],
                            item["word_end"],
                            str(item.get("replacement_word") or item.get("replacement") or ""),
                        )
                        if key in seen:
                            continue
                        seen.add(key)
                        (confirmed if output_bucket == "issues" else review).append(item)
        return {"issues": confirmed, "review_candidates": review, "metrics": counts}

    @staticmethod
    def _snippet_chunks(text, snippet):
        units = [
            (int(value["start"]), int(value["end"]))
            for value in snippet.get("segments") or []
            if isinstance(value, dict)
            and isinstance(value.get("start"), int)
            and isinstance(value.get("end"), int)
            and 0 <= value["start"] < value["end"] <= len(text)
        ]
        if not units:
            units = [(0, len(text))]
        for unit_start, unit_end in units:
            for local_offset, part in chunks(text[unit_start:unit_end]):
                yield unit_start + local_offset, part

    @staticmethod
    def _project_issue(raw_issue, *, offset, source_text, snippet, payload):
        if not isinstance(raw_issue, dict):
            raise TypoUnavailable("错别字候选格式无效")
        item = dict(raw_issue)
        for key in ("start", "end", "word_start", "word_end", "context_start", "context_end"):
            if isinstance(item.get(key), int):
                item[key] += offset
        start, end = item.get("start"), item.get("end")
        word_start, word_end = item.get("word_start"), item.get("word_end")
        if not isinstance(start, int) or not isinstance(end, int) or source_text[start:end] != item.get("original"):
            raise TypoUnavailable("错别字定位校验失败")
        if (
            not isinstance(word_start, int)
            or not isinstance(word_end, int)
            or source_text[word_start:word_end] != item.get("original_word")
        ):
            raise TypoUnavailable("错别字词语定位校验失败")
        source_ref = dict(snippet.get("source_ref") or {})
        source_segment = next(
            (
                value
                for value in snippet.get("segments") or []
                if isinstance(value, dict)
                and isinstance(value.get("start"), int)
                and isinstance(value.get("end"), int)
                and value["start"] <= word_start
                and word_end <= value["end"]
            ),
            None,
        )
        page = source_segment.get("page") if source_segment else snippet.get("page")
        bbox = source_segment.get("bbox") if source_segment else snippet.get("bbox")
        source_location_reliable = bool(
            source_segment
            and source_segment.get("page") is not None
            and snippet.get("source_location_reliable")
        )
        item.update(
            {
                "position": start,
                "page": page,
                "bbox": bbox,
                "side": snippet.get("side"),
                "document_identifier_id": snippet.get("document_identifier_id"),
                "file_name": snippet.get("file_name"),
                "source_evidence_id": source_ref.get("evidence_id"),
                "source_kind": source_ref.get("kind"),
                "source_text_length": len(source_text),
                "source_text_hash": hashlib.sha256(source_text.encode()).hexdigest(),
                "source_segment_start": source_segment.get("start") if source_segment else None,
                "source_segment_end": source_segment.get("end") if source_segment else None,
                "source_location_reliable": source_location_reliable,
                "model": payload.get("model"),
                "reference_model": payload.get("reference_model"),
                "verifier_model": payload.get("verifier_model"),
                "detector_model": payload.get("detector_model"),
                "font_sha256": payload.get("font_sha256"),
                "pinyin_version": payload.get("pinyin_version"),
                "pillow_version": payload.get("pillow_version"),
                "prompt_version": payload.get("prompt_version"),
                "jieba_version": payload.get("jieba_version"),
                "rule_version": payload.get("rule_version") or VERSION,
                "word_rule_version": payload.get("word_rule_version"),
            }
        )
        item["locations"] = [
            {
                "document_identifier_id": snippet.get("document_identifier_id"),
                "file_name": snippet.get("file_name"),
                "page": page,
                "bbox": bbox,
                "text": item.get("original_word"),
                "highlight_phrases": [item.get("original")],
            }
        ]
        return item


def _item_word_key(item: dict[str, Any]) -> tuple[Any, ...] | None:
    word_start, word_end = item.get("word_start"), item.get("word_end")
    start, end = item.get("start"), item.get("end")
    if not all(isinstance(value, int) for value in (word_start, word_end, start, end)):
        return None
    return (
        word_start,
        word_end,
        str(item.get("original_word") or ""),
        str(item.get("replacement_word") or ""),
        start - word_start,
        end - word_start,
        str(item.get("replacement") or ""),
    )


def _occurrence(item: dict[str, Any]) -> dict[str, Any]:
    return {
        key: item.get(key)
        for key in (
            "side",
            "document_identifier_id",
            "file_name",
            "page",
            "bbox",
            "source_evidence_id",
            "source_kind",
            "source_text_length",
            "source_text_hash",
            "source_segment_start",
            "source_segment_end",
            "source_location_reliable",
            "candidate_probability",
            "source_probability",
            "probability_ratio",
            "detector_score",
            "similarity_type",
            "glyph_similarity",
            "start",
            "end",
            "word_start",
            "word_end",
        )
        if item.get(key) is not None
    }


def _occurrence_identity(value: dict[str, Any]) -> str:
    body = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(body.encode()).hexdigest()[:24]


def common_word_edits(left_text, right_text, left_issues, right_issues):
    """Return one shared result only when the full word edit aligns on both sides."""
    blocks = SequenceMatcher(None, left_text, right_text, autojunk=False).get_matching_blocks()
    right_index: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
    for item in right_issues:
        key = _item_word_key(item)
        if key is not None:
            right_index.setdefault(key, []).append(item)

    results: list[dict[str, Any]] = []
    seen: set[str] = set()
    for left in left_issues:
        key = _item_word_key(left)
        if key is None:
            continue
        word_start, word_end = key[0], key[1]
        for block in blocks:
            if not (block.a <= word_start and word_end <= block.a + block.size):
                continue
            mapped_start = block.b + word_start - block.a
            mapped_end = block.b + word_end - block.a
            right_key = (mapped_start, mapped_end, *key[2:])
            matches = right_index.get(right_key) or []
            if not matches:
                break
            right = matches[0]
            occurrences = [_occurrence(left), _occurrence(right)]
            occurrence_ids = sorted(_occurrence_identity(value) for value in occurrences)
            shared_material = "|".join(
                [
                    str(left.get("original_word") or ""),
                    str(left.get("replacement_word") or ""),
                    *occurrence_ids,
                ]
            )
            shared_id = hashlib.sha256(shared_material.encode()).hexdigest()[:32]
            if shared_id in seen:
                break
            seen.add(shared_id)
            confirmed = (
                left.get("verification_status") == "confirmed"
                and right.get("verification_status") == "confirmed"
            )
            results.append(
                {
                    "shared_id": shared_id,
                    "matched_text": left.get("original_word"),
                    "suggestion": left.get("replacement_word"),
                    "original_word": left.get("original_word"),
                    "replacement_word": left.get("replacement_word"),
                    "original": left.get("original"),
                    "replacement": left.get("replacement"),
                    "highlight_text": left.get("original"),
                    "error_type": "substitution",
                    "verification_status": "confirmed" if confirmed else "review",
                    "review_reason": None if confirmed else left.get("review_reason") or right.get("review_reason") or "side_not_confirmed",
                    "candidate_probability": min(float(left.get("candidate_probability") or 0), float(right.get("candidate_probability") or 0)),
                    "source_probability": max(float(left.get("source_probability") or 0), float(right.get("source_probability") or 0)),
                    "probability_ratio": min(float(left.get("probability_ratio") or 0), float(right.get("probability_ratio") or 0)),
                    "detector_score": min(float(left.get("detector_score") or 0), float(right.get("detector_score") or 0)) if left.get("detector_score") is not None and right.get("detector_score") is not None else None,
                    "similarity_type": left.get("similarity_type") if left.get("similarity_type") == right.get("similarity_type") else "mixed",
                    "glyph_similarity": min(float(left["glyph_similarity"]), float(right["glyph_similarity"])) if left.get("glyph_similarity") is not None and right.get("glyph_similarity") is not None else None,
                    "rule_id": left.get("rule_id") if left.get("rule_id") == right.get("rule_id") else None,
                    "rule_version": left.get("rule_version") or right.get("rule_version") or VERSION,
                    "word_rule_version": left.get("word_rule_version") or right.get("word_rule_version"),
                    "verification_method": left.get("verification_method") if left.get("verification_method") == right.get("verification_method") else None,
                    "model": left.get("model") if left.get("model") == right.get("model") else None,
                    "verifier_model": left.get("verifier_model") if left.get("verifier_model") == right.get("verifier_model") else None,
                    "detector_model": left.get("detector_model") if left.get("detector_model") == right.get("detector_model") else None,
                    "font_sha256": left.get("font_sha256") if left.get("font_sha256") == right.get("font_sha256") else None,
                    "pinyin_version": left.get("pinyin_version") if left.get("pinyin_version") == right.get("pinyin_version") else None,
                    "pillow_version": left.get("pillow_version") if left.get("pillow_version") == right.get("pillow_version") else None,
                    "reference_model": left.get("reference_model") if left.get("reference_model") == right.get("reference_model") else None,
                    "prompt_version": left.get("prompt_version") if left.get("prompt_version") == right.get("prompt_version") else None,
                    "jieba_version": left.get("jieba_version") if left.get("jieba_version") == right.get("jieba_version") else None,
                    "occurrences": occurrences,
                    "locations": list(left.get("locations") or []) + list(right.get("locations") or []),
                }
            )
            break
    return results


def common_edits(left_text, right_text, left_issues, right_issues):
    """Legacy character-level intersection retained for old callers and fixtures."""
    blocks = SequenceMatcher(None, left_text, right_text, autojunk=False).get_matching_blocks()
    right_index = {(item.get("start"), item.get("end"), item.get("replacement")): item for item in right_issues}
    result = []
    for item in left_issues:
        start, end = item.get("start"), item.get("end")
        if not isinstance(start, int) or not isinstance(end, int):
            continue
        for block in blocks:
            if block.a <= start and end <= block.a + block.size:
                other = right_index.get((block.b + start - block.a, block.b + end - block.a, item.get("replacement")))
                if other is not None:
                    result.extend([item, other])
                break
    return result
