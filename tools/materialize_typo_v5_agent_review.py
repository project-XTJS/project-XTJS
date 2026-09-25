#!/usr/bin/env python3
"""Materialize auditable AI/policy DEV decisions; never mark them human-confirmed."""

from __future__ import annotations

import argparse
import hashlib
import json
from collections import Counter
from pathlib import Path

import jieba


def build(draft: Path, notes: dict):
    digest = hashlib.sha256(draft.read_bytes()).hexdigest()
    if digest != notes["source_sha256"]:
        raise ValueError("review_notes_source_hash_mismatch")
    spelling_to_legal = set(notes["spelling_draft_to_legal_word_indices"])
    spelling_to_uncertain = set(notes["spelling_draft_to_uncertain_indices"])
    if spelling_to_legal & spelling_to_uncertain:
        raise ValueError("conflicting_spelling_review_indices")
    false_dictionary_pairs = {tuple(pair) for pair in notes["legal_draft_to_spelling_pairs"]}
    matched_pairs = set()
    decisions = []
    category_indices = Counter()
    totals = Counter()
    for line in draft.open(encoding="utf-8"):
        row = json.loads(line)
        if row["split"] != "dev":
            raise ValueError("agent_review_dev_only")
        target_tokens = list(jieba.tokenize(row["target"]))
        for edit in row.get("scope_annotations") or []:
            draft_category = edit["draft_category"]
            index = category_indices[draft_category]
            category_indices[draft_category] += 1
            start = int(edit["start"])
            token = next(((word, a, b) for word, a, b in target_tokens if a <= start < b), None)
            source_word = row["text"][token[1]:token[2]] if token else None
            target_word = token[0] if token else None
            pair = (source_word, target_word)
            category, method, rationale = draft_category, "scope_policy", None
            if draft_category == "spelling":
                method = "individual_context_review"
                if index in spelling_to_legal:
                    category = "legal_word"
                elif index in spelling_to_uncertain:
                    category = "uncertain"
            elif draft_category == "legal_word" and pair in false_dictionary_pairs:
                category = "spelling"
                method = "individual_pair_review"
                rationale = "dictionary_false_positive"
                matched_pairs.add(pair)
            decisions.append({
                "project_id": row["project_id"], "start": start,
                "category": category, "draft_category": draft_category,
                "original": edit["original"], "replacement": edit["replacement"],
                "original_word": source_word, "replacement_word": target_word,
                "method": method, "rationale": rationale,
                "review_source": "agent",
            })
            totals[f"{draft_category}_to_{category}"] += 1
    if category_indices != {"spelling": 632, "legal_word": 952, "uncertain": 967}:
        raise ValueError(f"unexpected_review_queue_counts: {category_indices}")
    if (spelling_to_legal | spelling_to_uncertain) - set(range(category_indices["spelling"])):
        raise ValueError("spelling_review_index_out_of_range")
    if false_dictionary_pairs != matched_pairs:
        raise ValueError(f"unmatched_dictionary_false_positive_pairs: {false_dictionary_pairs - matched_pairs}")
    if len({(row["project_id"], row["start"]) for row in decisions}) != len(decisions):
        raise ValueError("duplicate_review_key")
    return decisions, dict(totals)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("draft", type=Path)
    parser.add_argument("notes", type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("复核输出已存在，拒绝覆盖")
    notes = json.loads(args.notes.read_text(encoding="utf-8"))
    decisions, counts = build(args.draft, notes)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x", encoding="utf-8") as stream:
        for decision in decisions:
            stream.write(json.dumps(decision, ensure_ascii=False) + "\n")
    print(json.dumps({"items": len(decisions), "counts": counts,
                      "output_sha256": hashlib.sha256(args.output.read_bytes()).hexdigest()},
                     ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
