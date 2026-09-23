#!/usr/bin/env python3
"""Extract public word-pair review items and compile dual-approved rules."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from collections import defaultdict
from pathlib import Path


HAN_WORD = re.compile(r"[\u4e00-\u9fff]{2,6}")


def read_jsonl(path: Path):
    with path.open(encoding="utf-8") as stream:
        for raw in stream:
            if raw.strip():
                yield json.loads(raw)


def extract(args: argparse.Namespace) -> None:
    try:
        import jieba
    except ImportError as exc:
        raise RuntimeError(
            "提取完整词语需要 jieba；请安装 requirements-typo-training.txt"
        ) from exc
    aggregates = defaultdict(
        lambda: {"count": 0, "sources": defaultdict(int), "examples": []}
    )
    for record in read_jsonl(args.dataset):
        if record.get("split") != args.split or record.get("is_correct"):
            continue
        text, target = record["text"], record["target"]
        tokens = list(jieba.tokenize(target, mode="default"))
        for edit in record.get("expected") or []:
            position = int(edit["start"])
            matches = [
                (word, start, end)
                for word, start, end in tokens
                if start <= position < end and 2 <= end - start <= 6
            ]
            if not matches:
                continue
            target_word, start, end = min(matches, key=lambda item: item[2] - item[1])
            source_word = text[start:end]
            if not HAN_WORD.fullmatch(source_word) or not HAN_WORD.fullmatch(target_word):
                continue
            differences = [
                index
                for index, pair in enumerate(zip(source_word, target_word))
                if pair[0] != pair[1]
            ]
            if differences != [position - start]:
                continue
            item = aggregates[(source_word, target_word)]
            item["count"] += 1
            item["sources"][record.get("source") or "unknown"] += 1
            if len(item["examples"]) < 5:
                context_start, context_end = max(0, start - 16), min(len(text), end + 16)
                item["examples"].append(
                    {
                        "project_id": record["project_id"],
                        "context": text[context_start:context_end],
                        "corrected_context": target[context_start:context_end],
                        "word_start": start,
                        "word_end": end,
                    }
                )
    ranked = sorted(
        aggregates.items(),
        key=lambda pair: (-pair[1]["count"], pair[0][1], pair[0][0]),
    )[: args.limit]
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8") as stream:
        for (source, target), metadata in ranked:
            identifier = hashlib.sha256(f"{source}\0{target}".encode()).hexdigest()[:12]
            payload = {
                "id": f"public-{identifier}",
                "source": source,
                "target": target,
                "occurrences": metadata["count"],
                "sources": dict(sorted(metadata["sources"].items())),
                "examples": metadata["examples"],
                "checks": {
                    "single_character_substitution": True,
                    "source_dictionary_frequency": int(jieba.dt.FREQ.get(source) or 0),
                    "target_dictionary_frequency": int(jieba.dt.FREQ.get(target) or 0),
                    "source_is_known_word": source in jieba.dt.FREQ,
                },
                "first_review": {"status": "pending", "reviewer": None, "reason": None},
                "second_review": {"status": "pending", "reviewer": None, "reason": None},
            }
            stream.write(json.dumps(payload, ensure_ascii=False) + "\n")
    print(json.dumps({"candidates": len(ranked), "output": str(args.output)}, ensure_ascii=False))


def approved(review: dict) -> bool:
    first = review.get("first_review") or {}
    second = review.get("second_review") or {}
    return (
        first.get("status") == "approved"
        and second.get("status") == "approved"
        and str(first.get("reviewer") or "").strip()
        and str(second.get("reviewer") or "").strip()
        and first.get("reviewer") != second.get("reviewer")
    )


def compile_rules(args: argparse.Namespace) -> None:
    base = json.loads(args.base_rules.read_text(encoding="utf-8"))
    protected = {str(value) for value in base.get("protected_terms") or []}
    corrections = {
        (item["source"], item["target"]): dict(item)
        for item in base.get("approved_corrections") or []
    }
    proposed = []
    for item in read_jsonl(args.review_file):
        if not approved(item):
            continue
        source, target = str(item.get("source") or ""), str(item.get("target") or "")
        if not HAN_WORD.fullmatch(source) or not HAN_WORD.fullmatch(target) or len(source) != len(target):
            raise ValueError(f"无效审核词对：{source}->{target}")
        if sum(old != new for old, new in zip(source, target)) != 1:
            raise ValueError(f"审核词对不是单字替换：{source}->{target}")
        if any(term in source or term in target for term in protected):
            raise ValueError(f"审核词对与保护词冲突：{source}->{target}")
        proposed.append(item)
    targets_by_source = defaultdict(set)
    for item in proposed:
        targets_by_source[item["source"]].add(item["target"])
    ambiguous = [source for source, targets in targets_by_source.items() if len(targets) > 1]
    if ambiguous:
        raise ValueError("同一原词存在多个改法：" + "、".join(sorted(ambiguous)))
    for item in proposed:
        conditions = item.get("conditions") or {}
        if not isinstance(conditions, dict) or any(
            not isinstance(conditions.get(key, []), list)
            or any(not isinstance(value, str) or not value for value in conditions.get(key, []))
            for key in (
                "required_left",
                "required_right",
                "forbidden_left",
                "forbidden_right",
            )
        ):
            raise ValueError(f"审核词对上下文条件无效：{item['source']}->{item['target']}")
        correction = {
            "id": item["id"],
            "source": item["source"],
            "target": item["target"],
            "review": {
                "first": item["first_review"]["reviewer"],
                "second": item["second_review"]["reviewer"],
                "evidence_occurrences": item.get("occurrences", 0),
            },
        }
        if conditions:
            correction["conditions"] = conditions
        corrections[(item["source"], item["target"])] = correction
    output = {
        "version": args.version,
        "protected_terms": sorted(protected),
        "approved_corrections": sorted(
            corrections.values(), key=lambda item: (item["source"], item["target"])
        ),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(output, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps({"approved_corrections": len(corrections), "output": str(args.output)}, ensure_ascii=False))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    extract_parser = subparsers.add_parser("extract")
    extract_parser.add_argument("dataset", type=Path)
    extract_parser.add_argument("--split", default="train")
    extract_parser.add_argument("--limit", type=int, default=200)
    extract_parser.add_argument("--output", type=Path, required=True)
    extract_parser.set_defaults(run=extract)
    compile_parser = subparsers.add_parser("compile")
    compile_parser.add_argument("--base-rules", type=Path, required=True)
    compile_parser.add_argument("--review-file", type=Path, required=True)
    compile_parser.add_argument("--version", required=True)
    compile_parser.add_argument("--output", type=Path, required=True)
    compile_parser.set_defaults(run=compile_rules)
    args = parser.parse_args()
    args.run(args)


if __name__ == "__main__":
    main()
