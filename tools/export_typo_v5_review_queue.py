#!/usr/bin/env python3
"""Export public DEV spelling-scope decisions requiring human review."""

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from tools.evaluate_duplicate_typo_candidates import load_records


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("draft", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("人工复核队列已存在，拒绝覆盖")
    rows = load_records(args.draft)
    if {row["split"] for row in rows} != {"dev"}:
        parser.error("只允许从公开开发集草稿生成队列")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with args.output.open("x", encoding="utf-8") as stream:
        for row in rows:
            for item in row.get("scope_annotations") or []:
                if item.get("review_source") == "human":
                    continue
                start = int(item["start"])
                stream.write(json.dumps({
                    "project_id": row["project_id"], "source": row.get("source"),
                    "start": start, "original": item["original"],
                    "replacement": item["replacement"],
                    "proposed_category": item["draft_category"],
                    "agent_category": item["category"] if item.get("review_source") == "agent" else None,
                    "context": row["text"][max(0, start - 24):min(len(row["text"]), start + 25)],
                    "instruction": "人工核对后新增 category=spelling/legal_word/uncertain",
                }, ensure_ascii=False) + "\n")
                count += 1
    print(json.dumps({"review_items": count, "output": str(args.output)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
