#!/usr/bin/env python3
"""Print compact, indexed public DEV edit contexts for manual scope review."""

import argparse
import json
from pathlib import Path

import jieba


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("draft", type=Path)
    parser.add_argument("--category", choices=("spelling", "legal_word", "uncertain"), required=True)
    parser.add_argument("--offset", type=int, default=0)
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--radius", type=int, default=13)
    args = parser.parse_args()
    jieba.initialize()
    matches = []
    for line in args.draft.open(encoding="utf-8"):
        row = json.loads(line)
        target = row["target"]
        tokens = list(jieba.tokenize(target))
        for item in row.get("scope_annotations") or []:
            if item["draft_category"] != args.category:
                continue
            start = item["start"]
            token = next(((word, a, b) for word, a, b in tokens if a <= start < b), None)
            original_word = row["text"][token[1]:token[2]] if token else "?"
            target_word = token[0] if token else "?"
            context = row["text"][max(0, start - args.radius):min(len(row["text"]), start + args.radius + 1)].replace("\n", " ")
            matches.append((row["project_id"], start, original_word, target_word, context))
    for index, (project, start, original, target, context) in enumerate(matches[args.offset:args.offset + args.limit], args.offset):
        print(f"{index:04d} {project}@{start} {original}→{target} | {context}")
    print(f"COUNT={len(matches)}")


if __name__ == "__main__":
    main()
