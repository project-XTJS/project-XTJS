#!/usr/bin/env python3
"""Train an original-text-only character detector on public train/dev splits.

The model never sees a proposed replacement. Ambiguous edits are masked from loss.
Run only in the isolated, CPU-limited v5 training container.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import re
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
os.environ["CUDA_VISIBLE_DEVICES"] = ""
import jieba
import torch
import torch.nn.functional as F
from torch.utils.data import DataLoader, Dataset
from transformers import BertForTokenClassification, BertTokenizerFast

from app.service.typo_runtime.contract import protected_spans

HANZI = re.compile(r"[\u4e00-\u9fff]")


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_split(path: Path, split: str, maximum: int | None) -> list[dict]:
    rows = []
    with path.open(encoding="utf-8") as stream:
        for line in stream:
            if not line.strip():
                continue
            row = json.loads(line)
            if row.get("split") != split:
                continue
            if not isinstance(row.get("text"), str) or not isinstance(row.get("target"), str):
                raise ValueError("invalid_public_record")
            rows.append(row)
            if maximum and len(rows) >= maximum:
                break
    if not rows:
        raise ValueError(f"empty_{split}_split")
    return rows


def scope_labels(row: dict, counters: Counter) -> list[int]:
    """1=spelling error, 0=correct/legal source, -100=uncertain alignment."""
    text, target = row["text"], row["target"]
    labels = [0 if HANZI.fullmatch(character) else -100 for character in text]
    if len(text) != len(target):
        counters["unaligned_rows"] += 1
        return [-100] * len(text)
    changed = [index for index, (a, b) in enumerate(zip(text, target)) if a != b]
    if not changed:
        counters["clean_rows"] += 1
        return labels
    spans = protected_spans(text)
    tokens = list(jieba.tokenize(target, mode="default"))
    for index in changed:
        if not HANZI.fullmatch(text[index]) or not HANZI.fullmatch(target[index]):
            labels[index] = -100
            counters["non_hanzi_edits"] += 1
            continue
        if any(a <= index < b for a, b in spans):
            labels[index] = -100
            counters["protected_edits"] += 1
            continue
        matches = [(word, a, b) for word, a, b in tokens if a <= index < b]
        if len(matches) != 1:
            labels[index] = -100
            counters["uncertain_boundary"] += 1
            continue
        word, start, end = matches[0]
        source_word = text[start:end]
        if not (2 <= len(word) <= 8 and all(HANZI.fullmatch(char) for char in word)
                and jieba.dt.FREQ.get(word, 0) and sum(a != b for a, b in zip(source_word, word)) == 1):
            labels[index] = -100
            counters["uncertain_word"] += 1
        elif jieba.dt.FREQ.get(source_word, 0):
            labels[index] = 0
            counters["legal_word_negative"] += 1
        else:
            labels[index] = 1
            counters["spelling_positive"] += 1
    return labels


class TextDataset(Dataset):
    def __init__(self, rows: list[dict], tokenizer: BertTokenizerFast, counters: Counter):
        self.examples = []
        for row in rows:
            labels = scope_labels(row, counters)
            text = row["text"]
            for offset in range(0, len(text), 240):
                part = text[offset:offset + 240]
                if not part:
                    continue
                encoded = tokenizer(part, return_offsets_mapping=True, truncation=False)
                if len(encoded["input_ids"]) > 256:
                    counters["overlong_chunks"] += 1
                    continue
                offsets = encoded.pop("offset_mapping")
                encoded["labels"] = [
                    labels[offset + a] if b == a + 1 else -100 for a, b in offsets
                ]
                self.examples.append(encoded)

    def __len__(self):
        return len(self.examples)

    def __getitem__(self, index):
        return self.examples[index]


def collate(batch, tokenizer):
    labels = [item["labels"] for item in batch]
    encoded = tokenizer.pad([{k: v for k, v in item.items() if k != "labels"} for item in batch], return_tensors="pt")
    encoded["labels"] = torch.tensor([row + [-100] * (encoded["input_ids"].shape[1] - len(row)) for row in labels])
    return encoded


def average_precision(labels, scores):
    positives = sum(labels)
    if not positives:
        return 0.0
    found = total = 0.0
    for rank, index in enumerate(sorted(range(len(scores)), key=lambda i: scores[i], reverse=True), 1):
        if labels[index]:
            found += 1
            total += found / rank
    return total / positives


def evaluate(model, loader):
    labels, scores = [], []
    model.eval()
    with torch.inference_mode():
        for batch in loader:
            target = batch.pop("labels")
            probabilities = model(**batch).logits.softmax(dim=-1)[:, :, 1]
            valid = target != -100
            labels.extend(target[valid].tolist())
            scores.extend(probabilities[valid].tolist())
    return average_precision(labels, scores), len(labels), sum(labels)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", type=Path, required=True)
    parser.add_argument("--base-model", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--max-train-records", type=int)
    parser.add_argument("--max-dev-records", type=int)
    parser.add_argument("--batch-size", type=int, default=8)
    parser.add_argument("--epochs", type=int, default=2)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    if not 1 <= args.epochs <= 2 or args.batch_size < 1:
        parser.error("epochs must be 1..2 and batch-size positive")
    if args.output.exists() and any(args.output.iterdir()):
        parser.error("output must be empty and independent")
    random.seed(args.seed)
    torch.manual_seed(args.seed)
    torch.set_num_threads(2)
    jieba.initialize()
    train_rows = read_split(args.dataset, "train", args.max_train_records)
    dev_rows = read_split(args.dataset, "dev", args.max_dev_records)
    for key in ("group_id", "normalized_text_sha256"):
        train_keys = {row.get(key) for row in train_rows if row.get(key)}
        dev_keys = {row.get(key) for row in dev_rows if row.get(key)}
        if train_keys & dev_keys:
            raise ValueError(f"train_dev_leakage:{key}")
    tokenizer = BertTokenizerFast.from_pretrained(args.base_model, local_files_only=True)
    train_counts, dev_counts = Counter(), Counter()
    train = TextDataset(train_rows, tokenizer, train_counts)
    dev = TextDataset(dev_rows, tokenizer, dev_counts)
    if not train_counts["spelling_positive"] or not dev_counts["spelling_positive"]:
        raise ValueError("no_in_scope_spelling_examples")
    pack = lambda items: collate(items, tokenizer)
    train_loader = DataLoader(train, batch_size=args.batch_size, shuffle=True, collate_fn=pack)
    dev_loader = DataLoader(dev, batch_size=args.batch_size, collate_fn=pack)
    model = BertForTokenClassification.from_pretrained(args.base_model, num_labels=2, local_files_only=True)
    optimizer = torch.optim.AdamW(model.parameters(), lr=2e-5, weight_decay=0.01)
    history, best = [], -1.0
    for epoch in range(args.epochs):
        model.train()
        for step, batch in enumerate(train_loader, 1):
            optimizer.zero_grad(set_to_none=True)
            labels = batch.pop("labels")
            logits = model(**batch).logits
            per_token = F.cross_entropy(logits.reshape(-1, 2), labels.reshape(-1), reduction="none", ignore_index=-100)
            weights = torch.where(labels == 1, 3.0, 1.0).reshape(-1)
            loss = (per_token * weights).sum() / (weights * (labels.reshape(-1) != -100)).sum().clamp(min=1)
            loss.backward()
            torch.nn.utils.clip_grad_norm_(model.parameters(), 1.0)
            optimizer.step()
            if step % 500 == 0:
                print(json.dumps({"epoch": epoch + 1, "step": step, "total_steps": len(train_loader)}), flush=True)
        ap, positions, positives = evaluate(model, dev_loader)
        history.append({"epoch": epoch + 1, "dev_ap": ap, "dev_positions": positions, "dev_positive": positives})
        print(json.dumps(history[-1]), flush=True)
        if ap > best:
            best = ap
            args.output.mkdir(parents=True, exist_ok=True)
            model.save_pretrained(args.output, safe_serialization=True)
            tokenizer.save_pretrained(args.output)
    manifest = {
        "model": "macbert-v5-original-detector", "revision": f"public-{args.seed}-{len(train_rows)}-{len(dev_rows)}",
        "model_sha256": sha256(args.output / "model.safetensors"),
        "tokenizer_sha256": sha256(args.output / "tokenizer.json"),
        "dataset_sha256": sha256(args.dataset), "base_model": str(args.base_model),
        "train_records": len(train_rows), "dev_records": len(dev_rows),
        "train_counts": dict(train_counts), "dev_counts": dict(dev_counts),
        "history": history, "test_split_used": False, "original_only": True,
    }
    (args.output / "model-manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(manifest, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
