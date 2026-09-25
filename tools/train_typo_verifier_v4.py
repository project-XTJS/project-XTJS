#!/usr/bin/env python3
"""CPU-only public-data verifier training; never modifies the deployed model."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
from pathlib import Path

os.environ["CUDA_VISIBLE_DEVICES"] = ""
import torch
from torch.utils.data import DataLoader
from transformers import BertForSequenceClassification, BertTokenizerFast

from app.service.typo_runtime.contract import protected_spans
from app.service.typo_runtime.worker import MacBertWorker


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def context_pair(text: str, start: int, replacement: str) -> tuple[str, str]:
    left, right = max(0, start - 48), min(len(text), start + 49)
    prefix, suffix = text[left:start] + "[unused1]", "[unused2]" + text[start + 1:right]
    return prefix + text[start] + suffix, prefix + replacement + suffix


def load_records(path: Path, split: str, max_records: int | None):
    rows = []
    for line in path.open(encoding="utf-8"):
        if not line.strip():
            continue
        row = json.loads(line)
        if row.get("split") != split:
            continue
        if not isinstance(row.get("text"), str) or not isinstance(row.get("target"), str) or len(row["text"]) != len(row["target"]):
            raise ValueError("non-aligned public record")
        rows.append(row)
        if max_records and len(rows) >= max_records:
            break
    if not rows:
        raise ValueError(f"empty split: {split}")
    return rows


def build_examples(rows, worker: MacBertWorker, *, split: str):
    examples = []
    for row_index, row in enumerate(rows, 1):
        text, target = row["text"], row["target"]
        gold = {index: character for index, character in enumerate(target) if character != text[index]}
        spans = protected_spans(text)
        candidates = []
        for offset in range(0, len(text), 240):
            part = text[offset:offset + 240]
            for item in worker.analyze(part, 0.10, 0.30, 2)["candidates"]:
                candidates.append((offset + item["start"], item["replacement"]))
        candidates.extend(gold.items())
        seen = set()
        for start, replacement in candidates:
            if (start, replacement) in seen or any(a <= start < b for a, b in spans):
                continue
            seen.add((start, replacement))
            source, proposed = context_pair(text, start, replacement)
            examples.append({
                "source": source, "proposed": proposed,
                "label": int(gold.get(start) == replacement),
                "project_id": row["project_id"], "group_id": row.get("group_id"),
                "source_dataset": row.get("source"),
            })
        if row_index % 1000 == 0:
            print(json.dumps({"stage": "candidate_generation", "split": split,
                              "rows_done": row_index, "rows_total": len(rows),
                              "examples": len(examples)}, ensure_ascii=False), flush=True)
    return examples


def average_precision(labels: list[int], scores: list[float]) -> float:
    positives = sum(labels)
    if not positives:
        return 0.0
    true_positives = 0
    precision_sum = 0.0
    for rank, index in enumerate(sorted(range(len(scores)), key=lambda i: scores[i], reverse=True), 1):
        if labels[index]:
            true_positives += 1
            precision_sum += true_positives / rank
    return precision_sum / positives


def collator(tokenizer):
    def pack(batch):
        encoded = tokenizer(
            [item["source"] for item in batch], [item["proposed"] for item in batch],
            padding=True, truncation=True, max_length=256, return_tensors="pt",
        )
        encoded["labels"] = torch.tensor([item["label"] for item in batch], dtype=torch.long)
        return encoded
    return pack


def evaluate(model, loader):
    labels, scores = [], []
    model.eval()
    with torch.inference_mode():
        for batch in loader:
            labels.extend(batch.pop("labels").tolist())
            scores.extend(model(**batch).logits.softmax(-1)[:, 1].tolist())
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
    if args.epochs < 1 or args.epochs > 2:
        parser.error("epochs must be 1 or 2")
    if args.output.exists() and any(args.output.iterdir()):
        parser.error("output must be an empty, independent directory")
    random.seed(args.seed)
    torch.manual_seed(args.seed)
    torch.set_num_threads(2)
    train_rows = load_records(args.dataset, "train", args.max_train_records)
    dev_rows = load_records(args.dataset, "dev", args.max_dev_records)
    if {r.get("group_id") for r in train_rows} & {r.get("group_id") for r in dev_rows}:
        raise ValueError("train/dev group leakage")
    if {r.get("normalized_text_sha256") for r in train_rows if r.get("normalized_text_sha256")} & {
        r.get("normalized_text_sha256") for r in dev_rows if r.get("normalized_text_sha256")
    }:
        raise ValueError("train/dev exact-text leakage")
    worker = MacBertWorker(str(args.base_model), device="cpu")
    train = build_examples(train_rows, worker, split="train")
    dev = build_examples(dev_rows, worker, split="dev")
    del worker
    if not train or not dev or not any(x["label"] for x in train) or not any(x["label"] for x in dev):
        raise ValueError("insufficient positive verifier examples")
    tokenizer = BertTokenizerFast.from_pretrained(args.base_model, local_files_only=True)
    tokenizer.add_special_tokens({"additional_special_tokens": ["[unused1]", "[unused2]"]})
    pack = collator(tokenizer)
    train_loader = DataLoader(train, batch_size=args.batch_size, shuffle=True, collate_fn=pack)
    dev_loader = DataLoader(dev, batch_size=args.batch_size, collate_fn=pack)
    model = BertForSequenceClassification.from_pretrained(args.base_model, num_labels=2, local_files_only=True)
    optimizer = torch.optim.AdamW(model.parameters(), lr=2e-5, weight_decay=0.01)
    best = -1.0
    history = []
    for epoch in range(args.epochs):
        model.train()
        for batch in train_loader:
            optimizer.zero_grad(set_to_none=True)
            model(**batch).loss.backward()
            torch.nn.utils.clip_grad_norm_(model.parameters(), 1.0)
            optimizer.step()
        ap, count, positives = evaluate(model, dev_loader)
        history.append({"epoch": epoch + 1, "dev_average_precision": ap, "dev_candidates": count, "dev_positive": positives})
        if ap > best:
            best = ap
            args.output.mkdir(parents=True, exist_ok=True)
            model.save_pretrained(args.output, safe_serialization=True)
            tokenizer.save_pretrained(args.output)
    model_file = args.output / "model.safetensors"
    manifest = {
        "model": "macbert-v4-candidate-verifier", "revision": f"public-{args.seed}-{len(train_rows)}-{len(dev_rows)}",
        "model_sha256": sha256(model_file), "dataset_sha256": sha256(args.dataset),
        "tokenizer_sha256": sha256(args.output / "tokenizer.json"),
        "base_model": str(args.base_model), "train_records": len(train_rows),
        "dev_records": len(dev_rows), "train_candidates": len(train), "dev_candidates": len(dev),
        "history": history, "test_split_used": False,
    }
    (args.output / "model-manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(manifest, ensure_ascii=False))


if __name__ == "__main__":
    main()
