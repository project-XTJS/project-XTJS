#!/usr/bin/env python3
"""Fine-tune the deployed MacBERT CSC model on frozen public JSONL data."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import random
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import torch
import torch.nn.functional as F
import transformers
from torch.utils.data import DataLoader, Dataset
from transformers import BertForMaskedLM, BertTokenizerFast, get_linear_schedule_with_warmup


HAN = re.compile(r"[\u4e00-\u9fff]")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_jsonl(path: Path, split: str) -> list[dict]:
    records = []
    with path.open(encoding="utf-8") as stream:
        for line_number, raw in enumerate(stream, 1):
            if not raw.strip():
                continue
            value = json.loads(raw)
            if value.get("split") != split:
                continue
            text, target = value.get("text"), value.get("target")
            if not isinstance(text, str) or not isinstance(target, str) or len(text) != len(target):
                raise ValueError(f"第 {line_number} 行不是等长训练样本")
            records.append(value)
    if not records:
        raise ValueError(f"数据集中没有 split={split!r}")
    return records


def chunk_boundaries(text: str, limit: int) -> Iterable[tuple[int, int]]:
    start = 0
    while start < len(text):
        end = min(len(text), start + limit)
        if end < len(text):
            marks = [match.end() for match in re.finditer(r"[。；！？\n]", text[start:end])]
            if marks and marks[-1] >= limit // 2:
                end = start + marks[-1]
        yield start, end
        start = end


def make_chunks(records: list[dict], max_characters: int) -> list[dict]:
    chunks = []
    for record_index, record in enumerate(records):
        text, target = record["text"], record["target"]
        for start, end in chunk_boundaries(text, max_characters):
            chunk_text, chunk_target = text[start:end], target[start:end]
            chunks.append(
                {
                    "record_index": record_index,
                    "project_id": record["project_id"],
                    "start": start,
                    "text": chunk_text,
                    "target": chunk_target,
                    "is_error": chunk_text != chunk_target,
                }
            )
    return chunks


class ChunkDataset(Dataset):
    def __init__(self, chunks: list[dict], indices: list[int]):
        self.chunks = chunks
        self.indices = indices

    def __len__(self) -> int:
        return len(self.indices)

    def __getitem__(self, index: int) -> dict:
        return self.chunks[self.indices[index]]


@dataclass
class BatchCollator:
    tokenizer: BertTokenizerFast
    max_length: int
    error_weight: float

    def __call__(self, batch: list[dict]) -> dict:
        texts = [item["text"] for item in batch]
        encoded = self.tokenizer(
            texts,
            padding=True,
            truncation=True,
            max_length=self.max_length,
            return_offsets_mapping=True,
            return_tensors="pt",
        )
        offsets = encoded.pop("offset_mapping")
        labels = torch.full_like(encoded["input_ids"], -100)
        loss_weights = torch.zeros_like(encoded["input_ids"], dtype=torch.float32)
        for batch_index, item in enumerate(batch):
            source, target = item["text"], item["target"]
            for token_index, (start, end) in enumerate(offsets[batch_index].tolist()):
                if end - start != 1 or end > len(source):
                    continue
                if not HAN.fullmatch(source[start:end]) or not HAN.fullmatch(target[start:end]):
                    continue
                target_id = self.tokenizer.convert_tokens_to_ids(target[start:end])
                if target_id == self.tokenizer.unk_token_id:
                    continue
                labels[batch_index, token_index] = target_id
                loss_weights[batch_index, token_index] = (
                    self.error_weight if source[start:end] != target[start:end] else 1.0
                )
        encoded["labels"] = labels
        encoded["loss_weights"] = loss_weights
        encoded["metadata"] = batch
        return encoded


def sample_epoch_indices(
    chunks: list[dict], *, correct_ratio: float, seed: int, size: int
) -> list[int]:
    correct = [index for index, item in enumerate(chunks) if not item["is_error"]]
    errors = [index for index, item in enumerate(chunks) if item["is_error"]]
    if not correct or not errors:
        raise ValueError("训练集必须同时包含正确句和错误句")
    rng = random.Random(seed)
    correct_count = round(size * correct_ratio)
    indices = rng.choices(correct, k=correct_count)
    indices.extend(rng.choices(errors, k=size - correct_count))
    rng.shuffle(indices)
    return indices


def sample_hard_negative_indices(
    chunks: list[dict],
    *,
    hard_negative_ids: set[str],
    seed: int,
    size: int,
) -> list[int]:
    hard = [
        index
        for index, item in enumerate(chunks)
        if not item["is_error"] and item["project_id"] in hard_negative_ids
    ]
    regular_correct = [
        index
        for index, item in enumerate(chunks)
        if not item["is_error"] and item["project_id"] not in hard_negative_ids
    ]
    errors = [index for index, item in enumerate(chunks) if item["is_error"]]
    if not hard or not regular_correct or not errors:
        raise ValueError("误报补训需要误报正确句、普通正确句和错误句")
    rng = random.Random(seed)
    hard_count = round(size * 0.20)
    regular_count = round(size * 0.40)
    indices = rng.choices(hard, k=hard_count)
    indices.extend(rng.choices(regular_correct, k=regular_count))
    indices.extend(rng.choices(errors, k=size - hard_count - regular_count))
    rng.shuffle(indices)
    return indices


def move_batch(batch: dict, device: torch.device) -> tuple[dict, torch.Tensor, torch.Tensor]:
    labels = batch.pop("labels").to(device)
    weights = batch.pop("loss_weights").to(device)
    batch.pop("metadata")
    inputs = {key: value.to(device) for key, value in batch.items()}
    return inputs, labels, weights


def weighted_loss(logits: torch.Tensor, labels: torch.Tensor, weights: torch.Tensor) -> torch.Tensor:
    flat = F.cross_entropy(
        logits.reshape(-1, logits.shape[-1]),
        labels.reshape(-1),
        ignore_index=-100,
        reduction="none",
    ).reshape_as(labels)
    active = labels.ne(-100)
    denominator = weights[active].sum().clamp_min(1.0)
    return (flat[active] * weights[active]).sum() / denominator


@torch.inference_mode()
def evaluate(
    model: BertForMaskedLM,
    tokenizer: BertTokenizerFast,
    records: list[dict],
    *,
    device: torch.device,
    max_length: int,
    batch_size: int,
    min_probability: float = 0.60,
    min_ratio: float = 10.0,
) -> dict:
    chunks = make_chunks(records, max_length - 2)
    expected_positions = set()
    expected_edits = set()
    correct_records = set()
    correct_characters = 0
    for record_index, record in enumerate(records):
        expected = record.get("expected") or []
        if not expected:
            correct_records.add(record_index)
            correct_characters += len(record["text"])
        for edit in expected:
            expected_positions.add((record_index, edit["start"], edit["end"]))
            expected_edits.add(
                (record_index, edit["start"], edit["end"], edit["replacement"])
            )
    predicted_positions = set()
    predicted_edits = set()
    model.eval()
    for offset in range(0, len(chunks), batch_size):
        batch = chunks[offset:offset + batch_size]
        encoded = tokenizer(
            [item["text"] for item in batch],
            padding=True,
            truncation=True,
            max_length=max_length,
            return_offsets_mapping=True,
            return_tensors="pt",
        )
        offsets = encoded.pop("offset_mapping")
        inputs = {key: value.to(device) for key, value in encoded.items()}
        with torch.autocast(
            device_type=device.type,
            dtype=torch.bfloat16,
            enabled=device.type == "cuda",
        ):
            logits = model(**inputs).logits
        probabilities = torch.softmax(logits.float(), dim=-1)
        confidence, token_ids = probabilities.max(dim=-1)
        source_probability = probabilities.gather(
            -1, inputs["input_ids"].unsqueeze(-1)
        ).squeeze(-1)
        for batch_index, item in enumerate(batch):
            for token_index, (start, end) in enumerate(offsets[batch_index].tolist()):
                if end - start != 1 or not HAN.fullmatch(item["text"][start:end]):
                    continue
                replacement = tokenizer.convert_ids_to_tokens(
                    int(token_ids[batch_index, token_index])
                )
                probability = float(confidence[batch_index, token_index])
                source_prob = float(source_probability[batch_index, token_index])
                if (
                    len(replacement) != 1
                    or not HAN.fullmatch(replacement)
                    or replacement == item["text"][start:end]
                    or probability < min_probability
                    or probability / max(source_prob, 1e-12) < min_ratio
                ):
                    continue
                absolute = item["start"] + start
                key = (item["record_index"], absolute, absolute + 1)
                predicted_positions.add(key)
                predicted_edits.add((*key, replacement))
    position_tp = len(predicted_positions & expected_positions)
    correction_tp = len(predicted_edits & expected_edits)
    precision = correction_tp / len(predicted_edits) if predicted_edits else 0.0
    recall = correction_tp / len(expected_edits) if expected_edits else 0.0
    position_precision = position_tp / len(predicted_positions) if predicted_positions else 0.0
    clean_false_positives = sum(
        1 for record_index, _, _ in predicted_positions if record_index in correct_records
    )
    f05 = 1.25 * precision * recall / (0.25 * precision + recall) if precision + recall else 0.0
    return {
        "records": len(records),
        "expected_edits": len(expected_edits),
        "predicted_edits": len(predicted_edits),
        "position_precision": position_precision,
        "modification_precision": precision,
        "recall": recall,
        "f0.5": f05,
        "correct_characters": correct_characters,
        "clean_false_positives": clean_false_positives,
        "false_positives_per_10k": clean_false_positives * 10000 / max(1, correct_characters),
        "all_keep_original": not predicted_edits,
    }


def save_model(
    model: BertForMaskedLM,
    tokenizer: BertTokenizerFast,
    output_dir: Path,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    model.save_pretrained(output_dir, safe_serialization=True)
    tokenizer.save_pretrained(output_dir)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("dataset", type=Path)
    parser.add_argument("--model", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--train-split", default="train")
    parser.add_argument("--dev-split", default="dev")
    parser.add_argument("--epochs", type=float, default=2.0)
    parser.add_argument("--micro-batch-size", type=int, default=8)
    parser.add_argument("--eval-batch-size", type=int, default=32)
    parser.add_argument("--gradient-accumulation", type=int, default=8)
    parser.add_argument("--learning-rate", type=float, default=2e-5)
    parser.add_argument("--weight-decay", type=float, default=0.01)
    parser.add_argument("--warmup-ratio", type=float, default=0.06)
    parser.add_argument("--max-length", type=int, default=256)
    parser.add_argument("--correct-ratio", type=float, default=0.60)
    parser.add_argument("--error-weight", type=float, default=3.0)
    parser.add_argument("--gradient-clip", type=float, default=1.0)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--max-updates", type=int)
    parser.add_argument("--benchmark-only", action="store_true")
    parser.add_argument("--cuda-memory-fraction", type=float, default=0.45)
    parser.add_argument(
        "--hard-negative-file",
        type=Path,
        help="JSONL records containing project_id; enables 20/40/40 hard-negative sampling",
    )
    args = parser.parse_args()

    if not 0 < args.correct_ratio < 1:
        raise ValueError("correct-ratio 必须在 0 和 1 之间")
    random.seed(args.seed)
    torch.manual_seed(args.seed)
    if torch.cuda.is_available():
        torch.cuda.manual_seed_all(args.seed)
        torch.cuda.set_per_process_memory_fraction(args.cuda_memory_fraction)
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

    tokenizer = BertTokenizerFast.from_pretrained(args.model, local_files_only=True)
    model = BertForMaskedLM.from_pretrained(args.model, local_files_only=True).to(device)
    train_records = read_jsonl(args.dataset, args.train_split)
    dev_records = read_jsonl(args.dataset, args.dev_split)
    chunks = make_chunks(train_records, args.max_length - 2)
    hard_negative_ids: set[str] = set()
    if args.hard_negative_file:
        with args.hard_negative_file.open(encoding="utf-8") as stream:
            for raw in stream:
                if raw.strip():
                    value = json.loads(raw)
                    hard_negative_ids.add(str(value["project_id"]))
    epoch_size = len(chunks)
    batches_per_epoch = math.ceil(epoch_size / args.micro_batch_size)
    updates_per_epoch = math.ceil(batches_per_epoch / args.gradient_accumulation)
    planned_updates = max(1, math.ceil(updates_per_epoch * args.epochs))
    if args.max_updates:
        planned_updates = min(planned_updates, args.max_updates)
    optimizer = torch.optim.AdamW(
        model.parameters(), lr=args.learning_rate, weight_decay=args.weight_decay
    )
    scheduler = get_linear_schedule_with_warmup(
        optimizer,
        num_warmup_steps=round(planned_updates * args.warmup_ratio),
        num_training_steps=planned_updates,
    )
    collator = BatchCollator(tokenizer, args.max_length, args.error_weight)
    logs = []
    initial_metrics = None
    best_metrics = None
    if not args.benchmark_only:
        initial_metrics = evaluate(
            model,
            tokenizer,
            dev_records,
            device=device,
            max_length=args.max_length,
            batch_size=args.eval_batch_size,
        )
        best_metrics = initial_metrics
        save_model(model, tokenizer, args.output_dir)

    started = time.monotonic()
    update = 0
    examples = 0
    running_loss = 0.0
    eval_interval = max(1, updates_per_epoch // 2)
    optimizer.zero_grad(set_to_none=True)
    epoch = 0
    stop = False
    while update < planned_updates and not stop:
        if hard_negative_ids:
            indices = sample_hard_negative_indices(
                chunks,
                hard_negative_ids=hard_negative_ids,
                seed=args.seed + epoch,
                size=epoch_size,
            )
        else:
            indices = sample_epoch_indices(
                chunks,
                correct_ratio=args.correct_ratio,
                seed=args.seed + epoch,
                size=epoch_size,
            )
        loader = DataLoader(
            ChunkDataset(chunks, indices),
            batch_size=args.micro_batch_size,
            shuffle=False,
            collate_fn=collator,
            num_workers=0,
        )
        model.train()
        for batch_index, batch in enumerate(loader, 1):
            current_size = len(batch["metadata"])
            inputs, labels, weights = move_batch(batch, device)
            with torch.autocast(
                device_type=device.type,
                dtype=torch.bfloat16,
                enabled=device.type == "cuda",
            ):
                logits = model(**inputs).logits
                loss = weighted_loss(logits, labels, weights)
                scaled_loss = loss / args.gradient_accumulation
            scaled_loss.backward()
            running_loss += float(loss.detach())
            examples += current_size
            should_step = (
                batch_index % args.gradient_accumulation == 0
                or batch_index == len(loader)
            )
            if not should_step:
                continue
            torch.nn.utils.clip_grad_norm_(model.parameters(), args.gradient_clip)
            optimizer.step()
            scheduler.step()
            optimizer.zero_grad(set_to_none=True)
            update += 1
            event = {
                "update": update,
                "epoch": epoch + batch_index / len(loader),
                "loss": running_loss / max(1, update),
                "learning_rate": scheduler.get_last_lr()[0],
                "elapsed_seconds": time.monotonic() - started,
            }
            if not args.benchmark_only and (
                update % eval_interval == 0 or update == planned_updates
            ):
                metrics = evaluate(
                    model,
                    tokenizer,
                    dev_records,
                    device=device,
                    max_length=args.max_length,
                    batch_size=args.eval_batch_size,
                )
                event["dev"] = metrics
                if hard_negative_ids:
                    improved = (
                        metrics["false_positives_per_10k"]
                        < best_metrics["false_positives_per_10k"]
                        and metrics["modification_precision"]
                        >= best_metrics["modification_precision"]
                        and metrics["recall"] >= initial_metrics["recall"] - 0.02
                    )
                else:
                    improved = metrics["f0.5"] > best_metrics["f0.5"]
                if improved and not metrics["all_keep_original"]:
                    best_metrics = metrics
                    save_model(model, tokenizer, args.output_dir)
                model.train()
            logs.append(event)
            print(json.dumps(event, ensure_ascii=False), flush=True)
            if update >= planned_updates:
                stop = True
                break
        epoch += 1

    elapsed = time.monotonic() - started
    report = {
        "schema_version": 1,
        "benchmark_only": args.benchmark_only,
        "device": str(device),
        "software": {
            "torch": torch.__version__,
            "transformers": transformers.__version__,
        },
        "dataset": str(args.dataset.resolve()),
        "dataset_sha256": sha256_file(args.dataset),
        "base_model": str(args.model.resolve()),
        "hyperparameters": vars(args) | {
            "dataset": str(args.dataset),
            "model": str(args.model),
            "output_dir": str(args.output_dir),
        },
        "train_records": len(train_records),
        "train_chunks": len(chunks),
        "dev_records": len(dev_records),
        "updates": update,
        "examples": examples,
        "elapsed_seconds": elapsed,
        "updates_per_second": update / max(elapsed, 1e-9),
        "peak_cuda_memory_bytes": torch.cuda.max_memory_allocated() if device.type == "cuda" else 0,
        "initial_dev": initial_metrics,
        "best_dev": best_metrics,
        "events": logs,
    }
    args.output_dir.mkdir(parents=True, exist_ok=True)
    (args.output_dir / "training-report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2, default=str) + "\n",
        encoding="utf-8",
    )
    if not args.benchmark_only:
        weights_path = args.output_dir / "model.safetensors"
        model_sha256 = sha256_file(weights_path)
        revision = (
            "public-csc-v1-"
            + report["dataset_sha256"][:12]
            + "-"
            + model_sha256[:12]
        )
        manifest = {
            "model": "xtjs/macbert4csc-public-high-precision",
            "revision": revision,
            "engine": "macbert-csc",
            "base_model": str(args.model.resolve()),
            "dataset_sha256": report["dataset_sha256"],
            "model_sha256": model_sha256,
            "training_report": "training-report.json",
            "accepted": False,
        }
        (args.output_dir / "model-manifest.json").write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
    print(json.dumps(report, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
