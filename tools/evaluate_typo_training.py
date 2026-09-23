#!/usr/bin/env python3
"""Cache raw MacBERT candidates and evaluate candidate/rule layers separately."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from collections import defaultdict
from pathlib import Path

import torch
from transformers import BertForMaskedLM, BertTokenizerFast

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.service.typo_runtime.contract import (
    classify_candidates,
    validate_candidates,
    word_rules,
)
from tools.evaluate_duplicate_typo_candidates import DEFAULT_GATE, assess_gate


HAN = re.compile(r"[\u4e00-\u9fff]")
PROBABILITY_GRID = (0.90, 0.93, 0.95, 0.97, 0.99)
RATIO_GRID = (20.0, 30.0, 50.0, 100.0)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_records(path: Path, splits: set[str]) -> list[dict]:
    records = []
    with path.open(encoding="utf-8") as stream:
        for raw in stream:
            if not raw.strip():
                continue
            record = json.loads(raw)
            if record.get("split") in splits:
                records.append(record)
    missing = splits - {record["split"] for record in records}
    if missing:
        raise ValueError("缺少数据分组：" + "、".join(sorted(missing)))
    return records


def chunks(text: str, limit: int = 240):
    start = 0
    while start < len(text):
        end = min(len(text), start + limit)
        if end < len(text):
            marks = [match.end() for match in re.finditer(r"[。；！？\n]", text[start:end])]
            if marks and marks[-1] >= limit // 2:
                end = start + marks[-1]
        yield start, text[start:end]
        start = end


@torch.inference_mode()
def predict(
    records: list[dict],
    *,
    model_path: Path,
    batch_size: int,
    min_probability: float,
    min_ratio: float,
    cuda_memory_fraction: float,
) -> list[dict]:
    if torch.cuda.is_available():
        torch.cuda.set_per_process_memory_fraction(cuda_memory_fraction)
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    tokenizer = BertTokenizerFast.from_pretrained(model_path, local_files_only=True)
    model = BertForMaskedLM.from_pretrained(model_path, local_files_only=True)
    model.eval().to(device)
    work = []
    for record_index, record in enumerate(records):
        for start, part in chunks(record["text"]):
            work.append((record_index, start, part))
    outputs = [
        {
            "project_id": record["project_id"],
            "split": record["split"],
            "source": record.get("source"),
            "candidates": [],
        }
        for record in records
    ]
    seen = [set() for _ in records]
    for batch_start in range(0, len(work), batch_size):
        batch = work[batch_start:batch_start + batch_size]
        encoded = tokenizer(
            [item[2] for item in batch],
            padding=True,
            truncation=False,
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
        for batch_index, (record_index, chunk_start, part) in enumerate(batch):
            for token_index, (start, end) in enumerate(offsets[batch_index].tolist()):
                if end - start != 1 or not HAN.fullmatch(part[start:end]):
                    continue
                replacement = tokenizer.convert_ids_to_tokens(
                    int(token_ids[batch_index, token_index])
                )
                probability = float(confidence[batch_index, token_index])
                source_prob = float(source_probability[batch_index, token_index])
                ratio = probability / max(source_prob, 1e-12)
                if (
                    len(replacement) != 1
                    or not HAN.fullmatch(replacement)
                    or replacement == part[start:end]
                    or probability < min_probability
                    or ratio < min_ratio
                ):
                    continue
                absolute = chunk_start + start
                key = (absolute, absolute + 1, replacement)
                if key in seen[record_index]:
                    continue
                seen[record_index].add(key)
                outputs[record_index]["candidates"].append(
                    {
                        "start": absolute,
                        "end": absolute + 1,
                        "original": part[start:end],
                        "replacement": replacement,
                        "candidate_probability": probability,
                        "source_probability": source_prob,
                        "probability_ratio": ratio,
                    }
                )
    return outputs


def prediction_cache(
    records: list[dict],
    *,
    dataset_path: Path,
    model_path: Path,
    cache_path: Path,
    batch_size: int,
    min_probability: float,
    min_ratio: float,
    cuda_memory_fraction: float,
) -> list[dict]:
    weights = model_path / "model.safetensors"
    identity = {
        "dataset_sha256": sha256_file(dataset_path),
        "model_sha256": sha256_file(weights),
        "project_ids_sha256": hashlib.sha256(
            "\n".join(record["project_id"] for record in records).encode()
        ).hexdigest(),
        "min_probability": min_probability,
        "min_ratio": min_ratio,
    }
    manifest_path = cache_path.with_suffix(cache_path.suffix + ".manifest.json")
    if cache_path.is_file() and manifest_path.is_file():
        if json.loads(manifest_path.read_text(encoding="utf-8")) == identity:
            with cache_path.open(encoding="utf-8") as stream:
                cached = [json.loads(line) for line in stream if line.strip()]
            if len(cached) == len(records):
                return cached
    cached = predict(
        records,
        model_path=model_path,
        batch_size=batch_size,
        min_probability=min_probability,
        min_ratio=min_ratio,
        cuda_memory_fraction=cuda_memory_fraction,
    )
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    with cache_path.open("w", encoding="utf-8") as stream:
        for result in cached:
            stream.write(json.dumps(result, ensure_ascii=False) + "\n")
    manifest_path.write_text(
        json.dumps(identity, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return cached


def metrics_for(
    records: list[dict],
    predictions: list[list[dict]],
    *,
    result_name: str,
    review_count: int = 0,
) -> dict:
    expected_positions = set()
    expected_edits = set()
    predicted_positions = set()
    predicted_edits = set()
    correct_records = set()
    correct_characters = 0
    for record_index, (record, values) in enumerate(zip(records, predictions)):
        expected = record.get("expected") or []
        if not expected:
            correct_records.add(record_index)
            correct_characters += len(record["text"])
        for edit in expected:
            expected_positions.add((record_index, edit["start"], edit["end"]))
            expected_edits.add(
                (record_index, edit["start"], edit["end"], edit["replacement"])
            )
        for value in values:
            key = (record_index, value["start"], value["end"])
            predicted_positions.add(key)
            predicted_edits.add((*key, value["replacement"]))
    position_tp = len(predicted_positions & expected_positions)
    modification_tp = len(predicted_edits & expected_edits)
    clean_false_positives = sum(
        1 for record_index, _, _ in predicted_positions if record_index in correct_records
    )
    return {
        "result_layer": result_name,
        "records": len(records),
        "expected_edits": len(expected_edits),
        "correct_characters": correct_characters,
        "confirmed_results": len(predicted_edits),
        "review_candidates": review_count,
        "position_true_positives": position_tp,
        "modification_true_positives": modification_tp,
        "false_positive_positions": len(predicted_positions - expected_positions),
        "correct_text_false_positives": clean_false_positives,
        "position_precision": position_tp / len(predicted_positions) if predicted_positions else 0.0,
        "modification_precision": modification_tp / len(predicted_edits) if predicted_edits else 0.0,
        "recall": modification_tp / len(expected_edits) if expected_edits else 0.0,
        "false_positives_per_10k": clean_false_positives * 10000 / max(1, correct_characters),
    }


def evaluate_configuration(
    records: list[dict],
    cached: list[dict],
    *,
    rules: dict,
    probability: float,
    ratio: float,
) -> tuple[dict, list[list[dict]], list[list[dict]]]:
    issues_by_record = []
    review_by_record = []
    for record, result in zip(records, cached):
        candidates = validate_candidates(record["text"], result)
        issues, review = classify_candidates(
            record["text"],
            candidates,
            auto_min_probability=probability,
            auto_min_probability_ratio=ratio,
            rules=rules,
        )
        issues_by_record.append(issues)
        review_by_record.append(review)
    metrics = metrics_for(
        records,
        issues_by_record,
        result_name="confirmed",
        review_count=sum(map(len, review_by_record)),
    )
    return metrics, issues_by_record, review_by_record


def parse_rules(values: list[str]) -> dict[str, dict]:
    if not values:
        return {"runtime": word_rules()}
    result = {}
    for value in values:
        if "=" not in value:
            raise ValueError("--rules 格式必须为 name=path")
        name, path = value.split("=", 1)
        result[name] = word_rules(path)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("dataset", type=Path)
    parser.add_argument("--model", type=Path, required=True)
    parser.add_argument("--split", action="append", required=True)
    parser.add_argument("--cache", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--rules", action="append", default=[])
    parser.add_argument("--batch-size", type=int, default=32)
    parser.add_argument("--candidate-probability", type=float, default=0.60)
    parser.add_argument("--candidate-ratio", type=float, default=10.0)
    parser.add_argument("--auto-probability", type=float, default=0.90)
    parser.add_argument("--auto-ratio", type=float, default=20.0)
    parser.add_argument("--search-thresholds", action="store_true")
    parser.add_argument("--cuda-memory-fraction", type=float, default=0.25)
    parser.add_argument("--hard-negative-output", type=Path)
    parser.add_argument("--hard-negative-limit", type=int, default=2000)
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args()

    records = load_records(args.dataset, set(args.split))
    cached = prediction_cache(
        records,
        dataset_path=args.dataset,
        model_path=args.model,
        cache_path=args.cache,
        batch_size=args.batch_size,
        min_probability=args.candidate_probability,
        min_ratio=args.candidate_ratio,
        cuda_memory_fraction=args.cuda_memory_fraction,
    )
    candidate_lists = [
        validate_candidates(record["text"], result)
        for record, result in zip(records, cached)
    ]
    if args.hard_negative_output:
        hard_negatives = []
        for record, candidates in zip(records, candidate_lists):
            if record.get("expected") or not candidates:
                continue
            hard_negatives.append(
                {
                    "project_id": record["project_id"],
                    "candidate_count": len(candidates),
                    "max_candidate_probability": max(
                        float(item["candidate_probability"]) for item in candidates
                    ),
                    "max_probability_ratio": max(
                        float(item["probability_ratio"]) for item in candidates
                    ),
                }
            )
        hard_negatives.sort(
            key=lambda item: (
                -item["candidate_count"],
                -item["max_candidate_probability"],
                -item["max_probability_ratio"],
                item["project_id"],
            )
        )
        args.hard_negative_output.parent.mkdir(parents=True, exist_ok=True)
        with args.hard_negative_output.open("w", encoding="utf-8") as stream:
            for item in hard_negatives[: args.hard_negative_limit]:
                stream.write(json.dumps(item, ensure_ascii=False) + "\n")
    candidate_metrics = metrics_for(records, candidate_lists, result_name="candidate")
    configurations = []
    rule_sets = parse_rules(args.rules)
    probabilities = PROBABILITY_GRID if args.search_thresholds else (args.auto_probability,)
    ratios = RATIO_GRID if args.search_thresholds else (args.auto_ratio,)
    for rule_name, rules in rule_sets.items():
        for probability in probabilities:
            for ratio in ratios:
                metrics, _, _ = evaluate_configuration(
                    records,
                    cached,
                    rules=rules,
                    probability=probability,
                    ratio=ratio,
                )
                configurations.append(
                    {
                        "rules": rule_name,
                        "word_rule_version": rules["version"],
                        "auto_probability": probability,
                        "auto_ratio": ratio,
                        "metrics": metrics,
                        "gate": assess_gate(metrics),
                    }
                )
    eligible = [
        item
        for item in configurations
        if item["metrics"]["position_precision"] >= DEFAULT_GATE["position_precision"]
        and item["metrics"]["modification_precision"] >= DEFAULT_GATE["modification_precision"]
        and item["metrics"]["false_positives_per_10k"] <= DEFAULT_GATE["false_positives_per_10k"]
    ]
    pool = eligible or configurations
    selected = max(
        pool,
        key=lambda item: (
            item["metrics"]["recall"],
            item["metrics"]["modification_precision"],
            item["metrics"]["confirmed_results"],
            -item["auto_probability"],
            -item["auto_ratio"],
        ),
    )
    by_split = {}
    for split in sorted(set(args.split)):
        indices = [index for index, record in enumerate(records) if record["split"] == split]
        subset_records = [records[index] for index in indices]
        subset_cached = [cached[index] for index in indices]
        subset_candidate = [candidate_lists[index] for index in indices]
        metrics, _, _ = evaluate_configuration(
            subset_records,
            subset_cached,
            rules=rule_sets[selected["rules"]],
            probability=selected["auto_probability"],
            ratio=selected["auto_ratio"],
        )
        by_split[split] = {
            "candidate": metrics_for(subset_records, subset_candidate, result_name="candidate"),
            "confirmed": metrics,
            "gate": assess_gate(metrics),
        }
    report = {
        "schema_version": 1,
        "dataset_sha256": sha256_file(args.dataset),
        "model_sha256": sha256_file(args.model / "model.safetensors"),
        "candidate_thresholds": {
            "probability": args.candidate_probability,
            "ratio": args.candidate_ratio,
        },
        "candidate_metrics": candidate_metrics,
        "selected": selected,
        "configurations": configurations,
        "by_split": by_split,
        "incomplete_checks": 0,
    }
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    if not args.quiet:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        print(
            json.dumps(
                {
                    "report": str(args.report),
                    "candidate_metrics": candidate_metrics,
                    "selected": selected,
                },
                ensure_ascii=False,
            )
        )


if __name__ == "__main__":
    main()
