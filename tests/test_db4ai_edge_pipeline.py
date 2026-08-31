from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from experiments.db4ai_edge_1_5b.pipeline import (
    audit_manifests,
    build_manifest,
    structured_metrics,
    validate_teacher,
)
from experiments.db4ai_edge_1_5b.storage_guard import storage_report


class Db4aiEdgePipelineTests(unittest.TestCase):
    def test_storage_guard_reports_reserve(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            report = storage_report(Path(temp), 0.001)
            self.assertTrue(report["passed"])
            self.assertGreater(report["free_bytes"], report["minimum_free_bytes"])

    def _jsonl(self, directory: Path, name: str, rows: list[dict]) -> Path:
        path = directory / name
        path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")
        return path

    def test_manifest_and_leakage_audit_detect_hash_overlap(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            base = Path(temp)
            train = self._jsonl(base, "train.jsonl", [{"sample_id": "train-1", "source_hash": "a" * 64}])
            test = self._jsonl(base, "test.jsonl", [{"sample_id": "test-1", "source_hash": "a" * 64}])
            train_manifest = build_manifest(train, "industrial", "train")
            test_manifest = build_manifest(test, "industrial", "test")
            train_path, test_path = base / "train.json", base / "test.json"
            train_path.write_text(json.dumps(train_manifest), encoding="utf-8")
            test_path.write_text(json.dumps(test_manifest), encoding="utf-8")
            report = audit_manifests([train_path, test_path])
            self.assertFalse(report["passed"])
            self.assertIn("a" * 64, report["duplicate_source_hashes"])

    def test_teacher_validation_rejects_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            base = Path(temp)
            target = {"event_type": "normal", "risk_attr": "low", "action": "pass", "review_intent": "none"}
            bad = dict(target, action="hold")
            source = self._jsonl(base, "teacher.jsonl", [
                {"sample_id": "ok", "scenario": "industrial", "hard_target": target, "teacher_target": target},
                {"sample_id": "bad", "scenario": "industrial", "hard_target": target, "teacher_target": bad},
            ])
            labels = Path(__file__).parents[1] / "experiments/db4ai_edge_1_5b/config/label_space.json"
            accepted, rejected = validate_teacher(source, labels)
            self.assertEqual([row["sample_id"] for row in accepted], ["ok"])
            self.assertIn("teacher_mismatch_action", rejected[0]["reasons"])

    def test_structured_metrics_count_invalid_json_as_tuple_error(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            base = Path(temp)
            target = {"event_type": "normal", "risk_attr": "low", "action": "pass", "review_intent": "none"}
            source = self._jsonl(base, "predictions.jsonl", [
                {"sample_id": "ok", "reference": target, "prediction": json.dumps(target)},
                {"sample_id": "bad", "reference": target, "prediction": "not-json"},
            ])
            metrics = structured_metrics(source)
            self.assertEqual(metrics["json_parse_rate"], 0.5)
            self.assertEqual(metrics["tuple_accuracy"], 0.5)
            self.assertEqual(metrics["field_accuracy"]["action"], 0.5)


if __name__ == "__main__":
    unittest.main()
