import importlib.util
import json
import tempfile
import unittest
from pathlib import Path

from app.service.typo_runtime.contract import classify_candidates
from tools import manage_typo_word_rules as RULES
from tools import prepare_typo_training_data as PREPARE


SCRIPT = Path(__file__).resolve().parents[1] / "tools" / "evaluate_duplicate_typo_candidates.py"
SPEC = importlib.util.spec_from_file_location("evaluate_duplicate_typo_candidates", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)

class TypoEvaluationTests(unittest.TestCase):
    def test_metrics_distinguish_position_and_replacement_precision(self):
        records = [
            {
                "project_id": "project-a",
                "split": "test",
                "text": "安全培圳，按排实施。",
                "expected": [
                    {"start": 3, "end": 4, "replacement": "训"},
                    {"start": 5, "end": 6, "replacement": "安"},
                ],
                "result": {
                    "issues": [
                        {"start": 3, "end": 4, "replacement": "训"},
                        {"start": 5, "end": 6, "replacement": "岸"},
                    ]
                },
            }
        ]
        metrics = MODULE.evaluate_records(records)
        self.assertEqual(metrics["position_precision"], 1.0)
        self.assertEqual(metrics["modification_precision"], 0.5)
        self.assertEqual(metrics["recall"], 0.5)
        self.assertEqual(metrics["false_positive_positions"], 0)
        self.assertEqual(metrics["correct_characters"], 0)

    def test_false_positives_per_10k_uses_only_fully_correct_text(self):
        records = [
            {
                "project_id": "project-a",
                "split": "test",
                "text": "完全正确" * 2500,
                "expected": [],
                "result": {"issues": [{"start": 0, "end": 1, "replacement": "完"}]},
            }
        ]
        metrics = MODULE.evaluate_records(records)
        self.assertEqual(metrics["correct_characters"], 10_000)
        self.assertEqual(metrics["correct_text_false_positives"], 1)
        self.assertEqual(metrics["false_positives_per_10k"], 1.0)

    def test_group_split_validation_rejects_project_or_text_leakage(self):
        with self.assertRaisesRegex(ValueError, "项目"):
            MODULE.validate_group_splits(
                [
                    {"project_id": "same", "split": "dev", "text": "文本一"},
                    {"project_id": "same", "split": "test", "text": "文本二"},
                ]
            )

    def test_candidate_metrics_include_review_candidates(self):
        records = [
            {
                "project_id": "project-a",
                "split": "test",
                "text": "安全培圳",
                "expected": [{"start": 3, "end": 4, "replacement": "训"}],
                "result": {
                    "issues": [],
                    "review_candidates": [
                        {"start": 3, "end": 4, "replacement": "训"}
                    ],
                },
            }
        ]
        metrics = MODULE.evaluate_records(records)
        self.assertEqual(metrics["confirmed_results"], 0)
        self.assertEqual(metrics["review_candidate_count"], 1)
        self.assertEqual(metrics["candidate_metrics"]["modification_precision"], 1.0)
        self.assertEqual(metrics["candidate_metrics"]["recall"], 1.0)

    def test_v3_hidden_candidates_are_not_reported_as_measured_candidate_recall(self):
        records = [{
            "project_id": "project-v3", "split": "test", "text": "安全培圳",
            "expected": [{"start": 3, "end": 4, "replacement": "训"}],
            "result": {
                "issues": [], "candidate_metrics_available": False,
                "candidates": None, "candidate_count": 1,
                "eligible_count": 0, "hidden_count": 1,
            },
        }]
        metrics = MODULE.evaluate_records(records)
        self.assertIsNone(metrics["candidate_metrics"])
        self.assertEqual((metrics["candidate_count"], metrics["hidden_count"]), (1, 1))

    def test_custom_rules_can_be_evaluated_without_replacing_runtime_rules(self):
        candidate = {
            "start": 3,
            "end": 4,
            "original": "圳",
            "replacement": "训",
            "candidate_probability": 0.96,
            "source_probability": 0.01,
            "probability_ratio": 96.0,
            "protected_span": False,
        }
        rules = {
            "version": "test-v1",
            "protected_terms": [],
            "approved_corrections": [
                {"id": "training-xun", "source": "培圳", "target": "培训"}
            ],
        }
        confirmed, review = classify_candidates(
            "安全培圳", [candidate], rules=rules
        )
        self.assertEqual(len(confirmed), 1)
        self.assertEqual(review, [])
        self.assertEqual(confirmed[0]["word_rule_version"], "test-v1")

        conditional_rules = json.loads(json.dumps(rules, ensure_ascii=False))
        conditional_rules["version"] = "test-v2"
        conditional_rules["approved_corrections"][0]["conditions"] = {
            "required_right": ["课程"]
        }
        confirmed, review = classify_candidates(
            "安全培圳", [candidate], rules=conditional_rules
        )
        self.assertEqual(confirmed, [])
        self.assertEqual(review[0]["review_reason"], "unverified_word")

    def test_public_data_validation_rejects_length_and_non_han_changes(self):
        length_change = PREPARE.Record("source", "train", "train", 1, "培训", "培训班")
        punctuation_change = PREPARE.Record("source", "train", "train", 2, "培训，", "培训。")
        self.assertEqual(PREPARE.validate(length_change), "length_change")
        self.assertEqual(PREPARE.validate(punctuation_change), "non_han_replacement")

    def test_rule_compiler_requires_two_distinct_approvals(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            base = root / "base.json"
            review = root / "review.jsonl"
            output = root / "output.json"
            base.write_text(
                json.dumps(
                    {
                        "version": "old",
                        "protected_terms": ["兼容性"],
                        "approved_corrections": [],
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            review.write_text(
                json.dumps(
                    {
                        "id": "self-own",
                        "source": "自已",
                        "target": "自己",
                        "occurrences": 10,
                        "first_review": {"status": "approved", "reviewer": "reviewer-a"},
                        "second_review": {"status": "approved", "reviewer": "reviewer-b"},
                    },
                    ensure_ascii=False,
                )
                + "\n",
                encoding="utf-8",
            )
            args = type(
                "Args",
                (),
                {
                    "base_rules": base,
                    "review_file": review,
                    "version": "new",
                    "output": output,
                },
            )()
            RULES.compile_rules(args)
            compiled = json.loads(output.read_text(encoding="utf-8"))
            self.assertEqual(compiled["version"], "new")
            self.assertEqual(compiled["approved_corrections"][0]["source"], "自已")
            self.assertEqual(compiled["approved_corrections"][0]["review"]["second"], "reviewer-b")
        with self.assertRaisesRegex(ValueError, "重复文本"):
            MODULE.validate_group_splits(
                [
                    {"project_id": "a", "split": "dev", "text": "相 同文本"},
                    {"project_id": "b", "split": "test", "text": "相同 文本"},
                ]
            )


if __name__ == "__main__":
    unittest.main()
