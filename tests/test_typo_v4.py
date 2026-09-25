import tempfile
import unittest
from pathlib import Path

from app.service.typo_runtime.contract import validate_candidates
from app.service.typo_runtime.manager import ModelManager
from app.service.typo_runtime.v4 import VERSION, budget_candidates, classify_v4
from tools.calibrate_typo_v4 import choose, summarize
from tools.evaluate_duplicate_typo_candidates import assess_gate
from tools.compare_typo_v4_latency import compare as compare_latency


def proposal(text, old, new, score=0.995):
    start = text.index(old)
    return {
        "start": start, "end": start + 1, "original": old, "replacement": new,
        "candidate_probability": 0.2, "source_probability": 0.4,
        "probability_ratio": 0.5, "verifier_score": score,
    }


class TypoV4Tests(unittest.TestCase):
    def test_calibration_uses_only_trace_candidates_and_requires_latency_gate(self):
        expected = {"start": 3, "end": 4, "replacement": "训"}
        candidate = {**expected, "verifier_score": 0.995, "verification_method": "macbert_verifier_cec3"}
        rows = [{"text": "安全培圳。", "expected": [expected], "result": {
            "eval_trace": True, "rule_version": VERSION,
            "raw_candidates": [candidate], "budget_candidates": [candidate],
            "calibration_candidates": [candidate],
            "calibration_reasons": [{"start": 3, "replacement": "训", "reason": "eligible_for_calibration"}],
        }}]
        self.assertEqual(summarize(rows, 0.99, 0.999)["recall"], 1.0)
        self.assertFalse(choose(rows)["ready_for_frozen_test"])
        measured = choose(rows, {
            "v3_p95_seconds": 1.0, "v4_p95_seconds": 1.2,
            "same_evidence": True, "cold_warm_matched": True,
            "supported_threshold": 0.99, "unsupported_threshold": 0.999,
        })
        self.assertTrue(measured["ready_for_frozen_test"])

    def test_final_v4_gate_requires_same_test_v3_recall(self):
        metrics = {
            "rule_versions": [VERSION], "position_precision": 1.0,
            "modification_precision": 1.0, "false_positives_per_10k": 0.0,
            "expected_edits": 200, "correct_characters": 100_000,
            "confirmed_results": 100, "recall": 0.5,
        }
        self.assertFalse(assess_gate(metrics)["passed"])
        self.assertFalse(assess_gate(metrics, baseline_recall=0.5)["passed"])
        self.assertTrue(assess_gate(metrics, baseline_recall=0.49)["passed"])

    def test_latency_report_rejects_nonidentical_evidence(self):
        baseline = {"samples": [{"text_sha256": "a"}], "cold_p95_seconds": 2.0, "warm_p95_seconds": 0.1}
        candidate = {"samples": [{"text_sha256": "b"}], "cold_p95_seconds": 2.1, "warm_p95_seconds": 0.2,
                     "supported_threshold": 0.99, "unsupported_threshold": 0.999}
        with self.assertRaisesRegex(ValueError, "证据"):
            compare_latency(baseline, candidate)
        candidate["samples"][0]["text_sha256"] = "a"
        self.assertTrue(compare_latency(baseline, candidate)["latency_gate_passed"])
    def test_subunit_ratio_and_top_eight_budget(self):
        text = "培圳" * 10
        raw = []
        for index in range(1, len(text), 2):
            item = proposal(text, "圳", "训")
            item["start"], item["end"] = index, index + 1
            item["probability_ratio"] = index / 10
            raw.append(item)
        validated = validate_candidates(text, {"candidates": raw}, allow_subunit_ratio=True)
        selected, skipped = budget_candidates(validated)
        self.assertEqual((len(selected), skipped), (8, 2))
        self.assertEqual(selected[0]["start"], 19)

    def test_other_cec3_edit_does_not_cancel_supported_position(self):
        text = "安全培圳，按排执行。"
        candidate = proposal(text, "圳", "训")
        corrected = "安全培训，安排执行。"
        issues, counts = classify_v4(text, [candidate], corrected, corrected,
                                     supported_threshold=0.99, unsupported_threshold=0.999)
        self.assertEqual([(x["original_word"], x["replacement_word"]) for x in issues], [("培圳", "培训")])
        self.assertEqual(counts["confirmed_count"], 1)

    def test_conflict_and_unstable_roundtrip_reject(self):
        text = "安全培圳。"
        candidate = proposal(text, "圳", "训")
        for corrected, second in (("安全培证。", "安全培证。"), ("安全培训。", "安全培圳。")):
            issues, _ = classify_v4(text, [candidate], corrected, second,
                                    supported_threshold=0.99, unsupported_threshold=0.999)
            self.assertEqual(issues, [])

    def test_verifier_only_requires_higher_threshold_and_valid_target_word(self):
        text = "安全培圳。"
        candidate = proposal(text, "圳", "训", 0.995)
        issues, _ = classify_v4(text, [candidate], text, text,
                                supported_threshold=0.99, unsupported_threshold=0.999)
        self.assertEqual(issues, [])

    def test_known_source_word_is_not_blanket_veto_but_low_score_is(self):
        text = "依法保护公司权利。"
        candidate = proposal(text, "利", "力", 0.8)
        corrected = text.replace("权利", "权力")
        issues, _ = classify_v4(text, [candidate], corrected, corrected,
                                supported_threshold=0.99, unsupported_threshold=0.999)
        self.assertEqual(issues, [])
        candidate["verifier_score"] = 1.0
        issues, _ = classify_v4(text, [candidate], corrected, corrected,
                                supported_threshold=0.99, unsupported_threshold=0.999)
        self.assertEqual([(x["original_word"], x["replacement_word"]) for x in issues], [("权利", "权力")])
        candidate["verifier_score"] = 0.9995
        issues, _ = classify_v4(text, [candidate], text, text,
                                supported_threshold=0.99, unsupported_threshold=0.999)
        self.assertEqual(len(issues), 1)
        wrong = proposal("数据链路。", "路", "络", 1.0)
        issues, _ = classify_v4("数据链路。", [wrong], "数据链路。", "数据链路。",
                                supported_threshold=0.99, unsupported_threshold=0.999)
        self.assertEqual(issues, [])

    def test_non_bmp_and_unrelated_insertion_keep_exact_location(self):
        text = "😀安全培圳。"
        candidate = proposal(text, "圳", "训")
        corrected = "😀安全培训。附"
        issues, _ = classify_v4(text, [candidate], corrected, corrected,
                                supported_threshold=0.99, unsupported_threshold=0.999)
        self.assertEqual([(x["start"], x["word_start"]) for x in issues], [(4, 3)])

    def test_v4_runtime_is_not_available(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(ValueError, "unsupported_typo_pipeline"):
                ModelManager(
                    [], worker_url="", model_id="macbert", cec3_model_id="cec3",
                    detector_model_id="unused", pipeline_version=VERSION,
                    font_path=None, font_sha256=None, pinyin_version=None,
                    cache_path=Path(directory) / "cache.sqlite",
                )


if __name__ == "__main__":
    unittest.main()
