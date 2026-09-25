"""Offline-only v5 spelling scope and original-detector tests."""

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from app.service.typo_runtime.contract import TypoUnavailable
from app.service.typo_runtime.manager import ModelManager
from app.service.typo_runtime.v5 import VERSION, ShapeSoundGate, classify_v5, select_candidates
from tools.train_typo_detector_v5 import scope_labels
from tools.prepare_typo_v5_scope import prepare
from tools.audit_typo_v5_scope_ceiling import audit as audit_scope_ceiling
from tools.calibrate_typo_v5 import choose, summarize
from tools.evaluate_duplicate_typo_candidates import assess_gate
from tools.compare_typo_v5_latency import compare as compare_latency
from collections import Counter


class AllowGate:
    def evidence(self, original, replacement):
        return {"similarity_type": "pinyin", "glyph_similarity": 0.5}


class RejectGate:
    def evidence(self, original, replacement):
        return None


def candidate(text, old, new):
    start = text.index(old)
    return {
        "start": start, "end": start + 1, "original": old, "replacement": new,
        "candidate_probability": 0.2, "source_probability": 0.1,
        "probability_ratio": 2.0, "detector_score": 0.99,
    }


class TypoV5Tests(unittest.TestCase):
    def test_original_known_word_never_confirms_even_with_all_model_support(self):
        for text, old, new, corrected in (
            ("保护公司权利。", "利", "力", "保护公司权力。"),
            ("使用数据链路。", "路", "络", "使用数据链络。"),
        ):
            with self.subTest(text=text):
                issues, counts = classify_v5(text, [candidate(text, old, new)], corrected, corrected, gate=AllowGate())
                self.assertEqual(issues, [])
                self.assertGreater(counts["source_valid_rejected_count"] + counts["word_invalid_count"], 0)

    def test_unknown_source_requires_every_gate(self):
        text = "安全培圳。"
        proposal = candidate(text, "圳", "训")
        issues, _ = classify_v5(text, [proposal], "安全培训。", "安全培训。", gate=AllowGate())
        self.assertEqual([(i["original_word"], i["replacement_word"]) for i in issues], [("培圳", "培训")])
        for corrected, roundtrip, gate in (
            (text, text, AllowGate()),
            ("安全培训。", text, AllowGate()),
            ("安全培训。", "安全培训。", RejectGate()),
        ):
            self.assertEqual(classify_v5(text, [proposal], corrected, roundtrip, gate=gate)[0], [])

    def test_extra_edit_and_unicode_codepoint_offset(self):
        text = "😀安全培圳，按排执行。"
        proposal = candidate(text, "圳", "训")
        corrected = "😀安全培训，安排执行。"
        issues, _ = classify_v5(text, [proposal], corrected, corrected, gate=AllowGate())
        self.assertEqual([(item["start"], item["word_start"]) for item in issues], [(4, 3)])
        issues, _ = classify_v5(text, [proposal], "😀安全培，安排执行。", "😀安全培，安排执行。", gate=AllowGate())
        self.assertEqual(issues, [])

    def test_detector_budget_and_legal_source_prefilter(self):
        text = "权利培圳" * 5
        proposals = []
        for start, char in enumerate(text):
            if char in ("利", "圳"):
                item = candidate(text, char, "力" if char == "利" else "训")
                item["start"], item["end"] = start, start + 1
                proposals.append(item)
        scores = [0.99] * len(text)
        selected, rejected, budget, legal = select_candidates(proposals, scores, threshold=0.9, limit=3, source_text=text)
        self.assertEqual(len(selected), 3)
        self.assertEqual((rejected, legal, budget), (0, 5, 2))

    def test_training_scope_keeps_legal_swaps_negative(self):
        counts = Counter()
        self.assertIn(1, scope_labels({"text": "安全培圳。", "target": "安全培训。"}, counts))
        self.assertNotIn(1, scope_labels({"text": "保护公司权利。", "target": "保护公司权力。"}, counts))
        self.assertEqual(counts["legal_word_negative"], 1)

    def test_agent_scope_review_does_not_pass_human_gate(self):
        row = {"project_id": "scope-1", "split": "dev", "text": "保护公司权利。", "target": "保护公司权力。"}
        start = row["text"].index("利")
        rows, counts = prepare([row], "dev", {(row["project_id"], start): "legal_word"}, review_source="agent")
        self.assertEqual(rows[0]["scope_review_status"], "agent_reviewed")
        self.assertEqual(rows[0]["scope_annotations"][0]["review_source"], "agent")
        self.assertEqual(counts["pending_human_review"], 1)
        with self.assertRaisesRegex(ValueError, "人工复核"):
            choose([{**rows[0], "result": {}}], 1)

    def test_dictionary_false_positive_requires_explicit_review_reason(self):
        row = {"project_id": "scope-2", "split": "dev", "text": "这是自已的选择。", "target": "这是自己的选择。"}
        start = row["text"].index("已")
        with self.assertRaisesRegex(ValueError, "dictionary_false_positive"):
            prepare([row], "dev", {(row["project_id"], start): "spelling"}, review_source="agent")
        decision = {"category": "spelling", "rationale": "dictionary_false_positive"}
        rows, counts = prepare([row], "dev", {(row["project_id"], start): decision}, review_source="agent")
        self.assertEqual(rows[0]["expected"], [{"start": start, "end": start + 1, "replacement": "己"}])
        self.assertEqual(counts["pending_human_review"], 1)
        with self.assertRaisesRegex(ValueError, "来源"):
            prepare([row], "dev", {(row["project_id"], start): {
                **decision, "review_source": "agent"}}, review_source="human")

    def test_scope_ceiling_counts_dictionary_false_positive_as_unreachable(self):
        wrong = {"project_id": "scope-2", "split": "dev", "text": "这是自已的选择。", "target": "这是自己的选择。"}
        start = wrong["text"].index("已")
        reviewed, _ = prepare([wrong], "dev", {(wrong["project_id"], start): {
            "category": "spelling", "rationale": "dictionary_false_positive"}}, review_source="agent")
        result = audit_scope_ceiling(reviewed, AllowGate(), 0.55)
        self.assertEqual(result["counts"]["source_in_jieba"], 1)
        self.assertEqual(result["counts"].get("structurally_eligible", 0), 0)

    def test_real_font_evidence_is_not_equivalent_to_typos(self):
        font = Path("/models/v5-assets/NotoSansSC-wght.ttf")
        if not font.is_file():
            self.skipTest("独立实验容器未挂载固定 Noto 字体")
        gate = ShapeSoundGate(font, glyph_threshold=0.75)
        self.assertIsNone(gate.evidence("圳", "训"))
        self.assertEqual(gate.evidence("利", "力")["similarity_type"], "pinyin")

    def test_partial_dev_trace_cannot_freeze_and_test_gate_needs_review_latency(self):
        with self.assertRaisesRegex(ValueError, "完整"):
            choose([{"split": "dev"}], expected_records=4994)
        metrics = {
            "rule_versions": [VERSION], "position_precision": 1.0,
            "modification_precision": 1.0, "false_positives_per_10k": 0.0,
            "expected_edits": 200, "correct_characters": 100_000,
            "confirmed_results": 100, "recall": 0.5,
            "scope_human_review_complete": False,
        }
        self.assertFalse(assess_gate(metrics, baseline_recall=0.4)["passed"])
        metrics["scope_human_review_complete"] = True
        self.assertFalse(assess_gate(metrics, baseline_recall=0.4)["passed"])
        latency = {"same_evidence": True, "cold_warm_matched": True, "latency_gate_passed": True}
        self.assertTrue(assess_gate(metrics, baseline_recall=0.4, latency_report=latency)["passed"])

    def test_dev_calibration_replays_detector_glyph_and_budget(self):
        text = "安全培圳。"
        proposal = candidate(text, "圳", "训")
        proposal.update({"similarity_type": "glyph", "glyph_similarity": 0.7})
        row = {
            "split": "dev", "text": text, "expected": [{"start": 3, "end": 4, "replacement": "训"}],
            "originally_correct": False, "scope_review_status": "human_confirmed",
            "result": {
                "rule_version": VERSION, "eval_trace": True,
                "trace_score_floor": 0.5, "glyph_threshold": 0,
                "raw_candidates": [proposal], "budget_candidates": [proposal],
                "calibration_candidates": [proposal], "calibration_reasons": [],
            },
        }
        self.assertEqual(summarize([row], 0.5, 0.65)["recall"], 1)
        self.assertEqual(summarize([row], 0.5, 0.75)["recall"], 0)
        latency = {"same_evidence": True, "cold_warm_matched": True, "latency_gate_passed": True,
                   "detector_threshold": 0.5, "glyph_threshold": 0.65}
        self.assertEqual(choose([row], 1, latency)["selected"]["glyph_threshold"], 0.65)

    def test_latency_requires_same_evidence_and_both_cache_states(self):
        baseline = {"samples": [{"text_sha256": "same"}], "cold_p95_seconds": 2.0, "warm_p95_seconds": 0.1}
        candidate = {"samples": [{"text_sha256": "other"}], "cold_p95_seconds": 2.5,
                     "warm_p95_seconds": 0.2, "detector_threshold": 0.9, "glyph_threshold": 0.75}
        with self.assertRaisesRegex(ValueError, "证据"):
            compare_latency(baseline, candidate)
        candidate["samples"][0]["text_sha256"] = "same"
        self.assertTrue(compare_latency(baseline, candidate)["latency_gate_passed"])

    def test_manager_requires_original_detector_and_cec3(self):
        with tempfile.TemporaryDirectory() as temporary:
            with patch("app.service.typo_runtime.manager.ShapeSoundGate") as gate_class:
                manager = ModelManager(
                    [], worker_url="http://unused", model_id="macbert", cec3_model_id="cec3",
                    detector_model_id="original-v1", pipeline_version=VERSION,
                    cache_path=Path(temporary) / "v5.sqlite", font_path=Path("/unused-font"),
                    font_sha256="fonthash", pinyin_version="0.55.0", detector_threshold=0.9,
                )
                gate_class.return_value.evidence.return_value = {"similarity_type": "pinyin", "glyph_similarity": 0.5}
            text = "安全培圳。"
            proposal = candidate(text, "圳", "训")
            def request(path, payload=None, timeout=3):
                if path == "/detect":
                    return {"scores": [0.1, 0.1, 0.1, 0.99, None]}
                if path == "/correct":
                    return {"candidates": [proposal], "corrected_text": "安全培训。"}
                if path == "/cec3":
                    return {"corrected_text": "安全培训。"}
                raise AssertionError(path)
            manager._start = lambda: None
            manager._request = request
            result = manager.check(text)
            self.assertEqual(result["rule_version"], VERSION)
            self.assertEqual(result["confirmed_count"], 1)
            self.assertEqual(result["review_candidates"], [])
            manager._request = lambda path, payload=None, timeout=3: (_ for _ in ()).throw(TypoUnavailable("model_failure"))
            with self.assertRaises(TypoUnavailable):
                manager.check(text + "新")


if __name__ == "__main__":
    unittest.main()
