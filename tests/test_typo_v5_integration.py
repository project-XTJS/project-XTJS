"""V5 offsets and two-sided duplicate alignment through the existing client."""

import unittest

from app.service.analysis.typo_client import DuplicateTypoService, common_word_edits
from app.service.typo_runtime.v5 import VERSION, classify_v5


class ShapeEvidence:
    def evidence(self, original, replacement):
        return {"similarity_type": "pinyin", "glyph_similarity": 0.7}


class TypoV5IntegrationTests(unittest.TestCase):
    def test_same_word_twice_is_two_common_errors_with_exact_locations(self):
        text = "😀安全培圳，培圳。"
        candidates = [
            {"start": start, "end": start + 1, "original": "圳", "replacement": "训",
             "candidate_probability": 0.2, "source_probability": 0.1,
             "probability_ratio": 2, "detector_score": 0.99}
            for start in (4, 7)
        ]
        corrected = "😀安全培训，培训。"
        issues, _ = classify_v5(text, candidates, corrected, corrected, gate=ShapeEvidence())
        self.assertEqual([item["start"] for item in issues], [4, 7])
        payload = {"model": "macbert", "reference_model": "cec3", "detector_model": "v5-detector",
                   "font_sha256": "font", "pinyin_version": "0.55.0", "rule_version": VERSION}
        client = DuplicateTypoService()
        def project(side, filename):
            snippet = {
                "text": text, "side": side, "document_identifier_id": filename,
                "file_name": filename, "source_location_reliable": True,
                "source_ref": {"evidence_id": "same-para", "kind": "block"},
                "segments": [{"start": 0, "end": len(text), "page": 2, "bbox": [0, 0, 10, 10]}],
            }
            return [client._project_issue(item, offset=0, source_text=text, snippet=snippet, payload=payload)
                    for item in issues]
        left = project("left", "A.pdf")
        right = project("right", "B.pdf")
        common = common_word_edits(text, text, left, right)
        self.assertEqual(len(common), 2)
        self.assertEqual(len({item["shared_id"] for item in common}), 2)
        self.assertEqual([item["occurrences"][0]["start"] for item in common], [4, 7])
        self.assertTrue(all(item["detector_model"] == "v5-detector" for item in common))
        self.assertEqual(common_word_edits(text, text, left, []), [])
        self.assertEqual(len(common_word_edits(text, text, left, project("right", "C.pdf"))), 2)


if __name__ == "__main__":
    unittest.main()
