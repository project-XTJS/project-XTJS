import unittest
from pathlib import Path

from app.service.analysis.itemized_pricing import ItemizedPricingChecker, _load_input_for_local_test


PROJECT_ROOT = Path(__file__).resolve().parents[1]
NEW_RESPONSE_SAMPLE = PROJECT_ROOT / "ocr_results" / "369" / "新response.json"
LEGACY_RESPONSE_SAMPLE = PROJECT_ROOT / "ocr_results" / "369" / "投标2（商务标文件）.json"


class ItemizedPricingRegressionTest(unittest.TestCase):
    def setUp(self) -> None:
        self.checker = ItemizedPricingChecker()

    def _run_checker(self, path: Path) -> dict:
        payload = _load_input_for_local_test(path)
        return self.checker.check_itemized_logic(payload)

    def test_new_response_keeps_continuation_rows_and_declared_total(self) -> None:
        result = self._run_checker(NEW_RESPONSE_SAMPLE)
        evidence = result.get("evidence") or {}
        extracted_items = evidence.get("extracted_items") or []
        total_candidates = evidence.get("total_candidates") or []
        sum_check = (result.get("checks") or {}).get("sum_consistency") or {}

        self.assertEqual(result.get("status"), "pass")
        self.assertGreaterEqual(len(extracted_items), 25)
        self.assertTrue(any("培训" in str(item.get("label") or "") for item in extracted_items))
        self.assertTrue(any("静区测试" in str(item.get("label") or "") for item in extracted_items))
        self.assertFalse(any(str(item.get("label") or "").startswith("小计") for item in extracted_items))
        self.assertEqual(sum_check.get("total_mode"), "preferential_total")
        self.assertEqual(sum_check.get("matched_total_label"), "小计")
        self.assertEqual(sum_check.get("preferential_total"), "4280000.00")
        self.assertTrue(
            any(
                item.get("label") == "小计" and item.get("amount") == "5843695.81"
                for item in total_candidates
            )
        )
        self.assertTrue(
            any(
                "最终优惠价" in str(item.get("label") or "") and item.get("amount") == "4280000.00"
                for item in total_candidates
            )
        )

    def test_legacy_response_still_passes(self) -> None:
        result = self._run_checker(LEGACY_RESPONSE_SAMPLE)
        self.assertEqual(result.get("status"), "pass")


if __name__ == "__main__":
    unittest.main()
