import json
import unittest
from pathlib import Path

from app.service.analysis.itemized_pricing import ItemizedPricingChecker


PROJECT_ROOT = Path(__file__).resolve().parents[1]
FIXTURE_DIR = PROJECT_ROOT / "ocr_results" / "450"
TENDER_PATH = FIXTURE_DIR / "450-model.json"


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


class ItemizedPricing450RegressionTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.checker = ItemizedPricingChecker()
        cls.tender_payload = _load_json(TENDER_PATH)

    def _run_case(self, bid_name: str) -> dict:
        bid_payload = _load_json(FIXTURE_DIR / bid_name)
        return self.checker.check_itemized_logic(bid_payload, tender_text=self.tender_payload)

    def test_liankun_passes(self) -> None:
        result = self._run_case("450-liankun.json")

        self.assertEqual(result["status"], "pass")
        self.assertTrue(result["passed"])
        self.assertEqual(result["checks"]["sum_consistency"]["status"], "pass")
        self.assertGreaterEqual(result["evidence"]["extracted_item_count"], 5)

    def test_zhengsheng_passes(self) -> None:
        result = self._run_case("450-zhengsheng.json")

        self.assertEqual(result["status"], "pass")
        self.assertTrue(result["passed"])
        self.assertEqual(result["checks"]["sum_consistency"]["status"], "pass")
        self.assertGreaterEqual(result["evidence"]["extracted_item_count"], 5)

    def test_zhisui_passes(self) -> None:
        result = self._run_case("450-zhisui.json")

        self.assertEqual(result["status"], "pass")
        self.assertTrue(result["passed"])
        self.assertEqual(result["checks"]["sum_consistency"]["status"], "pass")
        self.assertGreaterEqual(result["evidence"]["extracted_item_count"], 5)


if __name__ == "__main__":
    unittest.main()
