import importlib
import sys
import types
import unittest
from pathlib import Path


def _load_itemized_module():
    root = Path(__file__).resolve().parents[1]
    for name in list(sys.modules):
        if name == "app.service.analysis.itemized" or name.startswith(
            "app.service.analysis.itemized."
        ):
            sys.modules.pop(name, None)

    package_paths = {
        "app": root / "app",
        "app.service": root / "app" / "service",
        "app.service.analysis": root / "app" / "service" / "analysis",
    }
    for name, path in package_paths.items():
        package = types.ModuleType(name)
        package.__path__ = [str(path)]
        sys.modules[name] = package

    return importlib.import_module("app.service.analysis.itemized")


class ItemizedNormalModeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.itemized_module = _load_itemized_module()

    def setUp(self):
        self.checker = self.itemized_module.ItemizedPricingChecker()

    def test_normal_mode_compares_against_opening_total_when_present(self):
        text = """分项报价表
1 安装服务 2 项 500 1000
合计 1000
开标一览表
投标总价 1200
"""

        result = self.checker.check_itemized_logic(text)

        self.assertEqual(result["mode"], "normal")
        self.assertEqual(result["status"], "fail")
        sum_check = (result["checks"] or {}).get("sum_consistency") or {}
        self.assertEqual(sum_check.get("status"), "fail")
        self.assertEqual(sum_check.get("declared_total"), "1200.00")
        self.assertEqual(sum_check.get("matched_total_label"), "投标总价")
        self.assertEqual(sum_check.get("opening_total"), "1200.00")
        self.assertEqual(sum_check.get("opening_total_status"), "fail")
        self.assertIn(
            "开标一览表总价不一致",
            " ".join(result.get("details") or []),
        )

    def test_normal_mode_passes_when_opening_total_matches(self):
        text = """分项报价表
1 安装服务 2 项 500 1000
合计 1000
开标一览表
投标总价 1000
"""

        result = self.checker.check_itemized_logic(text)

        self.assertEqual(result["mode"], "normal")
        self.assertEqual(result["status"], "pass")
        sum_check = (result["checks"] or {}).get("sum_consistency") or {}
        self.assertEqual(sum_check.get("status"), "pass")
        self.assertEqual(sum_check.get("declared_total"), "1000.00")
        self.assertEqual(sum_check.get("matched_total_label"), "投标总价")
        self.assertEqual(sum_check.get("opening_total_status"), "pass")

    def test_normal_mode_extracts_clean_item_label(self):
        text = """分项报价表
1 安装服务 2 项 500 1000
合计 1000
"""

        result = self.checker.check_itemized_logic(text)

        extracted_items = (result.get("evidence") or {}).get("extracted_items") or []
        self.assertEqual(len(extracted_items), 1)
        self.assertEqual(extracted_items[0].get("label"), "安装服务")

  
if __name__ == "__main__":
    unittest.main()
