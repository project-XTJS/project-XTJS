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


class ItemizedRateModeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.itemized_module = _load_itemized_module()

    def setUp(self):
        self.checker = self.itemized_module.ItemizedPricingChecker()

    def test_downward_rate_uses_label_fallback_when_serial_changes(self):
        tender_text = """分项报价表
1 安装服务
2 调试服务
3 培训服务
"""
        bid_text = """分项报价表
1 安装服务 下浮率 3%
3 培训服务 下浮率 2%
4 调试服务 下浮率 1%
"""

        result = self.checker.check_itemized_logic(
            bid_text,
            tender_text=tender_text,
        )

        self.assertEqual(result["mode"], "downward_rate")
        self.assertEqual(result["status"], "pass")
        missing_item = (result["checks"] or {}).get("missing_item") or {}
        self.assertEqual(missing_item.get("status"), "pass")
        self.assertEqual(missing_item.get("missing_items"), [])
        self.assertEqual(
            missing_item.get("comparison_basis"),
            "tender_vs_bid_serial_then_label",
        )
        self.assertGreaterEqual(
            int((missing_item.get("match_strategies") or {}).get("label_fallback", 0)),
            1,
        )

        extracted_labels = {
            item.get("label") for item in (result.get("evidence") or {}).get("extracted_items") or []
        }
        self.assertIn("调试服务", extracted_labels)
        self.assertNotIn("调试服务 下浮率 1%", extracted_labels)

    def test_downward_rate_still_flags_truly_missing_items(self):
        tender_text = """分项报价表
1 安装服务
2 调试服务
3 培训服务
"""
        bid_text = """分项报价表
1 安装服务 下浮率 3%
3 培训服务 下浮率 2%
"""

        result = self.checker.check_itemized_logic(
            bid_text,
            tender_text=tender_text,
        )

        self.assertEqual(result["mode"], "downward_rate")
        self.assertEqual(result["status"], "fail")
        missing_item = (result["checks"] or {}).get("missing_item") or {}
        self.assertEqual(missing_item.get("status"), "fail")
        self.assertIn("2:调试服务", missing_item.get("missing_items") or [])
        self.assertEqual(
            missing_item.get("comparison_basis"),
            "tender_vs_bid_serial",
        )


if __name__ == "__main__":
    unittest.main()
