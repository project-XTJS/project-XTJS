# -*- coding: utf-8 -*-
"""招标文件审查模块回归测试：覆盖已修复的提取/判定 bug。"""

from __future__ import annotations

import unittest

from app.service.analysis.tender_compliance import TenderComplianceChecker


def _check(*texts: str):
    payload = {"layout_sections": [{"type": "text", "text": text} for text in texts]}
    result = TenderComplianceChecker().check(payload)
    return {item["code"]: item for item in result["checks"]}


class TenderComplianceRegressionTest(unittest.TestCase):
    def test_budget_only_treated_as_limit_synonym(self):
        """预算与限价视为同义：文档只有预算时，检查通过（未发现矛盾）。"""
        checks = _check("项目概况：本项目预算金额约为 1500 万元（含税）。")
        self.assertEqual(checks["budget_vs_limit"]["status"], "pass")

    def test_budget_below_limit_is_fail(self):
        """预算与限价同义：预算低于限价属于文档矛盾，判不通过。"""
        checks = _check(
            "招标公告：项目预算为 100 万元。",
            "招标公告：本项目最高限价为 120 万元。",
        )
        self.assertEqual(checks["budget_vs_limit"]["status"], "fail")

    def test_bid_security_amount_not_mixed_into_limit(self):
        """投标保证金（限价的 2%，即 ¥金额）不得被当作最高限价候选。"""
        checks = _check(
            "招标公告：最高限价为 9800000 元。",
            "投标人须知：最高限价 9800000 元。",
            "报价章节：最高限价 9800000 元。",
            "投标人须知：投标保证金为最高限价的 2%，即 196000 元。",
        )
        self.assertEqual(checks["limit_consistency"]["status"], "pass")

    def test_payment_consistency_tolerates_phrasing(self):
        """同一付款方式的不同措辞（签订后支付/预付）应判定一致。"""
        checks = _check(
            "须知前附表：付款方式：合同签订后支付预付款 30%，验收合格后支付 70%。",
            "技术需求：付款方式：预付款 30%，验收合格后支付 70%。",
            "合同条款：付款方式：合同签订后支付预付款 30%，验收合格后支付 70%。",
        )
        self.assertEqual(checks["payment_consistency"]["status"], "pass")

    def test_range_detector_ignores_years_and_serials(self):
        """年份/编号（2025-2027、XTJS2025-263）不得被当作分值区间。"""
        checks = _check(
            "评标办法：本项目实施期为 2025-2027 年度，项目编号 XTJS2025-263。"
        )
        values = checks["evaluation_method"]["values"]
        self.assertNotIn("分值区间 2025-2027 重复", values.get("anomalies") or [])
        self.assertNotIn("分值区间 2025-263 反向", values.get("anomalies") or [])

    def test_adjacent_score_ranges_not_reported_as_gap(self):
        """相邻整数区间（0-6 与 7-14）不应判为断层。"""
        checker = TenderComplianceChecker()
        anomalies = checker._score_range_anomalies(
            "测试项",
            [
                {"start": 0, "end": 6},
                {"start": 7, "end": 14},
                {"start": 15, "end": 20},
            ],
        )
        self.assertFalse(any("断层" in item for item in anomalies))

    def test_category_total_row_not_double_counted(self):
        """类别总分行（（二）技术部分得分 75）不得与细项重复累计。"""
        checks = _check(
            "评标办法：总分 100 分，商务分 25 分、技术分 75 分。",
            "技术评分标准：技术部分满分 75 分：技术水平评价 20 分，服务质量保障措施 10 分。",
        )
        status = checks["evaluation_method"]["status"]
        message = checks["evaluation_method"]["message"]
        # 解析不完整时应为 unclear（人工复核），而不是把类别总分重复计入后的错误 fail。
        self.assertNotIn("细项满分合计 95", message)
        self.assertIn(status, {"unclear", "fail"})

    def test_real_item_sum_mismatch_still_fails(self):
        """解析完整时，细项加总与大类分值不符仍应判 fail。"""
        checks = _check(
            "评标办法：总分 100 分，商务分 30 分、技术分 50 分、价格分 20 分。",
            "商务评分标准：商务部分满分 30 分：业绩 10 分，资质 10 分，人员 5 分。",
        )
        self.assertEqual(checks["evaluation_method"]["status"], "fail")

    def test_category_sum_mismatch_still_fails(self):
        """商务+技术+价格 与总分不符仍应判 fail。"""
        checks = _check("评标办法：总分 100 分，商务分 30 分、技术分 50 分、价格分 30 分。")
        self.assertEqual(checks["evaluation_method"]["status"], "fail")

    def test_zong_limited_price_and_multi_lot(self):
        """'总限价'（带 /年 单位、多包件）应能识别限价并计算保证金比例；多包件限价判 unclear 而非冲突。"""
        checks = _check(
            "投标人须知：采购预算 人民币 201.6 万元。有，最高限价为：包件1：租车服务费总限价人民币 172.8 万元/年；包件2：总限价人民币 28.8 万元/年。",
            "报价章节：包件1 总限价 172.8 万元/年，包件2 总限价 28.8 万元/年。",
            "投标人须知：包件1 应答保证金为人民币 30000 元，包件2 应答保证金为人民币 5000 元。",
        )
        self.assertEqual(checks["budget_vs_limit"]["status"], "pass")
        self.assertEqual(checks["limit_consistency"]["status"], "unclear")
        self.assertEqual(checks["bid_security_ratio"]["status"], "pass")
        ratio = checks["bid_security_ratio"]["values"].get("ratio_percent")
        self.assertIsNotNone(ratio)
        self.assertLessEqual(float(ratio), 2.0)

    def test_performance_security_not_required_pass(self):
        """文档明确'不设置/不收取'履约保证金时应判 pass，而不是 unclear。"""
        checks = _check("须知：履约保证金：本项目不设置履约保证金。")
        self.assertEqual(checks["performance_security_consistency"]["status"], "pass")

    def test_performance_security_not_required_plain_pass(self):
        """'不收取履约保证金'（无复选框）也应判 pass（排除词表不能把'履约保证金'自身排除）。"""
        checks = _check("合同条款：履约保证金：不收取履约保证金。")
        self.assertEqual(checks["performance_security_consistency"]["status"], "pass")

    def test_bid_security_checkbox_checked_set_is_required(self):
        """复选框中'□不收取 ■收取'时，应按收取处理并计算比例，不能误判免收。"""
        checks = _check(
            "投标人须知：最高限价 100 万元。",
            "投标人须知：□本项目不收取投标保证金 ■本项目收取投标保证金，金额为 1 万元。",
        )
        self.assertEqual(checks["bid_security_ratio"]["status"], "pass")
        self.assertEqual(checks["bid_security_ratio"]["values"].get("mode"), None)

    def test_bid_security_checkbox_checked_not_required(self):
        """复选框中'■无须提交 □设置'时，应判免收。"""
        checks = _check("投标人须知：■本项目无须提交投标保证金 □本项目设置投标保证金。")
        self.assertEqual(checks["bid_security_ratio"]["status"], "pass")
        self.assertEqual(checks["bid_security_ratio"]["values"].get("mode"), "not_required")

    def test_limit_inferred_from_budget_when_same(self):
        """'最高投标限价同预算'且无具体金额时，用预算推断限价。"""
        checks = _check(
            "投标人须知：项目预算 570 万元。",
            "投标人须知：5. 最高投标限价 同预算，报价超过最高限价的投标将被否决。",
        )
        self.assertEqual(checks["budget_vs_limit"]["status"], "pass")

    def test_evaluation_two_categories_with_price_weight(self):
        """商务+技术两类=总分（价格按权值计分）不应误判'缺价格分'为不通过。"""
        checks = _check(
            "评标办法：综合评分法，总分100分，各投标人的商务部分得分与技术部分得分之和为总得分。",
            "（一）商务部分得分 | 满分30分。",
            "（二）技术部分得分 | 满分70分。",
            "投标报价得分按价格权值30%计算。",
        )
        evaluation = checks["evaluation_method"]
        values = evaluation["values"]
        self.assertEqual(values.get("category_scores", {}).get("business"), 30.0)
        self.assertEqual(values.get("category_scores", {}).get("technical"), 70.0)
        self.assertNotIn("商务分+技术分+价格分=68", evaluation["message"])

    def test_bid_security_ratio_uses_budget_as_denominator(self):
        """无最高限价但有预算时（预算与限价同义），用预算作分母计算保证金比例。"""
        checks = _check(
            "招标公告：本项目预算金额约为 1500 万元。",
            "投标人须知：□本项目不收取投标保证金 ■本项目收取投标保证金，包件一金额为 0.8 万元。",
        )
        result = checks["bid_security_ratio"]
        self.assertEqual(result["status"], "pass")
        self.assertEqual(result["values"].get("denominator"), "budget")
        self.assertLessEqual(float(result["values"].get("ratio_percent")), 2.0)


if __name__ == "__main__":
    unittest.main()
