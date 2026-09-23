import unittest

from app.router.postgresql import (
    _collect_business_format_issue_rows,
    _collect_frontend_result_issue_sections,
    _report_issue_row,
    _report_row_color,
)


class ReviewExportStatusTests(unittest.TestCase):
    def test_retired_business_scope_diagnostic_is_not_exported(self):
        rows = _collect_business_format_issue_rows({
            "issues": [{
                "bidder_name": "测试投标人",
                "check_name": "商务标完整性审查",
                "issue": {
                    "title": "商务材料组成范围待确认",
                    "status": "unclear",
                    "message": "未能确定完整的商务材料组成范围，已识别材料继续检查。",
                },
            }],
        })
        self.assertEqual(rows, [])

    def test_optional_business_material_is_not_exported(self):
        rows = _collect_business_format_issue_rows({
            "issues": [{
                "bidder_name": "测试投标人",
                "check_name": "商务标完整性审查",
                "issue": {
                    "title": "附件13 残疾人福利性单位声明函（格式）",
                    "status": "missing",
                    "message": "未找到该材料",
                    "evidence": {"is_optional": True},
                },
            }],
        })
        self.assertEqual(rows, [])

    def test_unclear_and_not_applicable_are_failed_rows_with_analysis_notes(self):
        expected = {
            "unclear": "需人工复核",
            "not_applicable": "该项不适用",
        }
        for status, note in expected.items():
            with self.subTest(status=status):
                rows = []
                _report_issue_row(
                    rows,
                    problem="测试项",
                    description="原始分析",
                    reason="测试原因",
                    status=status,
                )
                self.assertEqual(rows[0][5], "不一致/不通过")
                self.assertIn(note, rows[0][1])
                self.assertEqual(str(_report_row_color(rows[0])), "C00000")

    def test_review_only_duplicate_is_not_exported_as_correct(self):
        item = {
            "result_key": "business_bid_duplicate_clusters",
            "source_result_key": "business_bid_duplicate_clusters",
            "title": "疑似共同错字",
            "status": "unclear",
            # Simulate an index created before explicit duplicate statuses were
            # preserved: payload is unclear while the row status says passed.
            "source_status": "passed",
            "risk_level": "none",
            "review_only": True,
            "files": ["甲商务标.pdf", "乙商务标.pdf"],
            "issue": {
                "title": "疑似共同错字",
                "status": "unclear",
                "message": "仅待复核候选",
            },
        }

        sections = _collect_frontend_result_issue_sections([item])
        row = sections[0][2][0][1][0]
        self.assertEqual(row[5], "不一致/不通过")
        self.assertIn("需人工复核", row[1])
        self.assertNotIn("正确无误", row[1])


if __name__ == "__main__":
    unittest.main()
