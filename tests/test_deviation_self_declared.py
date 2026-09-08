from __future__ import annotations

import unittest

from app.service.analysis.deviation import DeviationChecker
from app.service.analysis.unified import UnifiedBusinessReviewService


def _tender_without_star() -> dict:
    return {
        "pages": [
            {
                "page": 1,
                "text": "项目需求书\n供应商应按响应文件格式提交偏离表。",
            }
        ]
    }


def _bid_with_deviation_row(
    *,
    requirement: str,
    response: str,
    deviation: str,
    page: int = 5,
) -> dict:
    headers = [
        "序号",
        "比选文件的项目需求",
        "响应文件的响应",
        "偏离说明",
        "对应响应文件所在页",
    ]
    return {
        "pages": [
            {
                "page": page,
                "text": (
                    "响应文件偏离表-技术部分\n"
                    "序号 比选文件的项目需求 响应文件的响应 偏离说明\n"
                    f"1 {requirement} {response} {deviation}\n"
                    "注：对不满足比选文件要求的部分，必须明确如实填写并说明原因。"
                ),
            }
        ],
        "logical_tables": [
            {
                "pages": [page],
                "bbox": [31.0, 203.0, 580.0, 430.0],
                "headers": headers,
                "records": [
                    {
                        "序号": "1",
                        "比选文件的项目需求": requirement,
                        "响应文件的响应": response,
                        "偏离说明": deviation,
                        "对应响应文件所在页": "技术文件第10页",
                    }
                ],
            }
        ],
    }


class SelfDeclaredDeviationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.checker = DeviationChecker()

    def test_negative_deviation_is_reported_without_star_requirement(self) -> None:
        bid = _bid_with_deviation_row(
            requirement="周期性开展现场会议",
            response="负偏离，不开展周期性现场会议，改为线上会议",
            deviation="技术文件第10页",
        )

        result = self.checker.check_technical_deviation(_tender_without_star(), bid)

        self.assertEqual(result["compliance_status"], "fail")
        self.assertEqual(result["deviation_status"], "self_declared_negative_deviation")
        self.assertEqual(result["core_star_requirements_count"], 0)
        self.assertEqual(result["self_declared_negative_count"], 1)
        self.assertEqual(len(result["negative_deviation_items"]), 1)
        item = result["negative_deviation_items"][0]
        self.assertEqual(item["marker_type"], "self_declared")
        self.assertEqual(item["response_page"], 5)
        self.assertEqual(item["response_bbox"], [31.0, 203.0, 580.0, 430.0])
        self.assertEqual(item["response_document_role"], "business_bid")
        self.assertIn("负偏离", item["response_evidence"])

    def test_partial_compliance_is_treated_as_negative_deviation(self) -> None:
        bid = _bid_with_deviation_row(
            requirement="关键帧误差不超过正负一帧",
            response="部分满足，约有20%的关键帧超过误差要求",
            deviation="部分满足",
        )

        result = self.checker.check_technical_deviation(_tender_without_star(), bid)

        self.assertEqual(result["compliance_status"], "fail")
        self.assertEqual(result["self_declared_negative_count"], 1)
        self.assertIn("部分满足", result["negative_deviation_items"][0]["response_evidence"])

    def test_explicit_negative_wins_over_full_response_for_other_parts(self) -> None:
        bid = _bid_with_deviation_row(
            requirement="周期性开展现场会议",
            response="负偏离，不开展现场会议；其余条款完全响应",
            deviation="负偏离",
        )

        result = self.checker.check_technical_deviation(_tender_without_star(), bid)

        self.assertEqual(result["compliance_status"], "fail")
        self.assertEqual(result["self_declared_negative_count"], 1)

    def test_negated_negative_phrase_is_not_reported(self) -> None:
        bid = _bid_with_deviation_row(
            requirement="全部技术需求",
            response="全部满足",
            deviation="不存在负偏离",
        )

        result = self.checker.check_technical_deviation(_tender_without_star(), bid)

        self.assertEqual(result["compliance_status"], "pass")
        self.assertEqual(result["negative_deviation_items"], [])

    def test_no_deviation_with_template_note_is_not_false_positive(self) -> None:
        bid = _bid_with_deviation_row(
            requirement="全部技术需求",
            response="全部满足",
            deviation="无偏离",
        )

        result = self.checker.check_technical_deviation(_tender_without_star(), bid)

        self.assertEqual(result["compliance_status"], "pass")
        self.assertEqual(result["deviation_status"], "no_star_requirements")
        self.assertEqual(result["negative_deviation_items"], [])

    def test_filled_deviation_explanation_is_located_for_manual_review(self) -> None:
        bid = _bid_with_deviation_row(
            requirement="合同签订地法院管辖",
            response="改为诉讼发起地法院管辖",
            deviation="提起诉讼地变更为诉讼发起地",
        )

        result = self.checker.check_technical_deviation(_tender_without_star(), bid)

        self.assertEqual(result["compliance_status"], "unclear")
        self.assertEqual(result["self_declared_unclear_count"], 1)
        self.assertEqual(result["unclear_response_items"][0]["response_page"], 5)

    def test_normalized_issue_preserves_bid_page_location(self) -> None:
        bid = _bid_with_deviation_row(
            requirement="周期性开展现场会议",
            response="负偏离，不开展周期性现场会议",
            deviation="技术文件第10页",
        )
        raw = self.checker.check_technical_deviation(_tender_without_star(), bid)
        normalized = UnifiedBusinessReviewService()._normalize_deviation(raw)

        self.assertEqual(normalized["review"]["status"], "fail")
        self.assertIn("主动填写偏离", normalized["review"]["summary"])
        issue = normalized["issues"]["failed"][0]
        self.assertEqual(issue["page"], 5)
        self.assertEqual(issue["evidence"]["response_page"], 5)
        self.assertEqual(
            issue["evidence"]["response_locations"][0]["document_role"],
            "business_bid",
        )


if __name__ == "__main__":
    unittest.main()
