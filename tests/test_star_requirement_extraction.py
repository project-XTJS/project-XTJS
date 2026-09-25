from __future__ import annotations

import unittest

from app.service.analysis.deviation import DeviationChecker


def _table_tender(rows, headers, *, table_page=2):
    table_bbox = [80.0, 100.0, 510.0, 700.0]
    table_text = "\n".join(" | ".join(row) for row in rows)
    return {
        "layout_sections": [
            {"page": 1, "type": "text", "text": "第三章项目需求书", "bbox": [80, 60, 300, 90]},
            {"page": table_page, "type": "table", "text": table_text, "bbox": table_bbox},
            {"page": 3, "type": "text", "text": "第四章合同条款", "bbox": [80, 60, 300, 90]},
        ],
        "table_sections": [
            {"page": table_page, "table_index": 0, "type": "table", "text": table_text, "bbox": table_bbox},
        ],
        "logical_tables": [
            {
                "page": table_page,
                "pages": [table_page],
                "table_index": 0,
                "type": "table",
                "text": table_text,
                "bbox": table_bbox,
                "headers": headers,
                "rows": rows,
            },
        ],
    }


class StarRequirementExtractionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.checker = DeviationChecker()

    def test_table_rows_are_independent_and_duplicate_sources_are_ignored(self) -> None:
        tender = _table_tender(
            [
                ["序号", "重要性", "指标项", "指标要求"],
                ["1", "★", "品牌要求", "国产品牌"],
                ["2", "★", "CPU", "Ultra 9-285H"],
                ["3", "★", "内存", "≥32GB"],
                ["4", "★", "存储", "≥1TB 固态硬盘"],
                ["5", "", "屏幕", "≥14 英寸"],
                ["技术说明", "", "", "以上参数标记“★”代表实质性指标，不满足将被拒绝"],
            ],
            ["序号", "重要性", "指标项", "指标要求"],
        )

        requirements = self.checker._extract_star_requirements(tender)

        self.assertEqual(len(requirements), 4)
        self.assertEqual(
            [item["requirement"] for item in requirements],
            ["品牌要求：国产品牌", "CPU：Ultra 9-285H", "内存：≥32GB", "存储：≥1TB 固态硬盘"],
        )
        self.assertTrue(all(item["page"] == 2 for item in requirements))

    def test_repeated_page_header_that_is_really_a_data_row_is_kept(self) -> None:
        tender = _table_tender(
            [["5", "★", "屏幕尺寸", "≥14.0 英寸"], ["6", "★", "屏幕分辨率", "≥3072×1920"]],
            ["5", "★", "屏幕尺寸", "≥14.0 英寸"],
        )

        requirements = self.checker._extract_star_requirements(tender)

        self.assertEqual(len(requirements), 2)
        self.assertIn("屏幕尺寸：≥14.0 英寸", [item["requirement"] for item in requirements])

    def test_misparsed_data_header_is_kept_when_rows_omit_it(self) -> None:
        tender = _table_tender(
            [["6", "★", "屏幕分辨率", "≥3072×1920"]],
            ["5", "★", "屏幕尺寸", "≥14.0 英寸"],
        )

        requirements = self.checker._extract_star_requirements(tender)

        self.assertEqual(
            [item["requirement"] for item in requirements],
            ["屏幕尺寸：≥14.0 英寸", "屏幕分辨率：≥3072×1920"],
        )

    def test_marker_at_start_of_indicator_name_cell(self) -> None:
        tender = _table_tender(
            [["1", "", "★CPU", "≥32 核"], ["2", "", "内存", "≥64GB"]],
            ["序号", "重要性", "指标项", "指标要求"],
        )

        requirements = self.checker._extract_star_requirements(tender)

        self.assertEqual([item["requirement"] for item in requirements], ["CPU：≥32 核"])

    def test_marker_in_late_importance_column_but_not_note_column(self) -> None:
        tender = _table_tender(
            [
                ["1", "CPU", "≥32 核", "★", ""],
                ["2", "内存", "≥64GB", "", "★"],
            ],
            ["序号", "指标名称", "指标要求", "重要性", "备注"],
        )

        requirements = self.checker._extract_star_requirements(tender)

        self.assertEqual([item["requirement"] for item in requirements], ["CPU：≥32 核"])

    def test_starred_row_heading_applies_only_to_its_numbered_children(self) -> None:
        tender = _table_tender(
            [
                ["★技术服务要求", "1.投标人应提供现场技术支持。 2.投标人应建立故障响应流程。"],
                ["", "这行属于下一项，没有星号。"],
            ],
            ["需求类别", "需求说明"],
        )

        requirements = self.checker._extract_star_requirements(tender)

        self.assertEqual(len(requirements), 2)
        self.assertTrue(all(item["requirement"].startswith("技术服务要求：") for item in requirements))
        self.assertFalse(any("下一项" in item["requirement"] for item in requirements))

    def test_multiple_markers_in_one_cell_are_independent_and_deduplicated(self) -> None:
        body = " ".join([
            "★1.提供五年原厂质保（提供原厂承诺函）。",
            "★2.提供技术服务流程。",
            "★3.故障时派技术人员到场。",
            "★4.严重故障时更换整机。",
            "★5.免费修理或更换缺陷硬件。",
            "★6.提供以下支持：6.1 全天紧急技术支持。6.2 缺陷问题技术支持。",
            "★7.安排工程师上门安装调试。",
            "▲8.报价同时提供五年维保期满后的维保方案及报价；关键备件供应能力延续至少五年。",
        ])
        tender = _table_tender([["售后服务", body]], ["项目", "要求"])
        duplicate = dict(tender["logical_tables"][0], page=2, table_index=1)
        tender["logical_tables"].append(duplicate)

        requirements = self.checker._extract_star_requirements(tender)

        self.assertEqual(len(requirements), 8)
        self.assertEqual([item["marker_type"] for item in requirements], ["star"] * 7 + ["important"])
        self.assertEqual([item["requirement_kind"] for item in requirements], ["mandatory"] * 7 + ["bonus"])
        self.assertEqual(requirements[-1]["requirement_id"], "IMP-008")
        self.assertTrue(all(item["requirement"].startswith("售后服务：") for item in requirements))
        self.assertIn("6.1", requirements[5]["requirement"])
        self.assertIn("6.2", requirements[5]["requirement"])
        self.assertFalse(any("报价" in item["requirement"] for item in requirements[:7]))

    def test_importance_marker_and_triangle_keep_scoring_semantics(self) -> None:
        tender = _table_tender(
            [
                ["1", "▲", "响应时间", "不超过2小时"],
                ["2", "△", "技术培训", "每年两次"],
                ["3", "▲", "限价", "100万元"],
                ["4", "▲", "售后服务报价", "请提供报价明细"],
            ],
            ["序号", "重要性", "指标项", "指标要求"],
        )

        requirements = self.checker._extract_star_requirements(tender)

        self.assertEqual([item["marker_type"] for item in requirements], ["important", "triangle"])
        self.assertEqual([item["requirement_kind"] for item in requirements], ["bonus", "bonus"])

    def test_legend_and_non_deviation_obligations_are_excluded(self) -> None:
        tender = {
            "pages": [
                {"page": 1, "text": "第三章服务需求书"},
                {"page": 2, "text": "\n".join([
                    "本项目书中标注“★”为关键技术参数，不满足作无效标处理。",
                    "★交货期：合同签订后15天内到货。",
                    "★付款方式：验收后30天付款。",
                    "★报价要求：预算250万元。",
                    "★评分标准：满足条件得5分。",
                    "★投标人应提交相关资质证明材料。",
                    "★投标人须提供检测报告。",
                    "标注“▲”为重要技术参数，负偏离扣分。",
                    "▲为重要技术参数，负偏离扣分。",
                    "▲须提交资质证明材料。",
                    "★产品须通过国家CCC认证。",
                    "★提供7×24小时技术支持。",
                ])},
                {"page": 3, "text": "第四章合同条款"},
            ],
        }

        requirements = self.checker._extract_star_requirements(tender)

        self.assertEqual(len(requirements), 2)
        self.assertTrue(any("CCC认证" in item["requirement"] for item in requirements))
        self.assertTrue(any("技术支持" in item["requirement"] for item in requirements))

    def test_plain_table_fallback_extracts_only_marked_row(self) -> None:
        tender = _table_tender([], [])
        tender["logical_tables"] = []
        tender["table_sections"][0]["text"] = "1 | ★ | CPU | ≥32核\n2 | | 内存 | ≥64GB"
        tender["layout_sections"][1]["text"] = tender["table_sections"][0]["text"]

        requirements = self.checker._extract_star_requirements(tender)

        self.assertEqual([item["requirement"] for item in requirements], ["CPU：≥32核"])

    def test_unmarked_children_of_a_prose_heading_do_not_create_empty_heading(self) -> None:
        tender = {
            "pages": [
                {"page": 1, "text": "第三章项目需求书\n★建设要求\n1、用户侧链路须为独享资源。\n2、带宽应保持稳定。"},
                {"page": 2, "text": "第四章合同条款"},
            ],
        }

        requirements = self.checker._extract_star_requirements(tender)

        self.assertEqual(len(requirements), 2)
        self.assertTrue(all(item["requirement"].startswith("建设要求：") for item in requirements))

    def test_wrapped_qualification_is_excluded_after_ocr_line_join(self) -> None:
        tender = {
            "layout_sections": [
                {"page": 1, "text": "第三章服务需求书"},
                {"page": 2, "text": "★点到点专线属于基础电信业务，投标人需要提", "bbox": [60, 100, 300, 120]},
                {"page": 2, "text": "供相应经营许可证等资质。", "bbox": [60, 121, 300, 141]},
                {"page": 2, "text": "★链路可用率应达到99.9%。", "bbox": [60, 150, 300, 170]},
                {"page": 3, "text": "第四章合同条款"},
            ],
        }

        requirements = self.checker._extract_star_requirements(tender)

        self.assertEqual([item["requirement"] for item in requirements], ["链路可用率应达到99.9%。"])

    def test_numbered_sibling_heading_ends_starred_heading_scope(self) -> None:
        tender = {
            "pages": [
                {"page": 1, "text": "第三章项目需求书\n1、★建设要求\n1）用户侧链路须为独享资源。\n2、运维保障\n1）维护响应应在两小时内。"},
                {"page": 2, "text": "第四章合同条款"},
            ],
        }

        requirements = self.checker._extract_star_requirements(tender)

        self.assertEqual(len(requirements), 1)
        self.assertIn("用户侧链路", requirements[0]["requirement"])

    def test_separate_important_line_does_not_extend_star_requirement(self) -> None:
        tender = {
            "pages": [
                {"page": 1, "text": "第三章项目需求书\n★操作方式：触摸屏\n▲功能：支持多种模式"},
                {"page": 2, "text": "第四章合同条款"},
            ],
        }

        requirements = self.checker._extract_star_requirements(tender)

        self.assertEqual(
            [item["requirement"] for item in requirements],
            ["操作方式：触摸屏", "功能：支持多种模式"],
        )
        self.assertEqual(requirements[1]["marker_type"], "important")

    def test_important_technical_clause_with_parenthesized_proof_is_kept(self) -> None:
        tender = {
            "pages": [
                {"page": 1, "text": "第三章项目需求书\n▲密封圈垫：食品级硅胶（以具有CMA资质的检测报告为准）。"},
                {"page": 2, "text": "第四章合同条款"},
            ],
        }

        requirements = self.checker._extract_star_requirements(tender)

        self.assertEqual(len(requirements), 1)
        self.assertEqual(requirements[0]["marker_type"], "important")

    def test_important_only_missing_bid_is_warning_not_compliance_failure(self) -> None:
        tender = {
            "pages": [
                {"page": 1, "text": "第三章项目需求书\n▲功能：支持多种模式。"},
                {"page": 2, "text": "第四章合同条款"},
            ],
        }

        result = self.checker.check_technical_deviation(tender, {"pages": []})

        self.assertEqual(result["compliance_status"], "pass")
        self.assertEqual(result["core_star_requirements_count"], 0)
        self.assertEqual(result["bonus_requirements_count"], 1)
        self.assertEqual(result["stats"]["missing_count"], 0)
        self.assertEqual(result["stats"]["bonus_flagged_count"], 1)
        self.assertEqual(result["bonus_flagged_items"][0]["marker_type"], "important")

    def test_important_only_missing_deviation_table_is_warning(self) -> None:
        tender = {
            "pages": [
                {"page": 1, "text": "第三章项目需求书\n▲功能：支持多种模式。"},
                {"page": 2, "text": "第四章合同条款"},
            ],
        }

        result = self.checker.check_technical_deviation(
            tender, {"pages": [{"page": 1, "text": "已提交设备参数响应。"}]}
        )

        self.assertEqual(result["compliance_status"], "pass")
        self.assertEqual(result["stats"]["missing_count"], 0)
        self.assertEqual(result["stats"]["bonus_flagged_count"], 1)
        self.assertEqual(result["bonus_flagged_items"][0]["marker_type"], "important")

    def test_excluded_items_do_not_enter_missing_response_stats(self) -> None:
        tender = {
            "pages": [
                {"page": 1, "text": "第三章项目需求书\n★限价：100万元。\n★CPU核心数不少于32核。"},
                {"page": 2, "text": "第四章合同条款"},
            ],
        }

        result = self.checker.check_technical_deviation(tender, {"pages": []})

        self.assertEqual(result["core_star_requirements_count"], 1)
        self.assertEqual(result["stats"]["missing_count"], 1)
        self.assertEqual(len(result["star_requirements"]), 1)
        self.assertEqual(len(result["match_results"]), 1)
        self.assertNotIn("raw_text", result["star_requirements"][0])


if __name__ == "__main__":
    unittest.main()
