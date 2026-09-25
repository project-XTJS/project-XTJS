import unittest
from unittest.mock import patch

from app.service.analysis.compliance.consistency import ConsistencyChecker
from app.service.analysis.compliance.exact_template import build_pattern, compare_pattern
from app.service.analysis.compliance.template_extractor import TemplateExtractor
from app.service.analysis.compliance.structured_consistency import StructuredConsistencyEngine
from app.service.analysis.unified import UnifiedBusinessReviewService


class ConsistencyV33Tests(unittest.TestCase):
    def setUp(self):
        self.engine = ConsistencyChecker()._structured_engine

    @staticmethod
    def table_item(mode="fixed_rows"):
        return {
            "item_id": "template:attachment-3:table_grid:test",
            "required": True,
            "enabled": True,
            "source_locations": [{"page": 48, "type": "table_cell"}],
            "table_grid": {
                "headers": ["服务板块", "服务项目", "说明", "投标报价（元）"],
                "rows": [
                    {"cells": ["会议论坛", "嘉宾邀请", "必须邀请嘉宾。", ""], "page": 48},
                    {"cells": ["", "直播及摄录", "提供直播摄录服务。", ""], "page": 48},
                ],
                "locations": [{"page": 48, "type": "table_cell"}],
                "mode": mode,
                "scope_ambiguous": False,
            },
        }

    @staticmethod
    def bid_table(rows):
        return {"_logical_tables": [{
            "id": "bid-table", "pages": [5],
            "headers": ["服务板块", "服务项目", "说明", "投标报价（元）"],
            "rows": [
                ["服务板块", "服务项目", "说明", "投标报价（元）"],
                *rows,
            ],
        }]}

    def test_fixed_table_allows_prices_but_rejects_changed_deleted_or_moved_rows(self):
        first = ["会议论坛", "嘉宾邀请", "必须邀请嘉宾。", "1,010,000.00"]
        second = ["直播及摄录", "提供直播摄录服务。", "764,000.00", ""]
        item = self.table_item()
        for rows, expected in (
            ([first, second], "pass"),
            ([first, ["直播及摄录", "仅提供照片。", "764,000.00", ""]], "fail"),
            ([first], "fail"),
            ([second, first], "fail"),
        ):
            with self.subTest(rows=rows):
                result = self.engine._evaluate_table_grid(item, self.bid_table(rows))
                self.assertEqual(result[0]["status"], "pass")
                self.assertEqual(result[1]["status"], expected)

    def test_flexible_table_does_not_compare_blank_sample_rows(self):
        result = self.engine._evaluate_table_grid(
            self.table_item("flexible"),
            self.bid_table([["服务项目", "自选项目", "已填写内容", "98,000.00"]]),
        )
        self.assertEqual([item["status"] for item in result], ["pass"])

    def test_ocr_merged_later_summary_does_not_extend_fixed_price_grid(self):
        item = self.table_item()
        item["table_grid"]["rows"].append({"cells": ["合计金额", "", "", ""]})
        section = self.bid_table([
            ["会议论坛", "嘉宾邀请", "必须邀请嘉宾。", "100"],
            ["", "直播及摄录", "提供直播摄录服务。", "200"],
            ["合计金额", "", "", "300"],
            ["会务保障", "", "", ""],
        ])
        section["_logical_tables"][0]["pages"] = [5, 7]
        self.assertEqual(self.engine._evaluate_table_grid(item, section)[1]["status"], "pass")
        section["_logical_tables"][0]["pages"] = [5, 6]
        self.assertEqual(self.engine._evaluate_table_grid(item, section)[1]["status"], "fail")

    def test_missing_table_structure_is_unclear(self):
        result = self.engine._evaluate_table_grid(self.table_item(), {"_logical_tables": []})
        self.assertEqual([item["status"] for item in result], ["unclear", "unclear"])

    def test_unparsed_price_table_does_not_compare_flattened_rows(self):
        lines = ["项目名称：____", "序号 产品名称 数量 单价", "1 设备 15 255400", "注：价格含税"]
        self.assertEqual(
            self.engine._price_table_preamble(lines), ["项目名称：____"],
        )
        item = self.table_item("unresolved")
        item["table_grid"].update(headers=[], rows=[], scope_ambiguous=True)
        result = self.engine._evaluate_table_grid(item, {"_logical_tables": []})
        self.assertEqual([entry["status"] for entry in result], ["unclear"])
        skeleton = self.engine._build_attachment_skeleton(
            {"title": "附件3 分项报价表（格式不可更改）", "content": [
                "附件3 分项报价表（格式不可更改）", *lines,
            ], "locations": [{"page": 1}]},
            {"data": {"logical_tables": []}}, {},
        )
        self.assertEqual(
            [entry["kind"] for entry in skeleton["items"]],
            ["title", "fixed_clause", "table_grid"],
        )

    def test_table_body_extraction_keeps_fixed_notes_and_field_labels(self):
        lines = [
            "项目名称：______", "招标编号：______", "服务板块", "服务项目", "说明",
            "投标报价（元）", "嘉宾邀请", "必须邀请嘉宾。", "合计金额", "注：",
            "1.所有价格均为含税价。",
        ]
        result, ambiguous = StructuredConsistencyEngine._without_table_body(
            lines,
            ["服务板块", "服务项目", "说明", "投标报价（元）"],
            [{"cells": ["合计金额", "", "", ""]}],
        )
        self.assertFalse(ambiguous)
        self.assertEqual(result, lines[:2] + lines[9:])

    def test_wrapped_fixed_price_instruction_uses_adjacent_tender_ocr(self):
        prefix = "2.此分项报价表合计金额须"
        suffix = "与开标一览表投标总价一致。"
        payload = {"data": {"layout_sections": [
            {"page": 49, "text": prefix}, {"page": 49, "text": suffix},
            {"page": 49, "text": "3.下一项说明"},
            {"page": 49, "text": "4.最后一项说明"},
            {"page": 49, "text": "投标人名称：（盖章）"},
        ]}}
        self.assertEqual(
            self.engine._complete_price_table_instructions([prefix], payload, {49}),
            [prefix + suffix, "3.下一项说明", "4.最后一项说明"],
        )
        self.assertEqual(
            self.engine._complete_price_table_instructions([prefix], payload, {48}),
            [prefix],
        )
        table_payload = {"data": {"logical_tables": [{
            "pages": [49], "headers": ["服务板块", "服务项目", "说明", "投标报价（元）"],
            "rows": [["服务板块", "服务项目", "说明", "投标报价（元）"]],
        }]}}
        skeleton = self.engine._build_attachment_skeleton(
            {"title": "附件3 分项报价表（格式不可更改）", "content": [
                "附件3 分项报价表（格式不可更改）", "4.详细费用明细清单格式自拟。",
                "服务板块 服务项目 说明 投标报价（元）",
            ], "locations": [{"page": 49}]},
            table_payload, {},
        )
        self.assertFalse(skeleton["is_self_defined"])

    def test_reference_notes_do_not_erase_declaration(self):
        lines = [
            "本公司郑重声明。", "以上企业不属于大企业分支机构。",
            "本企业对上述声明内容的真实性负责。", "企业名称（盖章）：", "日期：",
            "注：", "1.本声明函适用于所有在中国境内依法设立的企业。",
            "各行业划型标准：", "（1）农、林、牧、渔业。",
        ]
        self.assertEqual(
            StructuredConsistencyEngine._without_reference_note(lines, "附件12 中小企业声明函（格式）"),
            lines[:5],
        )
        retained = StructuredConsistencyEngine._without_reference_note(lines, "附件12 中小企业声明函（格式）")
        self.assertEqual(compare_pattern(build_pattern(retained[2]), "")['status'], "fail")

    def test_disability_declaration_note_is_reference_only(self):
        lines = [
            "本单位郑重声明。", "本单位对上述声明的真实性负责。如有虚假，将依法承担相应责任。",
            "单位名称（加盖公章）：", "注：1.享受政策的单位应同时满足条件：", "1)安置人数不少于10人。",
        ]
        self.assertEqual(
            StructuredConsistencyEngine._without_reference_note(lines, "附件13 残疾人福利性单位声明函（格式）"),
            lines[:3],
        )

    def test_signoff_fields_are_delegated_without_removing_pledge(self):
        lines = [
            "本企业对上述声明内容的真实性负责。", "特此承诺！供应商（加盖公章）：",
            "法定代表人或授权委托人（签字或盖章）：", "日 期：2026年9月",
        ]
        self.assertEqual(
            StructuredConsistencyEngine._without_signoff_fields(lines),
            lines[:1] + ["特此承诺！"],
        )
        self.assertEqual(
            StructuredConsistencyEngine._without_signoff_fields([
                "参选人 参选人名称：（盖章）", "年 月 日 后附：信用报告。",
                "被授权人签字或盖章：______",
            ]),
            ["后附：信用报告。"],
        )
        self.assertEqual(
            StructuredConsistencyEngine._without_signoff_fields(
                ["日期：", "年", "月", "日", "后附：信用报告。"]
            ),
            ["后附：信用报告。"],
        )

    def test_signoff_ocr_does_not_leave_body_coverage_unresolved(self):
        candidate = [{
            "text": "投标人名称：某公司（盖章）", "source_ids": ["source-1"],
            "source_range": {"start_order": 1, "end_order": 1, "block_count": 1},
            "locations": [{"page": 5, "type": "text"}],
        }]
        evaluated = [{"kind": "table_header", "status": "pass", "bid_locations": [{"page": 5}]}]
        self.assertFalse(self.engine._source_coverage(candidate, evaluated)["complete"])
        self.assertTrue(self.engine._source_coverage(
            candidate, evaluated, delegated_signoff=True,
        )["complete"])

    def test_confirmed_ocr_word_ambiguity_is_narrow(self):
        difference = [{"type": "replace", "template_text": "否", "bid_text": "合"}]
        self.assertTrue(self.engine._known_ocr_word_ambiguity(
            "超过限价的将被否决投标。", "超过限价的将被合决投标。", difference,
        ))
        self.assertFalse(self.engine._known_ocr_word_ambiguity(
            "不得改变服务内容。", "不得更改服务内容。", difference,
        ))

    def test_submitted_optional_declaration_remains_in_skeleton(self):
        declaration = {
            "title": "附件12 中小企业声明函（格式）",
            "content": ["附件12 中小企业声明函（格式）", "本企业对上述声明内容的真实性负责。",
                        "企业名称（盖章）：", "注：", "各行业划型标准："],
            "title_locations": [{"page": 1}],
        }
        with patch.object(TemplateExtractor, "extract_consistency_templates", return_value=[]), \
             patch.object(TemplateExtractor, "filter_business_response_attachments", return_value=([declaration], True)), \
             patch.object(self.engine, "_index_model_attachments", return_value={}):
            skeleton = self.engine.build_template_skeleton({"data": {}})
        self.assertEqual(len(skeleton), 1)
        self.assertTrue(skeleton[0]["conditional_optional_declaration"])
        self.assertIn("真实性负责", skeleton[0]["reference_text"])
        self.assertNotIn("划型标准", skeleton[0]["reference_text"])

    def test_title_words_inside_fixed_clause_are_not_dropped(self):
        lines = [
            "附件3 分项报价表（格式可根据实际情况修改）",
            "2.此分项报价表合计总价须与开标一览表投标总价一致。",
            "附件4 授权书（格式）",
        ]
        result = StructuredConsistencyEngine._truncate_at_next_attachment_heading(
            lines, title=lines[0], strict_title_only=True,
        )
        self.assertEqual(result, lines[1:2])

    def test_display_counts_failed_items_only(self):
        review = UnifiedBusinessReviewService()._normalize_consistency([{
            "name": "附件1", "status": "fail", "missing_anchors": [],
            "difference_items": [
                {"item_id": "a", "status": "fail"},
                {"item_id": "a", "status": "fail"},
                {"item_id": "b", "status": "unclear"},
            ],
        }])
        self.assertIn("1 个固定内容项存在差异", review["issues"]["failed"][0]["message"])


if __name__ == "__main__":
    unittest.main()
