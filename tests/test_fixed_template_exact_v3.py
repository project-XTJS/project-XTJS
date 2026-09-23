import unittest
from unittest.mock import patch

from app.service.analysis.compliance.exact_template import (
    LEGACY_VERSION,
    V31_VERSION,
    VERSION,
    build_pattern,
    build_pattern_v31,
    character_differences,
    compare_pattern,
    compare_pattern_v31,
    compare_pattern_legacy,
)
from app.service.analysis.compliance.structured_consistency import StructuredConsistencyEngine
from app.service.analysis.compliance.consistency import ConsistencyChecker
from app.config.settings import settings
from app.service.analysis.compliance.template_pdf_evidence import native_text_conflicted


class FixedTemplateExactV3Tests(unittest.TestCase):
    def assert_status(self, template, bid, status):
        self.assertEqual(compare_pattern(build_pattern(template), bid)["status"], status)

    def test_version(self):
        self.assertEqual(VERSION, "fixed-template-exact-v3.2")
        self.assertEqual(V31_VERSION, "fixed-template-exact-v3.1")
        self.assertEqual(LEGACY_VERSION, "fixed-template-exact-v3")

    def test_only_declared_values_may_change(self):
        self.assert_status(
            "姓名：______；公司：\\underline{\\text{公司名称}}；金额：______元。",
            "姓名：张三；公司：甲公司；金额：1200元。",
            "pass",
        )

    def test_obligation_synonym_is_not_equal(self):
        self.assert_status("供应商必须遵守本条款。", "供应商应当遵守本条款。", "fail")

    def test_digit_decimal_case_width_and_punctuation_are_fixed(self):
        for template, bid in (
            ("费率为1.5%。", "费率为15%。"),
            ("型号ABC。", "型号abc。"),
            ("金额：100元。", "金额:100元。"),
            ("宽度10cm。", "宽度１０cm。"),
        ):
            with self.subTest(template=template, bid=bid):
                self.assert_status(template, bid, "fail")

    def test_extra_fixed_condition_fails(self):
        self.assert_status("我方接受全部条款。", "我方接受全部条款，但仅限本年度。", "fail")

    def test_bid_underline_cannot_expand_template_slot(self):
        self.assert_status("我方不得修改。", "我方\\underline{可以}修改。", "fail")

    def test_underlined_fixed_text_remains_fixed(self):
        self.assert_status(
            "我方\\underline{愿承担全部责任}。",
            "我方\\underline{愿承担部分责任}。",
            "fail",
        )

    def test_layout_line_break_is_ignored_but_space_boundary_remains(self):
        self.assert_status("我方接受全部\n条款。", "我方接受全部 条款。", "pass")

    def test_multiple_inline_fields_keep_reading_order(self):
        pattern = build_pattern("报价（小写）：120元（大写）：壹佰贰拾元")
        result = compare_pattern(pattern, "报价（小写）：300元（大写）：叁佰元")
        self.assertEqual(result["status"], "pass")
        self.assertEqual(result["captures"], ["300", "叁佰"])
        self.assertEqual([slot.index for slot in pattern.slots], [0, 1])
        self.assertEqual([slot.label for slot in pattern.slots], ["小写", "大写"])

    def test_non_fill_bracket_text_is_preserved(self):
        self.assert_status("项目（以下简称甲方）不得变更。", "项目不得变更。", "fail")

    def test_field_label_does_not_swallow_an_obligation(self):
        self.assert_status("单位名称：必须与营业执照一致", "单位名称：任意内容", "fail")

    def test_adjacent_data_fields_are_bounded_by_fixed_labels(self):
        self.assert_status(
            "7. 在册人数： （三）其他情况：",
            "7.在册人数：55人（三）其他情况：无",
            "pass",
        )

    def test_value_before_fixed_parenthetical_instruction_is_fillable(self):
        self.assert_status(
            "1. 专业人员分类及人数：（有专业职称人数及职称情况）",
            "1.专业人员分类及人数：3（有专业职称人数及职称情况）",
            "pass",
        )

    def test_ranges_are_zero_based_half_open_unicode_codepoints(self):
        differences = character_differences("甲😀不得。", "甲😀可以。")
        self.assertTrue(differences)
        replacement = next(item for item in differences if item["type"] == "replace")
        self.assertEqual(replacement["template_range"], {"start": 2, "end": 4})
        self.assertEqual(replacement["bid_range"], {"start": 2, "end": 4})

    def test_corrupt_native_pdf_text_layer_is_unclear_evidence(self):
        self.assertTrue(native_text_conflicted("正常正文\x01后续正文"))
        self.assertTrue(native_text_conflicted("人民币表<，单=为元>"))
        self.assertFalse(native_text_conflicted("固定条款 ABC-123，金额>0"))

    def test_declared_value_is_not_reported_as_fixed_difference(self):
        result = compare_pattern(
            build_pattern("姓名：______；必须遵守。"),
            "姓名：张三；应当遵守。",
        )
        self.assertEqual(result["status"], "fail")
        self.assertEqual(
            [(item["template_text"], item["bid_text"]) for item in result["differences"]],
            [("必须", "应当")],
        )

    def test_legacy_comparator_remains_available(self):
        result = compare_pattern_legacy(build_pattern("姓名：______"), "姓名：张三")
        self.assertEqual(result["status"], "pass")

    def test_v31_parser_and_comparator_are_frozen_for_rollback(self):
        pattern = build_pattern_v31("7. 在册人数： （三）其他情况：")
        self.assertNotIn("（三）", pattern.pattern_text)
        result = compare_pattern_v31(pattern, "7.在册人数：55人（三）其他情况：无")
        self.assertEqual(result["status"], "pass")

    def test_v32_preserves_numbered_next_field_and_reports_fixed_punctuation(self):
        pattern = build_pattern("1. 单位名称： （二）地址：")
        self.assertIn("（二）地址：", pattern.pattern_text)
        result = compare_pattern(pattern, "1、单位名称：甲公司（二）地址：北京")
        self.assertEqual(result["status"], "fail")
        self.assertEqual(
            [(item["template_text"], item["bid_text"]) for item in result["differences"]],
            [(".", "、")],
        )
        self.assertEqual(result["captures"], ["甲公司", "北京"])

    def test_v32_maps_compact_identity_form_fields_without_swallowing_labels(self):
        template = "兹证明（姓名），性别 ，年龄 ，身份证号码 ，现任我单位（职务）。"
        bid = "兹证明张三，性别男，年龄35，身份证号码310000，现任我单位经理。"
        result = compare_pattern(build_pattern(template), bid)
        self.assertEqual(result["status"], "pass")
        self.assertEqual(result["captures"], ["张三", "男", "35", "310000", "经理"])

    def test_formula_url_and_normal_acronyms_are_not_ocr_failures(self):
        engine = StructuredConsistencyEngine.__new__(StructuredConsistencyEngine)
        for text in (
            "单月合计总价=各项单价报价*预估数量之和。",
            "查询网址：https://www.creditchina.gov.cn/",
            "提供 SLA 和 AI PC 服务。",
        ):
            with self.subTest(text=text):
                self.assertIsNone(engine._explicit_text_issue_v32({"text": text}))

    def test_explicit_corrupt_text_evidence_remains_unclear(self):
        engine = StructuredConsistencyEngine.__new__(StructuredConsistencyEngine)
        issue = engine._explicit_text_issue_v32({"text": "固定正文\ufffd后续"})
        self.assertEqual(issue[0], "source_text_unavailable")

    def test_synthetic_table_columns_are_not_document_headers(self):
        entries = StructuredConsistencyEngine._logical_table_headers(
            {
                "logical_tables": [{
                    "pages": [4],
                    "headers": ["col_1", "col_2"],
                    "rows": [["项目名称：", "设备采购"], ["报价：", "100元"]],
                }]
            },
            {4},
        )
        self.assertFalse(any("col_1" in entry["text"] for entry in entries))
        self.assertTrue(any(entry["kind"] == "form_row" for entry in entries))

    def test_full_document_hit_outside_attachment_reports_scope_not_ocr(self):
        engine = StructuredConsistencyEngine.__new__(StructuredConsistencyEngine)
        engine.embedding = None
        item = {
            "item_id": "item-1", "kind": "fixed_clause", "label": "履约能力",
            "reference_text": "具备履行合同所必需的设备和专业技术能力。",
            "required": True, "enabled": True, "source_scope_status": "resolved",
            "source_locations": [{"page": 2}], "template_pattern": {},
        }
        document = StructuredConsistencyEngine._candidate_records(
            {"sections": [{
                "text": item["reference_text"], "page": 25, "bbox": [1, 2, 300, 20]
            }]},
            source_identity="project-260",
        )
        result = engine._evaluate_item_v32(
            item,
            assignment=None,
            local_candidates=[],
            document_candidates=document,
            attachment_pages={70},
            attachment_match={"confidence": "high"},
        )
        self.assertEqual(result["status"], "unclear")
        self.assertEqual(result["unclear_reasons"][0]["code"], "attachment_scope_unclear")
        self.assertEqual(
            result["unclear_reasons"][0]["search_scope"]["matched_pages"],
            [25],
        )

    def test_repeated_page_edge_is_removed_but_same_body_text_is_retained(self):
        sections = [
            {"text": "项目名称页眉", "page": page, "bbox": [10, 10, 200, 30], "page_height": 800}
            for page in range(1, 5)
        ] + [{
            "text": "项目名称页眉属于正文固定内容。", "page": 2,
            "bbox": [10, 300, 300, 340], "page_height": 800,
        }]
        repeated = StructuredConsistencyEngine._repeated_page_edge_texts(sections)
        self.assertIn("项目名称页眉", repeated)
        records = StructuredConsistencyEngine._document_candidate_records(
            {"layout_sections": sections}, source_identity="fixture"
        )
        self.assertTrue(any("属于正文固定内容" in record["text"] for record in records))
        self.assertFalse(any(record["text"] == "项目名称页眉" for record in records))

    def test_same_source_windows_do_not_create_alignment_ambiguity(self):
        engine = StructuredConsistencyEngine(ConsistencyChecker())
        item = engine._make_item(
            "attachment-1", "fixed_clause", "固定条款", "固定条款", True, [], "fixture"
        )
        item["source_scope_status"] = "resolved"
        candidates = [
            {
                "text": "固定条款", "source_ids": ["same-source"], "locations": [{"page": 1}],
                "source_spans": [{"source_id": "same-source"}],
                "source_range": {"start_order": order, "end_order": order, "block_count": 1},
            }
            for order in (1, 2)
        ]
        with patch.object(settings, "CONSISTENCY_TEMPLATE_ENGINE_VERSION", VERSION):
            assignment = engine._align_items_v32([item], candidates)[item["item_id"]]
        self.assertFalse(assignment["ambiguous"])

    def test_different_source_repeated_text_remains_locally_ambiguous(self):
        engine = StructuredConsistencyEngine(ConsistencyChecker())
        item = engine._make_item(
            "attachment-1", "fixed_clause", "固定条款", "固定条款", True, [], "fixture"
        )
        item["source_scope_status"] = "resolved"
        candidates = [
            {
                "text": "固定条款", "source_ids": [f"source-{order}"], "locations": [{"page": order}],
                "source_spans": [{"source_id": f"source-{order}"}],
                "source_range": {"start_order": order, "end_order": order, "block_count": 1},
            }
            for order in (1, 2)
        ]
        with patch.object(settings, "CONSISTENCY_TEMPLATE_ENGINE_VERSION", VERSION):
            assignment = engine._align_items_v32([item], candidates)[item["item_id"]]
        self.assertTrue(assignment["ambiguous"])

    def test_246_field_group_full_flow_maps_each_original_line(self):
        engine = StructuredConsistencyEngine(ConsistencyChecker())

        def make_item(text):
            item = engine._make_item(
                "attachment-5", "fixed_clause", text, text, True,
                [{"page": 29}], "fixture",
            )
            item["source_scope_status"] = "resolved"
            item["template_pattern"] = engine._public_pattern(build_pattern(text))
            return item

        skeleton = {
            "title": "附件5 参选人基本情况表（格式）",
            "reference_text": "1. 单位名称：\n2. 地址：\n3. 邮编：",
            "items": [make_item(value) for value in ("1. 单位名称：", "2. 地址：", "3. 邮编：")],
            "is_self_defined": False,
            "template_locations": [{"page": 29}],
            "underline_projection": {},
        }
        section = {
            "title": skeleton["title"],
            "text": "1、单位名称：甲公司 2. 地址：北京 3. 邮编：100000",
            "pages": [9],
            "sections": [{
                "id": "ocr-246-page-9",
                "text": "1、单位名称：甲公司 2. 地址：北京 3. 邮编：100000",
                "page": 9,
                "bbox": [10, 20, 500, 50],
            }],
            "_underline_evidence": {}, "_underline_locations": [], "_table_headers": [],
            "_source_identity": "project-246", "_document_candidates": [],
        }
        with patch.object(settings, "CONSISTENCY_TEMPLATE_ENGINE_VERSION", VERSION):
            result = engine._evaluate_attachment(
                skeleton, section, {"confidence": "high", "method": "title_exact"}
            )
        self.assertEqual(result["status"], "fail")
        self.assertEqual(
            [item["status"] for item in result["element_results"]],
            ["fail", "pass", "pass"],
        )
        self.assertEqual(
            result["element_results"][0]["differences"][0]["bid_text"],
            "、",
        )
        self.assertTrue(result["coverage"]["source_coverage"]["complete"])

    def test_field_candidate_stops_before_next_fixed_label(self):
        records = StructuredConsistencyEngine._candidate_records(
            {
                "sections": [{
                    "text": "1. 单位名称：甲公司 2. 地址：北京",
                    "page": 3,
                    "bbox": [10, 20, 300, 50],
                }]
            },
            source_identity="document-v1",
        )
        texts = [record["text"] for record in records]
        self.assertIn("1. 单位名称：甲公司", texts)
        self.assertIn("2. 地址：北京", texts)
        self.assertNotIn("1. 单位名称：甲公司 2. 地址：北京", texts)

    def test_three_blocks_keep_all_source_locations(self):
        records = StructuredConsistencyEngine._candidate_records(
            {
                "sections": [
                    {"text": "我方保证", "page": 1, "bbox": [1, 1, 10, 10]},
                    {"text": "严格遵守", "page": 1, "bbox": [1, 11, 10, 20]},
                    {"text": "全部固定条款", "page": 2, "bbox": [1, 1, 10, 10]},
                ]
            },
            source_identity="document-v1",
        )
        joined = next(
            record
            for record in records
            if record["text"] == "我方保证 严格遵守 全部固定条款"
        )
        self.assertEqual(joined["source_range"]["block_count"], 3)
        self.assertEqual(len(joined["source_spans"]), 3)
        self.assertEqual([item["page"] for item in joined["locations"]], [1, 1, 2])

    def test_long_source_text_is_not_truncated_for_location_matching(self):
        text = "固定内容" * 40
        records = StructuredConsistencyEngine._candidate_records(
            {"sections": [{"text": text, "page": 8, "bbox": [1, 2, 3, 4]}]},
            source_identity="document-v1",
        )
        self.assertEqual(records[0]["text"], text)
        self.assertEqual(records[0]["locations"][0]["text"], text)

    def test_signature_and_date_values_use_declared_slots(self):
        template = (
            "法定代表人或授权委托人（签字或盖章）： "
            "日期： 年 月 日 后附：信用报告。"
        )
        result = compare_pattern(
            build_pattern(template),
            "法定代表人或授权委托人（签字或盖章）：___ "
            "日期：2026年9月21日 后附：信用报告。",
        )
        self.assertEqual(result["status"], "pass")
        self.assertEqual(result["captures"], ["___ ", "2026年9月21日"])

    def test_salutation_recipient_and_company_are_declared_slots(self):
        result = compare_pattern(
            build_pattern("致（采购人名称）： 我公司承诺严格履约。"),
            "致（采购人名称）：上海采购有限公司 我公司承诺严格履约。",
        )
        self.assertEqual(result["status"], "pass")
        self.assertEqual(result["captures"], ["（采购人名称）", "上海采购有限公司"])

    def test_industry_and_email_values_are_declared_slots(self):
        self.assert_status(
            "行业类型： 电子邮箱： 后附：相关材料。",
            "行业类型：软件业 电子邮箱：contact@example.com 后附：相关材料。",
            "pass",
        )
        self.assert_status("电子邮件： ；", "电子邮件：___；", "pass")

    def test_cjk_ascii_layout_gap_does_not_remove_ascii_word_space(self):
        self.assert_status("提供 AI PC 服务。", "提供AI PC服务。", "pass")
        self.assert_status("提供 AI PC 服务。", "提供AIPC服务。", "fail")

    def test_chinese_punctuation_layout_space_is_ignored(self):
        self.assert_status(
            "查询打印网址：https://www.creditchina.gov.cn/",
            "查询打印网址： https://www.creditchina.gov.cn/",
            "pass",
        )

    def test_reverse_check_ignores_table_rows_and_unrelated_evidence_pages(self):
        evaluated = [{
            "status": "pass",
            "source_spans": [{"source_id": "a"}, {"source_id": "c"}],
            "source_range": {"start_order": 0, "end_order": 3},
            "bid_locations": [{"page": 1}],
        }]
        candidates = [
            {
                "text": "必须填写的表格数据内容",
                "source_ids": ["b"],
                "source_spans": [{"source_id": "b"}],
                "source_range": {"start_order": 1, "end_order": 1, "block_count": 1},
                "locations": [{"page": 1, "type": "table_cell"}],
            },
            {
                "text": "供应商必须遵守证明文件中的说明",
                "source_ids": ["d"],
                "source_spans": [{"source_id": "d"}],
                "source_range": {"start_order": 2, "end_order": 2, "block_count": 1},
                "locations": [{"page": 2, "type": "text"}],
            },
        ]
        self.assertEqual(
            StructuredConsistencyEngine._reverse_fixed_insertions(candidates, evaluated),
            [],
        )

    def test_body_candidate_removes_repeated_attachment_heading(self):
        records = StructuredConsistencyEngine._candidate_records(
            {
                "title": "附件16 ★服务承诺函",
                "sections": [{
                    "text": "十三、★服务承诺函 致（招标人）：某公司 我司承诺持续服务。",
                    "page": 5,
                    "bbox": [1, 2, 3, 4],
                }],
            },
            source_identity="document-v1",
        )
        self.assertTrue(any(record["text"].startswith("致（招标人）：") for record in records))

    def test_keyword_rich_prose_is_not_mistaken_for_table_header(self):
        engine = StructuredConsistencyEngine.__new__(StructuredConsistencyEngine)
        self.assertEqual(
            engine._table_header_items(
                "参选总价包含所有服务费用，漏报项目数量由供应商承担。",
                "attachment-1",
                [{"page": 1, "type": "text"}],
            ),
            [],
        )

    def test_split_parts_have_distinct_stable_source_ids(self):
        records = StructuredConsistencyEngine._candidate_records(
            {
                "sections": [{
                    "id": "ocr-block-1",
                    "text": "单位名称：甲公司 地址：上海",
                    "page": 1,
                }],
            },
            source_identity="document-v1",
        )
        singleton_ids = {
            tuple(record["source_ids"])
            for record in records
            if (record.get("source_range") or {}).get("block_count") == 1
        }
        self.assertEqual(
            singleton_ids,
            {("ocr-block-1:0",), ("ocr-block-1:1",)},
        )


if __name__ == "__main__":
    unittest.main()
