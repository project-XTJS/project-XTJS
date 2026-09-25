from __future__ import annotations

import unittest
from datetime import date
from unittest.mock import patch

from app.service.analysis.compliance.consistency import ConsistencyChecker
from app.service.analysis.compliance.structured_consistency import (
    StructuredConsistencyEngine,
)
from app.service.analysis.compliance.template_extractor import TemplateExtractor
from app.service.analysis.verification import VerificationChecker


class AttachmentTitleMatchingTests(unittest.TestCase):
    """附件标题匹配：大标题后括号内容不参与判定、引导动词剥离、合并标题按子标题匹配。"""

    def setUp(self) -> None:
        self.verifier = VerificationChecker(None)

    def test_parenthetical_variant_titles_are_compatible(self) -> None:
        self.assertTrue(
            self.verifier._attachment_titles_compatible(
                "中小企业声明函（工程）",
                "中小企业声明函（格式）",
            )
        )

    def test_leading_verb_prefix_is_ignored(self) -> None:
        self.assertTrue(
            self.verifier._attachment_titles_compatible(
                "提供强制采购节能产品承诺书（格式）",
                "强制采购节能产品承诺书",
            )
        )

    def test_merged_variant_item_matches_any_subtitle(self) -> None:
        self.assertTrue(
            self.verifier._attachment_titles_compatible(
                "中小企业声明函（格式） 中小企业声明函（工程）",
                "9. 中小企业声明函（工程）",
            )
        )

    def test_first_quote_and_opening_quote_are_complete_aliases(self) -> None:
        self.assertTrue(
            self.verifier._attachment_titles_compatible(
                "附件7 首次报价一览表（格式）",
                "三、开标一览表",
            )
        )

    def test_short_business_subheading_is_not_an_attachment_alias(self) -> None:
        self.assertFalse(
            self.verifier._attachment_titles_compatible(
                "A、商务",
                "商务条款偏离表",
            )
        )

    def test_nested_parenthetical_content_is_stripped(self) -> None:
        from app.service.analysis.attachment_synonyms import (
            strip_attachment_title_parenthetical_noise,
        )

        self.assertEqual(
            strip_attachment_title_parenthetical_noise("中小企业声明函（工程（一期））"),
            "中小企业声明函",
        )


class AttachmentDateCheckTests(unittest.TestCase):
    def setUp(self) -> None:
        self.verifier = VerificationChecker(None)
        self.attachment = {"requirements": {"requires_date": True}}
        self.deadline = {
            "date": date(2026, 9, 9),
            "text": "投标截止时间：2026年09月09日",
            "page": 1,
            "locations": [],
        }

    @staticmethod
    def _bid_section(*section_texts: str) -> dict:
        sections = [
            {"type": "text", "text": value, "page": 3, "bbox": [10, 20 + index * 30, 500, 40 + index * 30]}
            for index, value in enumerate(section_texts)
        ]
        return {
            "text": "\n".join(section_texts),
            "sections": sections,
            "pages": [3],
        }

    def test_damaged_date_label_stays_pending(self) -> None:
        result = self.verifier._date_check(
            self.attachment,
            self._bid_section("期：2026年09月08日"),
            self.deadline,
        )

        self.assertEqual(result["status"], "pending")
        self.assertIsNone(result["sign_date"])
        self.assertEqual(result["reason_code"], "sign_date_not_reliably_located")

    def test_single_date_after_deadline_is_late(self) -> None:
        result = self.verifier._date_check(
            self.attachment,
            self._bid_section("日期：2026年09月10日"),
            self.deadline,
        )

        self.assertEqual(result["status"], "late")
        self.assertEqual(result["sign_date"], "2026-09-10")

    def test_multiple_dates_still_use_contextual_date_rule(self) -> None:
        result = self.verifier._date_check(
            self.attachment,
            self._bid_section(
                "成立日期：2020年01月01日",
                "日期：2026年09月08日",
            ),
            self.deadline,
        )

        self.assertEqual(result["status"], "pass")
        self.assertEqual(result["sign_date"], "2026-09-08")
        self.assertEqual(result["match_method"], "contextual_date")

    def test_multiple_dates_reject_ocr_damaged_date_field_context(self) -> None:
        result = self.verifier._date_check(
            self.attachment,
            self._bid_section(
                "最终交付时间为2026年09月30日",
                "期：2026年09月08日",
            ),
            self.deadline,
        )

        self.assertEqual(result["status"], "pending")
        self.assertIsNone(result["sign_date"])

    def test_quality_period_is_not_treated_as_damaged_date_field(self) -> None:
        self.assertFalse(
            self.verifier._is_ocr_damaged_date_field_line(
                "质保期：2026年09月08日",
            )
        )

    def test_multiple_dates_without_context_are_not_forced_to_match(self) -> None:
        result = self.verifier._date_check(
            self.attachment,
            self._bid_section("2026年09月07日", "2026年09月08日"),
            self.deadline,
        )

        self.assertEqual(result["status"], "pending")


class ConsistencyOptionalAndReferenceTests(unittest.TestCase):
    """一致性审查：其他材料不认定为必须材料、划型标准等参考段不要求投标复述。"""

    def test_other_materials_template_is_optional(self) -> None:
        from app.service.analysis.compliance.template_extractor import (
            is_consistency_template_optional,
        )

        for title in ("（二）其他材料", "其他材料（格式）", "其他内容", "其它材料"):
            self.assertTrue(is_consistency_template_optional(title), title)
        self.assertFalse(
            is_consistency_template_optional("中小企业声明函（工程）"),
        )

    def test_reference_note_paragraph_is_not_required(self) -> None:
        from app.service.analysis.compliance.structured_consistency import (
            REFERENCE_NOTE_MARKERS,
        )

        paragraph = "2.本声明函适用于所有在中国境内依法设立的各类所有制企业"
        compact = "".join(paragraph.split())
        self.assertTrue(any(marker in compact for marker in REFERENCE_NOTE_MARKERS))

    def test_truncate_at_next_attachment_heading(self) -> None:
        lines = [
            "7.中小企业声明函（格式）",
            "本公司（联合体）郑重声明",
            "8. 《投标项目负责人基本情况表》",
            "9. 供应商书面声明（格式）",
        ]
        truncated = StructuredConsistencyEngine._truncate_at_next_attachment_heading(
            lines,
            title="中小企业声明函（格式） 中小企业声明函（工程）",
        )
        self.assertEqual(truncated, ["本公司（联合体）郑重声明"])

    def test_tender_location_index_uses_response_format_attachments(self) -> None:
        engine = ConsistencyChecker()._structured_engine
        response_attachment = {
            "attachment_number": "5",
            "title": "附件5 参选人基本情况表（格式）",
            "locations": [
                {"page": 61, "bbox": [10, 20, 30, 40], "text": "附件5"},
            ],
        }
        with patch.object(
            TemplateExtractor,
            "extract_response_format_attachments",
            return_value=[response_attachment],
        ):
            index = engine._index_model_attachments({})

        self.assertEqual(index["num:5"]["pages"], [61])
        self.assertEqual(
            index["title:参选人基本情况表"]["title"],
            "附件5 参选人基本情况表（格式）",
        )

    def test_exact_title_location_wins_over_same_number_location(self) -> None:
        engine = ConsistencyChecker()._structured_engine
        correct = {"title": "附件5 参选人基本情况表（格式）", "check_pages": [61]}
        wrong = {"title": "附件5 原子技能清单", "check_pages": [33]}
        section, pages = engine._accurate_attachment_pages(
            {
                "num:5": wrong,
                "title:参选人基本情况表": correct,
            },
            "5",
            "附件5 参选人基本情况表（格式）",
        )

        self.assertIs(section, correct)
        self.assertEqual(pages, [61])

    def test_consistency_title_score_checks_leading_form_title(self) -> None:
        score = ConsistencyChecker()._structured_engine._section_title_score(
            "附件6 近三年完成的类似项目业绩清单（格式）",
            {
                "title": "近三年以来类似项目业绩清单及证明材料",
                "sections": [
                    {
                        "type": "heading",
                        "text": "第六章 近三年以来类似项目业绩清单及证明材料",
                    },
                    {
                        "type": "heading",
                        "text": "附件6 近三年完成的类似项目业绩清单",
                    },
                    {"type": "text", "text": "项目名称：某项目"},
                    {"type": "heading", "text": "合同中的无关标题"},
                ],
            },
        )

        self.assertGreaterEqual(score, 0.6)

    def test_title_score_ignores_attachment_and_chapter_prefixes(self) -> None:
        score = ConsistencyChecker()._structured_engine._section_title_score(
            "附件10 项目人员配置表（格式）",
            {
                "title": "项目人员配置表",
                "sections": [
                    {"type": "heading", "text": "第十二章 项目人员配置表"},
                    {"type": "text", "text": "项目名称：某项目"},
                ],
            },
        )

        self.assertEqual(score, 1.0)

class VerificationAttachmentScopeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.verifier = VerificationChecker(None)

    def test_same_expected_attachment_form_heading_does_not_end_scope(self) -> None:
        title = "附件11 财务状况及税收、社会保障资金缴纳情况声明函"
        expected = self.verifier._attachment_title_hints(
            [{"attachment_number": "11", "title": title}]
        )
        chunk = [
            {
                "type": "heading",
                "text": "7. 财务状况，依法缴纳税收和社会保障资金的声明函。",
                "page": 96,
            },
            {
                "type": "heading",
                "text": "财务状况及税收、社会保障资金缴纳情况声明函",
                "page": 96,
            },
            {"type": "text", "text": "日期：2026年09月07日", "page": 96},
        ]

        effective = self.verifier._effective_attachment_check_chunk(chunk, expected)

        self.assertEqual(effective, chunk)

    def test_missing_optional_attachment_is_not_reported_as_missing(self) -> None:
        optional = {
            "attachment_number": "12",
            "title": "附件12 保证金缴纳凭证（格式）（如有）",
            "requirements": {
                "is_optional": True,
                "requires_signature": False,
                "requires_seal": False,
                "requires_date": False,
            },
        }
        deadline = {
            "date": date(2026, 9, 9),
            "text": "响应截止时间：2026年09月09日",
            "page": 1,
            "locations": [],
        }
        with (
            patch.object(self.verifier, "_seal_bundle", return_value={"detected": False, "count": 0, "texts": [], "locations": []}),
            patch.object(self.verifier, "_signature_bundle", return_value={"detected": False, "count": 0, "texts": [], "locations": []}),
            patch.object(self.verifier, "_bidder_name", return_value="测试公司"),
            patch.object(self.verifier, "_deadline_from_doc", return_value=deadline),
            patch.object(self.verifier, "_required_attachments", return_value=[optional]),
            patch.object(self.verifier, "_attachment_sections", return_value=[]),
            patch.object(self.verifier, "_match_attachment", return_value=None),
        ):
            result = self.verifier._check_pair({}, {})

        self.assertEqual(result["skipped_missing_attachments"], [])
        self.assertEqual(result["missing_attachment_results"], [])
        self.assertEqual(result["skipped_optional_attachments"], [optional["title"]])


if __name__ == "__main__":
    unittest.main()
