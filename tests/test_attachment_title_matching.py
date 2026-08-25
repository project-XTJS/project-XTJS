from __future__ import annotations

import unittest

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

    def test_nested_parenthetical_content_is_stripped(self) -> None:
        from app.service.analysis.attachment_synonyms import (
            strip_attachment_title_parenthetical_noise,
        )

        self.assertEqual(
            strip_attachment_title_parenthetical_noise("中小企业声明函（工程（一期））"),
            "中小企业声明函",
        )


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


if __name__ == "__main__":
    unittest.main()
