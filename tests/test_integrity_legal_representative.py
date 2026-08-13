from __future__ import annotations

import unittest

from app.service.analysis.compliance.integrity import IntegrityChecker


class IntegrityLegalRepresentativeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.checker = IntegrityChecker()

    def test_qualification_proof_is_not_satisfied_by_authorization_title(self) -> None:
        score, _, _ = self.checker._content_match_score(
            "9、法定代表人授权委托书",
            "法定代表人资格证明书",
        )

        self.assertEqual(score, 0)

    def test_qualification_proof_is_not_satisfied_by_authorized_person_social_security(self) -> None:
        score, _, _ = self.checker._content_match_score(
            "被授权人社保缴纳证明 兹证明 中电科思仪科技股份有限公司职工丰振华同志正常缴纳社保",
            "法定代表人资格证明书",
        )

        self.assertEqual(score, 0)

    def test_qualification_proof_matches_combined_proof_and_authorization_title(self) -> None:
        score, _, hits = self.checker._content_match_score(
            "二、法定代表人证明及法人授权委托书",
            "法定代表人资格证明书",
        )

        self.assertGreater(score, 0)
        self.assertIn("法定代表人证明", hits)

    def test_qualification_proof_matches_unit_principal_proof_title(self) -> None:
        score, _, hits = self.checker._content_match_score(
            "法定代表人/单位负责人直接投标的应提供法定代表人/单位负责人证明书及身份证",
            "法定代表人资格证明书",
        )

        self.assertGreater(score, 0)
        self.assertTrue(any("单位负责人证明书" in hit for hit in hits))

    def test_qualification_proof_matches_body_statement(self) -> None:
        score, _, hits = self.checker._content_match_score(
            "兹证明 周婉，现任我单位执行董事，系本公司法定代表人。",
            "法定代表人资格证明书",
        )

        self.assertGreater(score, 0)
        self.assertIn("兹证明", hits)


class IntegrityParentheticalTitleTests(unittest.TestCase):
    """大标题后括号内容不参与标题存在性判断（中小企业声明函（工程）≈（格式））。"""

    def setUp(self) -> None:
        self.checker = IntegrityChecker()

    def test_fullwidth_parenthetical_suffix_is_ignored(self) -> None:
        self.assertTrue(
            self.checker._smart_match(
                "中小企业声明函（工程）",
                "中小企业声明函（格式）",
            )
        )

    def test_halfwidth_parenthetical_suffix_is_ignored(self) -> None:
        self.assertTrue(
            self.checker._smart_match(
                "中小企业声明函(工程)",
                "中小企业声明函（格式）",
            )
        )

    def test_nested_parenthetical_content_is_fully_ignored(self) -> None:
        self.assertTrue(
            self.checker._smart_match(
                "中小企业声明函（工程（一期））",
                "中小企业声明函（格式）",
            )
        )
        self.assertEqual(
            self.checker._normalize_title_text("声明函（工程（一））"),
            "声明函",
        )

    def test_plain_title_matches_parenthetical_variant(self) -> None:
        self.assertTrue(
            self.checker._smart_match(
                "中小企业声明函",
                "中小企业声明函（工程）",
            )
        )

    def test_merged_variant_item_matches_any_subtitle(self) -> None:
        # 招标要求被合并为“中小企业声明函（格式） 中小企业声明函（工程）”时，
        # 投标文件里出现任一括号变体（或纯大标题）即视为满足。
        merged = "中小企业声明函（格式） 中小企业声明函（工程）"
        self.assertTrue(
            self.checker._smart_match("9. 中小企业声明函（工程）", merged)
        )
        self.assertTrue(
            self.checker._smart_match("7. 中小企业声明函", merged)
        )
        self.assertIn("中小企业声明函（工程）", self.checker._candidate_titles(merged))

    def test_leading_verb_prefix_is_ignored(self) -> None:
        # “提供/提交/需提供”等引导动词不参与标题判定：
        # 招标“提供强制采购节能产品承诺书（格式）”与投标标题“强制采购节能产品承诺书”视为同一标题。
        item = "提供强制采购节能产品承诺书（格式）"
        self.assertEqual(
            self.checker._normalize_target(item),
            "强制采购节能产品承诺书",
        )
        self.assertTrue(
            self.checker._smart_match("12. 强制采购节能产品承诺书", item)
        )
        self.assertTrue(
            self.checker._smart_match("10. 提供强制采购节能产品承诺书", item)
        )


if __name__ == "__main__":
    unittest.main()
