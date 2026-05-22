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


if __name__ == "__main__":
    unittest.main()
