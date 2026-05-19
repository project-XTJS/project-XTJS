from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from tools.project_folder_check_cli import (
    _build_comparison,
    _build_extra_comparison,
    _extract_business_review,
    _find_original_result_json,
    _normalize_selected_checks,
    _render_original_issues,
)


def _make_check(status: str, failed_titles: list[str] | None = None, unclear_titles: list[str] | None = None) -> dict:
    return {
        "review": {
            "status": status,
            "summary": status,
        },
        "issues": {
            "passed": [],
            "failed": [
                {"title": title, "status": "fail", "message": title}
                for title in (failed_titles or [])
            ],
            "unclear": [
                {"title": title, "status": "unclear", "message": title}
                for title in (unclear_titles or [])
            ],
        },
    }


def _make_bidder(bidder_key: str, bidder_name: str, checks: dict[str, dict]) -> dict:
    return {
        "bidder_key": bidder_key,
        "bidder_name": bidder_name,
        "documents": {
            "business": {"file_name": f"{bidder_key}商务标.json"},
            "technical": {"file_name": f"{bidder_key}技术标.json"},
        },
        "checks": checks,
    }


class ProjectFolderCheckCliTests(unittest.TestCase):
    def test_normalize_selected_checks_supports_numbers_and_aliases(self) -> None:
        checks = _normalize_selected_checks(["1,开标一览表检查", "verification"])
        self.assertEqual(
            checks,
            ["integrity_check", "pricing_check", "verification_check"],
        )

    def test_normalize_selected_checks_supports_new_numeric_options(self) -> None:
        checks = _normalize_selected_checks(["7,8,9,10"])
        self.assertEqual(
            checks,
            [
                "business_bid_duplicate_check",
                "technical_bid_duplicate_check",
                "personnel_reuse_check",
                "typo_check",
            ],
        )

    def test_extract_business_review_supports_wrapped_payload(self) -> None:
        payload = {
            "data": {
                "results": {
                    "business_bid_format_review": {
                        "review_type": "business_bid_format_review",
                        "bidders": [],
                    }
                }
            }
        }
        review = _extract_business_review(payload)
        self.assertIsNotNone(review)
        self.assertEqual(review["review_type"], "business_bid_format_review")

    def test_find_original_result_json_prefers_result_like_name(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            (base / "无关.json").write_text(json.dumps({"foo": "bar"}, ensure_ascii=False), encoding="utf-8")
            (base / "审查结果.json").write_text(
                json.dumps({"business_bid_format_review": {"bidders": []}}, ensure_ascii=False),
                encoding="utf-8",
            )
            (base / "other_review.json").write_text(
                json.dumps({"business_bid_format_review": {"bidders": []}}, ensure_ascii=False),
                encoding="utf-8",
            )

            path, _, review = _find_original_result_json(base, None)

            self.assertEqual(path.name, "审查结果.json")
            self.assertIn("bidders", review)

    def test_build_comparison_marks_improvement(self) -> None:
        old_review = {
            "bidders": [
                _make_bidder(
                    "甲公司",
                    "甲公司",
                    {
                        "integrity_check": _make_check("fail", failed_titles=["附件1"]),
                    },
                )
            ]
        }
        new_review = {
            "bidders": [
                _make_bidder(
                    "甲公司",
                    "甲公司",
                    {
                        "integrity_check": _make_check("pass"),
                    },
                )
            ]
        }

        comparison = _build_comparison(
            original_review=old_review,
            new_review=new_review,
            selected_checks=["integrity_check"],
        )

        self.assertEqual(comparison["change_summary"]["improved"], 1)
        bidder = comparison["bidders"][0]
        check = bidder["checks"]["integrity_check"]
        self.assertEqual(check["change"], "improved")
        self.assertEqual(check["resolved_failed_titles"], ["附件1"])

    def test_build_extra_comparison_marks_improvement(self) -> None:
        original_payload = {
            "personnel_reuse_check": {
                "summary": {
                    "document_count": 3,
                    "personnel_count": 7,
                    "reused_name_count": 2,
                    "suspicious": True,
                }
            }
        }
        new_extra_results = {
            "personnel_reuse_check": {
                "summary": {
                    "document_count": 3,
                    "personnel_count": 7,
                    "reused_name_count": 0,
                    "suspicious": False,
                }
            }
        }

        comparison = _build_extra_comparison(
            original_payload=original_payload,
            new_extra_results=new_extra_results,
            selected_checks=["personnel_reuse_check"],
        )

        self.assertEqual(comparison["change_summary"]["improved"], 1)
        self.assertEqual(
            comparison["items"]["personnel_reuse_check"]["change"],
            "improved",
        )

    def test_render_original_issues_only_shows_selected_check_problems(self) -> None:
        original_review = {
            "bidders": [
                _make_bidder(
                    "甲公司",
                    "甲公司",
                    {
                        "integrity_check": _make_check(
                            "fail",
                            failed_titles=["附件1", "附件2", "附件3", "附件4", "附件5"],
                        ),
                        "pricing_check": _make_check("pass"),
                    },
                )
            ]
        }

        lines = _render_original_issues(
            original_review=original_review,
            original_payload={},
            selected_checks=["integrity_check"],
        )
        rendered = "\n".join(lines)

        self.assertIn("原始结果问题概览:", rendered)
        self.assertIn("完整性审查", rendered)
        self.assertIn("附件1", rendered)
        self.assertIn("附件5", rendered)
        self.assertNotIn("开标一览表审查", rendered)


if __name__ == "__main__":
    unittest.main()
