import unittest
from unittest.mock import patch

from app.config.settings import settings
from app.core.document_types import DOCUMENT_TYPE_BUSINESS_BID, DOCUMENT_TYPE_TECHNICAL_BID
from app.service.analysis.duplicate_check.service import DuplicateCheckService
from app.service.analysis.duplicate_check.tender_content_excluder import (
    TenderContentIndex,
    tender_text_units,
)


class TenderContentIndexTests(unittest.TestCase):
    def test_matches_any_tender_page_and_preserves_added_response(self):
        payload = {
            "layout_sections": [
                {"page": 1, "text": "项目说明"},
                {"page": 9, "text": "设备应连续运行不少于二十四小时"},
            ]
        }
        index = TenderContentIndex(tender_text_units(payload))
        self.assertEqual(
            index.strip_text("我方承诺设备应连续运行不少于二十四小时，并免费上门维修"),
            "我方承诺\n并免费上门维修",
        )

    def test_ten_character_boundary_and_whitespace(self):
        index = TenderContentIndex(["一二三四五六七八九十"])
        self.assertEqual(index.strip_text("一二三四五六七八九"), "一二三四五六七八九")
        self.assertEqual(index.strip_text("一二三四五\n六七八九十"), "")

    def test_structured_cells_keep_bid_specific_short_values(self):
        index = TenderContentIndex(["序号", "品牌", "华为", "3000", "ABC-100"])
        self.assertEqual(
            index.strip_text("序号 | 品牌 | 华为 | 3000 | ABC-100", structured=True),
            "华为 | 3000 | ABC-100",
        )

    def test_table_rows_and_hashes_are_rebuilt_after_cleaning(self):
        index = TenderContentIndex(["设备应连续运行不少于二十四小时"])
        tables = [{"rows": ["设备应连续运行不少于二十四小时 | 我方提供上门维修服务"], "text": "old", "exact_hash": "old"}]
        blocks, cleaned = index.strip_blocks_and_tables([], tables)
        self.assertEqual(blocks, [])
        self.assertEqual(cleaned[0]["rows"], ["我方提供上门维修服务"])
        self.assertNotEqual(cleaned[0]["exact_hash"], "old")

    def test_html_table_cells_are_part_of_tender_corpus(self):
        payload = {"table_sections": [{"html": "<table><tr><td>设备连续运行不少于二十四小时</td></tr></table>"}]}
        index = TenderContentIndex(tender_text_units(payload))
        self.assertEqual(index.strip_text("设备连续运行不少于二十四小时"), "")


class TenderContentServiceTests(unittest.TestCase):
    def test_source_aware_star_row_retains_bidder_response(self):
        service = DuplicateCheckService()
        requirement = "★设备应连续运行不少于二十四小时"
        rows = [{
            "page": 1,
            "title": "技术偏离表",
            "requirement_text": requirement,
            "response_text": "我方提供全年免费上门维修服务",
            "deviation_text": "无偏离",
        }]
        sections = {"business": [{"page": 1, "title": "技术偏离表"}], "technical": [], "rows": rows}
        record = {"identifier_id": "bid-1", "content": {"layout_sections": []}}
        context = {"content_index": TenderContentIndex([requirement])}
        with patch.object(service._itemized_checker, "_prepare_document", return_value={"item_sections": []}), patch.object(
            service._deviation_checker, "_extract_bid_deviation_sections", return_value=sections
        ):
            prepared, reason = service._prepare_document(
                record, role=DOCUMENT_TYPE_BUSINESS_BID, template_context=context,
            )
        self.assertIsNone(reason)
        self.assertIsNotNone(prepared)
        self.assertIn("全年免费上门维修服务", prepared["full_text"])
        self.assertNotIn("连续运行不少于二十四小时", prepared["full_text"])

    def test_technical_bid_excludes_tender_image_before_scoring(self):
        service = DuplicateCheckService()
        record = {"identifier_id": "bid-1", "file_url": "bid-url"}
        images = [
            {"exact_hash": "tender-image", "pages": [1]},
            {"exact_hash": "bid-image", "pages": [2]},
        ]
        with patch.object(service, "_get_document_images", return_value=images):
            prepared = service._build_prepared_document(
                record, [], [], role=DOCUMENT_TYPE_TECHNICAL_BID,
                tender_image_hashes={"tender-image"},
            )
        self.assertEqual(prepared["exact_image_hashes"], {"bid-image"})

    def test_tender_image_hash_comes_from_linked_tender_file(self):
        service = DuplicateCheckService()
        record = {
            "tender_identifier_id": "tender-1",
            "tender_file_url": "tender-url",
            "tender_file_name": "tender.pdf",
            "tender_content": {"layout_sections": [{"page": 1, "text": "技术要求"}]},
        }
        with patch.object(service, "_get_document_images", return_value=[{"exact_hash": "shared", "pages": [1]}]) as images:
            context = service._get_tender_template_context(
                record, role=DOCUMENT_TYPE_TECHNICAL_BID, cache={},
            )
        self.assertEqual(context["image_hashes"], {"shared"})
        self.assertEqual(images.call_args.args[0]["file_url"], "tender-url")

    def test_missing_tender_keeps_existing_comparison(self):
        service = DuplicateCheckService()
        text = "两个投标人都提交了这段超过三十个字的完全相同内容，用于验证缺少招标文件时查重仍然执行。"
        records = [
            {"identifier_id": identifier, "relation_role": DOCUMENT_TYPE_TECHNICAL_BID,
             "content": {"layout_sections": [{"page": 1, "type": "text", "text": text}]}}
            for identifier in ("bid-1", "bid-2")
        ]
        with patch.object(settings, "TYPO_CHECK_ENABLED", False), patch.object(service, "_get_document_images", return_value=[]), patch.object(
            service, "_exclude_deviation_regions", side_effect=lambda payload, blocks, tables: (blocks, tables)
        ):
            result = service.check_project_documents(
                project_identifier="project-1", project=None, document_records=records,
                document_types=[DOCUMENT_TYPE_TECHNICAL_BID],
            )
        self.assertEqual(result["groups"][DOCUMENT_TYPE_TECHNICAL_BID]["pair_count"], 1)
        self.assertEqual(result["summary"]["suspicious_pair_count"], 1)


if __name__ == "__main__":
    unittest.main()
