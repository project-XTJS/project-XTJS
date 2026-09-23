import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from app.config.settings import settings
from app.service.analysis.compliance.consistency import ConsistencyChecker
from app.service.analysis.compliance.structured_consistency import StructuredConsistencyEngine
from app.service.analysis.compliance import template_pdf_evidence


def section(title, pages, check_pages, *, suffix=""):
    nodes = [
        {"type": "heading", "text": title, "page": check_pages[0]},
        *[
            {"type": "text", "text": f"正文{suffix}{page}", "page": page}
            for page in check_pages
        ],
    ]
    return {
        "title": title,
        "attachment_number": "1",
        "pages": pages,
        "check_pages": check_pages,
        "sections": nodes + [{"type": "text", "text": "证明材料", "page": pages[-1]}],
        "check_sections": nodes,
        "text": "\n".join(item["text"] for item in nodes) + "\n证明材料",
        "check_text": "\n".join(item["text"] for item in nodes),
    }


class AttachmentScopeTests(unittest.TestCase):
    def setUp(self):
        self.engine = StructuredConsistencyEngine(ConsistencyChecker())
        self.skeleton = {"title": "附件1 授权委托书", "attachment_number": "1"}

    def test_scope_uses_cross_page_check_body_and_excludes_later_proof(self):
        candidate = section("附件1 授权委托书", [10, 11, 12, 300], [10, 11, 12])
        resolved = self.engine.resolve_attachment_scope(self.skeleton, [candidate])
        self.assertEqual(resolved["location_status"], "matched")
        self.assertEqual(resolved["evidence_pages"], [10, 11, 12])
        self.assertEqual(resolved["section"]["pages"], [10, 11, 12])
        self.assertNotIn("证明材料", resolved["section"]["text"])

    def test_ambiguous_same_title_has_no_fallback_to_document_end(self):
        first = section("附件1 授权委托书", [10, 11, 300], [10, 11], suffix="甲")
        second = section("附件1 授权委托书", [30, 31, 300], [30, 31], suffix="乙")
        resolved = self.engine.resolve_attachment_scope(self.skeleton, [first, second])
        self.assertEqual(resolved["location_status"], "ambiguous")
        self.assertIsNone(resolved["section"])
        self.assertEqual(resolved["evidence_pages"], [])

    def test_duplicate_tender_attachment_index_is_marked_ambiguous(self):
        index = {}
        first = section("附件1 授权委托书", [10], [10])
        second = section("附件1 授权委托书", [20], [20])
        self.engine._add_attachment_index_entry(index, "title:授权委托书", first)
        self.engine._add_attachment_index_entry(index, "title:授权委托书", second)
        self.assertTrue(index["title:授权委托书"]["_ambiguous"])


class EvidenceCacheTests(unittest.TestCase):
    def test_project_evidence_builder_never_supplies_local_vlm_ocr(self):
        payload = {
            "_template_source": {
                "file_url": "minio://bucket/form.pdf",
                "content_checksum": "fixture-checksum",
            },
            "layout_sections": [],
        }
        built = {"version": "fixture", "pages": {"1": {"status": "unclear", "spans": [], "issues": ["证据不足"]}}}
        with tempfile.TemporaryDirectory() as temp_dir, \
                patch.object(settings, "BUSINESS_REVIEW_EVIDENCE_CACHE_ROOT", Path(temp_dir)), \
                patch("app.service.minio_service.MinioService.bucket_and_object_from_file_url", return_value=("bucket", "form.pdf")), \
                patch("app.service.minio_service.MinioService.get_object_bytes", return_value=(b"pdf-content", {})), \
                patch.object(template_pdf_evidence, "build_pdf_underline_evidence", return_value=built) as builder:
            template_pdf_evidence._source_bytes.clear()
            result = template_pdf_evidence.evidence_for(payload, [1])
        self.assertEqual(result["pages"]["1"]["status"], "unclear")
        self.assertNotIn("local_ocr", builder.call_args.kwargs)

    def test_cache_key_changes_with_document_content(self):
        payload = {"_template_source": {"file_url": "minio://bucket/form.pdf"}, "layout_sections": []}
        entries = [b"first-pdf", b"second-pdf"]
        result = {"version": "fixture", "pages": {"1": {"status": "ready", "spans": [], "issues": []}}}
        with tempfile.TemporaryDirectory() as temp_dir, \
                patch.object(settings, "BUSINESS_REVIEW_EVIDENCE_CACHE_ROOT", Path(temp_dir)), \
                patch("app.service.minio_service.MinioService.bucket_and_object_from_file_url", return_value=("bucket", "form.pdf")), \
                patch("app.service.minio_service.MinioService.get_object_bytes", side_effect=[(entries[0], {}), (entries[1], {})]), \
                patch.object(template_pdf_evidence, "build_pdf_underline_evidence", return_value=result) as builder:
            template_pdf_evidence.evidence_for(payload, [1])
            template_pdf_evidence.evidence_for(payload, [1])
            cache_files = list(Path(temp_dir).glob("*.json"))
        self.assertEqual(builder.call_count, 2)
        self.assertEqual(len(cache_files), 2)


if __name__ == "__main__":
    unittest.main()
