import copy
import unittest
from contextlib import contextmanager
from unittest.mock import patch

from app.service.analysis.duplicate_merge.review_projection import project_duplicate_payload
from app.service.postgresql_service import PostgreSQLService
from app.service.review_index import build_review_index
from app.router.postgresql import _load_or_build_project_merged_results


CONTENT = "服务器内存容量不少于64GB，支持扩展至128GB"


def source(left, right):
    return {"left_file_name": left, "right_file_name": right,
            "left_document_identifier_id": left, "right_document_identifier_id": right}


def occurrence(left, right, *, text=CONTENT, pages=(1, 2), bbox=(10, 20, 200, 40),
               kind="block", source_id="pair"):
    return {
        "kind": kind, "family": "block", "mode": "exact", "source_item_id": source_id,
        "docs": {left: {"pages": [pages[0]], "preview": text},
                 right: {"pages": [pages[1]], "preview": text}},
        "evidence": {"left_text": text, "right_text": text,
                     "left_bbox": list(bbox) if bbox else None,
                     "right_bbox": list(bbox) if bbox else None},
    }


def issue(name, files, entries, risk="medium", score=70):
    return {"cluster_id": name, "risk_level": risk, "score_value": score,
            "files": list(files), "occurrences": entries, "source_issue_ids": ["pair"]}


class DuplicateReviewProjectionTests(unittest.TestCase):
    def test_same_pair_position_across_detector_types_is_one_evidence(self):
        block = occurrence("A.pdf", "B.pdf")
        table = occurrence("A.pdf", "B.pdf", kind="table", source_id="pair2")
        similar = occurrence("A.pdf", "B.pdf", kind="similar_block", source_id="pair2")
        similar["mode"] = "similar"
        payload = {"document_type": "technical_bid", "source_items": {
            "pair": source("A.pdf", "B.pdf"), "pair2": source("A.pdf", "B.pdf")},
            "issues": [issue("block", ("A.pdf", "B.pdf"), [block]),
                       issue("table", ("A.pdf", "B.pdf"), [table, similar], "high", 88)]}
        original = copy.deepcopy(payload)
        projected = project_duplicate_payload(payload)
        self.assertEqual(len(projected["issues"]), 1)
        card = projected["issues"][0]
        self.assertEqual((card["occurrence_count"], card["source_evidence_count"]), (1, 3))
        self.assertEqual(card["risk_level"], "high")
        self.assertEqual(card["score_value"], 88)
        self.assertEqual(set(card["occurrences"][0]["source_item_ids"]), {"pair", "pair2"})
        self.assertEqual(len(card["pair_scores"]), 2)
        self.assertEqual(payload, original)
        self.assertIs(project_duplicate_payload(projected), projected)

    def test_same_content_across_pairs_is_one_card_but_each_real_page_is_located(self):
        payload = {"document_type": "technical_bid", "source_items": {
            "ab": source("A.pdf", "B.pdf"), "ac": source("A.pdf", "C.pdf")},
            "issues": [issue("ab", ("A.pdf", "B.pdf"), [occurrence("A.pdf", "B.pdf", source_id="ab")]),
                       issue("ac", ("A.pdf", "C.pdf"), [occurrence("A.pdf", "C.pdf", pages=(3, 4), source_id="ac")])]}
        cards = project_duplicate_payload(payload)["issues"]
        self.assertEqual(len(cards), 1)
        self.assertEqual(cards[0]["occurrence_count"], 2)
        self.assertEqual(set(cards[0]["participants"]), {"A.pdf", "B.pdf", "C.pdf"})
        self.assertEqual({(loc["file_name"], loc["page"]) for loc in cards[0]["locations"]},
                         {("A.pdf", 1), ("B.pdf", 2), ("A.pdf", 3), ("C.pdf", 4)})

    def test_same_table_box_different_text_and_numeric_models_remain_separate(self):
        a = occurrence("A.pdf", "B.pdf", text="品牌甲服务器型号X100，内存64GB")
        b = occurrence("A.pdf", "B.pdf", text="品牌乙服务器型号X200，内存128GB")
        payload = {"document_type": "technical_bid", "source_items": {"pair": source("A.pdf", "B.pdf")},
                   "issues": [issue("one", ("A.pdf", "B.pdf"), [a, b])]}
        cards = project_duplicate_payload(payload)["issues"]
        self.assertEqual(len(cards), 1)
        self.assertEqual(cards[0]["occurrence_count"], 2)
        payload["issues"] = [issue("one", ("A.pdf", "B.pdf"), [a]),
                             issue("two", ("A.pdf", "B.pdf"), [b])]
        self.assertEqual(len(project_duplicate_payload(payload)["issues"]), 2)

    def test_missing_bbox_does_not_collapse_two_ambiguous_occurrences(self):
        a = occurrence("A.pdf", "B.pdf", bbox=None)
        b = occurrence("A.pdf", "B.pdf", bbox=None)
        payload = {"document_type": "technical_bid", "source_items": {"pair": source("A.pdf", "B.pdf")},
                   "issues": [issue("one", ("A.pdf", "B.pdf"), [a, b])]}
        cards = project_duplicate_payload(payload)["issues"]
        self.assertEqual(len(cards), 1)
        self.assertEqual(cards[0]["occurrence_count"], 2)

    def test_identical_filenames_still_locate_two_distinct_documents(self):
        pair = source("同名.pdf", "同名.pdf")
        pair["left_document_identifier_id"] = "bidder-A"
        pair["right_document_identifier_id"] = "bidder-B"
        entry = occurrence("同名.pdf", "同名.pdf")
        entry["evidence"]["left_pages"] = [1]
        entry["evidence"]["right_pages"] = [4]
        payload = {"document_type": "technical_bid", "source_items": {"pair": pair},
                   "issues": [issue("same-name", ("同名.pdf", "同名.pdf"), [entry])]}
        card = project_duplicate_payload(payload)["issues"][0]
        self.assertEqual(card["file_count"], 2)
        self.assertEqual(card["occurrences"][0]["left_document_identifier_id"], "bidder-A")
        self.assertEqual(card["occurrences"][0]["right_document_identifier_id"], "bidder-B")
        self.assertEqual({(loc["document_identifier_id"], loc["page"]) for loc in card["locations"]},
                         {("bidder-A", 1), ("bidder-B", 4)})

    def test_nested_section_rectangle_yields_to_specific_matching_span(self):
        wide = occurrence("A.pdf", "B.pdf", bbox=(10, 10, 200, 120), kind="section")
        narrow = occurrence("A.pdf", "B.pdf", bbox=(20, 20, 100, 40), kind="block")
        payload = {"document_type": "technical_bid", "source_items": {"pair": source("A.pdf", "B.pdf")},
                   "issues": [issue("same", ("A.pdf", "B.pdf"), [wide, narrow])]}
        card = project_duplicate_payload(payload)["issues"][0]
        self.assertEqual(card["occurrence_count"], 2)
        self.assertEqual(len(card["locations"]), 2)
        self.assertTrue(all(loc["bbox"] == [20.0, 20.0, 100.0, 40.0] for loc in card["locations"]))

    def test_new_review_index_uses_one_group_and_preserves_both_source_references(self):
        payload = {"document_type": "technical_bid", "source_items": {
            "pair": source("A.pdf", "B.pdf"), "pair2": source("A.pdf", "B.pdf")},
            "issues": [issue("block", ("A.pdf", "B.pdf"), [occurrence("A.pdf", "B.pdf")]),
                       issue("table", ("A.pdf", "B.pdf"), [occurrence("A.pdf", "B.pdf", kind="table", source_id="pair2")])]}
        saved = []

        def save(value, **kwargs):
            saved.append((kwargs["kind"], value))
            return f"{kwargs['kind']}/{kwargs['identity']}"

        with patch("app.service.review_index.document_blob_store.save_review_index_object", side_effect=save):
            summary, _, rows = build_review_index(
                {"technical_bid_duplicate_check": {**payload, "storage_schema_version": 2}},
                project_identifier_id="project", result_version="version-1234567890",
            )
        self.assertEqual(summary["issue_count"], 1)
        self.assertEqual(rows[0]["evidence_count"], 1)
        self.assertEqual(rows[0]["list_payload"]["source_evidence_count"], 2)
        evidence = next(value for kind, value in saved if kind == "evidence")
        self.assertEqual(set(evidence["source_item_object_keys"]), {"pair", "pair2"})

    def test_reading_an_outdated_merge_builds_a_view_without_overwriting_the_result(self):
        class ReadOnlyService:
            def upsert_project_result_item(self, **_kwargs):
                raise AssertionError("a result was overwritten on read")

        record = {"result": {"technical_bid_duplicate_check": {"groups": {}}}}
        projected_key = "technical_bid_duplicate_clusters"
        with patch("app.router.postgresql.build_duplicate_merge_results", return_value={
            projected_key: {"issues": [], "config": {"merge_strategy": "old"}},
        }):
            returned, merged = _load_or_build_project_merged_results(
                identifier_id="project", result_record=record, db_service=ReadOnlyService(),
                requested_keys=[projected_key],
            )
        self.assertIs(returned, record)
        self.assertEqual(merged[projected_key]["issues"], [])
        self.assertNotIn(projected_key, record["result"])

    def test_historical_summary_uses_grouped_card_counts(self):
        class Cursor:
            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def execute(self, *_args):
                pass

            def fetchall(self):
                return []

        class Connection:
            def cursor(self, **_kwargs):
                return Cursor()

        @contextmanager
        def connection():
            yield Connection()

        service = PostgreSQLService()
        service._get_connection = connection
        head = {"identifier_id": "summary-project", "project_name": "示例", "result_version": "v" * 20,
                "review_index_status": "ready", "input_revision": 1, "result_input_revision": 1,
                "result_object_key": "saved-result", "review_summary": {
                    "issue_count": 2, "risk_counts": {"high": 2}, "status_counts": {"fail": 2},
                    "categories": [{"result_key": "technical_bid_duplicate_check", "issue_count": 2,
                                    "risk_counts": {"high": 2}, "status_counts": {"fail": 2}}],
                }}
        with patch.object(service, "_get_project_review_head", return_value=head), \
             patch.object(service, "_projected_duplicate_review_rows", return_value=[
                 {"risk_level": "high", "status": "failed"},
             ]):
            summary = service.get_project_review_summary("summary-project")
        self.assertEqual(summary["issue_count"], 1)
        self.assertEqual(summary["risk_counts"]["high"], 1)
        self.assertEqual(summary["status_counts"]["fail"], 1)
        self.assertEqual(summary["categories"][0]["issue_count"], 1)

    def test_historical_index_list_detail_evidence_and_export_share_group_id_without_writes(self):
        rows = []
        blobs = {}
        for index, (left, right, source_id) in enumerate((("A.pdf", "B.pdf", "ab"),
                                                          ("A.pdf", "C.pdf", "ac"))):
            issue_id = f"old-{index}"
            rows.append({
                "issue_id": issue_id, "issue_order": index,
                "result_key": "technical_bid_duplicate_check", "risk_level": "high",
                "status": "failed", "title": "重复内容", "description": "证据",
                "file_names": [left, right], "list_payload": {"risk_level": "high"},
                "detail_object_key": f"detail-{index}", "evidence_object_key": f"evidence-{index}",
                "evidence_count": 1,
            })
            blobs[f"detail-{index}"] = issue(issue_id, (left, right), [], "high", 80)
            blobs[f"evidence-{index}"] = {
                "occurrences": [occurrence(left, right, pages=(index + 1, index + 2), source_id=source_id)],
                "source_item_object_keys": {source_id: f"source-{source_id}"},
            }
            blobs[f"source-{source_id}"] = source(left, right)

        class Cursor:
            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def execute(self, query, *_args):
                self.query = query
                self.params = _args[0] if _args else ()

            def fetchall(self):
                if len(self.params) >= 3 and self.params[2] in {
                    "business_bid_duplicate_check", "technical_bid_duplicate_check"
                }:
                    return [row for row in rows if row["result_key"] == self.params[2]]
                return rows

            def fetchone(self):
                if "COUNT(*)" in self.query:
                    return {"total": len(rows)}
                return None

        class Connection:
            def cursor(self, **_kwargs):
                return Cursor()

        @contextmanager
        def connection():
            yield Connection()

        service = PostgreSQLService()
        service._get_connection = connection
        project_id = "historical-projection-project"
        head = {"identifier_id": project_id, "result_version": "version-1234567890",
                "input_revision": 1, "result_input_revision": 1}
        with patch.object(service, "_get_project_review_head", return_value=head), \
             patch.object(service, "_assert_review_version"), \
             patch.object(service, "upsert_project_result_item", side_effect=AssertionError("read path wrote a result")), \
             patch("app.service.postgresql_service.document_blob_store.read_blob", side_effect=lambda key: blobs[key]):
            listing = service.list_project_review_issues(
                project_id, result_version=head["result_version"], result_key="technical_bid_duplicate_check",
                limit=1, offset=0,
            )
            self.assertEqual(listing["total"], 1)
            group_id = listing["items"][0]["issue_id"]
            self.assertTrue(group_id.startswith("dupgroup-"))
            identifiers = service.list_project_review_issues(
                project_id, result_version=head["result_version"], result_key="technical_bid_duplicate_check",
                ids_only=True,
            )
            self.assertEqual([row["issue_id"] for row in identifiers["items"]], [group_id])
            filtered = service.list_project_review_issues(
                project_id, result_version=head["result_version"],
                result_key="technical_bid_duplicate_check", file_name="C.pdf", status="fail",
            )
            self.assertEqual(filtered["total"], 1)
            self.assertEqual(service.list_project_review_issues(
                project_id, result_version=head["result_version"],
                result_key="technical_bid_duplicate_check", file_name="D.pdf",
            )["total"], 0)
            global_listing = service.list_project_review_issues(
                project_id, result_version=head["result_version"], limit=1, offset=0,
            )
            self.assertEqual(global_listing["total"], 1)
            self.assertEqual(global_listing["items"][0]["issue_id"], group_id)
            detail = service.get_project_review_issue(project_id, group_id, result_version=head["result_version"])
            self.assertEqual(set(detail["data"]["participants"]), {"A.pdf", "B.pdf", "C.pdf"})
            evidence = service.get_project_review_issue(
                project_id, group_id, result_version=head["result_version"], evidence=True,
            )
            self.assertEqual(evidence["data"]["total"], 2)
            exported = service.get_project_review_export_payload(
                project_id, result_version=head["result_version"],
                review_statuses={group_id: {"status": "flagged"}},
            )
            self.assertEqual(len(exported["result"]), 1)
            self.assertEqual(exported["result"][0]["issue_id"], group_id)
            self.assertEqual(exported["result"][0]["frontend_review_status"], "flagged")


if __name__ == "__main__":
    unittest.main()
