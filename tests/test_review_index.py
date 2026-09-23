import unittest
from contextlib import contextmanager
from unittest.mock import Mock, patch

from app.service.analysis.duplicate_merge.storage import (
    DuplicateSourceReferenceError,
    compact_duplicate_payload,
    compact_project_duplicate_results,
    hydrate_duplicate_issue,
    validate_compact_duplicate_payload,
)
from app.service.cache_service import CacheResult, CacheUnavailableError, RedisCacheService
from app.service.postgresql_service import PostgreSQLService, _subtract_removed_review_issue_counts
from app.service.review_index import build_result_version, build_review_index


def legacy_duplicate_payload():
    source = {
        "left_file_name": "A.pdf",
        "right_file_name": "B.pdf",
        "duplicate_blocks": [{"left_text": "相同内容", "right_text": "相同内容"}],
    }
    return {
        "summary": {"cluster_count": 1},
        "document_type": "technical_bid",
        "issues": [{
            "cluster_id": "cluster-1",
            "title": "重复段落",
            "risk_level": "high",
            "files": ["A.pdf", "B.pdf"],
            "source_issues": [source],
            "occurrences": [{
                "item": source,
                "evidence": {"left_text": "相同内容", "right_text": "相同内容"},
                "docs": {"A.pdf": {"pages": [1]}, "B.pdf": {"pages": [2]}},
            }],
        }],
    }


class DuplicateStorageTests(unittest.TestCase):
    def test_round_trip_is_exact_and_conversion_is_idempotent(self):
        legacy = legacy_duplicate_payload()
        compact = compact_duplicate_payload(legacy)
        self.assertEqual(len(compact["source_items"]), 1)
        self.assertNotIn("item", compact["issues"][0]["occurrences"][0])
        self.assertEqual(hydrate_duplicate_issue(compact, compact["issues"][0]), legacy["issues"][0])
        self.assertIs(compact_duplicate_payload(compact), compact)

    def test_missing_source_reference_is_an_error(self):
        compact = compact_duplicate_payload(legacy_duplicate_payload())
        compact["issues"][0]["occurrences"][0]["source_item_id"] = "missing"
        with self.assertRaises(DuplicateSourceReferenceError):
            validate_compact_duplicate_payload(compact)

    def test_project_conversion_only_touches_merged_and_manual_cluster_payloads(self):
        legacy = legacy_duplicate_payload()
        raw_pair_result = {"issues": [{"item": {"large": "raw"}}]}
        result = {
            "technical_bid_duplicate_check": raw_pair_result,
            "technical_bid_duplicate_clusters": legacy,
            "manual_review_results": {"latest": {"technical_bid_duplicate_check": legacy}},
        }
        compact = compact_project_duplicate_results(result)
        self.assertIs(compact["technical_bid_duplicate_check"], raw_pair_result)
        self.assertEqual(compact["technical_bid_duplicate_clusters"]["storage_schema_version"], 2)
        self.assertEqual(compact["manual_review_results"]["latest"]["technical_bid_duplicate_check"]["storage_schema_version"], 2)


class ReviewIndexTests(unittest.TestCase):
    def test_retired_business_scope_diagnostic_is_not_indexed(self):
        result = {
            "business_bid_format_review": {
                "bidders": [{
                    "bidder_key": "bidder-1",
                    "checks": {
                        "integrity_check": {
                            "issues": {
                                "unclear": [{
                                    "id": "removed",
                                    "title": "商务材料组成范围待确认",
                                    "status": "unclear",
                                }],
                                "passed": [{
                                    "id": "kept",
                                    "title": "营业执照",
                                    "status": "pass",
                                }],
                            },
                        },
                    },
                }],
            },
        }
        with patch(
            "app.service.review_index.document_blob_store.save_review_index_object",
            side_effect=lambda value, **kwargs: f"{kwargs['kind']}/{kwargs['identity']}",
        ):
            summary, _, issues = build_review_index(
                result,
                project_identifier_id="project",
                result_version=build_result_version(result),
            )

        self.assertEqual([row["issue_id"] for row in issues], ["kept"])
        self.assertEqual(summary["issue_count"], 1)
        self.assertEqual(summary["status_counts"]["unclear"], 0)

    def test_legacy_summary_subtracts_retired_business_scope_diagnostic(self):
        summary = {
            "issue_count": 4,
            "risk_counts": {"none": 3, "medium": 1},
            "status_counts": {"pass": 3, "unclear": 1},
            "categories": [{
                "result_key": "business_bid_format_review",
                "issue_count": 4,
                "risk_counts": {"none": 3, "medium": 1},
                "status_counts": {"pass": 3, "unclear": 1},
            }],
        }
        cleaned = _subtract_removed_review_issue_counts(summary, [{
            "result_key": "business_bid_format_review",
            "risk_level": "medium",
            "status": "unclear",
            "count": 1,
        }])
        self.assertEqual(cleaned["issue_count"], 3)
        self.assertEqual(cleaned["status_counts"]["unclear"], 0)
        self.assertEqual(cleaned["risk_counts"]["medium"], 0)
        self.assertEqual(cleaned["categories"][0]["issue_count"], 3)
        self.assertFalse(cleaned["categories"][0]["has_risk"])

    def test_consistency_status_counts_are_independent_from_risk(self):
        result = {
            "business_bid_format_review": {
                "bidders": [{
                    "bidder_key": "bidder-1",
                    "checks": {
                        "consistency_check": {
                            "issues": {
                                "failed": [{"id": "f", "title": "固定内容变化", "status": "fail", "severity": "warning"}],
                                "unclear": [{"id": "u", "title": "边界待核验", "status": "unclear", "severity": "error"}],
                                "passed": [{"id": "p", "title": "固定内容一致", "status": "pass", "severity": "error"}],
                                "not_applicable": [{"id": "n", "title": "格式自拟", "status": "not_applicable", "severity": "error"}],
                            },
                        },
                    },
                }],
            },
        }
        with patch(
            "app.service.review_index.document_blob_store.save_review_index_object",
            side_effect=lambda value, **kwargs: f"{kwargs['kind']}/{kwargs['identity']}",
        ):
            summary, _, issues = build_review_index(
                result,
                project_identifier_id="project",
                result_version=build_result_version(result),
            )

        self.assertEqual({row["issue_id"]: row["status"] for row in issues}, {
            "f": "fail", "u": "unclear", "p": "pass", "n": "not_applicable",
        })
        self.assertEqual(summary["status_counts"], {
            "pass": 1, "fail": 1, "unclear": 1, "not_applicable": 1,
        })
        self.assertEqual(summary["review_item_count"], 4)
        self.assertEqual(summary["inconsistent_count"], 1)
        self.assertEqual(summary["unclear_count"], 1)
        self.assertEqual(summary["not_applicable_count"], 1)

    def test_optional_material_is_excluded_across_all_business_checks(self):
        title = "附件13 残疾人福利性单位声明函（格式）"
        result = {
            "business_bid_format_review": {
                "bidders": [{
                    "bidder_key": "bidder-1",
                    "checks": {
                        "integrity_check": {"issues": {"missing": [{
                            "id": "optional-source",
                            "title": title,
                            "status": "missing",
                            "evidence": {"is_optional": True},
                        }]}},
                        "consistency_check": {"issues": {"failed": [{
                            "id": "same-title-fail",
                            "title": title,
                            "status": "fail",
                            "evidence": {},
                        }]}},
                        "verification_check": {"issues": {"passed": [{
                            "id": "same-title-pass",
                            "title": title,
                            "status": "pass",
                            "evidence": {},
                        }]}},
                    },
                }],
            },
        }
        with patch(
            "app.service.review_index.document_blob_store.save_review_index_object",
            side_effect=lambda value, **kwargs: f"{kwargs['kind']}/{kwargs['identity']}",
        ):
            summary, _, issues = build_review_index(
                result,
                project_identifier_id="project",
                result_version=build_result_version(result),
            )
        self.assertFalse(any(row["issue_id"] in {
            "optional-source", "same-title-fail", "same-title-pass",
        } for row in issues))
        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0]["status"], "passed")
        self.assertEqual(summary["issue_count"], 1)

    def test_duplicate_component_uses_separate_source_detail_and_evidence_objects(self):
        component = compact_duplicate_payload(legacy_duplicate_payload())
        result = {"technical_bid_duplicate_check": component}
        saved = []

        def save(value, **kwargs):
            saved.append((kwargs["kind"], value))
            return f"{kwargs['kind']}/{kwargs['identity']}"

        version = build_result_version(result)
        with patch("app.service.review_index.document_blob_store.save_review_index_object", side_effect=save):
            summary, components, issues = build_review_index(
                result,
                project_identifier_id="project",
                result_version=version,
            )

        self.assertEqual(summary["issue_count"], 1)
        self.assertEqual(len(components), 1)
        self.assertEqual(len(issues), 1)
        component_object = next(value for kind, value in saved if kind == "component")
        evidence_object = next(value for kind, value in saved if kind == "evidence")
        self.assertNotIn("issues", component_object)
        self.assertNotIn("source_items", component_object)
        self.assertNotIn("source_items", evidence_object)
        self.assertTrue(evidence_object["source_item_object_keys"])
        self.assertEqual(sum(1 for kind, _ in saved if kind == "source"), 1)

    def test_duplicate_review_only_status_is_not_inferred_as_passed_from_risk(self):
        legacy = legacy_duplicate_payload()
        legacy["issues"][0].update({
            "risk_level": "none",
            "status": "unclear",
            "review_only": True,
        })
        result = {"technical_bid_duplicate_check": compact_duplicate_payload(legacy)}

        with patch(
            "app.service.review_index.document_blob_store.save_review_index_object",
            side_effect=lambda value, **kwargs: f"{kwargs['kind']}/{kwargs['identity']}",
        ):
            summary, _, issues = build_review_index(
                result,
                project_identifier_id="project",
                result_version=build_result_version(result),
            )

        self.assertEqual(issues[0]["status"], "unclear")
        self.assertEqual(summary["status_counts"]["unclear"], 1)
        self.assertEqual(summary["status_counts"]["pass"], 0)

    def test_duplicate_explicit_issue_ids_are_disambiguated_without_changing_first_id(self):
        result = {
            "format_check": {"issues": [{"id": "same-id", "title": "甲"}]},
            "tender_check": {"issues": [{"id": "same-id", "title": "乙"}]},
        }

        with patch(
            "app.service.review_index.document_blob_store.save_review_index_object",
            side_effect=lambda value, **kwargs: f"{kwargs['kind']}/{kwargs['identity']}",
        ):
            _, _, issues = build_review_index(
                result,
                project_identifier_id="project",
                result_version=build_result_version(result),
            )

        issue_ids = [item["issue_id"] for item in issues]
        self.assertEqual(issue_ids[0], "same-id")
        self.assertNotEqual(issue_ids[1], "same-id")
        self.assertEqual(len(set(issue_ids)), 2)

    def test_generic_issue_list_keeps_source_mapping_in_detail_only(self):
        issue = {
            "id": "consistency-1",
            "title": "固定内容待核验",
            "status": "unclear",
            "message": "已有 OCR 中未找到足够完整的对应固定内容。",
            "locations": [
                {"page": page, "bbox": [1, 2, 3, 4], "text": "正文" * 5000}
                for page in (3, 4)
            ],
        }
        result = {
            "business_bid_format_review": {
                "bidders": [{
                    "bidder_key": "bidder-1",
                    "documents": {"business": {"content": "大对象" * 5000}},
                    "checks": {"consistency_check": {"issues": {"unclear": [issue]}}},
                }],
            },
        }
        saved = []

        def save(value, **kwargs):
            saved.append((kwargs["kind"], value))
            return f"{kwargs['kind']}/{kwargs['identity']}"

        with patch("app.service.review_index.document_blob_store.save_review_index_object", side_effect=save):
            _, _, issues = build_review_index(
                result,
                project_identifier_id="project",
                result_version=build_result_version(result),
            )

        payload = issues[0]["list_payload"]
        detail = next(value for kind, value in saved if kind == "detail")
        self.assertNotIn("locations", payload)
        self.assertNotIn("documents", payload["_review_context"])
        self.assertEqual(payload["pages"], [3, 4])
        self.assertLess(len(str(payload)), 2048)
        self.assertEqual(detail["locations"], issue["locations"])

    def test_export_keeps_unique_sources_at_top_level_and_reads_objects_once(self):
        rows = [
            {
                "issue_id": f"issue-{index}",
                "result_key": "technical_bid_duplicate_check",
                "risk_level": "high",
                "status": "failed",
                "title": "重复内容",
                "description": "证据",
                "file_names": ["A.pdf", "B.pdf"],
                "list_payload": {"status": "unclear"} if index == 0 else {},
                "detail_object_key": f"detail-{index}",
                "evidence_object_key": f"evidence-{index}",
                "evidence_count": 1,
            }
            for index in range(2)
        ]

        class Cursor:
            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def execute(self, *_args, **_kwargs):
                return None

            def fetchall(self):
                return rows

        class Connection:
            def cursor(self, **_kwargs):
                return Cursor()

        @contextmanager
        def connection():
            yield Connection()

        service = PostgreSQLService()
        service._get_connection = connection
        object_values = {
            "detail-0": {"title": "甲"},
            "detail-1": {"title": "乙"},
            "evidence-0": {"occurrences": [{"source_item_id": "source"}], "source_item_object_keys": {"source": "source-key"}},
            "evidence-1": {"occurrences": [{"source_item_id": "source"}], "source_item_object_keys": {"source": "source-key"}},
            "source-key": {"duplicate_blocks": [{"left_text": "相同"}]},
        }

        with patch.object(service, "_get_project_review_head", return_value={
            "identifier_id": "project",
            "result_version": "version-1234567890",
            "input_revision": 1,
            "result_input_revision": 1,
        }), patch.object(service, "_assert_review_version"), patch(
            "app.service.postgresql_service.document_blob_store.read_blob",
            side_effect=lambda key: object_values[key],
        ) as read_blob:
            payload = service.get_project_review_export_payload(
                "project",
                result_version="version-1234567890",
            )

        self.assertEqual(payload["source_items"], {"source": object_values["source-key"]})
        self.assertNotIn("source_items", payload["result"][0]["evidence"])
        self.assertEqual(payload["result"][0]["evidence"]["source_item_ids"], ["source"])
        self.assertEqual(payload["result"][0]["source_status"], "unclear")
        self.assertEqual([call.args[0] for call in read_blob.call_args_list].count("source-key"), 1)

    def test_review_index_feature_switch_forces_legacy_compatibility_mode(self):
        class Cursor:
            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

        class Connection:
            def cursor(self, **_kwargs):
                return Cursor()

        @contextmanager
        def connection():
            yield Connection()

        service = PostgreSQLService()
        service._get_connection = connection
        head = {
            "identifier_id": "project",
            "project_name": "项目",
            "parsing_status": 3,
            "input_revision": 1,
            "result_input_revision": 1,
            "result_object_key": "result-key",
            "result_keys": ["technical_bid_duplicate_check"],
            "result_summary": {"result_keys": ["technical_bid_duplicate_check"]},
            "result_version": "version-1234567890",
            "result_update_time": None,
            "review_index_status": "ready",
            "review_summary": {"status": "ready", "issue_count": 1, "categories": []},
        }
        with patch.object(service, "_get_project_review_head", return_value=head), \
             patch("app.service.postgresql_service.settings.XTJS_REVIEW_INDEX_ENABLED", False):
            summary = service.get_project_review_summary("project")

        self.assertTrue(summary["compatibility_mode"])
        self.assertEqual(summary["status"], "legacy")


class ReviewCacheTests(unittest.TestCase):
    def test_oversized_review_cache_write_is_skipped(self):
        cache = RedisCacheService()
        cache.enabled = True
        with patch("app.service.cache_service.settings.XTJS_CACHE_MAX_ENTRY_BYTES", 8), \
             patch.object(cache, "get_json", return_value=Mock(hit=False)), \
             patch.object(cache, "set_json", wraps=cache.set_json), \
             patch.object(cache, "_redis") as redis_client:
            value, status = cache.get_or_set_json(
                "key", 10, lambda: {"value": "larger than eight"},
                allow_degraded_write=True,
            )
        self.assertEqual(value["value"], "larger than eight")
        self.assertEqual(status, "skip-large")
        redis_client.return_value.setex.assert_not_called()

    def test_review_cache_read_failure_falls_back_once(self):
        cache = RedisCacheService()
        factory = Mock(return_value={"summary": "authoritative"})
        with patch.object(cache, "get_json", side_effect=CacheUnavailableError("offline")):
            value, status = cache.get_or_set_json(
                "key", 10, factory,
                allow_degraded_read=True,
                allow_degraded_write=True,
            )
        self.assertEqual(value, {"summary": "authoritative"})
        self.assertEqual(status, "degraded-read")
        factory.assert_called_once_with()

    def test_review_cache_fill_failure_returns_authoritative_data(self):
        cache = RedisCacheService()
        factory = Mock(return_value={"items": [1]})
        with patch.object(cache, "get_json", return_value=CacheResult(hit=False)), \
             patch.object(cache, "set_json", side_effect=CacheUnavailableError("offline")):
            value, status = cache.get_or_set_json(
                "key", 10, factory,
                allow_degraded_read=True,
                allow_degraded_write=True,
            )
        self.assertEqual(value, {"items": [1]})
        self.assertEqual(status, "degraded-write")
        factory.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
