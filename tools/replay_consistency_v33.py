"""Replay stored business-bid OCR through the v3.3 consistency engine.

The default mode is read-only. --apply-preview requires an identical saved preview
and checks input/result versions under the project lock before writing. Run inside
the backend environment. The report omits document URLs.
"""

from __future__ import annotations

import argparse
from copy import deepcopy
import json
from pathlib import Path

from app.service.analysis.compliance.exact_template import _presentation_punctuation, plain_text
from app.service.analysis.project_input_loader import ProjectAnalysisInputLoader
from app.service.analysis.unified import UnifiedBusinessReviewService
from app.service.cache_service import invalidate_project_cache
from app.service.postgresql_service import PostgreSQLService


TARGET_PROJECTS = {
    "256": "2ac2f908-9362-44ed-9875-2eba2b7bacc8",
    "260": "0ba763a0-a008-4cb5-b43c-5c6711685349",
    "261": "9e5bdfdf-2478-46ed-a2d1-4d6651846c0b",
    "280": "8f1e8164-7261-4a7c-a23f-dec694e9696a",
    "258-a": "d9f00bea-8f6a-4b18-84ce-44229948b46d",
    "258-b": "741affa2-1ddc-44ae-9a2b-d9a899d340f4",
}


def _brief(segment: dict) -> dict:
    if not isinstance(segment, dict):
        return {}
    failing = [
        {"kind": item.get("kind"), "label": item.get("label"), "status": item.get("status")}
        for item in segment.get("element_results") or []
        if item.get("status") in {"fail", "unclear"}
    ]
    return {
        "name": segment.get("name"),
        "status": segment.get("status"),
        "reason": str(segment.get("difference_summary") or "")[:300],
        "items": failing[:8],
        "diffs": [
            {
                "label": str(item.get("label") or "")[:80],
                "template": str(item.get("template_text") or "")[:90],
                "bid": str(item.get("bid_text") or "")[:90],
            }
            for item in (segment.get("difference_items") or [])[:5]
            if item.get("status") == "fail"
        ],
    }


def replay(
    project_id: str, db: PostgreSQLService, service: UnifiedBusinessReviewService,
    work: dict | None = None,
) -> dict:
    record = db.get_project_result(project_id)
    payload = ProjectAnalysisInputLoader(db).load(project_id)
    if not record or not payload or record.get("results_stale"):
        return {"project_id": project_id, "error": "无当前有效结果或 OCR"}
    review = (record.get("result") or {}).get("business_bid_format_review") or {}
    documents = {
        str(doc.get("document_id") or doc.get("identifier_id")): doc
        for doc in payload.get("documents") or []
        if doc.get("document_type") == "business_bid"
    }
    skeleton_cache: dict[str, list] = {}
    bidders = []
    new_checks = {}
    for bidder in review.get("bidders") or []:
        meta = (bidder.get("documents") or {}).get("business") or {}
        doc = documents.get(str(meta.get("identifier_id") or ""))
        if not doc:
            bidders.append({"bidder": bidder.get("bidder_key"), "error": "当前材料中缺少对应商务标"})
            continue
        tender_meta = (bidder.get("documents") or {}).get("tender") or {}
        tender = doc.get("tender_content") or {}
        bid = doc.get("content") or {}
        tender_id = str(tender_meta.get("identifier_id") or doc.get("tender_identifier_id") or "")
        if tender_id not in skeleton_cache:
            skeleton_cache[tender_id] = service.consistency_checker.build_template_skeleton(tender)
        old_check = (bidder.get("checks") or {}).get("consistency_check") or {}
        new_check = service._execute_consistency_check(
            tender_payload=dict(tender, _template_source={
                "file_url": tender_meta.get("file_path"),
                "identifier_id": tender_id, "role": "tender",
                "content_checksum": tender_meta.get("sha256"),
            }),
            business_payload=dict(bid, _template_source={
                "file_url": meta.get("file_path"),
                "identifier_id": meta.get("identifier_id"), "role": "business_bid",
                "content_checksum": meta.get("sha256"),
            }),
            integrity_check=(bidder.get("checks") or {}).get("integrity_check") or {},
            prepared_skeletons=skeleton_cache[tender_id],
        )
        new_checks[str(meta.get("identifier_id"))] = new_check
        old_segments = (old_check.get("raw_result") or {}).get("evaluated_segments") or []
        new_segments = (new_check.get("raw_result") or {}).get("evaluated_segments") or []
        old_by_name = {str(segment.get("name") or ""): segment for segment in old_segments}
        new_by_name = {str(segment.get("name") or ""): segment for segment in new_segments}
        attachments = []
        names = list(dict.fromkeys([*old_by_name, *new_by_name]))
        for name in names:
            old = old_by_name.get(name) or {}
            new = new_by_name.get(name) or {}
            attachments.append({"old": _brief(old), "new": _brief(new)})
        bidders.append({
            "bidder": bidder.get("bidder_key"),
            "document_id": meta.get("identifier_id"),
            "old_status": (old_check.get("review") or {}).get("status"),
            "new_status": (new_check.get("review") or {}).get("status"),
            "engine_version": (new_check.get("raw_result") or {}).get("engine_version"),
            "attachments": attachments,
        })
    if work is not None:
        work.update(record=record, payload=payload, new_checks=new_checks)
    return {
        "project_id": project_id,
        "input_revision": record.get("input_revision"),
        "result_version": record.get("result_version"),
        "bidders": bidders,
    }


def _replace_consistency_rows(old_rows: list[dict], fresh_rows: list[dict]) -> list[dict]:
    replacement = [deepcopy(row) for row in fresh_rows if row.get("check_code") == "consistency_check"]
    result = []
    inserted = False
    for row in old_rows:
        if row.get("check_code") == "consistency_check":
            if not inserted:
                result.extend(replacement)
                inserted = True
            continue
        result.append(deepcopy(row))
    if not inserted:
        result.extend(replacement)
    assert [row for row in result if row.get("check_code") != "consistency_check"] == [
        row for row in old_rows if row.get("check_code") != "consistency_check"
    ]
    return result


def _candidate_result(work: dict, service: UnifiedBusinessReviewService) -> dict:
    record = work["record"]
    root = deepcopy(record["result"])
    review = root["business_bid_format_review"]
    documents = {
        str(doc.get("document_id") or doc.get("identifier_id")): doc
        for doc in work["payload"].get("documents") or []
        if doc.get("document_type") == "business_bid"
    }
    for bidder in review.get("bidders") or []:
        meta = (bidder.get("documents") or {}).get("business") or {}
        document_id = str(meta.get("identifier_id") or "")
        new_check = deepcopy(work["new_checks"][document_id])
        bidder["checks"]["consistency_check"] = new_check
        bidder["issues"] = service._aggregate_bidder_issues(bidder["checks"])
        bidder["summary"] = service._summarize_bidder_checks(bidder["checks"])
        tender_meta = (bidder.get("documents") or {}).get("tender") or {}
        technical_meta = (bidder.get("documents") or {}).get("technical") or {}
        new_guide = service._build_bidder_reading_guide(
            bidder_key=bidder["bidder_key"], bidder_name=bidder["bidder_name"],
            summary=bidder["summary"], checks={"consistency_check": new_check},
            tender_meta=tender_meta, business_meta=meta, technical_meta=technical_meta,
        )
        guide = bidder.get("reading_guide") or {}
        navigation = [
            row for row in guide.get("check_navigation") or []
            if row.get("check_code") != "consistency_check"
        ]
        navigation.extend(new_guide["check_navigation"])
        navigation.sort(key=lambda row: (
            service._review_status_sort_key(row.get("status")),
            service._check_display_index(row.get("check_code")),
        ))
        guide["check_navigation"] = navigation
        guide["overall_review_status"] = bidder["summary"]["overall_review_status"]
        guide["check_status_counts"] = bidder["summary"]["review_status_counts"]
        bidder["reading_guide"] = guide

    review["summary"] = service._summarize_review(review["bidders"])
    review["function_validation"] = service._summarize_function_validation(review["bidders"])
    tender_meta = (review.get("dataset") or {}).get("tender") or {}
    fresh_guide = service._build_review_reading_guide(
        tender_meta=tender_meta, bidders=review["bidders"],
    )
    review["reading_guide"]["bidder_overview"] = fresh_guide["bidder_overview"]

    tables = review.get("extraction_tables") or {}
    tender_table = tables.get("tender_table") or {}
    if not tender_table or len(tables.get("bidder_tables") or []) != len(review["bidders"]):
        raise ValueError("抽数表结构与投标人数量不一致")
    first = review["bidders"][0]
    first_id = str(((first.get("documents") or {}).get("business") or {}).get("identifier_id") or "")
    tender_payload = documents[first_id]["tender_content"]
    tender_rows = _replace_consistency_rows(
        tender_table.get("rows") or [],
        service._build_tender_extraction_rows(
            tender_payload=tender_payload, bidder_reviews=review["bidders"],
        ),
    )
    tender_table["rows"] = tender_rows
    tender_table["row_count"] = len(tender_rows)
    tender_table["check_row_counts"] = service._count_extraction_rows(tender_rows)
    all_bid_rows = []
    bidder_by_key = {str(bidder["bidder_key"]): bidder for bidder in review["bidders"]}
    for table in tables["bidder_tables"]:
        bidder = bidder_by_key[str(table.get("bidder_key") or "")]
        meta = (bidder.get("documents") or {}).get("business") or {}
        document = documents[str(meta.get("identifier_id") or "")]
        fresh = service._build_bid_extraction_rows(
            bidder=bidder, business_payload=document["content"], technical_payload=None,
        )
        rows = _replace_consistency_rows(table.get("rows") or [], fresh)
        table["rows"] = rows
        table["row_count"] = len(rows)
        table["check_row_counts"] = service._count_extraction_rows(rows)
        all_bid_rows.extend(rows)
    tables["catalog"] = service._build_extraction_catalog(
        tender_rows=tender_rows, bid_rows=all_bid_rows,
    )
    tables["summary"] = {
        "tender_row_count": len(tender_rows),
        "bidder_count": len(tables["bidder_tables"]),
        "bid_row_count": len(all_bid_rows),
        "total_row_count": len(tender_rows) + len(all_bid_rows),
    }
    return root


def _assert_preserved(before: dict, after: dict) -> None:
    assert before.keys() == after.keys()
    for key in before:
        if key != "business_bid_format_review":
            assert before[key] == after[key], f"非商务标结果被修改：{key}"
    old_review = before["business_bid_format_review"]
    new_review = after["business_bid_format_review"]
    for old_bidder, new_bidder in zip(old_review["bidders"], new_review["bidders"]):
        old_checks = old_bidder["checks"]
        new_checks = new_bidder["checks"]
        assert old_checks.keys() == new_checks.keys()
        for key in old_checks:
            if key != "consistency_check":
                assert old_checks[key] == new_checks[key], f"其他检查被修改：{key}"
        assert old_bidder["documents"] == new_bidder["documents"]
        assert old_bidder.get("bidder_identity") == new_bidder.get("bidder_identity")
    for name in ("tender_table", *range(len(old_review["extraction_tables"]["bidder_tables"]))):
        if name == "tender_table":
            old_rows = old_review["extraction_tables"][name]["rows"]
            new_rows = new_review["extraction_tables"][name]["rows"]
        else:
            old_rows = old_review["extraction_tables"]["bidder_tables"][name]["rows"]
            new_rows = new_review["extraction_tables"]["bidder_tables"][name]["rows"]
        assert [row for row in old_rows if row.get("check_code") != "consistency_check"] == [
            row for row in new_rows if row.get("check_code") != "consistency_check"
        ], "其他抽数行被修改"


def _apply_review(
    project_id: str, expected: dict, current: dict, work: dict,
    db: PostgreSQLService, service: UnifiedBusinessReviewService,
) -> str:
    if current != expected:
        return "SKIPPED: 重放与预览不一致"
    if any(bidder.get("error") for bidder in current.get("bidders") or []):
        return "SKIPPED: 存在未映射的投标文件"
    if any(bidder.get("engine_version") != "fixed-template-exact-v3.3" for bidder in current.get("bidders") or []):
        return "SKIPPED: 引擎版本不一致"
    if any((check.get("execution") or {}).get("status") != "ok" for check in work["new_checks"].values()):
        return "SKIPPED: 一致性检查执行失败"
    candidate = _candidate_result(work, service)
    _assert_preserved(work["record"]["result"], candidate)
    if candidate == work["record"]["result"]:
        return "UNCHANGED"
    with db._locked_project_result(project_id) as (cursor, project, existing):
        if (
            int(project["input_revision"]) != int(expected["input_revision"])
            or str(existing.get("result_version")) != str(expected["result_version"])
        ):
            return "SKIPPED: 输入或结果版本已改变"
        _assert_preserved(existing["result"], candidate)
        written = db._persist_project_result(cursor, project, candidate)
        _assert_preserved(existing["result"], written["result"])
    invalidate_project_cache(project_id)
    return f"UPDATED: {written['result_version']}"


def discover(db: PostgreSQLService) -> list[tuple[str, str, int]]:
    with db._get_connection() as connection, connection.cursor() as cursor:
        cursor.execute(
            """SELECT p.identifier_id,p.project_name FROM xtjs_projects p
               JOIN xtjs_result r ON p.identifier_id=r.project_identifier_id
               WHERE NOT p.deleted AND p.input_revision=r.input_revision"""
        )
        candidates = cursor.fetchall()
    found = []
    for project_id, name in candidates:
        if str(project_id) in TARGET_PROJECTS.values():
            continue
        record = db.get_project_result(str(project_id)) or {}
        review = (record.get("result") or {}).get("business_bid_format_review") or {}
        hits = 0
        for bidder in review.get("bidders") or []:
            raw = ((bidder.get("checks") or {}).get("consistency_check") or {}).get("raw_result") or {}
            for segment in raw.get("evaluated_segments") or []:
                if segment.get("status") != "fail":
                    continue
                if "报价表" in str(segment.get("name") or ""):
                    hits += 1
                    continue
                for item in segment.get("difference_items") or []:
                    left, right = str(item.get("template_text") or ""), str(item.get("bid_text") or "")
                    if left != right and _presentation_punctuation(plain_text(left)) == _presentation_punctuation(plain_text(right)):
                        hits += 1
                        break
        if hits:
            found.append((str(project_id), str(name), hits))
    return found


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("projects", nargs="*", default=list(TARGET_PROJECTS))
    parser.add_argument("--output", type=Path)
    parser.add_argument("--discover", action="store_true", help="列出其他当前项目中的报价表/标点误报候选")
    parser.add_argument("--apply-preview", type=Path, help="按已核对的只读预览作版本校验后写回")
    parser.add_argument("--validate-candidate", action="store_true", help="只读验证写回候选仅改变一致性及汇总")
    args = parser.parse_args()
    db = PostgreSQLService()
    service = UnifiedBusinessReviewService(db)
    if args.discover:
        print(json.dumps(discover(db), ensure_ascii=False, indent=2))
        return
    preview = {}
    if args.apply_preview:
        preview = {
            str(entry.get("project_id")): entry
            for entry in json.loads(args.apply_preview.read_text(encoding="utf-8"))
        }
    results = []
    for name in args.projects:
        project_id = TARGET_PROJECTS.get(name, name)
        work = {}
        result = replay(project_id, db, service, work)
        result["project_name"] = name
        results.append(result)
        if args.validate_candidate and not result.get("error"):
            candidate = _candidate_result(work, service)
            _assert_preserved(work["record"]["result"], candidate)
            print(name, "candidate validated", flush=True)
        if args.apply_preview:
            expected = preview.get(project_id)
            state = _apply_review(project_id, expected, result, work, db, service) if expected else "SKIPPED: 无预览"
            print(name, state, flush=True)
        else:
            print(name, "bidders", len(result.get("bidders") or []), "error", result.get("error"), flush=True)
    content = json.dumps(results, ensure_ascii=False, indent=2)
    if args.output:
        args.output.write_text(content, encoding="utf-8")
        print("report", args.output)
    else:
        print(content)


if __name__ == "__main__":
    main()
