from __future__ import annotations

import argparse
import hashlib
import html
import json
import sys
import traceback
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.core.document_types import (  # noqa: E402
    DOCUMENT_TYPE_BUSINESS_BID,
    DOCUMENT_TYPE_TECHNICAL_BID,
    DOCUMENT_TYPE_TENDER,
)
from app.service.analysis.bid_document_review import BidDocumentReviewService  # noqa: E402
from app.service.analysis.duplicate_check import DuplicateCheckService  # noqa: E402
from app.service.analysis.unified_business_review import UnifiedBusinessReviewService  # noqa: E402


BUSINESS_LABEL = "商务标"
TECHNICAL_LABEL = "技术标"
TENDER_LABEL = "招标文件"

BUSINESS_HINTS = (
    "商务标",
    "商务响应",
    "商务文件",
    "商务扫描",
    "商务",
    "business",
    "shangwu",
    "sangwu",
)
TECHNICAL_HINTS = (
    "技术标",
    "技术响应",
    "技术文件",
    "技术扫描",
    "技术",
    "technical",
)
TENDER_HINTS = (
    "招标",
    "采购文件",
    "tender",
)


@dataclass(frozen=True)
class LocalDocument:
    path: Path
    role: str
    bidder_key: str | None
    identifier_id: str
    content: dict[str, Any]
    meta: dict[str, Any]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    return payload if isinstance(payload, dict) else {"data": payload}


def data_node(payload: dict[str, Any]) -> dict[str, Any]:
    data = payload.get("data")
    return data if isinstance(data, dict) else payload


def max_page_in_payload(value: Any) -> int:
    if isinstance(value, dict):
        pages: list[int] = []
        for key, member in value.items():
            if key in {"page", "page_no", "page_num", "page_index"} and isinstance(member, int):
                pages.append(member)
            elif key in {"pages", "page_numbers", "page_nos"} and isinstance(member, list):
                pages.extend(item for item in member if isinstance(item, int))
            else:
                pages.append(max_page_in_payload(member))
        return max(pages or [0])
    if isinstance(value, list):
        return max((max_page_in_payload(item) for item in value), default=0)
    return 0


def page_count(payload: dict[str, Any]) -> int:
    container = data_node(payload)
    pages = container.get("pages")
    if isinstance(pages, list) and pages:
        return len(pages)
    return max(
        max_page_in_payload(container.get("layout_sections") or []),
        max_page_in_payload(container.get("logical_tables") or []),
        max_page_in_payload(container.get("native_tables") or []),
    )


def build_meta(path: Path, *, role: str, bidder_key: str | None, content: dict[str, Any]) -> dict[str, Any]:
    raw_bytes = path.read_bytes()
    stat = path.stat()
    container = data_node(content)
    return {
        "role": role,
        "bidder_key": bidder_key,
        "file_name": path.name,
        "file_path": str(path),
        "file_size": stat.st_size,
        "modified_at": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
        "sha256": hashlib.sha256(raw_bytes).hexdigest(),
        "layout_section_count": len(container.get("layout_sections", []) or []),
        "logical_table_count": len(container.get("logical_tables", []) or []),
        "native_table_count": len(container.get("native_tables", []) or []),
        "page_count": page_count(content),
        "source_type": "local_json",
    }


def make_identifier(path: Path, role: str) -> str:
    digest = hashlib.sha1(str(path.resolve()).encode("utf-8")).hexdigest()[:12]
    return f"local_{role}_{digest}"


def detect_role(path: Path) -> str | None:
    stem = path.stem.strip()
    lower = stem.lower()
    if any(hint.lower() in lower for hint in TENDER_HINTS) or lower.endswith("-model") or lower == "model":
        return DOCUMENT_TYPE_TENDER
    if any(hint.lower() in lower for hint in TECHNICAL_HINTS):
        return DOCUMENT_TYPE_TECHNICAL_BID
    if any(hint.lower() in lower for hint in BUSINESS_HINTS):
        return DOCUMENT_TYPE_BUSINESS_BID
    return None


def bidder_key_from_path(path: Path, role: str | None) -> str:
    stem = path.stem.strip()
    remove_tokens = [
        "投标文件",
        "响应文件",
        "电子投标文件",
        "扫描件",
        "商务标文件",
        "技术标文件",
        "商务响应文件",
        "技术响应文件",
        "商务标",
        "技术标",
        "商务",
        "技术",
        "business",
        "technical",
        "shangwu",
        "sangwu",
    ]
    normalized = stem
    for token in remove_tokens:
        normalized = normalized.replace(token, "")
        normalized = normalized.replace(token.upper(), "")
        normalized = normalized.replace(token.capitalize(), "")
    normalized = normalized.strip(" _-()（）[]【】")
    return normalized or stem


def role_label(role: str) -> str:
    if role == DOCUMENT_TYPE_TENDER:
        return TENDER_LABEL
    if role == DOCUMENT_TYPE_BUSINESS_BID:
        return BUSINESS_LABEL
    if role == DOCUMENT_TYPE_TECHNICAL_BID:
        return TECHNICAL_LABEL
    return role


def find_json_files(dataset_dir: Path) -> list[Path]:
    return sorted(
        {
            path.resolve()
            for pattern in ("*.json", "*.JSON")
            for path in dataset_dir.glob(pattern)
            if path.is_file()
        },
        key=lambda item: item.name.lower(),
    )


def load_documents(dataset_dir: Path, *, unknown_role: str) -> tuple[dict[str, Any], list[LocalDocument]]:
    files = find_json_files(dataset_dir)
    if not files:
        raise FileNotFoundError(f"no JSON files found under {dataset_dir}")

    classified: list[tuple[Path, str | None]] = [(path, detect_role(path)) for path in files]
    explicit_tenders = [path for path, role in classified if role == DOCUMENT_TYPE_TENDER]
    explicit_business = [path for path, role in classified if role == DOCUMENT_TYPE_BUSINESS_BID]
    explicit_technical = [path for path, role in classified if role == DOCUMENT_TYPE_TECHNICAL_BID]
    warnings: list[str] = []

    tender_path: Path | None = None
    if explicit_tenders:
        tender_path = sorted(
            explicit_tenders,
            key=lambda path: (
                0 if "招标" in path.stem else 1,
                0 if path.stem.lower().endswith("-model") else 1,
                path.name.lower(),
            ),
        )[0]
        if len(explicit_tenders) > 1:
            warnings.append(
                "found multiple tender JSON candidates; using "
                f"{tender_path.name}: {', '.join(path.name for path in explicit_tenders)}"
            )
    else:
        warnings.append("no tender JSON candidate found; format review and tender-template exclusion may be skipped")

    documents: list[LocalDocument] = []
    for path, detected_role in classified:
        role = detected_role
        if role is None:
            if unknown_role == "skip":
                warnings.append(f"skipped unclassified JSON file: {path.name}")
                continue
            role = DOCUMENT_TYPE_BUSINESS_BID if unknown_role == "business" else DOCUMENT_TYPE_TECHNICAL_BID
            warnings.append(f"treated unclassified JSON file as {role_label(role)}: {path.name}")

        if tender_path and path == tender_path:
            role = DOCUMENT_TYPE_TENDER
        elif role == DOCUMENT_TYPE_TENDER:
            warnings.append(f"skipped extra tender-like JSON file: {path.name}")
            continue

        content = load_json(path)
        bidder_key = None if role == DOCUMENT_TYPE_TENDER else bidder_key_from_path(path, role)
        documents.append(
            LocalDocument(
                path=path,
                role=role,
                bidder_key=bidder_key,
                identifier_id=make_identifier(path, role),
                content=content,
                meta=build_meta(path, role=role, bidder_key=bidder_key, content=content),
            )
        )

    discovered = {
        "dataset_dir": str(dataset_dir),
        "file_count": len(files),
        "tender_file": tender_path.name if tender_path else None,
        "explicit_business_file_count": len(explicit_business),
        "explicit_technical_file_count": len(explicit_technical),
        "warnings": warnings,
        "documents": [
            {
                "file_name": doc.path.name,
                "role": doc.role,
                "role_label": role_label(doc.role),
                "bidder_key": doc.bidder_key,
                "identifier_id": doc.identifier_id,
                "page_count": doc.meta.get("page_count"),
            }
            for doc in documents
        ],
    }
    return discovered, documents


def documents_by_role(documents: list[LocalDocument], role: str) -> list[LocalDocument]:
    return [doc for doc in documents if doc.role == role]


def build_document_records(documents: list[LocalDocument], tender: LocalDocument | None) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    relation_index = 1
    for doc in documents:
        if doc.role == DOCUMENT_TYPE_TENDER:
            continue
        records.append(
            {
                "relation_id": f"local_relation_{relation_index}",
                "relation_role": doc.role,
                "document_id": doc.identifier_id,
                "identifier_id": doc.identifier_id,
                "document_type": doc.role,
                "file_name": doc.path.name,
                "file_url": str(doc.path),
                "extracted": True,
                "content": doc.content,
                "tender_identifier_id": tender.identifier_id if tender else None,
                "tender_document_type": DOCUMENT_TYPE_TENDER if tender else None,
                "tender_file_name": tender.path.name if tender else None,
                "tender_file_url": str(tender.path) if tender else None,
                "tender_extracted": True if tender else None,
                "tender_content": tender.content if tender else None,
            }
        )
        relation_index += 1
    return records


def group_bidder_sources(documents: list[LocalDocument]) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for doc in documents:
        if doc.role not in {DOCUMENT_TYPE_BUSINESS_BID, DOCUMENT_TYPE_TECHNICAL_BID}:
            continue
        key = doc.bidder_key or doc.path.stem
        entry = grouped.setdefault(key, {"bidder_key": key})
        if doc.role == DOCUMENT_TYPE_BUSINESS_BID and "business" not in entry:
            entry["business"] = {"content": doc.content, "meta": doc.meta}
        elif doc.role == DOCUMENT_TYPE_TECHNICAL_BID and "technical" not in entry:
            entry["technical"] = {"content": doc.content, "meta": doc.meta}
    return [grouped[key] for key in sorted(grouped)]


def run_format_review(
    dataset_dir: Path,
    project_identifier: str,
    documents: list[LocalDocument],
) -> dict[str, Any]:
    service = UnifiedBusinessReviewService()
    tender_docs = documents_by_role(documents, DOCUMENT_TYPE_TENDER)
    tender = tender_docs[0] if tender_docs else None
    if tender is None:
        return {"skipped": True, "reason": "missing_tender_json"}

    try:
        review = service.review_dataset(dataset_dir, project_identifier=project_identifier)
        review["local_runner_mode"] = "native_dataset_discovery"
        return review
    except Exception as exc:
        fallback_note = f"{type(exc).__name__}: {exc}"

    bidder_sources = group_bidder_sources(documents)
    bidders: list[dict[str, Any]] = []
    for bidder in bidder_sources:
        if not bidder.get("business"):
            continue
        if bidder.get("technical"):
            bidders.append(
                service._review_bidder(
                    tender_payload=tender.content,
                    tender_meta=tender.meta,
                    bidder=bidder,
                )
            )
        else:
            business = bidder["business"]
            bidders.append(
                service._review_business_bidder(
                    tender_payload=tender.content,
                    tender_meta=tender.meta,
                    bidder_key=str(bidder.get("bidder_key") or "unknown_bidder"),
                    business_payload=business["content"],
                    business_meta=business["meta"],
                )
            )

    extraction_tables = service._build_review_extraction_tables(
        tender_payload=tender.content,
        tender_meta=tender.meta,
        bidder_sources=bidder_sources,
        bidder_reviews=bidders,
    )
    reading_guide = service._build_review_reading_guide(
        tender_meta=tender.meta,
        bidders=bidders,
    )
    return {
        "schema_version": service.RESULT_SCHEMA_VERSION,
        "review_type": "unified_business_review",
        "generated_at": utc_now_iso(),
        "project_identifier_id": project_identifier,
        "local_runner_mode": "fallback_local_discovery",
        "native_discovery_error": fallback_note,
        "dataset": {
            "base_dir": str(dataset_dir),
            "tender": tender.meta,
            "bidders": [
                {
                    "bidder_key": item.get("bidder_key"),
                    "business": (item.get("business") or {}).get("meta"),
                    "technical": (item.get("technical") or {}).get("meta"),
                }
                for item in bidder_sources
            ],
            "file_count": len(documents),
        },
        "reading_guide": reading_guide,
        "extraction_tables": extraction_tables,
        "function_validation": service._summarize_function_validation(bidders),
        "summary": service._summarize_review(bidders),
        "bidders": bidders,
    }


def run_duplicate_check(
    project_identifier: str,
    document_records: list[dict[str, Any]],
    document_type: str,
    max_pairs_per_type: int,
) -> dict[str, Any]:
    service = DuplicateCheckService()
    return service.check_project_documents(
        project_identifier=project_identifier,
        project={"identifier_id": project_identifier},
        document_records=document_records,
        document_types=[document_type],
        max_pairs_per_type=max_pairs_per_type,
    )


def run_bid_document_review(
    project_identifier: str,
    document_records: list[dict[str, Any]],
) -> dict[str, Any]:
    service = BidDocumentReviewService()
    return service.check_project_documents(
        project_identifier=project_identifier,
        project={"identifier_id": project_identifier},
        document_records=document_records,
        document_types=[DOCUMENT_TYPE_BUSINESS_BID, DOCUMENT_TYPE_TECHNICAL_BID],
    )


def clean_json(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): clean_json(member) for key, member in value.items()}
    if isinstance(value, (list, tuple)):
        return [clean_json(member) for member in value]
    if isinstance(value, set):
        return sorted(clean_json(member) for member in value)
    if isinstance(value, Path):
        return str(value)
    return value


def status_counts_text(counts: dict[str, Any] | None) -> str:
    if not isinstance(counts, dict):
        return "-"
    return " / ".join(f"{html.escape(str(key))}: {html.escape(str(value))}" for key, value in counts.items())


def render_pair_rows(result: dict[str, Any], doc_type: str) -> str:
    group = ((result.get("groups") or {}).get(doc_type) or {})
    items = group.get("items") or []
    if not items:
        return "<tr><td colspan='6'>无可疑查重对</td></tr>"
    rows = []
    for item in items[:50]:
        rows.append(
            "<tr>"
            f"<td>{html.escape(str(item.get('risk_level') or '-'))}</td>"
            f"<td>{html.escape(str(item.get('exact_match_score') or 0))}</td>"
            f"<td>{html.escape(str(item.get('left_file_name') or '-'))}</td>"
            f"<td>{html.escape(str(item.get('right_file_name') or '-'))}</td>"
            f"<td>{html.escape(str(((item.get('metrics') or {}).get('exact_block_count')) or 0))}</td>"
            f"<td>{html.escape(str(((item.get('metrics') or {}).get('exact_table_count')) or 0))}</td>"
            "</tr>"
        )
    return "\n".join(rows)


def render_typo_rows(result: dict[str, Any]) -> str:
    rows = []
    groups = result.get("groups") or {}
    for group in groups.values():
        typo = group.get("typo_check") or {}
        for document in typo.get("documents") or []:
            for item in document.get("items") or []:
                rows.append(
                    "<tr>"
                    f"<td>{html.escape(str(item.get('matched_text') or '-'))}</td>"
                    f"<td>{html.escape(str(item.get('suggestion') or '-'))}</td>"
                    f"<td>{html.escape(str(item.get('file_name') or document.get('file_name') or '-'))}</td>"
                    f"<td>{html.escape(str(item.get('page') or '-'))}</td>"
                    f"<td>{html.escape(str(item.get('text') or '-'))}</td>"
                    "</tr>"
                )
    if not rows:
        return "<tr><td colspan='5'>无错别字候选</td></tr>"
    return "\n".join(rows[:100])


def render_personnel_rows(result: dict[str, Any]) -> str:
    rows = []
    groups = result.get("groups") or {}
    for group in groups.values():
        reuse = group.get("personnel_reuse_check") or {}
        for item in (reuse.get("items") or reuse.get("reused_names") or []):
            files = sorted({str(entry.get("file_name") or "-") for entry in item.get("items") or []})
            pages = sorted({str(entry.get("page") or "-") for entry in item.get("items") or []})
            rows.append(
                "<tr>"
                f"<td>{html.escape(str(item.get('name') or '-'))}</td>"
                f"<td>{html.escape(str(item.get('risk_level') or '-'))}</td>"
                f"<td>{html.escape(str(item.get('document_count') or 0))}</td>"
                f"<td>{html.escape(', '.join(files))}</td>"
                f"<td>{html.escape(', '.join(pages))}</td>"
                "</tr>"
            )
    if not rows:
        return "<tr><td colspan='5'>未发现一人多用</td></tr>"
    return "\n".join(rows[:100])


def collect_runtime_warnings(result: dict[str, Any]) -> list[str]:
    warnings: list[str] = []
    format_review = result.get("format_review") or {}
    if format_review.get("skipped"):
        warnings.append(f"形式审查跳过：{format_review.get('reason') or 'unknown_reason'}")

    bid_review = result.get("bid_document_review") or {}
    for role, group in (bid_review.get("groups") or {}).items():
        typo = group.get("typo_check") or {}
        if typo.get("engine") == "model_unavailable":
            notes = typo.get("notes") or []
            load_error = next((item for item in notes if "Model load error:" in str(item)), "")
            suffix = f"；{load_error}" if load_error else ""
            warnings.append(f"{role_label(str(role))}错别字模型不可用，当前不会输出错别字候选{suffix}")

    seen = set()
    unique: list[str] = []
    for item in warnings:
        if item and item not in seen:
            seen.add(item)
            unique.append(item)
    return unique


def render_html_report(result: dict[str, Any]) -> str:
    discovery = result.get("discovery") or {}
    format_review = result.get("format_review") or {}
    business_dup = result.get("business_duplicate_check") or {}
    technical_dup = result.get("technical_duplicate_check") or {}
    bid_review = result.get("bid_document_review") or {}
    format_summary = format_review.get("summary") or {}
    bid_summary = bid_review.get("summary") or {}
    doc_rows = []
    for doc in discovery.get("documents") or []:
        doc_rows.append(
            "<tr>"
            f"<td>{html.escape(str(doc.get('role_label') or doc.get('role') or '-'))}</td>"
            f"<td>{html.escape(str(doc.get('file_name') or '-'))}</td>"
            f"<td>{html.escape(str(doc.get('bidder_key') or '-'))}</td>"
            f"<td>{html.escape(str(doc.get('page_count') or 0))}</td>"
            "</tr>"
        )
    report_warnings = list(discovery.get("warnings") or []) + collect_runtime_warnings(result)
    warning_items = "".join(
        f"<li>{html.escape(str(item))}</li>"
        for item in report_warnings
    )
    if not warning_items:
        warning_items = "<li>无</li>"

    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>全业务审查结果</title>
  <style>
    body {{ margin: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; background: #f7f7f4; color: #1c1c1c; }}
    header {{ padding: 28px 36px 18px; background: #19324a; color: #fff; }}
    main {{ padding: 24px 36px 40px; max-width: 1320px; margin: 0 auto; }}
    h1 {{ margin: 0 0 8px; font-size: 26px; }}
    h2 {{ margin: 28px 0 12px; font-size: 18px; }}
    .meta {{ color: #d8e0e7; font-size: 13px; }}
    .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(210px, 1fr)); gap: 12px; }}
    .card {{ background: #fff; border: 1px solid #d9ded8; border-radius: 8px; padding: 14px 16px; }}
    .label {{ color: #5f6862; font-size: 12px; }}
    .value {{ margin-top: 6px; font-size: 20px; font-weight: 700; }}
    table {{ width: 100%; border-collapse: collapse; background: #fff; border: 1px solid #d9ded8; }}
    th, td {{ padding: 9px 10px; border-bottom: 1px solid #e6e8e4; text-align: left; vertical-align: top; font-size: 13px; }}
    th {{ background: #eef1ed; font-weight: 700; }}
    code {{ background: #eef1ed; padding: 2px 5px; border-radius: 4px; }}
    ul {{ background: #fff; border: 1px solid #d9ded8; border-radius: 8px; padding: 12px 28px; }}
  </style>
</head>
<body>
  <header>
    <h1>全业务审查结果</h1>
    <div class="meta">{html.escape(str(result.get("project_identifier_id") or "-"))} | {html.escape(str(result.get("generated_at") or "-"))}</div>
  </header>
  <main>
    <section class="grid">
      <div class="card"><div class="label">JSON 文件数</div><div class="value">{html.escape(str(discovery.get("file_count") or 0))}</div></div>
      <div class="card"><div class="label">形式审查投标人数</div><div class="value">{html.escape(str(format_summary.get("bidder_count") or 0))}</div></div>
      <div class="card"><div class="label">商务查重可疑对</div><div class="value">{html.escape(str(((business_dup.get("summary") or {}).get("suspicious_pair_count")) or 0))}</div></div>
      <div class="card"><div class="label">技术查重可疑对</div><div class="value">{html.escape(str(((technical_dup.get("summary") or {}).get("suspicious_pair_count")) or 0))}</div></div>
      <div class="card"><div class="label">错别字候选</div><div class="value">{html.escape(str(bid_summary.get("typo_issue_count") or 0))}</div></div>
      <div class="card"><div class="label">一人多用姓名</div><div class="value">{html.escape(str(bid_summary.get("reused_name_count") or 0))}</div></div>
    </section>

    <h2>文件识别</h2>
    <table><thead><tr><th>角色</th><th>文件</th><th>投标人键</th><th>页数</th></tr></thead><tbody>{''.join(doc_rows)}</tbody></table>
    <h2>运行提示</h2>
    <ul>{warning_items}</ul>

    <h2>形式审查概览</h2>
    <table><tbody>
      <tr><th>运行模式</th><td>{html.escape(str(format_review.get("local_runner_mode") or ("skipped" if format_review.get("skipped") else "-")))}</td></tr>
      <tr><th>审查状态统计</th><td>{status_counts_text(format_summary.get("review_status_counts"))}</td></tr>
      <tr><th>校验状态统计</th><td>{status_counts_text(format_summary.get("validation_status_counts"))}</td></tr>
      <tr><th>跳过原因</th><td>{html.escape(str(format_review.get("reason") or format_review.get("native_discovery_error") or "-"))}</td></tr>
    </tbody></table>

    <h2>商务标查重</h2>
    <table><thead><tr><th>风险</th><th>分数</th><th>文件 A</th><th>文件 B</th><th>重复句</th><th>重复表</th></tr></thead><tbody>{render_pair_rows(business_dup, DOCUMENT_TYPE_BUSINESS_BID)}</tbody></table>

    <h2>技术标查重</h2>
    <table><thead><tr><th>风险</th><th>分数</th><th>文件 A</th><th>文件 B</th><th>重复句</th><th>重复表</th></tr></thead><tbody>{render_pair_rows(technical_dup, DOCUMENT_TYPE_TECHNICAL_BID)}</tbody></table>

    <h2>一人多用</h2>
    <table><thead><tr><th>姓名</th><th>风险</th><th>涉及文件数</th><th>文件</th><th>页码</th></tr></thead><tbody>{render_personnel_rows(bid_review)}</tbody></table>

    <h2>错别字</h2>
    <table><thead><tr><th>原字</th><th>建议</th><th>文件</th><th>页码</th><th>原文</th></tr></thead><tbody>{render_typo_rows(bid_review)}</tbody></table>
  </main>
</body>
</html>
"""


def run_pipeline(args: argparse.Namespace) -> dict[str, Any]:
    dataset_dir = Path(args.dataset).expanduser().resolve()
    if not dataset_dir.exists():
        raise FileNotFoundError(f"dataset does not exist: {dataset_dir}")
    if not dataset_dir.is_dir():
        raise NotADirectoryError(f"dataset is not a directory: {dataset_dir}")

    project_identifier = args.project_identifier or f"local_full_pipeline_{hashlib.sha1(str(dataset_dir).encode('utf-8')).hexdigest()[:10]}"
    discovery, documents = load_documents(dataset_dir, unknown_role=args.unknown_role)
    tender_docs = documents_by_role(documents, DOCUMENT_TYPE_TENDER)
    tender = tender_docs[0] if tender_docs else None
    records = build_document_records(documents, tender)

    result: dict[str, Any] = {
        "schema_version": "1.0",
        "review_type": "local_full_business_pipeline",
        "generated_at": utc_now_iso(),
        "project_identifier_id": project_identifier,
        "dataset_dir": str(dataset_dir),
        "discovery": discovery,
    }

    steps: list[dict[str, Any]] = []

    def run_step(name: str, func) -> Any:
        started = datetime.now(timezone.utc)
        try:
            value = func()
            steps.append(
                {
                    "name": name,
                    "status": "ok",
                    "started_at": started.isoformat(),
                    "finished_at": utc_now_iso(),
                }
            )
            return value
        except Exception as exc:
            steps.append(
                {
                    "name": name,
                    "status": "error",
                    "started_at": started.isoformat(),
                    "finished_at": utc_now_iso(),
                    "error": f"{type(exc).__name__}: {exc}",
                    "traceback": traceback.format_exc(),
                }
            )
            return {"error": f"{type(exc).__name__}: {exc}"}

    result["format_review"] = run_step(
        "business_format_review",
        lambda: run_format_review(dataset_dir, project_identifier, documents),
    )
    result["business_duplicate_check"] = run_step(
        "business_duplicate_check",
        lambda: run_duplicate_check(project_identifier, records, DOCUMENT_TYPE_BUSINESS_BID, args.max_pairs_per_type),
    )
    result["technical_duplicate_check"] = run_step(
        "technical_duplicate_check",
        lambda: run_duplicate_check(project_identifier, records, DOCUMENT_TYPE_TECHNICAL_BID, args.max_pairs_per_type),
    )
    result["bid_document_review"] = run_step(
        "bid_document_review_typo_and_personnel",
        lambda: run_bid_document_review(project_identifier, records),
    )
    result["steps"] = steps
    result["summary"] = {
        "step_status_counts": {
            "ok": sum(1 for item in steps if item.get("status") == "ok"),
            "error": sum(1 for item in steps if item.get("status") == "error"),
        },
        "business_duplicate_suspicious_pair_count": (
            ((result.get("business_duplicate_check") or {}).get("summary") or {}).get("suspicious_pair_count")
        ),
        "technical_duplicate_suspicious_pair_count": (
            ((result.get("technical_duplicate_check") or {}).get("summary") or {}).get("suspicious_pair_count")
        ),
        "typo_issue_count": ((result.get("bid_document_review") or {}).get("summary") or {}).get("typo_issue_count"),
        "personnel_reused_name_count": ((result.get("bid_document_review") or {}).get("summary") or {}).get("reused_name_count"),
    }
    return clean_json(result)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the local full business review pipeline on OCR JSON files.")
    parser.add_argument("--dataset", required=True, help="Directory containing OCR JSON files for one project.")
    parser.add_argument("--out-json", default="reports/full_business_pipeline_results.json", help="Output JSON path.")
    parser.add_argument("--out-html", default="reports/full_business_pipeline_summary.html", help="Output HTML path.")
    parser.add_argument("--project-identifier", default="", help="Optional project identifier used in reports.")
    parser.add_argument(
        "--unknown-role",
        choices=["business", "technical", "skip"],
        default="business",
        help="How to treat JSON files whose role cannot be inferred from the file name.",
    )
    parser.add_argument(
        "--max-pairs-per-type",
        type=int,
        default=0,
        help="Limit duplicate-check pairs per document type. 0 means no limit.",
    )
    return parser.parse_args()


def main() -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

    args = parse_args()
    result = run_pipeline(args)

    out_json = Path(args.out_json).expanduser().resolve()
    out_html = Path(args.out_html).expanduser().resolve()
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_html.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    out_html.write_text(render_html_report(result), encoding="utf-8")

    summary = result.get("summary") or {}
    print("全业务审查完成")
    print(f"JSON: {out_json}")
    print(f"HTML: {out_html}")
    print(f"步骤: {summary.get('step_status_counts')}")
    print(f"商务标查重可疑对: {summary.get('business_duplicate_suspicious_pair_count')}")
    print(f"技术标查重可疑对: {summary.get('technical_duplicate_suspicious_pair_count')}")
    print(f"错别字候选: {summary.get('typo_issue_count')}")
    print(f"一人多用姓名: {summary.get('personnel_reused_name_count')}")
    return 0 if (summary.get("step_status_counts") or {}).get("error", 0) == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
