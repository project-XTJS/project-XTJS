# -*- coding: utf-8 -*-
"""
将 xtjs_result 中的一条结果记录渲染为静态 HTML / CSV / Markdown 报告。

用途：
1. 避免在 PowerShell 内联脚本里因为控制台编码导致中文标题变成问号。
2. 给单条结果提供一个更便于人工复核的可视化页面。
"""

from __future__ import annotations

import argparse
import csv
import html
import json
import sys
from pathlib import Path
from typing import Any

import psycopg2


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.config.settings import settings


CHECK_ORDER = [
    "pricing_check",
    "integrity_check",
    "consistency_check",
    "itemized_pricing_check",
    "verification_check",
    "deviation_check",
]

STATUS_COLORS = {
    "pass": "#2e7d32",
    "fail": "#c62828",
    "unclear": "#ef6c00",
    "ok": "#1565c0",
    "correct": "#455a64",
    "failed": "#c62828",
    "error": "#c62828",
    "MISSING": "#6b7280",
}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="渲染 xtjs_result 静态报告")
    parser.add_argument("--result-id", type=int, required=True, help="xtjs_result.id")
    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="输出目录，例如 test_reports/project_93_static_review_clean",
    )
    return parser.parse_args()


def _connect():
    return psycopg2.connect(settings.DATABASE_URL)


def _fetch_result_row(result_id: int) -> tuple[int, str, dict[str, Any], Any, Any]:
    with _connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT id, project_identifier_id, result, create_time, update_time
                FROM public.xtjs_result
                WHERE id = %s
                """,
                (result_id,),
            )
            row = cursor.fetchone()
    if not row:
        raise ValueError(f"xtjs_result row not found: id={result_id}")
    return row


def _escape(value: Any) -> str:
    return html.escape(str(value if value is not None else ""))


def _badge(text: Any) -> str:
    label = str(text or "")
    color = STATUS_COLORS.get(label, "#455a64")
    return f"<span class='badge' style='background:{color};'>{_escape(label)}</span>"


def _project_ids_from_result(result: dict[str, Any]) -> list[str]:
    values: list[str] = []
    for item in result.values():
        if not isinstance(item, dict):
            continue
        project = item.get("project") or {}
        identifier = str(project.get("identifier_id") or "").strip()
        if identifier and identifier not in values:
            values.append(identifier)
    return values


def _summary_cards(result: dict[str, Any]) -> str:
    cards: list[str] = []
    for key in sorted(result.keys()):
        item = result.get(key) or {}
        if not isinstance(item, dict):
            continue
        summary = item.get("summary") or {}
        cards.append(
            "<div class='card'>"
            f"<div class='card-title'>{_escape(key)}</div>"
            f"<pre>{_escape(json.dumps(summary, ensure_ascii=False, indent=2))}</pre>"
            "</div>"
        )
    return "".join(cards)


def _build_observations(result: dict[str, Any], business_review: dict[str, Any]) -> list[str]:
    observations: list[str] = []

    summary = business_review.get("summary") or {}
    if summary:
        observations.append(
            "商务标形式审查共覆盖 "
            f"{summary.get('bidder_count', 0)} 家投标人，"
            f"总检查项 {summary.get('total_check_count', 0)}，"
            f"审查状态统计为 {summary.get('review_status_counts', {})}。"
        )

    bidders = business_review.get("bidders") or []
    if bidders:
        pricing_review = (((bidders[0].get("checks") or {}).get("pricing_check") or {}).get("review") or {})
        if pricing_review.get("status") == "fail":
            observations.append(f"报价检查失败：{pricing_review.get('summary')}")

    typo_summary = (result.get("typo_check") or {}).get("summary") or {}
    typo_issue_count = int(typo_summary.get("typo_issue_count") or 0)
    if typo_issue_count > 0:
        observations.append(
            f"错别字检查发现 {typo_issue_count} 条问题，当前结果显示至少 1 份文档存在疑似错别字。"
        )

    duplicate_summary = (result.get("business_bid_duplicate_check") or {}).get("summary") or {}
    pair_count = int(duplicate_summary.get("pair_count") or 0)
    if pair_count == 0:
        observations.append("商务标查重没有形成可比较的成对样本，因此当前没有重复对。")

    if bidders and not (((bidders[0].get("checks") or {}).get("deviation_check"))):
        observations.append("这条已落库结果中还没有 deviation_check，说明当时生成主报告时偏离表审查尚未接入或尚未重跑。")

    bid_document_review = result.get("bid_document_review") or {}
    bid_document_summary = bid_document_review.get("summary") or {}
    if int(bid_document_summary.get("skipped_document_count") or 0) > 0:
        observations.append("错别字/一人多用联合报告里有文档被跳过，通常意味着对应 OCR 内容缺失或不可用。")

    return observations


def _document_table_rows(documents: dict[str, Any]) -> str:
    rows: list[str] = []
    for role in ["tender", "business", "technical"]:
        meta = documents.get(role)
        if not meta:
            continue
        rows.append(
            "<tr>"
            f"<td>{_escape(role)}</td>"
            f"<td>{_escape(meta.get('file_name') or '')}</td>"
            f"<td>{_escape(meta.get('identifier_id') or '')}</td>"
            f"<td>{_escape(meta.get('page_count') or '')}</td>"
            f"<td>{_escape(meta.get('layout_section_count') or '')}</td>"
            f"<td>{_escape(meta.get('logical_table_count') or '')}</td>"
            "</tr>"
        )
    return "".join(rows)


def _check_rows(checks: dict[str, Any]) -> str:
    rows: list[str] = []
    for check_key in CHECK_ORDER:
        check = checks.get(check_key)
        if not isinstance(check, dict):
            rows.append(
                "<tr>"
                f"<td>{_escape(check_key)}</td>"
                f"<td>{_badge('MISSING')}</td>"
                "<td></td><td></td><td></td>"
                "</tr>"
            )
            continue

        review = check.get("review") or {}
        validation = check.get("validation") or {}
        execution = check.get("execution") or {}
        rows.append(
            "<tr>"
            f"<td>{_escape(check_key)}</td>"
            f"<td>{_badge(review.get('status') or '')}</td>"
            f"<td>{_badge(validation.get('status') or '')}</td>"
            f"<td>{_badge(execution.get('status') or '')}</td>"
            f"<td>{_escape(review.get('summary') or '')}</td>"
            "</tr>"
        )
    return "".join(rows)


def _bidder_panels(business_review: dict[str, Any]) -> str:
    panels: list[str] = []
    for bidder in business_review.get("bidders") or []:
        bidder_name = bidder.get("bidder_name") or bidder.get("name") or bidder.get("bidder") or "未命名投标人"
        documents = bidder.get("documents") or {}
        checks = bidder.get("checks") or {}
        panels.append(
            "<section class='panel'>"
            f"<h2>{_escape(bidder_name)}</h2>"
            "<h3>关联文档</h3>"
            "<table><thead><tr>"
            "<th>role</th><th>file_name</th><th>identifier_id</th>"
            "<th>page_count</th><th>layout_sections</th><th>logical_tables</th>"
            "</tr></thead><tbody>"
            f"{_document_table_rows(documents)}"
            "</tbody></table>"
            "<h3>检查矩阵</h3>"
            "<table><thead><tr>"
            "<th>check</th><th>review</th><th>validation</th><th>execution</th><th>summary</th>"
            "</tr></thead><tbody>"
            f"{_check_rows(checks)}"
            "</tbody></table>"
            "</section>"
        )
    return "".join(panels)


def _write_csv(
    csv_path: Path,
    row_id: int,
    result_project_identifier_id: str,
    embedded_project_identifiers: list[str],
    business_review: dict[str, Any],
) -> None:
    with csv_path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "row_id",
                "result_project_identifier_id",
                "embedded_project_identifier",
                "bidder_name",
                "check_key",
                "review_status",
                "review_summary",
                "validation_status",
                "execution_status",
            ]
        )
        for bidder in business_review.get("bidders") or []:
            bidder_name = bidder.get("bidder_name") or bidder.get("name") or bidder.get("bidder") or ""
            checks = bidder.get("checks") or {}
            for check_key in CHECK_ORDER:
                check = checks.get(check_key) or {}
                review = check.get("review") or {}
                validation = check.get("validation") or {}
                execution = check.get("execution") or {}
                writer.writerow(
                    [
                        row_id,
                        result_project_identifier_id,
                        ", ".join(embedded_project_identifiers),
                        bidder_name,
                        check_key,
                        review.get("status", "MISSING") if check else "MISSING",
                        review.get("summary", "") if check else "",
                        validation.get("status", "") if check else "",
                        execution.get("status", "") if check else "",
                    ]
                )


def _render_html(
    *,
    row_id: int,
    result_project_identifier_id: str,
    embedded_project_identifiers: list[str],
    result: dict[str, Any],
    create_time: Any,
    update_time: Any,
) -> str:
    business_review = result.get("business_bid_format_review") or {}
    summary = business_review.get("summary") or {}
    observations = _build_observations(result, business_review)

    mismatch_warning = ""
    if embedded_project_identifiers and result_project_identifier_id not in embedded_project_identifiers:
        mismatch_warning = (
            "<div class='warning'>"
            "<strong>项目标识不一致：</strong>"
            f"xtjs_result.project_identifier_id = <code>{_escape(result_project_identifier_id)}</code>，"
            "但子结果内嵌的 project.identifier_id = "
            f"<code>{_escape(', '.join(embedded_project_identifiers))}</code>。"
            "这通常意味着写库口径、结果归档或展示映射存在异常，建议优先复核。"
            "</div>"
        )

    observations_html = "".join(f"<li>{_escape(item)}</li>" for item in observations)

    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>xtjs_result 静态分析页面</title>
  <style>
    body {{ font-family: "Microsoft YaHei", "PingFang SC", Arial, sans-serif; margin: 0; background: #f4f7fb; color: #1f2937; }}
    .wrap {{ max-width: 1280px; margin: 0 auto; padding: 24px; }}
    .hero {{ background: linear-gradient(135deg, #0f172a, #1d4ed8); color: white; padding: 24px 28px; border-radius: 18px; box-shadow: 0 18px 40px rgba(29,78,216,.18); }}
    .hero h1 {{ margin: 0 0 10px; font-size: 28px; }}
    .hero p {{ margin: 6px 0; opacity: .94; }}
    .stats {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 14px; margin-top: 18px; }}
    .stat {{ background: rgba(255,255,255,.12); border: 1px solid rgba(255,255,255,.15); border-radius: 14px; padding: 14px; }}
    .stat-label {{ font-size: 13px; opacity: .85; }}
    .stat-value {{ margin-top: 6px; font-size: 24px; font-weight: 800; }}
    .warning {{ margin-top: 18px; padding: 14px 16px; background: #fff7ed; color: #9a3412; border: 1px solid #fdba74; border-radius: 12px; }}
    .panel {{ margin-top: 22px; background: white; border-radius: 16px; padding: 20px; box-shadow: 0 10px 28px rgba(15,23,42,.06); }}
    .panel h2 {{ margin: 0 0 14px; font-size: 22px; }}
    .panel h3 {{ margin: 18px 0 8px; font-size: 16px; color: #334155; }}
    .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr)); gap: 16px; }}
    .card {{ background: white; border-radius: 16px; padding: 18px; box-shadow: 0 10px 28px rgba(15,23,42,.06); }}
    .card-title {{ font-weight: 700; margin-bottom: 10px; }}
    table {{ width: 100%; border-collapse: collapse; margin-top: 12px; }}
    th, td {{ border-bottom: 1px solid #e5e7eb; text-align: left; vertical-align: top; padding: 10px 12px; font-size: 14px; }}
    th {{ background: #f8fafc; color: #334155; }}
    .badge {{ display:inline-block; padding: 4px 8px; border-radius: 999px; color: white; font-size: 12px; font-weight: 700; }}
    pre {{ margin: 0; white-space: pre-wrap; word-break: break-word; font-size: 12px; color: #334155; }}
    code {{ background: #eef2ff; padding: 2px 6px; border-radius: 6px; }}
    ul {{ margin: 10px 0 0 18px; }}
    .meta {{ color: #475569; }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="hero">
      <h1>xtjs_result 静态分析页面</h1>
      <p>结果行 ID：{_escape(row_id)}</p>
      <p>xtjs_result.project_identifier_id：<code>{_escape(result_project_identifier_id)}</code></p>
      <p>内嵌 project.identifier_id：<code>{_escape(', '.join(embedded_project_identifiers) if embedded_project_identifiers else '未提供')}</code></p>
      <p>create_time：{_escape(create_time)} | update_time：{_escape(update_time)}</p>
      <div class="stats">
        <div class="stat"><div class="stat-label">顶层结果键数量</div><div class="stat-value">{len(result.keys())}</div></div>
        <div class="stat"><div class="stat-label">商务标投标人数量</div><div class="stat-value">{summary.get('bidder_count', 0)}</div></div>
        <div class="stat"><div class="stat-label">商务标总检查项</div><div class="stat-value">{summary.get('total_check_count', 0)}</div></div>
        <div class="stat"><div class="stat-label">错别字问题数</div><div class="stat-value">{(result.get('typo_check') or {}).get('summary', {}).get('typo_issue_count', 0)}</div></div>
      </div>
      {mismatch_warning}
    </div>

    <section class="panel">
      <h2>重点观察</h2>
      <ul>{observations_html}</ul>
    </section>

    <section class="panel">
      <h2>顶层结果摘要</h2>
      <div class="grid">{_summary_cards(result)}</div>
    </section>

    {_bidder_panels(business_review)}
  </div>
</body>
</html>
"""


def _write_notes(
    notes_path: Path,
    *,
    row_id: int,
    result_project_identifier_id: str,
    embedded_project_identifiers: list[str],
    observations: list[str],
) -> None:
    lines = [
        "# xtjs_result 静态复核说明",
        "",
        f"- xtjs_result.id: {row_id}",
        f"- xtjs_result.project_identifier_id: {result_project_identifier_id}",
        f"- 子结果中 project.identifier_id: {', '.join(embedded_project_identifiers) if embedded_project_identifiers else '未提供'}",
        "",
        "## 重点观察",
    ]
    lines.extend(f"- {item}" for item in observations)
    notes_path.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    args = _parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    row_id, result_project_identifier_id, result, create_time, update_time = _fetch_result_row(args.result_id)
    embedded_project_identifiers = _project_ids_from_result(result)
    business_review = result.get("business_bid_format_review") or {}
    observations = _build_observations(result, business_review)

    html_path = args.output_dir / f"result_{row_id}_summary.html"
    csv_path = args.output_dir / f"result_{row_id}_checks_matrix.csv"
    notes_path = args.output_dir / f"result_{row_id}_notes.md"

    html_path.write_text(
        _render_html(
            row_id=row_id,
            result_project_identifier_id=result_project_identifier_id,
            embedded_project_identifiers=embedded_project_identifiers,
            result=result,
            create_time=create_time,
            update_time=update_time,
        ),
        encoding="utf-8",
    )
    _write_csv(csv_path, row_id, result_project_identifier_id, embedded_project_identifiers, business_review)
    _write_notes(
        notes_path,
        row_id=row_id,
        result_project_identifier_id=result_project_identifier_id,
        embedded_project_identifiers=embedded_project_identifiers,
        observations=observations,
    )

    print(html_path)
    print(csv_path)
    print(notes_path)


if __name__ == "__main__":
    main()
