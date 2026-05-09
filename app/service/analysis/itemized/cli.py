# itemized/cli.py
"""
分项报价 - 本地调试与命令行入口
"""

import argparse
import json
import re
import sys
from pathlib import Path

from app.service.analysis.itemized import ItemizedPricingChecker
from app.service.analysis.itemized.document_parser import _extract_text_from_payload


def _service_style_preprocess(text: str) -> str:
    """模拟服务层把多余空白压缩后的输入形态。"""
    return re.sub(r"\s+", " ", text.strip()) if text else ""


def _load_input_for_local_test(file_path: Path) -> object:
    """为本地调试读取文本或 JSON 文件，并自动解析 JSON。"""
    try:
        text = file_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise RuntimeError(f"读取文件失败: {exc}") from exc

    if file_path.suffix.lower() != ".json":
        return text

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text


def _collect_missing_amount_lines(checker: ItemizedPricingChecker, payload: object) -> list[str]:
    """收集疑似因 OCR 拆行而缺失金额的分项文本，便于本地排查。"""
    document = checker._prepare_document(payload)
    item_sections = document["item_sections"]
    candidate_sections = item_sections or document["total_sections"]

    missing_lines = []
    for section in candidate_sections:
        _, _, _, unresolved_rows = checker._extract_section_entries(section["lines"])
        for row in unresolved_rows:
            label = row.get("text") or row.get("label")
            if label:
                missing_lines.append(label)

    deduped = []
    seen = set()
    for line in missing_lines:
        key = re.sub(r"\s+", " ", line).strip()
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(key)
    return deduped


def _print_local_test_report(
    path: Path,
    checker: ItemizedPricingChecker,
    *,
    simulate_service: bool,
    tender_path: Path | None = None,
) -> int:
    """打印本地调试报告，便于离线验证抽取和校验效果。"""
    analysis_input = _load_input_for_local_test(path)
    analysis_text = (
        _service_style_preprocess(_extract_text_from_payload(analysis_input))
        if simulate_service
        else analysis_input
    )
    tender_text = None
    if tender_path is not None:
        loaded_tender_input = _load_input_for_local_test(tender_path)
        tender_text = (
            _service_style_preprocess(_extract_text_from_payload(loaded_tender_input))
            if simulate_service
            else loaded_tender_input
        )
    result = checker.check_itemized_logic(analysis_text, tender_text=tender_text)
    display_text = _extract_text_from_payload(analysis_text)

    print(f"\n=== {path} ===")
    if tender_path is not None:
        print(f"reference_tender: {tender_path}")
    print(f"text_length: {len(display_text)}")
    print(f"mode: {result.get('mode')}")
    print(f"status: {result.get('status')}")
    print(f"passed: {result.get('passed')}")
    print(f"summary: {result.get('summary')}")

    details = result.get("details") or []
    if details:
        print("details:")
        for detail in details:
            print(f"  - {detail}")

    checks = result.get("checks") or {}
    sum_check = checks.get("sum_consistency") or {}
    print("checks:")
    print(
        "  - sum_consistency: "
        f"{sum_check.get('status')} "
        f"(calc={sum_check.get('calculated_total')}, declared={sum_check.get('declared_total')}, diff={sum_check.get('difference')})"
    )
    print(
        "  - row_arithmetic: "
        f"{(checks.get('row_arithmetic') or {}).get('status')} "
        f"(issues={(checks.get('row_arithmetic') or {}).get('issue_count')})"
    )
    print(
        "  - duplicate_items: "
        f"{(checks.get('duplicate_items') or {}).get('status')} "
        f"(issues={(checks.get('duplicate_items') or {}).get('issue_count')})"
    )
    print(
        "  - missing_item: "
        f"{(checks.get('missing_item') or {}).get('status')} "
        f"(items={(checks.get('missing_item') or {}).get('missing_items')})"
    )

    evidence = result.get("evidence") or {}
    extracted_items = evidence.get("extracted_items") or []
    total_candidates = evidence.get("total_candidates") or []
    print(f"evidence: items={len(extracted_items)}, totals={len(total_candidates)}")
    for entry in extracted_items:
        print(f"  - item: {entry.get('label')} => {entry.get('amount')} ({entry.get('source')})")
    for entry in total_candidates:
        print(f"  - total: {entry.get('label')} => {entry.get('amount')} ({entry.get('source')})")

    missing_amount_lines = _collect_missing_amount_lines(checker, analysis_text)
    if missing_amount_lines:
        print("missing_amount_candidates:")
        for line in missing_amount_lines:
            print(f"  - {line}")

    return 0


def main(argv: list[str] | None = None) -> int:
    """命令行入口，支持单文件或招投标配对测试。"""
    parser = argparse.ArgumentParser(description="本地测试分项报价检查器。")
    parser.add_argument(
        "paths",
        nargs="*",
        default=["tender.json", "bid.json"],
        help="待测试的文本或 OCR JSON 文件路径。",
    )
    parser.add_argument(
        "--simulate-service",
        action="store_true",
        help="模拟 analysis_service 中压缩空白后的输入效果。",
    )
    parser.add_argument(
        "--bid",
        help="按业务模式指定待检查的投标文件路径。",
    )
    parser.add_argument(
        "--tender",
        help="在下浮率模式下指定对照用的招标文件路径。",
    )
    args = parser.parse_args(argv)

    checker = ItemizedPricingChecker()
    exit_code = 0
    if args.bid:
        bid_path = Path(args.bid).expanduser()
        if not bid_path.is_absolute():
            bid_path = Path.cwd() / bid_path
        tender_path = None
        if args.tender:
            tender_path = Path(args.tender).expanduser()
            if not tender_path.is_absolute():
                tender_path = Path.cwd() / tender_path
        try:
            _print_local_test_report(
                bid_path,
                checker,
                simulate_service=args.simulate_service,
                tender_path=tender_path,
            )
        except Exception as exc:  # pragma: no cover - local debug entrypoint
            print(f"\n=== {bid_path} ===")
            print(f"error: {exc}")
            return 1
        return 0

    for raw_path in args.paths:
        path = Path(raw_path).expanduser()
        if not path.is_absolute():
            path = Path.cwd() / path
        if not path.exists():
            print(f"\n=== {path} ===")
            print("error: 文件不存在。")
            exit_code = 1
            continue
        try:
            _print_local_test_report(path, checker, simulate_service=args.simulate_service)
        except Exception as exc:  # pragma: no cover - local debug entrypoint
            print(f"\n=== {path} ===")
            print(f"error: {exc}")
            exit_code = 1
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))