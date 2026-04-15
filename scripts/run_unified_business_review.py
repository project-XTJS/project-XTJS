from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.service.analysis.unified_business_review import UnifiedBusinessReviewService


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run unified business review and persist result into xtjs_result.")
    parser.add_argument(
        "--dataset-dir",
        required=True,
        help="Directory containing one tender JSON and paired business/technical bid JSON files.",
    )
    parser.add_argument(
        "--project-id",
        default=None,
        help="Project identifier to create or reuse in xtjs_projects.",
    )
    parser.add_argument(
        "--result-key",
        default=UnifiedBusinessReviewService.DEFAULT_RESULT_KEY,
        help="JSON key stored under xtjs_result.result.",
    )
    parser.add_argument(
        "--pretty",
        action="store_true",
        help="Print a formatted JSON summary to stdout.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    service = UnifiedBusinessReviewService()
    persisted = service.persist_dataset_review(
        args.dataset_dir,
        project_identifier=args.project_id,
        result_key=args.result_key,
    )

    summary = {
        "project_identifier_id": persisted["project"]["identifier_id"],
        "result_key": persisted["result_key"],
        "summary": persisted["review"]["summary"],
        "function_validation": persisted["review"]["function_validation"],
    }

    if args.pretty:
        print(json.dumps(summary, ensure_ascii=False, indent=2))
    else:
        print(json.dumps(summary, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
