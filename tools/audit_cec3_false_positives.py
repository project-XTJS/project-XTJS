"""List false auto-accepted CEC3 word edits from a grouped public test set."""

from pathlib import Path
import argparse
import json
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tools.evaluate_duplicate_typo_candidates import load_records, request_result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("dataset", type=Path)
    parser.add_argument("--split", default="test")
    parser.add_argument("--service-url", required=True)
    args = parser.parse_args()
    for record in load_records(args.dataset):
        if record["split"] != args.split:
            continue
        expected = {
            (item["start"], item["end"], item["replacement"])
            for item in record["expected"]
        }
        result = request_result(record["text"], service_url=args.service_url, timeout=180)
        for issue in result["issues"]:
            edit = (issue["start"], issue["end"], issue["replacement"])
            if edit in expected:
                continue
            print(json.dumps({
                "project_id": record["project_id"],
                "source": record.get("source"),
                "text": record["text"],
                "target": record.get("target"),
                "expected": record["expected"],
                "predicted": {
                    key: issue.get(key)
                    for key in ("start", "end", "original_word", "replacement_word", "original", "replacement")
                },
            }, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
