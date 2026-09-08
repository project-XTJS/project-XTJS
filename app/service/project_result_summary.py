"""Small persisted facts used by project cards, without copying report bodies."""

SUMMARY_VERSION = 1
HIDDEN_RESULT_KEYS = frozenset({
    "manual_review_results", "business_itemized_duplicate_check",
    "bid_response_duplicate_check", "typo_check",
})
MERGED_ALIASES = {
    "business_bid_duplicate_check": "business_bid_duplicate_clusters",
    "technical_bid_duplicate_check": "technical_bid_duplicate_clusters",
}


def is_result_key_visible(key: str) -> bool:
    return key not in HIDDEN_RESULT_KEYS


def _js_truthy(value) -> bool:
    # The existing browser status treats empty JSON objects/arrays as present.
    return isinstance(value, (dict, list)) or bool(value)


def build_project_result_summary(result: dict) -> dict:
    visible = dict(result or {})
    manual = visible.get("manual_review_results") or {}
    latest = (manual.get("latest") or {}) if isinstance(manual, dict) else {}
    if not isinstance(latest, dict):
        latest = {}
    visible.update(latest)
    # Match the display endpoint: manual raw overrides take precedence over
    # persisted merged duplicate results. No report bodies are deep-copied.
    for raw_key, merged_key in MERGED_ALIASES.items():
        merged = result.get(merged_key)
        if raw_key not in latest and isinstance(merged, dict) and merged:
            visible[raw_key] = merged
    excluded = {"duplicate_check", *MERGED_ALIASES.values()}
    visible = {key: value for key, value in visible.items()
               if key not in excluded and is_result_key_visible(key) and _js_truthy(value)}
    suspicious = False
    for value in visible.values():
        summary = value.get("summary") if isinstance(value, dict) else None
        try:
            suspicious |= float((summary or {}).get("suspicious") or 0) > 0
        except (ValueError, TypeError, AttributeError):
            pass
    return {"version": SUMMARY_VERSION, "result_count": len(visible),
            "has_suspicious": suspicious, "result_keys": sorted(visible)}
