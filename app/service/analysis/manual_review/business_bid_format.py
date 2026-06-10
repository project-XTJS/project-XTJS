"""Manual editable values and rerun logic for business-bid format review."""

from __future__ import annotations

import hashlib
import re
from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Callable

from fastapi import HTTPException

from app.service.analysis.unified import UnifiedBusinessReviewService
from app.service.analysis.project_input_loader import ProjectAnalysisInputLoader
from app.service.manual_review.working_copy import (
    DocumentWorkingCopyService,
    MANUAL_EXTRACTIONS_KEY,
)
from app.service.postgresql_service import PostgreSQLService


BUSINESS_FORMAT_RESULT_KEY = UnifiedBusinessReviewService.BUSINESS_RESULT_KEY
BUSINESS_FORMAT_EDITABLE_GROUPS = {
    "consistency_check": {"template_segment", "template_skeleton_item"},
    "pricing_check": {"price_constraint", "opening_amount"},
    "itemized_pricing_check": {"itemized_amount", "itemized_total"},
    "verification_check": {"attachment_result"},
}
BUSINESS_SIGNATURE_OCR_EVIDENCE_MODES = {
    "",
    "text_inline",
    "ocr_signature_section",
    "ocr_signature_region",
    "ocr_signature_location",
    "ocr_signature_location_fallback",
    "nearby_text_mark",
    "personal_seal_as_alternative",
}


def _utc_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _result_path(parts: list[Any]) -> str:
    value = ""
    for part in parts:
        if isinstance(part, int):
            value += f"[{part}]"
        else:
            token = str(part)
            value += token if not value else f".{token}"
    return value


def _parse_result_path(path: str) -> list[Any]:
    parts: list[Any] = []
    token = ""
    i = 0
    while i < len(path):
        ch = path[i]
        if ch == ".":
            if token:
                parts.append(token)
                token = ""
            i += 1
            continue
        if ch == "[":
            if token:
                parts.append(token)
                token = ""
            end = path.find("]", i)
            if end < 0:
                raise ValueError(f"invalid result_path: {path}")
            index_text = path[i + 1:end].strip()
            if not index_text.isdigit():
                raise ValueError(f"invalid result_path index: {path}")
            parts.append(int(index_text))
            i = end + 1
            continue
        token += ch
        i += 1
    if token:
        parts.append(token)
    return parts


def _set_path_value(root: Any, path: str, value: Any) -> bool:
    parts = _parse_result_path(path)
    if not parts:
        return False
    node = root
    for part in parts[:-1]:
        if isinstance(part, int):
            if not isinstance(node, list) or part < 0 or part >= len(node):
                return False
            node = node[part]
        else:
            if not isinstance(node, dict) or part not in node:
                return False
            node = node[part]
    last = parts[-1]
    if isinstance(last, int):
        if not isinstance(node, list) or last < 0 or last >= len(node):
            return False
        node[last] = value
        return True
    if not isinstance(node, dict):
        return False
    node[last] = value
    return True


def _manual_value_present(item: dict[str, Any]) -> bool:
    return "manual_value" in item and item.get("manual_value") is not None


def _editable_id(*, result_path: str, bidder_key: str, check_code: str, field_group: str, field_name: str) -> str:
    source = "|".join([bidder_key, check_code, field_group, field_name, result_path])
    digest = hashlib.sha1(source.encode("utf-8")).hexdigest()[:16]
    return f"{BUSINESS_FORMAT_RESULT_KEY}:{digest}"


def _stable_skeleton_editable_id(
    *,
    document_identifier_id: str,
    item_id: str,
) -> str:
    source = f"__tender__|template_skeleton_item|{document_identifier_id}|{item_id}"
    digest = hashlib.sha1(source.encode("utf-8")).hexdigest()[:16]
    return f"{BUSINESS_FORMAT_RESULT_KEY}:{digest}"


def _stable_consistency_editable_id(
    *,
    document_identifier_id: str,
    field_name: str,
) -> str:
    source = f"consistency|template_segment|{document_identifier_id}|{field_name}"
    digest = hashlib.sha1(source.encode("utf-8")).hexdigest()[:16]
    return f"{BUSINESS_FORMAT_RESULT_KEY}:{digest}"


def _coerce_manual_page_refs(value: Any) -> list[int]:
    pages: list[int] = []
    seen: set[int] = set()

    def add(item: Any) -> None:
        if isinstance(item, bool) or item is None:
            return
        if isinstance(item, int):
            if item > 0 and item not in seen:
                seen.add(item)
                pages.append(item)
            return
        if isinstance(item, str) and item.strip().isdigit():
            add(int(item.strip()))
            return
        if isinstance(item, dict):
            for key in ("page", "start_page", "page_number"):
                add(item.get(key))
            for key in ("pages", "page_refs", "section_pages"):
                add(item.get(key))
            return
        if isinstance(item, (list, tuple, set)):
            for child in item:
                add(child)

    add(value)
    return pages


def _business_review_from_record(record: dict[str, Any] | None) -> dict[str, Any]:
    result_payload = dict((record or {}).get("result") or {})
    review = result_payload.get(BUSINESS_FORMAT_RESULT_KEY)
    if not isinstance(review, dict):
        raise HTTPException(status_code=404, detail="business bid format review result not found")
    return review


def _business_manual_payload_from_documents(payload_data: dict[str, Any] | None) -> dict[str, Any]:
    merged: dict[str, dict[str, Any]] = {}
    updated_at = ""
    for document in (payload_data or {}).get("documents") or []:
        if not isinstance(document, dict):
            continue
        for source in (
            document.get("content"),
            document.get("tender_content"),
            ((document.get("review_content") or {}).get("effective_content") if isinstance(document.get("review_content"), dict) else None),
            ((document.get("tender_review_content") or {}).get("effective_content") if isinstance(document.get("tender_review_content"), dict) else None),
        ):
            if not isinstance(source, dict):
                continue
            manual_extractions = source.get(MANUAL_EXTRACTIONS_KEY) or {}
            if not isinstance(manual_extractions, dict):
                continue
            payload = manual_extractions.get(BUSINESS_FORMAT_RESULT_KEY) or {}
            if not isinstance(payload, dict):
                continue
            updated_at = str(payload.get("updated_at") or updated_at)
            for item in payload.get("items") or []:
                if not isinstance(item, dict):
                    continue
                editable_id = str(item.get("editable_id") or "").strip()
                if editable_id:
                    merged[editable_id] = dict(item)
        review_content = document.get("review_content") or {}
        if not isinstance(review_content, dict):
            continue
        inputs = review_content.get("inputs") or {}
        if not isinstance(inputs, dict):
            continue
        payload = inputs.get(BUSINESS_FORMAT_RESULT_KEY) or {}
        if not isinstance(payload, dict):
            continue
        updated_at = str(payload.get("updated_at") or updated_at)
        for item in payload.get("items") or []:
            if not isinstance(item, dict):
                continue
            editable_id = str(item.get("editable_id") or "").strip()
            if editable_id:
                merged[editable_id] = dict(item)
    return {
        "schema_version": "1.0",
        "result_key": BUSINESS_FORMAT_RESULT_KEY,
        "updated_at": updated_at or _utc_timestamp(),
        "items": list(merged.values()),
    }


def _business_manual_payload_for_project(
    *,
    identifier_id: str,
    db_service: PostgreSQLService,
) -> dict[str, Any]:
    payload_data = ProjectAnalysisInputLoader(db_service).load(identifier_id)
    return _business_manual_payload_from_documents(payload_data)


def _manual_items_by_id(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    items = payload.get("items") if isinstance(payload, dict) else []
    result: dict[str, dict[str, Any]] = {}
    for item in items or []:
        if not isinstance(item, dict):
            continue
        editable_id = str(item.get("editable_id") or "").strip()
        if editable_id:
            result[editable_id] = dict(item)
    return result


def _business_bidder_document(review: dict[str, Any], bidder: dict[str, Any], bidder_index: int) -> dict[str, Any]:
    documents = bidder.get("documents") or {}
    business_doc = documents.get("business") or documents.get("business_bid") or {}
    if isinstance(business_doc, dict) and business_doc:
        return business_doc
    dataset_bidders = ((review.get("dataset") or {}).get("bidders") or [])
    if bidder_index < len(dataset_bidders):
        dataset_doc = (dataset_bidders[bidder_index] or {}).get("business") or {}
        if isinstance(dataset_doc, dict):
            return dataset_doc
    return {}


def _business_tender_document(review: dict[str, Any]) -> dict[str, Any]:
    tender_table = (review.get("extraction_tables") or {}).get("tender_table") or {}
    document = tender_table.get("document") or {}
    if isinstance(document, dict) and document:
        return document
    dataset_tender = ((review.get("dataset") or {}).get("tender") or {})
    if isinstance(dataset_tender, dict) and dataset_tender:
        return dataset_tender
    tender_doc = ((review.get("documents") or {}).get("tender") or {})
    return tender_doc if isinstance(tender_doc, dict) else {}


def _business_verification_raw_result(review: dict[str, Any], bidder_index: int) -> dict[str, Any]:
    bidders = [item for item in (review.get("bidders") or []) if isinstance(item, dict)]
    if bidder_index >= len(bidders):
        return {}
    raw_result = (((bidders[bidder_index].get("checks") or {}).get("verification_check") or {}).get("raw_result") or {})
    return raw_result if isinstance(raw_result, dict) else {}


def _business_attachment_lookup_key(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    return re.sub(r"\s+", "", text).lower()


def _business_verification_attachment_lookup(review: dict[str, Any], bidder_index: int) -> dict[str, dict[str, Any]]:
    lookup: dict[str, dict[str, Any]] = {}
    for attachment in _business_verification_raw_result(review, bidder_index).get("attachment_results") or []:
        if not isinstance(attachment, dict):
            continue
        for value in (
            attachment.get("attachment_number"),
            attachment.get("title"),
            attachment.get("matched_bid_title"),
        ):
            key = _business_attachment_lookup_key(value)
            if key and key not in lookup:
                lookup[key] = attachment
    return lookup


def _match_business_verification_attachment(
    row: dict[str, Any],
    original_value: Any,
    lookup: dict[str, dict[str, Any]],
) -> dict[str, Any] | None:
    candidates: list[Any] = [row.get("field_name")]
    if isinstance(original_value, dict):
        candidates.extend(
            [
                original_value.get("attachment_number"),
                original_value.get("matched_bid_title"),
                original_value.get("title"),
            ]
        )
    for candidate in candidates:
        key = _business_attachment_lookup_key(candidate)
        if key and lookup.get(key):
            return lookup[key]
    return None


BUSINESS_SIGNATURE_NON_CONTENT_VALUES = {
    "",
    "已签字",
    "已盖章",
    "已签章",
    "通过",
    "pass",
    "found",
    "ok",
    "true",
}


def _compact_business_signature_text(text: Any) -> str:
    return re.sub(r"[^0-9A-Za-z\u4e00-\u9fa5]", "", str(text or ""))


def _business_signature_content_value(value: Any) -> str:
    text = str(value or "").strip()
    compact = _compact_business_signature_text(text).lower()
    if not text or compact in {_compact_business_signature_text(item).lower() for item in BUSINESS_SIGNATURE_NON_CONTENT_VALUES}:
        return ""
    return text


def _business_signature_evidence_candidates(value: dict[str, Any], mode: str) -> list[Any]:
    if mode == "personal_seal_as_alternative":
        return [value.get("value"), value.get("seal_text")]
    if mode == "nearby_text_mark":
        return [value.get("value"), value.get("evidence_text")]
    return [value.get("signature_text"), value.get("value")]


def _business_attachment_signature_evidence_texts(attachment: dict[str, Any]) -> list[str]:
    signature_check = attachment.get("signature_check") or {}
    if not isinstance(signature_check, dict):
        return []

    texts: list[str] = []
    seen: set[str] = set()
    for value in signature_check.get("filled_values") or []:
        if not isinstance(value, dict):
            continue
        mode = str(value.get("mode") or "").strip()
        if mode not in BUSINESS_SIGNATURE_OCR_EVIDENCE_MODES:
            continue
        evidence_text = ""
        for candidate in _business_signature_evidence_candidates(value, mode):
            evidence_text = _business_signature_content_value(candidate)
            if evidence_text:
                break
        if not evidence_text:
            continue
        key = _compact_business_signature_text(evidence_text)
        if key not in seen:
            seen.add(key)
            texts.append(evidence_text)
    return texts


def _compact_business_seal_text(text: Any) -> str:
    return re.sub(r"[^0-9A-Za-z\u4e00-\u9fa5]", "", str(text or ""))


def _looks_like_company_business_seal_text(text: Any) -> bool:
    compact = _compact_business_seal_text(text)
    return bool(re.search(r"(有限责任公司|股份有限公司|集团有限公司|有限公司|公司|专用章)$", compact) or "公司" in compact)


def _looks_like_person_business_seal_text(text: Any) -> bool:
    compact = _compact_business_seal_text(text)
    return bool(re.fullmatch(r"[\u4e00-\u9fa5]{2,4}|[A-Za-z]{2,20}", compact))


def _official_business_seal_texts(values: Any) -> list[str]:
    texts = list(dict.fromkeys(str(value or "").strip() for value in (values or []) if str(value or "").strip()))
    company_texts = [text for text in texts if _looks_like_company_business_seal_text(text)]
    if company_texts:
        return company_texts
    return [text for text in texts if not _looks_like_person_business_seal_text(text)]


def _business_attachment_seal_evidence_texts(attachment: dict[str, Any]) -> list[str]:
    seal_check = attachment.get("seal_check") or {}
    if not isinstance(seal_check, dict):
        return []

    texts: list[str] = []
    seen: set[str] = set()

    def add(text: Any, page: Any = None) -> None:
        value = str(text or "").strip()
        if not value:
            return
        key = _compact_business_seal_text(value)
        if key in seen:
            return
        seen.add(key)
        label = f"{value}（P{page}）" if page else value
        texts.append(label)

    for seal_text in _official_business_seal_texts(seal_check.get("seal_texts") or []):
        add(seal_text)
    best_match = seal_check.get("best_match") or {}
    if isinstance(best_match, dict) and str(best_match.get("mode") or "").strip() != "textual_seal_line":
        for seal_text in _official_business_seal_texts([best_match.get("seal_text")]):
            add(seal_text, best_match.get("page"))
    return texts[:10]


def _enrich_business_attachment_value(original_value: Any, attachment: dict[str, Any] | None) -> Any:
    if not isinstance(original_value, dict) or not isinstance(attachment, dict):
        return original_value
    value = dict(original_value)
    signature_evidence = _business_attachment_signature_evidence_texts(attachment)
    signature_check = attachment.get("signature_check") or {}
    signature_status = str(signature_check.get("status") or value.get("signature_status") or "").strip().lower() if isinstance(signature_check, dict) else str(value.get("signature_status") or "").strip().lower()
    if signature_evidence and not value.get("signature_evidence") and not value.get("signature_texts"):
        value["signature_evidence"] = signature_evidence[:10]
    if not value.get("signature_evidence") and not value.get("signature_texts") and signature_status in {"pass", "found", "pending"}:
        value["signature_parse_status"] = "unparsed"
    elif signature_evidence and not value.get("signature_parse_status"):
        value["signature_parse_status"] = "parsed"
    seal_evidence = _business_attachment_seal_evidence_texts(attachment)
    if seal_evidence and not value.get("seal_texts") and not value.get("seal_evidence"):
        value["seal_texts"] = seal_evidence[:10]
        value["seal_evidence"] = seal_evidence[:10]
    date_check = attachment.get("date_check") or {}
    if isinstance(date_check, dict):
        if not value.get("date_text"):
            date_text = date_check.get("matched_sign_text") or date_check.get("sign_date")
            if date_text:
                value["date_text"] = date_text
        if not value.get("deadline_date") and date_check.get("deadline_date"):
            value["deadline_date"] = date_check.get("deadline_date")
        if not value.get("deadline_text") and date_check.get("matched_deadline_text"):
            value["deadline_text"] = date_check.get("matched_deadline_text")
        if not value.get("deadline_page") and date_check.get("matched_deadline_page"):
            value["deadline_page"] = date_check.get("matched_deadline_page")
        if not value.get("deadline_locations") and date_check.get("deadline_locations"):
            value["deadline_locations"] = date_check.get("deadline_locations") or []
    return value


def _first_present_value(*values: Any) -> Any:
    for value in values:
        if value is not None and value != "":
            return value
    return None


def _compact_price_constraint_value(value: Any) -> Any:
    """Keep only the tender highest limit amount for manual review display."""
    if not isinstance(value, dict):
        return value
    amount = _first_present_value(
        value.get("amount_yuan"),
        value.get("limit_amount_yuan"),
        value.get("amount"),
    )
    return {"amount_yuan": amount} if amount is not None else None


def _status_from_amount_pair(small_amount: Any, capital_amount: Any) -> str | None:
    small = _decimal_from_manual(small_amount)
    capital = _decimal_from_manual(capital_amount)
    if small is None or capital is None:
        return "missing" if small is not None or capital is not None else None
    return "pass" if abs(small - capital) <= Decimal("0.01") else "fail"


def _limit_status_from_amounts(opening_amount: Any, tender_limit: Any) -> str | None:
    opening = _decimal_from_manual(opening_amount)
    limit = _decimal_from_manual(tender_limit)
    if opening is None or limit is None:
        return None
    return "pass" if opening <= limit + Decimal("0.01") else "fail"


def _compact_opening_amount_value(value: Any, *, tender_limit_value: Any = None) -> Any:
    """Keep opening quote amounts and the two pricing judgments only."""
    if not isinstance(value, dict):
        return value

    price_pairs = value.get("price_pairs") if isinstance(value.get("price_pairs"), list) else []
    first_pair = next((item for item in price_pairs if isinstance(item, dict)), {})
    small_amount = _first_present_value(
        value.get("small_amount_yuan"),
        value.get("small_amount"),
        first_pair.get("small_amount_yuan"),
        first_pair.get("small_amount"),
        value.get("amount_yuan"),
        value.get("amount"),
    )
    capital_amount = _first_present_value(
        value.get("capital_amount_yuan"),
        value.get("capital_amount"),
        first_pair.get("capital_amount_yuan"),
        first_pair.get("capital_amount"),
    )
    case_status = _first_present_value(
        value.get("case_consistency_status"),
        first_pair.get("case_consistency_status"),
        _status_from_amount_pair(small_amount, capital_amount),
    )
    limit_status = _limit_status_from_amounts(small_amount, tender_limit_value)

    compact: dict[str, Any] = {}
    if small_amount is not None:
        compact["small_amount_yuan"] = small_amount
    if capital_amount is not None:
        compact["capital_amount_yuan"] = capital_amount
    if case_status is not None:
        compact["case_consistency_status"] = case_status
    if limit_status is not None:
        compact["limit_comparison_status"] = limit_status
    return compact or None


def _make_business_editable_item(
    *,
    manual_by_id: dict[str, dict[str, Any]],
    result_path: str,
    bidder_key: str,
    bidder_name: str | None,
    check_code: str,
    field_group: str,
    field_name: str,
    original_value: Any,
    page_refs: list[int] | None = None,
    document: dict[str, Any] | None = None,
    stable_key: str | None = None,
) -> dict[str, Any]:
    document = document or {}
    editable_id = stable_key or _editable_id(
        result_path=result_path,
        bidder_key=bidder_key,
        check_code=check_code,
        field_group=field_group,
        field_name=field_name,
    )
    saved = manual_by_id.get(editable_id) or {}
    has_manual = _manual_value_present(saved)
    return {
        "editable_id": editable_id,
        "result_key": BUSINESS_FORMAT_RESULT_KEY,
        "result_path": result_path,
        "bidder_key": bidder_key,
        "bidder_name": bidder_name,
        "check_code": check_code,
        "field_group": field_group,
        "field_name": field_name,
        "original_value": original_value,
        "manual_value": saved.get("manual_value"),
        "has_manual_value": has_manual,
        "effective_value": saved.get("manual_value") if has_manual else original_value,
        "page_refs": list(page_refs or []),
        "document_identifier_id": (
            document.get("identifier_id")
            or document.get("document_identifier_id")
            or document.get("document_id")
        ),
        "file_name": document.get("file_name") or document.get("document_name"),
        "updated_at": saved.get("updated_at"),
    }


def _build_business_format_editable_items(
    review: dict[str, Any],
    manual_payload: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    manual_by_id = _manual_items_by_id(manual_payload or {})
    items: list[dict[str, Any]] = []
    bidders = [item for item in (review.get("bidders") or []) if isinstance(item, dict)]
    bidder_tables = [
        item for item in (((review.get("extraction_tables") or {}).get("bidder_tables") or []))
        if isinstance(item, dict)
    ]
    tender_table = ((review.get("extraction_tables") or {}).get("tender_table") or {})
    tender_rows = [row for row in (tender_table.get("rows") or []) if isinstance(row, dict)]
    pricing_tender_rows = [
        row
        for row in tender_rows
        if str(row.get("check_code") or "") == "pricing_check"
        and str(row.get("field_group") or "") == "price_constraint"
    ]
    primary_tender_limit_value = (
        _compact_price_constraint_value(pricing_tender_rows[0].get("value"))
        if pricing_tender_rows
        else None
    )
    tender_document = _business_tender_document(review)

    for tender_row_index, tender_row in enumerate(tender_rows):
        if str(tender_row.get("check_code") or "") != "consistency_check":
            continue
        if str(tender_row.get("field_group") or "") != "template_skeleton_item":
            continue
        original_value = tender_row.get("value")
        if not isinstance(original_value, dict):
            continue
        item_id = str(original_value.get("item_id") or "").strip()
        document_identifier_id = str(
            tender_document.get("identifier_id")
            or tender_document.get("document_identifier_id")
            or tender_document.get("document_id")
            or ""
        )
        path = _result_path(
            ["extraction_tables", "tender_table", "rows", tender_row_index, "value"]
        )
        items.append(
            _make_business_editable_item(
                manual_by_id=manual_by_id,
                result_path=path,
                bidder_key="__tender__",
                bidder_name=None,
                check_code="consistency_check",
                field_group="template_skeleton_item",
                field_name=str(original_value.get("label") or item_id),
                original_value=original_value,
                page_refs=_coerce_manual_page_refs(
                    tender_row.get("page_refs")
                    or tender_row.get("locations")
                    or tender_row
                ),
                document=tender_document,
                stable_key=_stable_skeleton_editable_id(
                    document_identifier_id=document_identifier_id,
                    item_id=item_id,
                ),
            )
        )

    for bidder_index, bidder in enumerate(bidders):
        bidder_key = str(bidder.get("bidder_key") or f"bidder_{bidder_index + 1}")
        bidder_name = str(bidder.get("bidder_name") or "").strip() or None
        document = _business_bidder_document(review, bidder, bidder_index)
        checks = bidder.get("checks") or {}

        for tender_row in pricing_tender_rows:
            tender_row_index = tender_rows.index(tender_row)
            path = _result_path(["extraction_tables", "tender_table", "rows", tender_row_index, "value"])
            items.append(
                _make_business_editable_item(
                    manual_by_id=manual_by_id,
                    result_path=path,
                    bidder_key=bidder_key,
                    bidder_name=bidder_name,
                    check_code="pricing_check",
                    field_group="price_constraint",
                    field_name=str(tender_row.get("field_name") or "tender_limit_or_budget"),
                    original_value=_compact_price_constraint_value(tender_row.get("value")),
                    page_refs=_coerce_manual_page_refs(tender_row.get("page_refs") or tender_row.get("locations") or tender_row),
                    document=tender_document,
                )
            )

        pricing_self_check = (((checks.get("pricing_check") or {}).get("raw_result") or {}).get("self_check") or {})
        if isinstance(pricing_self_check, dict) and pricing_self_check:
            path = _result_path(["bidders", bidder_index, "checks", "pricing_check", "raw_result", "self_check"])
            items.append(
                _make_business_editable_item(
                    manual_by_id=manual_by_id,
                    result_path=path,
                    bidder_key=bidder_key,
                    bidder_name=bidder_name,
                    check_code="pricing_check",
                    field_group="opening_amount",
                    field_name="opening_amount",
                    original_value=_compact_opening_amount_value(
                        pricing_self_check,
                        tender_limit_value=primary_tender_limit_value,
                    ),
                    page_refs=_coerce_manual_page_refs(pricing_self_check),
                    document=document,
                )
            )

        if bidder_index >= len(bidder_tables):
            continue
        attachment_lookup = _business_verification_attachment_lookup(review, bidder_index)
        table_rows = [row for row in (bidder_tables[bidder_index].get("rows") or []) if isinstance(row, dict)]
        for row_index, row in enumerate(table_rows):
            check_code = str(row.get("check_code") or "")
            field_group = str(row.get("field_group") or "")
            if field_group not in BUSINESS_FORMAT_EDITABLE_GROUPS.get(check_code, set()):
                continue
            path = _result_path(["extraction_tables", "bidder_tables", bidder_index, "rows", row_index, "value"])
            original_value = row.get("value")
            if check_code == "verification_check" and field_group == "attachment_result":
                original_value = _enrich_business_attachment_value(
                    original_value,
                    _match_business_verification_attachment(row, original_value, attachment_lookup),
                )
            items.append(
                _make_business_editable_item(
                    manual_by_id=manual_by_id,
                    result_path=path,
                    bidder_key=bidder_key,
                    bidder_name=bidder_name,
                    check_code=check_code,
                    field_group=field_group,
                    field_name=str(row.get("field_name") or field_group),
                    original_value=original_value,
                    page_refs=_coerce_manual_page_refs(row.get("page_refs") or row.get("locations") or row),
                    document=document,
                    stable_key=(
                        _stable_consistency_editable_id(
                            document_identifier_id=str(
                                document.get("identifier_id")
                                or document.get("document_identifier_id")
                                or document.get("document_id")
                                or ""
                            ),
                            field_name=str(row.get("field_name") or field_group),
                        )
                        if check_code == "consistency_check"
                        and field_group == "template_segment"
                        else None
                    ),
                )
            )
    return items


def _normalize_business_manual_items(
    raw_items: list[dict[str, Any]],
    *,
    existing_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    now = _utc_timestamp()
    merged = _manual_items_by_id(existing_payload or {})
    for raw in raw_items or []:
        if not isinstance(raw, dict):
            continue
        editable_id = str(raw.get("editable_id") or "").strip()
        if not editable_id:
            continue
        if raw.get("manual_value") is None:
            merged.pop(editable_id, None)
            continue
        item = dict(raw)
        item["updated_at"] = str(item.get("updated_at") or now)
        merged[editable_id] = item
    return {
        "schema_version": "1.0",
        "result_key": BUSINESS_FORMAT_RESULT_KEY,
        "updated_at": now,
        "items": list(merged.values()),
    }


def _decimal_from_manual(value: Any) -> Decimal | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float, Decimal)):
        try:
            return Decimal(str(value))
        except (InvalidOperation, ValueError):
            return None
    if isinstance(value, str):
        text = value.strip().replace(",", "")
        match = re.search(r"-?\d+(?:\.\d+)?", text)
        if not match:
            return None
        try:
            return Decimal(match.group(0))
        except (InvalidOperation, ValueError):
            return None
    if isinstance(value, dict):
        for key in (
            "amount_yuan",
            "amount",
            "manual_amount",
            "small_amount",
            "small_amount_yuan",
            "capital_amount",
            "capital_amount_yuan",
            "declared_total",
            "limit_amount_yuan",
            "bid_amount_yuan",
        ):
            amount = _decimal_from_manual(value.get(key))
            if amount is not None:
                return amount
    return None


def _amount_pair_from_value(value: Any) -> tuple[Decimal | None, Decimal | None]:
    if isinstance(value, dict):
        small = (
            _decimal_from_manual(value.get("small_amount"))
            or _decimal_from_manual(value.get("small_amount_yuan"))
            or _decimal_from_manual(value.get("small_price"))
            or _decimal_from_manual(value.get("amount_yuan"))
            or _decimal_from_manual(value.get("amount"))
        )
        capital = (
            _decimal_from_manual(value.get("capital_amount"))
            or _decimal_from_manual(value.get("capital_amount_yuan"))
            or _decimal_from_manual(value.get("capital_price"))
        )
        return small, capital
    amount = _decimal_from_manual(value)
    return amount, None


def _manual_issue(status: str, title: str, message: str, items: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "status": status,
        "title": title,
        "message": message,
        "severity": "error" if status == "fail" else ("warning" if status in {"missing", "unclear"} else "info"),
        "evidence": {
            "manual_review": True,
            "editable_ids": [item.get("editable_id") for item in items],
            "items": items[:20],
        },
    }


def _set_manual_check_summary(check: dict[str, Any], status: str, summary: str, items: list[dict[str, Any]]) -> None:
    issue = _manual_issue(status, "Manual corrected recognition", summary, items)
    check["manual_review"] = {
        "applied": True,
        "item_count": len(items),
        "updated_at": _utc_timestamp(),
        "editable_ids": [item.get("editable_id") for item in items],
    }
    check["validation"] = {"status": "correct", "reason": "Manual review inputs were applied."}
    check["review"] = {"status": status, "summary": summary}
    check["issues"] = {
        "passed": [issue] if status == "pass" else [],
        "failed": [issue] if status == "fail" else [],
        "missing": [issue] if status == "missing" else [],
        "unclear": [issue] if status not in {"pass", "fail", "missing"} else [],
    }
    metrics = dict(check.get("metrics") or {})
    metrics["manual_input_count"] = len(items)
    check["metrics"] = metrics


def _recompute_manual_consistency(check: dict[str, Any], items: list[dict[str, Any]]) -> None:
    has_missing = False
    has_unclear = False
    for item in items:
        value = item.get("effective_value")
        if not isinstance(value, dict):
            has_unclear = True
            continue
        manual_status = str(
            value.get("manual_status")
            or value.get("consistency_status")
            or value.get("status")
            or value.get("result")
            or ""
        ).strip().lower()
        if manual_status in {"pass", "found", "ok", "true", "通过", "一致"}:
            continue
        if manual_status in {"fail", "missing", "late", "false", "不通过", "不一致", "缺失"}:
            has_missing = True
            continue
        if value.get("missing_anchors") or value.get("unfilled_fields"):
            has_missing = True
        elif not manual_status:
            has_unclear = True
    status = "fail" if has_missing else ("unclear" if has_unclear else "pass")
    summary = (
        "Manual template consistency review passed."
        if status == "pass"
        else (
            "Manual template consistency review failed by business confirmation."
            if status == "fail"
            else "Manual template consistency review needs confirmation."
        )
    )
    _set_manual_check_summary(check, status, summary, items)


def _recompute_manual_pricing(check: dict[str, Any], items: list[dict[str, Any]]) -> None:
    opening_amount: Decimal | None = None
    capital_amount: Decimal | None = None
    tender_limit: Decimal | None = None
    details: list[str] = []

    for item in items:
        field_group = str(item.get("field_group") or "")
        value = item.get("effective_value")
        if field_group == "price_constraint":
            tender_limit = _decimal_from_manual(value)
            continue
        if field_group == "opening_amount":
            opening_amount, capital_amount = _amount_pair_from_value(value)

    status = "pass"
    if opening_amount is None:
        status = "missing"
        details.append("Manual opening amount is missing.")
    elif capital_amount is None:
        status = "unclear"
        details.append("Manual opening capital amount is missing; case consistency cannot be confirmed.")
    elif abs(opening_amount - capital_amount) <= Decimal("0.01"):
        details.append("Manual opening lowercase and uppercase amounts match.")
    else:
        status = "fail"
        details.append("Manual opening lowercase and uppercase amounts do not match.")

    if tender_limit is None:
        if status == "pass":
            status = "unclear"
        details.append("Manual tender highest limit is missing; limit comparison cannot be confirmed.")
    elif opening_amount is not None:
        if opening_amount <= tender_limit + Decimal("0.01"):
            details.append("Manual bid amount does not exceed tender highest limit.")
        else:
            status = "fail"
            details.append("Manual bid amount exceeds tender highest limit.")

    summary = " ".join(details) if details else "Manual pricing review needs confirmation."
    _set_manual_check_summary(check, status, summary, items)


def _manual_itemized_decimal(value: Any, keys: tuple[str, ...]) -> Decimal | None:
    if isinstance(value, dict):
        for key in keys:
            amount = _decimal_from_manual(value.get(key))
            if amount is not None:
                return amount
        return None
    return _decimal_from_manual(value)


def _manual_itemized_row_values(value: Any) -> tuple[Decimal | None, Decimal | None, Decimal | None, Decimal | None]:
    if not isinstance(value, dict):
        amount = _decimal_from_manual(value)
        return None, None, amount, amount

    quantity = _manual_itemized_decimal(value, ("quantity",))
    unit_price = _manual_itemized_decimal(value, ("unit_price",))
    ocr_total = _manual_itemized_decimal(
        value,
        (
            "ocr_total",
            "declared_line_total",
            "line_total",
            "raw_line_total",
            "total_amount",
            "subtotal",
        ),
    )
    calculated_total = (
        quantity * unit_price
        if quantity is not None and unit_price is not None
        else _manual_itemized_decimal(value, ("calculated_total", "expected_total", "amount_yuan", "amount"))
    )
    if ocr_total is None:
        ocr_total = _manual_itemized_decimal(value, ("amount_yuan", "amount"))
    return quantity, unit_price, ocr_total, calculated_total


def _manual_itemized_total_value(value: Any) -> Decimal | None:
    if isinstance(value, dict):
        return _manual_itemized_decimal(value, ("declared_total", "amount_yuan", "amount", "total_amount"))
    return _decimal_from_manual(value)


def _recompute_manual_itemized(check: dict[str, Any], items: list[dict[str, Any]]) -> None:
    row_totals: list[Decimal] = []
    totals: list[Decimal] = []
    row_failures = 0
    incomplete_rows = 0
    amount_only_rows = 0

    for item in items:
        if item.get("field_group") == "itemized_amount":
            quantity, unit_price, ocr_total, calculated_total = _manual_itemized_row_values(item.get("effective_value"))
            if calculated_total is None and ocr_total is None:
                incomplete_rows += 1
                continue
            if quantity is not None and unit_price is not None:
                if ocr_total is None:
                    incomplete_rows += 1
                elif abs(calculated_total - ocr_total) > Decimal("0.01"):
                    row_failures += 1
                row_totals.append(calculated_total)
            else:
                amount_only_rows += 1
                row_totals.append(calculated_total if calculated_total is not None else ocr_total)
        elif item.get("field_group") == "itemized_total":
            total = _manual_itemized_total_value(item.get("effective_value"))
            if total is not None:
                totals.append(total)

    if not row_totals:
        _set_manual_check_summary(check, "missing", "Manual itemized amount rows are missing.", items)
        return
    calculated = sum(row_totals, Decimal("0"))
    if not totals:
        _set_manual_check_summary(
            check,
            "missing",
            f"Manual itemized calculated sum is {calculated}; no declared total was provided.",
            items,
        )
        return
    declared = totals[-1]
    total_matches = abs(calculated - declared) <= Decimal("0.01")
    if row_failures or not total_matches:
        status = "fail"
    elif incomplete_rows:
        status = "missing"
    else:
        status = "pass"

    details = [
        f"Manual itemized row arithmetic failures: {row_failures}.",
        f"Manual itemized calculated sum: {calculated}; declared total: {declared}.",
        "Manual itemized sum matches declared total." if total_matches else "Manual itemized sum does not match declared total.",
    ]
    if incomplete_rows:
        details.append(f"Manual itemized incomplete rows: {incomplete_rows}.")
    if amount_only_rows:
        details.append(f"Manual itemized amount-only rows included in sum: {amount_only_rows}.")
    summary = " ".join(details)
    _set_manual_check_summary(check, status, summary, items)


def _verification_value_status(value: Any, field_group: str) -> str:
    if isinstance(value, str):
        return "pass" if value.strip() else "missing"
    if isinstance(value, list):
        return "pass" if value else "missing"
    if not isinstance(value, dict):
        return "unclear"
    statuses = []
    for key in ("signature_status", "seal_status", "date_status", "status"):
        raw = str(value.get(key) or "").strip().lower()
        if raw:
            statuses.append(raw)
    if field_group == "attachment_result" and (
        value.get("signature_text") or value.get("signature_texts") or value.get("signature_evidence")
    ):
        statuses.append("pass")
    if field_group == "attachment_result" and (
        value.get("seal_text") or value.get("seal_texts") or value.get("seal_evidence")
    ):
        statuses.append("pass")
    if value.get("date") or value.get("date_text"):
        statuses.append("pass")
    if any(item in {"fail", "late"} for item in statuses):
        return "fail"
    if any(item in {"missing", "missing_date"} for item in statuses):
        return "missing"
    if any(item in {"pass", "found"} for item in statuses):
        return "pass"
    return "unclear"


def _recompute_manual_verification(check: dict[str, Any], items: list[dict[str, Any]]) -> None:
    statuses = [_verification_value_status(item.get("effective_value"), str(item.get("field_group") or "")) for item in items]
    if any(status == "fail" for status in statuses):
        status = "fail"
        summary = "Manual signature/seal/date review has failed items."
    elif any(status == "missing" for status in statuses):
        status = "missing"
        summary = "Manual signature/seal/date review has missing items."
    elif any(status == "unclear" for status in statuses):
        status = "unclear"
        summary = "Manual signature/seal/date review has unclear items."
    else:
        status = "pass"
        summary = "Manual signature/seal/date review passed."
    _set_manual_check_summary(check, status, summary, items)


def _apply_manual_business_review_inputs(
    review: dict[str, Any],
    manual_payload: dict[str, Any],
) -> dict[str, Any]:
    corrected = deepcopy(review)
    editables = _build_business_format_editable_items(corrected, manual_payload)
    applied = [
        item
        for item in editables
        if item.get("has_manual_value")
        and item.get("field_group") != "template_skeleton_item"
    ]
    for item in applied:
        _set_path_value(corrected, str(item.get("result_path") or ""), item.get("manual_value"))
        item["effective_value"] = item.get("manual_value")

    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for item in editables:
        key = (str(item.get("bidder_key") or ""), str(item.get("check_code") or ""))
        grouped.setdefault(key, []).append(item)

    for bidder_index, bidder in enumerate(corrected.get("bidders") or []):
        if not isinstance(bidder, dict):
            continue
        bidder_key = str(bidder.get("bidder_key") or f"bidder_{bidder_index + 1}")
        checks = bidder.get("checks") or {}
        for check_code, check in checks.items():
            if check_code not in BUSINESS_FORMAT_EDITABLE_GROUPS or not isinstance(check, dict):
                continue
            check_items = grouped.get((bidder_key, check_code), [])
            if not any(item.get("has_manual_value") for item in check_items):
                continue
            if check_code == "consistency_check":
                _recompute_manual_consistency(check, check_items)
            elif check_code == "pricing_check":
                _recompute_manual_pricing(check, check_items)
            elif check_code == "itemized_pricing_check":
                _recompute_manual_itemized(check, check_items)
            elif check_code == "verification_check":
                _recompute_manual_verification(check, check_items)

    corrected["review_mode"] = "manual_corrected" if applied else corrected.get("review_mode") or "system"
    corrected["manual_updated_at"] = _utc_timestamp() if applied else corrected.get("manual_updated_at")
    corrected["manual_review_summary"] = {
        "applied": bool(applied),
        "applied_item_count": len(applied),
        "stored_item_count": len(_manual_items_by_id(manual_payload)),
    }
    return corrected


def _save_business_manual_inputs(
    *,
    identifier_id: str,
    db_service: PostgreSQLService,
    raw_items: list[dict[str, Any]],
    invalidate_project_cache: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    existing_record = db_service.get_project_result(identifier_id)
    _business_review_from_record(existing_record)
    existing_payload = _business_manual_payload_for_project(
        identifier_id=identifier_id,
        db_service=db_service,
    )
    payload = _normalize_business_manual_items(raw_items, existing_payload=existing_payload)
    items_by_document: dict[str, list[dict[str, Any]]] = {}
    touched_document_ids: set[str] = {
        str(item.get("document_identifier_id") or "").strip()
        for item in raw_items or []
        if isinstance(item, dict) and str(item.get("document_identifier_id") or "").strip()
    }
    for item in payload.get("items") or []:
        if not isinstance(item, dict):
            continue
        document_id = str(item.get("document_identifier_id") or "").strip()
        if document_id:
            items_by_document.setdefault(document_id, []).append(item)

    for document_id in sorted(set(items_by_document) | touched_document_ids):
        DocumentWorkingCopyService(db_service).apply_business_bid_format_review(
            document_id,
            {
                "schema_version": "1.0",
                "result_key": BUSINESS_FORMAT_RESULT_KEY,
                "updated_at": payload.get("updated_at") or _utc_timestamp(),
                "items": items_by_document.get(document_id, []),
            },
        )
    result_record = db_service.get_project_result(identifier_id)
    if invalidate_project_cache is not None:
        invalidate_project_cache(identifier_id)
    return result_record
