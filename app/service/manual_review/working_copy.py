from __future__ import annotations

from copy import deepcopy
from typing import Any

from app.service.manual_review_state import as_dict, utc_now_iso


MANUAL_EXTRACTIONS_KEY = "manual_extractions"
BUSINESS_BID_FORMAT_REVIEW_KEY = "business_bid_format_review"
PERSONNEL_REUSE_CHECK_KEY = "personnel_reuse_check"


class DocumentWorkingCopyService:
    """Central writer for document-level manual recognition working copies."""

    def __init__(self, db_service: Any) -> None:
        self.db_service = db_service

    def apply_business_bid_format_review(
        self,
        document_id: str,
        input_payload: dict[str, Any],
    ) -> dict[str, Any] | None:
        payload = self._with_common_metadata(
            input_payload,
            result_key=BUSINESS_BID_FORMAT_REVIEW_KEY,
        )
        return self.apply_input(
            document_id,
            domain_key=BUSINESS_BID_FORMAT_REVIEW_KEY,
            input_payload=payload,
            manual_extraction=payload,
        )

    def apply_personnel_reuse_check(
        self,
        document_id: str,
        input_payload: dict[str, Any],
    ) -> dict[str, Any] | None:
        review_payload = self.db_service.get_document_review_content(document_id)
        if not review_payload:
            return None
        input_payload = self._with_current_document_metadata(
            input_payload,
            document_id=document_id,
            review_payload=review_payload,
        )
        payload = self._with_common_metadata(
            input_payload,
            result_key=PERSONNEL_REUSE_CHECK_KEY,
        )
        extraction = self._personnel_manual_extraction(payload)
        return self.apply_input(
            document_id,
            domain_key=PERSONNEL_REUSE_CHECK_KEY,
            input_payload=payload,
            manual_extraction=extraction,
            review_payload=review_payload,
        )

    def apply_input(
        self,
        document_id: str,
        *,
        domain_key: str,
        input_payload: dict[str, Any],
        manual_extraction: dict[str, Any] | None = None,
        review_payload: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        if review_payload is None:
            review_payload = self.db_service.get_document_review_content(document_id)
        if not review_payload:
            return None

        review_content = as_dict(review_payload.get("review_content"))
        effective_content = deepcopy(as_dict(review_content.get("effective_content")))
        if manual_extraction is not None:
            effective_content = self._set_manual_extraction(
                effective_content,
                domain_key=domain_key,
                manual_extraction=manual_extraction,
            )

        return self.db_service.update_document_review_input(
            document_id,
            input_key=domain_key,
            input_value=input_payload,
            effective_content=effective_content,
        )

    @staticmethod
    def _with_current_document_metadata(
        payload: dict[str, Any],
        *,
        document_id: str,
        review_payload: dict[str, Any],
    ) -> dict[str, Any]:
        document_type = str(review_payload.get("document_type") or "").strip()
        if not document_type:
            return deepcopy(as_dict(payload))

        normalized = deepcopy(as_dict(payload))
        metadata = {
            "identifier_id": str(review_payload.get("identifier_id") or document_id or "").strip(),
            "document_identifier_id": str(review_payload.get("identifier_id") or document_id or "").strip(),
            "document_type": document_type,
            "file_name": str(review_payload.get("file_name") or "").strip(),
        }

        def matches_current_document(item: dict[str, Any]) -> bool:
            item_id = str(
                item.get("document_identifier_id")
                or item.get("identifier_id")
                or item.get("doc_id")
                or ""
            ).strip()
            return not item_id or item_id == metadata["document_identifier_id"]

        def apply_metadata(item: dict[str, Any]) -> dict[str, Any]:
            next_item = deepcopy(as_dict(item))
            if not matches_current_document(next_item):
                return next_item
            next_item["document_identifier_id"] = metadata["document_identifier_id"]
            next_item["identifier_id"] = str(next_item.get("identifier_id") or metadata["identifier_id"])
            next_item["document_type"] = metadata["document_type"]
            if metadata["file_name"] and not str(next_item.get("file_name") or "").strip():
                next_item["file_name"] = metadata["file_name"]
            nested_entries = next_item.get("personnel_entries")
            if isinstance(nested_entries, list):
                next_item["personnel_entries"] = [
                    apply_metadata(entry) if isinstance(entry, dict) else entry
                    for entry in nested_entries
                ]
            return next_item

        document = normalized.get("document")
        if isinstance(document, dict):
            normalized["document"] = apply_metadata(document)

        documents = normalized.get("documents")
        if isinstance(documents, list):
            normalized["documents"] = [
                apply_metadata(item) if isinstance(item, dict) else item
                for item in documents
            ]

        entries = normalized.get("personnel_entries")
        if isinstance(entries, list):
            normalized["personnel_entries"] = [
                apply_metadata(item) if isinstance(item, dict) else item
                for item in entries
            ]

        confirmed_names = normalized.get("confirmed_names")
        if isinstance(confirmed_names, list):
            normalized["confirmed_names"] = [
                apply_metadata(item) if isinstance(item, dict) else item
                for item in confirmed_names
            ]

        return normalized

    @staticmethod
    def _with_common_metadata(payload: dict[str, Any], *, result_key: str) -> dict[str, Any]:
        normalized = deepcopy(as_dict(payload))
        normalized["schema_version"] = str(normalized.get("schema_version") or "1.0")
        normalized["result_key"] = str(normalized.get("result_key") or result_key)
        normalized["updated_at"] = str(normalized.get("updated_at") or utc_now_iso())
        return normalized

    @staticmethod
    def _set_manual_extraction(
        effective_content: dict[str, Any],
        *,
        domain_key: str,
        manual_extraction: dict[str, Any],
    ) -> dict[str, Any]:
        next_content = deepcopy(as_dict(effective_content))
        manual_extractions = deepcopy(as_dict(next_content.get(MANUAL_EXTRACTIONS_KEY)))
        manual_extractions[str(domain_key)] = deepcopy(as_dict(manual_extraction))
        next_content[MANUAL_EXTRACTIONS_KEY] = manual_extractions
        return next_content

    @staticmethod
    def _personnel_manual_extraction(payload: dict[str, Any]) -> dict[str, Any]:
        normalized = deepcopy(as_dict(payload))
        documents = DocumentWorkingCopyService._payload_documents(normalized)
        entries: list[dict[str, Any]] = []
        for document in documents:
            for entry in document.get("personnel_entries") or []:
                if isinstance(entry, dict):
                    entries.append(deepcopy(entry))
        if not entries:
            for entry in normalized.get("personnel_entries") or []:
                if isinstance(entry, dict):
                    entries.append(deepcopy(entry))

        normalized["documents"] = documents
        normalized["personnel_entries"] = entries
        normalized["confirmation_status"] = str(
            normalized.get("confirmation_status") or "draft"
        )
        return normalized

    @staticmethod
    def _payload_documents(payload: dict[str, Any]) -> list[dict[str, Any]]:
        documents: list[dict[str, Any]] = []
        raw_documents = payload.get("documents")
        if isinstance(raw_documents, list):
            for item in raw_documents:
                if isinstance(item, dict):
                    documents.append(deepcopy(item))
        raw_document = payload.get("document")
        if isinstance(raw_document, dict):
            document_key = str(
                raw_document.get("document_identifier_id")
                or raw_document.get("identifier_id")
                or raw_document.get("file_name")
                or ""
            )
            existing_keys = {
                str(
                    item.get("document_identifier_id")
                    or item.get("identifier_id")
                    or item.get("file_name")
                    or ""
                )
                for item in documents
            }
            if not document_key or document_key not in existing_keys:
                documents.append(deepcopy(raw_document))
        return documents
