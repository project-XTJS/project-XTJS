from __future__ import annotations

from copy import deepcopy
from typing import Any

from app.service.manual_review.working_copy import (
    MANUAL_EXTRACTIONS_KEY,
    PERSONNEL_REUSE_CHECK_KEY,
)
from app.service.manual_review_state import as_dict
from app.service.workflow_scope import filter_project_payload


class ProjectAnalysisInputLoader:
    """Load project analysis inputs with effective document content and manual scope applied."""

    def __init__(self, db_service: Any) -> None:
        self.db_service = db_service

    def load(
        self,
        identifier_id: str,
        *,
        include_excluded: bool = False,
    ) -> dict[str, Any] | None:
        payload = self.db_service.get_project_documents_for_duplicate_check(identifier_id)
        if not payload:
            return None
        normalized = self._normalize_payload(payload)
        return filter_project_payload(normalized, include_excluded=include_excluded)

    def _normalize_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        normalized = dict(payload or {})
        normalized["documents"] = [
            self._normalize_document(document)
            for document in list(normalized.get("documents") or [])
            if isinstance(document, dict)
        ]
        return normalized

    def _normalize_document(self, document: dict[str, Any]) -> dict[str, Any]:
        normalized = dict(document)
        if "raw_content" not in normalized:
            normalized["raw_content"] = deepcopy(as_dict(normalized.get("content")))
        review_content = as_dict(normalized.get("review_content"))
        effective_content = as_dict(review_content.get("effective_content"))
        if effective_content:
            normalized["content"] = deepcopy(effective_content)

        if "tender_raw_content" not in normalized:
            normalized["tender_raw_content"] = deepcopy(
                as_dict(normalized.get("tender_content"))
            )
        tender_review_content = as_dict(normalized.get("tender_review_content"))
        effective_tender_content = as_dict(
            tender_review_content.get("effective_content")
        )
        if effective_tender_content:
            normalized["tender_content"] = deepcopy(effective_tender_content)
        personnel_payload = self._manual_personnel_payload(normalized)
        if personnel_payload:
            normalized["manual_personnel_reuse_check"] = personnel_payload
            entries = self._manual_personnel_entries(personnel_payload)
            if entries is not None:
                normalized["personnel_entries"] = entries
        return normalized

    @staticmethod
    def _manual_personnel_payload(document: dict[str, Any]) -> dict[str, Any]:
        for source in ProjectAnalysisInputLoader._manual_payload_sources(document):
            manual_extractions = as_dict(source.get(MANUAL_EXTRACTIONS_KEY))
            payload = as_dict(manual_extractions.get(PERSONNEL_REUSE_CHECK_KEY))
            if payload:
                return payload
        review_content = as_dict(document.get("review_content"))
        inputs = as_dict(review_content.get("inputs"))
        payload = as_dict(inputs.get(PERSONNEL_REUSE_CHECK_KEY))
        return payload

    @staticmethod
    def _manual_payload_sources(document: dict[str, Any]) -> list[dict[str, Any]]:
        review_content = as_dict(document.get("review_content"))
        return [
            as_dict(document.get("content")),
            as_dict(review_content.get("effective_content")),
        ]

    @staticmethod
    def _manual_personnel_entries(payload: dict[str, Any]) -> list[dict[str, Any]] | None:
        if "personnel_entries" in payload and isinstance(payload.get("personnel_entries"), list):
            return [
                deepcopy(item)
                for item in payload.get("personnel_entries") or []
                if isinstance(item, dict)
            ]
        documents = payload.get("documents")
        if isinstance(documents, list):
            entries: list[dict[str, Any]] = []
            for document in documents:
                if not isinstance(document, dict):
                    continue
                for entry in document.get("personnel_entries") or []:
                    if isinstance(entry, dict):
                        entries.append(deepcopy(entry))
            return entries
        document = payload.get("document")
        if isinstance(document, dict):
            return [
                deepcopy(item)
                for item in document.get("personnel_entries") or []
                if isinstance(item, dict)
            ]
        return None
