from __future__ import annotations

import hashlib
import html
import json
import re
from html.parser import HTMLParser
from itertools import combinations
from typing import Any

from app.core.document_types import DOCUMENT_TYPE_BUSINESS_BID, DOCUMENT_TYPE_TECHNICAL_BID

from .template_extractor import SectionClassifier


class _TableHTMLParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.rows: list[list[str]] = []
        self._current_row: list[str] | None = None
        self._current_cell_parts: list[str] | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        normalized = tag.lower()
        if normalized == "tr":
            self._current_row = []
            return
        if normalized in {"td", "th"} and self._current_row is not None:
            self._current_cell_parts = []
            return
        if normalized == "br" and self._current_cell_parts is not None:
            self._current_cell_parts.append("\n")

    def handle_endtag(self, tag: str) -> None:
        normalized = tag.lower()
        if normalized in {"td", "th"} and self._current_row is not None and self._current_cell_parts is not None:
            text = "".join(self._current_cell_parts)
            text = re.sub(r"\s+", " ", html.unescape(text)).strip()
            self._current_row.append(text)
            self._current_cell_parts = None
            return
        if normalized == "tr" and self._current_row is not None:
            if any(cell for cell in self._current_row):
                self.rows.append(self._current_row)
            self._current_row = None

    def handle_data(self, data: str) -> None:
        if self._current_cell_parts is not None:
            self._current_cell_parts.append(data)


class DuplicateCheckService:
    SUPPORTED_DOCUMENT_TYPES = (DOCUMENT_TYPE_BUSINESS_BID, DOCUMENT_TYPE_TECHNICAL_BID)
    PAGE_NUMBER_PATTERN = re.compile(r"^\d+$")
    SPLIT_LINE_PATTERN = re.compile(r"[\r\n]+")

    def check_project_documents(
        self,
        *,
        project_identifier: str,
        project: dict[str, Any] | None,
        document_records: list[dict[str, Any]],
        document_types: list[str] | None = None,
        max_evidence_sections: int = 5,
        max_pairs_per_type: int = 0,
    ) -> dict[str, Any]:
        requested_types = self._normalize_requested_types(document_types)
        prepared_groups: dict[str, list[dict[str, Any]]] = {item: [] for item in requested_types}
        skipped_groups: dict[str, list[dict[str, Any]]] = {item: [] for item in requested_types}

        dedupe_keys: set[tuple[str, str]] = set()
        for record in document_records:
            role = self._normalize_document_role(
                record.get("relation_role") or record.get("document_type")
            )
            if role not in requested_types:
                continue

            identifier_id = str(record.get("identifier_id") or "").strip()
            if not identifier_id:
                skipped_groups[role].append(
                    {
                        "relation_id": record.get("relation_id"),
                        "file_name": record.get("file_name"),
                        "reason": "missing_identifier_id",
                    }
                )
                continue

            dedupe_key = (role, identifier_id)
            if dedupe_key in dedupe_keys:
                continue
            dedupe_keys.add(dedupe_key)

            prepared = self._prepare_document(record)
            if prepared is None:
                skipped_groups[role].append(
                    {
                        "identifier_id": identifier_id,
                        "relation_id": record.get("relation_id"),
                        "file_name": record.get("file_name"),
                        "reason": "missing_or_unusable_ocr_content",
                    }
                )
                continue
            prepared["document_type"] = role
            prepared_groups[role].append(prepared)

        groups: dict[str, Any] = {}
        suspicious_pair_count = 0
        high_risk_pair_count = 0
        medium_risk_pair_count = 0

        for role in requested_types:
            documents = prepared_groups[role]
            pair_items = [
                self._compare_documents(left, right, role=role, max_evidence_sections=max_evidence_sections)
                for left, right in combinations(documents, 2)
            ]
            pair_items.sort(key=self._pair_sort_key, reverse=True)

            total_pair_count = len(pair_items)
            if max_pairs_per_type > 0:
                pair_items = pair_items[:max_pairs_per_type]

            suspicious = [
                item for item in pair_items if str(item.get("risk_level") or "none") != "none"
            ]
            high = [item for item in pair_items if item.get("risk_level") == "high"]
            medium = [item for item in pair_items if item.get("risk_level") == "medium"]

            suspicious_pair_count += len(suspicious)
            high_risk_pair_count += len(high)
            medium_risk_pair_count += len(medium)

            groups[role] = {
                "document_count": len(documents),
                "pair_count": total_pair_count,
                "reported_pair_count": len(pair_items),
                "suspicious_pair_count": len(suspicious),
                "high_risk_pair_count": len(high),
                "medium_risk_pair_count": len(medium),
                "documents": [
                    {
                        "identifier_id": item["identifier_id"],
                        "relation_id": item.get("relation_id"),
                        "file_name": item.get("file_name"),
                        "section_count": item.get("section_count", 0),
                        "block_count": item.get("block_count", 0),
                        "table_count": item.get("table_count", 0),
                    }
                    for item in documents
                ],
                "skipped_documents": skipped_groups[role],
                "items": pair_items,
            }

        return {
            "project": project or {"identifier_id": project_identifier},
            "config": {
                "detection_mode": "exact_only",
                "document_types": list(requested_types),
                "max_evidence_sections": int(max_evidence_sections),
                "max_pairs_per_type": int(max_pairs_per_type),
            },
            "groups": groups,
            "summary": {
                "requested_document_types": list(requested_types),
                "document_count": sum(groups[item]["document_count"] for item in groups),
                "pair_count": sum(groups[item]["pair_count"] for item in groups),
                "reported_pair_count": sum(groups[item]["reported_pair_count"] for item in groups),
                "suspicious_pair_count": suspicious_pair_count,
                "high_risk_pair_count": high_risk_pair_count,
                "medium_risk_pair_count": medium_risk_pair_count,
            },
        }

    def _normalize_requested_types(self, document_types: list[str] | None) -> tuple[str, ...]:
        if not document_types:
            return self.SUPPORTED_DOCUMENT_TYPES

        normalized: list[str] = []
        for item in document_types:
            role = self._normalize_document_role(item)
            if role not in self.SUPPORTED_DOCUMENT_TYPES:
                raise ValueError(f"Unsupported duplicate-check document type: {item}")
            if role not in normalized:
                normalized.append(role)
        return tuple(normalized) if normalized else self.SUPPORTED_DOCUMENT_TYPES

    def _normalize_document_role(self, value: Any) -> str:
        normalized = str(value or "").strip().lower()
        if normalized in {"business", "business_bid"}:
            return DOCUMENT_TYPE_BUSINESS_BID
        if normalized in {"technical", "technical_bid"}:
            return DOCUMENT_TYPE_TECHNICAL_BID
        return normalized

    def _pair_sort_key(self, item: dict[str, Any]) -> tuple[Any, ...]:
        metrics = item.get("metrics") or {}
        return (
            self._risk_rank(item.get("risk_level")),
            bool(item.get("exact_duplicate")),
            float(item.get("exact_match_score") or 0.0),
            int(metrics.get("exact_table_count") or 0),
            int(metrics.get("exact_section_count") or 0),
            int(metrics.get("exact_block_count") or 0),
        )

    def _prepare_document(self, record: dict[str, Any]) -> dict[str, Any] | None:
        payload = self._coerce_payload(record.get("content"))
        container = self._container(payload)
        ordered_blocks, table_entries = self._extract_ordered_blocks(container)

        if not ordered_blocks:
            fallback_text = self._fallback_text(container)
            if fallback_text:
                exact_key = self._compact_raw_text(fallback_text)
                ordered_blocks = [
                    {
                        "type": "text",
                        "page": 1,
                        "text": fallback_text,
                        "exact_key": exact_key,
                        "exact_hash": self._hash_text(exact_key),
                    }
                ]

        if not ordered_blocks:
            return None

        sections = self._build_sections(ordered_blocks)
        full_text = "\n".join(block["text"] for block in ordered_blocks if block.get("text"))
        exact_key = self._compact_raw_text(full_text)
        if len(exact_key) < 16:
            return None

        exact_block_map = {
            block["exact_hash"]: block
            for block in ordered_blocks
            if block["type"] != "heading" and len(block["exact_key"]) >= 18
        }
        exact_section_map = {
            section["exact_hash"]: section
            for section in sections
            if len(section["exact_key"]) >= 8
        }
        exact_table_map = {table["exact_hash"]: table for table in table_entries}

        return {
            "identifier_id": str(record.get("identifier_id") or ""),
            "relation_id": record.get("relation_id"),
            "file_name": str(record.get("file_name") or ""),
            "full_text": full_text,
            "exact_key": exact_key,
            "exact_hash": self._hash_text(exact_key),
            "blocks": ordered_blocks,
            "exact_block_hashes": set(exact_block_map.keys()),
            "exact_block_map": exact_block_map,
            "sections": sections,
            "section_count": len(sections),
            "exact_section_hashes": set(exact_section_map.keys()),
            "exact_section_map": exact_section_map,
            "block_count": len(ordered_blocks),
            "tables": table_entries,
            "table_count": len(table_entries),
            "exact_table_hashes": set(exact_table_map.keys()),
            "exact_table_map": exact_table_map,
        }

    def _coerce_payload(self, value: Any) -> dict[str, Any]:
        if isinstance(value, dict):
            return value
        if isinstance(value, str):
            stripped = value.strip()
            if stripped.startswith("{") and stripped.endswith("}"):
                try:
                    parsed = json.loads(stripped)
                except json.JSONDecodeError:
                    return {"text": value}
                return parsed if isinstance(parsed, dict) else {"text": value}
            return {"text": value}
        return {}

    def _container(self, payload: dict[str, Any]) -> dict[str, Any]:
        if isinstance(payload.get("data"), dict):
            return payload["data"]
        return payload

    def _extract_ordered_blocks(
        self,
        container: dict[str, Any],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        table_queues, table_entries = self._build_table_queues(container)
        layout_sections = self._layout_sections(container)
        ordered_blocks: list[dict[str, Any]] = []

        for section in layout_sections:
            section_type = str(section.get("type") or "text").strip().lower()
            if section_type in {"seal", "signature"}:
                continue

            page = section.get("page") if isinstance(section.get("page"), int) else 1
            if section_type == "table":
                page_tables = table_queues.get(page) or []
                text = page_tables.pop(0) if page_tables else ""
                if not text:
                    text = self._normalize_plain_text(
                        section.get("raw_text") or section.get("text") or ""
                    )
            else:
                text = self._normalize_plain_text(section.get("text") or section.get("raw_text") or "")

            if self._is_noise_block(text, section_type):
                continue

            exact_key = self._compact_raw_text(text)
            if section_type != "heading" and len(exact_key) < 8:
                continue

            ordered_blocks.append(
                {
                    "type": section_type,
                    "page": page,
                    "text": text,
                    "exact_key": exact_key,
                    "exact_hash": self._hash_text(exact_key),
                }
            )

        return ordered_blocks, table_entries

    def _layout_sections(self, container: dict[str, Any]) -> list[dict[str, Any]]:
        items = container.get("layout_sections")
        if not isinstance(items, list):
            return []

        normalized: list[dict[str, Any]] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            text = item.get("text") or item.get("raw_text")
            if not text:
                continue
            bbox = item.get("bbox") or item.get("bbox_ocr") or item.get("box")
            anchor = self._bbox_anchor(bbox)
            normalized.append(
                {
                    "page": item.get("page") if isinstance(item.get("page"), int) else 1,
                    "type": str(item.get("type") or "text").strip().lower() or "text",
                    "text": text,
                    "raw_text": item.get("raw_text"),
                    "_sort_y": anchor[1],
                    "_sort_x": anchor[0],
                }
            )

        normalized.sort(key=lambda item: (item["page"], item["_sort_y"], item["_sort_x"]))
        return normalized

    def _bbox_anchor(self, bbox: Any) -> tuple[int, int]:
        if isinstance(bbox, (list, tuple)) and len(bbox) >= 2:
            if all(isinstance(item, (int, float)) for item in bbox[:2]):
                return (int(round(float(bbox[0]))), int(round(float(bbox[1]))))
            if bbox and all(
                isinstance(item, (list, tuple))
                and len(item) >= 2
                and all(isinstance(value, (int, float)) for value in item[:2])
                for item in bbox
            ):
                xs = [int(round(float(item[0]))) for item in bbox]
                ys = [int(round(float(item[1]))) for item in bbox]
                return (min(xs), min(ys))
        return (10**9, 10**9)

    def _build_table_queues(
        self,
        container: dict[str, Any],
    ) -> tuple[dict[int, list[str]], list[dict[str, Any]]]:
        candidates: list[Any] = []
        if isinstance(container.get("logical_tables"), list):
            candidates.extend(container.get("logical_tables") or [])
        if isinstance(container.get("table_sections"), list):
            candidates.extend(container.get("table_sections") or [])

        page_queues: dict[int, list[str]] = {}
        table_entries: list[dict[str, Any]] = []
        for raw_item in candidates:
            if not isinstance(raw_item, dict):
                continue

            lines = self._table_to_lines(raw_item)
            if not lines:
                continue

            pages = raw_item.get("pages")
            if isinstance(pages, list):
                normalized_pages = [page for page in pages if isinstance(page, int)]
            else:
                page = raw_item.get("page")
                normalized_pages = [page] if isinstance(page, int) else [1]

            text = "\n".join(lines).strip()
            if not text:
                continue

            for page in normalized_pages or [1]:
                page_queues.setdefault(page, []).append(text)

            table_entries.append(
                {
                    "pages": normalized_pages or [1],
                    "text": text,
                    "rows": lines,
                    "exact_hash": self._hash_text(self._compact_raw_text(text)),
                }
            )

        return page_queues, table_entries

    def _table_to_lines(self, table: dict[str, Any]) -> list[str]:
        rows = table.get("rows")
        if isinstance(rows, list) and rows:
            result = []
            for row in rows:
                if isinstance(row, dict):
                    values = [str(value).strip() for value in row.values() if str(value).strip()]
                elif isinstance(row, list):
                    values = [str(value).strip() for value in row if str(value).strip()]
                else:
                    values = [str(row).strip()]
                if values:
                    result.append(" | ".join(values))
            if result:
                return result

        records = table.get("records")
        if isinstance(records, list) and records:
            result = []
            for record in records:
                if not isinstance(record, dict):
                    continue
                values = [str(value).strip() for value in record.values() if str(value).strip()]
                if values:
                    result.append(" | ".join(values))
            if result:
                return result

        for key in ("block_content", "html"):
            candidate = str(table.get(key) or "").strip()
            if "<table" in candidate.lower():
                parser = _TableHTMLParser()
                try:
                    parser.feed(candidate)
                    parser.close()
                except Exception:
                    parser = None
                if parser and parser.rows:
                    return [
                        " | ".join(cell for cell in row if cell)
                        for row in parser.rows
                        if any(cell for cell in row)
                    ]

        for key in ("raw_text", "text", "block_content"):
            candidate = self._normalize_plain_text(table.get(key) or "")
            if not candidate:
                continue
            lines = [line.strip() for line in self.SPLIT_LINE_PATTERN.split(candidate) if line.strip()]
            if lines:
                return lines
            return [candidate]

        return []

    def _normalize_plain_text(self, value: Any) -> str:
        text = html.unescape(str(value or ""))
        text = text.replace("\u3000", " ").replace("\xa0", " ")
        text = text.replace("\r\n", "\n").replace("\r", "\n")
        text = re.sub(r"[ \t\f\v]+", " ", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    def _compact_raw_text(self, text: str) -> str:
        normalized = self._normalize_plain_text(text)
        return re.sub(r"\s+", "", normalized)

    def _is_noise_block(self, text: str, section_type: str) -> bool:
        compact = self._compact_raw_text(text)
        if not compact:
            return True
        if self.PAGE_NUMBER_PATTERN.fullmatch(compact):
            return True
        if SectionClassifier.RE_TOC.search(text):
            return True
        if section_type != "heading" and len(compact) < 4:
            return True
        return False

    def _build_sections(self, blocks: list[dict[str, Any]]) -> list[dict[str, Any]]:
        sections: list[dict[str, Any]] = []
        current = self._new_section("document_prelude")

        for block in blocks:
            is_heading = block["type"] == "heading" or SectionClassifier.is_heading(block["text"])
            if is_heading and current["blocks"]:
                finalized = self._finalize_section(current)
                if finalized is not None:
                    sections.append(finalized)
                current = self._new_section(block["text"])
                current["pages"].add(int(block.get("page") or 1))
                continue

            current["blocks"].append(block)
            current["pages"].add(int(block.get("page") or 1))

        finalized = self._finalize_section(current)
        if finalized is not None:
            sections.append(finalized)

        if len(sections) <= 1:
            fallback_sections = self._build_fallback_sections(blocks)
            if fallback_sections:
                sections = fallback_sections

        return [section for section in sections if len(section.get("exact_key") or "") >= 8]

    def _new_section(self, title: str) -> dict[str, Any]:
        normalized_title = self._normalize_plain_text(title) or "document_prelude"
        return {"title": normalized_title, "pages": set(), "blocks": []}

    def _finalize_section(self, section: dict[str, Any]) -> dict[str, Any] | None:
        blocks = [block for block in section.get("blocks", []) if block.get("text")]
        if not blocks:
            return None

        text = "\n".join(block["text"] for block in blocks if block.get("text")).strip()
        if not text:
            return None

        exact_key = self._compact_raw_text(text)
        return {
            "title": section.get("title") or "document_prelude",
            "pages": sorted(page for page in section.get("pages", set()) if isinstance(page, int)),
            "text": text,
            "preview": self._clip(text, 160),
            "exact_key": exact_key,
            "exact_hash": self._hash_text(exact_key),
        }

    def _build_fallback_sections(
        self,
        blocks: list[dict[str, Any]],
        chunk_size: int = 10,
    ) -> list[dict[str, Any]]:
        content_blocks = [block for block in blocks if block["type"] != "heading"]
        if len(content_blocks) < 4:
            return []

        sections: list[dict[str, Any]] = []
        for index in range(0, len(content_blocks), chunk_size):
            chunk = content_blocks[index : index + chunk_size]
            if not chunk:
                continue

            text = "\n".join(block["text"] for block in chunk if block.get("text")).strip()
            if not text:
                continue

            exact_key = self._compact_raw_text(text)
            sections.append(
                {
                    "title": f"chunk_{len(sections) + 1}",
                    "pages": sorted(
                        {
                            int(block.get("page") or 1)
                            for block in chunk
                            if isinstance(block.get("page"), int) or block.get("page") is not None
                        }
                    ),
                    "text": text,
                    "preview": self._clip(text, 160),
                    "exact_key": exact_key,
                    "exact_hash": self._hash_text(exact_key),
                }
            )
        return sections

    def _fallback_text(self, container: dict[str, Any]) -> str:
        for key in ("content", "text", "full_text"):
            value = container.get(key)
            if isinstance(value, str) and value.strip():
                return self._normalize_plain_text(value)

        pages = container.get("pages")
        if isinstance(pages, list):
            parts = []
            for item in pages:
                if isinstance(item, dict):
                    text = self._normalize_plain_text(item.get("text") or "")
                    if text:
                        parts.append(text)
            return "\n".join(parts).strip()

        return ""

    def _compare_documents(
        self,
        left: dict[str, Any],
        right: dict[str, Any],
        *,
        role: str,
        max_evidence_sections: int,
    ) -> dict[str, Any]:
        block_metrics = self._compare_blocks(left, right, max_evidence_sections=max_evidence_sections)
        section_metrics = self._compare_sections(left, right, max_evidence_sections=max_evidence_sections)
        table_metrics = self._compare_tables(left, right, max_evidence_sections=max_evidence_sections)

        exact_duplicate = bool(left["exact_hash"] == right["exact_hash"])
        exact_match_score = self._exact_match_score(
            exact_duplicate=exact_duplicate,
            exact_block_overlap_ratio=float(block_metrics["exact_overlap_ratio"]),
            exact_section_match_ratio=float(section_metrics["exact_match_ratio"]),
            exact_table_match_ratio=float(table_metrics["exact_match_ratio"]),
        )
        risk_level = self._exact_risk_level(
            exact_duplicate=exact_duplicate,
            exact_match_score=exact_match_score,
            exact_block_count=int(block_metrics["exact_shared_count"]),
            exact_section_count=int(section_metrics["exact_match_count"]),
            exact_table_count=int(table_metrics["exact_match_count"]),
            exact_block_overlap_ratio=float(block_metrics["exact_overlap_ratio"]),
        )

        notes = []
        if not left["tables"] or not right["tables"]:
            notes.append("at_least_one_document_has_no_structured_table_content")
        if not left["sections"] or not right["sections"]:
            notes.append("at_least_one_document_has_no_stable_section_structure")

        return {
            "left_document_identifier": left["identifier_id"],
            "right_document_identifier": right["identifier_id"],
            "left_relation_id": left.get("relation_id"),
            "right_relation_id": right.get("relation_id"),
            "left_file_name": left.get("file_name"),
            "right_file_name": right.get("file_name"),
            "document_type": role,
            "exact_duplicate": exact_duplicate,
            "exact_match_score": round(exact_match_score, 4),
            "risk_level": risk_level,
            "suspicious": risk_level != "none",
            "metrics": {
                "exact_block_count": int(block_metrics["exact_shared_count"]),
                "exact_section_count": int(section_metrics["exact_match_count"]),
                "exact_table_count": int(table_metrics["exact_match_count"]),
                "exact_block_overlap_ratio": round(float(block_metrics["exact_overlap_ratio"]), 4),
                "exact_section_overlap_ratio": round(float(section_metrics["exact_match_ratio"]), 4),
                "exact_table_overlap_ratio": round(float(table_metrics["exact_match_ratio"]), 4),
            },
            "duplicate_blocks": block_metrics["items"],
            "duplicate_sections": section_metrics["items"],
            "duplicate_tables": table_metrics["items"],
            "notes": notes,
        }

    def _compare_blocks(
        self,
        left: dict[str, Any],
        right: dict[str, Any],
        *,
        max_evidence_sections: int,
    ) -> dict[str, Any]:
        common_hashes = left["exact_block_hashes"] & right["exact_block_hashes"]
        overlap_ratio = self._dice_ratio(left["exact_block_hashes"], right["exact_block_hashes"])

        items = []
        for block_hash in sorted(common_hashes):
            block = left["exact_block_map"].get(block_hash)
            if not block:
                continue
            items.append(
                {
                    "page": block.get("page"),
                    "type": block.get("type"),
                    "text": self._clip(block.get("text") or "", 160),
                }
            )
            if len(items) >= max_evidence_sections:
                break

        return {
            "exact_overlap_ratio": overlap_ratio,
            "exact_shared_count": len(common_hashes),
            "items": items,
        }

    def _compare_sections(
        self,
        left: dict[str, Any],
        right: dict[str, Any],
        *,
        max_evidence_sections: int,
    ) -> dict[str, Any]:
        common_hashes = left["exact_section_hashes"] & right["exact_section_hashes"]
        exact_match_ratio = len(common_hashes) / max(
            1,
            min(len(left["exact_section_hashes"]), len(right["exact_section_hashes"])),
        )

        items = []
        for section_hash in sorted(common_hashes):
            left_section = left["exact_section_map"].get(section_hash)
            right_section = right["exact_section_map"].get(section_hash)
            if not left_section or not right_section:
                continue
            items.append(
                {
                    "left_title": left_section["title"],
                    "right_title": right_section["title"],
                    "left_pages": left_section.get("pages", []),
                    "right_pages": right_section.get("pages", []),
                    "exact": True,
                    "left_preview": left_section.get("preview"),
                    "right_preview": right_section.get("preview"),
                }
            )
            if len(items) >= max_evidence_sections:
                break

        return {
            "exact_match_count": len(common_hashes),
            "exact_match_ratio": exact_match_ratio,
            "items": items,
        }

    def _compare_tables(
        self,
        left: dict[str, Any],
        right: dict[str, Any],
        *,
        max_evidence_sections: int,
    ) -> dict[str, Any]:
        common_hashes = left["exact_table_hashes"] & right["exact_table_hashes"]
        exact_match_ratio = len(common_hashes) / max(
            1,
            min(len(left["exact_table_hashes"]), len(right["exact_table_hashes"])),
        )

        items = []
        for table_hash in sorted(common_hashes):
            left_table = left["exact_table_map"].get(table_hash)
            right_table = right["exact_table_map"].get(table_hash)
            if not left_table or not right_table:
                continue
            items.append(
                {
                    "left_pages": left_table.get("pages", []),
                    "right_pages": right_table.get("pages", []),
                    "exact": True,
                    "sample_rows": [self._clip(row, 160) for row in left_table.get("rows", [])[:3]],
                }
            )
            if len(items) >= max_evidence_sections:
                break

        return {
            "exact_match_count": len(common_hashes),
            "exact_match_ratio": exact_match_ratio,
            "items": items,
        }

    def _dice_ratio(self, left: set[Any], right: set[Any]) -> float:
        if not left or not right:
            return 0.0
        if left == right:
            return 1.0
        overlap = len(left & right)
        return (2.0 * overlap) / float(len(left) + len(right))

    def _exact_match_score(
        self,
        *,
        exact_duplicate: bool,
        exact_block_overlap_ratio: float,
        exact_section_match_ratio: float,
        exact_table_match_ratio: float,
    ) -> float:
        if exact_duplicate:
            return 1.0
        score = (
            (0.45 * exact_section_match_ratio)
            + (0.35 * exact_block_overlap_ratio)
            + (0.20 * exact_table_match_ratio)
        )
        return min(round(score, 4), 0.9999)

    def _exact_risk_level(
        self,
        *,
        exact_duplicate: bool,
        exact_match_score: float,
        exact_block_count: int,
        exact_section_count: int,
        exact_table_count: int,
        exact_block_overlap_ratio: float,
    ) -> str:
        if exact_duplicate:
            return "high"
        if exact_table_count >= 2:
            return "high"
        if exact_match_score >= 0.35 and exact_section_count >= 5:
            return "high"
        if exact_section_count >= 3 or exact_table_count >= 1:
            return "medium"
        if exact_block_count >= 5 or exact_block_overlap_ratio >= 0.15:
            return "medium"
        if exact_section_count >= 1 or exact_block_count >= 1:
            return "low"
        return "none"

    def _risk_rank(self, risk_level: Any) -> int:
        mapping = {"high": 3, "medium": 2, "low": 1, "none": 0}
        return mapping.get(str(risk_level or "none"), 0)

    def _hash_text(self, text: str) -> str:
        return hashlib.sha256(str(text or "").encode("utf-8")).hexdigest()

    def _clip(self, text: str, max_chars: int) -> str:
        if len(text) <= max_chars:
            return text
        return f"{text[:max_chars].rstrip()}..."
