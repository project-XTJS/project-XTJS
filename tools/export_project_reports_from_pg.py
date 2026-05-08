import os
import difflib
import re
import sys
import html
import json
from pathlib import Path
from typing import Any
from types import MethodType

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.core.document_types import (
    DOCUMENT_TYPE_BUSINESS_BID,
    DOCUMENT_TYPE_TECHNICAL_BID,
)
from app.service.analysis.bid_document_review import BidDocumentReviewService
from app.service.analysis.consistency import ConsistencyChecker, DocumentProcessor
from app.service.analysis.deviation import DeviationChecker
from app.service.analysis.duplicate_check import DuplicateCheckService
from app.service.analysis.duplicate_merge import DuplicateResultMerger
from app.service.analysis.integrity import IntegrityChecker
from app.service.analysis.itemized_pricing import ItemizedPricingChecker
from app.service.analysis.pricing_reasonableness import ReasonablenessChecker
from app.service.analysis.template_extractor import TemplateExtractor
from app.service.analysis.unified_business_review import UnifiedBusinessReviewService
from app.service.analysis.verification import VerificationChecker
from app.service.analysis.visualizer import ReportVisualizer
from app.service.postgresql_service import PostgreSQLService


PROJECT_IDENTIFIER = os.getenv("XTJS_PROJECT_IDENTIFIER", "").strip()
OUTPUT_DIR = Path(os.getenv("XTJS_PROJECT_REPORT_DIR", f"./test_reports/{PROJECT_IDENTIFIER or 'project_from_pg'}"))
API_BASE_URL = os.getenv("XTJS_REPORT_API_BASE_URL", "http://127.0.0.1:8080").rstrip("/")
USE_STORED_PROJECT_RESULTS = os.getenv("XTJS_REPORT_USE_STORED_RESULTS", "1").strip().lower() not in {"0", "false", "no"}
RECOMPUTE_MISSING_PROJECT_RESULTS = os.getenv("XTJS_REPORT_RECOMPUTE_MISSING_RESULTS", "1").strip().lower() not in {"0", "false", "no"}
PROJECT_REVIEW_DISPLAY_OPTIONS = {
    "show_business_duplicates": True,
    "show_technical_duplicates": True,
    "show_personnel_reuse": True,
    "show_typos": True,
    "business_duplicates_only_mode": False,
}


def patch_visualizer_duplicate_display(visualizer: ReportVisualizer) -> None:
    def _normalize_locator_bbox(self, bbox):
        if not isinstance(bbox, (list, tuple)) or len(bbox) < 4:
            return None
        values = bbox[:4]
        if not all(isinstance(item, (int, float)) for item in values):
            return None
        x0, y0, x1, y1 = [int(round(float(item))) for item in values]
        if x1 <= x0 or y1 <= y0:
            return None
        return [x0, y0, x1, y1]

    def _merge_locator_bbox(self, left_bbox, right_bbox):
        left = _normalize_locator_bbox(self, left_bbox)
        right = _normalize_locator_bbox(self, right_bbox)
        if left and not right:
            return left
        if right and not left:
            return right
        if not left or not right:
            return None
        return [
            min(left[0], right[0]),
            min(left[1], right[1]),
            max(left[2], right[2]),
            max(left[3], right[3]),
        ]

    def _strip_locator_html(self, value):
        text = html.unescape(str(value or ""))
        text = re.sub(r"<[^>]+>", " ", text)
        return re.sub(r"\s+", " ", text).strip()

    def _compact_locator_text(self, value):
        text = _strip_locator_html(self, value)
        text = re.sub(r"[^\w\u4e00-\u9fff]+", "", text, flags=re.UNICODE)
        return text.lower()

    def _normalize_highlight_source(self, value):
        return re.sub(r"\s+", " ", str(value or "")).strip()

    def _common_highlight_phrase(self, left_text, right_text):
        left = _normalize_highlight_source(self, left_text)
        right = _normalize_highlight_source(self, right_text)
        if not left or not right:
            return ""
        matcher = difflib.SequenceMatcher(None, left, right)
        best = ""
        for match in sorted(matcher.get_matching_blocks(), key=lambda item: item.size, reverse=True):
            candidate = left[match.a: match.a + match.size].strip()
            if not candidate:
                continue
            compact = re.sub(r"[^\w\u4e00-\u9fff]+", "", candidate, flags=re.UNICODE)
            if len(compact) < 4:
                continue
            if compact.isdigit():
                continue
            best = candidate
            break
        if best:
            return best
        if left == right:
            return left
        return ""

    def _highlight_excerpt_html(self, text, phrase, *, exact=False, limit=180):
        raw = _normalize_highlight_source(self, text)
        if not raw:
            return "<span class='issue-muted'>暂无命中文本</span>"
        if not phrase:
            trimmed = self._project_trim_text(raw, limit)
            if exact:
                return f"<mark>{html.escape(trimmed)}</mark>"
            return html.escape(trimmed)

        index = raw.find(phrase)
        if index < 0:
            lowered_raw = raw.lower()
            lowered_phrase = phrase.lower()
            index = lowered_raw.find(lowered_phrase)
        if index < 0:
            trimmed_phrase = self._project_trim_text(phrase, limit)
            return f"<mark>{html.escape(trimmed_phrase)}</mark>"

        phrase_length = len(phrase)
        context = max(28, (limit - min(phrase_length, limit)) // 2)
        start = max(0, index - context)
        end = min(len(raw), index + phrase_length + context)
        excerpt = raw[start:end]
        relative_index = index - start
        before = excerpt[:relative_index]
        matched = excerpt[relative_index: relative_index + phrase_length]
        after = excerpt[relative_index + phrase_length:]
        prefix = "..." if start > 0 else ""
        suffix = "..." if end < len(raw) else ""
        return (
            f"{html.escape(prefix + before)}"
            f"<mark>{html.escape(matched)}</mark>"
            f"{html.escape(after + suffix)}"
        )

    def _occurrence_highlight_html(self, occurrence, file_name, other_file_name, *, source_lookup):
        docs = occurrence.get("docs") or {}
        doc = docs.get(file_name) or {}
        other_doc = docs.get(other_file_name) or {}
        preview_text = str(doc.get("preview") or "").strip()
        other_preview = str(other_doc.get("preview") or "").strip()
        mode = str(occurrence.get("mode") or "similar")
        kind = str(occurrence.get("kind") or "block")
        evidence = occurrence.get("evidence") or {}
        display_other = str((source_lookup.get(other_file_name) or {}).get("display_name") or other_file_name or "对照文件")

        phrase = ""
        if mode == "exact":
            if kind == "block":
                phrase = preview_text
            else:
                phrase = _common_highlight_phrase(self, preview_text, other_preview) or preview_text
        else:
            phrase = _common_highlight_phrase(self, preview_text, other_preview)
            if not phrase and kind == "similar_block":
                phrase = _common_highlight_phrase(
                    self,
                    evidence.get("left_text") if file_name == str((occurrence.get("item") or {}).get("left_file_name") or "") else evidence.get("right_text"),
                    evidence.get("right_text") if file_name == str((occurrence.get("item") or {}).get("left_file_name") or "") else evidence.get("left_text"),
                )

        body_html = _highlight_excerpt_html(self, preview_text or phrase, phrase, exact=(mode == "exact"))
        title = "重复命中" if mode == "exact" else f"相似命中（相似度 {float(occurrence.get('similarity') or 0):.2f}）"
        return (
            f"<div class='locator-highlight-block'>"
            f"<div class='locator-highlight-caption'>{html.escape(title)} · 对照 {html.escape(display_other)}</div>"
            f"<div class='locator-highlight-body'>{body_html}</div>"
            f"</div>"
        )

    def _iter_locator_candidates(self, payload, page):
        if not isinstance(payload, dict):
            return []
        candidates = []
        for table in payload.get("native_tables") or []:
            if not isinstance(table, dict):
                continue
            if int(table.get("page") or 0) != int(page or 0):
                continue
            bbox = _normalize_locator_bbox(
                self,
                table.get("block_bbox") or table.get("bbox") or table.get("bbox_ocr"),
            )
            text = _strip_locator_html(self, table.get("block_content") or "")
            if bbox and text:
                candidates.append({"family": "table", "bbox": bbox, "text": text, "score_bias": 0.12})
        for section in payload.get("layout_sections") or []:
            if not isinstance(section, dict):
                continue
            if int(section.get("page") or 0) != int(page or 0):
                continue
            bbox = _normalize_locator_bbox(
                self,
                section.get("bbox") or section.get("bbox_ocr") or section.get("box"),
            )
            text = _strip_locator_html(self, section.get("raw_text") or section.get("text") or "")
            if bbox and text:
                candidates.append(
                    {
                        "family": "table" if str(section.get("type") or "").lower() == "table" else "section",
                        "bbox": bbox,
                        "text": text,
                        "score_bias": 0.0,
                    }
                )
        return candidates

    def _occurrence_match_texts(self, occurrence, file_name):
        item = occurrence.get("item") or {}
        evidence = occurrence.get("evidence") or {}
        family = str(occurrence.get("family") or "block")
        is_left = file_name == str(item.get("left_file_name") or "")
        texts: list[str] = []
        if family == "table":
            row_key = "left_rows" if is_left else "right_rows"
            sample_key = "left_sample_rows" if is_left else "right_sample_rows"
            for value in list(evidence.get(row_key) or []) + list(evidence.get(sample_key) or []) + list(evidence.get("sample_rows") or []):
                if isinstance(value, str) and value.strip():
                    texts.append(value)
        elif family == "section":
            preview_key = "left_preview" if is_left else "right_preview"
            title_key = "left_title" if is_left else "right_title"
            for value in [evidence.get(preview_key), evidence.get(title_key)]:
                if isinstance(value, str) and value.strip():
                    texts.append(value)
        else:
            text_key = "left_text" if is_left else "right_text"
            for value in [evidence.get(text_key), evidence.get("text")]:
                if isinstance(value, str) and value.strip():
                    texts.append(value)
        deduped = []
        for text in texts:
            if text not in deduped:
                deduped.append(text)
        return deduped

    def _locator_candidate_score(self, candidate_text, texts, *, family):
        compact_candidate = _compact_locator_text(self, candidate_text)
        if not compact_candidate:
            return 0.0
        best = 0.0
        direct_hits = 0
        for text in texts:
            compact_text = _compact_locator_text(self, text)
            if not compact_text:
                continue
            if compact_text in compact_candidate:
                direct_hits += 1
                length_bonus = min(len(compact_text), len(compact_candidate)) / max(len(compact_text), len(compact_candidate), 1)
                best = max(best, 1.25 + length_bonus)
                continue
            ratio = difflib.SequenceMatcher(None, compact_text[:600], compact_candidate[:2400]).ratio()
            if ratio > best:
                best = ratio
        if direct_hits:
            return best + min(direct_hits * 0.08, 0.24)
        if family == "table" and best >= 0.48:
            return best
        if family == "section" and best >= 0.56:
            return best
        if family == "block" and best >= 0.62:
            return best
        return 0.0

    def _occurrence_bbox_for_file_page(self, occurrence, file_name, page, *, source_lookup):
        entry = source_lookup.get(file_name) or {}
        payload = entry.get("_payload_data")
        if not isinstance(payload, dict):
            return None
        family = str(occurrence.get("family") or "block")
        texts = _occurrence_match_texts(self, occurrence, file_name)
        if not texts:
            return None
        best_bbox = None
        best_score = 0.0
        for candidate in _iter_locator_candidates(self, payload, page):
            score = _locator_candidate_score(
                self,
                candidate.get("text") or "",
                texts,
                family=family if family in {"table", "section", "block"} else "block",
            )
            if family == "table" and candidate.get("family") == "table":
                score += float(candidate.get("score_bias") or 0)
            elif family != "table" and candidate.get("family") == "section":
                score += 0.03
            if score > best_score:
                best_score = score
                best_bbox = candidate.get("bbox")
        return best_bbox if best_score > 0 else None

    def _split_occurrence_ranges(self, left_pages, right_pages):
        left_ranges = self._coalesce_page_ranges(self._project_normalize_pages(left_pages))
        right_ranges = self._coalesce_page_ranges(self._project_normalize_pages(right_pages))
        if not left_ranges and not right_ranges:
            return [([], [])]
        if not left_ranges:
            return [([], list(range(start, end + 1))) for start, end in right_ranges]
        if not right_ranges:
            return [(list(range(start, end + 1)), []) for start, end in left_ranges]
        if len(left_ranges) == len(right_ranges):
            return [
                (list(range(left_start, left_end + 1)), list(range(right_start, right_end + 1)))
                for (left_start, left_end), (right_start, right_end) in zip(left_ranges, right_ranges)
            ]
        if len(left_ranges) == 1:
            left_start, left_end = left_ranges[0]
            left_span = list(range(left_start, left_end + 1))
            return [
                (left_span, list(range(right_start, right_end + 1)))
                for right_start, right_end in right_ranges
            ]
        if len(right_ranges) == 1:
            right_start, right_end = right_ranges[0]
            right_span = list(range(right_start, right_end + 1))
            return [
                (list(range(left_start, left_end + 1)), right_span)
                for left_start, left_end in left_ranges
            ]
        pair_count = min(len(left_ranges), len(right_ranges))
        return [
            (
                list(range(left_ranges[index][0], left_ranges[index][1] + 1)),
                list(range(right_ranges[index][0], right_ranges[index][1] + 1)),
            )
            for index in range(pair_count)
        ]

    def _explode_evidence_occurrences(self, evidence, kind):
        if not isinstance(evidence, dict):
            return [evidence]
        if kind in {"block", "similar_block"}:
            left_pages = self._project_normalize_pages(evidence.get("left_pages"), evidence.get("left_page"))
            right_pages = self._project_normalize_pages(evidence.get("right_pages"), evidence.get("right_page"))
            if not left_pages:
                left_pages = self._project_normalize_pages(evidence.get("page"))
            if not right_pages:
                right_pages = self._project_normalize_pages(evidence.get("page"))
            pairs = _split_occurrence_ranges(self, left_pages, right_pages)
            exploded = []
            for left_pair, right_pair in pairs:
                entry = dict(evidence)
                entry["page"] = None
                entry["pages"] = None
                entry["left_page"] = left_pair[0] if left_pair else None
                entry["right_page"] = right_pair[0] if right_pair else None
                entry["left_pages"] = left_pair
                entry["right_pages"] = right_pair
                exploded.append(entry)
            return exploded or [evidence]

        left_pages = self._project_normalize_pages(evidence.get("left_pages"))
        right_pages = self._project_normalize_pages(evidence.get("right_pages"))
        pairs = _split_occurrence_ranges(self, left_pages, right_pages)
        exploded = []
        for left_pair, right_pair in pairs:
            entry = dict(evidence)
            entry["left_pages"] = left_pair
            entry["right_pages"] = right_pair
            exploded.append(entry)
        return exploded or [evidence]

    def _cluster_family(self, kind):
        return kind[8:] if kind.startswith("similar_") else kind

    def _cluster_mode(self, kind):
        return "similar" if kind.startswith("similar_") else "exact"

    def _normalize_cluster_token(self, value):
        if value is None:
            return ""
        if isinstance(value, (list, dict)):
            text = json.dumps(value, ensure_ascii=False)
        else:
            text = str(value)
        text = html.unescape(text)
        text = re.sub(r"\d+(?:[\.,]\d+)?", "#", text)
        text = re.sub(r"[，,。；;：:、\[\]\(\){}（）“”‘’\"'`]+", " ", text)
        text = re.sub(r"\s+", " ", text).strip().lower()
        if len(text.replace("#", "").strip()) < 4:
            return ""
        return text

    def _occurrence_tokens(self, kind, evidence):
        candidates = []
        if kind in {"section", "similar_section"}:
            candidates.extend(
                [
                    evidence.get("left_preview"),
                    evidence.get("left_title"),
                    evidence.get("right_preview"),
                    evidence.get("right_title"),
                ]
            )
        elif kind == "block":
            candidates.append(evidence.get("text"))
        elif kind == "similar_block":
            candidates.extend([evidence.get("left_text"), evidence.get("right_text"), evidence.get("text")])
        elif kind == "table":
            sample_rows = evidence.get("sample_rows") or []
            candidates.extend(sample_rows)
            candidates.extend([sample_rows, evidence.get("sample_text"), evidence.get("header_signature")])
        elif kind == "similar_table":
            left_rows = evidence.get("left_sample_rows") or []
            right_rows = evidence.get("right_sample_rows") or []
            candidates.extend(left_rows)
            candidates.extend(right_rows)
            candidates.extend(
                [
                    left_rows,
                    right_rows,
                    evidence.get("sample_rows"),
                    evidence.get("header_signature"),
                ]
            )
        elif kind == "image":
            candidates.extend(
                [
                    evidence.get("image_hash"),
                    [
                        evidence.get("left_width"),
                        evidence.get("left_height"),
                        evidence.get("right_width"),
                        evidence.get("right_height"),
                    ],
                ]
            )
        tokens = []
        for candidate in candidates:
            token = _normalize_cluster_token(self, candidate)
            if token and token not in tokens:
                tokens.append(token)
        return tokens

    def _cluster_rank(self, cluster):
        mode = str(cluster.get("mode") or "similar")
        family = str(cluster.get("family") or "block")
        rank_map = {
            ("exact", "table"): 7,
            ("exact", "section"): 6,
            ("exact", "image"): 5,
            ("exact", "block"): 4,
            ("similar", "table"): 3,
            ("similar", "section"): 2,
            ("similar", "block"): 1,
        }
        return rank_map.get((mode, family), 0)

    def _ranges_cover(self, container_ranges, candidate_ranges, *, tolerance=0):
        if not container_ranges or not candidate_ranges:
            return False
        for candidate_start, candidate_end in candidate_ranges:
            matched = False
            for container_start, container_end in container_ranges:
                if (
                    container_start - tolerance <= candidate_start
                    and container_end + tolerance >= candidate_end
                ):
                    matched = True
                    break
            if not matched:
                return False
        return True

    def _clusters_are_textually_related(self, strong, weak):
        strong_tokens = [token for token in (strong.get("tokens") or []) if token]
        weak_tokens = [token for token in (weak.get("tokens") or []) if token]
        if not strong_tokens or not weak_tokens:
            return False
        if set(strong_tokens) & set(weak_tokens):
            return True
        for strong_token in strong_tokens:
            for weak_token in weak_tokens:
                if len(strong_token) >= len(weak_token):
                    if weak_token and weak_token in strong_token:
                        return True
                elif strong_token and strong_token in weak_token:
                    return True
        return False

    def _clusters_have_nested_ranges(self, strong, weak):
        strong_family = str(strong.get("family") or "block")
        weak_family = str(weak.get("family") or "block")
        common_files = set(strong.get("files") or []) & set(weak.get("files") or [])
        if len(common_files) < 2:
            return False
        tolerance = 1 if strong_family == "section" and weak_family == "block" else 0
        for file_name in common_files:
            strong_ranges = strong.get("doc_ranges_by_file", {}).get(file_name) or []
            weak_ranges = weak.get("doc_ranges_by_file", {}).get(file_name) or []
            if not _ranges_cover(self, strong_ranges, weak_ranges, tolerance=tolerance):
                return False
        return True

    def _occurrence_preview(self, kind, evidence, side):
        if kind in {"section", "similar_section"}:
            if side == "left":
                raw = evidence.get("left_preview") or evidence.get("left_title") or evidence.get("preview") or "-"
            else:
                raw = evidence.get("right_preview") or evidence.get("right_title") or evidence.get("preview") or "-"
            return self._project_trim_text(str(raw), 220)
        if kind == "block":
            return self._project_trim_text(str(evidence.get("text") or "-"), 220)
        if kind == "similar_block":
            raw = evidence.get(f"{side}_text") or evidence.get("text") or "-"
            return self._project_trim_text(str(raw), 220)
        if kind == "table":
            rows = evidence.get("sample_rows") or []
            return self._project_trim_text(json.dumps(rows, ensure_ascii=False), 220) if rows else "-"
        if kind == "similar_table":
            rows = evidence.get(f"{side}_sample_rows") or evidence.get("sample_rows") or []
            return self._project_trim_text(json.dumps(rows, ensure_ascii=False), 220) if rows else "-"
        if kind == "image":
            width = evidence.get(f"{side}_width")
            height = evidence.get(f"{side}_height")
            dimensions = ""
            if isinstance(width, int) and isinstance(height, int) and width > 0 and height > 0:
                dimensions = f"{width}x{height}"
            pages = self._project_normalize_pages(
                evidence.get(f"{side}_pages"),
                evidence.get(f"{side}_page"),
                evidence.get("page"),
            )
            page_label = ""
            if pages:
                coalesced = self._coalesce_page_ranges(pages)
                if coalesced:
                    start_page, end_page = coalesced[0]
                    page_label = f"第{start_page}页" if start_page == end_page else f"第{start_page}-{end_page}页"
            parts = [part for part in (page_label, dimensions, "相同图片") if part]
            return " / ".join(parts) if parts else "相同图片"
        return "-"

    def _build_exact_range_links_html(self, source_lookup, file_name, ranges):
        if not ranges:
            return "<span class='issue-muted'>页码待补充</span>"
        entry = source_lookup.get(file_name) or {}
        source_url = str(entry.get("source_url") or "")
        preview_template = str(entry.get("page_preview_url_template") or "")
        fragments = []
        for start_page, end_page in ranges:
            label = f"P{start_page}" if start_page == end_page else f"P{start_page}-P{end_page}"
            if preview_template:
                fragments.append(
                    self._project_build_locator_button_html(
                        entry=entry,
                        label=label,
                        page=start_page,
                        page_end=end_page,
                    )
                )
            elif source_url:
                href = self._project_append_page_fragment(source_url, start_page)
                fragments.append(
                    f"<a class='issue-link issue-page-link' href='{html.escape(href)}' "
                    f"target='_blank' rel='noreferrer'>{html.escape(label)}</a>"
                )
            else:
                fragments.append(f"<span>{html.escape(label)}</span>")
        return "<span class='issue-page-links'>" + " ".join(fragments) + "</span>"

    def _build_source_doc_cell_exact_html(self, source_lookup, file_name, ranges):
        entry = source_lookup.get(file_name) or {}
        display_name = str(entry.get("display_name") or file_name or "-")
        json_name = str(entry.get("json_name") or file_name or "")
        first_page = ranges[0][0] if ranges else None
        parts = [
            "<div class='issue-doc-cell'>",
            "<div>",
            self._project_build_source_file_link_html(
                source_lookup,
                file_name,
                label=display_name,
                page=first_page,
            ),
            "</div>",
        ]
        if json_name and json_name != display_name:
            parts.append(f"<div class='issue-subtext'>{html.escape(json_name)}</div>")
        parts.append("<div>")
        parts.append(_build_exact_range_links_html(self, source_lookup, file_name, ranges))
        parts.append("</div></div>")
        return "".join(parts)

    def _cluster_items(self, items):
        return DuplicateResultMerger(self).cluster_items(items)

    def _cluster_anchor(self, doc_type, cluster):
        files = cluster.get("files") or []
        parts = [doc_type, cluster.get("family"), cluster.get("mode"), *files]
        for file_name in files:
            for start_page, end_page in cluster.get("doc_ranges_by_file", {}).get(file_name, []) or []:
                parts.append(f"{file_name}:{start_page}-{end_page}")
        for token in cluster.get("tokens") or []:
            parts.append(token[:48])
        return f"duplicate-cluster-{doc_type}-{self._project_make_stable_token(*parts)}"

    def _cluster_metrics(self, cluster):
        return dict(cluster.get("metrics") or {})

    def _cluster_risk(self, cluster):
        return str(cluster.get("risk_level") or "none")

    def _cluster_score(self, cluster):
        return str(cluster.get("score_display") or "0")

    def _cluster_title(self, cluster):
        family = str(cluster.get("family") or "block")
        mode = str(cluster.get("mode") or "exact")
        title_map = {
            ("exact", "section"): "重复段落",
            ("exact", "block"): "重复句子",
            ("exact", "table"): "重复表格",
            ("exact", "image"): "重复图片",
            ("similar", "section"): "相似段落",
            ("similar", "block"): "相似句子",
            ("similar", "table"): "相似表格",
        }
        prefix = title_map.get((mode, family), "重复证据")
        previews_by_file = cluster.get("doc_previews_by_file") or {}
        preview_text = ""
        for file_name in cluster.get("files") or []:
            values = previews_by_file.get(file_name) or []
            if values:
                preview_text = str(values[0]).strip()
                if preview_text:
                    break
        preview_text = self._project_trim_text(preview_text, 42).strip(" -") if preview_text else ""
        return f"{prefix}：{preview_text}" if preview_text else prefix

    def _cluster_doc_list_html(self, cluster, *, source_lookup):
        ranges_by_file = cluster.get("doc_ranges_by_file") or {}
        parts = []
        for file_name in cluster.get("files") or []:
            parts.append(
                _build_source_doc_cell_exact_html(
                    self,
                    source_lookup,
                    file_name,
                    ranges_by_file.get(file_name) or [],
                )
            )
        return "<div class='issue-doc-list'>" + "".join(parts) + "</div>"

    def _cluster_locator_targets(self, cluster, *, source_lookup):
        targets = []
        for file_name in cluster.get("files") or []:
            entry = source_lookup.get(file_name) or {}
            document_key = self._project_locator_document_key(entry, file_name)
            if not document_key:
                continue
            range_map: dict[tuple[int, int], dict[str, Any]] = {}
            for occurrence in cluster.get("occurrences") or []:
                docs = occurrence.get("docs") or {}
                if file_name not in docs:
                    continue
                other_files = [name for name in docs.keys() if name != file_name]
                other_file_name = other_files[0] if other_files else ""
                pages = self._project_normalize_pages((docs.get(file_name) or {}).get("pages"))
                for start_page, end_page in self._coalesce_page_ranges(pages):
                    key = (start_page, end_page)
                    payload = range_map.setdefault(
                        key,
                        {
                            "label": f"P{start_page}" if start_page == end_page else f"P{start_page}-P{end_page}",
                            "page": start_page,
                            "pageEnd": end_page,
                        },
                    )
            ranges = sorted(range_map.values(), key=lambda item: (int(item.get("page") or 0), int(item.get("pageEnd") or 0)))
            normalized_pages = []
            for range_item in ranges:
                normalized_pages.extend(range(int(range_item["page"]), int(range_item["pageEnd"]) + 1))
            normalized_pages = self._project_normalize_pages(normalized_pages)
            if not normalized_pages:
                continue
            for range_item in ranges:
                blocks = list(range_item.pop("highlightBlocks", []) or [])
                range_item["highlightHtml"] = "".join(blocks)
                range_item["highlightTitle"] = "命中内容"
            targets.append(
                {
                    "document": document_key,
                    "title": str(entry.get("display_name") or file_name),
                    "json_name": str(entry.get("json_name") or file_name),
                    "default_page": normalized_pages[0],
                    "pages": normalized_pages,
                    "ranges": ranges,
                }
            )
        return targets

    def _cluster_preview_button(self, cluster, *, source_lookup, label="并排预览"):
        targets = _cluster_locator_targets(self, cluster, source_lookup=source_lookup)
        if len(targets) < 2:
            return "<span class='issue-muted'>暂无预览</span>"
        payload = html.escape(json.dumps(targets, ensure_ascii=False))
        return (
            f"<button type='button' class='issue-link issue-link-button locator-open-group' "
            f"data-label='{html.escape(str(label))}' "
            f"data-targets='{payload}'>"
            f"{html.escape(str(label))}</button>"
        )

    def _render_duplicate_rows(self, result, doc_type, *, source_lookup, issue_pages, current_files=None):
        items = list(self._project_iter_duplicate_items(result, doc_type, current_files=current_files))
        if not items:
            return "<tr><td colspan='8'>未发现相关可疑组</td></tr>"

        page_key = "business_duplicates" if doc_type == DOCUMENT_TYPE_BUSINESS_BID else "technical_duplicates"
        detail_page = issue_pages.get(page_key, "")
        rows = []
        for cluster in self._project_duplicate_cluster_items(items):
            metrics = _cluster_metrics(self, cluster)
            risk_level = _cluster_risk(self, cluster)
            detail_anchor = self._project_duplicate_cluster_anchor(doc_type, cluster)
            detail_href = self._project_issue_page_href(detail_page, detail_anchor)
            rows.append(
                f"<tr class='{self._project_severity_css_class(risk_level)}'>"
                f"<td>{html.escape(risk_level)}</td>"
                f"<td>{html.escape(_cluster_score(self, cluster))}</td>"
                f"<td>{self._project_build_duplicate_cluster_doc_list_html(cluster, source_lookup=source_lookup)}</td>"
                f"<td>{html.escape(self._project_metric_display(metrics, 'exact_section_count', 'similar_section_count'))}</td>"
                f"<td>{html.escape(self._project_metric_display(metrics, 'exact_block_count', 'similar_block_count'))}</td>"
                f"<td>{html.escape(self._project_metric_display(metrics, 'exact_table_count', 'similar_table_count'))}</td>"
                f"<td>{html.escape(self._project_metric_display(metrics, 'exact_image_count', 'similar_image_count'))}</td>"
                f"<td><div class='issue-action-stack'>{self._project_build_issue_detail_link(detail_href, '查看全部证据')}</div></td>"
                "</tr>"
            )
        return "\n".join(rows)

    def _render_cluster_evidence_html(self, cluster, *, source_lookup):
        family = str(cluster.get("family") or "block")
        mode = str(cluster.get("mode") or "exact")
        title_map = {
            ("exact", "section"): "重复段落证据",
            ("exact", "block"): "重复句证据",
            ("exact", "table"): "重复表格证据",
            ("exact", "image"): "重复图片证据",
            ("similar", "section"): "相似段落证据",
            ("similar", "block"): "相似句证据",
            ("similar", "table"): "相似表格证据",
        }
        title = title_map.get((mode, family), "证据")
        previews_by_file = cluster.get("doc_previews_by_file") or {}
        ranges_by_file = cluster.get("doc_ranges_by_file") or {}
        entries = []
        for file_name in cluster.get("files") or []:
            entry = source_lookup.get(file_name) or {}
            display_name = str(entry.get("display_name") or file_name or "-")
            json_name = str(entry.get("json_name") or file_name or "")
            preview_text = " / ".join(previews_by_file.get(file_name) or []) or "-"
            entries.append(
                "<div class='issue-evidence-doc'>"
                f"<div><strong>{html.escape(display_name)}：</strong>"
                f"{_build_exact_range_links_html(self, source_lookup, file_name, ranges_by_file.get(file_name) or [])}</div>"
                + (
                    f"<div class='issue-subtext'>{html.escape(json_name)}</div>"
                    if json_name and json_name != display_name
                    else ""
                )
                + f"<div class='issue-preview'>{html.escape(self._project_trim_text(preview_text, 220))}</div>"
                "</div>"
            )
        similarity_html = ""
        if mode == "similar":
            similarity_html = (
                f"<div class='issue-preview issue-muted'>相似度："
                f"{html.escape(str(cluster.get('similarity') or 0))}</div>"
            )
        preview_button = _cluster_preview_button(self, cluster, source_lookup=source_lookup, label="并排预览")
        return (
            f"<details open><summary>{html.escape(title)}（1）</summary>"
            "<ul class='issue-evidence-list'><li>"
            + "".join(entries)
            + similarity_html
            + f"<div class='issue-action-stack issue-evidence-actions'>{preview_button}</div>"
            + "</li></ul></details>"
        )

    def _build_duplicate_issue_detail_html(self, *, project_identifier, title, result, doc_type, source_lookup):
        items = list(self._project_iter_duplicate_items(result, doc_type))
        if not items:
            body = "<p class='issue-empty'>未发现相关可疑组。</p>"
        else:
            cards = []
            for cluster in self._project_duplicate_cluster_items(items):
                metrics = _cluster_metrics(self, cluster)
                risk_level = _cluster_risk(self, cluster)
                cluster_title = self._project_duplicate_cluster_title(cluster)
                cards.append(
                    f"""
                    <article id="{html.escape(self._project_duplicate_cluster_anchor(doc_type, cluster))}" class="issue-card {self._project_severity_css_class(risk_level)}">
                      <div class="issue-card-header">
                        <div>
                          <h2>{html.escape(cluster_title)}</h2>
                          <p class="issue-meta">风险：{html.escape(risk_level)} | 分数：{html.escape(_cluster_score(self, cluster))} | 涉及文件：{html.escape(str(len(cluster.get('files') or [])))}</p>
                        </div>
                        <div class="issue-card-tools">
                          <div class="issue-metrics">
                            <span>重复段 {html.escape(self._project_metric_display(metrics, 'exact_section_count', 'similar_section_count'))}</span>
                            <span>重复句 {html.escape(self._project_metric_display(metrics, 'exact_block_count', 'similar_block_count'))}</span>
                            <span>重复表 {html.escape(self._project_metric_display(metrics, 'exact_table_count', 'similar_table_count'))}</span>
                            <span>重复图 {html.escape(self._project_metric_display(metrics, 'exact_image_count', 'similar_image_count'))}</span>
                          </div>
                        </div>
                      </div>
                      {_cluster_doc_list_html(self, cluster, source_lookup=source_lookup)}
                      {_render_cluster_evidence_html(self, cluster, source_lookup=source_lookup)}
                    </article>
                    """
                )
            body = "".join(cards)
        return self._project_build_issue_page_shell(
            project_identifier=project_identifier,
            title=title,
            body=body,
            source_lookup=source_lookup,
        )

    visualizer._project_duplicate_cluster_items = MethodType(_cluster_items, visualizer)
    visualizer._project_duplicate_cluster_anchor = MethodType(_cluster_anchor, visualizer)
    visualizer._project_duplicate_cluster_metrics = MethodType(_cluster_metrics, visualizer)
    visualizer._project_duplicate_cluster_risk = MethodType(_cluster_risk, visualizer)
    visualizer._project_duplicate_cluster_score = MethodType(_cluster_score, visualizer)
    visualizer._project_duplicate_cluster_title = MethodType(_cluster_title, visualizer)
    visualizer._project_build_duplicate_cluster_doc_list_html = MethodType(_cluster_doc_list_html, visualizer)
    visualizer._project_render_duplicate_rows = MethodType(_render_duplicate_rows, visualizer)
    visualizer._project_build_duplicate_issue_detail_html = MethodType(_build_duplicate_issue_detail_html, visualizer)
    visualizer._project_locator_bbox_for_occurrence = MethodType(_occurrence_bbox_for_file_page, visualizer)
    visualizer._project_cluster_locator_targets_debug = MethodType(_cluster_locator_targets, visualizer)


def unwrap_document_payload(payload: dict[str, Any]) -> dict[str, Any]:
    data = payload.get("data")
    return data if isinstance(data, dict) else payload


def safe_slug(value: str, fallback: str) -> str:
    text = Path(str(value or "").strip()).stem or fallback
    normalized = re.sub(r"[^\w\u4e00-\u9fff-]+", "_", text, flags=re.UNICODE).strip("_")
    return normalized or fallback


def build_record_slug(record: dict[str, Any], fallback: str) -> str:
    base = safe_slug(str(record.get("file_name") or ""), fallback)
    identifier = str(record.get("identifier_id") or "").strip()
    if not identifier:
        return base
    return f"{base}_{identifier[-6:]}"


def guess_source_kind(record: dict[str, Any]) -> str:
    target = str(record.get("file_name") or record.get("file_url") or "").lower()
    if target.endswith(".pdf"):
        return "pdf"
    if target.endswith((".png", ".jpg", ".jpeg")):
        return "image"
    return "unknown"


def build_source_lookup(
    *,
    records: list[dict[str, Any]],
    api_base_url: str,
) -> dict[str, dict[str, Any]]:
    lookup: dict[str, dict[str, Any]] = {}
    for record in records:
        file_name = str(record.get("file_name") or "").strip()
        if not file_name:
            continue
        payload = unwrap_document_payload(record.get("content") or {})
        identifier_id = str(record.get("identifier_id") or "").strip()
        source_kind = guess_source_kind(record)
        locator_document_key = f"doc_{identifier_id}" if identifier_id else ""
        source_url = (
            f"{api_base_url}/api/postgresql/documents/{identifier_id}/source"
            if identifier_id and source_kind in {"pdf", "image"}
            else ""
        )
        page_preview_url_template = (
            f"{api_base_url}/api/postgresql/documents/{identifier_id}/preview/pages/{{page}}"
            if identifier_id and source_kind in {"pdf", "image"}
            else ""
        )
        lookup[file_name] = {
            "json_name": file_name,
            "display_name": str(payload.get("filename") or file_name),
            "document_identifier": identifier_id,
            "locator_document_key": locator_document_key,
            "source_kind": source_kind,
            "source_url": source_url,
            "page_preview_url_template": page_preview_url_template,
            "_payload_data": payload,
        }
    return lookup


def extend_preview_config_with_project_sources(
    preview_config: dict[str, Any],
    source_lookup: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    documents = dict((preview_config or {}).get("documents") or {})
    for entry in source_lookup.values():
        if not isinstance(entry, dict):
            continue
        document_key = str(entry.get("locator_document_key") or "").strip()
        source_url = str(entry.get("source_url") or "").strip()
        preview_template = str(entry.get("page_preview_url_template") or "").strip()
        if not document_key or not source_url or not preview_template or document_key in documents:
            continue
        documents[document_key] = {
            "title": str(entry.get("display_name") or entry.get("json_name") or document_key),
            "source_kind": str(entry.get("source_kind") or "synthetic"),
            "source_url": source_url,
            "page_preview_url_template": preview_template,
            "pages": {},
        }
    return {"documents": documents}


def build_document_preview_config(
    *,
    bidder_record: dict[str, Any],
    tender_record: dict[str, Any],
    api_base_url: str,
) -> dict[str, Any]:
    def build_entry(record: dict[str, Any], default_title: str) -> dict[str, Any]:
        payload = unwrap_document_payload(record.get("content") or {})
        identifier_id = str(record.get("identifier_id") or "").strip()
        source_kind = guess_source_kind(record)
        source_url = (
            f"{api_base_url}/api/postgresql/documents/{identifier_id}/source"
            if identifier_id and source_kind in {"pdf", "image"}
            else ""
        )
        entry = {
            "title": str(payload.get("filename") or record.get("file_name") or default_title),
            "source_kind": source_kind if source_url else "synthetic",
            "source_url": source_url,
            "pages": {},
        }
        if source_url:
            entry["page_preview_url_template"] = (
                f"{api_base_url}/api/postgresql/documents/{identifier_id}/preview/pages/{{page}}"
            )
        return entry

    return {
        "documents": {
            "bidder": build_entry(bidder_record, "投标文件"),
            "tender": build_entry(tender_record, "招标文件"),
        }
    }


def build_business_infos(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    raw_items: list[dict[str, str]] = []
    for record in records:
        payload = unwrap_document_payload(record.get("content") or {})
        display_name = str(payload.get("filename") or record.get("file_name") or record.get("identifier_id") or "business_bid")
        slug = build_record_slug(record, "business_bid")
        raw_items.append({"display_name": display_name, "slug": slug})

    duplicate_names = {
        item["display_name"]
        for item in raw_items
        if sum(1 for other in raw_items if other["display_name"] == item["display_name"]) > 1
    }

    infos: list[dict[str, Any]] = []
    for item in raw_items:
        display_name = item["display_name"]
        if display_name in duplicate_names:
            display_name = f"{display_name} ({item['slug']})"
        infos.append(
            {
                "name": display_name,
                "filename": item["slug"],
                "url": f"report_{item['slug']}.html",
            }
        )
    return infos


def load_stored_project_results(
    *,
    db_service: PostgreSQLService,
    project_identifier: str,
) -> dict[str, Any]:
    record = db_service.get_project_result(project_identifier)
    if not record:
        return {}
    payload = record.get("result")
    return payload if isinstance(payload, dict) else {}


def recompute_and_persist_project_result(
    *,
    db_service: PostgreSQLService,
    project_identifier: str,
    document_records: list[dict[str, Any]],
    result_key: str,
) -> dict[str, Any]:
    duplicate_service = DuplicateCheckService()
    review_service = BidDocumentReviewService()

    if result_key == "business_bid_duplicate_check":
        result = duplicate_service.check_project_documents(
            project_identifier=project_identifier,
            project={"identifier_id": project_identifier},
            document_records=document_records,
            document_types=[DOCUMENT_TYPE_BUSINESS_BID],
            max_pairs_per_type=50,
        )
    elif result_key == "technical_bid_duplicate_check":
        result = duplicate_service.check_project_documents(
            project_identifier=project_identifier,
            project={"identifier_id": project_identifier},
            document_records=document_records,
            document_types=[DOCUMENT_TYPE_TECHNICAL_BID],
            max_pairs_per_type=50,
        )
    elif result_key == "bid_document_review":
        requested_types = [DOCUMENT_TYPE_BUSINESS_BID]
        if any(record.get("document_type") == DOCUMENT_TYPE_TECHNICAL_BID for record in document_records):
            requested_types.append(DOCUMENT_TYPE_TECHNICAL_BID)
        result = review_service.check_project_documents(
            project_identifier=project_identifier,
            project={"identifier_id": project_identifier},
            document_records=document_records,
            document_types=requested_types,
        )
    elif result_key == UnifiedBusinessReviewService.BUSINESS_RESULT_KEY:
        result = UnifiedBusinessReviewService(db_service=db_service).persist_project_business_review(
            project_identifier=project_identifier,
        )
    else:
        raise ValueError(f"unsupported project result key: {result_key}")

    if result_key != UnifiedBusinessReviewService.BUSINESS_RESULT_KEY:
        db_service.upsert_project_result_item(
            project_identifier_id=project_identifier,
            result_key=result_key,
            result_value=result,
        )
    return result


def resolve_project_results(
    *,
    db_service: PostgreSQLService,
    project_identifier: str,
    document_records: list[dict[str, Any]],
) -> dict[str, Any]:
    stored = load_stored_project_results(
        db_service=db_service,
        project_identifier=project_identifier,
    ) if USE_STORED_PROJECT_RESULTS else {}

    result_bundle = {
        "project_identifier": project_identifier,
        "business_bid_format_review": stored.get(UnifiedBusinessReviewService.BUSINESS_RESULT_KEY) or {},
        "business_duplicate_check": stored.get("business_bid_duplicate_check") or {},
        "technical_duplicate_check": stored.get("technical_bid_duplicate_check") or {},
        "bid_document_review": stored.get("bid_document_review") or {},
    }

    if not RECOMPUTE_MISSING_PROJECT_RESULTS:
        return result_bundle

    recompute_plan = (
        ("business_bid_duplicate_check", "business_duplicate_check"),
        ("technical_bid_duplicate_check", "technical_duplicate_check"),
        ("bid_document_review", "bid_document_review"),
    )
    for storage_key, bundle_key in recompute_plan:
        if result_bundle[bundle_key]:
            continue
        result_bundle[bundle_key] = recompute_and_persist_project_result(
            db_service=db_service,
            project_identifier=project_identifier,
            document_records=document_records,
            result_key=storage_key,
        )

    if not result_bundle["business_bid_format_review"]:
        try:
            result_bundle["business_bid_format_review"] = recompute_and_persist_project_result(
                db_service=db_service,
                project_identifier=project_identifier,
                document_records=document_records,
                result_key=UnifiedBusinessReviewService.BUSINESS_RESULT_KEY,
            )
        except Exception:
            result_bundle["business_bid_format_review"] = {}

    return result_bundle


def generate_business_report(
    *,
    visualizer: ReportVisualizer,
    bidder_record: dict[str, Any],
    tender_record: dict[str, Any],
    output_dir: Path,
    business_infos: list[dict[str, Any]],
    project_results: dict[str, Any],
    source_lookup: dict[str, dict[str, Any]],
    issue_pages: dict[str, str],
) -> None:
    bidder_payload = bidder_record.get("content") or {}
    tender_payload = tender_record.get("content") or {}

    integrity_report = IntegrityChecker().check_integrity(tender_payload, bidder_payload)
    consistency_report = ConsistencyChecker().compare_raw_data(tender_payload, bidder_payload)
    deviation_report = DeviationChecker().check_technical_deviation(tender_payload, bidder_payload)
    pricing_report = ItemizedPricingChecker().check_itemized_logic(bidder_payload, tender_text=tender_payload)

    reason_checker = ReasonablenessChecker()
    reasonableness_report = [
        reason_checker.check_bid_price_against_tender_limit(tender_payload, bidder_payload),
        reason_checker.check_price_compliance(bidder_payload),
    ]
    verification_report = VerificationChecker(None).check_seal_and_date(tender_payload, bidder_payload)

    templates = TemplateExtractor.extract_consistency_templates(tender_payload)
    model_segments = [{"title": item["title"], "text": "\n".join(item["content"])} for item in templates]
    bidder_segments = DocumentProcessor.segment_document(bidder_payload, templates, is_test_file=True)

    bidder_slug = build_record_slug(bidder_record, "business_bid")
    output_html_path = output_dir / f"report_{bidder_slug}.html"
    detail_dir_name = f"details_{bidder_slug}"
    detail_dir_path = output_dir / detail_dir_name

    switcher_info = {
        "current_file": bidder_slug,
        "files": [
            {**item, "active": item["filename"] == bidder_slug}
            for item in business_infos
        ],
    }

    html_report = visualizer.generate_html(
        integrity_report=integrity_report,
        consistency_report=consistency_report,
        test_segments=bidder_segments,
        model_segments=model_segments,
        deviation_report=deviation_report,
        pricing_report=pricing_report,
        reasonableness_report=reasonableness_report,
        verification_report=verification_report,
        file_switcher_info=switcher_info,
        detail_dir=str(detail_dir_path),
        detail_href_prefix=detail_dir_name,
        document_preview_config=extend_preview_config_with_project_sources(
            build_document_preview_config(
                bidder_record=bidder_record,
                tender_record=tender_record,
                api_base_url=API_BASE_URL,
            ),
            source_lookup,
        ),
    )

    html_report = visualizer.inject_project_review_section(
        html_report,
        visualizer.build_project_review_section(
            project_results,
            source_lookup=source_lookup,
            issue_pages=issue_pages,
            current_business_file=str(bidder_record.get("file_name") or ""),
            display_options=PROJECT_REVIEW_DISPLAY_OPTIONS,
        ),
    )
    if PROJECT_REVIEW_DISPLAY_OPTIONS.get("business_duplicates_only_mode"):
        html_report = visualizer.focus_project_review_section(
            html_report,
            section_title="商务标查重",
        )
    output_html_path.write_text(html_report, encoding="utf-8")
    print(f"已生成: {output_html_path}")


def main() -> None:
    if not PROJECT_IDENTIFIER:
        raise SystemExit("请先设置 XTJS_PROJECT_IDENTIFIER")

    db_service = PostgreSQLService()
    payload = db_service.get_project_documents_for_duplicate_check(PROJECT_IDENTIFIER)
    if not payload:
        raise SystemExit(f"项目不存在: {PROJECT_IDENTIFIER}")

    records = list(payload.get("documents") or [])
    if not records:
        raise SystemExit(f"项目下没有可用文档: {PROJECT_IDENTIFIER}")

    business_records = [record for record in records if record.get("document_type") == DOCUMENT_TYPE_BUSINESS_BID]
    technical_records = [record for record in records if record.get("document_type") == DOCUMENT_TYPE_TECHNICAL_BID]
    if not business_records:
        raise SystemExit(f"项目下没有商务标文档: {PROJECT_IDENTIFIER}")

    tender_record = {
        "identifier_id": business_records[0].get("tender_identifier_id"),
        "file_name": business_records[0].get("tender_file_name"),
        "file_url": business_records[0].get("tender_file_url"),
        "content": business_records[0].get("tender_content") or {},
        "document_type": "tender",
    }

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    visualizer = ReportVisualizer()
    patch_visualizer_duplicate_display(visualizer)
    business_infos = build_business_infos(business_records)
    project_results = resolve_project_results(
        db_service=db_service,
        project_identifier=PROJECT_IDENTIFIER,
        document_records=records,
    )
    source_lookup = build_source_lookup(
        records=[tender_record, *business_records, *technical_records],
        api_base_url=API_BASE_URL,
    )
    issue_pages = visualizer.write_project_issue_pages(
        output_dir=OUTPUT_DIR,
        project_identifier=PROJECT_IDENTIFIER,
        project_results=project_results,
        source_lookup=source_lookup,
    )

    summary_html = visualizer.build_project_summary_html(
        project_identifier=PROJECT_IDENTIFIER,
        business_infos=business_infos,
        project_results=project_results,
        source_lookup=source_lookup,
        issue_pages=issue_pages,
        display_options=PROJECT_REVIEW_DISPLAY_OPTIONS,
    )
    summary_path = OUTPUT_DIR / "project_review_summary.html"
    summary_path.write_text(summary_html, encoding="utf-8")
    print(f"已生成: {summary_path}")

    for bidder_record in business_records:
        generate_business_report(
            visualizer=visualizer,
            bidder_record=bidder_record,
            tender_record=tender_record,
            output_dir=OUTPUT_DIR,
            business_infos=business_infos,
            project_results=project_results,
            source_lookup=source_lookup,
            issue_pages=issue_pages,
        )


if __name__ == "__main__":
    main()
