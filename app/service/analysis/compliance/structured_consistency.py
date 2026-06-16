"""Structured template skeleton extraction and deterministic consistency checks."""

from __future__ import annotations

import difflib
import hashlib
import re
from copy import deepcopy
from typing import Any, Iterable

from app.config.settings import settings
from app.service.manual_review.working_copy import MANUAL_EXTRACTIONS_KEY

from .embedding_service import get_embedding_service
from .template_extractor import TemplateExtractor


ENGINE_VERSION = "structured-template-v1"
VALID_STATUSES = {"pass", "missing", "unclear", "skipped"}
DELEGATED_KINDS = {"signature", "seal", "date"}

VARIABLE_LABELS: dict[str, tuple[str, ...]] = {
    "项目名称": ("项目名称", "项目名"),
    "项目编号": ("项目编号", "招标编号", "采购编号", "比选编号"),
    "投标人名称": ("投标人名称", "参选人名称", "供应商名称", "公司名称", "单位名称"),
    "服务期限": ("服务期限", "服务期", "履约期限", "合同期限"),
    "金额小写": ("小写", "投标价格小写", "投标报价小写", "报价小写"),
    "金额大写": ("大写", "投标价格大写", "投标报价大写", "报价大写"),
}
SIGNATURE_MARKERS = ("签字", "签名", "法定代表人", "授权代表")
SEAL_MARKERS = ("盖章", "公章", "印章")
DATE_MARKERS = ("日期", "年月日", "签署日")
OBLIGATION_MARKERS = (
    "应当",
    "必须",
    "不得",
    "承诺",
    "保证",
    "声明",
    "同意",
    "接受",
    "遵守",
    "承担",
    "负责",
    "确认",
    "符合",
)
TABLE_HEADER_MARKERS = (
    "序号",
    "项目名称",
    "投标价格",
    "报价",
    "服务期限",
    "数量",
    "单位",
    "税率",
    "单价",
    "总价",
    "偏离",
    "说明",
    "备注",
)
# 可填写表格的题注词：含这些词（或以“表”结尾的短题注）的段落属于需投标人填写的表格题注，
# 仅校验其在投标附件中是否存在，不对填写内容做“是否被改动”的一致性检查。
FILLABLE_TABLE_TITLE_MARKERS = (
    "情况表",
    "组成表",
    "明细表",
    "报价表",
    "汇总表",
    "一览表",
    "登记表",
    "信息表",
    "人员表",
    "配置表",
)
TABLE_HEADER_ALIASES: dict[str, tuple[str, ...]] = {
    "项目名称": ("项目名称", "项目名"),
    "投标价格": ("投标价格", "投标报价", "报价"),
    "服务期限": ("服务期限", "服务期", "履约期限"),
    "数量": ("数量", "数目"),
    "单位": ("单位", "计量单位"),
    "税率": ("税率", "增值税税率"),
    "单价": ("单价", "含税单价", "不含税单价"),
    "总价": ("总价", "含税总价", "不含税总价"),
    "偏离": ("偏离", "偏差"),
    "说明": ("说明", "响应说明"),
    "备注": ("备注", "注"),
}
PLACEHOLDER_RE = re.compile(
    r"_{2,}"
)
PAGE_NO_RE = re.compile(r"^\s*(?:第\s*)?\d+\s*(?:页|/\s*\d+)?\s*$")
NUMBER_PREFIX_RE = re.compile(
    r"^\s*(?:附件|附表)?\s*(?:\d+(?:[-－]\d+)*|[一二三四五六七八九十]+)[、.．)）]?\s*"
)

TEXT_LAYER_UNDERLINE_TEXT_RE = re.compile(
    r"\$?\s*\\underline\{\s*\\text\{\s*(?P<content>[^{}\n$]{0,500})\s*\}\s*\}\s*\$?"
)
TEXT_LAYER_UNDERLINE_RE = re.compile(
    r"\$?\s*\\underline\{\s*(?P<content>[^{}\n$]{0,500})\s*\}\s*\$?"
)
TEXT_LAYER_TEXT_RE = re.compile(
    r"\\text\{\s*(?P<content>[^{}\n$]{0,500})\s*\}"
)
TEXT_LAYER_DANGLING_PREFIX_RE = re.compile(
    r"\$?\s*\\underline\s*\{\s*(?:\\text\s*\{\s*)?"
)


def strip_text_layer_noise(value: Any) -> str:
    text = str(value or "")
    if not text:
        return ""
    cleaned = text.replace("\u3000", " ").replace("\xa0", " ")
    cleaned = cleaned.replace("\r\n", "\n").replace("\r", "\n")
    previous = None
    while cleaned != previous:
        previous = cleaned
        cleaned = TEXT_LAYER_UNDERLINE_TEXT_RE.sub(
            lambda match: (match.group("content") or "").strip(),
            cleaned,
        )
        cleaned = TEXT_LAYER_UNDERLINE_RE.sub(
            lambda match: (match.group("content") or "").strip(),
            cleaned,
        )
        cleaned = TEXT_LAYER_TEXT_RE.sub(
            lambda match: (match.group("content") or "").strip(),
            cleaned,
        )
    cleaned = TEXT_LAYER_DANGLING_PREFIX_RE.sub("", cleaned)
    cleaned = cleaned.replace("\\underline", "").replace("\\text", "")
    cleaned = cleaned.replace("$", "").replace("{", "").replace("}", "")
    cleaned = cleaned.replace("\\", "")
    cleaned = re.sub(r"[ \t\f\v]+", " ", cleaned)
    cleaned = re.sub(r" *\n *", "\n", cleaned)
    return cleaned.strip()


def normalize_text(value: Any) -> str:
    text = strip_text_layer_noise(value)
    text = PLACEHOLDER_RE.sub("", text)
    return "".join(ch.lower() for ch in text if ch.isalnum() or "\u4e00" <= ch <= "\u9fff")


def normalize_raw_text(value: Any) -> str:
    text = strip_text_layer_noise(value)
    return "".join(ch.lower() for ch in text if ch.isalnum() or "\u4e00" <= ch <= "\u9fff")


def lexical_similarity(left: str, right: str) -> float:
    a = normalize_text(left)
    b = normalize_text(right)
    if not a or not b:
        return 0.0
    if a in b or b in a:
        return min(len(a), len(b)) / max(len(a), len(b))
    return difflib.SequenceMatcher(None, a, b).ratio()


def _data_node(payload: dict[str, Any]) -> dict[str, Any]:
    node = payload.get("data", payload) if isinstance(payload, dict) else {}
    return node if isinstance(node, dict) else {}


def _manual_skeleton_values(payload: dict[str, Any]) -> list[dict[str, Any]]:
    sources = [payload, _data_node(payload)]
    values: list[dict[str, Any]] = []
    seen: set[str] = set()
    for source in sources:
        manual = source.get(MANUAL_EXTRACTIONS_KEY) if isinstance(source, dict) else None
        review = (manual or {}).get("business_bid_format_review") if isinstance(manual, dict) else None
        for item in (review or {}).get("items") or []:
            if not isinstance(item, dict):
                continue
            if str(item.get("bidder_key") or "") != "__tender__":
                continue
            if str(item.get("field_group") or "") != "template_skeleton_item":
                continue
            value = item.get("manual_value")
            if not isinstance(value, dict):
                continue
            item_id = str(value.get("item_id") or "").strip()
            if item_id and item_id not in seen:
                seen.add(item_id)
                values.append(deepcopy(value))
    return values


class StructuredConsistencyEngine:
    """Build a tender skeleton, align it to bid attachments, and apply rules."""

    def __init__(self, checker: Any) -> None:
        self.checker = checker
        self.embedding = get_embedding_service()

    def build_template_skeleton(self, model_json: dict[str, Any]) -> list[dict[str, Any]]:
        templates = TemplateExtractor.extract_consistency_templates(model_json)
        manual_values = {item["item_id"]: item for item in _manual_skeleton_values(model_json)}
        model_attachment_index = self._index_model_attachments(model_json)
        attachments: list[dict[str, Any]] = []
        for template in templates:
            attachment = self._build_attachment_skeleton(template, model_json, model_attachment_index)
            next_items: list[dict[str, Any]] = []
            auto_ids: set[str] = set()
            for item in attachment["items"]:
                auto_ids.add(item["item_id"])
                override = manual_values.get(item["item_id"])
                if override:
                    item.update(self._validated_manual_override(override))
                    item["source"] = "manual"
                next_items.append(item)
            attachment_key = attachment["attachment_key"]
            for item_id, override in manual_values.items():
                if item_id in auto_ids or f"template:{attachment_key}:" not in item_id:
                    continue
                manual_item = self._manual_only_item(override, attachment)
                if manual_item:
                    next_items.append(manual_item)
            attachment["items"] = next_items
            attachments.append(attachment)
        return attachments

    def compare(
        self,
        model_json: dict[str, Any],
        test_json: dict[str, Any],
        integrity_raw: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        skeletons = self.build_template_skeleton(model_json)
        templates = [
            {"title": item["title"], "text": item["reference_text"]}
            for item in skeletons
        ]
        bid_by_no, bid_sections = (
            self.checker._build_attachment_lookup(test_json, templates)
            if templates
            else ({}, [])
        )
        model_status = self.embedding.status()
        results: list[dict[str, Any]] = []

        for skeleton in skeletons:
            title = skeleton["title"]
            integrity_skip = self.checker._integrity_skip_reason_for_title(title, integrity_raw)
            if integrity_skip:
                results.append(self._skipped_segment(skeleton, integrity_skip, model_status))
                continue

            attachment_match = self._match_attachment_with_integrity_fallback(
                skeleton,
                bid_by_no,
                bid_sections,
            )
            matched = attachment_match.get("section")
            if matched is None:
                if skeleton["is_optional"]:
                    results.append(
                        self._skipped_segment(
                            skeleton,
                            {"type": "optional_attachment_not_provided"},
                            model_status,
                            attachment_match=attachment_match,
                        )
                    )
                else:
                    results.append(
                        self._unmatched_segment(skeleton, attachment_match, model_status)
                    )
                continue

            result = self._evaluate_attachment(skeleton, matched, attachment_match)
            result["model_status"] = self.embedding.status()
            results.append(result)
        return results

    def _build_attachment_skeleton(
        self,
        template: dict[str, Any],
        model_json: dict[str, Any],
        model_attachment_index: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        title = str(template.get("title") or "").strip()
        content_lines = self._clean_lines(template.get("content") or [])
        attachment_number = self.checker._verification_checker._attachment_number(title)
        attachment_key = self._attachment_key(attachment_number, title)
        locations = [
            deepcopy(location)
            for location in template.get("locations") or []
            if isinstance(location, dict)
        ]
        # 把模板定位收敛到该附件在招标文件中的“单个准确页”，排除封面/目录/尾页。
        # 复用 verification 的附件切块逻辑（check_pages 已剔除封面/非正文）。
        matched_section, accurate_pages = self._accurate_attachment_pages(
            model_attachment_index or {}, attachment_number, title
        )
        if accurate_pages:
            locations = self._constrain_template_locations(locations, matched_section, accurate_pages)
            allowed_pages = set(accurate_pages)
        else:
            allowed_pages = self._location_pages(locations)
        items: list[dict[str, Any]] = []
        items.append(
            self._make_item(
                attachment_key=attachment_key,
                kind="title",
                label=title,
                reference_text=title,
                required=True,
                locations=locations,
                source="auto",
            )
        )

        self_defined = self.checker._is_self_defined_format_template(
            title, "\n".join(content_lines)
        )
        for paragraph in self._paragraphs(content_lines, title):
            items.extend(
                self._classify_paragraph(
                    paragraph,
                    attachment_key=attachment_key,
                    locations=self._locations_for_text(
                        model_json,
                        paragraph,
                        allowed_pages=allowed_pages,
                    ) or locations,
                    self_defined=self_defined,
                )
            )

        items = self._relax_fillable_table_items(items)

        deduped: list[dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()
        for item in items:
            key = (item["kind"], normalize_text(item["reference_text"]))
            if not key[1] or key in seen:
                continue
            seen.add(key)
            deduped.append(item)
        return {
            "attachment_key": attachment_key,
            "attachment_number": attachment_number,
            "title": title,
            "reference_text": "\n".join(content_lines),
            "template_locations": locations,
            "is_optional": bool(template.get("is_optional")),
            "is_self_defined": self_defined,
            "items": deduped,
        }

    def _is_fillable_table_title(self, text: str) -> bool:
        """判断段落是否为“可填写表格”的题注（如“项目管理机构人员情况表”）。"""
        normalized = normalize_text(text)
        if not normalized:
            return False
        if any(marker in normalized for marker in FILLABLE_TABLE_TITLE_MARKERS):
            return True
        # 以“表”结尾的短题注（避免误伤含“表”字的长固定条款）。
        return len(normalized) <= 30 and normalized.endswith("表")

    def _relax_fillable_table_items(
        self,
        items: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """含可填写表格的附件：把表题注(fixed_clause)与表头(table_header)标为仅存在性校验。

        投标人需在这类表格中填写内容，其文本必然与空白模板不同，因此只确认表格在投标附件
        中存在，不对填写内容做“是否被改动”的一致性检查。其余固定条款保持严格检查不变。
        """
        has_table = any(item.get("kind") == "table_header" for item in items)
        if not has_table:
            return items
        for item in items:
            kind = item.get("kind")
            if kind == "table_header":
                item["fillable_existence"] = True
            elif kind == "fixed_clause" and self._is_fillable_table_title(
                item.get("reference_text") or ""
            ):
                item["fillable_existence"] = True
        return items

    def _index_model_attachments(self, model_json: dict[str, Any]) -> dict[str, dict[str, Any]]:
        """对招标文件按附件切块一次，建立 编号/标题 -> 区段 的索引，用于精确定位附件页。"""
        index: dict[str, dict[str, Any]] = {}
        verification_checker = self.checker._verification_checker
        try:
            sections = verification_checker._attachment_sections(model_json)
        except Exception:
            return index
        for section in sections or []:
            if not isinstance(section, dict):
                continue
            number = section.get("attachment_number")
            if number:
                index.setdefault(f"num:{number}", section)
            title_key = verification_checker._attachment_title_key(str(section.get("title") or ""))
            if title_key:
                index.setdefault(f"title:{title_key}", section)
        return index

    def _accurate_attachment_pages(
        self,
        index: dict[str, Any],
        attachment_number: str | None,
        title: str,
    ) -> tuple[dict[str, Any] | None, list[int]]:
        """在招标文件附件索引中匹配当前附件，返回其区段与有效正文页（check_pages）。"""
        section = None
        if attachment_number:
            section = index.get(f"num:{attachment_number}")
        if section is None:
            title_key = self.checker._verification_checker._attachment_title_key(title or "")
            if title_key:
                section = index.get(f"title:{title_key}")
        if not isinstance(section, dict):
            return None, []
        pages = [
            page
            for page in (section.get("check_pages") or section.get("pages") or [])
            if isinstance(page, int) and page > 0
        ]
        return section, pages

    def _constrain_template_locations(
        self,
        raw_locations: list[dict[str, Any]],
        section: dict[str, Any] | None,
        pages: list[int],
    ) -> list[dict[str, Any]]:
        """把模板定位收敛到附件的“单个准确页”（首个正文页），排除封面/目录/尾页。"""
        if not pages:
            return raw_locations
        primary = pages[0]
        accurate_set = set(pages)
        on_primary = [
            loc for loc in raw_locations
            if isinstance(loc, dict) and self._coerce_page(loc.get("page")) == primary
        ]
        if on_primary:
            return on_primary
        on_attachment = [
            loc for loc in raw_locations
            if isinstance(loc, dict) and self._coerce_page(loc.get("page")) in accurate_set
        ]
        if on_attachment:
            return on_attachment
        # 原始模板定位没有命中真实附件页（例如错误地落在封面），改用附件标题区段的定位。
        section_locs = [
            loc for loc in self.checker._serialize_section_locations(section)
            if self._coerce_page(loc.get("page")) == primary
        ]
        if section_locs:
            return section_locs[:1]
        return [{"page": primary, "document_role": "tender"}]

    def _classify_paragraph(
        self,
        text: str,
        *,
        attachment_key: str,
        locations: list[dict[str, Any]],
        self_defined: bool,
    ) -> list[dict[str, Any]]:
        compact = re.sub(r"\s+", "", text)
        if not compact:
            return []
        if any(marker in compact for marker in SIGNATURE_MARKERS):
            return [self._make_item(attachment_key, "signature", "签字要求", text, True, locations, "auto")]
        if any(marker in compact for marker in SEAL_MARKERS):
            return [self._make_item(attachment_key, "seal", "盖章要求", text, True, locations, "auto")]
        if any(marker in compact for marker in DATE_MARKERS) and len(compact) <= 40:
            return [self._make_item(attachment_key, "date", "日期", text, True, locations, "auto")]

        variable_items = self._variable_items(text, attachment_key, locations)
        header_items = self._table_header_items(text, attachment_key, locations)
        if header_items:
            return variable_items + header_items
        if variable_items and self._looks_like_field_row(text):
            return variable_items
        if self_defined:
            return variable_items

        normalized = normalize_text(text)
        required = any(marker in normalized for marker in OBLIGATION_MARKERS)
        if len(normalized) < 10 and not required:
            return variable_items
        return variable_items + [
            self._make_item(
                attachment_key,
                "fixed_clause",
                self._clause_label(text),
                text,
                required,
                locations,
                "auto",
            )
        ]

    def _variable_items(
        self,
        text: str,
        attachment_key: str,
        locations: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        normalized = normalize_raw_text(text)
        alias_hit_count = sum(
            1
            for aliases in VARIABLE_LABELS.values()
            if any(normalize_raw_text(alias) in normalized for alias in aliases)
        )
        items: list[dict[str, Any]] = []
        for canonical, aliases in VARIABLE_LABELS.items():
            alias = next((value for value in aliases if normalize_text(value) in normalized), None)
            if not alias:
                continue
            explicit_label = bool(
                re.search(
                    rf"{re.escape(alias)}\s*(?:[:：]|_{{2,}})",
                    text,
                )
            )
            compact = re.sub(r"\s+", "", text)
            short_label = len(compact) <= 30 and compact.startswith(alias)
            if not explicit_label and alias_hit_count < 2 and not short_label:
                continue
            items.append(
                self._make_item(
                    attachment_key,
                    "variable_field",
                    canonical,
                    f"{alias}：______",
                    True,
                    locations,
                    "auto",
                )
            )
        return items

    def _table_header_items(
        self,
        text: str,
        attachment_key: str,
        locations: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        normalized = normalize_text(text)
        present = [marker for marker in TABLE_HEADER_MARKERS if normalize_text(marker) in normalized]
        table_like = "|" in text or len(present) >= 3
        if not table_like:
            return []
        return [
            self._make_item(
                attachment_key,
                "table_header",
                marker,
                marker,
                marker != "备注",
                locations,
                "auto",
            )
            for marker in present
        ]

    def _evaluate_attachment(
        self,
        skeleton: dict[str, Any],
        section: dict[str, Any],
        attachment_match: dict[str, Any],
    ) -> dict[str, Any]:
        bid_text = strip_text_layer_noise(section.get("text") or "")
        candidate_texts = self._candidate_texts(section)
        locations = self.checker._serialize_section_locations(section)
        element_results: list[dict[str, Any]] = []
        for item in skeleton["items"]:
            element_results.append(
                self._evaluate_item(
                    item,
                    bid_text=bid_text,
                    candidates=candidate_texts,
                    bid_locations=locations,
                    attachment_match=attachment_match,
                )
            )

        required = [
            item
            for item in element_results
            if item.get("required") and item.get("enabled") and item.get("kind") not in DELEGATED_KINDS
        ]
        if any(item["status"] == "missing" for item in required):
            status = "missing"
        elif any(item["status"] == "unclear" for item in required):
            status = "unclear"
        elif required:
            status = "pass"
        else:
            status = "skipped" if skeleton["is_self_defined"] else "pass"

        missing = [
            str(item.get("label") or item.get("reference_text") or "")
            for item in required
            if item["status"] == "missing"
        ]
        difference_items = [
            {
                "item_id": item["item_id"],
                "type": item["difference_category"],
                "difference_category": item["difference_category"],
                "label": item["label"],
                "template_text": item["reference_text"],
                "bid_text": strip_text_layer_noise(item.get("matched_text") or ""),
                "status": item["status"],
            }
            for item in element_results
            if item.get("difference_category")
        ]
        categories = [str(item["difference_category"]) for item in difference_items]
        category_priority = (
            "fixed_clause_missing",
            "possible_rewrite",
            "alignment_unclear",
            "format_only",
            "allowed_variable",
        )
        primary_category = next(
            (category for category in category_priority if category in categories),
            None,
        )
        return {
            "name": skeleton["title"],
            "status": status,
            "is_passed": status in {"pass", "skipped"},
            "engine_version": ENGINE_VERSION,
            "attachment_match": self._public_attachment_match(attachment_match),
            "element_results": element_results,
            "difference_category": primary_category,
            "missing_anchors": missing,
            "missing_anchor_locations": [
                {
                    "anchor": item["label"],
                    "locations": item.get("template_locations") or [],
                }
                for item in required
                if item["status"] == "missing"
            ],
            "unfilled_fields": [],
            "template_text": skeleton["reference_text"],
            "bid_text": bid_text,
            "difference_items": difference_items,
            "difference_summary": self._difference_summary(status, difference_items),
            "pages": list(section.get("pages") or []),
            "locations": locations,
            "template_attachment_locations": skeleton["template_locations"],
            "template_locations": self._problem_template_locations(element_results)
            or skeleton["template_locations"],
            "tender_highlight_locations": self._problem_template_locations(element_results)
            or skeleton["template_locations"],
            **(
                {"skip_reason": {"type": "self_defined_format"}}
                if status == "skipped"
                else {}
            ),
        }

    def _evaluate_item(
        self,
        item: dict[str, Any],
        *,
        bid_text: str,
        candidates: list[str],
        bid_locations: list[dict[str, Any]],
        attachment_match: dict[str, Any],
    ) -> dict[str, Any]:
        result = {
            "item_id": item["item_id"],
            "kind": item["kind"],
            "label": item["label"],
            "reference_text": item["reference_text"],
            "required": bool(item["required"]),
            "enabled": bool(item["enabled"]),
            "status": "skipped",
            "match_method": "delegated" if item["kind"] in DELEGATED_KINDS else "none",
            "lexical_score": 0.0,
            "embedding_score": None,
            "difference_category": None,
            "template_locations": deepcopy(item.get("source_locations") or []),
            "bid_locations": [],
            "fillable_existence": bool(item.get("fillable_existence")),
        }
        if not item["enabled"] or item["kind"] in DELEGATED_KINDS:
            return result
        if item["kind"] == "title":
            result.update(status="pass", match_method=attachment_match.get("method") or "attachment")
            return result

        reference = item["reference_text"]
        comparison_reference = reference
        comparison_candidates = candidates
        if item["kind"] == "fixed_clause":
            comparison_reference = self.checker._fixed_body_line(reference) or reference
            comparison_candidates = [
                self.checker._fixed_body_line(candidate) or candidate
                for candidate in candidates
            ]
        best_text, best_lexical, second_lexical = self._best_lexical(
            comparison_reference,
            comparison_candidates,
        )
        result["lexical_score"] = round(best_lexical, 4)
        result["matched_text"] = best_text
        if best_text:
            result["bid_locations"] = self._locations_for_candidate(best_text, bid_locations)

        if item["kind"] == "variable_field":
            aliases = VARIABLE_LABELS.get(item["label"], (item["label"],))
            if any(normalize_text(alias) in normalize_text(bid_text) for alias in aliases):
                result.update(
                    status="pass",
                    match_method="lexical",
                    lexical_score=max(best_lexical, 1.0),
                    difference_category="allowed_variable",
                )
                return result
            return self._finish_structural_absence(result, attachment_match, best_lexical)

        if item["kind"] == "table_header":
            aliases = TABLE_HEADER_ALIASES.get(item["label"], (item["label"],))
            if (
                any(normalize_text(alias) in normalize_text(bid_text) for alias in aliases)
                or best_lexical >= settings.CONSISTENCY_PARAGRAPH_MATCH_THRESHOLD
            ):
                result.update(
                    status="pass",
                    match_method="lexical",
                    difference_category=("format_only" if best_lexical < 1.0 else None),
                )
                return result
            return self._semantic_or_absent(
                result,
                reference,
                candidates,
                best_lexical,
                second_lexical,
                attachment_match,
                structural=True,
            )

        slot_spec = self.checker._build_dynamic_slot_spec(reference)
        if slot_spec and self.checker._dynamic_slot_spec_matches(slot_spec, candidates):
            result.update(
                status="pass",
                match_method="deterministic_slot",
                lexical_score=max(best_lexical, settings.CONSISTENCY_TEXT_PASS_THRESHOLD),
                difference_category="allowed_variable",
            )
            return result

        reference_norm = normalize_text(comparison_reference)
        bid_norm = normalize_text(
            self.checker._build_fixed_body(bid_text) or bid_text
        )
        if reference_norm and reference_norm in bid_norm:
            result.update(status="pass", match_method="contains", lexical_score=1.0)
            return result
        if best_lexical >= settings.CONSISTENCY_TEXT_PASS_THRESHOLD:
            result.update(
                status="pass",
                match_method="lexical",
                difference_category="format_only",
            )
            return result
        return self._semantic_or_absent(
            result,
            reference,
            candidates,
            best_lexical,
            second_lexical,
            attachment_match,
            structural=False,
        )

    def _semantic_or_absent(
        self,
        result: dict[str, Any],
        reference: str,
        candidates: list[str],
        best_lexical: float,
        second_lexical: float,
        attachment_match: dict[str, Any],
        *,
        structural: bool,
    ) -> dict[str, Any]:
        # 可填写表格的题注/表头：未在主体命中即视为“存在性无法严格确认”，判跳过而非不通过。
        if result.get("fillable_existence"):
            result.update(status="skipped", difference_category=None)
            return result
        scores = self.embedding.similarities(reference, candidates)
        if scores is not None:
            result_model = self.embedding.status()
            if result_model["status"] == "loaded" and scores:
                order = sorted(range(len(scores)), key=lambda index: scores[index], reverse=True)
                best_index = order[0]
                best_score = scores[best_index]
                second_score = scores[order[1]] if len(order) > 1 else 0.0
                result["embedding_score"] = round(best_score, 4)
                if not result.get("matched_text"):
                    result["matched_text"] = candidates[best_index]
                margin = best_score - second_score
                if (
                    best_score >= settings.CONSISTENCY_PARAGRAPH_MATCH_THRESHOLD
                    and margin >= settings.CONSISTENCY_MATCH_MARGIN
                ):
                    result.update(
                        status="unclear",
                        match_method="embedding",
                        difference_category="alignment_unclear" if structural else "possible_rewrite",
                    )
                    return result
                if best_score >= settings.CONSISTENCY_PARAGRAPH_UNMATCHED_THRESHOLD:
                    result.update(
                        status="unclear",
                        match_method="embedding",
                        difference_category="alignment_unclear",
                    )
                    return result

        if not result["required"]:
            result.update(status="skipped", difference_category=None)
            return result
        deterministic = (
            self._supports_deterministic_attachment_decision(attachment_match)
            and len(normalize_text(reference)) >= 12
            and len(normalize_text("".join(candidates))) >= 24
            and best_lexical < settings.CONSISTENCY_DETERMINISTIC_MISSING_MAX_LEXICAL
        )
        if deterministic:
            result.update(
                status="missing",
                match_method="exhaustive_lexical",
                difference_category="fixed_clause_missing",
            )
        else:
            result.update(
                status="unclear",
                match_method="lexical_fallback",
                difference_category="alignment_unclear",
            )
        return result

    def _finish_structural_absence(
        self,
        result: dict[str, Any],
        attachment_match: dict[str, Any],
        best_lexical: float,
    ) -> dict[str, Any]:
        if not result["required"]:
            result["status"] = "skipped"
        elif self._supports_deterministic_attachment_decision(attachment_match) and best_lexical < 0.35:
            result.update(
                status="missing",
                match_method="exhaustive_lexical",
                difference_category="fixed_clause_missing",
            )
        else:
            result.update(
                status="unclear",
                match_method="lexical_fallback",
                difference_category="alignment_unclear",
            )
        return result

    def _match_attachment(
        self,
        skeleton: dict[str, Any],
        sections: list[dict[str, Any]],
    ) -> dict[str, Any]:
        expected_no = str(skeleton.get("attachment_number") or "")
        if expected_no:
            exact = [
                section
                for section in sections
                if str(section.get("attachment_number") or "") == expected_no
            ]
            if len(exact) == 1:
                return {
                    "section": exact[0],
                    "method": "attachment_number",
                    "score": 1.0,
                    "margin": 1.0,
                    "confidence": "high",
                }
            if len(exact) > 1:
                sections = exact

        title = skeleton["title"]
        scores = [
            lexical_similarity(
                title,
                str(section.get("title") or section.get("text") or "").splitlines()[0],
            )
            for section in sections
        ]
        if scores:
            order = sorted(range(len(scores)), key=lambda index: scores[index], reverse=True)
            best_index = order[0]
            best = scores[best_index]
            second = scores[order[1]] if len(order) > 1 else 0.0
            margin = best - second
            if (
                best >= settings.CONSISTENCY_TITLE_MATCH_THRESHOLD
                and margin >= settings.CONSISTENCY_MATCH_MARGIN
            ):
                return {
                    "section": sections[best_index],
                    "method": "lexical",
                    "score": round(best, 4),
                    "margin": round(margin, 4),
                    "confidence": "high",
                }

        embedding_scores = self.embedding.similarities(
            title,
            [str(section.get("title") or section.get("text") or "").splitlines()[0] for section in sections],
        )
        if embedding_scores:
            order = sorted(
                range(len(embedding_scores)),
                key=lambda index: embedding_scores[index],
                reverse=True,
            )
            best_index = order[0]
            best = embedding_scores[best_index]
            second = embedding_scores[order[1]] if len(order) > 1 else 0.0
            margin = best - second
            if (
                best >= settings.CONSISTENCY_TITLE_MATCH_THRESHOLD
                and margin >= settings.CONSISTENCY_MATCH_MARGIN
            ):
                return {
                    "section": sections[best_index],
                    "method": "embedding",
                    "score": round(best, 4),
                    "margin": round(margin, 4),
                    "confidence": "high",
                }
            if best >= settings.CONSISTENCY_TITLE_UNMATCHED_THRESHOLD:
                return {
                    "section": None,
                    "method": "embedding",
                    "score": round(best, 4),
                    "margin": round(margin, 4),
                    "confidence": "unclear",
                }
        best = max(scores, default=0.0)
        return {
            "section": None,
            "method": "lexical",
            "score": round(best, 4),
            "margin": 0.0,
            "confidence": "unclear" if best >= settings.CONSISTENCY_TITLE_UNMATCHED_THRESHOLD else "low",
        }

    def _match_attachment_with_integrity_fallback(
        self,
        skeleton: dict[str, Any],
        bid_by_no: dict[str, list[dict[str, Any]]],
        sections: list[dict[str, Any]],
    ) -> dict[str, Any]:
        strict_match = self._match_attachment(skeleton, sections)
        if strict_match.get("section") is not None:
            return strict_match
        candidate_match = self._integrity_candidate_attachment_match(
            skeleton,
            bid_by_no,
            sections,
        )
        if candidate_match.get("section") is not None:
            return candidate_match
        return strict_match

    def _integrity_candidate_attachment_match(
        self,
        skeleton: dict[str, Any],
        bid_by_no: dict[str, list[dict[str, Any]]],
        sections: list[dict[str, Any]],
    ) -> dict[str, Any]:
        verifier = self.checker._verification_checker
        expected_title = str(skeleton.get("title") or "")
        expected_number = str(skeleton.get("attachment_number") or "").strip()
        probe = {
            "attachment_number": expected_number or None,
            "title": verifier._attachment_title(expected_title),
        }
        matched = verifier._match_attachment(probe, bid_by_no, sections)
        if not isinstance(matched, dict):
            return {}

        same_number = [
            section
            for section in sections
            if expected_number
            and str(section.get("attachment_number") or "").strip() == expected_number
        ]
        title_candidates = [
            section
            for section in sections
            if verifier._attachment_titles_compatible(
                probe["title"] or expected_title,
                str(section.get("title") or section.get("text") or ""),
            )
        ]
        matched_title = str(matched.get("title") or matched.get("text") or "").splitlines()[0]
        lexical_score = round(lexical_similarity(expected_title, matched_title), 4)
        method = "integrity_fallback"
        margin = 0.0

        if same_number and any(section == matched for section in same_number):
            if len(same_number) == 1:
                method = "integrity_attachment_number"
                return {
                    "section": matched,
                    "method": method,
                    "score": 1.0,
                    "margin": 1.0,
                    "confidence": "candidate",
                }
            method = "integrity_attachment_number_title"
            margin = self._attachment_title_margin(expected_title, same_number)
        elif len(title_candidates) == 1 and title_candidates[0] == matched:
            method = "integrity_unique_title"
            margin = 1.0
        elif any(section == matched for section in title_candidates):
            method = "integrity_title"
            margin = self._attachment_title_margin(expected_title, title_candidates)

        return {
            "section": matched,
            "method": method,
            "score": lexical_score,
            "margin": round(margin, 4),
            "confidence": "candidate",
        }

    def _unmatched_segment(
        self,
        skeleton: dict[str, Any],
        attachment_match: dict[str, Any],
        model_status: dict[str, Any],
    ) -> dict[str, Any]:
        return {
            "name": skeleton["title"],
            "status": "unclear",
            "is_passed": False,
            "engine_version": ENGINE_VERSION,
            "model_status": model_status,
            "attachment_match": self._public_attachment_match(attachment_match),
            "element_results": [],
            "difference_category": "alignment_unclear",
            "missing_anchors": [],
            "unfilled_fields": [],
            "template_text": skeleton["reference_text"],
            "bid_text": "",
            "difference_items": [
                {
                    "type": "alignment_unclear",
                    "difference_category": "alignment_unclear",
                    "label": "未可靠定位对应附件",
                    "template_text": skeleton["title"],
                    "bid_text": "",
                    "status": "unclear",
                }
            ],
            "difference_summary": "未可靠定位投标文件中的对应附件，需要人工复核。",
            "pages": [],
            "locations": [],
            "template_attachment_locations": skeleton["template_locations"],
            "template_locations": skeleton["template_locations"],
            "tender_highlight_locations": skeleton["template_locations"],
        }

    @staticmethod
    def _attachment_title_margin(
        expected_title: str,
        sections: list[dict[str, Any]],
    ) -> float:
        scores = sorted(
            (
                lexical_similarity(
                    expected_title,
                    str(section.get("title") or section.get("text") or "").splitlines()[0],
                )
                for section in sections
            ),
            reverse=True,
        )
        if not scores:
            return 0.0
        return scores[0] - (scores[1] if len(scores) > 1 else 0.0)

    @staticmethod
    def _supports_deterministic_attachment_decision(
        attachment_match: dict[str, Any],
    ) -> bool:
        confidence = str(attachment_match.get("confidence") or "").strip().lower()
        method = str(attachment_match.get("method") or "").strip().lower()
        return confidence == "high" or (
            confidence == "candidate" and method.startswith("integrity_")
        )

    def _skipped_segment(
        self,
        skeleton: dict[str, Any],
        reason: dict[str, Any],
        model_status: dict[str, Any],
        *,
        attachment_match: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return {
            "name": skeleton["title"],
            "status": "skipped",
            "is_passed": True,
            "engine_version": ENGINE_VERSION,
            "model_status": model_status,
            "attachment_match": self._public_attachment_match(attachment_match or {}),
            "element_results": [],
            "difference_category": None,
            "missing_anchors": [],
            "unfilled_fields": [],
            "difference_items": [],
            "difference_summary": "",
            "pages": [],
            "locations": [],
            "template_attachment_locations": skeleton["template_locations"],
            "template_locations": skeleton["template_locations"],
            "tender_highlight_locations": skeleton["template_locations"],
            "skip_reason": reason,
        }

    def _make_item(
        self,
        attachment_key: str,
        kind: str,
        label: str,
        reference_text: str,
        required: bool,
        locations: list[dict[str, Any]],
        source: str,
    ) -> dict[str, Any]:
        identity = normalize_text(label) or normalize_text(reference_text)
        digest = hashlib.sha1(f"{kind}|{identity}".encode("utf-8")).hexdigest()[:10]
        return {
            "item_id": f"template:{attachment_key}:{kind}:{digest}",
            "kind": kind,
            "label": str(label or kind),
            "reference_text": str(reference_text or ""),
            "required": bool(required),
            "enabled": True,
            "confirmation_status": "auto",
            "source": source,
            "source_locations": deepcopy(locations),
            "extraction_confidence": 0.9 if kind in {"title", "variable_field", "table_header"} else 0.75,
        }

    @staticmethod
    def _validated_manual_override(value: dict[str, Any]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        if str(value.get("kind") or "") in {
            "title",
            "fixed_clause",
            "variable_field",
            "table_header",
            "signature",
            "seal",
            "date",
        }:
            result["kind"] = str(value["kind"])
        for key in ("label", "reference_text", "confirmation_status"):
            if key in value:
                result[key] = str(value.get(key) or "")
        for key in ("required", "enabled"):
            if key in value:
                result[key] = bool(value.get(key))
        return result

    def _manual_only_item(
        self,
        value: dict[str, Any],
        attachment: dict[str, Any],
    ) -> dict[str, Any] | None:
        kind = str(value.get("kind") or "")
        reference = str(value.get("reference_text") or "")
        if not kind or not reference:
            return None
        item = self._make_item(
            attachment["attachment_key"],
            kind,
            str(value.get("label") or reference[:30]),
            reference,
            bool(value.get("required", False)),
            attachment["template_locations"],
            "manual",
        )
        item["item_id"] = str(value.get("item_id") or item["item_id"])
        item.update(self._validated_manual_override(value))
        return item

    @staticmethod
    def _attachment_key(number: Any, title: str) -> str:
        if str(number or "").strip():
            return f"attachment-{str(number).strip().replace('－', '-')}"
        digest = hashlib.sha1(normalize_text(title).encode("utf-8")).hexdigest()[:10]
        return f"attachment-{digest}"

    @staticmethod
    def _clean_lines(lines: Iterable[Any]) -> list[str]:
        result: list[str] = []
        seen: set[str] = set()
        for raw in lines:
            for line in str(raw or "").splitlines():
                text = re.sub(r"\s+", " ", line).strip()
                key = normalize_text(text)
                if not text or not key or PAGE_NO_RE.match(text):
                    continue
                if key in seen:
                    continue
                seen.add(key)
                result.append(text)
        return result

    @staticmethod
    def _paragraphs(lines: list[str], title: str) -> list[str]:
        title_key = normalize_text(title)
        paragraphs: list[str] = []
        buffer: list[str] = []
        for line in lines:
            if normalize_text(line) == title_key:
                continue
            if "|" in line or len(line) > 180 or re.match(r"^\s*\d+[、.．)）]", line):
                if buffer:
                    paragraphs.append(" ".join(buffer))
                    buffer = []
                paragraphs.append(line)
                continue
            if line.endswith(("。", "；", ";", "：", ":")):
                buffer.append(line)
                paragraphs.append(" ".join(buffer))
                buffer = []
            else:
                buffer.append(line)
        if buffer:
            paragraphs.append(" ".join(buffer))
        return paragraphs

    @staticmethod
    def _looks_like_field_row(text: str) -> bool:
        compact = re.sub(r"\s+", "", text)
        return (
            len(compact) <= 100
            and ("：" in text or ":" in text or "__" in text)
            and not any(marker in compact for marker in OBLIGATION_MARKERS)
        )

    @staticmethod
    def _clause_label(text: str) -> str:
        value = NUMBER_PREFIX_RE.sub("", str(text or "")).strip()
        return value[:36] + ("…" if len(value) > 36 else "")

    @staticmethod
    def _candidate_texts(section: dict[str, Any]) -> list[str]:
        values: list[str] = []
        for item in section.get("sections") or []:
            if not isinstance(item, dict):
                continue
            text = strip_text_layer_noise(item.get("text") or "")
            text = re.sub(r"\s+", " ", text).strip()
            if text and not PAGE_NO_RE.match(text):
                values.extend(part.strip() for part in text.splitlines() if part.strip())
        if not values:
            values.extend(
                part.strip()
                for part in strip_text_layer_noise(section.get("text") or "").splitlines()
                if part.strip()
            )
        return list(dict.fromkeys(values))

    @staticmethod
    def _best_lexical(reference: str, candidates: list[str]) -> tuple[str, float, float]:
        ranked = sorted(
            ((candidate, lexical_similarity(reference, candidate)) for candidate in candidates),
            key=lambda item: item[1],
            reverse=True,
        )
        if not ranked:
            return "", 0.0, 0.0
        return ranked[0][0], ranked[0][1], ranked[1][1] if len(ranked) > 1 else 0.0

    @staticmethod
    def _locations_for_candidate(
        candidate: str,
        locations: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        key = normalize_text(candidate)
        return [
            deepcopy(location)
            for location in locations
            if key and key in normalize_text(location.get("text"))
        ][:8]

    @staticmethod
    def _locations_for_text(
        payload: dict[str, Any],
        text: str,
        *,
        allowed_pages: set[int] | None = None,
    ) -> list[dict[str, Any]]:
        target = normalize_text(text)
        if not target:
            return []
        locations: list[dict[str, Any]] = []
        for section in _data_node(payload).get("layout_sections") or []:
            if not isinstance(section, dict):
                continue
            page = StructuredConsistencyEngine._coerce_page(section.get("page"))
            if allowed_pages and page not in allowed_pages:
                continue
            section_text = str(section.get("text") or "")
            section_key = normalize_text(section_text)
            if not section_key:
                continue
            if target not in section_key and lexical_similarity(text, section_text) < 0.72:
                continue
            locations.append(
                {
                    "page": section.get("page"),
                    "bbox": section.get("bbox") or section.get("box"),
                    "text": section_text[:240],
                    "type": str(section.get("type") or "text"),
                    "coordinate_system": str(section.get("coordinate_system") or "pdf_point"),
                }
            )
        return locations[:8]

    @staticmethod
    def _coerce_page(value: Any) -> int | None:
        try:
            page = int(float(str(value).strip()))
        except (TypeError, ValueError):
            return None
        return page if page > 0 else None

    @classmethod
    def _location_pages(cls, locations: Iterable[dict[str, Any]]) -> set[int]:
        pages: set[int] = set()
        for location in locations or []:
            if not isinstance(location, dict):
                continue
            page = cls._coerce_page(location.get("page"))
            if page is not None:
                pages.add(page)
        return pages

    @staticmethod
    def _problem_template_locations(
        elements: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []
        seen: set[tuple[Any, ...]] = set()
        for item in elements:
            if item.get("status") not in {"missing", "unclear"}:
                continue
            for location in item.get("template_locations") or []:
                if not isinstance(location, dict):
                    continue
                key = (
                    location.get("page"),
                    tuple(location.get("bbox") or []),
                    location.get("text"),
                )
                if key in seen:
                    continue
                seen.add(key)
                result.append(deepcopy(location))
        return result

    @staticmethod
    def _public_attachment_match(value: dict[str, Any]) -> dict[str, Any]:
        return {
            key: value.get(key)
            for key in ("method", "score", "margin", "confidence")
            if key in value
        }

    @staticmethod
    def _difference_summary(status: str, differences: list[dict[str, Any]]) -> str:
        if status == "pass":
            return "所有必检模板骨架项均已通过。"
        if status == "missing":
            count = sum(1 for item in differences if item.get("status") == "missing")
            return f"发现 {count} 个确定性缺失的必检骨架项。"
        if status == "unclear":
            return "存在疑似改写或对齐不确定项，需要人工复核。"
        return "该附件无需执行固定条款一致性检查。"
