"""Structured template skeleton extraction and deterministic consistency checks."""

from __future__ import annotations

import difflib
import hashlib
import re
from copy import deepcopy
from typing import Any, Iterable

from app.config.settings import settings
from app.service.manual_review.working_copy import MANUAL_EXTRACTIONS_KEY

from ..attachment_synonyms import strip_attachment_title_parenthetical_noise
from .embedding_service import get_embedding_service
from .exact_template import (
    LEGACY_VERSION as LEGACY_EXACT_ENGINE_VERSION,
    V31_VERSION as V31_EXACT_ENGINE_VERSION,
    VERSION as EXACT_ENGINE_VERSION,
    build_pattern,
    build_pattern_v31,
    compare_pattern,
    compare_pattern_v31,
    compare_pattern_legacy,
    fixed_text,
    plain_text,
    with_locations,
)
from .template_extractor import TemplateExtractor
from .underline_projection import project_text, markup_spans
from .template_pdf_evidence import evidence_for


ENGINE_VERSION = EXACT_ENGINE_VERSION
VALID_STATUSES = {"pass", "fail", "unclear", "not_applicable"}
DELEGATED_KINDS: set[str] = set()
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

# 招标文件格式附件里的参考/注解性段落（如中小企业划型标准说明），
# 属于给投标人的填写指引，不要求投标文件复述，不参与缺失判定。
REFERENCE_NOTE_MARKERS = (
    "划型",
    "划分标准",
    "工信部联企业",
    "信部联企业",
    "参照本规定",
    "适用于所有在中国境内",
    "不属于中小企业划型",
    "各行业划型标准",
    "填写上述声明",
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
PAGE_NO_RE = re.compile(
    r"^\s*(?:第\s*)?\d+\s*页(?:\s*共\s*\d+\s*页)?\s*$"
    r"|^\s*\d+\s*/\s*\d+\s*$"
    r"|^\s*\d+\s*$"
)
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
# 承诺书等附件开头的“致：××公司：”抬头：比对时必须材料一致性时过滤，
# 避免招标/投标两侧抬头（含填空的公司名）差异被误判为模板改写。
SALUTATION_RE = re.compile(
    r"(?:^|[\n\r。；;])[\s　]*致[:：]?\s*[^：:\n\r]{0,60}[:：]"
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
    # Salutations and their fixed labels are part of the template.  Declared
    # values inside them are handled by bounded slots, never by deleting the
    # whole line.
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

    @property
    def engine_version(self) -> str:
        configured = str(
            getattr(settings, "CONSISTENCY_TEMPLATE_ENGINE_VERSION", ENGINE_VERSION)
            or ENGINE_VERSION
        ).strip()
        if configured == LEGACY_EXACT_ENGINE_VERSION:
            return LEGACY_EXACT_ENGINE_VERSION
        if configured == V31_EXACT_ENGINE_VERSION:
            return V31_EXACT_ENGINE_VERSION
        return ENGINE_VERSION

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
        prepared_skeletons: list[dict[str, Any]] | None = None,
    ) -> list[dict[str, Any]]:
        skeletons = prepared_skeletons if prepared_skeletons is not None else self.build_template_skeleton(model_json)
        templates = [
            {"title": item["title"], "text": item["reference_text"]}
            for item in skeletons
        ]
        bid_by_no, bid_sections = (
            self.checker._build_attachment_lookup(test_json, templates)
            if templates
            else ({}, [])
        )
        source = test_json.get("_template_source") if isinstance(test_json, dict) else {}
        source_identity = str(
            (source or {}).get("content_checksum")
            or (source or {}).get("sha256")
            or (source or {}).get("identifier_id")
            or "business-bid"
        )
        document_candidates: list[dict[str, Any]] = []
        if self.engine_version == ENGINE_VERSION:
            document_candidates = self._document_candidate_records(
                test_json,
                source_identity=source_identity,
            )
        model_status = self.embedding.status()
        results: list[dict[str, Any]] = []

        for skeleton in skeletons:
            if TemplateExtractor._review_material_excluded(skeleton):
                continue
            if skeleton.get("is_self_defined"):
                results.append(self._skipped_segment(
                    skeleton,
                    {"type": "self_defined_format"},
                    model_status,
                ))
                continue
            title = skeleton["title"]
            integrity_skip = self.checker._integrity_skip_reason_for_title(title, integrity_raw)
            attachment_match = self._match_attachment_with_integrity_fallback(
                skeleton,
                bid_by_no,
                bid_sections,
            )
            matched = attachment_match.get("section")
            if (integrity_skip and not skeleton.get('optionality_conflict')
                    and attachment_match.get('location_status') == 'not_found'):
                results.append(self._skipped_segment(skeleton, integrity_skip, model_status))
                continue
            if matched is None:
                if skeleton.get('optionality_conflict'):
                    result = self._unmatched_segment(skeleton, attachment_match, model_status)
                    result['optionality_conflict'] = True
                    result['optionality_locations'] = skeleton.get('optionality_locations') or []
                    results.append(result)
                    continue
                if skeleton["is_optional"] and attachment_match.get("location_status") == "not_found":
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

            bid_evidence = evidence_for(test_json, list(matched.get('pages') or []))
            matched = dict(matched, _underline_evidence=bid_evidence,
                           _underline_locations=self._underline_locations(test_json, matched.get('sections') or []),
                           _table_headers=(
                               self._logical_table_headers_v31(test_json, set(matched.get('pages') or []))
                               if self.engine_version != ENGINE_VERSION
                               else self._logical_table_headers(test_json, set(matched.get('pages') or []))
                           ),
                           _source_identity=source_identity,
                           _document_candidates=document_candidates,
                           _layout_exclusions=(
                               deepcopy(document_candidates[0].get("layout_exclusions") or [])
                               if document_candidates else []
                           ))
            result = self._evaluate_attachment(skeleton, matched, attachment_match)
            result["model_status"] = self.embedding.status()
            results.append(result)
        for skeleton, result in zip(skeletons, results):
            result['is_optional'] = skeleton['is_optional']
            result['optionality_locations'] = skeleton.get('optionality_locations') or []
            result['applicability_status'] = skeleton.get('applicability_status') or 'required'
            result['condition_text'] = skeleton.get('condition_text') or ''
            result['applicability_locations'] = skeleton.get('applicability_locations') or []
            if skeleton.get('optionality_conflict'):
                result.update(status='unclear', is_passed=False, optionality_conflict=True,
                              difference_summary='招标对该附件的必交与可选声明冲突，需要人工确认。')
        return results

    def _build_attachment_skeleton(
        self,
        template: dict[str, Any],
        model_json: dict[str, Any],
        model_attachment_index: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        title = str(template.get("title") or "").strip()
        content_lines = self._truncate_at_next_attachment_heading(
            self._clean_lines(template.get("content") or []),
            title=title,
        )
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
        scope_ambiguous = bool((matched_section or {}).get("_ambiguous"))
        if scope_ambiguous:
            locations = []
            allowed_pages = set()
        elif accurate_pages:
            locations = self._constrain_template_locations(locations, matched_section, accurate_pages)
            allowed_pages = set(accurate_pages)
        else:
            allowed_pages = self._location_pages(locations)
        raw_template = "\n".join(str(line) for line in template.get("content") or [])
        template_evidence = evidence_for(model_json, list(allowed_pages))
        projection = project_text(raw_template, evidence=template_evidence,
                                  pages=list(allowed_pages), locations=self._underline_locations(model_json, (matched_section or {}).get('sections') or locations), require_physical=True, require_scope=True)
        # Build the authoritative representation from the unprojected tender
        # text.  Projection remains evidence only: it must never erase fixed
        # words merely because they are underlined.
        content_lines = self._truncate_at_next_attachment_heading(
            self._clean_lines(raw_template.splitlines()), title=title
        )
        layout_exclusions: list[dict[str, Any]] = []
        if self.engine_version == ENGINE_VERSION:
            layout_exclusions = self._template_layout_exclusions(model_json)
            excluded_keys = {
                normalize_raw_text(entry.get("text") or "")
                for entry in layout_exclusions
            }
            content_lines = [
                line for line in content_lines
                if normalize_raw_text(line) not in excluded_keys
            ]
        items: list[dict[str, Any]] = []
        items.append(
            self._make_item(
                attachment_key=attachment_key,
                kind="title",
                label=self._fixed_attachment_title(title),
                reference_text=self._fixed_attachment_title(title),
                required=True,
                locations=locations,
                source="auto",
            )
        )

        self_defined = self.checker._is_self_defined_format_template(
            title, "\n".join(content_lines)
        )
        for paragraph_index, paragraph in enumerate(self._paragraphs(content_lines, title)):
            units = (
                self._split_structural_text(paragraph)
                if self.engine_version == ENGINE_VERSION
                else [(paragraph, 0, len(paragraph))]
            )
            for unit_index, (unit_text, unit_start, unit_end) in enumerate(units):
                unit_locations = self._locations_for_text(
                    model_json,
                    unit_text,
                    allowed_pages=allowed_pages,
                ) or locations
                classified = self._classify_paragraph(
                    unit_text,
                    attachment_key=attachment_key,
                    locations=unit_locations,
                    self_defined=self_defined,
                )
                for item in classified:
                    item["template_source_unit"] = {
                        "source_id": hashlib.sha256(
                            f"{attachment_key}|{paragraph_index}|{unit_index}|{unit_text}".encode("utf-8")
                        ).hexdigest()[:20],
                        "paragraph_index": paragraph_index,
                        "unit_index": unit_index,
                        "original_range": {"start": unit_start, "end": unit_end},
                        "locations": deepcopy(unit_locations),
                    }
                items.extend(classified)

        header_pages = self._location_pages(locations)
        if not header_pages and allowed_pages:
            header_pages = {min(allowed_pages)}
        structured_headers = self._logical_table_header_items(
            model_json,
            attachment_key=attachment_key,
            allowed_pages=header_pages,
            fallback_locations=locations,
        )
        if structured_headers:
            items = [item for item in items if item.get("kind") != "table_header"]
            items.extend(structured_headers)

        # Repeated fixed text is retained.  Collapsing it would make full
        # coverage impossible and can hide a deletion in one occurrence.
        deduped: list[dict[str, Any]] = []
        occurrence_by_id: dict[str, int] = {}
        for item in items:
            if not plain_text(item.get("reference_text")):
                continue
            base_id = item["item_id"]
            occurrence_by_id[base_id] = occurrence_by_id.get(base_id, 0) + 1
            if occurrence_by_id[base_id] > 1:
                item["item_id"] = f"{base_id}:occurrence-{occurrence_by_id[base_id]}"
            pattern_builder = build_pattern if self.engine_version == ENGINE_VERSION else build_pattern_v31
            item["template_pattern"] = self._public_pattern(pattern_builder(item["reference_text"]))
            fixed_reference = plain_text(item["reference_text"])
            if (
                self.engine_version == LEGACY_EXACT_ENGINE_VERSION
                and
                item.get("kind") == "fixed_clause"
                and len(fixed_reference) >= 30
                and not fixed_reference.endswith(("。", "；", ";", "！", "!", "？", "?", "：", ":", "）", ")"))
            ):
                item["boundary_status"] = "unclear"
            item["source_evidence_unclear"] = self._locations_touch_unclear_evidence(
                item.get("source_locations") or locations,
                template_evidence,
            )
            item["source_scope_status"] = "ambiguous" if scope_ambiguous else "resolved"
            deduped.append(item)
        return {
            "underline_projection": projection,
            "attachment_key": attachment_key,
            "attachment_number": attachment_number,
            "title": title,
            "reference_text": "\n".join(content_lines),
            "template_locations": locations,
            "template_pages": sorted(allowed_pages),
            "template_scope_status": "ambiguous" if scope_ambiguous else "resolved",
            "is_optional": bool(template.get("is_optional")),
            "optionality_conflict": bool(template.get('optionality_conflict')),
            "optionality_locations": list(template.get('optionality_locations') or []),
            "applicability_status": str(template.get('applicability_status') or 'required'),
            "condition_text": str(template.get('condition_text') or ''),
            "applicability_locations": list(template.get('applicability_locations') or []),
            "is_self_defined": self_defined,
            "items": deduped,
            "layout_exclusions": layout_exclusions,
        }

    @classmethod
    def _truncate_at_next_attachment_heading(
        cls,
        lines: list[str],
        *,
        title: str,
    ) -> list[str]:
        """把模板内容截断到下一个附件标题之前。

        招标文件格式附件（如中小企业声明函）的 OCR 段落可能把下一份附件
        （《投标项目负责人基本情况表》、供应商书面声明）吸进同一模板，
        这些内容不属于当前附件，投标文件无需复述。
        """
        title_norms = {
            normalize_text(strip_attachment_title_parenthetical_noise(part))
            for part in re.split(r"\s+", title)
            if part.strip()
        }
        result: list[str] = []
        for line in lines:
            compact = normalize_text(strip_attachment_title_parenthetical_noise(line))
            # 当前附件自己的标题行（如“7.中小企业声明函（格式）”）不截断、也不生成子项。
            if compact and any(
                (norm and (norm in compact or compact in norm))
                for norm in title_norms
            ):
                continue
            if cls._is_next_attachment_heading(line):
                break
            result.append(line)
        return result

    @staticmethod
    def _is_next_attachment_heading(line: str) -> bool:
        """判断是否为下一份附件的标题行（编号 + 书名号标题，或编号 + （格式）短标题）。"""
        text = str(line or "").strip()
        if not text:
            return False
        if re.match(r"^\s*附件\s*\d+", text):
            return True
        if re.match(r"^\s*\d+\s*[.、)）]\s*《", text):
            return True
        if re.match(r"^\s*\d+\s*[.、)）]\s*.{1,24}[（(]格式[）)]\s*$", text):
            return True
        return False

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
        """在“响应文件格式”区域建立 编号/标题 -> 区段 的索引。"""
        index: dict[str, dict[str, Any]] = {}
        verification_checker = self.checker._verification_checker
        try:
            response_attachments = TemplateExtractor.extract_response_format_attachments(model_json)
        except Exception:
            response_attachments = []

        sections: list[dict[str, Any]] = []
        for attachment in response_attachments or []:
            if not isinstance(attachment, dict):
                continue
            locations = [
                location
                for location in attachment.get("locations") or attachment.get("title_locations") or []
                if isinstance(location, dict)
            ]
            pages = list(
                dict.fromkeys(
                    page
                    for location in locations
                    if isinstance((page := location.get("page")), int) and page > 0
                )
            )
            sections.append(
                {
                    "attachment_number": attachment.get("attachment_number"),
                    "title": attachment.get("title"),
                    "pages": pages,
                    "check_pages": pages,
                    "sections": locations,
                }
            )

        # 兼容没有“响应文件格式”章节的旧模板，保留原有全文附件索引兜底。
        if not sections:
            try:
                sections = verification_checker._attachment_sections(model_json)
            except Exception:
                return index
        for section in sections or []:
            if not isinstance(section, dict):
                continue
            number = section.get("attachment_number")
            if number:
                self._add_attachment_index_entry(index, f"num:{number}", section)
            title_key = verification_checker._attachment_title_key(str(section.get("title") or ""))
            if title_key:
                self._add_attachment_index_entry(index, f"title:{title_key}", section)
        return index

    @staticmethod
    def _add_attachment_index_entry(
        index: dict[str, dict[str, Any]],
        key: str,
        section: dict[str, Any],
    ) -> None:
        existing = index.get(key)
        if existing is None:
            index[key] = section
            return
        candidates = list(existing.get("_candidates") or [existing])
        if section not in candidates:
            candidates.append(section)
        index[key] = {"_ambiguous": True, "_candidates": candidates}

    def _accurate_attachment_pages(
        self,
        index: dict[str, Any],
        attachment_number: str | None,
        title: str,
    ) -> tuple[dict[str, Any] | None, list[int]]:
        """在招标文件附件索引中匹配当前附件，返回其区段与有效正文页（check_pages）。"""
        section = None
        title_key = self.checker._verification_checker._attachment_title_key(title or "")
        if title_key:
            section = index.get(f"title:{title_key}")
        if section is None and attachment_number:
            section = index.get(f"num:{attachment_number}")
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
        if not re.search(r"[A-Za-z\u3400-\u9fff]", compact):
            return []
        # Only an explicitly self-defined/non-applicable attachment is exempt.
        # "注/说明/参考" inside a fixed-format attachment remains fixed text.
        if self_defined:
            return []
        header_items = self._table_header_items(text, attachment_key, locations)
        if header_items:
            return header_items

        # Every body paragraph is classified.  Slots are derived from this
        # tender text by build_pattern; no keyword can skip the paragraph.
        return [
            self._make_item(
                attachment_key,
                "fixed_clause",
                self._clause_label(text),
                text,
                True,
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
            # “根据贵方为（项目名称）项目（项目编号：____）……”中的项目名称
            # 是正文内的可填占位，不是必须原样保留的字段标签；正文骨架会另行校验。
            # 只有明确的“标签：值”或独立短字段才生成变量字段项。
            if not explicit_label and not short_label:
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
        positions = []
        for marker in TABLE_HEADER_MARKERS:
            position = normalized.find(normalize_text(marker))
            if position >= 0:
                positions.append((position, marker))
        present = [marker for _, marker in sorted(positions)]
        table_evidence = any(
            "table" in str(location.get("type") or "").lower()
            or "cell" in str(location.get("type") or "").lower()
            for location in locations
            if isinstance(location, dict)
        )
        table_like = "|" in text or (len(present) >= 3 and table_evidence)
        if not table_like:
            return []
        item = self._make_item(
            attachment_key,
            "table_header",
            "固定表头及列顺序",
            "｜".join(present),
            True,
            locations,
            "auto",
        )
        item["header_cells"] = present
        return [item]

    def _logical_table_header_items(
        self,
        payload: dict[str, Any],
        *,
        attachment_key: str,
        allowed_pages: set[int],
        fallback_locations: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []
        seen: set[str] = set()
        entries = (
            self._logical_table_headers_v31(payload, allowed_pages)
            if self.engine_version != ENGINE_VERSION
            else self._logical_table_headers(payload, allowed_pages)
        )
        for entry in entries:
            if entry.get("kind") != "header":
                continue
            reference = entry["text"]
            if reference in seen:
                continue
            seen.add(reference)
            item = self._make_item(
                attachment_key,
                "table_header",
                "固定表头及列顺序",
                reference,
                True,
                entry.get("locations") or fallback_locations,
                "logical_table",
            )
            item["header_cells"] = entry["headers"]
            result.append(item)
        return result

    @classmethod
    def _logical_table_headers(cls, payload: dict[str, Any], pages: set[int]) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []
        for table_index, table in enumerate(_data_node(payload).get("logical_tables") or []):
            if not isinstance(table, dict):
                continue
            table_pages = {
                int(page) for page in table.get("pages") or []
                if str(page).isdigit() and int(page) > 0
            }
            if pages and not (pages & table_pages):
                continue
            headers = [plain_text(value) for value in table.get("headers") or []]
            headers = [value for value in headers if value]
            rows = cls._logical_table_rows(table)
            synthetic = bool(headers) and all(
                re.fullmatch(r"col_\d+", value, re.I) for value in headers
            )
            if synthetic or not headers:
                recovered = cls._recover_actual_table_header(rows)
                if recovered:
                    headers = recovered
                    synthetic = False
            locations = [
                {
                    "page": page,
                    "type": "table_cell",
                    "coordinate_system": "pdf_point",
                    "table_id": table.get("id") or f"logical-table-{table_index}",
                }
                for page in sorted(table_pages)
            ]
            if headers and not synthetic:
                result.append({
                    "kind": "header",
                    "headers": headers,
                    "text": "｜".join(headers),
                    "locations": locations,
                    "table_structure_status": "resolved",
                })
            # Rows are source evidence, not headers.  This is especially
            # important for label/value forms whose parser keys are col_N.
            for row_index, row in enumerate(rows):
                values = [plain_text(value) for value in row if plain_text(value)]
                if not values:
                    continue
                result.append({
                    "kind": "form_row" if cls._looks_like_label_value_row(values) else "data_row",
                    "headers": [],
                    "text": "｜".join(values),
                    "locations": [dict(location, row_index=row_index) for location in locations],
                    "table_structure_status": (
                        "resolved" if cls._looks_like_label_value_row(values) else "data"
                    ),
                })
        return result

    @staticmethod
    def _logical_table_headers_v31(payload: dict[str, Any], pages: set[int]) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []
        for table in _data_node(payload).get("logical_tables") or []:
            if not isinstance(table, dict):
                continue
            table_pages = {
                int(page) for page in table.get("pages") or []
                if str(page).isdigit() and int(page) > 0
            }
            if pages and not (pages & table_pages):
                continue
            headers = [plain_text(value) for value in table.get("headers") or []]
            headers = [value for value in headers if value]
            if headers:
                result.append({
                    "headers": headers,
                    "text": "｜".join(headers),
                    "locations": [
                        {"page": page, "type": "table_cell", "coordinate_system": "pdf_point"}
                        for page in sorted(table_pages)
                    ],
                })
        return result

    @staticmethod
    def _logical_table_rows(table: dict[str, Any]) -> list[list[str]]:
        raw_rows = table.get("rows") or table.get("body") or table.get("data") or []
        result: list[list[str]] = []
        for row in raw_rows:
            if isinstance(row, dict):
                values = list(row.values())
            elif isinstance(row, (list, tuple)):
                values = list(row)
            else:
                continue
            result.append([
                str(value.get("text") if isinstance(value, dict) and "text" in value else value or "").strip()
                for value in values
            ])
        return result

    @staticmethod
    def _recover_actual_table_header(rows: list[list[str]]) -> list[str]:
        if not rows:
            return []
        first = [plain_text(value) for value in rows[0] if plain_text(value)]
        if len(first) < 2 or any(len(value) > 40 for value in first):
            return []
        hints = (
            *TABLE_HEADER_MARKERS,
            "编号", "名称", "内容", "规格", "型号", "金额", "合计", "功能",
        )
        hits = sum(any(hint in value for hint in hints) for value in first)
        return first if hits >= 2 else []

    @staticmethod
    def _looks_like_label_value_row(values: list[str]) -> bool:
        if not values or len(values) > 4:
            return False
        first = str(values[0] or "").strip()
        return bool(
            first.endswith(("：", ":"))
            or any(label in first for aliases in VARIABLE_LABELS.values() for label in aliases)
        )

    @staticmethod
    def _fixed_attachment_title(title: str) -> str:
        value = re.sub(
            r"^\s*附件\s*\d+(?:\s*[-－—]\s*\d+)*\s*",
            "",
            str(title or ""),
        )
        value = strip_attachment_title_parenthetical_noise(value)
        value = re.sub(r"^[、.．)）]+\s*", "", value)
        return re.sub(r"\s+", " ", value).strip(" ：:；;，,。")

    @staticmethod
    def _underline_locations(payload, locations):
        container = payload.get('data') if isinstance(payload.get('data'), dict) else payload
        unit = container.get('bbox_coordinate_space')
        result = []
        for location in locations:
            if unit not in ('pdf', 'pdf_point', 'pdf_points') and location.get('coordinate_system') not in ('pdf_point', 'pdf_points'):
                continue
            box = location.get('bbox')
            if not isinstance(box, (list, tuple)) or len(box) != 4:
                continue
            box = list(box)
            if location.get('bbox_format') == 'xywh':
                box[2] += box[0]; box[3] += box[1]
            result.append(dict(location, bbox=box))
        return result

    def _evaluate_attachment(
        self,
        skeleton: dict[str, Any],
        section: dict[str, Any],
        attachment_match: dict[str, Any],
    ) -> dict[str, Any]:
        # Bid-side underlining is presentation only and can never create an
        # allowed range.  Keep a projection solely as auditable OCR evidence.
        projection = project_text(
            section.get('text') or '',
            evidence=section.get('_underline_evidence'),
            pages=section.get('pages') or [],
            locations=section.get('_underline_locations') or [],
            require_physical=False,
            require_scope=True,
        )
        bid_text = plain_text(section.get('text') or '')
        legacy_mode = self.engine_version == LEGACY_EXACT_ENGINE_VERSION
        v31_mode = self.engine_version == V31_EXACT_ENGINE_VERSION
        candidate_texts = self._candidate_texts(section) if legacy_mode else []
        candidate_records = (
            []
            if legacy_mode
            else self._candidate_records(
                section,
                source_identity=str(section.get("_source_identity") or "business-bid"),
            )
        )
        if legacy_mode:
            candidate_texts.extend(
                entry["text"] for entry in section.get('_table_headers') or []
            )
            candidate_texts.append(str(section.get('title') or ''))
        else:
            for record in candidate_records:
                record.setdefault("search_scope", "attachment")
            candidate_records.extend(
                self._table_candidate_records(
                    section.get("_table_headers") or [],
                    start_order=len(candidate_records) + 1,
                    source_identity=str(section.get("_source_identity") or "business-bid"),
                )
            )
            if not bid_text:
                bid_text = plain_text("\n".join(
                    record.get("text") or ""
                    for record in candidate_records
                    if int((record.get("source_range") or {}).get("block_count") or 1) == 1
                ))
        locations = self.checker._serialize_section_locations(section)
        element_results: list[dict[str, Any]] = []
        minimum_order = 0
        alignments = (
            self._align_items_v32(skeleton.get("items") or [], candidate_records)
            if not legacy_mode and not v31_mode
            else {}
        )
        for item in skeleton["items"]:
            if legacy_mode:
                item_candidates = (
                    [self._fixed_attachment_title(str(section.get('title') or ''))]
                    if item.get("kind") == "title"
                    else candidate_texts
                )
                evaluated = self._evaluate_item_v3(
                    item,
                    bid_text=bid_text,
                    candidates=item_candidates,
                    bid_locations=locations,
                    attachment_match=attachment_match,
                    evidence=section.get('_underline_evidence') or {},
                )
            elif v31_mode:
                item_candidates = (
                    [self._title_candidate_record(section)]
                    if item.get("kind") == "title"
                    else candidate_records
                )
                evaluated = self._evaluate_item_v31(
                    item,
                    candidates=item_candidates,
                    attachment_match=attachment_match,
                    minimum_order=minimum_order,
                )
                source_range = evaluated.get("source_range") or {}
                if (
                    item.get("kind") != "title"
                    and evaluated.get("status") in {"pass", "fail"}
                    and isinstance(source_range.get("end_order"), int)
                ):
                    minimum_order = max(minimum_order, int(source_range["end_order"]) + 1)
            else:
                if item.get("kind") == "title":
                    title_record = self._title_candidate_record(section)
                    assignment = {
                        "record": title_record,
                        "compared": compare_pattern(
                            build_pattern(item.get("reference_text") or ""),
                            title_record.get("text") or "",
                        ),
                        "score": 1.0,
                        "ambiguous": False,
                    }
                else:
                    assignment = alignments.get(str(item.get("item_id") or ""))
                evaluated = self._evaluate_item_v32(
                    item,
                    assignment=assignment,
                    local_candidates=candidate_records,
                    document_candidates=section.get("_document_candidates") or [],
                    attachment_pages=set(section.get("pages") or []),
                    attachment_match=attachment_match,
                )
            element_results.append(evaluated)

        has_template_table = any(
            item.get("kind") == "table_header" and item.get("enabled")
            for item in skeleton.get("items") or []
        )
        if (
            not legacy_mode
            and (not v31_mode or not has_template_table)
            and self._supports_deterministic_attachment_decision(attachment_match)
        ):
            element_results.extend(
                self._reverse_fixed_insertions(
                    candidate_records,
                    element_results,
                    include_table=not v31_mode,
                )
            )

        source_coverage = {}
        if not legacy_mode and not v31_mode:
            source_coverage = self._source_coverage_v32(candidate_records, element_results)
            existing_required_nonpass = any(
                item.get("required")
                and item.get("enabled")
                and item.get("status") in {"fail", "unclear"}
                for item in element_results
            )
            if source_coverage.get("unresolved") and not existing_required_nonpass:
                element_results.append({
                    "item_id": "source-coverage:unresolved",
                    "kind": "coverage",
                    "label": "附件正文覆盖",
                    "reference_text": "",
                    "required": True,
                    "enabled": True,
                    "status": "unclear",
                    "match_method": "source_coverage_unresolved",
                    "lexical_score": 0.0,
                    "difference_category": "alignment_unclear",
                    "template_locations": [],
                    "bid_locations": [
                        location
                        for entry in source_coverage["unresolved"][:20]
                        for location in entry.get("locations") or []
                    ],
                    "differences": [],
                    "unclear_reasons": [{
                        "code": "correspondence_unresolved",
                        "message": "附件内仍有正文来源范围未归入固定内容、填写区或排版内容。",
                        "basis": "正文覆盖记录存在未分类来源区间",
                        "affected_ranges": [
                            entry.get("source_range") or {}
                            for entry in source_coverage["unresolved"][:20]
                        ],
                    }],
                    "source_range": {},
                    "source_spans": [],
                    "fillable_mapping": [],
                })

        required = [
            item
            for item in element_results
            if item.get("required") and item.get("enabled") and item.get("kind") not in DELEGATED_KINDS
        ]
        if any(item["status"] == "fail" for item in required):
            status = "fail"
        elif any(item["status"] == "unclear" for item in required):
            status = "unclear"
        elif required:
            status = "pass"
        else:
            status = "not_applicable" if skeleton["is_self_defined"] else "unclear"

        reference_projection = skeleton.get('underline_projection') or {}
        body_items = [item for item in required if item.get('kind') != 'title']
        if not body_items and not skeleton["is_self_defined"]:
            status = 'unclear'
        if not plain_text(bid_text):
            status = 'unclear'
        missing = [
            str(item.get("label") or item.get("reference_text") or "")
            for item in required
            if item["status"] == "fail" and item.get("difference_category") == "fixed_content_deleted"
        ]
        difference_items = []
        for item in element_results:
            for difference in item.get("differences") or []:
                difference_items.append({
                    "item_id": item["item_id"],
                    "difference_category": item.get("difference_category"),
                    "label": item["label"],
                    "status": item["status"],
                    **difference,
                })
            if item.get("difference_category") and not item.get("differences"):
                difference_items.append({
                    "item_id": item["item_id"],
                    "type": item["difference_category"],
                    "difference_category": item["difference_category"],
                    "label": item["label"],
                    "template_text": item["reference_text"],
                    "bid_text": plain_text(item.get("matched_text") or ""),
                    "status": item["status"],
                    "template_locations": item.get("template_locations") or [],
                    "bid_locations": item.get("bid_locations") or [],
                })
        categories = [str(item["difference_category"]) for item in difference_items]
        category_priority = (
            "fixed_content_changed",
            "fixed_content_deleted",
            "fixed_content_inserted",
            "fixed_content_moved",
            "alignment_unclear",
            "fillable_range_unclear",
        )
        primary_category = next(
            (category for category in category_priority if category in categories),
            None,
        )
        unclear_reasons = self._collect_unclear_reasons(element_results)
        return {
            "name": skeleton["title"],
            "underline_projection": {"tender": reference_projection, "bid": projection},
            "status": status,
            "is_passed": status in {"pass", "not_applicable"},
            "engine_version": self.engine_version,
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
                if item["status"] in {"fail", "unclear"}
                and item.get("difference_category") == "fixed_content_deleted"
            ],
            "unfilled_fields": [],
            "template_text": skeleton["reference_text"],
            "bid_text": bid_text,
            "difference_items": difference_items,
            "difference_summary": self._difference_summary(status, difference_items),
            "unclear_reasons": unclear_reasons,
            "unresolved_ranges": [
                {
                    "item_id": item.get("item_id"),
                    "label": item.get("label"),
                    "reasons": item.get("unclear_reasons") or [],
                    "template_locations": item.get("template_locations") or [],
                    "bid_locations": item.get("bid_locations") or [],
                    "source_range": item.get("source_range") or {},
                }
                for item in element_results
                if item.get("status") == "unclear"
            ],
            "coverage": {
                "total_fixed_items": len(required),
                "compared_fixed_items": sum(item.get("status") in {"pass", "fail"} for item in required),
                "passed_fixed_items": sum(item.get("status") == "pass" for item in required),
                "failed_fixed_items": sum(item.get("status") == "fail" for item in required),
                "unclear_fixed_items": sum(item.get("status") == "unclear" for item in required),
                "complete": bool(required) and all(item.get("status") in {"pass", "fail"} for item in required),
                "item_status_counts": {
                    key: sum(item.get("status") == key for item in required)
                    for key in ("pass", "fail", "unclear", "not_applicable")
                },
                "source_coverage": source_coverage,
            },
            "pages": list(section.get("pages") or []),
            "locations": locations,
            "template_attachment_locations": skeleton["template_locations"],
            "template_locations": self._problem_template_locations(element_results)
            or skeleton["template_locations"],
            "tender_highlight_locations": self._problem_template_locations(element_results)
            or skeleton["template_locations"],
            **(
                {"skip_reason": {"type": "self_defined_format"}}
                if status == "not_applicable"
                else {}
            ),
        }

    def _align_items_v32(
        self,
        items: list[dict[str, Any]],
        candidates: list[dict[str, Any]],
    ) -> dict[str, dict[str, Any]]:
        """Globally align attachment items to non-overlapping OCR ranges."""
        body = [
            item for item in items
            if item.get("enabled") and item.get("kind") != "title"
        ]
        options_by_item = [self._item_options_v32(item, candidates) for item in body]
        # end_order -> (score, selected options).  A skipped item remains None;
        # unlike v3.1 this does not force later items behind an early guess.
        states: dict[int, tuple[float, list[dict[str, Any] | None]]] = {
            -1: (0.0, [])
        }
        for options in options_by_item:
            next_states: dict[int, tuple[float, list[dict[str, Any] | None]]] = {}
            for previous_end, (score, path) in states.items():
                skipped = (score - 0.35, path + [None])
                existing = next_states.get(previous_end)
                if existing is None or skipped[0] > existing[0]:
                    next_states[previous_end] = skipped
                for option in options:
                    source_range = option["record"].get("source_range") or {}
                    start = int(source_range.get("start_order") or 0)
                    end = int(source_range.get("end_order") or start)
                    if start <= previous_end:
                        continue
                    candidate_state = (score + float(option["quality"]), path + [option])
                    existing = next_states.get(end)
                    if existing is None or candidate_state[0] > existing[0]:
                        next_states[end] = candidate_state
            states = dict(
                sorted(
                    next_states.items(),
                    key=lambda entry: entry[1][0],
                    reverse=True,
                )[:300]
            )
        if not states:
            return {}
        _, path = max(states.values(), key=lambda entry: entry[0])
        selected_starts = [
            int((option["record"].get("source_range") or {}).get("start_order") or 0)
            if option else None
            for option in path
        ]
        selected_ends = [
            int((option["record"].get("source_range") or {}).get("end_order") or 0)
            if option else None
            for option in path
        ]
        result: dict[str, dict[str, Any]] = {}
        for index, (item, selected) in enumerate(zip(body, path)):
            if selected is None:
                continue
            previous_end = max(
                (value for value in selected_ends[:index] if value is not None),
                default=-1,
            )
            next_start = min(
                (value for value in selected_starts[index + 1:] if value is not None),
                default=10**12,
            )
            selected_range = selected["record"].get("source_range") or {}
            selected_position = (
                selected_range.get("start_order"),
                selected_range.get("end_order"),
            )
            selected_source_ids = set(selected["record"].get("source_ids") or [])
            alternatives = []
            for option in options_by_item[index]:
                source_range = option["record"].get("source_range") or {}
                position = (source_range.get("start_order"), source_range.get("end_order"))
                if position == selected_position:
                    continue
                if selected_source_ids.intersection(option["record"].get("source_ids") or []):
                    # Overlapping windows are alternate projections of the
                    # same OCR source, not two document positions.
                    continue
                start = int(source_range.get("start_order") or 0)
                end = int(source_range.get("end_order") or start)
                if not (start > previous_end and end < next_start):
                    continue
                if float(selected["quality"]) - float(option["quality"]) <= float(
                    settings.CONSISTENCY_MATCH_MARGIN
                ):
                    alternatives.append(option)
            result[str(item.get("item_id") or "")] = {
                **selected,
                "score": selected["retrieval_score"],
                "ambiguous": bool(alternatives),
                "alternatives": [
                    deepcopy(option["record"].get("source_range") or {})
                    for option in alternatives[:5]
                ],
            }
        return result

    def _item_options_v32(
        self,
        item: dict[str, Any],
        candidates: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        records = [record for record in candidates if isinstance(record, dict)]
        if item.get("kind") == "table_header":
            table_records = [
                record for record in records
                if record.get("candidate_kind") == "header"
                or any(
                    "table" in str(location.get("type") or "").lower()
                    or "cell" in str(location.get("type") or "").lower()
                    for location in record.get("locations") or []
                )
            ]
            records = self._project_table_candidate_records(table_records)
        pattern = build_pattern(item.get("reference_text") or "")
        options: list[dict[str, Any]] = []
        seen: dict[tuple[int, int], dict[str, Any]] = {}
        for record in records:
            compared = compare_pattern(pattern, record.get("compare_text") or record.get("text") or "")
            comparison_text = str(
                compared.get("comparison_bid_text")
                or record.get("compare_text")
                or record.get("text")
                or ""
            )
            score = lexical_similarity(fixed_text(pattern), comparison_text)
            anchored = self._has_fixed_anchor_v32(fixed_text(pattern), comparison_text)
            if compared.get("status") != "pass" and not anchored and score < 0.65:
                continue
            status_weight = {
                "pass": 5.0,
                "fail": 2.0,
                "unclear": 1.0,
            }.get(str(compared.get("status") or ""), 0.0)
            source_range = record.get("source_range") or {}
            block_count = max(1, int(source_range.get("block_count") or 1))
            quality = status_weight + score - min(0.35, (block_count - 1) * 0.03)
            option = {
                "record": record,
                "compared": compared,
                "retrieval_score": score,
                "quality": quality,
            }
            position = (
                int(source_range.get("start_order") or 0),
                int(source_range.get("end_order") or 0),
            )
            current = seen.get(position)
            if current is None or quality > current["quality"]:
                seen[position] = option
        options = sorted(
            seen.values(),
            key=lambda option: (
                -float(option["quality"]),
                self._record_order_key(option["record"]),
            ),
        )
        return options[:24]

    @staticmethod
    def _has_fixed_anchor_v32(reference: str, candidate: str) -> bool:
        left = plain_text(reference)
        right = plain_text(candidate)
        if not left or not right:
            return False
        labels = re.findall(
            r"(?:[A-Za-z][A-Za-z0-9 ]{1,24}|[\u3400-\u9fff]{2,20})\s*[:：]",
            left,
        )
        if any(normalize_text(label) in normalize_text(right) for label in labels):
            return True
        chunks = [
            value
            for value in re.split(r"[\s，。；：、！？,.!?;:（）()【】\[\]《》]+", left)
            if len(normalize_text(value)) >= 4
        ]
        normalized_right = normalize_text(right)
        return any(normalize_text(value) in normalized_right for value in chunks[:12])

    @staticmethod
    def _fixed_anchor_terms_v32(reference: str) -> list[str]:
        value = plain_text(reference)
        labels = re.findall(
            r"(?:[A-Za-z][A-Za-z0-9 ]{1,24}|[\u3400-\u9fff]{2,20})\s*[:：]",
            value,
        )
        chunks = re.split(r"[\s，。；：、！？,.!?;:（）()【】\[\]《》]+", value)
        terms = [normalize_text(item) for item in [*labels, *chunks]]
        return list(dict.fromkeys(item for item in terms if len(item) >= 4))[:12]

    def _evaluate_item_v32(
        self,
        item: dict[str, Any],
        *,
        assignment: dict[str, Any] | None,
        local_candidates: list[dict[str, Any]],
        document_candidates: list[dict[str, Any]],
        attachment_pages: set[int],
        attachment_match: dict[str, Any],
    ) -> dict[str, Any]:
        result = {
            "item_id": item["item_id"],
            "kind": item["kind"],
            "label": item["label"],
            "reference_text": item["reference_text"],
            "required": bool(item["required"]),
            "enabled": bool(item["enabled"]),
            "status": "not_applicable" if not item["enabled"] else "unclear",
            "match_method": "none",
            "lexical_score": 0.0,
            "embedding_score": None,
            "difference_category": None,
            "template_locations": deepcopy(item.get("source_locations") or []),
            "bid_locations": [],
            "fillable_existence": bool(item.get("fillable_existence")),
            "source": item.get("source"),
            "template_pattern": deepcopy(item.get("template_pattern") or {}),
            "differences": [],
            "unclear_reasons": [],
            "source_range": {},
            "source_spans": [],
            "fillable_mapping": [],
            "fixed_content_coverage": {"status": "unresolved", "complete": False},
            "text_map": [],
        }
        if item.get("source_scope_status") == "ambiguous":
            return self._mark_unclear(
                result,
                code="attachment_scope_unclear",
                message="招标模板中的附件范围无法唯一确定。",
                basis="模板附件索引存在多个同等匹配区段",
            )
        if not item["enabled"]:
            return result
        if assignment is None:
            if item.get("kind") == "table_header" and any(
                record.get("candidate_kind") in {"form_row", "data_row"}
                for record in local_candidates
            ):
                return self._mark_unclear(
                    result,
                    code="table_structure_unclear",
                    message="现有表格单元格可读，但无法确认实际表头及列关系。",
                    basis="解析器只提供数据行或标签/填写值行，未提供可靠实际表头",
                )
            global_match = self._outside_attachment_match_v32(
                item,
                document_candidates,
                attachment_pages,
            )
            if global_match is not None:
                return self._mark_unclear(
                    result,
                    code="attachment_scope_unclear",
                    message="对应文字已在同一文件其他页识别，但现有证据不能确认属于当前附件。",
                    record=global_match["record"],
                    lexical_score=global_match["retrieval_score"],
                    basis="全文索引命中位于当前附件页范围之外",
                    search_scope={
                        "type": "same_document_existing_ocr",
                        "attachment_pages": sorted(attachment_pages),
                        "matched_pages": sorted(self._record_pages(global_match["record"])),
                    },
                )
            nearest = self._nearest_candidate_v32(item, local_candidates)
            issue = self._explicit_text_issue_v32((nearest or {}).get("record"))
            if issue is not None:
                code, message, basis = issue
                return self._mark_unclear(
                    result,
                    code=code,
                    message=message,
                    record=(nearest or {}).get("record"),
                    lexical_score=(nearest or {}).get("retrieval_score"),
                    basis=basis,
                )
            return self._mark_unclear(
                result,
                code="correspondence_unresolved",
                message="现有文字中尚未建立唯一的固定内容对应关系。",
                record=(nearest or {}).get("record"),
                lexical_score=(nearest or {}).get("retrieval_score"),
                basis="候选仅用于排序，未达到固定锚点和有序结构约束",
                search_scope={"type": "matched_attachment", "pages": sorted(attachment_pages)},
            )

        record = assignment["record"]
        compared = assignment["compared"]
        self._attach_candidate(result, record)
        result.update(
            lexical_score=round(float(assignment.get("score") or 0.0), 4),
            matched_text=str(record.get("text") or ""),
            text_map=deepcopy(compared.get("text_map") or []),
            fillable_mapping=self._fillable_mapping_v32(compared),
        )
        if assignment.get("ambiguous"):
            return self._mark_unclear(
                result,
                code="correspondence_ambiguous",
                message="附件内存在多个同等有效的对应位置。",
                record=record,
                lexical_score=assignment.get("score"),
                basis="有序对齐后仍有多个不重叠的等价位置",
                search_scope={"alternative_ranges": assignment.get("alternatives") or []},
            )
        if not self._supports_deterministic_attachment_decision(attachment_match):
            return self._mark_unclear(
                result,
                code="attachment_scope_unclear",
                message="对应附件尚未可靠定位。",
                record=record,
                lexical_score=assignment.get("score"),
                basis="附件标题或完整性证据不足",
            )
        if compared.get("status") == "unclear":
            issues = [str(value) for value in compared.get("issues") or []]
            return self._mark_unclear(
                result,
                code=("fillable_boundary_unclear" if build_pattern(item["reference_text"]).slots else "correspondence_unresolved"),
                message=(
                    "填写区域边界无法由现有模板固定锚点唯一确定。"
                    if build_pattern(item["reference_text"]).slots
                    else "对应文字存在未解决的结构边界。"
                ),
                record=record,
                lexical_score=assignment.get("score"),
                basis="；".join(issues) or "比较单元边界不唯一",
            )
        if compared.get("status") == "pass":
            result.update(
                status="pass",
                match_method="ordered_exact_fixed_text_v32",
                difference_category=None,
                fixed_content_coverage={
                    "status": "compared",
                    "complete": True,
                    "template_codepoints": len(fixed_text(build_pattern(item["reference_text"]))),
                    "source_ids": deepcopy(record.get("source_ids") or []),
                },
            )
            return result

        issue = self._explicit_text_issue_v32(record)
        if issue is not None:
            code, message, basis = issue
            return self._mark_unclear(
                result,
                code=code,
                message=message,
                record=record,
                lexical_score=assignment.get("score"),
                basis=basis,
            )
        differences = with_locations(
            compared.get("differences") or [],
            template_locations=result["template_locations"],
            bid_locations=result.get("bid_locations") or [],
        )
        if not differences:
            return self._mark_unclear(
                result,
                code="correspondence_unresolved",
                message="候选文字不一致，但尚不能还原为可靠的固定内容差异。",
                record=record,
                lexical_score=assignment.get("score"),
                basis="字符差异为空或无法映射回已有来源",
            )
        categories = {difference.get("type") for difference in differences}
        category = (
            "fixed_content_moved" if "move" in categories
            else "fixed_content_changed" if "replace" in categories
            else "fixed_content_inserted" if "insert" in categories
            else "fixed_content_deleted"
        )
        result.update(
            status="fail",
            match_method="ordered_fixed_text_diff_v32",
            difference_category=category,
            differences=differences,
            fixed_content_coverage={
                "status": "compared",
                "complete": True,
                "template_codepoints": len(fixed_text(build_pattern(item["reference_text"]))),
                "source_ids": deepcopy(record.get("source_ids") or []),
            },
        )
        return result

    def _outside_attachment_match_v32(
        self,
        item: dict[str, Any],
        candidates: list[dict[str, Any]],
        attachment_pages: set[int],
    ) -> dict[str, Any] | None:
        outside = [
            record for record in candidates
            if not attachment_pages.intersection(self._record_pages(record))
        ]
        anchors = self._fixed_anchor_terms_v32(item.get("reference_text") or "")
        if anchors:
            outside = [
                record for record in outside
                if any(
                    anchor in str(record.get("search_text") or normalize_text(record.get("text") or ""))
                    for anchor in anchors
                )
            ]
        options = self._item_options_v32(item, outside)
        return options[0] if options else None

    def _nearest_candidate_v32(
        self,
        item: dict[str, Any],
        candidates: list[dict[str, Any]],
    ) -> dict[str, Any] | None:
        pattern = build_pattern(item.get("reference_text") or "")
        ranked: list[dict[str, Any]] = []
        for record in candidates:
            compared = compare_pattern(pattern, record.get("compare_text") or record.get("text") or "")
            score = lexical_similarity(
                fixed_text(pattern),
                str(compared.get("comparison_bid_text") or record.get("text") or ""),
            )
            ranked.append({"record": record, "compared": compared, "retrieval_score": score})
        return max(ranked, key=lambda value: value["retrieval_score"], default=None)

    @staticmethod
    def _record_pages(record: dict[str, Any]) -> set[int]:
        return {
            int(location.get("page"))
            for location in record.get("locations") or []
            if str(location.get("page") or "").isdigit()
        }

    @staticmethod
    def _explicit_text_issue_v32(
        record: dict[str, Any] | None,
    ) -> tuple[str, str, str] | None:
        if not record:
            return None
        evidence = [
            item for item in record.get("text_evidence") or [] if isinstance(item, dict)
        ]
        for item in evidence:
            status = str(item.get("ocr_status") or item.get("recognition_status") or "").lower()
            if status in {"failed", "error", "unavailable", "missing"} or item.get("recognition_error") or item.get("decode_error"):
                return (
                    "source_text_unavailable",
                    "原始文字不可用，无法确认逐字结论。",
                    str(item)[:500],
                )
            if item.get("text_source_conflict") or item.get("native_text_conflict"):
                return (
                    "source_text_conflict",
                    "已有文字来源之间存在明确冲突。",
                    str(item)[:500],
                )
        text = str(record.get("text") or "")
        if "\ufffd" in text or any(ord(char) < 32 and char not in "\n\r\t" for char in text):
            return (
                "source_text_unavailable",
                "原始文字含替代字符或损坏控制字符。",
                "候选文字包含 U+FFFD 或非法控制字符",
            )
        if re.search(r"(?:Ã.|Â.|â€|锟斤拷)", text):
            return (
                "text_reliability_unclear",
                "已有文字出现损坏编码特征，文字可靠性待确认。",
                "候选文字命中明确的损坏编码模式",
            )
        return None

    @staticmethod
    def _fillable_mapping_v32(compared: dict[str, Any]) -> list[dict[str, Any]]:
        ranges = compared.get("fillable_ranges") or []
        if ranges:
            return [deepcopy(value) for value in ranges]
        return [
            {"index": index, "label": f"填写区{index + 1}", "bid_text": value}
            for index, value in enumerate(compared.get("captures") or [])
        ]

    def _evaluate_item_v31(
        self,
        item: dict[str, Any],
        *,
        candidates: list[dict[str, Any]],
        attachment_match: dict[str, Any],
        minimum_order: int,
    ) -> dict[str, Any]:
        result = {
            "item_id": item["item_id"],
            "kind": item["kind"],
            "label": item["label"],
            "reference_text": item["reference_text"],
            "required": bool(item["required"]),
            "enabled": bool(item["enabled"]),
            "status": "not_applicable" if not item["enabled"] else "unclear",
            "match_method": "none",
            "lexical_score": 0.0,
            "embedding_score": None,
            "difference_category": None,
            "template_locations": deepcopy(item.get("source_locations") or []),
            "bid_locations": [],
            "fillable_existence": bool(item.get("fillable_existence")),
            "source": item.get("source"),
            "template_pattern": deepcopy(item.get("template_pattern") or {}),
            "differences": [],
            "unclear_reasons": [],
            "source_range": {},
            "source_spans": [],
            "fillable_mapping": [],
        }
        if item.get("source_scope_status") == "ambiguous":
            return self._mark_unclear(
                result,
                code="template_scope_ambiguous",
                message="招标模板中的附件页范围无法唯一确定。",
            )
        if not item["enabled"]:
            return result

        records = [record for record in candidates if isinstance(record, dict)]
        if item.get("kind") != "title":
            ordered = [
                record
                for record in records
                if int((record.get("source_range") or {}).get("start_order") or 0)
                >= minimum_order
            ]
            if ordered:
                records = ordered
        if item.get("kind") == "table_header":
            records = self._project_table_candidate_records(records)

        pattern = build_pattern_v31(item["reference_text"])
        exact_matches: list[tuple[dict[str, Any], dict[str, Any]]] = []
        pattern_unclear: list[tuple[dict[str, Any], dict[str, Any]]] = []
        for record in records:
            compared = compare_pattern_v31(pattern, record.get("text") or "")
            if compared["status"] == "pass":
                exact_matches.append((record, compared))
            elif compared["status"] == "unclear":
                pattern_unclear.append((record, compared))

        exact_matches = self._minimal_exact_matches(exact_matches)
        if exact_matches:
            first_start = int(
                (exact_matches[0][0].get("source_range") or {}).get("start_order") or 0
            )
            first_matches = [
                entry
                for entry in exact_matches
                if int((entry[0].get("source_range") or {}).get("start_order") or 0)
                == first_start
            ]
            if len(first_matches) > 1:
                return self._mark_unclear(
                    result,
                    code="alignment_ambiguous",
                    message="同一位置存在多个无法消歧的固定内容候选。",
                    record=first_matches[0][0],
                )
            record, compared = first_matches[0]
            self._attach_candidate(result, record)
            result.update(
                status="pass",
                match_method="ordered_exact_fixed_text",
                lexical_score=1.0,
                matched_text=record.get("text") or "",
                fillable_values=compared.get("captures") or [],
                fillable_mapping=self._fillable_mapping(
                    item.get("template_pattern") or {},
                    compared.get("captures") or [],
                ),
                difference_category=None,
            )
            return result

        if pattern_unclear:
            record = self._ordered_record([entry[0] for entry in pattern_unclear])
            return self._mark_unclear(
                result,
                code="fillable_boundary_unclear",
                message="填写区域在现有文字中有多个可能边界。",
                record=record,
            )

        ranked = sorted(
            (
                (record, lexical_similarity(item["reference_text"], str(record.get("text") or "")))
                for record in records
            ),
            key=lambda entry: (-entry[1], self._record_order_key(entry[0])),
        )
        if not ranked or ranked[0][1] < 0.82:
            return self._mark_unclear(
                result,
                code="source_text_missing",
                message="已有 OCR 中未找到足够完整的对应固定内容。",
                record=ranked[0][0] if ranked else None,
                lexical_score=ranked[0][1] if ranked else 0.0,
            )

        best_score = ranked[0][1]
        near = [entry for entry in ranked if best_score - entry[1] < settings.CONSISTENCY_MATCH_MARGIN]
        best_record = self._ordered_record([entry[0] for entry in near])
        best_text = str((best_record or {}).get("text") or "")
        result["lexical_score"] = round(best_score, 4)
        if best_record:
            self._attach_candidate(result, best_record)
            result["matched_text"] = best_text
        positions = {
            (
                (entry[0].get("source_range") or {}).get("start_order"),
                (entry[0].get("source_range") or {}).get("end_order"),
            )
            for entry in near
        }
        if len(positions) > 1 and not self._positions_resolved_by_order(near, minimum_order):
            return self._mark_unclear(
                result,
                code="alignment_ambiguous",
                message="已有 OCR 中存在多个同等可能的对应位置。",
                record=best_record,
                lexical_score=best_score,
            )
        if self._looks_ocr_uncertain(item["reference_text"], best_text):
            return self._mark_unclear(
                result,
                code="source_text_conflict",
                message="已有 OCR 文字包含异常字符，无法确认逐字差异。",
                record=best_record,
                lexical_score=best_score,
            )
        if not self._supports_deterministic_attachment_decision(attachment_match):
            return self._mark_unclear(
                result,
                code="attachment_alignment_unclear",
                message="对应附件尚未可靠定位。",
                record=best_record,
                lexical_score=best_score,
            )

        compared = compare_pattern_v31(pattern, best_text)
        if compared.get("status") == "unclear":
            return self._mark_unclear(
                result,
                code="fillable_boundary_unclear",
                message="填写区域边界无法由现有模板证据唯一确定。",
                record=best_record,
                lexical_score=best_score,
            )
        raw_differences = compared.get("differences") or []
        if self._looks_like_displaced_fill(item["reference_text"], raw_differences):
            return self._mark_unclear(
                result,
                code="fillable_boundary_unclear",
                message="签字填写内容与固定标签的 OCR 阅读顺序发生交叉。",
                record=best_record,
                lexical_score=best_score,
            )
        if self._looks_like_unmarked_fill(pattern, raw_differences):
            return self._mark_unclear(
                result,
                code="fillable_boundary_unclear",
                message="已有模板文字显示为项目名称填写位置，但填写线边界不完整。",
                record=best_record,
                lexical_score=best_score,
            )
        if self._looks_like_fragmented_ocr(item["reference_text"], best_text, raw_differences):
            return self._mark_unclear(
                result,
                code="source_text_conflict",
                message="已有 OCR 在该固定段落中出现多处碎片化缺字或异常前缀。",
                record=best_record,
                lexical_score=best_score,
            )
        differences = with_locations(
            raw_differences,
            template_locations=result["template_locations"],
            bid_locations=result.get("bid_locations") or [],
        )
        if not differences:
            return self._mark_unclear(
                result,
                code="fixed_difference_unresolved",
                message="候选文字未完全一致，但无法从已有文字中可靠分离固定内容差异。",
                record=best_record,
                lexical_score=best_score,
            )
        categories = {difference.get("type") for difference in differences}
        category = (
            "fixed_content_moved" if "move" in categories
            else "fixed_content_changed" if "replace" in categories
            else "fixed_content_inserted" if "insert" in categories
            else "fixed_content_deleted"
        )
        result.update(
            status="fail",
            match_method="ordered_fixed_text_diff",
            difference_category=category,
            differences=differences,
            fillable_mapping=self._fillable_mapping(
                item.get("template_pattern") or {},
                compared.get("captures") or [],
            ),
        )
        return result

    @staticmethod
    def _looks_like_displaced_fill(
        reference: str,
        differences: list[dict[str, Any]],
    ) -> bool:
        if not differences or not any(marker in str(reference or "")[:40] for marker in SIGNATURE_MARKERS):
            return False
        for difference in differences:
            value = str(difference.get("bid_text") or "").strip()
            location = difference.get("template_range") or {}
            if (
                difference.get("type") != "insert"
                or int(location.get("start", -1)) != 0
                or len(value) > 20
                or any(marker in value for marker in OBLIGATION_MARKERS)
            ):
                return False
        return True

    @staticmethod
    def _looks_like_unmarked_fill(
        pattern: Any,
        differences: list[dict[str, Any]],
    ) -> bool:
        if not differences or any(difference.get("type") != "insert" for difference in differences):
            return False
        fixed = fixed_text(pattern)
        markers = ("参与项目", "参加项目", "为项目")
        for difference in differences:
            value = str(difference.get("bid_text") or "").strip()
            offset = int((difference.get("template_range") or {}).get("start", -1))
            context = fixed[max(0, offset - 12):offset + 12]
            if (
                not value
                or len(value) > 50
                or any(marker in value for marker in OBLIGATION_MARKERS)
                or not any(marker in context for marker in markers)
            ):
                return False
        return True

    @staticmethod
    def _looks_like_fragmented_ocr(
        reference: str,
        candidate: str,
        differences: list[dict[str, Any]],
    ) -> bool:
        if not differences:
            return False
        ref = plain_text(reference)
        bid = plain_text(candidate)
        if (
            re.match(r"^\s*\d+[.．、)]", ref)
            and re.match(r"^\s*\d+[A-Za-z]", bid)
        ):
            return True
        if len(ref) < 80 or len(differences) < 3:
            return False
        small_losses = 0
        for difference in differences:
            if difference.get("type") not in {"delete", "replace"}:
                continue
            left = str(difference.get("template_text") or "")
            right = str(difference.get("bid_text") or "")
            if len(left) <= 4 and len(right) <= 4:
                small_losses += 1
        return small_losses >= 3

    def _evaluate_item_v3(
        self,
        item: dict[str, Any],
        *,
        bid_text: str,
        candidates: list[str],
        bid_locations: list[dict[str, Any]],
        attachment_match: dict[str, Any],
        evidence: dict[str, Any],
    ) -> dict[str, Any]:
        result = {
            "item_id": item["item_id"],
            "kind": item["kind"],
            "label": item["label"],
            "reference_text": item["reference_text"],
            "required": bool(item["required"]),
            "enabled": bool(item["enabled"]),
            "status": "not_applicable" if not item["enabled"] else "unclear",
            "match_method": "none",
            "lexical_score": 0.0,
            "embedding_score": None,
            "difference_category": None,
            "template_locations": deepcopy(item.get("source_locations") or []),
            "bid_locations": [],
            "fillable_existence": bool(item.get("fillable_existence")),
            "source": item.get("source"),
            "template_pattern": deepcopy(item.get("template_pattern") or {}),
            "differences": [],
            "boundary_status": item.get("boundary_status"),
        }
        if item.get("source_scope_status") == "ambiguous":
            result.update(
                status="unclear",
                match_method="template_scope_ambiguous",
                difference_category="alignment_unclear",
            )
            return result
        if not item["enabled"]:
            return result

        reference = item["reference_text"]
        comparison_reference = reference
        comparison_candidates = list(candidates)
        if item["kind"] == "table_header":
            table_candidates = (
                [candidate for candidate in candidates if "｜" in candidate]
                if item.get("source") == "logical_table"
                else candidates
            )
            comparison_candidates = self._table_header_candidates(table_candidates)
        pattern = build_pattern_v31(comparison_reference)

        exact_matches = []
        unclear_matches = []
        for candidate in comparison_candidates:
            compared = compare_pattern_legacy(pattern, candidate)
            if compared["status"] == "pass":
                exact_matches.append((candidate, compared))
            elif compared["status"] == "unclear":
                unclear_matches.append((candidate, compared))
        if len(exact_matches) == 1:
            best_text, compared = exact_matches[0]
            bid_locs = self._locations_for_candidate(best_text, bid_locations)
            result.update(
                status="pass",
                match_method="exact_fixed_text",
                lexical_score=1.0,
                matched_text=best_text,
                bid_locations=bid_locs,
                fillable_values=compared.get("captures") or [],
                difference_category=None,
            )
            return result
        if len(exact_matches) > 1:
            result.update(
                status="unclear",
                match_method="ambiguous_exact_alignment",
                matched_text=exact_matches[0][0],
                difference_category="fillable_range_unclear",
            )
            return result

        if item.get("boundary_status") == "unclear":
            result.update(
                status="unclear",
                match_method="template_boundary_unclear",
                difference_category="alignment_unclear",
            )
            return result

        best_text, best_lexical, second_lexical = self._best_lexical(
            comparison_reference,
            comparison_candidates,
        )
        result["lexical_score"] = round(best_lexical, 4)
        result["matched_text"] = best_text
        if best_text:
            result["bid_locations"] = self._locations_for_candidate(best_text, bid_locations)
        if unclear_matches or (best_text and best_lexical > 0 and abs(best_lexical - second_lexical) < settings.CONSISTENCY_MATCH_MARGIN):
            result.update(
                status="unclear",
                match_method="ambiguous_alignment",
                difference_category="alignment_unclear",
            )
            return result
        if best_lexical < 0.82:
            result.update(
                status="unclear",
                match_method="ocr_or_alignment_unclear",
                difference_category="alignment_unclear",
            )
            return result
        if self._looks_ocr_uncertain(reference, best_text):
            result.update(
                status="unclear",
                match_method="local_ocr_required",
                difference_category="alignment_unclear",
            )
            return result
        if not best_text or not self._supports_deterministic_attachment_decision(attachment_match):
            result.update(
                status="unclear",
                match_method="candidate_unresolved",
                difference_category="alignment_unclear",
            )
            return result

        compared = compare_pattern_legacy(pattern, best_text)
        bid_locs = result.get("bid_locations") or []
        if not bid_locs or item.get("source_evidence_unclear") or self._locations_touch_unclear_evidence(
            bid_locs or bid_locations,
            evidence,
        ):
            result.update(
                status="unclear",
                match_method="local_ocr_required",
                difference_category="alignment_unclear",
            )
            return result
        differences = with_locations(
            compared.get("differences") or [],
            template_locations=result["template_locations"],
            bid_locations=bid_locs,
        )
        categories = {item.get("type") for item in differences}
        if "move" in categories:
            category = "fixed_content_moved"
        elif "replace" in categories:
            category = "fixed_content_changed"
        elif "insert" in categories:
            category = "fixed_content_inserted"
        else:
            category = "fixed_content_deleted"
        result.update(
            status="fail",
            match_method="exact_fixed_text",
            difference_category=category,
            differences=differences,
        )
        return result

    @staticmethod
    def _locations_touch_unclear_evidence(
        locations: list[dict[str, Any]],
        evidence: dict[str, Any],
    ) -> bool:
        pages = evidence.get("pages") if isinstance(evidence, dict) else {}
        if not isinstance(pages, dict):
            return False
        location_pages = {
            int(location.get("page"))
            for location in locations or []
            if isinstance(location, dict) and str(location.get("page") or "").isdigit()
        }
        for page in location_pages:
            entry = pages.get(str(page), pages.get(page))
            if isinstance(entry, dict) and str(entry.get("status") or "ready") != "ready":
                return True
        return False

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

    def _match_attachment(self, skeleton: dict[str, Any], sections: list[dict[str, Any]]) -> dict[str, Any]:
        return self.checker._verification_checker._resolve_attachment(skeleton, sections)

    def resolve_attachment_scope(
        self,
        skeleton: dict[str, Any],
        sections: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """Resolve one expected form to the exact body used by every downstream check.

        The verification parser deliberately retains the full section for audit and
        signature evidence.  Template comparison and PDF underline evidence must use
        the bounded ``check_*`` view, otherwise the last detected form can extend to
        the end of a several-hundred-page bid document.
        """
        resolved = dict(self._match_attachment(skeleton, sections))
        section = resolved.get("section")
        if not isinstance(section, dict):
            resolved["evidence_pages"] = []
            return resolved
        scoped = self.checker._verification_checker._scoped_bid_section_for_checks(section)
        if not isinstance(scoped, dict):
            resolved.update(section=None, location_status="ambiguous", evidence_pages=[])
            return resolved
        pages = [
            int(page)
            for page in (scoped.get("check_pages") or scoped.get("pages") or [])
            if isinstance(page, int) and page > 0
        ]
        resolved["section"] = scoped
        resolved["evidence_pages"] = list(dict.fromkeys(pages))
        return resolved

    def _match_attachment_with_integrity_fallback(
        self,
        skeleton: dict[str, Any],
        bid_by_no: dict[str, list[dict[str, Any]]],
        sections: list[dict[str, Any]],
    ) -> dict[str, Any]:
        # Location is shared with signature/date checks. Body differences are
        # evaluated below and must not erase a reliably located form.
        return self.resolve_attachment_scope(skeleton, sections)

    def _section_title_score(self, expected_title: str, section: dict[str, Any]) -> float:
        """比较附件开头连续标题，兼容“章节标题 + 正式表名”的两层结构。"""
        candidates = [str(section.get("title") or "").strip()]
        for item in (section.get("sections") or [])[:6]:
            if not isinstance(item, dict):
                continue
            text = str(item.get("text") or "").strip()
            if not text:
                continue
            if str(item.get("type") or "").strip().lower() != "heading":
                break
            candidates.append(text)
        verifier = self.checker._verification_checker
        expected_core = verifier._attachment_title(expected_title)
        expected_key = verifier._attachment_title_key(expected_title)
        scores = []
        for candidate in candidates:
            if not candidate:
                continue
            scores.append(lexical_similarity(expected_title, candidate))
            scores.append(
                lexical_similarity(expected_core, verifier._attachment_title(candidate))
            )
            scores.append(
                lexical_similarity(expected_key, verifier._attachment_title_key(candidate))
            )
        return max(
            scores,
            default=0.0,
        )

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
            "engine_version": self.engine_version,
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
            "location_status": attachment_match.get('location_status', 'not_found'),
            "location_candidates": attachment_match.get('candidates', []),
            "pages": [],
            "locations": [],
            "template_attachment_locations": skeleton["template_locations"],
            "template_locations": skeleton["template_locations"],
            "tender_highlight_locations": skeleton["template_locations"],
        }

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
            "status": "not_applicable",
            "is_passed": True,
            "engine_version": self.engine_version,
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
    def _public_pattern(pattern: Any) -> dict[str, Any]:
        return {
            "display_text": pattern.display_text,
            "fixed_segments": list(pattern.fixed_segments),
            "slots": [
                {
                    "index": slot.index,
                    "label": slot.label,
                    "source": slot.source,
                    "range": {"start": slot.start, "end": slot.end},
                }
                for slot in pattern.slots
            ],
            "issues": list(pattern.issues),
        }

    @staticmethod
    def _validated_manual_override(value: dict[str, Any]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        if str(value.get("kind") or "") in {
            "title",
            "fixed_clause",
            "table_header",
        }:
            result["kind"] = str(value["kind"])
        for key in ("label", "reference_text", "confirmation_status"):
            if key in value:
                result[key] = str(value.get(key) or "")
        # Historical "variable" edits cannot exempt fixed tender text.  They
        # remain in the audit record but do not expand an allowed range.
        for key in ("enabled",):
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
        for raw in lines:
            for line in str(raw or "").splitlines():
                text = re.sub(r"\s+", " ", line).strip()
                key = normalize_text(text)
                if not text or not key or PAGE_NO_RE.match(text):
                    continue
                if len(key) <= 32 and re.search(r"(?:招标|比选|采购)文件$", key):
                    continue
                result.append(text)
        return result

    @classmethod
    def _template_layout_exclusions(cls, payload: dict[str, Any]) -> list[dict[str, Any]]:
        sections = [
            item for item in _data_node(payload).get("layout_sections") or []
            if isinstance(item, dict)
        ]
        repeated = cls._repeated_page_edge_texts(sections)
        result: list[dict[str, Any]] = []
        seen: set[tuple[Any, ...]] = set()
        for section in sections:
            text = str(section.get("text") or "").strip()
            kind = str(
                section.get("document_role")
                or section.get("semantic_type")
                or section.get("type")
                or ""
            ).lower()
            reason = None
            if re.sub(r"\s+", "", text) in repeated:
                reason = "repeated_page_edge"
            elif any(value in kind for value in ("header", "footer", "页眉", "页脚")):
                reason = "explicit_page_margin"
            if not text or reason is None:
                continue
            key = (
                text,
                cls._coerce_page(section.get("page")),
                tuple(section.get("bbox") or section.get("box") or []),
            )
            if key in seen:
                continue
            seen.add(key)
            result.append({
                "text": text,
                "page": cls._coerce_page(section.get("page")),
                "bbox": deepcopy(section.get("bbox") or section.get("box")),
                "reason": reason,
            })
        return result

    @staticmethod
    def _paragraphs(lines: list[str], title: str) -> list[str]:
        title_key = normalize_text(title)
        paragraphs: list[str] = []
        buffer: list[str] = []
        for line in lines:
            if normalize_text(line) == title_key:
                continue
            if re.fullmatch(r"\s*(?:\(?\d+(?:[-－]\d+)*\)?[.．、)）]?)\s*", line):
                buffer.append(line)
                continue
            if "|" in line:
                if buffer:
                    paragraphs.append(" ".join(buffer))
                    buffer = []
                paragraphs.append(line)
                continue
            if re.match(r"^\s*\d+[、.．)）]", line):
                if buffer:
                    paragraphs.append(" ".join(buffer))
                buffer = [line]
                if line.endswith(("。", "；", ";")):
                    paragraphs.append(" ".join(buffer))
                    buffer = []
                continue
            if line.endswith(("。", "；", ";")):
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

    @classmethod
    def _candidate_records(
        cls,
        section: dict[str, Any],
        *,
        source_identity: str,
    ) -> list[dict[str, Any]]:
        blocks: list[dict[str, Any]] = []
        order = 0
        excluded_texts = {
            normalize_raw_text(item.get("text") or "")
            for item in section.get("_layout_exclusions") or []
            if isinstance(item, dict) and normalize_raw_text(item.get("text") or "")
        }
        raw_sections = [item for item in section.get("sections") or [] if isinstance(item, dict)]
        if not raw_sections:
            raw_sections = [
                {"text": text, "page": None, "type": "text"}
                for text in strip_text_layer_noise(section.get("text") or "").splitlines()
                if text.strip()
            ]
        for source_index, source in enumerate(raw_sections):
            raw_text = strip_text_layer_noise(source.get("text") or "")
            if normalize_raw_text(raw_text) in excluded_texts:
                continue
            if cls._is_repeated_attachment_heading(
                raw_text,
                str(section.get("title") or ""),
                str(source.get("type") or ""),
            ):
                continue
            raw_text = cls._strip_repeated_attachment_title(
                raw_text,
                str(section.get("title") or ""),
            )
            parts = cls._split_structural_text(raw_text)
            for part_index, (text, original_start, original_end) in enumerate(parts):
                cleaned = re.sub(r"\s+", " ", text).strip()
                if not cleaned or PAGE_NO_RE.match(cleaned):
                    continue
                page = cls._coerce_page(source.get("page"))
                supplied_id = str(
                    source.get("block_id")
                    or source.get("section_id")
                    or source.get("id")
                    or ""
                ).strip()
                base_identity = supplied_id or hashlib.sha256(
                    (
                        f"{source_identity}|{page}|{source_index}|"
                        f"{raw_text}"
                    ).encode("utf-8")
                ).hexdigest()[:20]
                identity = (
                    f"{base_identity}:{part_index}"
                    if len(parts) > 1
                    else base_identity
                )
                location = {
                    "page": page,
                    "bbox": deepcopy(source.get("bbox") or source.get("box")),
                    "text": cleaned,
                    "type": str(source.get("type") or "text"),
                    "coordinate_system": str(
                        source.get("coordinate_system") or "pdf_point"
                    ),
                    "source_id": identity,
                    "source_block_id": base_identity,
                    "location_precision": cls._source_location_precision(source),
                }
                blocks.append({
                    "text": cleaned,
                    "raw_text": text,
                    "search_text": normalize_text(cleaned),
                    "compare_text": plain_text(cleaned),
                    "source_id": identity,
                    "source_parent": source_index,
                    "split_count": len(parts),
                    "part_index": part_index,
                    "order": order,
                    "page": page,
                    "kind": str(source.get("type") or "text").lower(),
                    "location": location,
                    "original_range": {
                        "start": original_start,
                        "end": original_end,
                    },
                    "text_evidence": {
                        key: deepcopy(source.get(key))
                        for key in (
                            "ocr_status", "recognition_status", "recognition_error",
                            "decode_error", "text_source_conflict", "native_text_conflict",
                        )
                        if source.get(key) not in (None, "", False)
                    },
                })
                order += 1

        records: list[dict[str, Any]] = []
        seen: set[tuple[Any, ...]] = set()
        for start in range(len(blocks)):
            window: list[dict[str, Any]] = []
            for end in range(start, min(len(blocks), start + 8)):
                block = blocks[end]
                if window and not cls._can_join_candidate_blocks(window[-1], block):
                    break
                window.append(block)
                candidate = cls._candidate_from_blocks(window)
                if len(candidate["text"]) > 2000:
                    break
                key = (
                    candidate["text"],
                    tuple(candidate["source_ids"]),
                    (candidate["source_range"] or {}).get("start_order"),
                    (candidate["source_range"] or {}).get("end_order"),
                )
                if key not in seen:
                    seen.add(key)
                    records.append(candidate)
                if block["text"].endswith(("。", "；", ";", "！", "!", "？", "?")):
                    break
        return records

    @classmethod
    def _document_candidate_records(
        cls,
        payload: dict[str, Any],
        *,
        source_identity: str,
    ) -> list[dict[str, Any]]:
        """Build a read-only full-document index from already saved OCR."""
        sections = [
            deepcopy(item)
            for item in _data_node(payload).get("layout_sections") or []
            if isinstance(item, dict)
        ]
        repeated_edges = cls._repeated_page_edge_texts(sections)
        included: list[dict[str, Any]] = []
        exclusions: list[dict[str, Any]] = []
        for section in sections:
            text = str(section.get("text") or "").strip()
            kind = str(
                section.get("document_role")
                or section.get("semantic_type")
                or section.get("type")
                or ""
            ).lower()
            key = re.sub(r"\s+", "", text)
            explicit = any(value in kind for value in ("header", "footer", "页眉", "页脚"))
            if text and (explicit or key in repeated_edges):
                exclusions.append({
                    "text": text,
                    "page": cls._coerce_page(section.get("page")),
                    "reason": "explicit_page_margin" if explicit else "repeated_page_edge",
                    "bbox": deepcopy(section.get("bbox") or section.get("box")),
                })
                continue
            included.append(section)
        records = cls._candidate_records(
            {"sections": included, "title": ""},
            source_identity=source_identity,
        )
        for index, record in enumerate(records):
            record["search_scope"] = "document"
            if index == 0:
                record["layout_exclusions"] = exclusions
        return records

    @classmethod
    def _repeated_page_edge_texts(cls, sections: list[dict[str, Any]]) -> set[str]:
        text_pages: dict[str, set[int]] = {}
        document_pages = {
            page
            for item in sections
            if (page := cls._coerce_page(item.get("page"))) is not None
        }
        for item in sections:
            text = str(item.get("text") or "").strip()
            page = cls._coerce_page(item.get("page"))
            box = item.get("bbox") or item.get("box")
            if not text or page is None or len(re.sub(r"\s+", "", text)) > 80:
                continue
            if not isinstance(box, (list, tuple)) or len(box) < 4:
                continue
            page_height = item.get("page_height") or item.get("image_height")
            top, bottom = box[1], box[3]
            if not isinstance(top, (int, float)) or not isinstance(bottom, (int, float)):
                continue
            near_edge = float(top) <= 180
            if isinstance(page_height, (int, float)) and page_height > 0:
                near_edge = near_edge or float(bottom) >= float(page_height) * 0.88
            if near_edge:
                text_pages.setdefault(re.sub(r"\s+", "", text), set()).add(page)
        page_count = max(1, len(document_pages))
        return {
            text
            for text, pages in text_pages.items()
            if len(pages) >= 3 and len(pages) / page_count >= 0.35
        }

    @classmethod
    def _is_repeated_attachment_heading(
        cls,
        text: str,
        title: str,
        source_type: str,
    ) -> bool:
        value = str(text or "").strip()
        if not value or len(value) > 100 or re.search(r"[。；;]", value):
            return False
        title_core = cls._fixed_attachment_title(title)
        value_core = cls._fixed_attachment_title(value)
        if not title_core or not value_core:
            return False
        score = lexical_similarity(title_core, value_core)
        return score >= (0.6 if "heading" in source_type.lower() else 0.78)

    @classmethod
    def _strip_repeated_attachment_title(cls, text: str, title: str) -> str:
        value = str(text or "").strip()
        title_core = cls._fixed_attachment_title(title)
        if not value or not title_core:
            return value
        lines = value.splitlines()
        if len(lines) > 1 and lexical_similarity(title_core, lines[0]) >= 0.8:
            return "\n".join(lines[1:]).strip()
        salutation = re.search(r"(?:^|\s)(致(?:[（(][^）)]{1,30}[）)])?\s*[:：]?)", value)
        if salutation and 0 < salutation.start(1) <= 100:
            prefix = value[:salutation.start(1)].strip()
            prefix_core = cls._fixed_attachment_title(prefix)
            if (
                lexical_similarity(title_core, prefix_core) >= 0.65
                or normalize_text(title_core) in normalize_text(prefix_core)
                or normalize_text(prefix_core) in normalize_text(title_core)
            ):
                return value[salutation.start(1):].strip()
        if value.startswith(title_core):
            return value[len(title_core):].strip()
        return value

    @staticmethod
    def _split_structural_text(text: str) -> list[tuple[str, int, int]]:
        value = str(text or "")
        if not value.strip():
            return []
        labels = (
            "项目名称|项目编号|招标编号|采购编号|比选编号|投标人名称|参选人名称|"
            "供应商名称|供应商全称|供应商地址|公司名称|单位名称|行业类型|姓名|职务|地址|金额|日期|电话|手机|"
            "邮编|传真|联系人|电子邮箱|注册资本|实收资本|经营范围|其他情况"
        )
        marker = re.compile(
            rf"(?:^|(?<=\s))(?:[（(]?\d+(?:[-－]\d+)*[）).．、]?\s*)?"
            rf"(?:{labels})\s*[:：]"
        )
        matches = list(marker.finditer(value))
        if len(matches) < 2 or matches[0].start() > 4:
            return [(value, 0, len(value))]
        parts: list[tuple[str, int, int]] = []
        for index, match in enumerate(matches):
            start = match.start()
            end = matches[index + 1].start() if index + 1 < len(matches) else len(value)
            if index == 0 and start:
                start = 0
            part = value[start:end]
            if part.strip():
                parts.append((part, start, end))
        return parts or [(value, 0, len(value))]

    @staticmethod
    def _source_location_precision(source: dict[str, Any]) -> str:
        if not isinstance(source.get("bbox") or source.get("box"), (list, tuple)):
            return "none"
        kind = str(source.get("type") or "").lower()
        if "cell" in kind or kind == "table":
            return "cell"
        if kind in {"line", "text_line"}:
            return "line"
        return "block"

    @staticmethod
    def _can_join_candidate_blocks(left: dict[str, Any], right: dict[str, Any]) -> bool:
        if left.get("source_parent") == right.get("source_parent") and int(left.get("split_count") or 1) > 1:
            return False
        left_kind = str(left.get("kind") or "")
        right_kind = str(right.get("kind") or "")
        if any("table" in kind or "cell" in kind for kind in (left_kind, right_kind)):
            return False
        if "heading" in right_kind:
            return False
        if "heading" in left_kind:
            return False
        left_page = left.get("page")
        right_page = right.get("page")
        if left_page and right_page and right_page not in {left_page, left_page + 1}:
            return False
        if str(left.get("text") or "").endswith(("。", "；", ";", "！", "!", "？", "?")):
            return False
        return True

    @staticmethod
    def _candidate_from_blocks(blocks: list[dict[str, Any]]) -> dict[str, Any]:
        text_parts: list[str] = []
        source_spans: list[dict[str, Any]] = []
        cursor = 0
        for block in blocks:
            text = str(block.get("text") or "")
            if text_parts:
                text_parts.append(" ")
                cursor += 1
            start = cursor
            text_parts.append(text)
            cursor += len(text)
            source_spans.append({
                "source_id": block.get("source_id"),
                "candidate_range": {"start": start, "end": cursor},
                "original_range": deepcopy(block.get("original_range") or {}),
                "location": deepcopy(block.get("location") or {}),
            })
        value = "".join(text_parts)
        return {
            "text": value,
            "raw_text": value,
            "search_text": normalize_text(value),
            "compare_text": plain_text(value),
            "source_ids": [block.get("source_id") for block in blocks],
            "source_spans": source_spans,
            "text_evidence": [
                deepcopy(block.get("text_evidence") or {})
                for block in blocks
                if block.get("text_evidence")
            ],
            "locations": [deepcopy(block.get("location") or {}) for block in blocks],
            "source_range": {
                "start_order": int(blocks[0]["order"]),
                "end_order": int(blocks[-1]["order"]),
                "block_count": len(blocks),
                "start_page": blocks[0].get("page"),
                "end_page": blocks[-1].get("page"),
            },
        }

    @classmethod
    def _table_candidate_records(
        cls,
        entries: list[dict[str, Any]],
        *,
        start_order: int,
        source_identity: str,
    ) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        for index, entry in enumerate(entries):
            text = str(entry.get("text") or "").strip()
            if not text:
                continue
            source_id = hashlib.sha256(
                f"{source_identity}|table|{index}|{text}".encode("utf-8")
            ).hexdigest()[:20]
            locations = [deepcopy(value) for value in entry.get("locations") or []]
            order = start_order + index
            records.append({
                "text": text,
                "raw_text": text,
                "search_text": normalize_text(text),
                "compare_text": plain_text(text),
                "source_ids": [source_id],
                "source_spans": [{
                    "source_id": source_id,
                    "candidate_range": {"start": 0, "end": len(text)},
                    "original_range": {"start": 0, "end": len(text)},
                    "location": locations[0] if locations else {},
                }],
                "locations": locations,
                "candidate_kind": entry.get("kind") or "header",
                "table_structure_status": entry.get("table_structure_status") or "resolved",
                "search_scope": "attachment",
                "source_range": {
                    "start_order": order,
                    "end_order": order,
                    "block_count": 1,
                },
            })
        return records

    @classmethod
    def _title_candidate_record(cls, section: dict[str, Any]) -> dict[str, Any]:
        text = cls._fixed_attachment_title(str(section.get("title") or ""))
        raw_locations = [
            value
            for value in (
                list(section.get("title_locations") or [])
                + [
                    item for item in section.get("sections") or []
                    if isinstance(item, dict)
                    and "heading" in str(item.get("type") or "").lower()
                ][:2]
            )
            if isinstance(value, dict)
        ]
        locations = [
            {
                "page": cls._coerce_page(value.get("page")),
                "bbox": deepcopy(value.get("bbox") or value.get("box")),
                "text": str(value.get("text") or section.get("title") or ""),
                "type": str(value.get("type") or "heading"),
                "coordinate_system": str(value.get("coordinate_system") or "pdf_point"),
                "source_id": "attachment-title",
                "location_precision": cls._source_location_precision(value),
            }
            for value in raw_locations
        ]
        return {
            "text": text,
            "source_ids": ["attachment-title"],
            "source_spans": [{
                "source_id": "attachment-title",
                "candidate_range": {"start": 0, "end": len(text)},
                "original_range": {"start": 0, "end": len(str(section.get("title") or ""))},
                "location": locations[0] if locations else {},
            }],
            "locations": locations,
            "source_range": {"start_order": -1, "end_order": -1, "block_count": 1},
        }

    @classmethod
    def _project_table_candidate_records(
        cls,
        records: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        projected: list[dict[str, Any]] = []
        seen: set[tuple[str, tuple[Any, ...]]] = set()
        for record in records:
            for text in cls._table_header_candidates([str(record.get("text") or "")]):
                key = (text, tuple(record.get("source_ids") or []))
                if key in seen:
                    continue
                seen.add(key)
                value = deepcopy(record)
                value["text"] = text
                projected.append(value)
        return projected

    @classmethod
    def _minimal_exact_matches(
        cls,
        matches: list[tuple[dict[str, Any], dict[str, Any]]],
    ) -> list[tuple[dict[str, Any], dict[str, Any]]]:
        unique: list[tuple[dict[str, Any], dict[str, Any]]] = []
        seen: set[tuple[Any, ...]] = set()
        for record, compared in matches:
            key = (
                tuple(record.get("source_ids") or []),
                str(record.get("text") or ""),
            )
            if key in seen:
                continue
            seen.add(key)
            unique.append((record, compared))
        minimal: list[tuple[dict[str, Any], dict[str, Any]]] = []
        for candidate in unique:
            record = candidate[0]
            source_ids = set(record.get("source_ids") or [])
            start = (record.get("source_range") or {}).get("start_order")
            if any(
                set(other[0].get("source_ids") or []) < source_ids
                and (other[0].get("source_range") or {}).get("start_order") == start
                for other in unique
            ):
                continue
            minimal.append(candidate)
        return sorted(minimal, key=lambda entry: cls._record_order_key(entry[0]))

    @staticmethod
    def _record_order_key(record: dict[str, Any]) -> tuple[int, int, int]:
        source_range = record.get("source_range") or {}
        return (
            int(source_range.get("start_order") or 0),
            int(source_range.get("block_count") or 1),
            len(str(record.get("text") or "")),
        )

    @classmethod
    def _ordered_record(cls, records: list[dict[str, Any]]) -> dict[str, Any] | None:
        values = [record for record in records if isinstance(record, dict)]
        return min(values, key=cls._record_order_key) if values else None

    @classmethod
    def _positions_resolved_by_order(
        cls,
        ranked: list[tuple[dict[str, Any], float]],
        minimum_order: int,
    ) -> bool:
        starts = sorted({
            int((record.get("source_range") or {}).get("start_order") or 0)
            for record, _ in ranked
            if int((record.get("source_range") or {}).get("start_order") or 0) >= minimum_order
        })
        return bool(starts) and starts[0] >= minimum_order

    @staticmethod
    def _attach_candidate(result: dict[str, Any], record: dict[str, Any]) -> None:
        result["bid_locations"] = deepcopy(record.get("locations") or [])
        result["source_range"] = deepcopy(record.get("source_range") or {})
        result["source_spans"] = deepcopy(record.get("source_spans") or [])
        result["location_precision"] = StructuredConsistencyEngine._candidate_location_precision(record)

    @staticmethod
    def _candidate_location_precision(record: dict[str, Any]) -> str:
        precision = {
            str(
                location.get("location_precision")
                or (
                    "cell" if "cell" in str(location.get("type") or "").lower()
                    else "line" if str(location.get("type") or "").lower() in {"line", "text_line"}
                    else "block" if isinstance(location.get("bbox"), (list, tuple))
                    else "none"
                )
            )
            for location in record.get("locations") or []
            if isinstance(location, dict)
        }
        for value in ("character", "word", "line", "cell", "block"):
            if value in precision:
                return value
        return "none"

    @classmethod
    def _mark_unclear(
        cls,
        result: dict[str, Any],
        *,
        code: str,
        message: str,
        record: dict[str, Any] | None = None,
        lexical_score: float | None = None,
        basis: str | None = None,
        search_scope: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if record:
            cls._attach_candidate(result, record)
            result["matched_text"] = str(record.get("text") or "")
        if lexical_score is not None:
            result["lexical_score"] = round(float(lexical_score), 4)
        reason = {"code": code, "message": message}
        if basis:
            reason["basis"] = basis
        if record:
            reason["affected_source_range"] = deepcopy(record.get("source_range") or {})
            reason["source_ids"] = deepcopy(record.get("source_ids") or [])
        if search_scope:
            reason["search_scope"] = deepcopy(search_scope)
        result.update(
            status="unclear",
            match_method=code,
            difference_category=(
                "fillable_range_unclear"
                if code == "fillable_boundary_unclear"
                else "alignment_unclear"
            ),
            unclear_reasons=[reason],
        )
        return result

    @staticmethod
    def _fillable_mapping(
        pattern: dict[str, Any],
        captures: list[str],
    ) -> list[dict[str, Any]]:
        slots = pattern.get("slots") if isinstance(pattern, dict) else []
        return [
            {
                "index": slot.get("index", index),
                "label": slot.get("label") or f"填写区{index + 1}",
                "source": slot.get("source"),
                "template_range": deepcopy(slot.get("range") or {}),
                "bid_text": captures[index] if index < len(captures) else "",
            }
            for index, slot in enumerate(slots or [])
        ]

    @staticmethod
    def _collect_unclear_reasons(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        grouped: dict[tuple[str, str], dict[str, Any]] = {}
        for item in items:
            if item.get("status") != "unclear":
                continue
            for reason in item.get("unclear_reasons") or []:
                key = (str(reason.get("code") or "unknown"), str(reason.get("message") or "待核验"))
                entry = grouped.setdefault(key, {
                    "item_ids": [],
                    "basis": reason.get("basis"),
                    "search_scope": deepcopy(reason.get("search_scope") or {}),
                    "affected_ranges": [],
                })
                entry["item_ids"].append(str(item.get("item_id") or ""))
                affected = reason.get("affected_source_range") or reason.get("affected_ranges")
                if isinstance(affected, dict) and affected:
                    entry["affected_ranges"].append(deepcopy(affected))
                elif isinstance(affected, list):
                    entry["affected_ranges"].extend(deepcopy(affected[:20]))
        return [
            {
                "code": code,
                "message": message,
                "affected_item_count": len(entry["item_ids"]),
                "item_ids": entry["item_ids"],
                **({"basis": entry["basis"]} if entry.get("basis") else {}),
                **({"search_scope": entry["search_scope"]} if entry.get("search_scope") else {}),
                **({"affected_ranges": entry["affected_ranges"]} if entry.get("affected_ranges") else {}),
            }
            for (code, message), entry in grouped.items()
        ]

    @staticmethod
    def _reverse_fixed_insertions(
        candidates: list[dict[str, Any]],
        evaluated: list[dict[str, Any]],
        *,
        include_table: bool = False,
    ) -> list[dict[str, Any]]:
        consumed = {
            str(span.get("source_id") or "")
            for item in evaluated
            for span in item.get("source_spans") or []
            if str(span.get("source_id") or "")
        }
        consumed_orders = [
            int((item.get("source_range") or {}).get(edge))
            for item in evaluated
            if item.get("status") in {"pass", "fail"}
            for edge in ("start_order", "end_order")
            if isinstance((item.get("source_range") or {}).get(edge), int)
            and int((item.get("source_range") or {}).get(edge)) >= 0
        ]
        if len(consumed_orders) < 2:
            return []
        lower_order, upper_order = min(consumed_orders), max(consumed_orders)
        consumed_pages = {
            int(location.get("page"))
            for item in evaluated
            if item.get("status") in {"pass", "fail"}
            for location in item.get("bid_locations") or []
            if str(location.get("page") or "").isdigit()
        }
        additions: list[dict[str, Any]] = []
        seen: set[str] = set()
        for record in candidates:
            source_range = record.get("source_range") or {}
            if int(source_range.get("block_count") or 1) != 1:
                continue
            order = source_range.get("start_order")
            if not isinstance(order, int) or not lower_order < order < upper_order:
                continue
            locations = [
                location for location in record.get("locations") or []
                if isinstance(location, dict)
            ]
            pages = {
                int(location.get("page"))
                for location in locations
                if str(location.get("page") or "").isdigit()
            }
            if not pages or not (pages & consumed_pages):
                continue
            if not include_table and any(
                "table" in str(location.get("type") or "").lower()
                or "cell" in str(location.get("type") or "").lower()
                for location in locations
            ):
                continue
            source_ids = [str(value or "") for value in record.get("source_ids") or []]
            if not source_ids or any(value in consumed for value in source_ids):
                continue
            text = str(record.get("text") or "").strip()
            key = normalize_text(text)
            if not key or key in seen or len(key) < 12:
                continue
            if not any(marker in text for marker in OBLIGATION_MARKERS):
                continue
            seen.add(key)
            locations = deepcopy(locations)
            differences = with_locations(
                [{
                    "type": "insert",
                    "template_text": "",
                    "bid_text": text,
                    "template_range": {"start": 0, "end": 0},
                    "bid_range": {"start": 0, "end": len(plain_text(text))},
                }],
                template_locations=[],
                bid_locations=locations,
            )
            additions.append({
                "item_id": f"reverse-insert:{hashlib.sha1(key.encode('utf-8')).hexdigest()[:12]}",
                "kind": "fixed_clause",
                "label": "投标文件新增固定正文",
                "reference_text": "",
                "required": True,
                "enabled": True,
                "status": "fail",
                "match_method": "reverse_ordered_check",
                "lexical_score": 0.0,
                "difference_category": "fixed_content_inserted",
                "template_locations": [],
                "bid_locations": locations,
                "matched_text": text,
                "differences": differences,
                "unclear_reasons": [],
                "source_range": deepcopy(source_range),
                "source_spans": deepcopy(record.get("source_spans") or []),
                "fillable_mapping": [],
            })
        return additions

    @staticmethod
    def _source_coverage_v32(
        candidates: list[dict[str, Any]],
        evaluated: list[dict[str, Any]],
    ) -> dict[str, Any]:
        consumed = {
            str(span.get("source_id") or "")
            for item in evaluated
            for span in item.get("source_spans") or []
            if str(span.get("source_id") or "")
        }
        compared_pages = {
            int(location.get("page"))
            for item in evaluated
            if item.get("kind") != "title" and item.get("status") in {"pass", "fail", "unclear"}
            for location in item.get("bid_locations") or []
            if str(location.get("page") or "").isdigit()
        }
        unclear_ranges = [
            item.get("source_range") or {}
            for item in evaluated
            if item.get("status") == "unclear" and item.get("source_range")
        ]
        entries: list[dict[str, Any]] = []
        seen: set[str] = set()
        for record in candidates:
            source_range = record.get("source_range") or {}
            if int(source_range.get("block_count") or 1) != 1:
                continue
            source_ids = [str(value or "") for value in record.get("source_ids") or []]
            if not source_ids or source_ids[0] in seen:
                continue
            seen.add(source_ids[0])
            text = str(record.get("text") or "").strip()
            if not text:
                continue
            kind = str(record.get("candidate_kind") or "")
            location_types = {
                str(location.get("type") or "").lower()
                for location in record.get("locations") or []
            }
            record_pages = {
                int(location.get("page"))
                for location in record.get("locations") or []
                if str(location.get("page") or "").isdigit()
            }
            order = source_range.get("start_order")
            covered_by_unclear = any(
                isinstance(order, int)
                and int(value.get("start_order") or 0) <= order <= int(value.get("end_order") or 0)
                for value in unclear_ranges
            )
            if source_ids[0] in consumed:
                classification = "fixed_or_mapped_fill"
            elif covered_by_unclear:
                classification = "pending_item_mapping"
            elif compared_pages and record_pages and not (record_pages & compared_pages):
                classification = "supporting_evidence_outside_form_pages"
            elif kind in {"data_row", "form_row"}:
                classification = "structured_table_data"
            elif kind == "header":
                classification = "structured_table_projection"
            elif any("heading" in value for value in location_types):
                classification = "layout_heading"
            elif PAGE_NO_RE.match(text):
                classification = "layout_page_number"
            elif any("table" in value or "cell" in value for value in location_types):
                classification = "structured_table_data"
            elif any(
                marker in value
                for value in location_types
                for marker in ("signature", "seal", "image", "stamp")
            ):
                classification = "fillable_or_non_text_evidence"
            elif (
                len(normalize_text(text)) <= 40
                and not any(marker in text for marker in OBLIGATION_MARKERS)
                and not any(mark in text for mark in ("：", ":", "。", "；", ";"))
            ):
                classification = "fillable_value_or_layout"
            elif len(normalize_text(text)) < 4:
                classification = "layout_fragment"
            else:
                classification = "unresolved"
            entries.append({
                "source_ids": source_ids,
                "classification": classification,
                "source_range": deepcopy(source_range),
                "locations": deepcopy(record.get("locations") or []),
                "text_excerpt": text[:160],
            })
        counts: dict[str, int] = {}
        for entry in entries:
            classification = str(entry["classification"])
            counts[classification] = counts.get(classification, 0) + 1
        unresolved = [entry for entry in entries if entry["classification"] == "unresolved"]
        return {
            "complete": not unresolved,
            "counts": counts,
            "unresolved": unresolved[:100],
            "total_source_ranges": len(entries),
        }

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
        # OCR 会在换页处把一个固定句拆成两个版面块，甚至把后半句误标成 heading。
        # 补充相邻两块窗口即可恢复句子，同时仍严格限制在已匹配附件内部。
        adjacent = [
            f"{values[index]} {values[index + 1]}"
            for index in range(len(values) - 1)
        ]
        return values + adjacent

    @staticmethod
    def _looks_ocr_uncertain(reference: str, candidate: str) -> bool:
        """Conservatively defer differences containing typical OCR artifacts."""
        value = str(candidate or "")
        if not value:
            return True
        unusual = sum(
            1 for char in value
            if not (
                char.isspace() or char.isalnum() or "\u3400" <= char <= "\u9fff"
                or char in "，。；：、！？,.!?;:（）()【】[]《》“”‘’/%％+-—－_｜|"
            )
        )
        reference_cjk = sum("\u3400" <= char <= "\u9fff" for char in str(reference or ""))
        candidate_latin = sum("A" <= char <= "z" for char in value)
        return unusual / max(1, len(value)) >= 0.02 or (
            reference_cjk >= 8 and candidate_latin / max(1, len(value)) >= 0.08
        )

    @staticmethod
    def _table_header_candidates(candidates: list[str]) -> list[str]:
        """Project candidate table lines to exact canonical header order.

        Values and repeated data rows are intentionally excluded.  Header
        spelling and order are not normalized through aliases.
        """
        result: list[str] = []
        for candidate in candidates:
            if "｜" in candidate:
                cells = [plain_text(cell) for cell in candidate.split("｜")]
                cells = [cell for cell in cells if cell]
                if cells:
                    result.append("｜".join(cells))
                continue
            normalized = normalize_text(candidate)
            positions = []
            for marker in TABLE_HEADER_MARKERS:
                position = normalized.find(normalize_text(marker))
                if position >= 0:
                    positions.append((position, marker))
            if positions:
                result.append("｜".join(marker for _, marker in sorted(positions)))
        return list(dict.fromkeys(result))

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
            if item.get("status") not in {"fail", "unclear"}:
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
            return "固定内容完整覆盖且逐字一致。"
        if status == "fail":
            count = sum(1 for item in differences if item.get("status") == "fail")
            return f"发现 {count or len(differences)} 处已确认的固定内容差异。"
        if status == "unclear":
            return "存在填写边界、OCR 或对应位置无法唯一确认的范围，需要人工核验。"
        return "该附件无需执行固定条款一致性检查。"
