# -*- coding: utf-8 -*-
"""检查结果构建 Mixin"""
import re
from typing import Any


class ResultsMixin:
    """负责组装最终检查结果字典。"""

    # 依赖方法
    _extract_star_requirements: Any
    _extract_bid_deviation_sections: Any
    _detect_global_no_deviation: Any
    _collect_table_coverage: Any
    _match_one_star: Any
    _match_patterns: Any
    _norm: Any
    _clip: Any
    _coerce_payload: Any
    _extract_text: Any
    _extract_pair: Any

    def check_technical_deviation(
        self,
        tender_document: Any,
        bid_document: Any | None = None,
        technical_bid_document: Any | None = None,
    ) -> dict:
        """对招标文件和投标文件进行偏离检查。"""
        tender = self._coerce_payload(tender_document)
        if bid_document is None:
            pair = self._extract_pair(tender)
            if not pair:
                return self._single_doc_result(tender)
            tender, bid = pair
        else:
            bid = self._coerce_payload(bid_document)
        technical_bid = self._coerce_payload(technical_bid_document) if technical_bid_document is not None else None
        return self._run_check(tender, bid, technical_bid)

    def compare_raw_data(self, tender_raw_json: Any, bid_raw_json: Any) -> dict:
        """对外一致性接口。"""
        return self.check_technical_deviation(tender_raw_json, bid_raw_json)

    def _run_check(
        self,
        tender_payload: dict,
        bid_payload: dict,
        technical_bid_payload: dict | None = None,
    ) -> dict:
        """核心检查逻辑。"""
        star_requirements = self._extract_star_requirements(tender_payload)
        tender_template_requirements = self._technical_deviation_template_requirements(tender_payload)
        sections = self._extract_combined_bid_deviation_sections(bid_payload, technical_bid_payload)
        global_stmt = self._detect_global_no_deviation(sections["combined_text"])
        table_coverage = self._collect_table_coverage(sections)
        self_declared_items = self._extract_self_declared_deviation_items(sections)

        if not star_requirements and not tender_template_requirements:
            if self_declared_items:
                return self._build_self_declared_deviation_result(
                    sections,
                    global_stmt,
                    table_coverage,
                    self_declared_items,
                )
            return self._build_empty_star_result(sections, global_stmt, table_coverage)
        if not star_requirements and not sections.get("technical"):
            if self_declared_items:
                return self._build_self_declared_deviation_result(
                    sections,
                    global_stmt,
                    table_coverage,
                    self_declared_items,
                )
            return self._build_missing_technical_deviation_table_result(
                tender_template_requirements,
                sections,
                table_coverage,
            )
        if not star_requirements:
            if self_declared_items:
                return self._build_self_declared_deviation_result(
                    sections,
                    global_stmt,
                    table_coverage,
                    self_declared_items,
                )
            return self._build_empty_star_result(sections, global_stmt, table_coverage)
        bid_texts = [self._extract_text(bid_payload)]
        if technical_bid_payload is not None:
            bid_texts.append(self._extract_text(technical_bid_payload))
        if not "\n".join(str(text or "").strip() for text in bid_texts).strip():
            return self._build_missing_bid_content_result(star_requirements, sections, table_coverage)
        if not sections.get("business") and not sections.get("technical"):
            return self._build_missing_deviation_table_result(star_requirements, sections, table_coverage)

        requirements = star_requirements
        matches = [self._match_one_star(item, sections) for item in requirements]
        # 对每个标记项做语义符合度打分（BGE，离线），每项都带 semantic_*/needs_manual。
        self._apply_semantic_judgement(matches)

        # 必须项(★)缺失/负偏离 → 失败；加分项(△)未达标 → 提示(不致失败)。
        missing_items: list[dict[str, Any]] = []   # 仅必须项，驱动合规
        negative_items: list[dict[str, Any]] = []  # 仅必须项
        unclear_items: list[dict[str, Any]] = []   # 仅必须项
        bonus_items: list[dict[str, Any]] = []     # 加分项中的缺失/负偏离/不明确，提示用
        mandatory_total = 0
        bonus_total = 0
        responded = 0
        positive = 0
        no_dev = 0
        listed = 0
        bonus_responded = 0

        for item in matches:
            is_bonus = item.get("requirement_kind") == "bonus"
            dev_type = str(item.get("deviation_type") or "unclear")
            if is_bonus:
                bonus_total += 1
            else:
                mandatory_total += 1

            if not item.get("responded"):
                item["response_status"] = "missing"
                if is_bonus:
                    item["risk_level"] = "medium"
                    bonus_items.append(self._summary_item(item, "missing"))
                else:
                    item["risk_level"] = "high"
                    missing_items.append(self._summary_item(item, "missing"))
                continue

            if is_bonus:
                bonus_responded += 1
            else:
                responded += 1

            if dev_type == "negative_deviation":
                item["response_status"] = "negative_deviation"
                if is_bonus:
                    item["risk_level"] = "medium"
                    bonus_items.append(self._summary_item(item, "negative_deviation"))
                else:
                    item["risk_level"] = "high"
                    negative_items.append(self._summary_item(item, "negative_deviation"))
            elif dev_type == "positive_deviation":
                item["response_status"] = "positive_deviation"
                item["risk_level"] = "low"
                if not is_bonus:
                    positive += 1
            elif dev_type == "no_deviation":
                item["response_status"] = "no_deviation"
                item["risk_level"] = "low"
                if not is_bonus:
                    no_dev += 1
            elif dev_type == "listed_response":
                item["response_status"] = "listed_response"
                item["risk_level"] = "low"
                if not is_bonus:
                    listed += 1
            else:
                item["response_status"] = "unclear_deviation"
                if is_bonus:
                    item["risk_level"] = "medium"
                    bonus_items.append(self._summary_item(item, "unclear_deviation"))
                else:
                    item["risk_level"] = "high"
                    unclear_items.append(self._summary_item(item, "unclear_deviation"))

        extra_declared_items = self._exclude_declared_items_covered_by_matches(
            self_declared_items,
            matches,
        )
        for item in extra_declared_items:
            matches.append(item)
            if item.get("deviation_type") == "negative_deviation":
                negative_items.append(self._summary_item(item, "negative_deviation"))
            else:
                unclear_items.append(self._summary_item(item, "unclear_deviation"))

        total = len(requirements) + len(extra_declared_items)
        missing = len(missing_items)
        negative = len(negative_items)
        unclear = len(unclear_items)
        bonus_flagged = len(bonus_items)
        status, deviation_status, summary = self._overall_status(
            mandatory_total, missing, negative, unclear, bonus_total, bonus_flagged
        )

        findings = [
            f"在招标文件中检测到 {mandatory_total} 条 ★ 强制性要求、{bonus_total} 条 △ 加分项。"
        ]
        findings.append(f"★ 已响应 {responded} 条，缺失 {missing} 条，负偏离 {negative} 条，不明确 {unclear} 条。")
        findings.append(f"△ 已响应 {bonus_responded} 条，未达标 {bonus_flagged} 条（加分项，不计入合规失败）。")
        findings.append(f"合规响应数量（无偏离/正偏离/列明未负响应）：{no_dev + positive + listed} 条。")
        declared_negative_count = sum(
            1
            for item in extra_declared_items
            if item.get("deviation_type") == "negative_deviation"
        )
        declared_unclear_count = len(extra_declared_items) - declared_negative_count
        if extra_declared_items:
            findings.append(
                f"偏离表另有投标人主动声明的负偏离 {declared_negative_count} 条、"
                f"需复核偏离 {declared_unclear_count} 条。"
            )
            if declared_negative_count:
                status, deviation_status = "fail", "fail"
                summary = (
                    f"{summary}；偏离表主动声明负偏离 {declared_negative_count} 条"
                    f"、需复核偏离 {declared_unclear_count} 条。"
                )
            elif status == "pass":
                status, deviation_status = "unclear", "unclear_response"
                summary = f"{summary}；偏离表主动填写了 {declared_unclear_count} 条偏离说明，需人工复核。"

        return {
            "mode": "tender_technical_bid_json",
            "summary": summary,
            "compliance_status": status,
            "deviation_status": deviation_status,
            "requirement_extraction_mode": "star_triangle",
            "core_requirements_count": total,
            "core_star_requirements_count": mandatory_total,
            "mandatory_requirements_count": mandatory_total,
            "bonus_requirements_count": bonus_total,
            "self_declared_deviation_count": len(extra_declared_items),
            "self_declared_negative_count": declared_negative_count,
            "self_declared_unclear_count": declared_unclear_count,
            "deviation_tables": {
                "business_found": bool(sections["business"]),
                "technical_found": bool(sections["technical"]),
                "business_section_count": len(sections["business"]),
                "technical_section_count": len(sections["technical"]),
            },
            "business_catalog_pages": sections.get("catalog_pages") or [],
            "business_catalog_locations": sections.get("catalog_locations") or [],
            "table_coverage": table_coverage,
            "global_response_statement": global_stmt,
            "star_requirements": requirements,
            "match_results": matches,
            "missing_response_items": missing_items,
            "negative_deviation_items": negative_items,
            "unclear_response_items": unclear_items,
            "bonus_flagged_items": bonus_items,
            "stats": {
                "responded_count": responded + len(extra_declared_items),
                "missing_count": missing,
                "negative_deviation_count": negative,
                "positive_deviation_count": positive,
                "no_deviation_count": no_dev,
                "listed_response_count": listed,
                "unclear_deviation_count": unclear,
                "explicit_response_count": responded + len(extra_declared_items),
                "bonus_total_count": bonus_total,
                "bonus_responded_count": bonus_responded,
                "bonus_flagged_count": bonus_flagged,
                "covered_by_global_statement_count": 0,
                "covered_by_deviation_table_count": 0,
            },
            "key_findings": findings,
            "extracted_parameters": [x["requirement"] for x in requirements]
            + [x["requirement"] for x in extra_declared_items],
        }

    def _extract_self_declared_deviation_items(
        self,
        sections: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """提取偏离表中投标人主动填写的负偏离或待复核偏离。

        该检查独立于招标文件是否存在 ★ 条款。优先使用逻辑表格的列级结果，
        只有对应页没有逻辑表格行时才使用纯文本回退，避免把表尾“对不满足项应
        如实填写”的模板说明误当作投标人的负偏离声明。
        """
        rows = [row for row in (sections.get("rows") or []) if isinstance(row, dict)]
        logical_pages = {
            (str(row.get("document_role") or ""), row.get("page"))
            for row in rows
            if row.get("source") == "logical_table" and self._is_deviation_table_row(row)
        }
        items: list[dict[str, Any]] = []
        seen: set[tuple[str, str, int | None, str]] = set()

        for row in rows:
            if not self._is_deviation_table_row(row):
                continue
            if row.get("source") == "section_text" and (
                str(row.get("document_role") or ""), row.get("page")
            ) in logical_pages:
                continue

            deviation_type = self._self_declared_deviation_type(row)
            if deviation_type not in {"negative_deviation", "unclear_deviation"}:
                continue

            requirement = self._clip(
                str(row.get("requirement_text") or "").strip() or "偏离表主动声明",
                260,
            )
            evidence_parts: list[str] = []
            for value in (row.get("response_text"), row.get("deviation_text")):
                text = str(value or "").strip()
                if text and text not in evidence_parts:
                    evidence_parts.append(text)
            response_evidence = self._clip("；".join(evidence_parts), 320)
            page = row.get("page") if isinstance(row.get("page"), int) else None
            key = (
                self._norm(requirement)[:180],
                self._norm(response_evidence)[:180],
                page,
                str(row.get("document_role") or ""),
            )
            if key in seen:
                continue
            seen.add(key)

            item_index = len(items) + 1
            items.append(
                {
                    "requirement_id": f"SELF-DECLARED-DEVIATION-{item_index:03d}",
                    "requirement": requirement,
                    "source_type": "self_declared_deviation",
                    "marker_type": "self_declared",
                    "requirement_kind": "declared_deviation",
                    "requirement_page": None,
                    "requirement_bbox": None,
                    "section_type": str(row.get("group") or "unknown"),
                    "responded": True,
                    "explicit_response": True,
                    "response_status": deviation_type,
                    "response_evidence": response_evidence,
                    "response_section": str(row.get("group") or "unknown"),
                    "response_section_title": str(row.get("title") or "偏离表"),
                    "response_page": page,
                    "response_page_end": self._deviation_table_end_page(sections, page),
                    "response_bbox": row.get("bbox"),
                    "response_document_role": row.get("document_role") or "business_bid",
                    "response_line_number": None,
                    "material_text": row.get("material_text") or "",
                    "material_locations": row.get("material_locations") or [],
                    "match_score": 1.0 if deviation_type == "negative_deviation" else 0.6,
                    "deviation_type": deviation_type,
                    "risk_level": "high" if deviation_type == "negative_deviation" else "medium",
                    "semantic_score": None,
                    "semantic_status": (
                        "投标人主动声明"
                        if deviation_type == "negative_deviation"
                        else "待人工复核"
                    ),
                    "needs_manual": deviation_type != "negative_deviation",
                }
            )
        return items

    def _is_deviation_table_row(self, row: dict[str, Any]) -> bool:
        """限制在真正的商务/技术偏离表内，避免扫描普通业务表格。"""
        title = re.sub(r"\s+", "", str(row.get("title") or ""))
        return bool(
            "偏离表" in title
            or any(token in title for token in self.BUSINESS_TITLES + self.TECH_TITLES)
        )

    def _self_declared_deviation_type(self, row: dict[str, Any]) -> str | None:
        """按偏离说明列、响应列判定主动声明的偏离类型。"""
        deviation_text = str(row.get("deviation_text") or "").strip()
        response_text = str(row.get("response_text") or "").strip()
        classifications = [
            self._classify_self_declared_text(text)
            for text in (deviation_text, response_text)
            if text
        ]
        if "negative_deviation" in classifications:
            return "negative_deviation"
        if any(value in {"no_deviation", "positive_deviation"} for value in classifications):
            return None
        if row.get("source") == "logical_table" and self._meaningful_deviation_explanation(deviation_text):
            return "unclear_deviation"
        return None

    def _classify_self_declared_text(self, text: str) -> str | None:
        """识别明确负偏离，并让“无偏离+模板提示”优先按无偏离处理。"""
        value = str(text or "").strip()
        if not value:
            return None
        partial_patterns = (
            r"部\s*分\s*(?:满足|响应|符合)",
            r"未\s*完全\s*(?:满足|响应|符合)",
        )
        if self._match_patterns(value, partial_patterns):
            return "negative_deviation"
        compact = re.sub(r"\s+", "", value)
        negated_negative = bool(
            re.search(r"(?:无|没有|不存在|未发现|不构成)(?:任何)?负偏离", compact)
        )
        if "负偏离" in compact and not negated_negative:
            return "negative_deviation"
        if negated_negative:
            return "no_deviation"
        if self._match_patterns(value, self.NO_DEV_PATTERNS):
            return "no_deviation"
        if self._match_patterns(value, self.NEG_DEV_PATTERNS):
            return "negative_deviation"
        if self._match_patterns(value, self.POS_DEV_PATTERNS):
            return "positive_deviation"
        return None

    def _meaningful_deviation_explanation(self, text: str) -> bool:
        """偏离说明列有实质填写但未标明正/负时，作为待复核偏离输出。"""
        value = re.sub(r"\s+", "", str(text or ""))
        if not value or value in {"/", "-", "—", "无", "不适用", "无偏离"}:
            return False
        if re.fullmatch(
            r"(?:(?:详见|见)?(?:技术|商务|响应|投标)?文件)?(?:第|P)?\d{1,4}(?:页)?",
            value,
            re.IGNORECASE,
        ):
            return False
        if all(token in value for token in ("偏离说明", "响应文件")) and len(value) <= 40:
            return False
        return len(self._norm(value)) >= 4

    def _exclude_declared_items_covered_by_matches(
        self,
        declared_items: list[dict[str, Any]],
        matches: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """星标条款已命中同一偏离行时不重复输出主动声明项。"""
        result: list[dict[str, Any]] = []
        for declared in declared_items:
            declared_req = self._norm(declared.get("requirement") or "")
            declared_evidence = self._norm(declared.get("response_evidence") or "")
            covered = False
            for match in matches:
                if match.get("response_page") != declared.get("response_page"):
                    continue
                match_req = self._norm(match.get("requirement") or "")
                match_evidence = self._norm(match.get("response_evidence") or "")
                requirement_overlap = bool(
                    min(len(declared_req), len(match_req)) >= 8
                    and (declared_req in match_req or match_req in declared_req)
                )
                evidence_overlap = bool(
                    min(len(declared_evidence), len(match_evidence)) >= 6
                    and (declared_evidence in match_evidence or match_evidence in declared_evidence)
                )
                if requirement_overlap or evidence_overlap:
                    covered = True
                    break
            if not covered:
                result.append(declared)
        return result

    def _build_self_declared_deviation_result(
        self,
        sections: dict[str, Any],
        global_stmt: dict[str, Any],
        table_coverage: dict[str, Any],
        items: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """无 ★ 条款时，仍以偏离表中的主动声明生成可定位审查结果。"""
        negative_items = [
            self._summary_item(item, "negative_deviation")
            for item in items
            if item.get("deviation_type") == "negative_deviation"
        ]
        unclear_items = [
            self._summary_item(item, "unclear_deviation")
            for item in items
            if item.get("deviation_type") == "unclear_deviation"
        ]
        negative_count = len(negative_items)
        unclear_count = len(unclear_items)
        if negative_count:
            compliance_status = "fail"
            deviation_status = "self_declared_negative_deviation"
        else:
            compliance_status = "unclear"
            deviation_status = "self_declared_deviation_unclear"
        summary = (
            "招标文件中未发现有效 ★ 强制性要求；"
            f"但偏离表主动声明负偏离 {negative_count} 条、需复核偏离 {unclear_count} 条。"
        )
        return {
            "mode": "tender_technical_bid_json",
            "summary": summary,
            "compliance_status": compliance_status,
            "deviation_status": deviation_status,
            "requirement_extraction_mode": "self_declared_deviation",
            "core_requirements_count": len(items),
            "core_star_requirements_count": 0,
            "mandatory_requirements_count": 0,
            "bonus_requirements_count": 0,
            "self_declared_deviation_count": len(items),
            "self_declared_negative_count": negative_count,
            "self_declared_unclear_count": unclear_count,
            "deviation_tables": {
                "business_found": bool(sections["business"]),
                "technical_found": bool(sections["technical"]),
                "business_section_count": len(sections["business"]),
                "technical_section_count": len(sections["technical"]),
            },
            "business_catalog_pages": sections.get("catalog_pages") or [],
            "business_catalog_locations": sections.get("catalog_locations") or [],
            "table_coverage": table_coverage,
            "global_response_statement": global_stmt,
            "star_requirements": [],
            "match_results": items,
            "missing_response_items": [],
            "negative_deviation_items": negative_items,
            "unclear_response_items": unclear_items,
            "bonus_flagged_items": [],
            "stats": {
                "responded_count": len(items),
                "missing_count": 0,
                "negative_deviation_count": negative_count,
                "positive_deviation_count": 0,
                "no_deviation_count": 0,
                "listed_response_count": 0,
                "unclear_deviation_count": unclear_count,
                "explicit_response_count": len(items),
                "bonus_total_count": 0,
                "bonus_responded_count": 0,
                "bonus_flagged_count": 0,
                "covered_by_global_statement_count": 0,
                "covered_by_deviation_table_count": len(items),
            },
            "key_findings": [
                "招标文件中未发现有效 ★ 强制性要求，已继续检查投标人偏离表。",
                f"定位到主动声明负偏离 {negative_count} 条、需复核偏离 {unclear_count} 条。",
            ],
            "extracted_parameters": [item["requirement"] for item in items],
        }

    def _technical_deviation_template_requirements(self, tender_payload: dict) -> list[dict[str, Any]]:
        """Extract tender-side technical deviation table templates as auditable requirements."""
        sections = self._extract_bid_deviation_sections(tender_payload, document_role="tender")
        requirements: list[dict[str, Any]] = []
        seen: set[str] = set()
        for section in sections.get("technical") or []:
            title = str(section.get("title") or "技术偏离表").strip()
            text = str(section.get("text") or "").strip()
            key = self._norm(f"{title}|{section.get('page')}|{text[:120]}")
            if not key or key in seen:
                continue
            seen.add(key)
            requirements.append(
                {
                    "requirement_id": f"TECH-DEVIATION-TABLE-{len(requirements)+1:03d}",
                    "requirement": "招标文件要求提供技术偏离表",
                    "source_type": "technical_deviation_table_template",
                    "section_type": "technical_deviation_table_template",
                    "template_title": title,
                    "template_text": self._clip(text, 240),
                    "page": section.get("page"),
                    "bbox": section.get("bbox"),
                    "line_number": section.get("start_line"),
                }
            )
        return requirements

    def _extract_combined_bid_deviation_sections(
        self,
        business_payload: dict,
        technical_payload: dict | None = None,
    ) -> dict[str, Any]:
        """Extract deviation tables from both business and technical bid documents."""
        parts = [
            self._extract_bid_deviation_sections(business_payload, document_role="business_bid")
        ]
        if technical_payload is not None:
            parts.append(
                self._extract_bid_deviation_sections(technical_payload, document_role="technical_bid")
            )

        combined: dict[str, Any] = {
            "business": [],
            "technical": [],
            "rows": [],
            "catalog_pages": [],
            "catalog_locations": [],
        }
        combined_text_parts: list[str] = []
        table_start_pages: set[int] = set()
        for part in parts:
            for key in ("business", "technical", "rows", "catalog_locations"):
                combined[key].extend(part.get(key) or [])
            for page in part.get("catalog_pages") or []:
                if page not in combined["catalog_pages"]:
                    combined["catalog_pages"].append(page)
            for page in part.get("table_start_pages") or []:
                if isinstance(page, int):
                    table_start_pages.add(page)
            text = str(part.get("combined_text") or "").strip()
            if text:
                combined_text_parts.append(text)
        combined["catalog_pages"] = sorted(page for page in combined["catalog_pages"] if isinstance(page, int))
        # 用于把"响应所在页"扩成整张偏离表跨度（下一张表起始页 - 1）
        combined["table_start_pages"] = sorted(table_start_pages)
        combined["combined_text"] = "\n\n".join(combined_text_parts)
        return combined

    def _single_doc_result(self, payload: dict) -> dict:
        """单文档输入时生成提示结果。"""
        sections = self._extract_bid_deviation_sections(payload)
        return self._build_empty_star_result(sections, self._detect_global_no_deviation(sections["combined_text"]), self._collect_table_coverage(sections))

    def _build_empty_star_result(self, sections, global_stmt, table_coverage):
        """构建无星标要求时的结果。"""
        return {
            "mode": "tender_technical_bid_json",
            "summary": "招标文件中未发现带 ★ 的强制性要求，已跳过偏离比对。",
            "compliance_status": "pass",
            "deviation_status": "no_star_requirements",
            "requirement_extraction_mode": "star",
            "core_requirements_count": 0,
            "core_star_requirements_count": 0,
            "deviation_tables": {
                "business_found": bool(sections["business"]),
                "technical_found": bool(sections["technical"]),
                "business_section_count": len(sections["business"]),
                "technical_section_count": len(sections["technical"]),
            },
            "business_catalog_pages": sections.get("catalog_pages") or [],
            "business_catalog_locations": sections.get("catalog_locations") or [],
            "table_coverage": table_coverage,
            "global_response_statement": global_stmt,
            "star_requirements": [],
            "match_results": [],
            "missing_response_items": [],
            "negative_deviation_items": [],
            "unclear_response_items": [],
            "stats": {
                "responded_count": 0,
                "missing_count": 0,
                "negative_deviation_count": 0,
                "positive_deviation_count": 0,
                "no_deviation_count": 0,
                "listed_response_count": 0,
                "unclear_deviation_count": 0,
                "explicit_response_count": 0,
                "covered_by_global_statement_count": 0,
                "covered_by_deviation_table_count": 0,
            },
            "key_findings": ["招标文件中未发现带 ★ 的强制性要求，无需执行偏离比对。"],
            "extracted_parameters": [],
        }

    def _build_missing_technical_deviation_table_result(self, requirements, sections, table_coverage):
        """商务标未提供技术偏离表时，按内容缺失判定为 missing。"""
        evidence = "商务标文件中未识别到技术偏离表，无法核验技术响应。"
        issue_requirements = list(requirements or [])
        missing_items = [
            {
                "requirement_id": item["requirement_id"],
                "requirement": item["requirement"],
                "source_type": item.get("source_type"),
                "section_type": item.get("section_type"),
                "template_title": item.get("template_title"),
                "template_text": item.get("template_text"),
                "requirement_page": item.get("page"),
                "requirement_bbox": item.get("bbox"),
                "requirement_line_number": item.get("line_number"),
                "response_status": "technical_deviation_table_missing",
                "response_evidence": evidence,
            }
            for item in issue_requirements
        ]
        matches = [
            {
                "responded": False,
                "risk_level": "high",
                "match_score": 0.0,
                "requirement": item["requirement"],
                "source_type": item.get("source_type"),
                "requirement_page": item.get("page"),
                "requirement_bbox": item.get("bbox"),
                "requirement_line_number": item.get("line_number"),
                "section_type": item.get("section_type"),
                "template_title": item.get("template_title"),
                "template_text": item.get("template_text"),
                "response_page": None,
                "deviation_type": "missing",
                "requirement_id": item["requirement_id"],
                "response_status": "technical_deviation_table_missing",
                "response_section": "",
                "explicit_response": False,
                "response_evidence": evidence,
                "response_line_number": None,
                "response_section_title": "",
            }
            for item in issue_requirements
        ]
        total = len(issue_requirements)
        star_total = sum(
            1
            for item in issue_requirements
            if item.get("source_type") != "technical_deviation_table_template"
        )
        missing_count = len(issue_requirements)
        summary = "商务标文件中未识别到技术偏离表，无法完成偏离比对。"
        if star_total:
            summary = f"共发现 {star_total} 条带 ★ 的强制性要求，但{summary}"
        elif total:
            summary = f"招标文件包含技术偏离表格式，但{summary}"
        return {
            "mode": "tender_technical_bid_json",
            "summary": summary,
            "compliance_status": "missing",
            "deviation_status": "technical_deviation_table_missing",
            "requirement_extraction_mode": "star" if star_total else "technical_deviation_table_template",
            "core_requirements_count": total,
            "core_star_requirements_count": star_total,
            "deviation_tables": {
                "business_found": bool(sections["business"]),
                "technical_found": False,
                "business_section_count": len(sections["business"]),
                "technical_section_count": 0,
            },
            "business_catalog_pages": sections.get("catalog_pages") or [],
            "business_catalog_locations": sections.get("catalog_locations") or [],
            "table_coverage": table_coverage,
            "global_response_statement": None,
            "star_requirements": requirements,
            "match_results": matches,
            "missing_response_items": missing_items,
            "negative_deviation_items": [],
            "unclear_response_items": [],
            "stats": {
                "responded_count": 0,
                "missing_count": missing_count,
                "negative_deviation_count": 0,
                "positive_deviation_count": 0,
                "no_deviation_count": 0,
                "listed_response_count": 0,
                "unclear_deviation_count": 0,
                "explicit_response_count": 0,
                "covered_by_global_statement_count": 0,
                "covered_by_deviation_table_count": 0,
            },
            "key_findings": [
                (
                    f"在招标文件中检测到 {star_total} 条带 ★ 的强制性要求。"
                    if star_total
                    else "在招标文件中检测到技术偏离表格式/模板。"
                ),
                "商务标文件中未识别到技术偏离表，按未提供必需响应表归类为缺失。",
            ],
            "extracted_parameters": [x["requirement"] for x in requirements],
        }

    def _build_missing_deviation_table_result(self, requirements, sections, table_coverage):
        """商务标未识别到商务/技术偏离表时，按缺少偏离表返回。"""
        evidence = "商务标文件中未识别到商务偏离表或技术偏离表，无法核验偏离响应。"
        missing_items = [
            {
                "requirement_id": item["requirement_id"],
                "requirement": item["requirement"],
                "requirement_page": item.get("page"),
                "requirement_bbox": item.get("bbox"),
                "response_page": None,
                "response_bbox": None,
                "response_status": "deviation_table_missing",
                "response_evidence": evidence,
            }
            for item in requirements
        ]
        matches = [
            {
                "responded": False,
                "risk_level": "high",
                "match_score": 0.0,
                "requirement": item["requirement"],
                "requirement_page": item.get("page"),
                "requirement_bbox": item.get("bbox"),
                "section_type": item.get("section_type"),
                "response_page": None,
                "deviation_type": "missing",
                "requirement_id": item["requirement_id"],
                "response_status": "deviation_table_missing",
                "response_section": "",
                "explicit_response": False,
                "response_evidence": evidence,
                "response_line_number": None,
                "response_section_title": "",
            }
            for item in requirements
        ]
        total = len(requirements)
        return {
            "mode": "tender_technical_bid_json",
            "summary": f"共发现 {total} 条带 ★ 的强制性要求，但商务标文件中未识别到商务偏离表或技术偏离表。",
            "compliance_status": "missing",
            "deviation_status": "deviation_table_missing",
            "requirement_extraction_mode": "star",
            "core_requirements_count": total,
            "core_star_requirements_count": total,
            "deviation_tables": {
                "business_found": False,
                "technical_found": False,
                "business_section_count": 0,
                "technical_section_count": 0,
            },
            "business_catalog_pages": sections.get("catalog_pages") or [],
            "business_catalog_locations": sections.get("catalog_locations") or [],
            "table_coverage": table_coverage,
            "global_response_statement": None,
            "star_requirements": requirements,
            "match_results": matches,
            "missing_response_items": missing_items,
            "negative_deviation_items": [],
            "unclear_response_items": [],
            "stats": {
                "responded_count": 0,
                "missing_count": total,
                "negative_deviation_count": 0,
                "positive_deviation_count": 0,
                "no_deviation_count": 0,
                "listed_response_count": 0,
                "unclear_deviation_count": 0,
                "explicit_response_count": 0,
                "covered_by_global_statement_count": 0,
                "covered_by_deviation_table_count": 0,
            },
            "key_findings": [
                f"在招标文件中检测到 {total} 条带 ★ 的强制性要求。",
                "商务标文件中未识别到商务偏离表或技术偏离表，按缺少偏离表处理。",
            ],
            "extracted_parameters": [x["requirement"] for x in requirements],
        }

    def _build_missing_bid_content_result(self, requirements, sections, table_coverage):
        """投标文件 OCR/内容缺失时按 missing 返回。"""
        missing_items = [
            {
                "requirement_id": item["requirement_id"],
                "requirement": item["requirement"],
                "requirement_page": item.get("page"),
                "requirement_bbox": item.get("bbox"),
                "response_status": "bid_content_missing",
                "response_evidence": "投标文件 OCR 内容为空，无法核验响应。",
            }
            for item in requirements
        ]
        matches = [
            {
                "responded": False,
                "risk_level": "high",
                "match_score": 0.0,
                "requirement": item["requirement"],
                "requirement_page": item.get("page"),
                "requirement_bbox": item.get("bbox"),
                "section_type": item.get("section_type"),
                "response_page": None,
                "deviation_type": "missing",
                "requirement_id": item["requirement_id"],
                "response_status": "bid_content_missing",
                "response_section": "",
                "explicit_response": False,
                "response_evidence": "投标文件 OCR 内容为空，无法核验响应。",
                "response_line_number": None,
                "response_section_title": "",
            }
            for item in requirements
        ]
        total = len(requirements)
        return {
            "mode": "tender_technical_bid_json",
            "summary": f"共发现 {total} 条带 ★ 的强制性要求，但投标文件 OCR 内容为空，无法完成偏离比对。",
            "compliance_status": "missing",
            "deviation_status": "bid_content_missing",
            "requirement_extraction_mode": "star",
            "core_requirements_count": total,
            "core_star_requirements_count": total,
            "deviation_tables": {
                "business_found": bool(sections["business"]),
                "technical_found": bool(sections["technical"]),
                "business_section_count": len(sections["business"]),
                "technical_section_count": len(sections["technical"]),
            },
            "business_catalog_pages": sections.get("catalog_pages") or [],
            "business_catalog_locations": sections.get("catalog_locations") or [],
            "table_coverage": table_coverage,
            "global_response_statement": None,
            "star_requirements": requirements,
            "match_results": matches,
            "missing_response_items": missing_items,
            "negative_deviation_items": [],
            "unclear_response_items": [],
            "stats": {
                "responded_count": 0,
                "missing_count": total,
                "negative_deviation_count": 0,
                "positive_deviation_count": 0,
                "no_deviation_count": 0,
                "listed_response_count": 0,
                "unclear_deviation_count": 0,
                "explicit_response_count": 0,
                "covered_by_global_statement_count": 0,
                "covered_by_deviation_table_count": 0,
            },
            "key_findings": [
                f"在招标文件中检测到 {total} 条带 ★ 的强制性要求。",
                "投标文件 OCR 内容为空，按响应内容缺失处理。",
            ],
            "extracted_parameters": [x["requirement"] for x in requirements],
        }

    def _extract_pair(self, payload: dict) -> tuple[dict, dict] | None:
        """尝试从单个输入的字典中自动提取招标文件和投标文件的配对。"""
        keys = (
            ("tender_document", "business_bid_document"),
            ("tender", "business_bid"),
            ("tender_json", "business_bid_json"),
            ("招标文件", "商务标文件"),
            ("tender_document", "technical_bid_document"),
            ("tender", "technical_bid"),
            ("tender_json", "technical_bid_json"),
            ("招标文件", "技术标文件"),
            ("tender_document", "bid_document"),
            ("tender", "bid"),
            ("tender_json", "bid_json"),
            ("招标文件", "投标文件"),
        )
        candidates = [payload]
        data = payload.get("data")
        if isinstance(data, dict):
            candidates.append(data)
        docs = payload.get("documents")
        if isinstance(docs, dict):
            candidates.append(docs)
        for container in candidates:
            for tk, bk in keys:
                if tk in container and bk in container:
                    return self._coerce_payload(container[tk]), self._coerce_payload(container[bk])
        return None

    def _overall_status(
        self,
        mandatory_total: int,
        missing: int,
        negative: int,
        unclear: int,
        bonus_total: int = 0,
        bonus_flagged: int = 0,
    ) -> tuple[str, str, str]:
        """根据统计结果生成总体状态和摘要。

        合规判定仅依据 ★ 必须项；△ 加分项未达标只做提示，不导致失败。
        """
        bonus_note = (
            f"；另有 {bonus_total} 条 △ 加分项，其中 {bonus_flagged} 条未达标(缺失/负偏离/不明确)。"
            if bonus_total
            else ""
        )
        if mandatory_total == 0:
            if bonus_total:
                return "pass", "pass", f"未发现 ★ 强制性要求{bonus_note}"
            return "pass", "no_star_requirements", "未发现带 ★ 的强制性要求，已跳过比对。"
        if negative > 0:
            return (
                "fail",
                "fail",
                f"共 {mandatory_total} 条 ★ 强制性要求；缺失={missing}，负偏离={negative}{bonus_note}",
            )
        if missing > 0:
            return (
                "missing",
                "missing_response",
                f"共 {mandatory_total} 条 ★ 强制性要求；缺失={missing}，负偏离={negative}{bonus_note}",
            )
        return (
            "pass",
            "pass",
            f"偏离响应已覆盖全部 {mandatory_total} 条 ★ 强制性要求，且未发现负偏离{bonus_note}",
        )

    def _summary_item(self, item: dict[str, Any], response_status: str) -> dict[str, Any]:
        """构建缺失/负偏离/加分等摘要项的统一字段集（含标记类型与语义判定）。"""
        return {
            "requirement_id": item["requirement_id"],
            "requirement": item["requirement"],
            "marker_type": item.get("marker_type"),
            "requirement_kind": item.get("requirement_kind"),
            "source_type": item.get("source_type"),
            "response_status": response_status,
            "response_evidence": item.get("response_evidence", ""),
            "response_page": item.get("response_page"),
            "response_page_end": item.get("response_page_end"),
            "response_bbox": item.get("response_bbox"),
            "response_document_role": item.get("response_document_role"),
            "response_section": item.get("response_section"),
            "response_section_title": item.get("response_section_title"),
            "material_text": item.get("material_text"),
            "material_locations": item.get("material_locations") or [],
            "requirement_page": item.get("requirement_page"),
            "requirement_bbox": item.get("requirement_bbox"),
            "semantic_score": item.get("semantic_score"),
            "semantic_status": item.get("semantic_status"),
            "needs_manual": item.get("needs_manual"),
        }

    def _apply_semantic_judgement(self, matches: list[dict[str, Any]]) -> None:
        """对每个标记项做语义符合度判定。

        已响应项用 BGE 向量计算「需求 ↔ 响应证据」相似度，≥阈值记为“符合”、
        否则“存疑”；模型不可用、无证据或未响应一律标记为 needs_manual=True，
        确保每个 ★/△ 项都能由人工或模型确认。
        """
        from app.config.settings import settings

        threshold = float(getattr(settings, "DEVIATION_SEMANTIC_PASS_THRESHOLD", 0.70))
        service = None
        try:
            from app.service.analysis.compliance.embedding_service import get_embedding_service

            service = get_embedding_service()
        except Exception:
            service = None

        for item in matches:
            item.setdefault("semantic_score", None)
            item.setdefault("semantic_status", None)
            if not item.get("responded"):
                item["needs_manual"] = True
                continue
            requirement = str(item.get("requirement") or "").strip()
            evidence = str(item.get("response_evidence") or "").strip()
            score: float | None = None
            if service is not None and requirement and evidence:
                try:
                    scores = service.similarities(requirement, [evidence])
                    if scores:
                        score = round(float(scores[0]), 4)
                except Exception:
                    score = None
            if score is None:
                item["semantic_score"] = None
                item["semantic_status"] = None
                item["needs_manual"] = True
                continue
            item["semantic_score"] = score
            if score >= threshold:
                item["semantic_status"] = "符合"
                item["needs_manual"] = False
            else:
                item["semantic_status"] = "存疑"
                item["needs_manual"] = True
