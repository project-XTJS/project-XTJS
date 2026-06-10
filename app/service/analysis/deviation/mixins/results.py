# -*- coding: utf-8 -*-
"""检查结果构建 Mixin"""
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

        if not star_requirements and not tender_template_requirements:
            return self._build_empty_star_result(sections, global_stmt, table_coverage)
        if not star_requirements and not sections.get("technical"):
            return self._build_missing_technical_deviation_table_result(
                tender_template_requirements,
                sections,
                table_coverage,
            )
        if not star_requirements:
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

        missing_items: list[dict[str, Any]] = []
        negative_items: list[dict[str, Any]] = []
        unclear_items: list[dict[str, Any]] = []
        responded = 0
        positive = 0
        no_dev = 0
        listed = 0

        for item in matches:
            dev_type = str(item.get("deviation_type") or "unclear")
            if not item.get("responded"):
                missing_items.append(
                    {
                        "requirement_id": item["requirement_id"],
                        "requirement": item["requirement"],
                        "requirement_page": item.get("requirement_page"),
                        "requirement_bbox": item.get("requirement_bbox"),
                        "response_page": item.get("response_page"),
                        "response_bbox": item.get("response_bbox"),
                        "response_document_role": item.get("response_document_role"),
                        "response_status": "missing",
                    }
                )
                item["response_status"] = "missing"
                item["risk_level"] = "high"
                continue

            responded += 1
            if dev_type == "negative_deviation":
                negative_items.append(
                    {
                        "requirement_id": item["requirement_id"],
                        "requirement": item["requirement"],
                        "response_evidence": item.get("response_evidence", ""),
                        "response_page": item.get("response_page"),
                        "response_bbox": item.get("response_bbox"),
                        "response_document_role": item.get("response_document_role"),
                        "requirement_page": item.get("requirement_page"),
                        "requirement_bbox": item.get("requirement_bbox"),
                    }
                )
                item["response_status"] = "negative_deviation"
                item["risk_level"] = "high"
            elif dev_type == "positive_deviation":
                positive += 1
                item["response_status"] = "positive_deviation"
                item["risk_level"] = "low"
            elif dev_type == "no_deviation":
                no_dev += 1
                item["response_status"] = "no_deviation"
                item["risk_level"] = "low"
            elif dev_type == "listed_response":
                listed += 1
                item["response_status"] = "listed_response"
                item["risk_level"] = "low"
            else:
                unclear_items.append(
                    {
                        "requirement_id": item["requirement_id"],
                        "requirement": item["requirement"],
                        "response_evidence": item.get("response_evidence", ""),
                        "response_page": item.get("response_page"),
                        "response_bbox": item.get("response_bbox"),
                        "response_document_role": item.get("response_document_role"),
                        "requirement_page": item.get("requirement_page"),
                        "requirement_bbox": item.get("requirement_bbox"),
                    }
                )
                item["response_status"] = "unclear_deviation"
                item["risk_level"] = "high"

        total = len(requirements)
        missing = len(missing_items)
        negative = len(negative_items)
        unclear = len(unclear_items)
        status, deviation_status, summary = self._overall_status(total, missing, negative, unclear)

        findings = [f"在招标文件中检测到 {total} 条带 ★ 的强制性要求。"]
        findings.append(f"已响应 {responded} 条，缺失 {missing} 条，负偏离 {negative} 条，不明确 {unclear} 条。")
        findings.append(f"合规响应数量（无偏离/正偏离/列明未负响应）：{no_dev + positive + listed} 条。")

        return {
            "mode": "tender_technical_bid_json",
            "summary": summary,
            "compliance_status": status,
            "deviation_status": deviation_status,
            "requirement_extraction_mode": "star",
            "core_requirements_count": total,
            "core_star_requirements_count": len(star_requirements),
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
            "stats": {
                "responded_count": responded,
                "missing_count": missing,
                "negative_deviation_count": negative,
                "positive_deviation_count": positive,
                "no_deviation_count": no_dev,
                "listed_response_count": listed,
                "unclear_deviation_count": unclear,
                "explicit_response_count": responded,
                "covered_by_global_statement_count": 0,
                "covered_by_deviation_table_count": 0,
            },
            "key_findings": findings,
            "extracted_parameters": [x["requirement"] for x in requirements],
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
        for part in parts:
            for key in ("business", "technical", "rows", "catalog_locations"):
                combined[key].extend(part.get(key) or [])
            for page in part.get("catalog_pages") or []:
                if page not in combined["catalog_pages"]:
                    combined["catalog_pages"].append(page)
            text = str(part.get("combined_text") or "").strip()
            if text:
                combined_text_parts.append(text)
        combined["catalog_pages"] = sorted(page for page in combined["catalog_pages"] if isinstance(page, int))
        combined["combined_text"] = "\n\n".join(combined_text_parts)
        return combined

    def _single_doc_result(self, payload: dict) -> dict:
        """单文档输入时生成提示结果。"""
        star_requirements = self._extract_star_requirements(payload)
        requirements = star_requirements
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

    def _overall_status(self, total: int, missing: int, negative: int, unclear: int) -> tuple[str, str, str]:
        """根据统计结果生成总体状态和摘要。"""
        if total == 0:
            return "pass", "no_star_requirements", "未发现带 ★ 的强制性要求，已跳过比对。"
        if negative > 0:
            return (
                "fail",
                "fail",
                f"共发现 {total} 条带 ★ 的强制性要求；缺失={missing}，负偏离={negative}。",
            )
        if missing > 0:
            return (
                "missing",
                "missing_response",
                f"共发现 {total} 条带 ★ 的强制性要求；缺失={missing}，负偏离={negative}。",
            )
        return "pass", "pass", "偏离响应部分已覆盖全部带 ★ 的强制性要求，且未发现负偏离。"
