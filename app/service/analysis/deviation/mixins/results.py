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
    _coerce_payload: Any
    _extract_pair: Any

    def check_technical_deviation(self, tender_document: Any, bid_document: Any | None = None) -> dict:
        """对招标文件和投标文件进行偏离检查。"""
        tender = self._coerce_payload(tender_document)
        if bid_document is None:
            pair = self._extract_pair(tender)
            if not pair:
                return self._single_doc_result(tender)
            tender, bid = pair
        else:
            bid = self._coerce_payload(bid_document)
        return self._run_check(tender, bid)

    def compare_raw_data(self, tender_raw_json: Any, bid_raw_json: Any) -> dict:
        """对外一致性接口。"""
        return self.check_technical_deviation(tender_raw_json, bid_raw_json)

    def _run_check(self, tender_payload: dict, bid_payload: dict) -> dict:
        """核心检查逻辑。"""
        star_requirements = self._extract_star_requirements(tender_payload)
        sections = self._extract_bid_deviation_sections(bid_payload)
        global_stmt = self._detect_global_no_deviation(sections["combined_text"])
        table_coverage = self._collect_table_coverage(sections)

        if not star_requirements:
            return self._build_empty_star_result(sections, global_stmt, table_coverage)

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
                missing_items.append({"requirement_id": item["requirement_id"], "requirement": item["requirement"]})
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
        if missing > 0 or negative > 0:
            return (
                "fail",
                "fail",
                f"共发现 {total} 条带 ★ 的强制性要求；缺失={missing}，负偏离={negative}。",
            )
        return "pass", "pass", "偏离响应部分已覆盖全部带 ★ 的强制性要求，且未发现负偏离。"