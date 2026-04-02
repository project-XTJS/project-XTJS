"""
偏离条款合规性检查模块
负责人：高海斌
"""

from __future__ import annotations

import json
import re
from difflib import SequenceMatcher
from html import unescape
from typing import Any


class DeviationChecker:
    # 仅识别真正星标，不识别 * 乘号
    STAR_RE = re.compile(r"★")
    ITEM_MARKER_RE = re.compile(
        r"(?:★\s*)?[（(]\d{1,2}[)）]|(?:(?<=^)|(?<=\s))\d{1,2}[、.．](?!\d)"
    )
    TABLE_ROW_MARKER_RE = re.compile(
        r"(?:(?<=^)|(?<=\s))\d{1,3}(?:(?:[、.．)](?!\d))|\s+)"
    )

    BUSINESS_TITLES = ("商务条款偏离表", "商务偏离表", "商务条款响应表", "商务偏离")
    TECH_TITLES = ("技术条款偏离表", "技术偏离表", "技术条款响应表", "技术偏离")
    STOP_HINTS = ("投标人基本情况", "资格证明", "报价", "开标一览", "承诺", "目录", "附录", "类似项目", "法定代表人")
    REQUIREMENT_CHAPTER_STRONG_HINTS = (
        "项目需求书",
        "服务需求书",
        "项目需求",
        "服务需求",
        "技术标准和要求",
        "技术标准及要求",
        "工程总承包任务书及技术要求",
        "任务书及技术要求",
        "技术要求",
        "标准和要求",
        "标准及要求",
    )
    REQUIREMENT_CHAPTER_WEAK_HINTS = ("需求", "要求", "标准", "技术", "任务书")
    REQUIREMENT_CHAPTER_EXCLUDE_HINTS = (
        "投标人须知",
        "评标",
        "合同",
        "报价",
        "资格",
        "格式",
        "附录",
        "目录",
        "清单",
        "招标公告",
        "投标文件组成",
    )

    NO_DEV_PATTERNS = (
        r"无偏离",
        r"未偏离",
        r"没有偏离",
        r"偏离说明[:：]?\s*无",
        r"全部响应",
        r"完全响应",
    )
    POS_DEV_PATTERNS = (r"正偏离", r"优于", r"(?<!不)(?<!不得)高于", r"更优", r"超出", r"提升")
    NEG_DEV_PATTERNS = (r"负偏离", r"不满足", r"不响应", r"不符合", r"无法", r"未提供", r"(?<!不)(?<!不得)低于", r"缺失", r"不支持", r"有偏离")

    def check_technical_deviation(self, tender_document: Any, bid_document: Any | None = None) -> dict:
        """
        支持：
        1) check_technical_deviation(招标JSON, 技术标JSON)
        2) check_technical_deviation(single_text_or_json) -> 返回输入不足提示
        """
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
        return self.check_technical_deviation(tender_raw_json, bid_raw_json)

    def _run_check(self, tender_payload: dict, bid_payload: dict) -> dict:
        star_requirements = self._extract_star_requirements(tender_payload)
        sections = self._extract_bid_deviation_sections(bid_payload)
        global_stmt = self._detect_global_no_deviation(sections["combined_text"])
        table_coverage = self._collect_table_coverage(sections)

        # 严格规则：无★则直接通过，不比对
        if not star_requirements:
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
        star_requirements = self._extract_star_requirements(payload)
        requirements = star_requirements
        sections = self._extract_bid_deviation_sections(payload)
        return {
            "mode": "single_document",
            "summary": "要求响应校验需要同时提供招标 JSON 和商务标 JSON。",
            "compliance_status": "manual_review",
            "deviation_status": "insufficient_input",
            "requirement_extraction_mode": "star",
            "core_requirements_count": len(requirements),
            "core_star_requirements_count": len(star_requirements),
            "deviation_tables": {
                "business_found": bool(sections["business"]),
                "technical_found": bool(sections["technical"]),
                "business_section_count": len(sections["business"]),
                "technical_section_count": len(sections["technical"]),
            },
            "global_response_statement": self._detect_global_no_deviation(sections["combined_text"]),
            "star_requirements": requirements,
            "match_results": [],
            "negative_deviation_items": [],
            "stats": {
                "responded_count": 0,
                "missing_count": len(requirements),
                "negative_deviation_count": 0,
                "positive_deviation_count": 0,
                "no_deviation_count": 0,
                "listed_response_count": 0,
                "unclear_deviation_count": 0,
                "explicit_response_count": 0,
                "covered_by_global_statement_count": 0,
                "covered_by_deviation_table_count": 0,
            },
            "key_findings": ["输入不足：当前仅提供了单份文档。"],
            "extracted_parameters": [x["requirement"] for x in requirements],
        }

    def _extract_star_requirements(self, tender_payload: dict) -> list[dict[str, Any]]:
        lines = self._page_lines(tender_payload)
        out: list[dict[str, Any]] = []
        seen = set()
        scopes = self._chapter_scopes_for_star(lines)
        for start_idx, end_idx, chapter_title in scopes:
            for entry in self._iter_star_requirement_entries(
                lines,
                start_idx=start_idx,
                end_idx=end_idx,
                chapter_title=chapter_title,
            ):
                req = self._clean_req(entry["text"])
                req_norm = self._norm(req)
                if len(req_norm) < 4 or req_norm in seen:
                    continue
                seen.add(req_norm)
                out.append(
                    {
                        "requirement_id": f"STAR-{len(out)+1:03d}",
                        "requirement": req,
                        "section_type": entry["section_type"],
                        "page": entry["page"],
                        "line_number": entry["line_number"],
                        "normalized_requirement": req_norm,
                        "fragments": self._fragments(req),
                        "chapter_title": entry["chapter_title"],
                    }
                )
        return out

    def _chapter_scopes_for_star(self, lines: list[dict[str, Any]]) -> list[tuple[int, int, str]]:
        """
        在“需求/要求/标准/任务书”类章节中提取星标条款，不限定必须是第三章。
        """
        if not lines:
            return []

        def compact(text: str) -> str:
            return re.sub(r"\s+", "", str(text or "")).replace("：", "").replace(":", "")

        def is_chapter_heading(text: str) -> bool:
            t = compact(text)
            if not re.match(r"^第[一二三四五六七八九十百0-9]+章", t):
                return False
            if len(re.findall(r"第[一二三四五六七八九十百0-9]+章", t)) > 1:
                return False
            return len(t) <= 36

        def chapter_score(text: str) -> int:
            title = compact(text)
            if not title or not is_chapter_heading(title):
                return 0
            if any(token in title for token in self.REQUIREMENT_CHAPTER_EXCLUDE_HINTS):
                return 0

            score = 0
            for token in self.REQUIREMENT_CHAPTER_STRONG_HINTS:
                if token in title:
                    score += 6
            for token in self.REQUIREMENT_CHAPTER_WEAK_HINTS:
                if token in title:
                    score += 2
            if "技术" in title:
                score += 2
            return score

        chapter_starts = [
            idx for idx, item in enumerate(lines) if is_chapter_heading(str(item.get("text", "")))
        ]
        if not chapter_starts:
            return [(0, len(lines) - 1, "full_document")]

        scopes: list[tuple[int, int, str, int]] = []
        for position, start_idx in enumerate(chapter_starts):
            end_idx = (
                chapter_starts[position + 1] - 1
                if position + 1 < len(chapter_starts)
                else len(lines) - 1
            )
            title = str(lines[start_idx].get("text", ""))
            score = chapter_score(title)
            if score <= 0:
                continue
            scopes.append((start_idx, end_idx, title, score))

        if not scopes:
            return [(0, len(lines) - 1, "full_document")]

        best_score = max(score for _, _, _, score in scopes)
        selected = [
            (start_idx, end_idx, title)
            for start_idx, end_idx, title, score in scopes
            if score >= max(2, best_score - 2)
        ]
        return selected

    def _extract_bid_deviation_sections(self, bid_payload: dict) -> dict[str, Any]:
        line_items = self._page_lines(bid_payload)
        business = self._collect_sections(line_items, self.BUSINESS_TITLES)
        technical = self._collect_sections(line_items, self.TECH_TITLES)
        if not business and not technical:
            generic = self._collect_sections(line_items, ("偏离表",))
            for sec in generic:
                head = "\n".join((sec.get("lines") or [])[:3])
                if "技术" in head:
                    technical.append(sec)
                elif "商务" in head:
                    business.append(sec)
                else:
                    business.append(sec)
                    technical.append(sec)

        business = self._dedupe_sections(business)
        technical = self._dedupe_sections(technical)
        rows = self._extract_deviation_rows(bid_payload, business, technical)
        combined = "\n\n".join(x["text"] for x in business + technical if x.get("text"))
        return {"business": business, "technical": technical, "combined_text": combined, "rows": rows}

    def _is_table_row_start(self, line: str) -> bool:
        return bool(re.match(r"^\s*\d{1,3}(?:\s*[.,)\u3001\uff0e\uff09]|\s+)", str(line or "")))  

    def _looks_like_response_row(self, line: str) -> bool:
        text = str(line or "")
        if not self._is_table_row_start(text):
            return False
        if re.search(r"\bP\d{1,3}(?:-P?\d{1,3})?\b", text, re.IGNORECASE):
            return True
        if "偏离" in text or "响应" in text:
            return True
        return False

    def _collect_table_coverage(self, sections: dict[str, Any]) -> dict[str, dict[str, Any]]:
        coverage: dict[str, dict[str, Any]] = {}
        for group in ("business", "technical"):
            best = {
                "covered": False,
                "title": "",
                "row_count": 0,
                "response_row_count": 0,
                "sample": "",
            }
            best_score = (0, 0, 0)
            for sec in sections.get(group) or []:
                lines = sec.get("lines") or self._split_lines(sec.get("text", ""))
                row_lines = [ln for ln in lines if self._is_table_row_start(ln)]
                response_lines = [ln for ln in lines if self._looks_like_response_row(ln)]
                row_count = len(row_lines)
                response_row_count = len(response_lines)
                covered = bool(response_row_count > 0 or row_count >= 2)
                score = (1 if covered else 0, response_row_count, row_count)
                if score > best_score:
                    best_score = score
                    best = {
                        "covered": covered,
                        "title": str(sec.get("title") or ""),
                        "row_count": row_count,
                        "response_row_count": response_row_count,
                        "sample": str((response_lines or row_lines or [""])[0]).strip(),
                    }
            coverage[group] = best
        return coverage

    def _extract_deviation_rows(
        self,
        bid_payload: dict,
        business_sections: list[dict[str, Any]],
        technical_sections: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        doc = self._doc_container(bid_payload)
        section_hints = technical_sections + business_sections

        logical_tables = doc.get("logical_tables")
        if isinstance(logical_tables, list):
            for table in logical_tables:
                if isinstance(table, dict):
                    rows.extend(self._extract_rows_from_logical_table(table, section_hints=section_hints))

        for section in technical_sections:
            rows.extend(self._extract_rows_from_section(section, "technical"))
        for section in business_sections:
            rows.extend(self._extract_rows_from_section(section, "business"))

        out: list[dict[str, Any]] = []
        seen = set()
        for row in rows:
            joined_key = self._norm(row.get("joined_text", ""))[:260]
            if not joined_key or joined_key in seen:
                continue
            seen.add(joined_key)
            out.append(row)
        return out

    def _extract_rows_from_logical_table(
        self,
        table: dict[str, Any],
        *,
        section_hints: list[dict[str, Any]] | None = None,
    ) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        headers = [str(x or "").strip() for x in (table.get("headers") or [])]
        pages = [x for x in (table.get("pages") or []) if isinstance(x, int)]
        page_no = pages[0] if pages else None
        if page_no is None:
            raw_page = table.get("page")
            try:
                page_no = int(raw_page) if raw_page is not None else None
            except (TypeError, ValueError):
                page_no = None
        title = self._resolve_logical_table_title(table, page_no=page_no, section_hints=section_hints)

        records = table.get("records")
        if isinstance(records, list):
            for record in records:
                if isinstance(record, dict):
                    row = self._build_row_from_record(record, headers=headers, page_no=page_no, title=title)
                    if row:
                        out.append(row)
        if out:
            return out

        rows = table.get("rows")
        if not isinstance(rows, list):
            native_headers, native_records = self._extract_native_table_records(table)
            for record in native_records:
                row = self._build_row_from_record(record, headers=native_headers, page_no=page_no, title=title)
                if row:
                    out.append(row)
            return out

        for values in rows:
            if not isinstance(values, list):
                continue
            record = {}
            for idx, value in enumerate(values):
                key = headers[idx] if idx < len(headers) else f"col_{idx + 1}"
                record[str(key)] = value
            row = self._build_row_from_record(record, headers=headers, page_no=page_no, title=title)
            if row:
                out.append(row)

        if out:
            return out

        native_headers, native_records = self._extract_native_table_records(table)
        for record in native_records:
            row = self._build_row_from_record(record, headers=native_headers, page_no=page_no, title=title)
            if row:
                out.append(row)
        return out

    def _extract_native_table_records(self, table: dict[str, Any]) -> tuple[list[str], list[dict[str, Any]]]:
        raw_html = table.get("block_content") or table.get("html") or table.get("text") or ""
        if not isinstance(raw_html, str) or not raw_html.strip():
            return [], []

        html_rows = self._parse_html_table_rows(raw_html)
        if not html_rows:
            return [], []

        header_like = self._looks_like_header_row(html_rows[0])
        headers = html_rows[0] if header_like else [f"col_{idx + 1}" for idx in range(len(html_rows[0]))]
        data_rows = html_rows[1:] if header_like else html_rows

        records: list[dict[str, Any]] = []
        for values in data_rows:
            if not isinstance(values, list):
                continue
            record = {
                headers[idx] if idx < len(headers) else f"col_{idx + 1}": str(value or "").strip()
                for idx, value in enumerate(values)
                if str(value or "").strip()
            }
            if record:
                records.append(record)

        return headers, records

    def _parse_html_table_rows(self, raw_html: str) -> list[list[str]]:
        html = str(raw_html or "")
        if "<tr" not in html.lower() or ("<td" not in html.lower() and "<th" not in html.lower()):
            return []

        rows: list[list[str]] = []
        for row_html in re.findall(r"(?is)<tr\b[^>]*>(.*?)</tr>", html):
            cells: list[str] = []
            for cell_html in re.findall(r"(?is)<t[dh]\b[^>]*>(.*?)</t[dh]>", row_html):
                cell_text = self._normalize_markup_text(cell_html, preserve_lines=False)
                if cell_text:
                    cells.append(cell_text)
                else:
                    cells.append("")
            if any(cell.strip() for cell in cells):
                rows.append(cells)
        return rows

    def _looks_like_header_row(self, values: list[str]) -> bool:
        if not isinstance(values, list) or not values:
            return False
        joined = "".join(str(value or "").strip() for value in values)
        return any(token in joined for token in ("需求", "要求", "条款", "响应", "应答", "偏离", "说明", "备注"))

    def _resolve_logical_table_title(
        self,
        table: dict[str, Any],
        *,
        page_no: int | None,
        section_hints: list[dict[str, Any]] | None = None,
    ) -> str:
        best_nearby_title = ""
        best_nearby_rank: tuple[int, int] | None = None
        for section in section_hints or []:
            if not isinstance(section, dict):
                continue
            title = str(section.get("title") or "").strip()
            if not title:
                continue
            section_page = section.get("page")
            if page_no is not None and section_page == page_no:
                return title
            if not isinstance(page_no, int) or not isinstance(section_page, int):
                continue
            distance = abs(section_page - page_no)
            if distance > 4:
                continue
            rank = (distance, 0 if section_page <= page_no else 1)
            if best_nearby_rank is None or rank < best_nearby_rank:
                best_nearby_rank = rank
                best_nearby_title = title

        if best_nearby_title:
            return best_nearby_title

        for candidate in (
            table.get("title"),
            table.get("caption"),
            table.get("name"),
            table.get("id"),
            table.get("block_id"),
            table.get("block_label"),
        ):
            title = str(candidate or "").strip()
            if title and not self._is_generic_table_title(title):
                return title

        return f"第{page_no}页表格" if page_no is not None else "logical_table"

    def _is_generic_table_title(self, title: str) -> bool:
        compact = re.sub(r"\s+", "", str(title or ""))
        if not compact:
            return True
        if compact.lower() in {"table", "logical_table"}:
            return True
        return bool(re.fullmatch(r"(?:table_)?\d+", compact, re.IGNORECASE))

    def _build_row_from_record(
        self,
        record: dict[str, Any],
        *,
        headers: list[str],
        page_no: int | None,
        title: str,
    ) -> dict[str, Any] | None:
        ordered_keys = list(record.keys())
        if headers and all(str(h or "").strip() in record for h in headers):
            ordered_keys = [str(h or "").strip() for h in headers]

        requirement_parts: list[str] = []
        response_parts: list[str] = []
        deviation_parts: list[str] = []
        ordered_cells: list[tuple[str, str]] = []

        for key in ordered_keys:
            value = str(record.get(key) or "").strip()
            if not value:
                continue
            ordered_cells.append((str(key or "").strip(), value))
            role = self._column_role(key)
            if role == "requirement":
                requirement_parts.append(value)
            elif role == "response":
                response_parts.append(value)
            elif role == "deviation":
                deviation_parts.append(value)

        if not ordered_cells:
            return None

        if not requirement_parts:
            inferred = self._infer_generic_row_columns(ordered_cells)
            if inferred is None:
                return None
            requirement, response, deviation = inferred
        else:
            requirement = " ".join(requirement_parts).strip()
            response = " ".join(response_parts).strip()
            deviation = " ".join(deviation_parts).strip()

        joined_text = " ".join(part for part in (requirement, response, deviation) if part).strip()
        if len(self._norm(requirement or joined_text)) < 4:
            return None

        return {
            "group": self._guess_row_group(title, joined_text),
            "source": "logical_table",
            "page": page_no,
            "title": title,
            "requirement_text": requirement,
            "response_text": response,
            "deviation_text": deviation,
            "joined_text": joined_text,
            "requirement_norm": self._norm(requirement),
            "response_norm": self._norm(response),
            "deviation_norm": self._norm(deviation),
            "joined_norm": self._norm(joined_text),
        }

    def _infer_generic_row_columns(self, ordered_cells: list[tuple[str, str]]) -> tuple[str, str, str] | None:
        values = [value for _, value in ordered_cells if value]
        if len(values) < 2:
            return None

        dev_idx: int | None = None
        page_idx: int | None = None
        req_idx: int | None = None

        for idx, value in enumerate(values):
            if dev_idx is None and self._match_patterns(value, self.NO_DEV_PATTERNS + self.POS_DEV_PATTERNS + self.NEG_DEV_PATTERNS):
                dev_idx = idx
            if page_idx is None and re.search(r"\bP?\d{1,4}(?:\s*[-~]\s*P?\d{1,4})?\b", value, re.IGNORECASE):
                page_idx = idx
            if req_idx is None and ("★" in value or len(self._norm(value)) >= 10):
                req_idx = idx

        if req_idx is None:
            return None

        search_limit = dev_idx if dev_idx is not None else (page_idx if page_idx is not None else len(values))
        if req_idx >= search_limit:
            return None

        requirement = values[req_idx]
        response = ""
        for idx in range(req_idx + 1, search_limit):
            candidate = values[idx]
            if candidate and candidate != requirement:
                response = candidate
                break

        deviation = values[dev_idx] if dev_idx is not None else ""
        if not response and not deviation:
            return None
        return requirement, response, deviation

    def _column_role(self, label: str) -> str | None:
        text = str(label or "").strip().lower()
        if not text:
            return None
        if any(token in text for token in ("招标文件的招标需求", "招标需求", "招标文件需求", "需求", "要求", "条款")):
            return "requirement"
        if any(token in text for token in ("投标文件的响应", "投标响应", "响应内容", "响应", "应答", "回复")):
            return "response"
        if any(token in text for token in ("偏离说明", "偏离", "说明", "备注")):
            return "deviation"
        return None

    def _extract_rows_from_section(self, section: dict[str, Any], group: str) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        title = str(section.get("title") or "")
        raw_text = re.sub(r"\s+", " ", str(section.get("text") or "")).strip()
        for segment in self._split_table_row_segments(raw_text):
            normalized = self._norm(segment)
            if len(normalized) < 8:
                continue
            has_response_marker = bool(
                "响应" in segment
                or self._match_patterns(segment, self.NO_DEV_PATTERNS + self.POS_DEV_PATTERNS + self.NEG_DEV_PATTERNS)
                or re.search(r"\bP\d{1,4}(?:\s*[-~]\s*P?\d{1,4})?\b", segment, re.IGNORECASE)
            )
            if not ("★" in segment or has_response_marker):
                continue
            out.append(
                {
                    "group": group,
                    "source": "section_text",
                    "page": section.get("page"),
                    "title": title,
                    "requirement_text": segment,
                    "response_text": segment if has_response_marker else "",
                    "deviation_text": segment if self._match_patterns(segment, self.NO_DEV_PATTERNS + self.POS_DEV_PATTERNS + self.NEG_DEV_PATTERNS) else "",
                    "joined_text": segment,
                    "requirement_norm": normalized,
                    "response_norm": self._norm(segment) if has_response_marker else "",
                    "deviation_norm": self._norm(segment) if self._match_patterns(segment, self.NO_DEV_PATTERNS + self.POS_DEV_PATTERNS + self.NEG_DEV_PATTERNS) else "",
                    "joined_norm": normalized,
                }
            )
        return out

    def _split_table_row_segments(self, text: str) -> list[str]:
        raw = re.sub(r"\s+", " ", str(text or "")).strip()
        if not raw:
            return []

        segments: list[str] = []
        _, numbered_segments = self._split_numbered_segments(raw)
        if numbered_segments:
            segments.extend(numbered_segments)

        if not segments:
            matches = list(self.TABLE_ROW_MARKER_RE.finditer(raw))
            if len(matches) >= 2:
                for idx, match in enumerate(matches):
                    start = match.start()
                    end = matches[idx + 1].start() if idx + 1 < len(matches) else len(raw)
                    segment = raw[start:end].strip()
                    if segment:
                        segments.append(segment)

        if not segments and ("★" in raw or "偏离" in raw or "响应" in raw):
            segments = [raw]

        merged_segments: list[str] = []
        idx = 0
        while idx < len(segments):
            segment = segments[idx]
            if re.match(r"^\s*\d{1,3}(?:[、.．)]?)\s*$", segment) and idx + 1 < len(segments):
                segment = f"{segment} {segments[idx + 1]}".strip()
                idx += 1
            if idx + 1 < len(segments):
                current_sub = re.search(r"[（(]\d{1,2}[)）]", segment)
                next_sub = re.match(r"^\s*[（(]\d{1,2}[)）]", segments[idx + 1])
                if (
                    current_sub
                    and next_sub
                    and current_sub.group(0).replace("(", "（").replace(")", "）")
                    == next_sub.group(0).strip().replace("(", "（").replace(")", "）")
                    and "响应" not in segment
                    and not self._match_patterns(segment, self.NO_DEV_PATTERNS + self.POS_DEV_PATTERNS + self.NEG_DEV_PATTERNS)
                    and not re.search(r"\bP\d{1,4}(?:\s*[-~]\s*P?\d{1,4})?\b", segment, re.IGNORECASE)
                ):
                    segment = f"{segment} {segments[idx + 1]}".strip()
                    idx += 1
            merged_segments.append(segment)
            idx += 1

        out: list[str] = []
        seen = set()
        for segment in merged_segments:
            key = self._norm(segment)[:220]
            if key and key not in seen:
                seen.add(key)
                out.append(segment)
        return out

    def _guess_row_group(self, title: str, text: str) -> str:
        joined = f"{title}\n{text}"
        if any(token in joined for token in ("商务", "合同", "付款", "交货", "质保", "资质", "售后", "工期")):
            return "business"
        if any(token in joined for token in ("技术", "指标", "参数", "性能", "功能", "配置", "温度", "增益", "频率")):
            return "technical"
        return "unknown"

    def _match_one_star_from_rows(self, requirement: dict[str, Any], rows: list[dict[str, Any]]) -> dict | None:
        req_norm = requirement["normalized_requirement"]
        frags = requirement["fragments"]
        best_row: dict[str, Any] | None = None
        best_rank = (-1, -1, -1.0)
        best_score = 0.0
        best_hits = 0
        best_long_hit = False

        for row in rows:
            candidates = [row.get("requirement_norm", ""), row.get("joined_norm", "")]
            row_score = 0.0
            row_hits = 0
            row_long_hit = False
            for candidate in candidates:
                if not candidate:
                    continue
                compare_left = req_norm[:160]
                compare_right = candidate[: max(160, min(len(candidate), len(req_norm) + 40))]
                ratio = SequenceMatcher(None, compare_left, compare_right).ratio()
                hits = sum(1 for frag in frags if frag and frag in candidate)
                long_hit = any(len(frag) >= 6 and frag in candidate for frag in frags)
                contains = req_norm in candidate or candidate in req_norm
                score = ratio + min(hits, 3) * 0.22 + (0.45 if contains else 0.0)
                if score > row_score:
                    row_score = score
                    row_hits = hits
                    row_long_hit = long_hit

            matched = bool(row_score >= 0.68 or (row_hits >= 2 and row_score >= 0.48) or row_long_hit)
            if not matched:
                continue

            row_has_response = self._row_has_response(row)
            rank = (
                1 if row_has_response else 0,
                1 if row.get("source") == "logical_table" else 0,
                row_score,
            )
            if rank > best_rank:
                best_rank = rank
                best_score = row_score
                best_hits = row_hits
                best_long_hit = row_long_hit
                best_row = row

        if best_row is None:
            return None

        analysis_text = "\n".join(
            part for part in (best_row.get("response_text", ""), best_row.get("deviation_text", ""), best_row.get("joined_text", "")) if part
        )
        responded = self._row_has_response(best_row)
        if not responded:
            dev_type = "missing"
        elif self._match_patterns(analysis_text, self.NEG_DEV_PATTERNS):
            dev_type = "negative_deviation"
        elif self._match_patterns(analysis_text, self.POS_DEV_PATTERNS):
            dev_type = "positive_deviation"
        elif self._match_patterns(analysis_text, self.NO_DEV_PATTERNS):
            dev_type = "no_deviation"
        else:
            dev_type = "listed_response"

        evidence = best_row.get("response_text") or best_row.get("deviation_text") or best_row.get("joined_text", "")
        return {
            "requirement_id": requirement["requirement_id"],
            "requirement": requirement["requirement"],
            "section_type": requirement["section_type"],
            "responded": responded,
            "explicit_response": responded,
            "response_status": "responded" if responded else "missing",
            "response_evidence": self._clip(evidence, 240) if responded else "",
            "response_section": best_row.get("group", ""),
            "response_section_title": best_row.get("title", ""),
            "response_page": best_row.get("page"),
            "response_line_number": None,
            "match_score": round(float(best_score), 4),
            "deviation_type": dev_type,
            "risk_level": "high" if (not responded or dev_type == "negative_deviation") else "low",
            "_match_hits": best_hits,
            "_match_long_hit": best_long_hit,
        }

    def _row_has_response(self, row: dict[str, Any]) -> bool:
        if row.get("response_norm") or row.get("deviation_norm"):
            return True
        joined = str(row.get("joined_text") or "")
        if "响应" in joined:
            return True
        if self._match_patterns(joined, self.NO_DEV_PATTERNS + self.POS_DEV_PATTERNS + self.NEG_DEV_PATTERNS):
            return True
        return bool(re.search(r"\bP\d{1,4}(?:\s*[-~]\s*P?\d{1,4})?\b", joined, re.IGNORECASE))

    def _match_one_star(self, requirement: dict[str, Any], sections: dict[str, Any]) -> dict:
        row_match = self._match_one_star_from_rows(requirement, sections.get("rows") or [])
        if row_match is not None:
            row_match.pop("_match_hits", None)
            row_match.pop("_match_long_hit", None)
            return row_match

        search_order = ("technical", "business") if requirement["section_type"] == "technical" else ("business", "technical")
        best = {
            "matched": False,
            "score": 0.0,
            "line": "",
            "section": "",
            "title": "",
            "page": None,
            "line_number": None,
            "hits": 0,
            "long_hit": False,
        }
        req_norm = requirement["normalized_requirement"]
        frags = requirement["fragments"]
        candidate_texts: list[str] = []

        for group in search_order:
            for sec in sections[group]:
                section_line_items = sec.get("line_items")
                if isinstance(section_line_items, list) and section_line_items:
                    iter_items = section_line_items
                else:
                    iter_items = [
                        {"page": sec.get("page"), "line_number": None, "text": line}
                        for line in (sec.get("lines") or self._split_lines(sec.get("text", "")))
                    ]

                for item in iter_items:
                    line = str(item.get("text") or "")
                    line_norm = self._norm(line)
                    if len(line_norm) < 2:
                        continue
                    ratio = SequenceMatcher(None, req_norm[:120], line_norm[:120]).ratio()
                    hits = sum(1 for f in frags if f and f in line_norm)
                    long_hit = any(len(f) >= 6 and f in line_norm for f in frags)
                    score = ratio + min(hits, 3) * 0.22 + (0.35 if (req_norm in line_norm or line_norm in req_norm) else 0.0)
                    if "偏离" in line:
                        score += 0.05

                    # 收集多个可能命中的条款，再统一判断偏离类型。
                    line_is_hit = bool(score >= 0.62 or (hits >= 2 and score >= 0.45) or long_hit)
                    if line_is_hit:
                        candidate_texts.append(str(line or "").strip())

                    if score > best["score"]:
                        best = {
                            "matched": True,
                            "score": score,
                            "line": line.strip(),
                            "section": group,
                            "title": sec.get("title", ""),
                            "page": item.get("page"),
                            "line_number": item.get("line_number"),
                            "hits": hits,
                            "long_hit": long_hit,
                        }

                if not best["matched"]:
                    sec_norm = self._norm(sec.get("text", ""))
                    if any(len(f) >= 6 and f in sec_norm for f in frags):
                        best = {
                            "matched": True,
                            "score": 0.58,
                            "line": "core_fragment_hit",
                            "section": group,
                            "title": sec.get("title", ""),
                            "page": sec.get("page"),
                            "line_number": sec.get("start_line"),
                            "hits": 1,
                            "long_hit": True,
                        }

        strict_match = bool(best["matched"] and (best["score"] >= 0.72 or (best["hits"] >= 2 and best["score"] >= 0.48) or best["long_hit"]))
        matched = bool(strict_match or candidate_texts)

        # 判定优先级：
        # 1) 只要存在负偏离就判定为负偏离
        # 2) 否则只要存在无偏离/正偏离就判定为通过
        # 3) 其余情况判定为不明确
        merged_candidates = "\n".join(candidate_texts)
        if matched and self._match_patterns(merged_candidates, self.NEG_DEV_PATTERNS):
            dev_type = "negative_deviation"
        elif matched and self._match_patterns(merged_candidates, self.POS_DEV_PATTERNS):
            dev_type = "positive_deviation"
        elif matched and self._match_patterns(merged_candidates, self.NO_DEV_PATTERNS):
            dev_type = "no_deviation"
        elif matched:
            dev_type = "listed_response"
        else:
            dev_type = "missing"

        return {
            "requirement_id": requirement["requirement_id"],
            "requirement": requirement["requirement"],
            "section_type": requirement["section_type"],
            "responded": matched,
            "explicit_response": matched,
            "response_status": "responded" if matched else "missing",
            "response_evidence": best["line"] if matched else "",
            "response_section": best["section"],
            "response_section_title": best["title"],
            "response_page": best["page"],
            "response_line_number": best["line_number"],
            "match_score": round(float(best["score"]), 4),
            "deviation_type": dev_type,
            "risk_level": "high" if (not matched or dev_type == "negative_deviation") else "low",
        }

    def _dev_type(self, text: str) -> str:
        if self._match_patterns(text, self.NO_DEV_PATTERNS):
            return "no_deviation"
        if self._match_patterns(text, self.NEG_DEV_PATTERNS):
            return "negative_deviation"
        if self._match_patterns(text, self.POS_DEV_PATTERNS):
            return "positive_deviation"
        return "unclear"

    def _overall_status(self, total: int, missing: int, negative: int, unclear: int) -> tuple[str, str, str]:
        if total == 0:
            return "pass", "no_star_requirements", "未发现带 ★ 的强制性要求，已跳过比对。"
        if missing > 0 or negative > 0:
            return (
                "fail",
                "fail",
                f"共发现 {total} 条带 ★ 的强制性要求；缺失={missing}，负偏离={negative}。",
            )
        return "pass", "pass", "偏离响应部分已覆盖全部带 ★ 的强制性要求，且未发现负偏离。"

    def _extract_pair(self, payload: dict) -> tuple[dict, dict] | None:
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

    def _collect_sections(
        self,
        line_items: list[dict[str, Any]],
        anchors: tuple[str, ...],
        window: int = 220,
    ) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        texts = [str(item.get("text") or "") for item in line_items]
        for i, item in enumerate(line_items):
            line = texts[i]
            anchor = next((x for x in anchors if x in line), None)
            if not anchor:
                continue
            if self._is_catalog_like_line(line):
                continue
            end = min(len(line_items), i + window)
            table_mode = any(
                "招标文件" in probe and "投标文件" in probe and ("响应" in probe or "偏离" in probe)
                for probe in texts[i : min(i + 12, len(texts))]
            )
            for c in range(i + 1, end):
                if c - i < 8:
                    continue
                now = texts[c]
                if any(t in now for t in self.BUSINESS_TITLES + self.TECH_TITLES):
                    end = c
                    break
                if table_mode:
                    now_compact = re.sub(r"\s+", "", now)
                    if any(h in now for h in self.STOP_HINTS) and "偏离" not in now and len(now_compact) <= 40:
                        end = c
                        break
                elif self._is_section_boundary(now):
                    end = c
                    break
            chunk_items = line_items[i:end]
            chunk = [str(chunk_item.get("text") or "") for chunk_item in chunk_items]
            text = "\n".join(chunk).strip()
            if len(self._norm(text)) >= 20:
                out.append(
                    {
                        "title": anchor,
                        "page": item.get("page"),
                        "start_line": item.get("line_number", i + 1),
                        "lines": chunk,
                        "line_items": chunk_items,
                        "text": text,
                    }
                )
        return out

    def _dedupe_sections(self, sections: list[dict[str, Any]]) -> list[dict[str, Any]]:
        out = []
        seen = set()
        for s in sections:
            key = self._norm(s.get("text", ""))[:240]
            if key and key not in seen:
                seen.add(key)
                out.append(s)
        return out

    def _detect_global_no_deviation(self, text: str) -> dict:
        pats = (
            r"(全部|所有).{0,8}(响应|满足).{0,18}(无偏离|未偏离|没有偏离)",
            r"(无偏离|未偏离).{0,18}(全部|所有).{0,8}(响应|满足)",
            r"完全响应.{0,20}(要求|条款).{0,20}(无偏离|未偏离|没有偏离)",
        )
        for p in pats:
            m = re.search(p, text or "", re.IGNORECASE | re.DOTALL)
            if m:
                return {"detected": True, "matched_text": self._clip(m.group(0), 120), "coverage_type": "global_no_deviation_statement"}
        return {"detected": False, "matched_text": "", "coverage_type": "none"}

    def _coerce_payload(self, value: Any) -> dict:
        if isinstance(value, dict):
            return value
        if isinstance(value, str):
            raw = value.strip()
            if raw.startswith("{") or raw.startswith("["):
                try:
                    loaded = json.loads(raw)
                except json.JSONDecodeError:
                    return {"content": value}
                return loaded if isinstance(loaded, dict) else {"data": loaded}
            return {"content": value}
        return {}

    def _has_extractable_fields(self, obj: Any) -> bool:
        if not isinstance(obj, dict):
            return False
        return any(
            key in obj
            for key in (
                "content",
                "text",
                "pages",
                "blocks",
                "layout_sections",
                "table_sections",
                "logical_tables",
            )
        )

    def _merge_unique_parts(self, parts: list[str], *, norm_cap: int = 240) -> list[str]:
        merged: list[str] = []
        seen = set()
        for item in parts:
            text = str(item or "").strip()
            key = self._norm(text)[:norm_cap]
            if text and key and key not in seen:
                seen.add(key)
                merged.append(text)
        return merged

    def _section_text(self, section: Any) -> str:
        if isinstance(section, str):
            return section.strip()
        if not isinstance(section, dict):
            return ""

        parts: list[str] = []
        for key in ("text", "raw_text", "markdown", "html", "pred_html", "content", "caption", "block_content"):
            val = section.get(key)
            if isinstance(val, str) and val.strip():
                parts.append(self._normalize_markup_text(val, preserve_lines=key in {"html", "pred_html", "block_content"}))

        for key in ("cell_texts", "texts", "rec_texts", "headers"):
            val = section.get(key)
            if isinstance(val, list):
                parts.extend(
                    self._normalize_markup_text(x, preserve_lines=False)
                    for x in val
                    if self._normalize_markup_text(x, preserve_lines=False)
                )

        rows = section.get("rows")
        if isinstance(rows, list):
            for row in rows:
                if isinstance(row, list):
                    parts.append(" ".join(str(x or "").strip() for x in row if str(x or "").strip()))
                elif isinstance(row, dict):
                    parts.append(" ".join(str(x or "").strip() for x in row.values() if str(x or "").strip()))

        records = section.get("records")
        if isinstance(records, list):
            for row in records:
                if isinstance(row, dict):
                    parts.append(" ".join(str(x or "").strip() for x in row.values() if str(x or "").strip()))

        for key, val in section.items():
            if key.startswith("col_") and isinstance(val, str) and val.strip():
                parts.append(self._normalize_markup_text(val, preserve_lines=False))

        return "\n".join(self._merge_unique_parts(parts)).strip()

    def _normalize_markup_text(self, value: Any, *, preserve_lines: bool) -> str:
        text = unescape(str(value or ""))
        if not text.strip():
            return ""

        text = re.sub(r"(?is)<img\b[^>]*alt=['\"]([^'\"]*)['\"][^>]*>", r" \1 ", text)
        text = re.sub(r"(?is)<img\b[^>]*>", " ", text)

        if preserve_lines:
            text = re.sub(r"(?i)<br\s*/?>", "\n", text)
            text = re.sub(r"(?i)</t[dh]>", "\t", text)
            text = re.sub(r"(?i)</tr>", "\n", text)
            text = re.sub(r"(?i)</?(table|thead|tbody|tfoot|tr|p|div|section|article)[^>]*>", "\n", text)
            text = re.sub(r"(?i)</?(td|th)[^>]*>", " ", text)
            text = re.sub(r"<[^>]+>", " ", text)
            text = text.replace("\r\n", "\n").replace("\r", "\n")
            text = re.sub(r"[^\S\n\t]+", " ", text)
            text = re.sub(r" *\t *", "\t", text)
            text = re.sub(r"[ \t]*\n[ \t]*", "\n", text)
            text = re.sub(r"\n{3,}", "\n\n", text)
            lines: list[str] = []
            for raw_line in text.splitlines():
                cells = [re.sub(r" {2,}", " ", cell).strip() for cell in raw_line.split("\t")]
                cleaned = "\t".join(cell for cell in cells if cell).strip()
                if cleaned:
                    lines.append(cleaned)
            return "\n".join(lines)

        text = re.sub(r"<[^>]+>", " ", text)
        text = re.sub(r"\s+", " ", text).strip()
        return text

    def _section_items(self, doc: dict) -> list[dict[str, Any]]:
        sections: list[dict[str, Any]] = []
        seen = set()

        for source_idx, key in enumerate(("layout_sections", "table_sections", "logical_tables")):
            raw_sections = doc.get(key)
            if not isinstance(raw_sections, list):
                continue
            for item_idx, item in enumerate(raw_sections):
                if isinstance(item, dict):
                    page_raw = item.get("page")
                    if page_raw is None and key == "logical_tables":
                        pages = item.get("pages")
                        if isinstance(pages, list) and pages:
                            page_raw = pages[0]
                    default_type = "table" if key in ("table_sections", "logical_tables") else "text"
                    section_type = str(item.get("type") or default_type).strip().lower() or "text"
                    text = self._section_text(item)
                else:
                    page_raw = None
                    section_type = "table" if key in ("table_sections", "logical_tables") else "text"
                    text = str(item or "").strip()

                if not text:
                    continue

                page_no: int | None
                try:
                    page_no = int(page_raw) if page_raw is not None else None
                except (TypeError, ValueError):
                    page_no = None

                signature = (page_no, section_type, self._norm(text)[:260])
                if not signature[2] or signature in seen:
                    continue
                seen.add(signature)
                sections.append(
                    {
                        "page": page_no,
                        "type": section_type,
                        "text": text,
                        "_source_order": source_idx,
                        "_item_order": item_idx,
                    }
                )

        sections.sort(
            key=lambda x: (
                x["page"] if isinstance(x.get("page"), int) else 10**9,
                x.get("_source_order", 0),
                x.get("_item_order", 0),
            )
        )
        return sections
    def _doc_container(self, payload: dict) -> dict:
        if self._has_extractable_fields(payload):
            return payload
        data = payload.get("data")
        if self._has_extractable_fields(data):
            return data
        doc = payload.get("document")
        if self._has_extractable_fields(doc):
            return doc
        return payload

    def _extract_text(self, payload: dict) -> str:
        doc = self._doc_container(payload)
        parts: list[str] = []
        for key in ("content", "text"):
            val = doc.get(key)
            if isinstance(val, str) and val.strip():
                parts.append(val.strip())
        pages = doc.get("pages")
        if isinstance(pages, list):
            parts.extend(str((x.get("text") if isinstance(x, dict) else x) or "").strip() for x in pages)
        blocks = doc.get("blocks")
        if isinstance(blocks, list):
            parts.extend(str((x.get("text") if isinstance(x, dict) else "") or "").strip() for x in blocks)
        parts.extend(section["text"] for section in self._section_items(doc))
        return "\n".join(self._merge_unique_parts(parts)).strip()

    def _page_lines(self, payload: dict) -> list[dict[str, Any]]:
        doc = self._doc_container(payload)
        pages = doc.get("pages")
        out: list[dict[str, Any]] = []
        if isinstance(pages, list):
            for idx, page in enumerate(pages, start=1):
                page_no, text = idx, ""
                if isinstance(page, dict):
                    page_no = int(page.get("page") or idx)
                    text = str(page.get("text") or "")
                else:
                    text = str(page or "")
                for ln, line in enumerate(self._split_lines(text), start=1):
                    out.append({"page": page_no, "line_number": ln, "text": line})

        if not out:
            section_line_counter: dict[int | None, int] = {}
            for section in self._section_items(doc):
                page_no = section.get("page")
                for line in self._split_lines(section.get("text", "")):
                    current = section_line_counter.get(page_no, 0) + 1
                    section_line_counter[page_no] = current
                    out.append({"page": page_no, "line_number": current, "text": line})

        if out:
            return out
        for ln, line in enumerate(self._split_lines(self._extract_text(payload)), start=1):
            out.append({"page": None, "line_number": ln, "text": line})
        return out

    def _merge_req_line(self, lines: list[dict[str, Any]], idx: int, max_idx: int | None = None) -> str:
        cur = lines[idx]["text"]
        if len(self._norm(cur)) >= 18:
            return cur
        parts = [cur]
        upper = len(lines) - 1 if max_idx is None else min(max_idx, len(lines) - 1)
        for step in (1, 2):
            c = idx + step
            if c > upper:
                break
            nxt = lines[c]["text"]
            if self._has_star_marker(nxt):
                break
            if self._is_boundary(nxt):
                break
            parts.append(nxt)
            if len(self._norm(" ".join(parts))) >= 24:
                break
        return " ".join(parts).strip()

    def _iter_star_requirement_entries(
        self,
        lines: list[dict[str, Any]],
        *,
        start_idx: int,
        end_idx: int,
        chapter_title: str,
    ) -> list[dict[str, Any]]:
        entries: list[dict[str, Any]] = []
        current: dict[str, Any] | None = None

        def flush_current() -> None:
            nonlocal current
            if not current:
                return
            merged = " ".join(str(part or "").strip() for part in current["parts"] if str(part or "").strip())
            merged = re.sub(r"\s+", " ", merged).strip()
            if merged and self._has_star_marker(merged):
                entries.append(
                    {
                        "text": merged,
                        "page": current["page"],
                        "line_number": current["line_number"],
                        "section_type": current["section_type"],
                        "chapter_title": current["chapter_title"],
                    }
                )
            current = None

        for idx in range(start_idx, end_idx + 1):
            item = lines[idx]
            line = str(item.get("text") or "").strip()
            if not line:
                continue

            prefix, segments = self._split_numbered_segments(line)
            if segments:
                if prefix and current is not None:
                    current["parts"].append(prefix)
                elif prefix and current is None and self._has_star_marker(prefix):
                    segments[0] = f"{prefix} {segments[0]}".strip()

                is_boundary_line = self._is_boundary(line)
                for segment in segments:
                    flush_current()
                    if is_boundary_line and not self._has_star_marker(segment):
                        continue
                    current = {
                        "page": item["page"],
                        "line_number": item["line_number"],
                        "section_type": self._infer_section(lines, idx),
                        "chapter_title": chapter_title,
                        "parts": [segment],
                    }
                continue

            if current is not None and self._can_append_requirement_line(current, line, item["page"]):
                current["parts"].append(line)
                continue

            flush_current()
            if self._has_star_marker(line):
                current = {
                    "page": item["page"],
                    "line_number": item["line_number"],
                    "section_type": self._infer_section(lines, idx),
                    "chapter_title": chapter_title,
                    "parts": [line],
                }

        flush_current()
        return entries

    def _split_numbered_segments(self, text: str) -> tuple[str, list[str]]:
        raw = str(text or "").strip()
        if not raw:
            return "", []

        matches = list(self.ITEM_MARKER_RE.finditer(raw))
        if not matches:
            return raw, []

        prefix = raw[: matches[0].start()].strip()
        segments: list[str] = []
        for idx, match in enumerate(matches):
            start = match.start()
            end = matches[idx + 1].start() if idx + 1 < len(matches) else len(raw)
            segment = raw[start:end].strip()
            if segment:
                segments.append(segment)
        return prefix, segments

    def _can_append_requirement_line(
        self,
        current: dict[str, Any],
        line: str,
        page_no: int | None,
    ) -> bool:
        if self._is_boundary(line):
            return False
        merged = " ".join(str(part or "").strip() for part in current.get("parts", []) if str(part or "").strip())
        merged = re.sub(r"\s+", " ", merged).strip()
        if merged and re.search(r"[。！？!?]\s*$", merged):
            return False
        if current.get("page") != page_no and merged and re.search(r"[；;]\s*$", merged):
            return False
        return True

    def _infer_section(self, lines: list[dict[str, Any]], idx: int) -> str:
        ctx = "\n".join(x["text"] for x in lines[max(0, idx - 6) : idx + 1])
        if any(k in ctx for k in ("技术", "参数", "指标", "性能", "配置", "功能")):
            return "technical"
        if any(k in ctx for k in ("商务", "合同", "付款", "交付", "工期", "资质", "资格")):
            return "business"
        return "unknown"

    def _fragments(self, text: str) -> list[str]:
        segs = re.split(r"[，,。；;：:\s（）()【】《》\"'‘’、\-]+", self._clean_req(text))
        vals = []
        for s in segs:
            n = self._norm(s)
            if len(n) >= 4:
                vals.append(n)
        if not vals:
            n = self._norm(text)
            if len(n) >= 4:
                vals = [n[: min(12, len(n))]] + ([n[-10:]] if len(n) > 14 else [])
        out, seen = [], set()
        for v in sorted(vals, key=len, reverse=True):
            if v not in seen:
                seen.add(v)
                out.append(v)
            if len(out) >= 6:
                break
        return out

    def _split_lines(self, text: str) -> list[str]:
        t = (
            str(text or "")
            .replace("\\r\\n", "\n")
            .replace("\\n", "\n")
            .replace("\\r", "\n")
            .replace("\r\n", "\n")
            .replace("\r", "\n")
            .replace("\u3000", " ")
            .replace("\xa0", " ")
        )
        if "\n" not in t:
            t = re.sub(r"([。；;！？!?])", r"\1\n", t)
        return [re.sub(r"[ \t\f\v]+", " ", x).strip() for x in t.split("\n") if x and x.strip()]

    def _clean_req(self, text: str) -> str:
        t = self.STAR_RE.sub("", str(text or ""))
        t = self._normalize_math_text(t)
        t = re.sub(
            r"^\s*(?:第[一二三四五六七八九十百]+[条章节项点]|[一二三四五六七八九十]+[、.．]|[0-9]+[、.．)]|[（(]\d{1,2}[)）])\s*",
            "",
            t,
        )
        return re.sub(r"\s+", " ", t).strip("，,；; ")

    def _norm(self, text: str) -> str:
        t = self.STAR_RE.sub("", str(text or ""))
        t = self._normalize_math_text(t)
        t = re.sub(r"[\s\u3000\xa0]+", "", t)
        t = t.replace("℃", "c").replace("°c", "c").replace("°C", "c").replace("°", "")
        t = t.replace("×", "x").replace("∗", "*")
        t = re.sub(r"[，,。；;：:！？!?（）()【】\[\]《》<>“”\"'‘’、\-_/\\]", "", t)
        return t.lower()

    def _normalize_math_text(self, text: str) -> str:
        t = str(text or "")
        replacements = (
            ("\\leq", "≤"),
            ("\\geq", "≥"),
            ("\\pm", "±"),
            ("\\times", "×"),
            ("\\sim", "~"),
            ("\\cdot", "·"),
            ("\\mu", "μ"),
        )
        for source, target in replacements:
            t = t.replace(source, target)

        t = re.sub(r"\\mathrm\s*\{\s*c\s*\}", "℃", t, flags=re.IGNORECASE)
        t = re.sub(r"\^\s*\{\s*\\circ\s*\}", "°", t, flags=re.IGNORECASE)
        t = re.sub(r"\^\s*\{\s*([0-9]+)\s*\}", r"\1", t)
        t = re.sub(r"\\(?:text|mathrm|operatorname)\s*\{([^{}]*)\}", r"\1", t)
        t = re.sub(r"[$^{}]", "", t)
        return re.sub(r"\s+", " ", t).strip()

    def _is_boundary(self, line: str) -> bool:
        c = re.sub(r"\s+", "", str(line or ""))
        if not c:
            return False
        if any(h in c for h in self.STOP_HINTS) and "偏离" not in c:
            return True
        return bool(re.match(r"^(第[一二三四五六七八九十百]+[章节部分]|[一二三四五六七八九十]+[、.．]|[0-9]{1,2}[、.．])", c) and len(c) <= 40)

    def _is_section_boundary(self, line: str) -> bool:
        c = re.sub(r"\s+", "", str(line or ""))
        if not c:
            return False
        if any(h in c for h in self.STOP_HINTS) and "偏离" not in c:
            return True
        return bool(re.match(r"^(第[一二三四五六七八九十百]+[章节部分]|[一二三四五六七八九十]+[、.．])", c) and len(c) <= 40)

    def _match_patterns(self, text: str, patterns: tuple[str, ...]) -> bool:
        return any(re.search(p, text or "", re.IGNORECASE) for p in patterns)

    def _clip(self, text: str, max_chars: int) -> str:
        t = re.sub(r"\s+", " ", str(text or "").strip())
        return t if len(t) <= max_chars else f"{t[:max_chars].rstrip()}..."

    def _has_star_marker(self, text: str) -> bool:
        return bool(self.STAR_RE.search(text or ""))

    def _is_catalog_like_line(self, line: str) -> bool:
        compact = re.sub(r"\s+", "", str(line or ""))
        if not compact:
            return False
        if "目录" in compact:
            return True
        if re.search(r"(?:\.{2,}|…{2,}|。{2,})\d{1,4}$", compact):
            return True
        return len(re.findall(r"(?:\.{2,}|…{2,}|。{2,})\d{1,4}", compact)) >= 2
