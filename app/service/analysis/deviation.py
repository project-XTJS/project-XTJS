"""
偏离条款合规性检查模块
负责人：高海斌
"""

from __future__ import annotations

import json
import re
from difflib import SequenceMatcher
from typing import Any


class DeviationChecker:
    # 仅识别真正星标，不识别 * 乘号
    STAR_RE = re.compile(r"★")

    BUSINESS_TITLES = ("商务条款偏离表", "商务偏离表", "商务条款响应表", "商务偏离")
    TECH_TITLES = ("技术条款偏离表", "技术偏离表", "技术条款响应表", "技术偏离")
    STOP_HINTS = ("投标人基本情况", "资格证明", "报价", "开标一览", "承诺", "目录", "附录", "类似项目", "法定代表人")

    NO_DEV_PATTERNS = (
        r"无偏离",
        r"未偏离",
        r"没有偏离",
        r"偏离说明[:：]?\s*无",
        r"全部响应",
        r"完全响应",
    )
    POS_DEV_PATTERNS = (r"正偏离", r"优于", r"高于", r"更优", r"超出", r"提升")
    NEG_DEV_PATTERNS = (r"负偏离", r"不满足", r"不响应", r"不符合", r"无法", r"未提供", r"低于", r"缺失", r"不支持", r"有偏离")

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
        findings.append(f"合规响应数量（无偏离/正偏离）：{no_dev + positive} 条。")

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
            "summary": "要求响应校验需要同时提供招标 JSON 和技术标 JSON。",
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
        start_idx, end_idx = self._chapter_scope_for_star(lines)
        out: list[dict[str, Any]] = []
        seen = set()
        for i in range(start_idx, end_idx + 1):
            item = lines[i]
            line = item["text"]
            if not self._has_star_marker(line):
                continue
            req = self._clean_req(self._merge_req_line(lines, i, max_idx=end_idx))
            req_norm = self._norm(req)
            if len(req_norm) < 4 or req_norm in seen:
                continue
            seen.add(req_norm)
            section_type = self._infer_section(lines, i)
            out.append(
                {
                    "requirement_id": f"STAR-{len(out)+1:03d}",
                    "requirement": req,
                    "section_type": section_type,
                    "page": item["page"],
                    "line_number": item["line_number"],
                    "normalized_requirement": req_norm,
                    "fragments": self._fragments(req),
                }
            )
        return out

    def _chapter_scope_for_star(self, lines: list[dict[str, Any]]) -> tuple[int, int]:
        """
        仅在 “第三章 项目需求书” 到 “第四章 合同条款（结束）” 的范围内提取星标。
        """
        if not lines:
            return 0, -1

        def compact(text: str) -> str:
            return re.sub(r"\s+", "", str(text or "")).replace("：", "").replace(":", "")

        def is_chapter_heading(text: str) -> bool:
            t = compact(text)
            if not re.match(r"^第[一二三四五六七八九十百0-9]+章", t):
                return False
            if len(re.findall(r"第[一二三四五六七八九十百0-9]+章", t)) > 1:
                return False
            return len(t) <= 36

        def is_chapter3_title(text: str) -> bool:
            t = compact(text)
            if not (t.startswith("第三章") or t.startswith("第3章")):
                return False
            if "项目需求" not in t:
                return False
            if len(re.findall(r"第[一二三四五六七八九十百0-9]+章", t)) > 1:
                return False
            return len(t) <= 36

        def is_chapter4_title(text: str) -> bool:
            t = compact(text)
            if not (t.startswith("第四章") or t.startswith("第4章")):
                return False
            if "合同" not in t:
                return False
            if len(re.findall(r"第[一二三四五六七八九十百0-9]+章", t)) > 1:
                return False
            return len(t) <= 36

        start_idx: int | None = None
        chapter4_idx: int | None = None

        for idx, item in enumerate(lines):
            raw_text = str(item.get("text", ""))
            if start_idx is None and is_chapter3_title(raw_text):
                start_idx = idx
                continue
            if start_idx is not None and chapter4_idx is None and is_chapter4_title(raw_text):
                chapter4_idx = idx
                break

        if start_idx is None:
            return 0, len(lines) - 1
        if chapter4_idx is None:
            return start_idx, len(lines) - 1

        end_idx = len(lines) - 1
        for idx in range(chapter4_idx + 1, len(lines)):
            if is_chapter_heading(lines[idx].get("text", "")):
                end_idx = idx - 1
                break
        if end_idx < start_idx:
            end_idx = len(lines) - 1
        return start_idx, end_idx

    def _extract_bid_deviation_sections(self, bid_payload: dict) -> dict[str, Any]:
        lines = [x["text"] for x in self._page_lines(bid_payload)]
        business = self._collect_sections(lines, self.BUSINESS_TITLES)
        technical = self._collect_sections(lines, self.TECH_TITLES)
        if not business and not technical:
            generic = self._collect_sections(lines, ("偏离表",))
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
        combined = "\n\n".join(x["text"] for x in business + technical if x.get("text"))
        return {"business": business, "technical": technical, "combined_text": combined}

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

    def _match_one_star(self, requirement: dict[str, Any], sections: dict[str, Any]) -> dict:
        search_order = ("technical", "business") if requirement["section_type"] == "technical" else ("business", "technical")
        best = {"matched": False, "score": 0.0, "line": "", "section": "", "title": "", "hits": 0, "long_hit": False}
        req_norm = requirement["normalized_requirement"]
        frags = requirement["fragments"]
        candidate_texts: list[str] = []

        for group in search_order:
            for sec in sections[group]:
                lines = sec.get("lines") or self._split_lines(sec.get("text", ""))

                for line in lines:
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
        elif matched and self._match_patterns(merged_candidates, self.NO_DEV_PATTERNS):
            dev_type = "no_deviation"
        elif matched and self._match_patterns(merged_candidates, self.POS_DEV_PATTERNS):
            dev_type = "positive_deviation"
        else:
            dev_type = self._dev_type(best["line"]) if matched else "unclear"

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
            "match_score": round(float(best["score"]), 4),
            "deviation_type": dev_type,
            "risk_level": "high" if (not matched or dev_type == "negative_deviation") else ("medium" if dev_type == "unclear" else "low"),
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
        if missing > 0 or negative > 0 or unclear > 0:
            return (
                "fail",
                "fail",
                f"共发现 {total} 条带 ★ 的强制性要求；缺失={missing}，负偏离={negative}，不明确={unclear}。",
            )
        return "pass", "pass", "所有带 ★ 的强制性要求均已响应，且结论为无偏离或正偏离。"
    def _extract_pair(self, payload: dict) -> tuple[dict, dict] | None:
        keys = (
            ("tender_document", "technical_bid_document"),
            ("tender", "technical_bid"),
            ("tender_json", "technical_bid_json"),
            ("招标文件", "技术标文件"),
            ("tender_document", "business_bid_document"),
            ("tender", "business_bid"),
            ("tender_json", "business_bid_json"),
            ("招标文件", "商务标文件"),
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

    def _collect_sections(self, lines: list[str], anchors: tuple[str, ...], window: int = 220) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for i, line in enumerate(lines):
            anchor = next((x for x in anchors if x in line), None)
            if not anchor:
                continue
            end = min(len(lines), i + window)
            for c in range(i + 1, end):
                if c - i < 8:
                    continue
                now = lines[c]
                if any(t in now for t in self.BUSINESS_TITLES + self.TECH_TITLES):
                    end = c
                    break
                if self._is_boundary(now):
                    end = c
                    break
            chunk = lines[i:end]
            text = "\n".join(chunk).strip()
            if len(self._norm(text)) >= 20:
                out.append({"title": anchor, "start_line": i + 1, "lines": chunk, "text": text})
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
        for key in ("text", "raw_text", "markdown", "html", "pred_html", "content", "caption"):
            val = section.get(key)
            if isinstance(val, str) and val.strip():
                parts.append(val.strip())

        for key in ("cell_texts", "texts", "rec_texts"):
            val = section.get(key)
            if isinstance(val, list):
                parts.extend(str(x or "").strip() for x in val if str(x or "").strip())

        return "\n".join(self._merge_unique_parts(parts)).strip()

    def _section_items(self, doc: dict) -> list[dict[str, Any]]:
        sections: list[dict[str, Any]] = []
        seen = set()

        for source_idx, key in enumerate(("layout_sections", "table_sections")):
            raw_sections = doc.get(key)
            if not isinstance(raw_sections, list):
                continue
            for item_idx, item in enumerate(raw_sections):
                if isinstance(item, dict):
                    page_raw = item.get("page")
                    section_type = str(item.get("type") or ("table" if key == "table_sections" else "text")).strip().lower() or "text"
                    text = self._section_text(item)
                else:
                    page_raw = None
                    section_type = "table" if key == "table_sections" else "text"
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
            if self._is_boundary(nxt):
                break
            parts.append(nxt)
            if len(self._norm(" ".join(parts))) >= 24:
                break
        return " ".join(parts).strip()

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
        t = re.sub(r"^\s*(第[一二三四五六七八九十百]+[条章节项点]|[一二三四五六七八九十]+[、.．]|[0-9]+[、.．)])\s*", "", t)
        return re.sub(r"\s+", " ", t).strip("，,；; ")

    def _norm(self, text: str) -> str:
        t = self.STAR_RE.sub("", str(text or ""))
        t = re.sub(r"[\s\u3000\xa0]+", "", t)
        t = re.sub(r"[，,。；;：:！？!?（）()【】\[\]《》<>“”\"'‘’、\-_/\\]", "", t)
        return t.lower()

    def _is_boundary(self, line: str) -> bool:
        c = re.sub(r"\s+", "", str(line or ""))
        if not c:
            return False
        if any(h in c for h in self.STOP_HINTS) and "偏离" not in c:
            return True
        return bool(re.match(r"^(第[一二三四五六七八九十百]+[章节部分]|[一二三四五六七八九十]+[、.．]|[0-9]{1,2}[、.．])", c) and len(c) <= 40)

    def _match_patterns(self, text: str, patterns: tuple[str, ...]) -> bool:
        return any(re.search(p, text or "", re.IGNORECASE) for p in patterns)

    def _clip(self, text: str, max_chars: int) -> str:
        t = re.sub(r"\s+", " ", str(text or "").strip())
        return t if len(t) <= max_chars else f"{t[:max_chars].rstrip()}..."

    def _has_star_marker(self, text: str) -> bool:
        return bool(self.STAR_RE.search(text or ""))
