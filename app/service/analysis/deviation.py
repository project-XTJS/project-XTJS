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
    STAR_RE = re.compile(r"[★☆⭐]")
    BUSINESS_TITLES = ("商务条款偏离表", "商务偏离表", "商务条款响应表", "商务偏离")
    TECH_TITLES = ("技术条款偏离表", "技术偏离表", "技术条款响应表", "技术偏离")
    STOP_HINTS = ("投标人基本情况", "资格证明", "报价", "开标一览", "承诺", "目录", "附录", "类似项目", "法定代表人")
    NO_DEV_PATTERNS = (r"无偏离", r"未偏离", r"没有偏离", r"偏离说明[:：]?\s*无", r"全部响应", r"完全响应")
    POS_DEV_PATTERNS = (r"正偏离", r"优于", r"高于", r"更优", r"超出", r"提升")
    NEG_DEV_PATTERNS = (r"负偏离", r"不满足", r"不响应", r"不符合", r"无法", r"未提供", r"低于", r"缺失", r"不支持", r"有偏离")

    def check_technical_deviation(self, tender_document: Any, bid_document: Any | None = None) -> dict:
        """
        支持:
        1) check_technical_deviation(tender_json, bid_json)
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
        stars = self._extract_star_requirements(tender_payload)
        sections = self._extract_bid_deviation_sections(bid_payload)
        global_stmt = self._detect_global_no_deviation(sections["combined_text"])

        matches = [self._match_one_star(item, sections) for item in stars]
        explicit_count = sum(1 for item in matches if item["explicit_response"])
        if stars and explicit_count == 0 and global_stmt["detected"]:
            for item in matches:
                if not item["responded"]:
                    item["responded"] = True
                    item["response_status"] = "covered_by_global_statement"
                    item["response_evidence"] = global_stmt["matched_text"] or "偏离表存在“全部响应/无偏离”声明"
                    item["deviation_type"] = "无偏离"
                    item["risk_level"] = "medium"

        total = len(stars)
        responded = sum(1 for x in matches if x["responded"])
        missing = max(total - responded, 0)
        negative = sum(1 for x in matches if x["deviation_type"] == "负偏离")
        positive = sum(1 for x in matches if x["deviation_type"] == "正偏离")
        no_dev = sum(1 for x in matches if x["deviation_type"] == "无偏离")
        unclear = sum(1 for x in matches if x["deviation_type"] == "未明确")
        covered = sum(1 for x in matches if x["response_status"] == "covered_by_global_statement")

        status, deviation_status, summary = self._overall_status(total, missing, negative, unclear, explicit_count, covered)
        findings = [f"识别到“★”核心条款 {total} 条。"]
        if total:
            findings.append(f"偏离表已响应 {responded} 条，缺失 {missing} 条。")
            if explicit_count == 0 and covered == total:
                findings.append("偏离表采用总括“全部响应/无偏离”，未逐条列示★条款。")
            findings.append(f"负偏离 {negative} 条。")
        else:
            findings.append("招标文件未识别到“★”条款。")

        negatives = [
            {"requirement_id": x["requirement_id"], "requirement": x["requirement"], "response_evidence": x["response_evidence"]}
            for x in matches
            if x["deviation_type"] == "负偏离"
        ]

        return {
            "mode": "tender_bid_json",
            "summary": summary,
            "compliance_status": status,
            "deviation_status": deviation_status,
            "core_star_requirements_count": total,
            "deviation_tables": {
                "business_found": bool(sections["business"]),
                "technical_found": bool(sections["technical"]),
                "business_section_count": len(sections["business"]),
                "technical_section_count": len(sections["technical"]),
            },
            "global_response_statement": global_stmt,
            "star_requirements": stars,
            "match_results": matches,
            "negative_deviation_items": negatives,
            "stats": {
                "responded_count": responded,
                "missing_count": missing,
                "negative_deviation_count": negative,
                "positive_deviation_count": positive,
                "no_deviation_count": no_dev,
                "unclear_deviation_count": unclear,
                "explicit_response_count": explicit_count,
                "covered_by_global_statement_count": covered,
            },
            "key_findings": findings,
            # 向后兼容老字段
            "extracted_parameters": [x["requirement"] for x in stars],
        }

    def _single_doc_result(self, payload: dict) -> dict:
        stars = self._extract_star_requirements(payload)
        sections = self._extract_bid_deviation_sections(payload)
        return {
            "mode": "single_document",
            "summary": "需要同时提供招标文件JSON和投标文件JSON，才能完成“★条款-偏离表”逐项核验。",
            "compliance_status": "manual_review",
            "deviation_status": "insufficient_input",
            "core_star_requirements_count": len(stars),
            "deviation_tables": {
                "business_found": bool(sections["business"]),
                "technical_found": bool(sections["technical"]),
                "business_section_count": len(sections["business"]),
                "technical_section_count": len(sections["technical"]),
            },
            "global_response_statement": self._detect_global_no_deviation(sections["combined_text"]),
            "star_requirements": stars,
            "match_results": [],
            "negative_deviation_items": [],
            "stats": {
                "responded_count": 0,
                "missing_count": len(stars),
                "negative_deviation_count": 0,
                "positive_deviation_count": 0,
                "no_deviation_count": 0,
                "unclear_deviation_count": 0,
                "explicit_response_count": 0,
                "covered_by_global_statement_count": 0,
            },
            "key_findings": ["当前输入不足：仅有单文档。"],
            "extracted_parameters": [x["requirement"] for x in stars],
        }

    def _extract_star_requirements(self, tender_payload: dict) -> list[dict[str, Any]]:
        lines = self._page_lines(tender_payload)
        out: list[dict[str, Any]] = []
        seen = set()
        for i, item in enumerate(lines):
            line = item["text"]
            if not self._has_star_marker(line):
                continue
            req = self._clean_req(self._merge_req_line(lines, i))
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
        combined = "\n\n".join(x["text"] for x in business + technical if x.get("text"))
        return {"business": self._dedupe_sections(business), "technical": self._dedupe_sections(technical), "combined_text": combined}

    def _match_one_star(self, requirement: dict[str, Any], sections: dict[str, Any]) -> dict:
        search_order = ("technical", "business") if requirement["section_type"] == "technical" else ("business", "technical")
        best = {"matched": False, "score": 0.0, "line": "", "section": "", "title": "", "hits": 0, "long_hit": False}
        req_norm = requirement["normalized_requirement"]
        frags = requirement["fragments"]
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
                    if score > best["score"]:
                        best = {"matched": True, "score": score, "line": line.strip(), "section": group, "title": sec.get("title", ""), "hits": hits, "long_hit": long_hit}
                if not best["matched"]:
                    sec_norm = self._norm(sec.get("text", ""))
                    if any(len(f) >= 6 and f in sec_norm for f in frags):
                        best = {"matched": True, "score": 0.58, "line": "命中核心片段", "section": group, "title": sec.get("title", ""), "hits": 1, "long_hit": True}

        matched = bool(best["matched"] and (best["score"] >= 0.72 or (best["hits"] >= 2 and best["score"] >= 0.48) or best["long_hit"]))
        dev_type = self._dev_type(best["line"]) if matched else "未明确"
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
            "risk_level": "high" if (not matched or dev_type == "负偏离") else ("medium" if dev_type == "未明确" else "low"),
        }

    def _dev_type(self, text: str) -> str:
        if self._match_patterns(text, self.NO_DEV_PATTERNS):
            return "无偏离"
        if self._match_patterns(text, self.NEG_DEV_PATTERNS):
            return "负偏离"
        if self._match_patterns(text, self.POS_DEV_PATTERNS):
            return "正偏离"
        return "未明确"

    def _overall_status(self, total: int, missing: int, negative: int, unclear: int, explicit_count: int, covered: int) -> tuple[str, str, str]:
        if total == 0:
            return "pass", "no_star_requirements", "招标文件中未识别到“★”核心条款。"
        if missing > 0:
            return "fail", "fail", f"共识别 {total} 条“★”核心条款，其中 {missing} 条未在偏离表中找到有效响应。"
        if explicit_count == 0 and covered == total:
            return "manual_review", "covered_by_global_statement", "偏离表仅提供“全部响应/无偏离”总括声明，建议人工复核覆盖充分性。"
        if negative > 0:
            return "risk", "risk", f"“★”条款均有响应，但识别到 {negative} 条负偏离，需重点关注。"
        if unclear > 0:
            return "manual_review", "manual_review", f"“★”条款已响应，但 {unclear} 条偏离类型未明确。"
        return "pass", "pass", "“★”核心条款已完成响应，且未发现负偏离。"

    def _extract_pair(self, payload: dict) -> tuple[dict, dict] | None:
        keys = (("tender_document", "bid_document"), ("tender", "bid"), ("tender_json", "bid_json"), ("招标文件", "投标文件"))
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

    def _doc_container(self, payload: dict) -> dict:
        data = payload.get("data")
        if isinstance(data, dict) and any(k in data for k in ("content", "text", "pages", "blocks")):
            return data
        doc = payload.get("document")
        if isinstance(doc, dict) and any(k in doc for k in ("content", "text", "pages", "blocks")):
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
        merged, seen = [], set()
        for p in parts:
            key = self._norm(p)[:200]
            if p and key and key not in seen:
                seen.add(key)
                merged.append(p)
        return "\n".join(merged).strip()

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
        if out:
            return out
        for ln, line in enumerate(self._split_lines(self._extract_text(payload)), start=1):
            out.append({"page": None, "line_number": ln, "text": line})
        return out

    def _merge_req_line(self, lines: list[dict[str, Any]], idx: int) -> str:
        cur = lines[idx]["text"]
        if len(self._norm(cur)) >= 18:
            return cur
        parts = [cur]
        for step in (1, 2):
            c = idx + step
            if c >= len(lines):
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
        segs = re.split(r"[，,。；;：:\s、/|（）()【】《》<>\-]+", self._clean_req(text))
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
        t = str(text or "").replace("\r\n", "\n").replace("\r", "\n").replace("\u3000", " ").replace("\xa0", " ")
        if "\n" not in t:
            t = re.sub(r"([。；;！？!?])", r"\1\n", t)
        return [re.sub(r"[ \t\f\v]+", " ", x).strip() for x in t.split("\n") if x and x.strip()]

    def _clean_req(self, text: str) -> str:
        t = self.STAR_RE.sub("", str(text or ""))
        t = t.replace("*", "")
        t = re.sub(r"^\s*(第[一二三四五六七八九十百]+[条章节项点]|[一二三四五六七八九十]+[、.．]|[0-9]+[、.．)])\s*", "", t)
        return re.sub(r"\s+", " ", t).strip("：:;；- ")

    def _norm(self, text: str) -> str:
        t = self.STAR_RE.sub("", str(text or ""))
        t = t.replace("*", "")
        t = re.sub(r"[\s\u3000\xa0]+", "", t)
        t = re.sub(r"[，,。；;：:（）()【】\[\]《》<>“”\"'‘’·、|丨\-_/\\]", "", t)
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
        if self.STAR_RE.search(text or ""):
            return True
        return bool(re.search(r"\*\s*[A-Za-z0-9\u4e00-\u9fff]", str(text or "")))
