# -*- coding: utf-8 -*-
"""星标条款匹配 Mixin"""
import re
from difflib import SequenceMatcher
from typing import Any


class MatchMixin:
    """负责将星标条款与偏离表行/段落进行匹配。"""

    # 依赖常量
    NO_DEV_PATTERNS: tuple
    POS_DEV_PATTERNS: tuple
    NEG_DEV_PATTERNS: tuple

    # 依赖工具
    _norm: Any
    _match_patterns: Any
    _clip: Any
    _has_star_marker: Any
    _split_lines: Any

    def _match_one_star(self, requirement: dict[str, Any], sections: dict[str, Any]) -> dict:
        """综合匹配一条星标要求：优先匹配偏离表行，其次全文段落。"""
        row_match = self._match_one_star_from_rows(requirement, sections.get("rows") or [])
        if row_match is not None:
            row_match.pop("_match_hits", None)
            row_match.pop("_match_long_hit", None)
            # 行级精定位：偏离表常把多条★塞进同一行（行的起始页会让多条★塌缩到同一页）。
            # 命中行后再在投标全文行级里定位到该要求真正所在的那一行，刷新 response_page/bbox，提升每条★的页码精度。
            located = self._locate_best_response_line(requirement, sections)
            if located and located.get("page"):
                row_match["response_page"] = located["page"]
                if located.get("bbox") not in (None, "", []):
                    row_match["response_bbox"] = located["bbox"]
                if located.get("line_number") is not None:
                    row_match["response_line_number"] = located["line_number"]
                if located.get("document_role"):
                    row_match["response_document_role"] = located["document_role"]
            # 偏离表常整块只标起始页 → 把响应页扩成整张偏离表跨度（保证响应一定在展示范围内）
            row_match["response_page_end"] = self._deviation_table_end_page(
                sections, row_match.get("response_page")
            )
            return row_match

        search_order = ("technical", "business") if requirement["section_type"] == "technical" else ("business", "technical")
        best = {
            "matched": False,
            "score": 0.0,
            "line": "",
            "section": "",
            "title": "",
            "page": None,
            "bbox": None,
            "document_role": None,
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
                        {"page": sec.get("page"), "bbox": sec.get("bbox"), "line_number": None, "text": line}
                        for line in (sec.get("lines") or self._split_lines(sec.get("text", "")))
                    ]

                for _idx, item in enumerate(iter_items):
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
                            "bbox": item.get("bbox"),
                            "document_role": sec.get("document_role"),
                            "line_number": item.get("line_number"),
                            "hits": hits,
                            "long_hit": long_hit,
                            "_items": iter_items,
                            "_idx": _idx,
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
                            "bbox": sec.get("bbox"),
                            "document_role": sec.get("document_role"),
                            "line_number": sec.get("start_line"),
                            "hits": 1,
                            "long_hit": True,
                        }

        strict_match = bool(best["matched"] and (best["score"] >= 0.72 or (best["hits"] >= 2 and best["score"] >= 0.48) or best["long_hit"]))
        matched = bool(strict_match or candidate_texts)

        merged_candidates = "\n".join(candidate_texts)
        # "对应材料"页码：先扫匹配文本；扫不到则从匹配行往后看几行（同一偏离表行的"对应材料"单元格，
        # 扫描件/文本表里需求与材料常被 OCR 拆到相邻行），遇到下一条★/▲即停。
        material_text = self._scan_material_text(merged_candidates + " " + str(best.get("line") or ""))
        if not material_text and best.get("_items") is not None and best.get("_idx") is not None:
            window = []
            items = best["_items"]
            # 往后扫至下一条★前（响应单元格可能很长），找"对应材料"页码
            for it in items[best["_idx"] + 1: best["_idx"] + 30]:
                txt = str((it or {}).get("text") or "")
                if "★" in txt or "▲" in txt or "△" in txt:
                    break
                window.append(txt)
                if self._scan_material_text(txt):
                    break
            material_text = self._scan_material_text(" ".join(window))
        material_locations = self._parse_material_locations(material_text)

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
            "marker_type": requirement.get("marker_type", "star"),
            "requirement_kind": requirement.get("requirement_kind", "mandatory"),
            "requirement_page": requirement.get("page"),
            "requirement_bbox": requirement.get("bbox"),
            "section_type": requirement["section_type"],
            "responded": matched,
            "explicit_response": matched,
            "response_status": "responded" if matched else "missing",
            "response_evidence": best["line"] if matched else "",
            "response_section": best["section"],
            "response_section_title": best["title"],
            "response_page": best["page"],
            "response_page_end": self._deviation_table_end_page(sections, best["page"]),
            "response_bbox": best.get("bbox"),
            "response_document_role": best.get("document_role"),
            "response_line_number": best["line_number"],
            # 全文匹配路径：匹配文本 + 往后看几行（同行的"对应材料"单元格）扫出的页码引用
            "material_text": material_text,
            "material_locations": material_locations,
            "match_score": round(float(best["score"]), 4),
            "deviation_type": dev_type,
            "risk_level": "high" if (not matched or dev_type == "negative_deviation") else "low",
        }

    def _deviation_table_end_page(self, sections: dict[str, Any], start_page: Any) -> Any:
        """把"响应所在页"扩成整张偏离表的结束页 = 下一张表起始页 - 1。

        偏离表常被解析成一整块、所有行只标起始页（逐行真实视觉页在 OCR 阶段已丢失），
        无法逐条★精确定位；用"下一张表起始页 - 1"作为偏离表结束边界，把投标侧展示范围
        覆盖整张偏离表，确保该★的响应一定落在范围内。取不到时回退为起始页本身。
        """
        if not isinstance(start_page, int):
            return None
        starts = sections.get("table_start_pages") or []
        nxt = next((p for p in starts if isinstance(p, int) and p > start_page), None)
        end = (nxt - 1) if nxt else start_page
        return end if end >= start_page else start_page

    def _locate_best_response_line(self, requirement: dict[str, Any], sections: dict[str, Any]) -> dict | None:
        """在投标全文行级里定位与该要求最匹配的一行，返回精确的 page/bbox。

        用于在偏离表行命中后细化页码：偏离表常把多条★合并到一行，行的起始页会让多条★
        都落到同一页；这里按要求文本在行级（line_items）找最强匹配行，落到其真实页码。
        仅在足够强（含长片段/包含/高相似度）时返回，避免乱跳。
        """
        search_order = (
            ("technical", "business")
            if requirement.get("section_type") == "technical"
            else ("business", "technical")
        )
        req_norm = requirement["normalized_requirement"]
        frags = requirement["fragments"]
        best: dict[str, Any] | None = None
        best_score = 0.0

        for group in search_order:
            for sec in sections.get(group, []):
                section_line_items = sec.get("line_items")
                if isinstance(section_line_items, list) and section_line_items:
                    iter_items = section_line_items
                else:
                    iter_items = [
                        {"page": sec.get("page"), "bbox": sec.get("bbox"), "line_number": None, "text": line}
                        for line in (sec.get("lines") or self._split_lines(sec.get("text", "")))
                    ]

                for item in iter_items:
                    if not item.get("page"):
                        continue
                    line_norm = self._norm(str(item.get("text") or ""))
                    if len(line_norm) < 4:
                        continue
                    ratio = SequenceMatcher(None, req_norm[:120], line_norm[:120]).ratio()
                    hits = sum(1 for f in frags if f and f in line_norm)
                    long_hit = any(len(f) >= 6 and f in line_norm for f in frags)
                    contains = req_norm in line_norm or line_norm in req_norm
                    score = ratio + min(hits, 3) * 0.22 + (0.35 if contains else 0.0)
                    # 精定位需较强证据，避免把页码跳到无关行
                    strong = bool(long_hit or contains or (hits >= 2 and ratio >= 0.4) or ratio >= 0.7)
                    if strong and score > best_score:
                        best_score = score
                        best = {
                            "page": item.get("page"),
                            "bbox": item.get("bbox"),
                            "line_number": item.get("line_number"),
                            "document_role": sec.get("document_role"),
                            "score": score,
                        }

        return best

    def _match_one_star_from_rows(self, requirement: dict[str, Any], rows: list[dict[str, Any]]) -> dict | None:
        """从已解析的表格行中寻找与某条星标要求最佳匹配的行。"""
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
            "marker_type": requirement.get("marker_type", "star"),
            "requirement_kind": requirement.get("requirement_kind", "mandatory"),
            "requirement_page": requirement.get("page"),
            "requirement_bbox": requirement.get("bbox"),
            "section_type": requirement["section_type"],
            "responded": responded,
            "explicit_response": responded,
            "response_status": "responded" if responded else "missing",
            "response_evidence": self._clip(evidence, 240) if responded else "",
            "response_section": best_row.get("group", ""),
            "response_section_title": best_row.get("title", ""),
            "response_page": best_row.get("page"),
            "response_bbox": best_row.get("bbox"),
            "response_document_role": best_row.get("document_role"),
            "response_line_number": None,
            # "对应材料投标文件所在页"列：指向技术册/商务册具体页，供人工跳转核验
            "material_text": best_row.get("material_text") or "",
            "material_locations": best_row.get("material_locations") or [],
            "match_score": round(float(best_score), 4),
            "deviation_type": dev_type,
            "risk_level": "high" if (not responded or dev_type == "negative_deviation") else "low",
            "_match_hits": best_hits,
            "_match_long_hit": best_long_hit,
        }

    def _row_has_response(self, row: dict[str, Any]) -> bool:
        """判断偏离行是否实际包含响应内容。"""
        if row.get("response_norm") or row.get("deviation_norm"):
            return True
        joined = str(row.get("joined_text") or "")
        if "响应" in joined:
            return True
        if self._match_patterns(joined, self.NO_DEV_PATTERNS + self.POS_DEV_PATTERNS + self.NEG_DEV_PATTERNS):
            return True
        return bool(re.search(r"\bP\d{1,4}(?:\s*[-~]\s*P?\d{1,4})?\b", joined, re.IGNORECASE))
