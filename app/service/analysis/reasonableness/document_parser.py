# pricing_reasonableness/document_parser.py
"""
报价合理性 - 文档解析 Mixin

负责输入解析（支持 OCR JSON、JSON 字符串、纯文本）、
开标一览表定位、文本合并等。
"""

import json
import os
import re
from typing import Any, Dict, List, Optional, Tuple

class DocumentParserMixin:
    # 依赖常量（来自 __init__ 中定义的实例属性）
    BID_OPENING_TITLES: list
    ITEMIZED_SECTION_TITLES: list
    SECTION_END_TITLES: list

    # 输入解析
    def _parse_input(self, source: Any) -> Dict:
        """将各种输入格式统一解析为内部使用的结构化字典。"""
        if isinstance(source, dict):
            return self._parse_json_dict(source)

        if isinstance(source, str):
            stripped = source.strip()
            if os.path.isfile(stripped) and stripped.lower().endswith(".json"):
                try:
                    with open(stripped, "r", encoding="utf-8") as f:
                        data = json.load(f)
                    return self._parse_json_dict(data)
                except Exception:
                    pass
            if stripped.startswith("{") and stripped.endswith("}"):
                try:
                    data = json.loads(stripped)
                    return self._parse_json_dict(data)
                except Exception:
                    pass
            return {
                "raw_text": source,
                "sections": [{"page": None, "type": "text", "text": source}],
                "table_sections": [],
                "logical_tables": [],
            }

        text = str(source) if source is not None else ""
        return {
            "raw_text": text,
            "sections": [{"page": None, "type": "text", "text": text}],
            "table_sections": [],
            "logical_tables": [],
        }

    def _parse_json_dict(self, data: Dict) -> Dict:
        """从 OCR JSON 结构中扁平化提取 layout_sections、table_sections 等。"""
        payload = data.get("data", data)
        layout_sections = payload.get("layout_sections", []) or []
        table_sections = payload.get("table_sections", []) or []
        logical_tables = payload.get("logical_tables", []) or []

        sections = []
        for sec in layout_sections:
            page = sec.get("page")
            sec_type = sec.get("type", "text")
            text = sec.get("text") or sec.get("raw_text") or ""
            if text:
                sections.append({"page": page, "type": sec_type, "text": text})

        parsed_table_sections = []
        for sec in table_sections:
            page = sec.get("page")
            text = sec.get("text") or sec.get("raw_text") or ""
            if text:
                parsed_table_sections.append({"page": page, "type": "table", "text": text})

        raw_text = "\n".join(sec["text"] for sec in sections if sec["text"])
        return {
            "raw_text": raw_text,
            "sections": sections,
            "table_sections": parsed_table_sections,
            "logical_tables": logical_tables,
        }

    # 开标一览表定位
    def _score_page_candidate(self, page_sections: List[Dict]) -> int:
        page_text = "\n".join(sec["text"] for sec in page_sections if sec["text"])
        normalized_page_text = self._normalize(page_text)
        if self._has_page_heading_title(page_sections, self.ITEMIZED_SECTION_TITLES):
            return -1000
        score = 0
        if self._contains_bid_opening_title(page_text):
            score += 8
        if "目录" in normalized_page_text:
            score -= 20
        if any(self._is_catalog_line(sec["text"]) for sec in page_sections):
            score -= 8
        direct_keys = ["小写", "大写", "参选总价", "投标总价", "报价总价"]
        score += sum(3 for k in direct_keys if k in normalized_page_text)
        float_keys = ["下浮率", "投标下浮率", "税率", "投标报价", "暂估金额", "业务名称"]
        score += sum(3 for k in float_keys if k in normalized_page_text)
        if any(sec.get("type") == "table" for sec in page_sections):
            score += 6
        rule_keys = ["不低于", "不少于", "低于或等于", "否决", "大于", "小于", "须"]
        score += sum(2 for k in rule_keys if k in normalized_page_text)
        return score

    def _group_sections_by_page(self, sections: List[Dict]) -> Dict[int, List[Dict]]:
        page_map: Dict[int, List[Dict]] = {}
        for sec in sections:
            page = sec.get("page")
            if page is None:
                continue
            page_map.setdefault(page, []).append(sec)
        return page_map

    def _locate_bid_opening_page_and_text(self, parsed: Dict) -> Tuple[Optional[int], str]:
        sections = parsed.get("sections", [])
        page_map = self._group_sections_by_page(sections)
        best_page = None
        best_score = -999
        best_text = ""
        for page, page_sections in page_map.items():
            score = self._score_page_candidate(page_sections)
            if score > best_score:
                best_score = score
                best_page = page
                best_text = "\n".join(sec["text"] for sec in page_sections if sec["text"])
        if best_page is None or best_score < 3:
            raw_text = parsed.get("raw_text", "")
            extracted = self._extract_bid_opening_section_from_text(raw_text)
            return None, extracted
        ordered_sections = sections
        collected = []
        started = False
        start_page = best_page
        for sec in ordered_sections:
            page = sec.get("page")
            text = sec.get("text", "")
            normalized = self._normalize(text)
            if page == start_page and not started:
                started = True
            if not started:
                continue
            if page is not None and start_page is not None and page > start_page + 1:
                break
            if any(title in normalized for title in self.SECTION_END_TITLES):
                break
            collected.append(text)
        merged_text = "\n".join(collected).strip()
        if not merged_text:
            merged_text = best_text
        return best_page, merged_text

    def _extract_bid_opening_section_from_text(self, text: str) -> str:
        if not text or not text.strip():
            return ""
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        normalized_lines = [self._normalize(line) for line in lines]
        best_idx = None
        best_score = -999
        for idx, line in enumerate(normalized_lines):
            if not self._contains_bid_opening_title(line):
                continue
            score = 0
            window = "\n".join(lines[idx : idx + 12])
            normalized_window = self._normalize(window)
            if self._is_catalog_line(lines[idx]):
                score -= 12
            if "目录" in normalized_window:
                score -= 12
            for key in ["小写", "大写", "下浮率", "税率", "报价", "投标报价", "暂估金额"]:
                if key in normalized_window:
                    score += 3
            if score > best_score:
                best_score = score
                best_idx = idx
        if best_idx is None:
            return ""
        end_idx = len(lines)
        for idx in range(best_idx + 1, len(normalized_lines)):
            current = normalized_lines[idx]
            if any(title in current for title in self.SECTION_END_TITLES):
                end_idx = idx
                break
        return "\n".join(lines[best_idx:end_idx]).strip()

    # 文本合并（用于招标限价提取）
    def _iter_all_text_blocks(self, parsed: Dict) -> List[Dict]:
        blocks = []
        for sec in parsed.get("sections", []) or []:
            text = sec.get("text") or ""
            if text and str(text).strip():
                blocks.append(
                    {"page": sec.get("page"), "type": sec.get("type", "text"), "text": str(text)}
                )
        for sec in parsed.get("table_sections", []) or []:
            text = sec.get("text") or ""
            if text and str(text).strip():
                blocks.append({"page": sec.get("page"), "type": "table", "text": str(text)})
        return blocks

    def _merge_texts_by_page(self, parsed: Dict) -> Dict[Optional[int], str]:
        page_map: Dict[Optional[int], List[str]] = {}
        for block in self._iter_all_text_blocks(parsed):
            page = block.get("page")
            page_map.setdefault(page, []).append(block.get("text", ""))
        return {
            page: "\n".join([x for x in texts if x and str(x).strip()]).strip()
            for page, texts in page_map.items()
        }