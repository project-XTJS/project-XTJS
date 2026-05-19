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

    def _has_bid_opening_context(self, text: str) -> bool:
        """判断文本是否具备开标一览表常见上下文字段。"""
        if not text or not str(text).strip():
            return False
        normalized = self._normalize(self._strip_price_markup(str(text)))
        meta_hits = sum(
            1
            for tokens in (
                ("项目名称", "采购项目", "标的名称"),
                ("招标编号", "项目编号", "采购编号", "比选编号"),
            )
            if any(token in normalized for token in tokens)
        )
        detail_hits = sum(
            1
            for tokens in (
                ("货币单位", "币种"),
                ("交货期", "交付期", "工期", "服务期"),
                ("交货地点", "交付地点", "服务地点", "实施地点"),
                ("质保期", "保修期", "质保"),
            )
            if any(token in normalized for token in tokens)
        )
        return detail_hits >= 2 or (detail_hits >= 1 and meta_hits >= 1)

    def _bid_total_label_patterns(self) -> list[str]:
        return [
            r"参选总价",
            r"投标总价",
            r"报价总价",
            r"响应总报价",
            r"投标报价总价",
            r"总报价",
            r"最终报价(?:\s*[\(（]?\s*总价\s*[、,，/]?\s*元?\s*[\)）]?)?",
        ]

    def _has_bid_total_amount_signal(
        self, text: str, *, assume_opening_context: bool = False
    ) -> bool:
        """判断页面是否出现了开标一览表中的总价字段和对应金额。"""
        if not text or not str(text).strip():
            return False
        search_text = self._strip_price_markup(str(text))
        context_ok = assume_opening_context or self._has_bid_opening_context(search_text)
        if not context_ok:
            return False

        label_pattern = r"(?:%s)" % "|".join(self._bid_total_label_patterns())
        patterns = [
            label_pattern
            + r"[^\n]{0,20}?(?:小写[：:]?)?\s*[￥¥]?\s*\d[\d,，,]*(?:\.\d+)?\s*元?",
            label_pattern
            + r"[^\n]{0,20}?(?:为人民币)?[^\n]{0,40}?大写[：:]?[零〇壹贰叁肆伍陆柒捌玖拾佰仟万亿圆元角分整正]+"
            r"[^\n]{0,40}?小写[：:]?\s*[￥¥]?\s*\d[\d,，]*(?:\.\d+)?\s*元?",
            r"小写[：:]?\s*[￥¥]?\s*\d[\d,，]*(?:\.\d+)?\s*元?"
            r".{0,60}?大写[：:]?[零〇壹贰叁肆伍陆柒捌玖拾佰仟万亿圆元角分整正]+",
            r"大写[：:]?[零〇壹贰叁肆伍陆柒捌玖拾佰仟万亿圆元角分整正]+"
            r".{0,60}?小写[：:]?\s*[￥¥]?\s*\d[\d,，]*(?:\.\d+)?\s*元?",
        ]
        return any(
            re.search(pattern, search_text, re.IGNORECASE | re.DOTALL)
            for pattern in patterns
        )

    def _looks_like_itemized_total_page(self, text: str) -> bool:
        """识别分项报价合计页面，避免误当成开标一览表。"""
        if not text or not str(text).strip():
            return False
        stripped_text = self._strip_price_markup(str(text))
        normalized_text = self._normalize(stripped_text)
        if any(title in normalized_text for title in self.ITEMIZED_SECTION_TITLES):
            return True
        if "小计" in normalized_text:
            return True

        row_like_hits = 0
        for raw_line in stripped_text.splitlines():
            line = raw_line.strip()
            if not line:
                continue
            compact = self._normalize(line)
            if "小计" in compact:
                row_like_hits += 1
                continue
            if not re.match(r"^\d+(?:\.\d+)*", compact):
                continue
            if not re.search(r"(?:￥|¥|\d[\d,，]*(?:\.\d+)?)", line):
                continue
            if any(unit in compact for unit in ("套", "台", "项", "个", "批", "次", "人", "年", "月")):
                row_like_hits += 1
        return row_like_hits >= 3

    # 开标一览表定位
    def _score_page_candidate(self, page_sections: List[Dict]) -> int:
        page_text = "\n".join(sec["text"] for sec in page_sections if sec["text"])
        normalized_page_text = self._normalize(page_text)
        if self._has_page_heading_title(page_sections, self.ITEMIZED_SECTION_TITLES):
            return -1000
        has_heading_title = self._has_page_heading_title(page_sections, self.BID_OPENING_TITLES)
        has_bid_total_amount = self._has_bid_total_amount_signal(
            page_text, assume_opening_context=has_heading_title
        )
        has_float_rate_keywords = self._contains_float_rate_keywords(page_text)
        itemized_total_like = self._looks_like_itemized_total_page(page_text)
        score = 0
        if has_heading_title:
            score += 18
        elif self._contains_bid_opening_title(page_text):
            score += 6
        if "目录" in normalized_page_text:
            score -= 20
        if any(self._is_catalog_line(sec["text"]) for sec in page_sections):
            score -= 8
        if has_bid_total_amount:
            score += 12
        if has_float_rate_keywords:
            score += 8
        if any(sec.get("type") == "table" for sec in page_sections):
            score += 4 if (has_heading_title or has_bid_total_amount or has_float_rate_keywords) else 1
        if itemized_total_like:
            score -= 16
        if "投标保证书" in normalized_page_text or "比选保证书" in normalized_page_text:
            score -= 10
        if any(token in normalized_page_text for token in ("合同金额", "合同总价", "采购合同", "设备采购合同")):
            score -= 10
        if "偏离表" in normalized_page_text:
            score -= 8
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
        best_page_sections = page_map.get(best_page, [])
        best_has_heading_title = self._has_page_heading_title(
            best_page_sections, self.BID_OPENING_TITLES
        )
        if self._has_bid_total_amount_signal(
            best_text, assume_opening_context=best_has_heading_title
        ):
            return best_page, best_text.strip()
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
            if self._has_bid_total_amount_signal(
                window, assume_opening_context=self._contains_bid_opening_title(window)
            ):
                score += 12
            if self._contains_float_rate_keywords(window):
                score += 8
            if self._looks_like_itemized_total_page(window):
                score -= 16
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

