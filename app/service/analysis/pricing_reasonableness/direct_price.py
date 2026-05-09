# pricing_reasonableness/direct_price.py
"""
报价合理性 - 直接报价检查 Mixin

提供大小写金额对的提取、中文大写金额解析以及直接报价校验。
"""

import re
from typing import Any, Dict, List, Optional

class DirectPriceMixin:
    # 依赖常量
    CAPITAL_NUM: dict
    SMALL_UNITS: dict
    BIG_UNITS: dict

    # 小写金额提取
    def _clean_small_price(self, s: str) -> Optional[float]:
        if not s:
            return None
        raw = s.strip()
        raw = raw.replace("￥", "").replace("¥", "")
        raw = raw.replace(",", "").replace("，", "")
        raw = raw.replace(" ", "")
        raw = re.sub(r"(?i)rmb", "", raw)
        raw = raw.replace("元", "")
        match = re.search(r"\d+(?:\.\d+)?", raw)
        if not match:
            return None
        try:
            return float(match.group())
        except ValueError:
            return None

    # 文本净化
    def _strip_price_markup(self, text: str) -> str:
        if not text:
            return ""
        cleaned = str(text)
        cleaned = re.sub(r'\\(?:underline|text)\s*\{', '', cleaned)
        cleaned = cleaned.replace("{", " ").replace("}", " ")
        cleaned = cleaned.replace("$", " ").replace("\\", " ")
        cleaned = cleaned.replace("_", " ")
        return re.sub(r"\s+", " ", cleaned).strip()

    # 中文大写金额解析
    def _parse_capital_integer(self, s: str) -> int:
        total = 0
        section = 0
        number = 0
        for ch in s:
            if ch in self.CAPITAL_NUM:
                number = self.CAPITAL_NUM[ch]
            elif ch in self.SMALL_UNITS:
                unit = self.SMALL_UNITS[ch]
                if number == 0:
                    number = 1
                section += number * unit
                number = 0
            elif ch in self.BIG_UNITS:
                big_unit = self.BIG_UNITS[ch]
                section += number
                if section == 0:
                    section = 1
                total += section * big_unit
                section = 0
                number = 0
        return total + section + number

    def _capital_to_number(self, capital_str: str) -> Optional[float]:
        if not capital_str or not capital_str.strip():
            return None
        s = capital_str.strip()
        s = re.sub(r"\s+", "", s)
        s = s.replace("人民币", "")
        s = s.replace("圆", "元")
        jiao = 0.0
        fen = 0.0
        jiao_match = re.search(r"([零〇壹贰叁肆伍陆柒捌玖])角", s)
        fen_match = re.search(r"([零〇壹贰叁肆伍陆柒捌玖])分", s)
        if jiao_match:
            jiao = self.CAPITAL_NUM.get(jiao_match.group(1), 0) * 0.1
        if fen_match:
            fen = self.CAPITAL_NUM.get(fen_match.group(1), 0) * 0.01
        if "元" in s:
            integer_part = s.split("元")[0]
        else:
            integer_part = re.sub(r"[角分整正]", "", s)
        integer_part = re.sub(r"[整正角分]", "", integer_part)
        if not integer_part:
            integer_value = 0
        else:
            integer_value = self._parse_capital_integer(integer_part)
        return round(integer_value + jiao + fen, 2)

    # 大小写金额对提取
    def _extract_direct_price_pairs(self, section_text: str) -> List[Dict]:
        if not section_text or not section_text.strip():
            return []
        lines = [self._strip_price_markup(line.strip()) for line in section_text.splitlines() if line.strip()]
        pairs = []
        current_small_str = None
        current_small_val = None
        for line in lines:
            inline_match = re.search(
                r"小写[：:]\s*([￥¥]?\s*[\d,，]+(?:\.\d+)?\s*元?)"
                r".{0,50}?"
                r"大写[：:]\s*([零〇壹贰叁肆伍陆柒捌玖拾佰仟万亿元角分整正圆]+)",
                line,
            )
            if inline_match:
                small_str = inline_match.group(1).strip()
                capital_str = inline_match.group(2).strip()
                pairs.append({
                    "small_price_str": small_str,
                    "small_price": self._clean_small_price(small_str),
                    "capital_price_str": capital_str,
                    "capital_price": self._capital_to_number(capital_str),
                })
                current_small_str = None
                current_small_val = None
                continue
            small_match = re.search(
                r"小写[：:]\s*([￥¥]?\s*[\d,，]+(?:\.\d+)?\s*元?)", line
            )
            if small_match:
                current_small_str = small_match.group(1).strip()
                current_small_val = self._clean_small_price(current_small_str)
                capital_match_same_line = re.search(
                    r"大写[：:]\s*([零〇壹贰叁肆伍陆柒捌玖拾佰仟万亿元角分整正圆]+)", line
                )
                if capital_match_same_line:
                    capital_str = capital_match_same_line.group(1).strip()
                    pairs.append({
                        "small_price_str": current_small_str,
                        "small_price": current_small_val,
                        "capital_price_str": capital_str,
                        "capital_price": self._capital_to_number(capital_str),
                    })
                    current_small_str = None
                    current_small_val = None
                continue
            capital_match = re.search(
                r"大写[：:]\s*([零〇壹贰叁肆伍陆柒捌玖拾佰仟万亿元角分整正圆]+)", line
            )
            if capital_match and current_small_str is not None:
                capital_str = capital_match.group(1).strip()
                pairs.append({
                    "small_price_str": current_small_str,
                    "small_price": current_small_val,
                    "capital_price_str": capital_str,
                    "capital_price": self._capital_to_number(capital_str),
                })
                current_small_str = None
                current_small_val = None
        return pairs