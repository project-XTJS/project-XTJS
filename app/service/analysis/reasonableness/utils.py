# pricing_reasonableness/utils.py
"""
报价合理性 - 基础工具 Mixin

提供文本归一化、数字清洗、结果构建、关键词判断等基础方法。
"""

from typing import Any, Dict, List, Optional


class UtilsMixin:
    """基础工具 Mixin，作为 ReasonablenessChecker 的基类之一。"""

    # 需要由最终子类提供的常量（在 __init__ 中设置）
    CAPITAL_NUM: dict
    SMALL_UNITS: dict
    BIG_UNITS: dict
    BID_OPENING_TITLES: list
    ITEMIZED_SECTION_TITLES: list
    FLOAT_RULE_PHRASES: list
    RATE_QUOTE_KEYWORDS: list
    DISCOUNT_RATE_KEYWORDS: list
    COMMON_TAX_RATES: set

    # 文本归一化
    def _normalize(self, s: str) -> str:
        """去除所有空白字符，便于关键词匹配。"""
        if s is None:
            return ""
        return __import__("re").sub(r"\s+", "", str(s))

    # 关键词检测
    def _contains_bid_opening_title(self, text: str) -> bool:
        """判断文本中是否包含开标/报价一览表的标题关键词。"""
        normalized = self._normalize(text)
        return any(title in normalized for title in self.BID_OPENING_TITLES)

    def _has_page_heading_title(self, page_sections: List[Dict], titles: List[str]) -> bool:
        """判断某一页的区段列表中是否出现了给定的 heading 标题。"""
        import re
        for sec in page_sections:
            text = str(sec.get("text") or "").strip()
            if not text:
                continue
            normalized = self._normalize(text)
            if not any(title in normalized for title in titles):
                continue
            if self._is_catalog_line(text):
                continue

            section_type = str(sec.get("type") or "").strip().lower()
            if section_type == "heading":
                return True

            compact = re.sub(r"\s+", "", text)
            if len(compact) <= 40:
                return True
        return False

    def _contains_direct_price_keywords(self, text: str) -> bool:
        """判断文本中是否含有直接报价的大写/小写特征。"""
        normalized = self._normalize(text)
        return (
            ("小写" in normalized and "大写" in normalized)
            or "参选总价" in normalized
            or "投标总价" in normalized
            or "报价总价" in normalized
        )

    def _contains_float_rate_keywords(self, text: str) -> bool:
        """判断文本中是否含有下浮率/折扣率报价特征关键词。"""
        normalized = self._normalize(text)
        return (
            any(keyword in normalized for keyword in getattr(self, "RATE_QUOTE_KEYWORDS", ["下浮率"]))
            or ("税率" in normalized and "报价" in normalized)
        )

    def _contains_discount_rate_keywords(self, text: str) -> bool:
        """判断文本中是否含有折扣率报价特征关键词。"""
        normalized = self._normalize(text)
        return any(keyword in normalized for keyword in getattr(self, "DISCOUNT_RATE_KEYWORDS", ["折扣率"]))

    # 数字清洗
    def _safe_float(self, value: str) -> Optional[float]:
        """安全地将字符串转换为浮点数，失败返回 None。"""
        if value is None:
            return None
        try:
            return float(str(value))
        except Exception:
            return None

    def _clean_percent(self, s: Any) -> Optional[float]:
        """清洗百分数字符串，去除百分号和逗号后转为浮点数。"""
        if s is None:
            return None
        text = str(s).strip()
        text = text.replace("%", "").replace("％", "")
        text = text.replace(",", "").replace("，", "")
        text = text.strip()
        if not text:
            return None
        try:
            return float(text)
        except Exception:
            return None

    def _clean_number(self, s: Any) -> Optional[float]:
        """清洗数字字符串（去除逗号、单位等），返回浮点数。"""
        if s is None:
            return None
        text = str(s).strip()
        text = text.replace(",", "").replace("，", "")
        text = text.replace("万元", "").replace("元", "")
        text = text.strip()
        if not text or text == "/":
            return None
        try:
            return float(text)
        except Exception:
            return None

    # 结果构建
    def _build_result(
        self,
        result_text: str,
        price_type: str,
        summary: List[str],
        *,
        pages: Optional[List[int]] = None,
        locations: Optional[List[Dict]] = None,
        extra: Optional[Dict] = None,
    ) -> Dict:
        """构建统一格式的检查结果字典。"""
        normalized_pages = []
        seen_pages = set()
        for page in pages or []:
            if not isinstance(page, int) or page in seen_pages:
                continue
            seen_pages.add(page)
            normalized_pages.append(page)

        normalized_locations = []
        seen_locations = set()
        for location in locations or []:
            if not isinstance(location, dict):
                continue
            page = (
                location.get("page") if isinstance(location.get("page"), int) else None
            )
            label = str(location.get("label") or "").strip()
            document = str(location.get("document") or "").strip()
            key = (document, page, label)
            if key in seen_locations:
                continue
            seen_locations.add(key)
            normalized_locations.append(
                {
                    "page": page,
                    "label": label,
                    "text": str(location.get("text") or "").strip()[:120],
                    "document": document,
                }
            )

        result = {
            "result": result_text,
            "type": price_type,
            "summary": summary,
            "pages": normalized_pages,
            "locations": normalized_locations,
        }
        if isinstance(extra, dict):
            result.update(extra)
        return result

    def _build_fail_result(
        self,
        reason: str,
        *,
        pages: Optional[List[int]] = None,
        locations: Optional[List[Dict]] = None,
    ) -> Dict:
        """快捷生成“未识别/缺失”状态的检查结果。"""
        return self._build_result(
            "未识别",
            "未识别",
            [reason],
            pages=pages,
            locations=locations,
        )

    # 目录行判断（会被 DocumentParserMixin 使用）
    def _is_catalog_line(self, line: str) -> bool:
        """判断一行是否为目录行（不应作为报价内容）。"""
        import re
        normalized = self._normalize(line)

        if "目录" in normalized:
            return True
        if "格式参见本章附件" in normalized:
            return True

        if re.search(r"\.\.\-?\d+", normalized):
            return True
        if re.search(r"\.{2,}\-?\d+", normalized):
            return True
        if re.search(r"……\-?\d+", normalized):
            return True
        if re.search(r"…+\-?\d+", normalized):
            return True

        # 若一行包含多个章节标题关键词，很可能是目录
        catalog_title_hits = sum(
            1 for token in [
                "投标保证书",
                "比选保证书",
                "开标一览表",
                "报价一览表",
                "投标一览表",
                "响应报价一览表",
                "参选报价一览表",
                "分项报价表",
                "商务条款偏离表",
                "技术条款偏离表",
                "投标人基本情况介绍",
                "类似项目业绩清单",
                "投标人的资格证明文件",
                "项目人员情况",
                "授权委托书",
                "法定代表人身份证明",
            ]
            if token in normalized
        )
        if catalog_title_hits >= 2:
            return True

        return False
