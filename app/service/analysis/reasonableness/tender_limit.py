# pricing_reasonableness/tender_limit.py
"""
报价合理性 - 招标限价检查 Mixin

提供从招标文件中提取最高限价/控制价/预算、从投标文件中提取总报价金额，
并执行限价合规比对的功能。
"""

import re
from typing import Any, Dict, List, Optional, Tuple

class TenderLimitMixin:
    # 依赖常量
    TENDER_LIMIT_STRONG_KEYWORDS: list
    TENDER_LIMIT_MEDIUM_KEYWORDS: list
    TENDER_LIMIT_WEAK_KEYWORDS: list
    TENDER_LIMIT_EXCLUDE_KEYWORDS: list
    BID_TOTAL_KEYWORDS: list

    # 金额候选提取
    def _convert_amount_to_yuan(self, value: float, unit: str) -> float:
        unit = (unit or "").strip()
        if unit in {"亿", "亿元"}:
            return value * 100000000
        if unit in {"万", "万元"}:
            return value * 10000
        return value

    def _format_amount_yuan(self, value: float) -> str:
        return f"{value:.2f}元"

    def _build_amount_compare_tokens(self, raw_amount: str, amount_yuan: float) -> List[str]:
        """将金额文本统一折算后生成可比较 token，兼容 OCR 中的空格差异。"""
        tokens = {self._normalize(raw_amount)}
        if amount_yuan <= 0:
            return [token for token in tokens if token]

        rounded = round(float(amount_yuan), 2)
        if abs(rounded - round(rounded)) < 0.01:
            int_amount = int(round(rounded))
            tokens.add(self._normalize(f"{int_amount}元"))
            if int_amount % 10000 == 0:
                wan_value = int_amount / 10000
                if abs(wan_value - round(wan_value)) < 0.01:
                    wan_int = int(round(wan_value))
                    tokens.add(self._normalize(f"{wan_int}万"))
                    tokens.add(self._normalize(f"{wan_int}万元"))
            if int_amount % 100000000 == 0:
                yi_value = int_amount / 100000000
                if abs(yi_value - round(yi_value)) < 0.01:
                    yi_int = int(round(yi_value))
                    tokens.add(self._normalize(f"{yi_int}亿"))
                    tokens.add(self._normalize(f"{yi_int}亿元"))
        else:
            tokens.add(self._normalize(f"{rounded:.2f}元"))

        return [token for token in tokens if token]

    def _keyword_near_amount(self, context: str, raw_amount: str, amount_yuan: float) -> bool:
        """先把金额文本规范化后再比邻近关系，避免“500 万元”与“500万元”失配。"""
        normalized_context = self._normalize(context)
        if not normalized_context:
            return False
        keyword_pattern = (
            r"(最高限价|最高投标限价|最高响应限价|最高报价限价|招标控制价|控制价|"
            r"采购预算|预算金额|项目预算|最高总价|限价)"
        )
        for token in self._build_amount_compare_tokens(raw_amount, amount_yuan):
            pattern = keyword_pattern + r".{0,20}?" + re.escape(token)
            if re.search(pattern, normalized_context):
                return True
        return False

    def _amount_local_context(
        self, text: str, start: int, end: int, *, window: int = 40
    ) -> str:
        if not text:
            return ""
        left = max(0, int(start) - window)
        right = min(len(text), int(end) + window)
        return str(text[left:right])

    def _amount_local_line_context(self, text: str, start: int, end: int) -> str:
        if not text:
            return ""
        left = str(text).rfind("\n", 0, int(start))
        right = str(text).find("\n", int(end))
        if left == -1:
            left = 0
        else:
            left += 1
        if right == -1:
            right = len(text)
        return str(text[left:right])

    def _is_guarantee_amount_context(self, context: str) -> bool:
        normalized = self._normalize(context)
        if not normalized:
            return False
        guarantee_tokens = [
            "投标保证金",
            "保证金",
            "保函",
            "保证金提交截止时间",
            "以保证金实际到账为准",
            "开户银行",
            "账号",
            "账户",
            "转账方式",
        ]
        return any(token in normalized for token in guarantee_tokens)

    def _has_budget_amount_signal(self, context: str) -> bool:
        normalized = self._normalize(context)
        if not normalized:
            return False
        budget_tokens = ["总预算", "项目预算", "预算金额", "采购预算", "预算"]
        return any(token in normalized for token in budget_tokens)

    def _looks_like_same_budget_limit_context(self, context: str) -> bool:
        normalized = self._normalize(context)
        if not normalized:
            return False
        return bool(
            re.search(
                r"(最高限价|最高投标限价|最高响应限价|最高报价限价|招标控制价|控制价).{0,20}?同预算",
                normalized,
            )
            or re.search(
                r"同预算.{0,20}?(最高限价|最高投标限价|最高响应限价|最高报价限价|招标控制价|控制价)",
                normalized,
            )
        )

    def _extract_money_candidates_from_text(self, text: str) -> List[Dict]:
        if not text or not str(text).strip():
            return []
        candidates: List[Dict] = []

        arabic_pattern = re.compile(
            r"(?:人民币)?\s*([￥¥]?\s*\d[\d,，]*(?:\.\d+)?)\s*(亿元|亿|万元|万|元)"
        )
        for m in arabic_pattern.finditer(text):
            raw_num = m.group(1).strip()
            unit = m.group(2).strip()
            num = raw_num.replace("￥", "").replace("¥", "")
            num = num.replace(",", "").replace("，", "").strip()
            try:
                value = float(num)
            except Exception:
                continue
            amount_yuan = self._convert_amount_to_yuan(value, unit)
            if amount_yuan <= 0:
                continue
            candidates.append({
                "raw_amount": f"{raw_num}{unit}",
                "amount_yuan": round(amount_yuan, 2),
                "start": m.start(),
                "end": m.end(),
                "unit": unit,
                "is_capital": False,
            })

        capital_pattern = re.compile(
            r"(?:人民币)?\s*([零〇壹贰叁肆伍陆柒捌玖拾佰仟万亿元角分整正圆]+)"
        )
        for m in capital_pattern.finditer(text):
            raw_capital = m.group(1).strip()
            if "元" not in raw_capital and "圆" not in raw_capital:
                continue
            amount_yuan = self._capital_to_number(raw_capital)
            if amount_yuan is None or amount_yuan <= 0:
                continue
            candidates.append({
                "raw_amount": raw_capital,
                "amount_yuan": round(amount_yuan, 2),
                "start": m.start(),
                "end": m.end(),
                "unit": "中文大写",
                "is_capital": True,
            })

        dedup: Dict[Tuple[float, int, int], Dict] = {}
        for cand in candidates:
            key = (cand["amount_yuan"], cand["start"], cand["end"])
            dedup[key] = cand
        return list(dedup.values())

    # 限价候选评分与收集
    def _pick_keyword_near_amount(self, context: str) -> str:
        normalized = self._normalize(context)
        for keyword in (
            self.TENDER_LIMIT_STRONG_KEYWORDS
            + self.TENDER_LIMIT_MEDIUM_KEYWORDS
            + self.TENDER_LIMIT_WEAK_KEYWORDS
        ):
            if keyword in normalized:
                return keyword
        return ""

    def _score_tender_limit_candidate(
        self, context: str, raw_amount: str, amount_yuan: float
    ) -> int:
        normalized = self._normalize(context)
        score = 0

        strong_hits = sum(1 for k in self.TENDER_LIMIT_STRONG_KEYWORDS if k in normalized)
        medium_hits = sum(1 for k in self.TENDER_LIMIT_MEDIUM_KEYWORDS if k in normalized)
        weak_hits = sum(1 for k in self.TENDER_LIMIT_WEAK_KEYWORDS if k in normalized)

        score += strong_hits * 120
        score += medium_hits * 60
        score += weak_hits * 12

        if (
            "资金来源" in normalized
            or "财政资金" in normalized
            or "自筹资金" in normalized
        ) and ("预算" in normalized or "限价" in normalized or "控制价" in normalized):
            score += 30

        if "本项目" in normalized or "项目名称" in normalized or "采购项目" in normalized:
            score += 10

        # 金额先折算成数字再生成规范化 token，兼容 OCR 把“500万元”切成“500 万元”。
        if self._keyword_near_amount(context, raw_amount, amount_yuan):
            score += 40

        if any(
            token in context
            for token in ["每月", "每年", "每人", "每日", "每次", "单价", "/月", "/年", "/人", "/次"]
        ):
            score -= 30

        for kw in self.TENDER_LIMIT_EXCLUDE_KEYWORDS:
            if kw in normalized:
                score -= 45

        if amount_yuan < 1:
            score -= 100
        elif amount_yuan < 1000:
            score -= 20

        if (
            raw_amount.endswith("万")
            or raw_amount.endswith("万元")
            or raw_amount.endswith("亿")
            or raw_amount.endswith("亿元")
        ):
            score += 6

        return score

    def _collect_tender_limit_candidates(self, parsed: Dict) -> List[Dict]:
        page_text_map = self._merge_texts_by_page(parsed)
        all_candidates: List[Dict] = []
        all_keywords = (
            self.TENDER_LIMIT_STRONG_KEYWORDS
            + self.TENDER_LIMIT_MEDIUM_KEYWORDS
            + self.TENDER_LIMIT_WEAK_KEYWORDS
        )

        for page, page_text in page_text_map.items():
            if not page_text:
                continue

            lines = [
                line.strip()
                for line in str(page_text).splitlines()
                if line and str(line).strip()
            ]
            normalized_page = self._normalize(page_text)

            if any(k in normalized_page for k in all_keywords):
                money_candidates = self._extract_money_candidates_from_text(page_text)
                for cand in money_candidates:
                    line_context = self._amount_local_line_context(
                        page_text, cand["start"], cand["end"]
                    )
                    local_context = self._amount_local_context(
                        page_text, cand["start"], cand["end"]
                    )
                    if self._is_guarantee_amount_context(line_context):
                        continue
                    if not (
                        self._keyword_near_amount(
                            line_context, cand["raw_amount"], cand["amount_yuan"]
                        )
                        or self._has_budget_amount_signal(line_context)
                    ):
                        continue
                    score = (
                        self._score_tender_limit_candidate(
                            page_text, cand["raw_amount"], cand["amount_yuan"]
                        )
                        - 15
                    )
                    all_candidates.append({
                        "page": page,
                        "amount_yuan": cand["amount_yuan"],
                        "raw_amount": cand["raw_amount"],
                        "keyword": self._pick_keyword_near_amount(line_context) or self._pick_keyword_near_amount(page_text),
                        "score": score,
                        "context": local_context or page_text[:400],
                    })

            for idx, line in enumerate(lines):
                normalized_line = self._normalize(line)
                if not any(k in normalized_line for k in all_keywords):
                    continue

                start = max(0, idx - 2)
                end = min(len(lines), idx + 3)
                context = "\n".join(lines[start:end]).strip()
                money_candidates = self._extract_money_candidates_from_text(context)

                for cand in money_candidates:
                    line_context = self._amount_local_line_context(
                        context, cand["start"], cand["end"]
                    )
                    local_context = self._amount_local_context(
                        context, cand["start"], cand["end"]
                    )
                    if self._is_guarantee_amount_context(line_context):
                        continue
                    score = self._score_tender_limit_candidate(
                        context, cand["raw_amount"], cand["amount_yuan"]
                    )
                    all_candidates.append({
                        "page": page,
                        "amount_yuan": cand["amount_yuan"],
                        "raw_amount": cand["raw_amount"],
                        "keyword": self._pick_keyword_near_amount(line_context) or self._pick_keyword_near_amount(context),
                        "score": score,
                        "context": local_context or context,
                    })

        dedup: Dict[Tuple[Optional[int], float, str], Dict] = {}
        for cand in all_candidates:
            key = (cand["page"], round(cand["amount_yuan"], 2), cand["keyword"])
            if key not in dedup or cand["score"] > dedup[key]["score"]:
                dedup[key] = cand

        return sorted(
            dedup.values(),
            key=lambda x: (x["score"], x["amount_yuan"]),
            reverse=True,
        )

    def _extract_tender_max_limit(self, tender_source: Any) -> Optional[Dict]:
        parsed = self._parse_input(tender_source)
        candidates = self._collect_tender_limit_candidates(parsed)
        if not candidates:
            return None
        page_text_map = self._merge_texts_by_page(parsed)
        same_budget_mode = any(
            self._looks_like_same_budget_limit_context(page_text)
            for page_text in page_text_map.values()
            if page_text
        )
        if same_budget_mode:
            budget_candidates = [
                cand
                for cand in candidates
                if self._has_budget_amount_signal(cand.get("context", ""))
                and not self._is_guarantee_amount_context(cand.get("context", ""))
            ]
            if not budget_candidates:
                return None
            best_budget = budget_candidates[0]
            if best_budget["score"] < 20:
                return None
            return {
                **best_budget,
                "keyword": best_budget.get("keyword") or "预算",
            }
        best = candidates[0]
        if best["score"] < 40:
            return None
        return best

    def _is_direct_quote_mode(self, bid_source: Any) -> bool:
        parsed = self._parse_input(bid_source)
        _, bid_opening_text = self._locate_bid_opening_page_and_text(parsed)
        if not bid_opening_text or not bid_opening_text.strip():
            return False
        if self._extract_direct_price_pairs(bid_opening_text):
            return True
        return self._extract_bid_total_amount(bid_source) is not None

    # 投标总价提取
    def _extract_bid_total_amount(self, bid_source: Any) -> Optional[Dict]:
        parsed = self._parse_input(bid_source)
        bid_page, bid_opening_text = self._locate_bid_opening_page_and_text(parsed)

        if not bid_opening_text or not bid_opening_text.strip():
            return None

        normalized_opening_text = self._strip_price_markup(bid_opening_text)
        has_opening_context = (
            self._contains_bid_opening_title(bid_opening_text)
            or self._has_bid_opening_context(bid_opening_text)
        )
        if not self._has_bid_total_amount_signal(
            bid_opening_text,
            assume_opening_context=self._contains_bid_opening_title(bid_opening_text),
        ):
            return None

        label_pattern = r"(?:%s)" % "|".join(self._bid_total_label_patterns())
        direct_total_patterns = [
            rf"({label_pattern})[^\n\d]{{0,20}}[：:]?\s*([￥¥]?\s*[\d,，]+(?:\.\d+)?\s*元?)",
            rf"({label_pattern})[^\n]{{0,20}}?小写[：:]?\s*([￥¥]?\s*[\d,，]+(?:\.\d+)?\s*元?)",
            r"小写[：:]?\s*([￥¥]?\s*[\d,，]+(?:\.\d+)?\s*元?)",
        ]

        search_texts = [normalized_opening_text]
        if normalized_opening_text != bid_opening_text:
            search_texts.append(bid_opening_text)

        for search_text in search_texts:
            for pattern in direct_total_patterns:
                for m in re.finditer(pattern, search_text):
                    raw_amount = (
                        m.group(2).strip()
                        if len(m.groups()) >= 2
                        else m.group(1).strip()
                    )
                    amount = self._clean_small_price(raw_amount)
                    if amount is None:
                        continue
                    return {
                        "page": bid_page,
                        "amount_yuan": round(amount, 2),
                        "raw_amount": raw_amount,
                        "context": normalized_opening_text[:400] or bid_opening_text[:400],
                    }

        price_pairs = self._extract_direct_price_pairs(normalized_opening_text)
        for pair in price_pairs:
            small_price = pair.get("small_price")
            if small_price is None:
                continue
            return {
                "page": bid_page,
                "amount_yuan": round(float(small_price), 2),
                "raw_amount": pair.get("small_price_str") or str(small_price),
                "context": bid_opening_text[:400],
            }

        if has_opening_context and not self._looks_like_itemized_total_page(bid_opening_text):
            line_patterns = [
                r"(合计)[^\n\d]{0,20}[：:]?\s*([￥¥]?\s*[\d,，]+(?:\.\d+)?\s*元?)",
            ]
            for search_text in search_texts:
                for pattern in line_patterns:
                    for m in re.finditer(pattern, search_text):
                        raw_amount = m.group(2).strip()
                        amount = self._clean_small_price(raw_amount)
                        if amount is None:
                            continue
                        return {
                            "page": bid_page,
                            "amount_yuan": round(amount, 2),
                            "raw_amount": raw_amount,
                            "context": normalized_opening_text[:400] or bid_opening_text[:400],
                        }

        return None

    # 限价比对主入口
    def check_bid_price_against_tender_limit(
        self, tender_source: Any, bid_source: Any
    ) -> Dict:
        tender_limit = self._extract_tender_max_limit(tender_source)
        if not tender_limit:
            if self._is_direct_quote_mode(bid_source):
                return {
                    "result": "合格",
                    "type": "最高限价校验",
                    "summary": ["未在招标文件中识别到最高限价/预算/控制价相关金额，按项目未设置最高限价处理。"],
                    "pages": [],
                    "locations": [],
                }
            return {
                "result": "失败",
                "type": "最高限价校验",
                "summary": ["未在招标文件中识别到最高限价/预算/控制价相关金额"],
                "pages": [],
                "locations": [],
            }

        bid_total = self._extract_bid_total_amount(bid_source)
        if not bid_total:
            tender_page = tender_limit.get("page")
            return {
                "result": "失败",
                "type": "最高限价校验",
                "summary": ["未在投标文件中识别到投标总金额/参选总价/报价总价"],
                "pages": [tender_page] if isinstance(tender_page, int) else [],
                "locations": (
                    [{"page": tender_page, "label": "招标限价", "document": "tender"}]
                    if isinstance(tender_page, int)
                    else []
                ),
            }

        tender_amount = tender_limit["amount_yuan"]
        bid_amount = bid_total["amount_yuan"]
        passed = bid_amount <= tender_amount + 0.01
        diff = round(abs(bid_amount - tender_amount), 2)

        tender_page = tender_limit.get("page")
        bid_page = bid_total.get("page")

        summary = [
            f"招标最高限价：{tender_limit['raw_amount']}（折算 {self._format_amount_yuan(tender_amount)}），位置：第 {tender_page if tender_page is not None else '?'} 页，命中关键词：{tender_limit.get('keyword') or '未标记'}",
            f"投标总金额：{bid_total['raw_amount']}（折算 {self._format_amount_yuan(bid_amount)}），位置：第 {bid_page if bid_page is not None else '?'} 页",
            f"比较结果：投标总金额 {'<=' if passed else '>'} 招标最高限价，差额 {self._format_amount_yuan(diff)}，{'合格' if passed else '不合格'}",
        ]

        pages = []
        locations = []
        if isinstance(tender_page, int):
            pages.append(tender_page)
            locations.append({
                "page": tender_page, "label": "招标限价", "document": "tender",
            })
        if isinstance(bid_page, int):
            if bid_page not in pages:
                pages.append(bid_page)
            locations.append({
                "page": bid_page, "label": "投标总价", "document": "bidder",
            })

        return {
            "result": "合格" if passed else "失败",
            "type": "最高限价校验",
            "summary": summary,
            "pages": pages,
            "locations": locations,
        }
