# pricing_reasonableness/__init__.py
"""
报价合理性检查模块（重构版）

通过 Mixin 多重继承组装 ReasonablenessChecker，调度各个模块完成报价逻辑校验。
"""

import re
from typing import Any, Dict
from .business_rules import BusinessRulesMixin
from .evidence import compare_capital_amounts
from .utils import UtilsMixin
from .document_parser import DocumentParserMixin
from .direct_price import DirectPriceMixin
from .float_rate import FloatRateMixin
from .tender_limit import TenderLimitMixin


class ReasonablenessChecker(
    BusinessRulesMixin,
    UtilsMixin,
    DocumentParserMixin,
    DirectPriceMixin,
    FloatRateMixin,
    TenderLimitMixin,
):
    """报价合理性检查类，支持直接报价与下浮率报价两种模式。"""

    def __init__(self, min_float_rate: float | None = None):
        # 兼容旧调用参数；不再作为缺少招标规则时的阈值
        self.min_float_rate = min_float_rate

        # --- 以下是你在原有 __init__.py 中定义的常量初始化 ---
        self.CAPITAL_NUM = {"零": 0, "〇": 0, "壹": 1, "贰": 2, "叁": 3, "肆": 4, "伍": 5, "陆": 6, "柒": 7, "捌": 8, "玖": 9}
        self.SMALL_UNITS = {"拾": 10, "佰": 100, "仟": 1000}
        self.BIG_UNITS = {"万": 10000, "亿": 100000000}
        self.BID_OPENING_TITLES = ["开标一览表", "报价一览表", "投标一览表", "响应报价一览表", "参选报价一览表"]
        self.BID_SECURITY_TITLES = ["投标保证书", "比选保证书", "投标保证金", "响应保证金", "报价保证金", "保证金缴纳凭证", "保证金回单"]
        self.ITEMIZED_SECTION_TITLES = ["分项报价表"]
        self.SECTION_END_TITLES = ["分项报价表", "已标价工程量清单", "工程量清单", "商务条款偏离表", "技术条款偏离表", "投标人基本情况介绍", "类似项目业绩清单", "投标人的资格证明文件", "项目人员情况", "资格审查资料", "法定代表人身份证明", "授权委托书", "投标保证书", "比选保证书", "承诺函"]
        self.FLOAT_RULE_PHRASES = ["低于或等于", "高于或等于", "不得超过", "不超过", "不得高于", "不得大于", "不得低于", "不得小于", "不低于", "不少于", "不高于", "不大于", "大于", "高于", "低于", "小于", "等于"]
        self.RATE_QUOTE_KEYWORDS = ["投标下浮率", "报价下浮率", "下浮率", "投标折扣率", "报价折扣率", "折扣率", "优惠率", "折让率"]
        self.DISCOUNT_RATE_KEYWORDS = ["投标折扣率", "报价折扣率", "折扣率"]
        self.COMMON_TAX_RATES = {3.0, 6.0, 9.0, 13.0}
        self.TENDER_LIMIT_STRONG_KEYWORDS = ["最高限价", "最高投标限价", "最高响应限价", "最高报价限价", "招标控制价", "控制价", "最高控制价", "最高总价"]
        self.TENDER_LIMIT_MEDIUM_KEYWORDS = ["采购预算", "预算金额", "项目预算", "预算价", "预算", "限价", "总价限价", "投标限价", "响应限价", "采购金额", "最高采购限价"]
        self.TENDER_LIMIT_WEAK_KEYWORDS = ["资金来源", "财政资金", "自筹资金", "国库资金", "专项资金"]
        self.TENDER_LIMIT_EXCLUDE_KEYWORDS = ["营业收入", "净利润", "资产总额", "注册资本", "合同金额", "中标金额", "成交金额", "业绩", "发票", "报价明细", "分项报价表", "开标一览表"]
        self.BID_TOTAL_KEYWORDS = ["参选总价", "投标总价", "投标价格", "报价总价", "响应总报价", "总报价", "总价", "合计"]


    def check_price_compliance(self, source: Any, *, tender_source: Any = None) -> Dict:
        result = self._check_price_compliance_core(source, tender_source=tender_source)
        if result.get("quote_mode") == "rate":
            return result
        parsed = self._parse_input(source)
        page, opening = self._locate_bid_opening_page_and_text(parsed)
        if re.search(r"(?:下浮率|折扣率|优惠率|折让率)\s*[：:|]?\s*\d+(?:\.\d+)?\s*[%％]", opening or ""):
            rates = self._check_tender_rates(source, tender_source, parsed, page, opening)
            states = [result.get("status", "unclear"), rates["status"]]
            status = "fail" if "fail" in states else ("pass" if all(x=="pass" for x in states) else "unclear")
            result.update(status=status, result={"pass":"合格","fail":"失败","unclear":"待复核"}[status], quote_mode="mixed",
                rate_rows=rates["rate_rows"], tender_rules=rates.get("tender_rules", []),
                summary=list(result.get("summary") or [])+rates["summary"],
                locations=list(result.get("locations") or [])+rates["locations"])
        return result

    def _check_price_compliance_core(self, source: Any, *, tender_source: Any = None) -> Dict:
        """执行报价合规性检查，自动识别直接报价或下浮率模式。"""
        # 1. 解析输入
        parsed = self._parse_input(source)
        
        # 2. 定位开标一览表
        bid_page, bid_opening_text = self._locate_bid_opening_page_and_text(parsed)
        fallback_pages = [bid_page] if isinstance(bid_page, int) else []
        fallback_locations = [{"page": bid_page, "label": "开标一览表", "document": "bidder"}] if isinstance(bid_page, int) else []

        if not bid_opening_text:
            return self._build_result("待复核", "未识别", ["未找到开标/报价/投标一览表正文"], pages=fallback_pages, locations=fallback_locations, extra={"status":"unclear"})

        # 3. 尝试分流模式 A：直接报价（检查大小写金额对）
        price_pairs = self._extract_direct_price_pairs(bid_opening_text)
        if not price_pairs and self._contains_float_rate_keywords(bid_opening_text) and not self._extract_bid_amounts(source):
            return self._check_tender_rates(source, tender_source, parsed, bid_page, bid_opening_text)
        if price_pairs:
            summary = []
            has_mismatch = False
            has_missing_info = False
            normalized_pairs = []
            for pair in price_pairs:
                small_price = pair["small_price"]
                capital_price = pair["capital_price"]
                small_str = pair["small_price_str"]
                capital_str = pair["capital_price_str"]

                if small_price is None or capital_price is None:
                    case_status = "missing"
                    has_missing_info = True
                elif compare_capital_amounts(small_price, capital_price) == "pass":
                    case_status = "pass"
                else:
                    case_status = "fail"
                    has_mismatch = True
                status_label = {
                    "pass": "一致",
                    "fail": "不一致",
                    "missing": "缺少信息",
                }[case_status]
                summary.append(
                    f"小写 {small_str or '未识别'} 与大写 {capital_str or '未识别'} {status_label}"
                )
                normalized_pairs.append(
                    {
                        **pair.get("money_evidence", {}),
                        "small_raw_amount": small_str,
                        "small_amount_yuan": small_price,
                        "capital_raw_amount": capital_str,
                        "capital_amount_yuan": capital_price,
                        "case_consistency_status": case_status,
                    }
                )

            first_pair = normalized_pairs[0] if normalized_pairs else {}
            overall_case_status = "fail" if has_mismatch else ("missing" if has_missing_info else "pass")
            result_text = "失败" if overall_case_status == "fail" else (
                "缺少信息" if overall_case_status == "missing" else "合格"
            )
            return self._build_result(
                result_text=result_text,
                price_type="直接报价",
                summary=summary,
                pages=fallback_pages,
                locations=fallback_locations,
                extra={
                    **first_pair,
                    "locations": fallback_locations,
                    "page": bid_page,
                    "status": "unclear" if overall_case_status == "missing" else overall_case_status,
                    "amount_yuan": first_pair.get("small_amount_yuan"),
                    "raw_amount": first_pair.get("small_raw_amount") or first_pair.get("capital_raw_amount"),
                    "capital_amount": first_pair.get("capital_amount_yuan"),
                    "capital_raw_amount": first_pair.get("capital_raw_amount"),
                    "case_consistency_status": overall_case_status,
                    "case_consistency_summary": "；".join(summary),
                    "price_pairs": normalized_pairs,
                    "amount_facts": self._extract_bid_amounts(source),
                },
            )

        direct_total = self._extract_bid_total_amount(source)
        if direct_total:
            raw_amount = str(direct_total.get("raw_amount") or "").strip()
            amount_yuan = direct_total.get("amount_yuan")
            page = direct_total.get("page")
            summary = [
                f"已识别直接报价：{raw_amount or amount_yuan}，位置：第 {page} 页"
                if isinstance(page, int)
                else f"已识别直接报价：{raw_amount or amount_yuan}"
            ]
            summary.append("大小写金额对缺少信息，无法确认大小写报价是否一致。")
            locations = (
                [{"page": page, "label": "投标总价", "document": "bidder"}]
                if isinstance(page, int)
                else fallback_locations
            )
            pages = [page] if isinstance(page, int) else fallback_pages
            return self._build_result(
                result_text="缺少信息",
                price_type="直接报价",
                summary=summary,
                pages=pages,
                locations=locations,
                extra={
                    **direct_total,
                    "status": "unclear",
                    "amount_yuan": amount_yuan,
                    "raw_amount": raw_amount,
                    "capital_amount": None,
                    "capital_raw_amount": None,
                    "case_consistency_status": "missing",
                    "case_consistency_summary": "已识别直接报价，但缺少可比对的大写或小写金额信息。",
                    "amount_facts": self._extract_bid_amounts(source),
                },
            )

        return self._check_tender_rates(source, tender_source, parsed, bid_page, bid_opening_text)

    def check_price_reasonableness(self, source: Any, *, tender_source: Any = None) -> Dict:
        """主入口别名"""
        return self.check_price_compliance(source, tender_source=tender_source)
