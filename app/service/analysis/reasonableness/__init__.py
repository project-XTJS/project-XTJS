# pricing_reasonableness/__init__.py
"""
报价合理性检查模块（重构版）

通过 Mixin 多重继承组装 ReasonablenessChecker，调度各个模块完成报价逻辑校验。
"""

from typing import Any, Dict
from .utils import UtilsMixin
from .document_parser import DocumentParserMixin
from .direct_price import DirectPriceMixin
from .float_rate import FloatRateMixin
from .tender_limit import TenderLimitMixin


class ReasonablenessChecker(
    UtilsMixin,
    DocumentParserMixin,
    DirectPriceMixin,
    FloatRateMixin,
    TenderLimitMixin,
):
    """报价合理性检查类，支持直接报价与下浮率报价两种模式。"""

    def __init__(self, min_float_rate: float = 1.5):
        # 兜底下浮率阈值
        self.min_float_rate = min_float_rate

        # --- 以下是你在原有 __init__.py 中定义的常量初始化 ---
        self.CAPITAL_NUM = {"零": 0, "〇": 0, "壹": 1, "贰": 2, "叁": 3, "肆": 4, "伍": 5, "陆": 6, "柒": 7, "捌": 8, "玖": 9}
        self.SMALL_UNITS = {"拾": 10, "佰": 100, "仟": 1000}
        self.BIG_UNITS = {"万": 10000, "亿": 100000000}
        self.BID_OPENING_TITLES = ["开标一览表", "报价一览表", "投标一览表", "响应报价一览表", "参选报价一览表"]
        self.ITEMIZED_SECTION_TITLES = ["分项报价表"]
        self.SECTION_END_TITLES = ["分项报价表", "已标价工程量清单", "工程量清单", "商务条款偏离表", "技术条款偏离表", "投标人基本情况介绍", "类似项目业绩清单", "投标人的资格证明文件", "项目人员情况", "资格审查资料", "法定代表人身份证明", "授权委托书", "投标保证书", "比选保证书", "承诺函"]
        self.FLOAT_RULE_PHRASES = ["低于或等于", "高于或等于", "不低于", "不少于", "不高于", "不大于", "大于", "高于", "低于", "小于", "等于"]
        self.COMMON_TAX_RATES = {3.0, 6.0, 9.0, 13.0}
        self.TENDER_LIMIT_STRONG_KEYWORDS = ["最高限价", "最高投标限价", "最高响应限价", "最高报价限价", "招标控制价", "控制价", "最高控制价", "最高总价"]
        self.TENDER_LIMIT_MEDIUM_KEYWORDS = ["采购预算", "预算金额", "项目预算", "预算价", "预算", "限价", "总价限价", "投标限价", "响应限价", "采购金额", "最高采购限价"]
        self.TENDER_LIMIT_WEAK_KEYWORDS = ["资金来源", "财政资金", "自筹资金", "国库资金", "专项资金"]
        self.TENDER_LIMIT_EXCLUDE_KEYWORDS = ["营业收入", "净利润", "资产总额", "注册资本", "合同金额", "中标金额", "成交金额", "业绩", "发票", "报价明细", "分项报价表", "开标一览表"]
        self.BID_TOTAL_KEYWORDS = ["参选总价", "投标总价", "报价总价", "响应总报价", "总报价", "总价", "合计"]


    def check_price_compliance(self, source: Any) -> Dict:
        """执行报价合规性检查，自动识别直接报价或下浮率模式。"""
        # 1. 解析输入
        parsed = self._parse_input(source)
        
        # 2. 定位开标一览表
        bid_page, bid_opening_text = self._locate_bid_opening_page_and_text(parsed)
        fallback_pages = [bid_page] if isinstance(bid_page, int) else []
        fallback_locations = [{"page": bid_page, "label": "开标一览表", "document": "bidder"}] if isinstance(bid_page, int) else []

        if not bid_opening_text:
            return self._build_fail_result(
                "未找到开标/报价/投标一览表正文",
                pages=fallback_pages,
                locations=fallback_locations,
            )

        # 3. 尝试分流模式 A：直接报价（检查大小写金额对）
        price_pairs = self._extract_direct_price_pairs(bid_opening_text)
        if price_pairs:
            summary = []
            all_match = True
            for pair in price_pairs:
                small_price = pair["small_price"]
                capital_price = pair["capital_price"]
                small_str = pair["small_price_str"]
                capital_str = pair["capital_price_str"]

                is_match = (
                    small_price is not None
                    and capital_price is not None
                    and abs(small_price - capital_price) < 0.01
                )
                if not is_match:
                    all_match = False
                summary.append(
                    f"小写 {small_str} 与大写 {capital_str} {'一致' if is_match else '不一致'}，{'合格' if is_match else '不合格'}"
                )

            return self._build_result(
                result_text="合格" if all_match else "失败",
                price_type="直接报价",
                summary=summary,
                pages=fallback_pages,
                locations=fallback_locations,
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
            locations = (
                [{"page": page, "label": "投标总价", "document": "bidder"}]
                if isinstance(page, int)
                else fallback_locations
            )
            pages = [page] if isinstance(page, int) else fallback_pages
            return self._build_result(
                result_text="合格",
                price_type="直接报价",
                summary=summary,
                pages=pages,
                locations=locations,
            )

        # 4. 尝试分流模式 B：下浮率报价（提取规则与行）
        rules = self._extract_float_rate_rules(bid_opening_text)
        rows = self._extract_float_rate_rows(parsed, bid_page, bid_opening_text, rules)

        if rows:
            passed, summary = self._check_float_rate_rows_compliance(rows, rules)
            # 整理涉及的页面和位置
            row_pages = []
            seen_pages = set()
            row_locations = []
            for row in rows:
                for page in row.get("pages") or []:
                    if not isinstance(page, int):
                        continue
                    if page not in seen_pages:
                        seen_pages.add(page)
                        row_pages.append(page)
                    row_locations.append({
                        "page": page,
                        "label": str(row.get("biz_name_raw") or row.get("biz_name") or "下浮率报价"),
                        "text": str(row.get("raw_line") or ""),
                        "document": "bidder",
                    })
            return self._build_result(
                result_text="合格" if passed else "失败",
                price_type="下浮率报价",
                summary=summary,
                pages=row_pages,
                locations=row_locations,
            )

        # 5. 兜底：单一下浮率识别
        single_float_rate = self._extract_single_float_rate_from_table(parsed, bid_page, bid_opening_text)
        if single_float_rate is not None:
            # 应用规则判断逻辑...
            if "__generic__" in rules:
                rule = rules["__generic__"]
                passed = self._compare_by_rule(single_float_rate, rule["op"], rule["threshold"])
                summary = [f"下浮率：{single_float_rate:.2f}% {rule['op']} {rule['threshold']:g}% ，{'合格' if passed else '不合格'}"]
            else:
                passed = single_float_rate > self.min_float_rate
                summary = [f"下浮率：{single_float_rate:.2f}% > {self.min_float_rate:g}% ，{'合格' if passed else '不合格'}"]

            return self._build_result(
                result_text="合格" if passed else "失败",
                price_type="下浮率报价",
                summary=summary,
                pages=fallback_pages,
                locations=fallback_locations,
            )

        return self._build_fail_result(
            "一览表中未找到直接报价或下浮率报价信息",
            pages=fallback_pages,
            locations=fallback_locations,
        )

    def check_price_reasonableness(self, source: Any) -> Dict:
        """主入口别名"""
        return self.check_price_compliance(source)
