import json
import re
from typing import Any, Dict, List, Optional, Tuple


class ReasonablenessChecker:
    """报价合理性检查类（简洁输出版）"""

    def __init__(self, min_float_rate: float = 1.5):
        # 仅作为兜底阈值；优先使用文档中抽取到的规则
        self.min_float_rate = min_float_rate

        self.CAPITAL_NUM = {
            "零": 0, "〇": 0,
            "壹": 1, "贰": 2, "叁": 3, "肆": 4, "伍": 5,
            "陆": 6, "柒": 7, "捌": 8, "玖": 9
        }
        self.SMALL_UNITS = {
            "拾": 10,
            "佰": 100,
            "仟": 1000
        }
        self.BIG_UNITS = {
            "万": 10000,
            "亿": 100000000
        }

        self.BID_OPENING_TITLES = [
            "开标一览表",
            "报价一览表",
            "投标一览表",
            "响应报价一览表",
            "参选报价一览表",
        ]

        self.SECTION_END_TITLES = [
            "分项报价表",
            "已标价工程量清单",
            "工程量清单",
            "商务条款偏离表",
            "技术条款偏离表",
            "投标人基本情况介绍",
            "类似项目业绩清单",
            "投标人的资格证明文件",
            "项目人员情况",
            "资格审查资料",
            "法定代表人身份证明",
            "授权委托书",
            "投标保证书",
            "比选保证书",
            "承诺函",
        ]

    # =========================================================
    # 1. 通用基础
    # =========================================================
    def _normalize(self, s: str) -> str:
        if s is None:
            return ""
        return re.sub(r"\s+", "", str(s))

    def _contains_bid_opening_title(self, text: str) -> bool:
        normalized = self._normalize(text)
        return any(title in normalized for title in self.BID_OPENING_TITLES)

    def _contains_direct_price_keywords(self, text: str) -> bool:
        normalized = self._normalize(text)
        return (
                ("小写" in normalized and "大写" in normalized)
                or "参选总价" in normalized
                or "投标总价" in normalized
                or "报价总价" in normalized
        )

    def _contains_float_rate_keywords(self, text: str) -> bool:
        normalized = self._normalize(text)
        return (
                "下浮率" in normalized
                or ("税率" in normalized and "报价" in normalized)
                or "投标下浮率" in normalized
        )

    def _safe_float(self, value: str) -> Optional[float]:
        if value is None:
            return None
        try:
            return float(str(value))
        except Exception:
            return None

    def _build_result(self, result_text: str, price_type: str, summary: List[str]) -> Dict:
        return {
            "result": result_text,
            "type": price_type,
            "summary": summary
        }

    def _build_fail_result(self, reason: str) -> Dict:
        return {
            "result": "失败",
            "type": "未识别",
            "summary": [reason]
        }

    # =========================================================
    # 2. 输入解析：支持 OCR JSON / JSON 字符串 / 纯文本
    # =========================================================
    def _parse_input(self, source: Any) -> Dict:
        if isinstance(source, dict):
            return self._parse_json_dict(source)

        if isinstance(source, str):
            stripped = source.strip()
            if stripped.startswith("{") and stripped.endswith("}"):
                try:
                    data = json.loads(stripped)
                    return self._parse_json_dict(data)
                except Exception:
                    pass

            return {
                "raw_text": source,
                "sections": [{"page": None, "type": "text", "text": source}],
                "table_sections": []
            }

        text = str(source) if source is not None else ""
        return {
            "raw_text": text,
            "sections": [{"page": None, "type": "text", "text": text}],
            "table_sections": []
        }

    def _parse_json_dict(self, data: Dict) -> Dict:
        payload = data.get("data", data)

        layout_sections = payload.get("layout_sections", []) or []
        table_sections = payload.get("table_sections", []) or []

        sections = []
        for sec in layout_sections:
            page = sec.get("page")
            sec_type = sec.get("type", "text")
            text = sec.get("text") or sec.get("raw_text") or ""
            if text:
                sections.append({
                    "page": page,
                    "type": sec_type,
                    "text": text
                })

        parsed_table_sections = []
        for sec in table_sections:
            page = sec.get("page")
            text = sec.get("text") or sec.get("raw_text") or ""
            if text:
                parsed_table_sections.append({
                    "page": page,
                    "type": "table",
                    "text": text
                })

        raw_text = "\n".join(sec["text"] for sec in sections if sec["text"])

        return {
            "raw_text": raw_text,
            "sections": sections,
            "table_sections": parsed_table_sections
        }

    # =========================================================
    # 3. 动态定位“开标/报价一览表”正文所在页
    # =========================================================
    def _is_catalog_line(self, line: str) -> bool:
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

    def _score_page_candidate(self, page_sections: List[Dict]) -> int:
        page_text = "\n".join(sec["text"] for sec in page_sections if sec["text"])
        normalized_page_text = self._normalize(page_text)

        score = 0

        if self._contains_bid_opening_title(page_text):
            score += 8

        if "目录" in normalized_page_text:
            score -= 20

        if any(self._is_catalog_line(sec["text"]) for sec in page_sections):
            score -= 8

        direct_keys = [
            "小写", "大写", "参选总价", "投标总价", "报价总价"
        ]
        score += sum(3 for k in direct_keys if k in normalized_page_text)

        float_keys = [
            "下浮率", "投标下浮率", "税率", "投标报价", "暂估金额", "业务名称"
        ]
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
            window = "\n".join(lines[idx: idx + 12])
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

    # =========================================================
    # 4. 金额处理（直接报价）
    # =========================================================
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

    def _extract_direct_price_pairs(self, section_text: str) -> List[Dict]:
        if not section_text or not section_text.strip():
            return []

        lines = [line.strip() for line in section_text.splitlines() if line.strip()]
        pairs = []

        current_small_str = None
        current_small_val = None

        for line in lines:
            inline_match = re.search(
                r"小写[：:]\s*([￥¥]?\s*[\d,，]+(?:\.\d+)?\s*元?)"
                r".{0,50}?"
                r"大写[：:]\s*([零〇壹贰叁肆伍陆柒捌玖拾佰仟万亿元角分整正圆]+)",
                line
            )
            if inline_match:
                small_str = inline_match.group(1).strip()
                capital_str = inline_match.group(2).strip()

                pairs.append({
                    "small_price_str": small_str,
                    "small_price": self._clean_small_price(small_str),
                    "capital_price_str": capital_str,
                    "capital_price": self._capital_to_number(capital_str)
                })
                current_small_str = None
                current_small_val = None
                continue

            small_match = re.search(
                r"小写[：:]\s*([￥¥]?\s*[\d,，]+(?:\.\d+)?\s*元?)",
                line
            )
            if small_match:
                current_small_str = small_match.group(1).strip()
                current_small_val = self._clean_small_price(current_small_str)

                capital_match_same_line = re.search(
                    r"大写[：:]\s*([零〇壹贰叁肆伍陆柒捌玖拾佰仟万亿元角分整正圆]+)",
                    line
                )
                if capital_match_same_line:
                    capital_str = capital_match_same_line.group(1).strip()
                    pairs.append({
                        "small_price_str": current_small_str,
                        "small_price": current_small_val,
                        "capital_price_str": capital_str,
                        "capital_price": self._capital_to_number(capital_str)
                    })
                    current_small_str = None
                    current_small_val = None
                continue

            capital_match = re.search(
                r"大写[：:]\s*([零〇壹贰叁肆伍陆柒捌玖拾佰仟万亿元角分整正圆]+)",
                line
            )
            if capital_match and current_small_str is not None:
                capital_str = capital_match.group(1).strip()
                pairs.append({
                    "small_price_str": current_small_str,
                    "small_price": current_small_val,
                    "capital_price_str": capital_str,
                    "capital_price": self._capital_to_number(capital_str)
                })
                current_small_str = None
                current_small_val = None

        return pairs

    # =========================================================
    # 5. 下浮率：逐行抽取 + 规则抽取 + 逐行判断
    # =========================================================
    def _normalize_biz_name(self, name: str) -> str:
        n = self._normalize(name)
        n = n.replace("费", "")
        n = n.replace("项目", "")
        n = n.replace("业务名称", "")
        return n

    def _extract_float_rate_rows(self, section_text: str) -> List[Dict]:
        if not section_text or not section_text.strip():
            return []

        lines = [line.strip() for line in section_text.splitlines() if line.strip()]
        rows = []

        for line in lines:
            normalized = self._normalize(line)

            if any(k in normalized for k in [
                "业务名称", "暂估金额", "投标下浮率", "投标报价",
                "备注", "注：", "注:", "合计", "法定代表人", "日期",
                "项目名称", "开标一览表", "报价一览表", "投标一览表"
            ]):
                continue

            if not any(k in normalized for k in ["维护", "抢维修", "资产盘点"]):
                continue

            compact_line = re.sub(r"\s+", " ", line).strip()

            biz_match = re.match(r"^\s*([^\d]+?)\s+\d", compact_line)
            biz_name = biz_match.group(1).strip() if biz_match else compact_line

            nums = re.findall(r"\d+(?:\.\d+)?", compact_line)
            if len(nums) < 4:
                continue

            try:
                estimated_amount = float(nums[0])
                tax_rate = float(nums[1])
                float_rate = float(nums[2])
                bid_price = float(nums[3])
            except ValueError:
                continue

            if not (0 <= tax_rate <= 100):
                continue
            if not (0 <= float_rate <= 100):
                continue

            rows.append({
                "biz_name": self._normalize_biz_name(biz_name),
                "estimated_amount": estimated_amount,
                "tax_rate": tax_rate,
                "float_rate": float_rate,
                "bid_price": bid_price,
                "raw_line": compact_line
            })

        return rows

    def _phrase_to_operator(self, phrase: str) -> str:
        mapping = {
            "不低于": ">=",
            "不少于": ">=",
            "大于": ">",
            "高于": ">",
            "低于": "<",
            "小于": "<",
            "不高于": "<=",
            "不大于": "<=",
        }
        return mapping.get(phrase, ">=")

    def _extract_float_rate_rules(self, section_text: str) -> Dict[str, Dict]:
        if not section_text or not section_text.strip():
            return {}

        text = self._normalize(section_text)
        rules: Dict[str, Dict] = {}

        # 1) 多行业务规则
        pattern = r"(代维区域维护费|代维区域抢维修费|代维区域资产盘点费)下浮率(不低于|不少于|大于|高于|低于|小于|不高于|不大于)(\d+(?:\.\d+)?)%"
        for biz, phrase, threshold in re.findall(pattern, text):
            biz_name = self._normalize_biz_name(biz)
            base_op = self._phrase_to_operator(phrase)

            rules[biz_name] = {
                "raw_rule": f"{biz}下浮率{phrase}{threshold}%",
                "phrase": phrase,
                "base_op": base_op,
                "op": base_op,
                "threshold": float(threshold)
            }

        # 2) 通用单一下浮率规则
        generic_patterns = [
            r"本项目下浮率(?:须须|须|应|必须)?(大于|高于|不低于|不少于|低于|小于|不高于|不大于)[“\"]?(\d+(?:\.\d+)?)%[”\"]?",
            r"下浮率(?:须须|须|应|必须)?(大于|高于|不低于|不少于|低于|小于|不高于|不大于)[“\"]?(\d+(?:\.\d+)?)%[”\"]?",
        ]
        for gp in generic_patterns:
            m = re.search(gp, text)
            if m:
                phrase = m.group(1)
                threshold = float(m.group(2))
                base_op = self._phrase_to_operator(phrase)
                rules["__generic__"] = {
                    "raw_rule": f"下浮率{phrase}{threshold}%",
                    "phrase": phrase,
                    "base_op": base_op,
                    "op": base_op,
                    "threshold": threshold
                }
                break

        # 若命中“低于或等于...将被否决”，则把 >= 收紧成 >
        if "低于或等于所要求的下浮比例" in text and "其投标将被否决" in text:
            for biz_name in rules:
                if rules[biz_name]["op"] in {">=", ">"}:
                    rules[biz_name]["op"] = ">"

        # 若命中“否则其投标将被否决”，且原规则是“不低于/不少于”，也收紧成 >
        if "否则其投标将被否决" in text:
            for biz_name in rules:
                if rules[biz_name]["op"] in {">=", ">"} and rules[biz_name]["phrase"] in {"不低于", "不少于"}:
                    rules[biz_name]["op"] = ">"

        return rules

    def _compare_by_rule(self, actual: float, op: str, threshold: float) -> bool:
        if op == ">":
            return actual > threshold
        if op == ">=":
            return actual >= threshold
        if op == "<":
            return actual < threshold
        if op == "<=":
            return actual <= threshold
        if op == "==":
            return abs(actual - threshold) < 1e-9
        return False

    def _match_rule_for_row(self, biz_name: str, rules: Dict[str, Dict]) -> Optional[Dict]:
        normalized_biz = self._normalize_biz_name(biz_name)

        if normalized_biz in rules:
            return rules[normalized_biz]

        for rule_name, rule in rules.items():
            if rule_name == "__generic__":
                continue
            if rule_name in normalized_biz or normalized_biz in rule_name:
                return rule

        aliases = {
            "代维区域维护": ["维护"],
            "代维区域抢维修": ["抢维修"],
            "代维区域资产盘点": ["资产盘点"],
        }
        for canonical, keys in aliases.items():
            if any(k in normalized_biz for k in keys) and canonical in rules:
                return rules[canonical]

        if "__generic__" in rules:
            return rules["__generic__"]

        return None

    def _check_float_rate_rows_compliance(self, rows: List[Dict], rules: Dict[str, Dict]) -> Tuple[bool, List[str]]:
        if not rows:
            return False, ["未找到下浮率业务行"]

        summary = []
        all_passed = True

        for row in rows:
            biz_name = row["biz_name"]
            float_rate = row["float_rate"]
            matched_rule = self._match_rule_for_row(biz_name, rules)

            if matched_rule:
                op = matched_rule["op"]
                threshold = matched_rule["threshold"]
                passed = self._compare_by_rule(float_rate, op, threshold)
                if not passed:
                    all_passed = False
                summary.append(
                    f"{biz_name}：{float_rate:.2f}% {op} {threshold:g}% ，{'合格' if passed else '不合格'}"
                )
            else:
                passed = float_rate > self.min_float_rate
                if not passed:
                    all_passed = False
                summary.append(
                    f"{biz_name}：{float_rate:.2f}% > {self.min_float_rate:g}% ，{'合格' if passed else '不合格'}"
                )

        return all_passed, summary

    def _extract_single_float_rate_from_table(self, parsed: Dict, bid_opening_text: str) -> Optional[float]:
        """
        优先从表格中提取“实际下浮率”，避免误取规则门槛值
        """
        candidates = []

        for sec in parsed.get("table_sections", []):
            table_text = sec.get("text", "")
            normalized = self._normalize(table_text)

            if "下浮率" not in normalized:
                continue

            percents = re.findall(r"(\d+(?:\.\d+)?)\s*%", table_text)
            for p in percents:
                val = self._safe_float(p)
                if val is None:
                    continue
                if 0 <= val <= 100:
                    candidates.append(val)

        if not candidates:
            percents = re.findall(r"(\d+(?:\.\d+)?)\s*%", bid_opening_text)
            for p in percents:
                val = self._safe_float(p)
                if val is None:
                    continue
                if 0 <= val <= 100:
                    candidates.append(val)

        if not candidates:
            return None

        # 常见税率优先排除
        non_tax_candidates = [v for v in candidates if v not in {3.0, 6.0, 9.0, 13.0}]
        if non_tax_candidates:
            return non_tax_candidates[0]

        return candidates[0]

    def _extract_single_float_rate_fallback(self, text: str) -> Optional[float]:
        if not text or not text.strip():
            return None

        lines = [line.strip() for line in text.splitlines() if line.strip()]
        normalized_lines = [self._normalize(line) for line in lines]

        inline_patterns = [
            r"(?:实际)?(?:报价)?下浮率[：:\s（(]*%?[）)]*[：:\s]*([\d]+(?:\.\d+)?)\s*%",
            r"(?:实际)?(?:报价)?下浮率[：:\s]*([\d]+(?:\.\d+)?)",
            r"(?:实际)?(?:报价)?下浮[：:\s]*([\d]+(?:\.\d+)?)\s*%",
            r"(?:优惠率)[：:\s]*([\d]+(?:\.\d+)?)\s*%",
            r"(?:折扣率)[：:\s]*([\d]+(?:\.\d+)?)\s*%",
        ]

        for line in normalized_lines:
            # 跳过明显规则行，避免取到 1.5%
            if any(k in line for k in
                   ["大于", "高于", "不低于", "不少于", "低于", "小于", "不高于", "不大于", "否决", "否则"]):
                continue

            for pattern in inline_patterns:
                match = re.search(pattern, line, re.IGNORECASE)
                if match:
                    val = self._safe_float(match.group(1))
                    if val is not None and 0 <= val <= 100:
                        return val

        return None

    # =========================================================
    # 6. 核心校验
    # =========================================================
    def check_price_compliance(self, source: Any) -> Dict:
        parsed = self._parse_input(source)
        _, bid_opening_text = self._locate_bid_opening_page_and_text(parsed)

        if not bid_opening_text:
            return self._build_fail_result("未找到开标/报价/投标一览表正文")

        # 1) 直接报价
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
                summary=summary
            )

        # 2) 多业务下浮率
        rows = self._extract_float_rate_rows(bid_opening_text)
        rules = self._extract_float_rate_rules(bid_opening_text)

        if rows:
            passed, summary = self._check_float_rate_rows_compliance(rows, rules)
            return self._build_result(
                result_text="合格" if passed else "失败",
                price_type="下浮率报价",
                summary=summary
            )

        # 3) 单一下浮率兜底
        single_float_rate = self._extract_single_float_rate_from_table(parsed, bid_opening_text)
        if single_float_rate is None:
            single_float_rate = self._extract_single_float_rate_fallback(bid_opening_text)

        if single_float_rate is not None:
            # 优先使用通用规则
            if "__generic__" in rules:
                rule = rules["__generic__"]
                op = rule["op"]
                threshold = rule["threshold"]
                passed = self._compare_by_rule(single_float_rate, op, threshold)
                summary = [f"下浮率：{single_float_rate:.2f}% {op} {threshold:g}% ，{'合格' if passed else '不合格'}"]
            elif len(rules) == 1:
                only_rule = list(rules.values())[0]
                op = only_rule["op"]
                threshold = only_rule["threshold"]
                passed = self._compare_by_rule(single_float_rate, op, threshold)
                summary = [f"下浮率：{single_float_rate:.2f}% {op} {threshold:g}% ，{'合格' if passed else '不合格'}"]
            else:
                passed = single_float_rate > self.min_float_rate
                summary = [
                    f"下浮率：{single_float_rate:.2f}% > {self.min_float_rate:g}% ，{'合格' if passed else '不合格'}"]

            return self._build_result(
                result_text="合格" if passed else "失败",
                price_type="下浮率报价",
                summary=summary
            )

        return self._build_fail_result("一览表中未找到直接报价或下浮率报价信息")

    def check_price_reasonableness(self, source: Any) -> Dict:
        return self.check_price_compliance(source)
