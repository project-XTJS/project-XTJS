import re
from typing import Dict, List, Optional


class ReasonablenessChecker:
    """开标一览表报价合理性检查类"""

    def __init__(self, min_float_rate: float = 1.5):
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

    # =========================================================
    # 1. 开标一览表提取
    # =========================================================
    def _normalize(self, s: str) -> str:
        if s is None:
            return ""
        return re.sub(r"\s+", "", str(s))

    def _is_catalog_line(self, line: str) -> bool:
        """
        更严格识别目录行。
        解决这种情况：
        一、投标保证书..-3 二、开标一览表..-5 三、商务条款偏离表..6
        """
        normalized = self._normalize(line)

        # 传统目录特征
        if "格式参见本章附件" in normalized:
            return True

        # 目录引导符 + 页码
        if re.search(r"\.\.\-?\d+", normalized):
            return True
        if re.search(r"\.{2,}\d+", normalized):
            return True
        if re.search(r"……\d+", normalized):
            return True

        # 一行里出现多个章节标题，通常就是目录
        catalog_title_hits = sum(
            1 for token in [
                "投标保证书",
                "开标一览表",
                "分项报价表",
                "商务条款偏离表",
                "技术条款偏离表",
                "投标人基本情况介绍",
                "类似项目业绩清单",
                "投标人的资格证明文件",
                "项目人员情况",
            ]
            if token in normalized
        )
        if catalog_title_hits >= 2:
            return True

        # 以章节序号开头，且包含目录特征
        if re.search(r"^[一二三四五六七八九十]+、", normalized) and (
            ".." in normalized or ".-" in normalized or "..." in normalized
        ):
            return True

        return False

    def _score_bid_opening_candidate(self, lines: List[str], idx: int) -> int:
        """
        给“开标一览表”候选起点打分。
        分数越高，越像正文，而不是目录。
        """
        window = lines[idx: idx + 10]  # 缩小窗口，避免目录行因为后面正文太远而误命中
        window_text = "\n".join(window)
        normalized_window = self._normalize(window_text)
        current_line = self._normalize(lines[idx])

        direct_keys = [
            "附件2开标一览表",
            "项目名称",
            "招标编号",
            "货币单位",
            "投标总价",
            "小写",
            "大写",
            "交货期",
            "交货地点",
            "质保期",
        ]
        float_keys = [
            "投标函附录A",
            "建设工程名称",
            "单位工程名称",
            "下浮率",
            "税率",
            "报价",
        ]

        direct_hit = sum(1 for key in direct_keys if key in normalized_window)
        float_hit = sum(1 for key in float_keys if key in normalized_window)

        score = direct_hit * 3 + float_hit * 3

        # 当前行本身越像标题，分越高
        if current_line in {"开标一览表", "二、开标一览表"}:
            score += 6
        if "投标函附录A" in current_line and "开标一览表" in current_line:
            score += 6
        if "附件2开标一览表" in current_line:
            score += 6

        # 当前行是目录则强力减分
        if self._is_catalog_line(lines[idx]):
            score -= 10

        return score

    def _extract_bid_opening_section(self, text: str) -> str:
        if not text or not text.strip():
            return ""

        lines = [line.strip() for line in text.splitlines() if line.strip()]
        normalized_lines = [self._normalize(line) for line in lines]

        best_idx = None
        best_score = -999

        for idx, line in enumerate(normalized_lines):
            if "开标一览表" not in line:
                continue

            score = self._score_bid_opening_candidate(lines, idx)

            if score > best_score:
                best_score = score
                best_idx = idx

        # 分太低，说明找到的都是目录型命中
        if best_idx is None or best_score < 3:
            return ""

        start_idx = best_idx

        end_idx = len(lines)
        for idx in range(start_idx + 1, len(normalized_lines)):
            current = normalized_lines[idx]

            if any(title in current for title in [
                "分项报价表",
                "商务条款偏离表",
                "技术条款偏离表",
                "投标人基本情况介绍",
                "类似项目业绩清单",
                "投标人的资格证明文件",
                "项目人员情况"
            ]):
                end_idx = idx
                break

        return "\n".join(lines[start_idx:end_idx]).strip()

    # =========================================================
    # 2. 金额处理
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

    # =========================================================
    # 3. 直接报价信息提取
    # =========================================================
    def _extract_direct_price_pairs(self, section_text: str) -> List[Dict]:
        if not section_text or not section_text.strip():
            return []

        lines = [line.strip() for line in section_text.splitlines() if line.strip()]
        pairs = []

        current_small_str = None
        current_small_val = None

        for line in lines:
            # 同一行：小写 + 大写
            inline_match = re.search(
                r"小写[：:]\s*([￥¥]?\s*[\d,，]+(?:\.\d+)?\s*元?)"
                r".{0,30}?"
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

            # 跨行：先小写
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

            # 跨行：后大写
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
    # 4. 下浮率相关
    # =========================================================
    def _is_threshold_line(self, line: str) -> bool:
        normalized = self._normalize(line)
        return any(key in normalized for key in [
            "大于", "小于", "不少于", "不低于", "不高于",
            "须", "应", "否则", "否决", "废标", "注：", "注:"
        ])

    def _parse_percent_value(self, s: str) -> Optional[float]:
        match = re.search(r"(\d+(?:\.\d+)?)\s*%", s)
        if not match:
            return None
        try:
            return float(match.group(1))
        except ValueError:
            return None

    def _extract_float_rate(self, text: str) -> Optional[float]:
        if not text or not text.strip():
            return None

        lines = [line.strip() for line in text.splitlines() if line.strip()]
        normalized_lines = [self._normalize(line) for line in lines]

        inline_patterns = [
            r"(?:实际)?(?:报价)?下浮率[：:\s（(]*%?[）)]*[：:\s]*([\d]+(?:\.\d+)?)\s*%",
            r"(?:实际)?(?:报价)?下浮[：:\s]*([\d]+(?:\.\d+)?)\s*%",
        ]
        for line in normalized_lines:
            if self._is_threshold_line(line):
                continue
            for pattern in inline_patterns:
                match = re.search(pattern, line, re.IGNORECASE)
                if match:
                    try:
                        return float(match.group(1))
                    except ValueError:
                        pass

        for idx, line in enumerate(normalized_lines):
            if "下浮率" not in line:
                continue

            window = normalized_lines[idx: idx + 8]
            candidates = []
            for j, wline in enumerate(window[1:], start=1):
                if self._is_threshold_line(wline):
                    continue

                percent_val = self._parse_percent_value(wline)
                if percent_val is None:
                    continue

                candidates.append((j, percent_val, wline))

            if candidates:
                candidates.sort(key=lambda x: x[0])
                return candidates[0][1]

        joined = "\n".join(normalized_lines)
        if "下浮率" in joined and "税率" in joined:
            percent_candidates = []
            for idx, line in enumerate(normalized_lines):
                percent_val = self._parse_percent_value(line)
                if percent_val is None:
                    continue

                context = "".join(normalized_lines[max(0, idx - 1): min(len(normalized_lines), idx + 2)])
                if any(key in context for key in ["大于", "小于", "不少于", "不低于", "否则", "否决", "须"]):
                    continue

                percent_candidates.append(percent_val)

            if percent_candidates:
                return percent_candidates[0]

        return None

    def _check_float_rate_compliance(self, float_rate: Optional[float]) -> Dict:
        if float_rate is None:
            return {
                "is_qualified": False,
                "message": "未找到下浮率信息"
            }

        if float_rate > self.min_float_rate:
            return {
                "is_qualified": True,
                "message": f"下浮率 {float_rate}% ，满足“大于 {self.min_float_rate}%”要求"
            }

        return {
            "is_qualified": False,
            "message": f"下浮率 {float_rate}% ，不满足“大于 {self.min_float_rate}%”要求"
        }

    # =========================================================
    # 5. 核心校验
    # =========================================================
    def check_price_compliance(self, text: str) -> Dict:
        result = {
            "has_bid_opening_section": False,
            "price_check_type": None,
            "check_result": "pending",
            "details": {},
            "price_score": 0
        }

        bid_opening_text = self._extract_bid_opening_section(text)
        if not bid_opening_text:
            result["check_result"] = "fail"
            result["details"] = {
                "error": "未找到开标一览表正文"
            }
            return result

        result["has_bid_opening_section"] = True

        # 1) 先尝试直接报价模式
        price_pairs = self._extract_direct_price_pairs(bid_opening_text)
        if price_pairs:
            result["price_check_type"] = "直接报价"

            checked_pairs = []
            all_match = True

            for pair in price_pairs:
                small_price = pair["small_price"]
                capital_price = pair["capital_price"]

                is_match = (
                    small_price is not None
                    and capital_price is not None
                    and abs(small_price - capital_price) < 0.01
                )

                if not is_match:
                    all_match = False

                checked_pairs.append({
                    "small_price_str": pair["small_price_str"],
                    "small_price": small_price,
                    "capital_price_str": pair["capital_price_str"],
                    "capital_price": capital_price,
                    "capital_matches_small": is_match
                })

            result["details"]["price_pairs"] = checked_pairs
            result["details"]["pair_count"] = len(checked_pairs)

            if all_match:
                result["check_result"] = "合格"
                result["price_score"] = 100
            else:
                result["check_result"] = "失败"
                result["price_score"] = 0
                result["details"]["error"] = "存在大小写金额不一致的报价项"

            return result

        # 2) 再识别下浮率报价模式
        float_rate = self._extract_float_rate(bid_opening_text)
        if float_rate is not None:
            compliance = self._check_float_rate_compliance(float_rate)

            result["price_check_type"] = "下浮率报价"
            result["check_result"] = "合格" if compliance["is_qualified"] else "失败"
            result["price_score"] = 100 if compliance["is_qualified"] else 0
            result["details"].update({
                "float_rate": float_rate,
                "min_float_rate": self.min_float_rate,
                "is_qualified": compliance["is_qualified"],
                "message": compliance["message"]
            })
            return result

        # 3) 未识别
        result["price_check_type"] = "未识别"
        result["check_result"] = "fail"
        result["details"]["error"] = "开标一览表中未找到直接报价或下浮率报价信息"
        return result

    def check_price_reasonableness(self, text: str) -> Dict:
        return self.check_price_compliance(text)