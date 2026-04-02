import json
import os
import re
from typing import Any, Dict, List, Optional, Tuple


class ReasonablenessChecker:
    """报价合理性检查类（保留直接报价逻辑，仅增强下浮率报价逻辑）"""

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

        self.FLOAT_RULE_PHRASES = [
            "低于或等于",
            "高于或等于",
            "不低于",
            "不少于",
            "不高于",
            "不大于",
            "大于",
            "高于",
            "低于",
            "小于",
            "等于",
        ]

        self.COMMON_TAX_RATES = {3.0, 6.0, 9.0, 13.0}

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

    def _clean_percent(self, s: Any) -> Optional[float]:
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
                "logical_tables": []
            }

        text = str(source) if source is not None else ""
        return {
            "raw_text": text,
            "sections": [{"page": None, "type": "text", "text": text}],
            "table_sections": [],
            "logical_tables": []
        }

    def _parse_json_dict(self, data: Dict) -> Dict:
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
            "table_sections": parsed_table_sections,
            "logical_tables": logical_tables
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
    # 4. 金额处理（直接报价）——保持你的原代码不变
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
    # 5. 下浮率逻辑（增强版）
    # =========================================================
    def _normalize_biz_name(self, name: str) -> str:
        n = self._normalize(name)
        for token in [
            "业务名称", "单位工程名称", "建设工程名称", "项目名称",
            "报价", "投标报价", "投标下浮率", "下浮率", "税率",
            "暂估金额", "备注", "（含税/万元）", "(含税/万元)",
            "（%）", "(%)", "费", "项目"
        ]:
            n = n.replace(token, "")
        return n.strip("：:()（）-—_/、 ")

    def _char_ngrams(self, s: str, n: int = 2) -> set:
        if not s:
            return set()
        if len(s) <= n:
            return {s}
        return {s[i:i + n] for i in range(len(s) - n + 1)}

    def _name_similarity(self, a: str, b: str) -> float:
        aa = self._normalize_biz_name(a)
        bb = self._normalize_biz_name(b)
        if not aa or not bb:
            return 0.0
        if aa == bb:
            return 1.0
        if aa in bb or bb in aa:
            return 0.9
        ga = self._char_ngrams(aa, 2)
        gb = self._char_ngrams(bb, 2)
        if not ga or not gb:
            return 0.0
        return len(ga & gb) / max(len(ga | gb), 1)

    def _phrase_to_operator(self, phrase: str) -> str:
        mapping = {
            "低于或等于": "<=",
            "高于或等于": ">=",
            "不低于": ">=",
            "不少于": ">=",
            "大于": ">",
            "高于": ">",
            "低于": "<",
            "小于": "<",
            "不高于": "<=",
            "不大于": "<=",
            "等于": "==",
        }
        return mapping.get(phrase, ">=")

    def _split_rule_sentences(self, text: str) -> List[str]:
        if not text:
            return []
        parts = re.split(r"[。\n；;]", text)
        return [p.strip() for p in parts if p and p.strip()]

    def _extract_float_rate_rules(self, section_text: str) -> Dict[str, Dict]:
        if not section_text or not section_text.strip():
            return {}

        text = section_text
        normalized_text = self._normalize(text)
        rules: Dict[str, Dict] = {}

        sentences = self._split_rule_sentences(text)

        biz_pattern = (
            r"(?P<name>[\u4e00-\u9fa5A-Za-z0-9（）()\-、/]+?)"
            r"(?:报价)?(?:费|服务费|业务|项目)?"
            r"下浮率"
            r"(?:须|应|必须|需|不得)?"
            r"(?P<phrase>低于或等于|高于或等于|不低于|不少于|不高于|不大于|大于|高于|低于|小于|等于)"
            r"[“\"']?(?P<threshold>\d+(?:\.\d+)?)%[”\"']?"
        )

        for sentence in sentences:
            normalized_sentence = self._normalize(sentence)
            if "下浮率" not in normalized_sentence:
                continue

            for m in re.finditer(biz_pattern, normalized_sentence):
                raw_name = m.group("name")
                phrase = m.group("phrase")
                threshold = float(m.group("threshold"))
                biz_name = self._normalize_biz_name(raw_name)
                if not biz_name:
                    continue

                base_op = self._phrase_to_operator(phrase)
                rules[biz_name] = {
                    "raw_rule": m.group(0),
                    "biz_name": biz_name,
                    "phrase": phrase,
                    "base_op": base_op,
                    "op": base_op,
                    "threshold": threshold
                }

        generic_patterns = [
            r"本项目下浮率(?:须|应|必须|需|不得)?(低于或等于|高于或等于|不低于|不少于|不高于|不大于|大于|高于|低于|小于|等于)[“\"']?(\d+(?:\.\d+)?)%[”\"']?",
            r"下浮率(?:须|应|必须|需|不得)?(低于或等于|高于或等于|不低于|不少于|不高于|不大于|大于|高于|低于|小于|等于)[“\"']?(\d+(?:\.\d+)?)%[”\"']?",
        ]
        for gp in generic_patterns:
            m = re.search(gp, normalized_text)
            if m:
                phrase = m.group(1)
                threshold = float(m.group(2))
                base_op = self._phrase_to_operator(phrase)
                rules["__generic__"] = {
                    "raw_rule": m.group(0),
                    "biz_name": "__generic__",
                    "phrase": phrase,
                    "base_op": base_op,
                    "op": base_op,
                    "threshold": threshold
                }
                break

        # 语义收紧
        if (
                "低于或等于所要求的下浮比例" in normalized_text or "低于或等于所要求下浮比例" in normalized_text) and "否决" in normalized_text:
            for key in rules:
                if rules[key]["op"] in {">=", ">"}:
                    rules[key]["op"] = ">"

        if (
                "高于或等于所要求的下浮比例" in normalized_text or "高于或等于所要求下浮比例" in normalized_text) and "否决" in normalized_text:
            for key in rules:
                if rules[key]["op"] in {"<=", "<"}:
                    rules[key]["op"] = "<"

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

        best_rule = None
        best_score = 0.0

        for rule_name, rule in rules.items():
            if rule_name == "__generic__":
                continue
            score = self._name_similarity(normalized_biz, rule_name)
            if score > best_score:
                best_score = score
                best_rule = rule

        if best_rule is not None and best_score >= 0.35:
            return best_rule

        if "__generic__" in rules:
            return rules["__generic__"]

        return None

    def _table_has_float_keywords(self, tb: Dict) -> bool:
        texts: List[str] = []

        for h in tb.get("headers", []) or []:
            if h:
                texts.append(str(h))

        for row in (tb.get("rows", []) or [])[:6]:
            if isinstance(row, list):
                texts.extend(str(x) for x in row if x is not None)

        for rec in (tb.get("records", []) or [])[:6]:
            if isinstance(rec, dict):
                texts.extend(str(v) for v in rec.values() if v is not None)

        normalized = self._normalize(" ".join(texts))
        return (
                "下浮率" in normalized
                and (
                        "税率" in normalized
                        or "报价" in normalized
                        or "业务名称" in normalized
                        or "单位工程名称" in normalized
                        or "建设工程名称" in normalized
                        or "项目名称" in normalized
                )
        )

    def _is_generic_table_headers(self, headers: List[str]) -> bool:
        normalized_headers = [self._normalize(h) for h in headers if self._normalize(h)]
        if not normalized_headers:
            return True
        return all(re.fullmatch(r"col_\d+", h, re.IGNORECASE) for h in normalized_headers)

    def _is_header_like_cell(self, cell: Any) -> bool:
        normalized = self._normalize(cell)
        if not normalized:
            return False
        header_tokens = [
            "业务名称", "单位工程名称", "建设工程名称", "项目名称",
            "报价", "投标报价", "下浮率", "投标下浮率", "税率", "备注"
        ]
        return any(token in normalized for token in header_tokens)

    def _pick_bid_opening_logical_tables(self, parsed: Dict, bid_page: Optional[int]) -> List[Dict]:
        logical_tables = parsed.get("logical_tables", []) or []
        if not logical_tables:
            return []

        candidates = []
        for tb in logical_tables:
            pages = tb.get("pages", []) or []
            if bid_page is not None and bid_page not in pages:
                continue
            if self._table_has_float_keywords(tb):
                candidates.append(tb)

        if candidates:
            return candidates

        for tb in logical_tables:
            if self._table_has_float_keywords(tb):
                candidates.append(tb)
        return candidates

    def _find_key_by_candidates(self, record: Dict, candidates: List[str]) -> Optional[str]:
        """优先精确匹配，再做弱匹配，避免“报价”误命中“投标下浮率”之类字段。"""
        keys = list(record.keys())
        normalized_map = {self._normalize(k): k for k in keys}

        for cand in candidates:
            nc = self._normalize(cand)
            if nc in normalized_map:
                return normalized_map[nc]

        for real_key in keys:
            nk = self._normalize(real_key)
            for cand in candidates:
                nc = self._normalize(cand)
                if nk == nc:
                    return real_key

        for real_key in keys:
            nk = self._normalize(real_key)
            for cand in candidates:
                nc = self._normalize(cand)
                if nc and nk.startswith(nc):
                    return real_key

        for real_key in keys:
            nk = self._normalize(real_key)
            for cand in candidates:
                nc = self._normalize(cand)
                if nc and nc in nk:
                    return real_key

        return None

    def _extract_float_rate_rows_from_record_table(self, tb: Dict) -> List[Dict]:
        rows = []
        records = tb.get("records", []) or []

        for rec in records:
            if not isinstance(rec, dict):
                continue

            biz_key = self._find_key_by_candidates(rec, ["业务名称", "单位工程名称", "建设工程名称", "项目名称"])
            float_key = self._find_key_by_candidates(rec, ["投标下浮率（%）", "下浮率（%）", "投标下浮率", "下浮率"])
            tax_key = self._find_key_by_candidates(rec, ["税率（%）", "税率"])
            est_key = self._find_key_by_candidates(rec, ["暂估金额（含税/万元）", "暂估金额", "暂估金额（万元）"])
            bid_key = self._find_key_by_candidates(rec, ["投标报价（含税/万元）", "投标报价", "报价"])

            if not biz_key or not float_key:
                continue

            biz_name_raw = str(rec.get(biz_key, "")).strip()
            biz_name = self._normalize_biz_name(biz_name_raw)
            if not biz_name or biz_name in {"合计", "总计"}:
                continue

            float_raw = rec.get(float_key)
            bid_raw = rec.get(bid_key) if bid_key else None

            float_rate = self._clean_percent(float_raw)
            if float_rate is None or not (0 <= float_rate <= 100):
                continue

            rows.append({
                "biz_name": biz_name,
                "biz_name_raw": biz_name_raw,
                "estimated_amount": self._clean_number(rec.get(est_key)) if est_key else None,
                "tax_rate": self._clean_percent(rec.get(tax_key)) if tax_key else None,
                "float_rate": float_rate,
                "bid_price": self._clean_number(bid_raw) if bid_raw is not None else None,
                "raw_line": json.dumps(rec, ensure_ascii=False)
            })

        return rows

    def _find_logical_table_data_start(self, raw_rows: List[List[Any]]) -> int:
        for idx, row in enumerate(raw_rows):
            cells = [str(x).strip() for x in row if str(x).strip()]
            if not cells:
                continue
            if all(self._is_header_like_cell(c) for c in cells):
                continue
            has_percent = any("%" in c or "％" in c for c in cells)
            has_number = any(re.search(r"\d", c) for c in cells)
            if has_percent or has_number:
                return idx
        return len(raw_rows)

    def _extract_float_rate_rows_from_row_table(self, tb: Dict) -> List[Dict]:
        rows = []
        raw_rows = tb.get("rows", []) or []
        if not raw_rows:
            return rows

        data_start = self._find_logical_table_data_start(raw_rows)

        for row in raw_rows[data_start:]:
            if not isinstance(row, list):
                continue

            cells = [str(x).strip() if x is not None else "" for x in row]
            non_empty = [c for c in cells if c]
            if not non_empty:
                continue

            normalized_joined = self._normalize(" ".join(non_empty))
            if "合计" in normalized_joined or all(self._is_header_like_cell(c) for c in non_empty):
                continue

            biz_idx = None
            biz_name_raw = ""
            for idx, cell in enumerate(cells):
                if not cell or cell == "/":
                    continue
                if self._is_header_like_cell(cell):
                    continue
                if re.search(r"[一-龥A-Za-z]", cell):
                    biz_idx = idx
                    biz_name_raw = cell
                    break

            if biz_idx is None:
                continue

            biz_name = self._normalize_biz_name(biz_name_raw)
            if not biz_name or biz_name in {"合计", "总计"}:
                continue

            tokens = []
            for idx, cell in enumerate(cells):
                if idx == biz_idx:
                    continue
                if not cell or cell == "/":
                    continue
                for m in re.finditer(r"(\d+(?:\.\d+)?)(\s*[%％])?", cell):
                    val = self._safe_float(m.group(1))
                    if val is None:
                        continue
                    tokens.append({
                        "value": val,
                        "has_percent": bool(m.group(2)),
                        "cell_index": idx,
                    })

            if not tokens:
                continue

            percent_values = [t["value"] for t in tokens if t["has_percent"] and 0 <= t["value"] <= 100]
            non_tax_percents = [v for v in percent_values if v not in self.COMMON_TAX_RATES]

            float_rate = None
            if non_tax_percents:
                float_rate = non_tax_percents[0]
            elif percent_values:
                float_rate = percent_values[0]
            else:
                small_non_tax = [t["value"] for t in tokens if
                                 0 <= t["value"] <= 100 and t["value"] not in self.COMMON_TAX_RATES]
                if small_non_tax:
                    float_rate = small_non_tax[0]

            if float_rate is None or not (0 <= float_rate <= 100):
                continue

            tax_rate = None
            for t in tokens:
                val = t["value"]
                if abs(val - float_rate) < 1e-9:
                    continue
                if val in self.COMMON_TAX_RATES or (0 <= val <= 20 and not t["has_percent"]):
                    tax_rate = val
                    break

            big_values = [t["value"] for t in tokens if t["value"] > 100]
            estimated_amount = big_values[0] if big_values else None
            bid_price = big_values[-1] if len(big_values) >= 2 else None

            rows.append({
                "biz_name": biz_name,
                "biz_name_raw": biz_name_raw,
                "estimated_amount": estimated_amount,
                "tax_rate": tax_rate,
                "float_rate": float_rate,
                "bid_price": bid_price,
                "raw_line": " | ".join(non_empty),
            })

        return rows

    def _extract_float_rate_rows_from_logical_tables(self, parsed: Dict, bid_page: Optional[int]) -> List[Dict]:
        rows = []
        tables = self._pick_bid_opening_logical_tables(parsed, bid_page)

        for tb in tables:
            table_rows = self._extract_float_rate_rows_from_record_table(tb)
            if not table_rows:
                table_rows = self._extract_float_rate_rows_from_row_table(tb)

            for row in table_rows:
                key = (row["biz_name"], round(row["float_rate"], 4))
                if not any((r["biz_name"], round(r["float_rate"], 4)) == key for r in rows):
                    rows.append(row)

        return rows

    def _parse_flat_row_numbers(self, segment: str) -> Optional[Dict]:
        if not segment or not segment.strip():
            return None

        token_matches = list(re.finditer(r"(\d+(?:\.\d+)?)(\s*[%％])?", segment))
        if len(token_matches) < 3:
            return None

        values = []
        for m in token_matches:
            val = self._safe_float(m.group(1))
            if val is None:
                continue
            values.append({
                "value": val,
                "has_percent": bool(m.group(2)),
            })

        if len(values) < 3:
            return None

        percent_values = [x["value"] for x in values if x["has_percent"] and 0 <= x["value"] <= 100]
        non_tax_percents = [v for v in percent_values if v not in self.COMMON_TAX_RATES]

        float_rate = None
        if non_tax_percents:
            float_rate = non_tax_percents[0]
        elif len(percent_values) >= 2:
            float_rate = percent_values[1]
        elif percent_values:
            float_rate = percent_values[0]

        if float_rate is None or not (0 <= float_rate <= 100):
            return None

        tax_rate = None
        for v in percent_values:
            if abs(v - float_rate) < 1e-9:
                continue
            tax_rate = v
            break

        numeric_values = [x["value"] for x in values]
        estimated_amount = numeric_values[0] if numeric_values else None
        bid_price = numeric_values[-1] if len(numeric_values) >= 2 else None

        return {
            "estimated_amount": estimated_amount,
            "tax_rate": tax_rate,
            "float_rate": float_rate,
            "bid_price": bid_price,
        }

    def _extract_float_rate_rows_from_flat_text(self, bid_opening_text: str, rules: Dict[str, Dict]) -> List[Dict]:
        if not bid_opening_text or not bid_opening_text.strip():
            return []

        rows = []
        seen = set()

        lines = [line.strip() for line in bid_opening_text.splitlines() if line.strip()]
        table_like_lines = [
            line for line in lines
            if "业务名称" in line and "下浮率" in line and ("投标报价" in line or "报价" in line)
        ]

        search_texts = table_like_lines[:] if table_like_lines else lines[:]

        candidate_names = []
        for key in rules.keys():
            if key != "__generic__":
                candidate_names.append(key)
        candidate_names.extend([
            "代维区域维护",
            "代维区域抢维修",
            "代维区域资产盘点",
            "维护",
            "抢维修",
            "资产盘点",
            "盘点",
        ])

        normalized_candidate_names = []
        used = set()
        for name in candidate_names:
            norm = self._normalize_biz_name(name)
            if norm and norm not in used:
                used.add(norm)
                normalized_candidate_names.append(norm)

        for text in search_texts:
            working_text = text
            if "业务名称" in working_text and "备注" in working_text:
                working_text = working_text.split("备注", 1)[1].strip()
            elif "业务名称" in working_text and "投标报价" in working_text:
                pos = working_text.find("投标报价")
                tail = working_text[pos:]
                m = re.search(r"投标报价[^\u4e00-\u9fa5A-Za-z0-9]*", tail)
                if m:
                    cut = pos + m.end()
                    working_text = working_text[cut:].strip()

            matches = []
            for name in normalized_candidate_names:
                for m in re.finditer(re.escape(name), working_text):
                    matches.append((m.start(), m.end(), name))

            if not matches:
                continue

            matches.sort(key=lambda x: (x[0], -(x[1] - x[0])))
            merged_points = []
            last_end = -1
            for start, end_pos, name in matches:
                if start < last_end:
                    continue
                merged_points.append((start, end_pos, name))
                last_end = end_pos

            for idx, (start, end_pos, name) in enumerate(merged_points):
                end = merged_points[idx + 1][0] if idx + 1 < len(merged_points) else len(working_text)
                segment = working_text[start:end].strip()
                if not segment:
                    continue
                if "合计" in segment:
                    segment = segment.split("合计", 1)[0].strip()
                if not segment:
                    continue

                parsed = self._parse_flat_row_numbers(segment)
                if not parsed:
                    continue

                biz_name_raw = name
                biz_name = self._normalize_biz_name(biz_name_raw)
                if not biz_name or biz_name in {"合计", "总计"}:
                    continue

                key = (biz_name, round(parsed["float_rate"], 4))
                if key in seen:
                    continue
                seen.add(key)

                rows.append({
                    "biz_name": biz_name,
                    "biz_name_raw": biz_name_raw,
                    "estimated_amount": parsed["estimated_amount"],
                    "tax_rate": parsed["tax_rate"],
                    "float_rate": parsed["float_rate"],
                    "bid_price": parsed["bid_price"],
                    "raw_line": segment,
                })

        dedup = {}
        for row in rows:
            key = (row["biz_name"], round(row["float_rate"], 4))
            if key not in dedup:
                dedup[key] = row

        return list(dedup.values())

    def _extract_float_rate_rows(self, parsed: Dict, bid_page: Optional[int], bid_opening_text: str,
                                 rules: Dict[str, Dict]) -> List[Dict]:
        # 1) 优先用结构化 logical_tables
        rows = self._extract_float_rate_rows_from_logical_tables(parsed, bid_page)
        if rows:
            return rows

        # 2) 再从扁平表格文本兜底
        rows = self._extract_float_rate_rows_from_flat_text(bid_opening_text, rules)
        return rows

    def _check_float_rate_rows_compliance(self, rows: List[Dict], rules: Dict[str, Dict]) -> Tuple[bool, List[str]]:
        if not rows:
            return False, ["未找到下浮率业务行"]

        summary = []
        all_passed = True

        for row in rows:
            biz_name = row.get("biz_name_raw") or row.get("biz_name")
            float_rate = row["float_rate"]
            matched_rule = self._match_rule_for_row(row["biz_name"], rules)

            if matched_rule:
                op = matched_rule["op"]
                threshold = matched_rule["threshold"]
                passed = self._compare_by_rule(float_rate, op, threshold)
                if not passed:
                    all_passed = False

                summary.append(
                    f"{biz_name}：下浮率 {float_rate:.2f}% ，规则 {op} {threshold:g}% ，{'合格' if passed else '不合格'}"
                )
            else:
                passed = float_rate > self.min_float_rate
                if not passed:
                    all_passed = False

                summary.append(
                    f"{biz_name}：下浮率 {float_rate:.2f}% ，未匹配到专属规则，按兜底规则 > {self.min_float_rate:g}% ，{'合格' if passed else '不合格'}"
                )

        return all_passed, summary

    def _extract_single_float_rate_from_logical_tables(self, parsed: Dict, bid_page: Optional[int]) -> Optional[float]:
        rows = self._extract_float_rate_rows_from_logical_tables(parsed, bid_page)
        if len(rows) == 1:
            return rows[0]["float_rate"]
        return None

    def _extract_single_float_rate_from_table(self, parsed: Dict, bid_page: Optional[int], bid_opening_text: str) -> \
            Optional[float]:
        """
        优先从 logical_tables 中提取单一下浮率；
        如果没有，再从表格/正文中兜底，但要避免误取规则门槛值和编号行。
        """
        val = self._extract_single_float_rate_from_logical_tables(parsed, bid_page)
        if val is not None:
            return val

        candidates = []

        # table_sections 扁平文本兜底
        for sec in parsed.get("table_sections", []):
            page = sec.get("page")
            if bid_page is not None and page is not None and page != bid_page:
                continue

            table_text = sec.get("text", "")
            if "下浮率" not in self._normalize(table_text):
                continue

            # 从“下浮率”字段附近取百分比
            matches = re.findall(r"下浮率[^0-9]{0,8}(\d+(?:\.\d+)?)\s*%", table_text)
            for m in matches:
                v = self._safe_float(m)
                if v is not None and 0 <= v <= 100:
                    candidates.append(v)

            if not candidates:
                percents = re.findall(r"(\d+(?:\.\d+)?)\s*%", table_text)
                for p in percents:
                    v = self._safe_float(p)
                    if v is not None and 0 <= v <= 100 and v not in self.COMMON_TAX_RATES:
                        candidates.append(v)

        if not candidates:
            val = self._extract_single_float_rate_fallback(bid_opening_text)
            if val is not None:
                candidates.append(val)

        if not candidates:
            return None

        non_tax_candidates = [v for v in candidates if v not in self.COMMON_TAX_RATES]
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
            r"(?:实际)?(?:报价)?下浮率[：:\s]*([\d]+(?:\.\d+)?)\s*%",
            r"(?:实际)?(?:报价)?下浮[：:\s]*([\d]+(?:\.\d+)?)\s*%",
            r"(?:优惠率)[：:\s]*([\d]+(?:\.\d+)?)\s*%",
            r"(?:折扣率)[：:\s]*([\d]+(?:\.\d+)?)\s*%",
        ]

        for raw_line, line in zip(lines, normalized_lines):
            # 跳过规则行
            if any(k in line for k in
                   ["大于", "高于", "不低于", "不少于", "低于", "小于", "不高于", "不大于", "等于", "否决", "否则"]):
                continue

            # 跳过纯编号说明行，例如：1. / 2. / 3.
            if re.match(r"^\d+[\.、:：]", raw_line):
                continue

            for pattern in inline_patterns:
                match = re.search(pattern, line, re.IGNORECASE)
                if match:
                    val = self._safe_float(match.group(1))
                    if val is not None and 0 <= val <= 100 and val not in self.COMMON_TAX_RATES:
                        return val

        return None

    # =========================================================
    # 6. 核心校验
    # =========================================================
    def check_price_compliance(self, source: Any) -> Dict:
        parsed = self._parse_input(source)
        bid_page, bid_opening_text = self._locate_bid_opening_page_and_text(parsed)

        if not bid_opening_text:
            return self._build_fail_result("未找到开标/报价/投标一览表正文")

        # 1) 直接报价 —— 保持你的原逻辑
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

        # 2) 下浮率报价
        rules = self._extract_float_rate_rules(bid_opening_text)
        rows = self._extract_float_rate_rows(parsed, bid_page, bid_opening_text, rules)

        if rows:
            passed, summary = self._check_float_rate_rows_compliance(rows, rules)
            return self._build_result(
                result_text="合格" if passed else "失败",
                price_type="下浮率报价",
                summary=summary
            )

        # 3) 单一下浮率兜底
        single_float_rate = self._extract_single_float_rate_from_table(parsed, bid_page, bid_opening_text)

        if single_float_rate is not None:
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
                    f"下浮率：{single_float_rate:.2f}% > {self.min_float_rate:g}% ，{'合格' if passed else '不合格'}"
                ]

            return self._build_result(
                result_text="合格" if passed else "失败",
                price_type="下浮率报价",
                summary=summary
            )

        return self._build_fail_result("一览表中未找到直接报价或下浮率报价信息")

    def check_price_reasonableness(self, source: Any) -> Dict:
        return self.check_price_compliance(source)
