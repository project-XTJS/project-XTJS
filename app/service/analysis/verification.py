"""
签字盖章与日期合规性检查模块
负责人：镇昊天、张化飞
"""
import re
from datetime import date
from difflib import SequenceMatcher


class VerificationChecker:
    """签字/盖章/日期合规校验器。"""
    # 支持识别的日期格式正则（命名分组统一解析 year/month/day）
    DATE_PATTERNS = [
        r"(?P<year>20\d{2})[年\-/\.](?P<month>0?[1-9]|1[0-2])[月\-/\.](?P<day>0?[1-9]|[12]\d|3[01])日?",
        r"(?P<year>20\d{2})(?P<month>0[1-9]|1[0-2])(?P<day>0[1-9]|[12]\d|3[01])",
    ]
    # 与“投标截止时间”相关的语义锚点
    DEADLINE_ANCHORS = [
        "投标截止时间",
        "投标截止日期",
        "递交截止时间",
        "递交截止日期",
        "投标文件递交截止时间",
        "开标时间",
    ]
    # 与“签署/落款日期”相关的语义锚点
    SIGN_DATE_ANCHORS = [
        "签署日期",
        "签订日期",
        "填写日期",
        "落款日期",
    ]
    # 必须命中的关键位规则（位置标识 -> 可接受锚词）
    POSITION_RULES = [
        ("bidder_name", ["投标人名称", "投标人", "供应商名称", "供应商"]),
        ("representative_signature", ["法定代表人", "授权代表", "委托代理人"]),
    ]
    # 用于判断签字行为是否出现的关键词
    SIGN_MARKERS = ["签字", "签章", "盖章", "签名", "手签"]
    # 用于提取公司名的常见字段锚点
    COMPANY_ANCHORS = [
        "投标人名称",
        "供应商名称",
        "供应商",
        "单位名称",
        "公司名称",
        "企业名称",
    ]

    def __init__(self, ocr_service):
        """构造函数。"""
        # OCR 服务实例（当前模块主要消费其上游元数据）
        self.ocr_service = ocr_service

    def _normalize_seal_texts(self, seal_texts) -> list:
        """将印章文本统一转为去空白后的字符串列表。"""
        if not seal_texts:
            return []
        if isinstance(seal_texts, str):
            text = seal_texts.strip()
            return [text] if text else []
        if isinstance(seal_texts, (list, tuple, set)):
            normalized = []
            for item in seal_texts:
                if item is None:
                    continue
                value = str(item).strip()
                if value:
                    normalized.append(value)
            return normalized
        value = str(seal_texts).strip()
        return [value] if value else []

    def _extract_text_for_date(self, extraction_result: dict) -> str:
        """从 extraction_result 中提取用于日期识别的文本主内容。"""
        # 优先使用聚合文本字段
        content = extraction_result.get("content") or extraction_result.get("text") or ""
        if content:
            return str(content)
        # 回退到分页文本拼接
        pages = extraction_result.get("pages", [])
        if isinstance(pages, list):
            parts = []
            for page in pages:
                if isinstance(page, dict):
                    text = page.get("text")
                    if text:
                        parts.append(str(text))
                elif isinstance(page, str):
                    parts.append(page)
            return "\n".join(parts)
        return ""

    def _parse_date(self, year: str, month: str, day: str):
        """把 year/month/day 安全转换为 date 对象，失败返回 None。"""
        try:
            return date(int(year), int(month), int(day))
        except (TypeError, ValueError):
            return None

    def _extract_date_candidates(self, text: str) -> list:
        """抽取文本中的所有候选日期（含原文位置）。"""
        # 候选结构：{"date": date, "text": 原始匹配串, "start": 起始索引, "end": 结束索引}
        candidates = []
        if not text:
            return candidates
        for pattern in self.DATE_PATTERNS:
            for match in re.finditer(pattern, text):
                parsed = self._parse_date(match.group("year"), match.group("month"), match.group("day"))
                if not parsed:
                    continue
                if parsed.year < 2000 or parsed.year > 2100:
                    continue
                candidates.append(
                    {
                        "date": parsed,
                        "text": match.group(0),
                        "start": match.start(),
                        "end": match.end(),
                    }
                )
        return candidates

    def _extract_anchored_date(self, text: str, anchors: list, window: int = 120):
        """在锚点附近窗口内抽取日期，并返回最晚日期。"""
        # 适用场景：签署日期、投标截止日期等“语义附近日期”识别
        if not text:
            return None
        candidates = self._extract_date_candidates(text)
        if not candidates:
            return None
        anchored = []
        for anchor in anchors:
            for match in re.finditer(re.escape(anchor), text):
                left = max(0, match.start() - window)
                right = min(len(text), match.end() + window)
                for item in candidates:
                    if left <= item["start"] <= right:
                        anchored.append(item)
            compact_text = re.sub(r"\s+", "", text)
            compact_anchor = re.sub(r"\s+", "", anchor)
            if compact_anchor and compact_anchor in compact_text:
                for item in candidates:
                    anchored.append(item)
        if anchored:
            return max(anchored, key=lambda item: item["date"])
        return None

    def _extract_deadline_from_lines(self, text: str):
        """按行扫描“截止类语义”，提取投标截止日期（取最早）。"""
        # 截止日期通常是约束上限，取最早更保守
        lines = [line for line in (text or "").splitlines() if line.strip()]
        if not lines:
            return None
        hit_dates = []
        for idx, line in enumerate(lines):
            compact = re.sub(r"\s+", "", line)
            if "截止" not in compact:
                continue
            if not any(keyword in compact for keyword in ("投标", "递交", "开标")):
                continue
            scope = "\n".join(lines[max(0, idx - 1): min(len(lines), idx + 2)])
            candidates = self._extract_date_candidates(scope)
            if candidates:
                hit_dates.extend(candidates)
        if hit_dates:
            return min(hit_dates, key=lambda item: item["date"])
        return None

    def _extract_sign_date_from_lines(self, text: str):
        """按行扫描“签署类语义”，提取签署日期（取最晚）。"""
        # 签署日期通常以落款日期为准，取最晚更贴近实际签署时间
        lines = [line for line in (text or "").splitlines() if line.strip()]
        if not lines:
            return None
        hit_dates = []
        for idx, line in enumerate(lines):
            compact = re.sub(r"\s+", "", line)
            if "截止" in compact or "开标" in compact:
                continue
            if "日期" not in compact:
                continue
            if not any(keyword in compact for keyword in ("签署", "签订", "填写", "落款", "代表人")):
                continue
            candidates = self._extract_date_candidates(line)
            if candidates:
                hit_dates.extend(candidates)
        if hit_dates:
            return max(hit_dates, key=lambda item: item["date"])
        return None

    def _extract_sign_date_item(self, text: str, date_check: dict):
        """多策略提取签署日期：行级 -> 锚点级 -> 全文兜底。"""
        line_based = self._extract_sign_date_from_lines(text)
        if line_based:
            return line_based
        anchored = self._extract_anchored_date(text, self.SIGN_DATE_ANCHORS, window=120)
        if anchored:
            return anchored
        if date_check.get("extracted_date"):
            parsed = date.fromisoformat(date_check["extracted_date"])
            return {
                "date": parsed,
                "text": date_check.get("matched_text") or parsed.isoformat(),
                "start": 0,
                "end": 0,
            }
        return None

    def _extract_deadline_date(self, extraction_result: dict, text: str):
        """提取投标截止日期：显式字段 -> 全文锚点 -> 行级语义。"""
        # 优先读上游明确传入的截止日期字段，避免误判
        for key in ("deadline_date", "tender_deadline", "bid_deadline"):
            raw = extraction_result.get(key)
            if isinstance(raw, str):
                item = self._extract_anchored_date(raw, self.DEADLINE_ANCHORS, window=30)
                if item:
                    return item
                all_dates = self._extract_date_candidates(raw)
                if all_dates:
                    return max(all_dates, key=lambda entry: entry["date"])
            elif isinstance(raw, date):
                return {"date": raw, "text": raw.isoformat(), "start": 0, "end": 0}
        anchored = self._extract_anchored_date(text, self.DEADLINE_ANCHORS, window=160)
        if anchored:
            return anchored
        line_based = self._extract_deadline_from_lines(text)
        if line_based:
            return line_based
        return None

    def _evaluate_deadline_compliance(self, sign_date_item, deadline_item) -> dict:
        """校验签署日期是否不晚于截止日期。"""
        if not sign_date_item:
            return {
                "status": "missing_sign_date",
                "sign_date": None,
                "deadline_date": deadline_item["date"].isoformat() if deadline_item else None,
                "matched_sign_text": None,
                "matched_deadline_text": deadline_item["text"] if deadline_item else None,
            }
        if not deadline_item:
            return {
                "status": "missing_deadline",
                "sign_date": sign_date_item["date"].isoformat(),
                "deadline_date": None,
                "matched_sign_text": sign_date_item["text"],
                "matched_deadline_text": None,
            }
        is_before_deadline = sign_date_item["date"] <= deadline_item["date"]
        return {
            "status": "pass" if is_before_deadline else "fail",
            "sign_date": sign_date_item["date"].isoformat(),
            "deadline_date": deadline_item["date"].isoformat(),
            "matched_sign_text": sign_date_item["text"],
            "matched_deadline_text": deadline_item["text"],
            "is_before_deadline": is_before_deadline,
            "days_gap": (deadline_item["date"] - sign_date_item["date"]).days,
        }

    def _evaluate_required_positions(self, text: str, seal_detected: bool) -> dict:
        """校验关键签字位是否齐全，并结合签章存在性给出结果。"""
        # normalized_text：用于统一做关键字/锚点匹配
        normalized_text = text or ""
        # found_positions：命中的关键位；missing_positions：缺失关键位
        found_positions = []
        missing_positions = []
        # representative_context_ok：代表人锚点附近是否出现签字关键词
        representative_context_ok = False
        # marker_matches：全文命中的签字关键词列表
        marker_matches = []

        for position_name, anchors in self.POSITION_RULES:
            matched_anchor = None
            for anchor in anchors:
                anchor_match = re.search(re.escape(anchor), normalized_text)
                if anchor_match:
                    matched_anchor = anchor
                    if position_name == "representative_signature":
                        left = max(0, anchor_match.start() - 80)
                        right = min(len(normalized_text), anchor_match.end() + 120)
                        context = normalized_text[left:right]
                        representative_context_ok = any(marker in context for marker in self.SIGN_MARKERS)
                    break
            if matched_anchor:
                found_positions.append({"position": position_name, "matched_anchor": matched_anchor})
            else:
                missing_positions.append(position_name)

        for marker in self.SIGN_MARKERS:
            if marker in normalized_text:
                marker_matches.append(marker)

        # 只要“代表人局部命中签字词”或“全文出现签字词”之一成立，即认为有签字行为
        signature_present = representative_context_ok or bool(marker_matches)
        status = "pass"
        if missing_positions or not signature_present or not seal_detected:
            status = "fail"

        return {
            "status": status,
            "required_positions": [item[0] for item in self.POSITION_RULES],
            "found_positions": found_positions,
            "missing_positions": missing_positions,
            "signature_markers_found": marker_matches,
            "signature_present": signature_present,
            "seal_present": seal_detected,
        }

    def _evaluate_date(self, text: str) -> dict:
        """提取全文日期并按“距今天数”评估时效性。"""
        if not text or not text.strip():
            return {
                "status": "missing",
                "extracted_date": None,
                "matched_text": None,
                "days_from_today": None,
            }

        candidates = self._extract_date_candidates(text)

        if not candidates:
            return {
                "status": "missing",
                "extracted_date": None,
                "matched_text": None,
                "days_from_today": None,
            }

        latest = max(candidates, key=lambda item: item["date"])
        latest_date, matched_text = latest["date"], latest["text"]
        delta_days = (date.today() - latest_date).days

        if delta_days < 0:
            status = "future"
        elif delta_days <= 365:
            status = "valid"
        else:
            status = "expired"

        return {
            "status": status,
            "extracted_date": latest_date.isoformat(),
            "matched_text": matched_text,
            "days_from_today": delta_days,
        }

    def _normalize_company_text(self, text: str) -> str:
        """公司名归一化：去噪、去标点、去括号注释。"""
        if not text:
            return ""
        normalized = str(text)
        normalized = re.sub(r"[（\(].*?[）\)]", "", normalized)
        normalized = re.sub(r"[^0-9A-Za-z\u4e00-\u9fa5]", "", normalized)
        for token in ("投标人名称", "供应商名称", "供应商", "公司名称", "企业名称"):
            normalized = normalized.replace(token, "")
        return normalized

    def _extract_company_candidates(self, extraction_result: dict, text: str) -> list:
        """提取公司名候选集（显式字段+锚点右侧+公司后缀正则）。"""
        # candidates：原始候选列表（后续会做去重与截断）
        candidates = []
        # 优先使用结构化字段
        explicit_keys = ("company_name", "supplier_name", "bidder_name")
        for key in explicit_keys:
            value = extraction_result.get(key)
            if value:
                normalized = self._normalize_company_text(value)
                if len(normalized) >= 4:
                    candidates.append(normalized)

        lines = [line.strip() for line in (text or "").splitlines() if line.strip()]
        for line in lines:
            compact = re.sub(r"\s+", "", line)
            for anchor in self.COMPANY_ANCHORS:
                if anchor in compact:
                    right_text = compact.split(anchor, 1)[-1]
                    right_text = re.sub(r"^[：:、\-\s]+", "", right_text)
                    right_text = re.split(r"[，,。；; ]", right_text)[0]
                    normalized = self._normalize_company_text(right_text)
                    if len(normalized) >= 4:
                        candidates.append(normalized)

            match = re.search(
                r"([A-Za-z0-9\u4e00-\u9fa5]{4,40}(?:有限责任公司|股份有限公司|集团有限公司|有限公司|公司))",
                compact,
            )
            if match:
                normalized = self._normalize_company_text(match.group(1))
                if len(normalized) >= 4:
                    candidates.append(normalized)

        deduped = []
        seen = set()
        for item in candidates:
            if item not in seen:
                deduped.append(item)
                seen.add(item)
        # 只保留前 8 个候选，避免噪声无限扩散
        return deduped[:8]

    def _score_seal_company_match(self, company_name: str, seal_text: str) -> float:
        """计算公司名与印章文本相似度分数（0~1）。"""
        company_norm = self._normalize_company_text(company_name)
        seal_norm = self._normalize_company_text(seal_text)
        if not company_norm or not seal_norm:
            return 0.0

        # contain_bonus：存在包含关系时给予额外加分
        contain_bonus = 0.0
        min_len = min(len(company_norm), len(seal_norm))
        if min_len >= 4 and (company_norm in seal_norm or seal_norm in company_norm):
            contain_bonus = 0.35

        # ratio：序列相似度；overlap：字符集合重叠率
        ratio = SequenceMatcher(None, company_norm, seal_norm).ratio()
        common_chars = set(company_norm) & set(seal_norm)
        overlap = len(common_chars) / max(len(set(company_norm)), 1)
        score = max(ratio, overlap) + contain_bonus
        return min(score, 1.0)

    def _evaluate_seal_company_match(self, extraction_result: dict, text: str, seal_texts: list, seal_detected: bool) -> dict:
        """评估“印章文本是否与公司名匹配”。"""
        if not seal_detected:
            return {
                "status": "fail",
                "matched": False,
                "reason": "seal_not_detected",
                "company_candidates": [],
                "best_match": None,
            }

        # company_candidates：从文本中提取出的公司名称候选
        company_candidates = self._extract_company_candidates(extraction_result, text)
        if not company_candidates:
            return {
                "status": "pending",
                "matched": False,
                "reason": "company_name_not_found",
                "company_candidates": [],
                "best_match": None,
            }

        if not seal_texts:
            return {
                "status": "pending",
                "matched": False,
                "reason": "seal_text_not_found",
                "company_candidates": company_candidates,
                "best_match": None,
            }

        # best：当前最佳匹配对（company_name, seal_text, score）
        best = None
        for company in company_candidates:
            for seal_text in seal_texts:
                score = self._score_seal_company_match(company, seal_text)
                if (best is None) or (score > best["score"]):
                    best = {
                        "company_name": company,
                        "seal_text": seal_text,
                        "score": round(score, 4),
                    }

        # threshold：公司名匹配通过阈值
        threshold = 0.55
        matched = bool(best and best["score"] >= threshold)
        return {
            "status": "pass" if matched else "fail",
            "matched": matched,
            "reason": "matched" if matched else "low_similarity",
            "threshold": threshold,
            "company_candidates": company_candidates,
            "best_match": best,
        }

    def check_seal_and_date(self, extraction_result: dict) -> dict:
        """
        统一校验入口：
        1) 印章检测与归一化；
        2) 关键签字位校验；
        3) 日期提取与截止时效校验；
        4) 印章内容与公司名匹配；
        5) 生成最终合规状态。
        """
        # extraction_result：上游抽取结果（文本、分页、印章、截止日期等）
        extraction_result = extraction_result or {}
        # seal_texts：归一化后的印章文本列表
        seal_texts = self._normalize_seal_texts(extraction_result.get("seal_texts", []))
        # seal_count：印章数量，至少不小于 seal_texts 实际条数
        seal_count = extraction_result.get("seal_count", len(seal_texts))
        try:
            seal_count = int(seal_count)
        except (TypeError, ValueError):
            seal_count = len(seal_texts)
        seal_count = max(seal_count, len(seal_texts))
        # seal_detected：最终印章存在判定
        detected_flag = extraction_result.get("seal_detected")
        seal_detected = bool(detected_flag) if detected_flag is not None else (seal_count > 0)
        if seal_texts and not seal_detected:
            seal_detected = True
        # text_for_check：用于位置/日期/公司名提取的主文本
        text_for_check = self._extract_text_for_date(extraction_result)
        # date_check：全文日期时效评估
        date_check = self._evaluate_date(text_for_check)
        # sign_date_item / deadline_item：签署日期与截止日期
        sign_date_item = self._extract_sign_date_item(text_for_check, date_check)
        deadline_item = self._extract_deadline_date(extraction_result, text_for_check)
        # deadline_check：签署日期是否不晚于截止日期
        deadline_check = self._evaluate_deadline_compliance(sign_date_item, deadline_item)
        # position_check：关键位完整性校验
        position_check = self._evaluate_required_positions(text_for_check, seal_detected)
        # seal_company_check：印章文本与公司名匹配校验
        seal_company_check = self._evaluate_seal_company_match(
            extraction_result=extraction_result,
            text=text_for_check,
            seal_texts=seal_texts,
            seal_detected=seal_detected,
        )
        # compliance_status：最终合规结论（pass/fail/pending）
        compliance_status = "pass"
        if (
            position_check["status"] != "pass"
            or deadline_check["status"] == "fail"
            or seal_company_check["status"] == "fail"
        ):
            compliance_status = "fail"
        if deadline_check["status"] in ("missing_deadline", "missing_sign_date") or seal_company_check["status"] == "pending":
            compliance_status = "pending"

        return {
            "seal_detected": seal_detected,
            "seal_count": seal_count,
            "seal_contents": seal_texts,
            "date_check": date_check,
            "position_check": position_check,
            "deadline_check": deadline_check,
            "seal_company_check": seal_company_check,
            "compliance_status": compliance_status,
        }
