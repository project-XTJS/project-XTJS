from __future__ import annotations

import json
import re
from datetime import date
from difflib import SequenceMatcher
from typing import Any

from .template_extractor import TemplateExtractor


class VerificationChecker:
    DATE_PATTERNS = (
        r"(?P<year>20\d{2})[年/\-.](?P<month>1[0-2]|0?[1-9])[月/\-.](?P<day>3[01]|[12]\d|0?[1-9])(?:日)?",
        r"(?P<year>20\d{2})(?P<month>0[1-9]|1[0-2])(?P<day>0[1-9]|[12]\d|3[01])",
    )
    DEADLINE_ANCHORS = (
        "投标截止时间", "投标截止日期", "递交截止时间", "递交截止日期",
        "投标文件递交截止时间", "投标文件递交截止日期", "提交截止时间", "提交截止日期",
        "开标时间", "开标日期",
    )
    SIGNATURE_MARKERS = ("签字或盖章", "签字", "签章", "签名", "手签")
    SIGNATURE_ANCHORS = ("法定代表人", "授权代表", "授权委托人", "委托代理人", "被授权人", "代表人")
    SIGNATURE_PLACEHOLDER_TEXTS = ("已签字", "已盖章", "已签章")
    SEAL_MARKERS = ("盖章", "公章", "加盖公章")
    DATE_FIELD_ANCHORS = ("日期", "填写日期", "签署日期", "落款日期", "签订日期")
    COMPANY_ANCHORS = ("投标人名称", "投标人", "供应商名称", "供应商", "单位名称", "公司名称", "企业名称", "声明人")
    OPTIONAL_MARKERS = ("如有", "可选", "如适用", "如需")
    EXCLUDE_ATTACHMENTS = ("拟派项目负责人情况表", "项目人员配置表", "人员配置表")
    ATTACHMENT_RE = re.compile(r"^\s*(?:[（(]?\d+(?:\s*[-－]\s*\d+)?[)）\.、]?\s*)?附件\s*(?P<number>\d+(?:\s*[-－]\s*\d+)*)")
    COMMON_ATTACHMENT_TITLES = (
        "投标保证书",
        "开标一览表",
        "分项报价表",
        "商务条款偏离表",
        "技术条款偏离表",
        "投标人基本情况表",
        "类似项目业绩清单",
        "法定代表人资格证明书",
        "法定代表人授权委托书",
        "投标人承诺声明函",
        "不参与围标串标承诺书",
        "保证金缴纳凭证",
        "财务状况及税收、社会保障资金缴纳情况声明函",
        "制造商声明函",
        "制造商授权书",
        "投标人认为需加以说明的其他内容",
    )
    COMPANY_RE = re.compile(r"([A-Za-z0-9\u4e00-\u9fa5]{4,60}(?:有限责任公司|股份有限公司|集团有限公司|有限公司|公司))")

    def __init__(self, ocr_service: Any):
        self.ocr_service = ocr_service

    def check_seal_and_date(self, extraction_result: Any, bid_document: Any | None = None) -> dict:
        tender_document, actual_bid_document = self._extract_document_pair(extraction_result, bid_document)
        if tender_document is not None and actual_bid_document is not None:
            return self._check_pair(tender_document, actual_bid_document)
        return self._check_single(extraction_result)

    def _check_pair(self, tender_document: Any, bid_document: Any) -> dict:
        tender = self._as_document(tender_document) or {}
        bid = self._as_document(bid_document) or {}
        seal_bundle = self._seal_bundle(bid)
        signature_bundle = self._signature_bundle(bid)
        bidder_name = self._bidder_name(bid, seal_bundle["texts"])
        deadline = self._deadline_from_doc(tender)
        template_required = self._required_attachments(tender)
        expected_attachments = self._attachment_title_hints(template_required)
        bid_sections = self._attachment_sections(bid, seal_bundle["locations"], signature_bundle["locations"], expected_attachments)
        bid_by_no = {x["attachment_number"]: x for x in bid_sections if x.get("attachment_number")}
        required = self._required_attachments(tender, bid_by_no, bid_sections)

        results, skipped_missing_attachments, missing_signatures, pending_signatures, missing_seals, missing_dates, late_dates = [], [], [], [], [], [], []
        for item in required:
            section = self._match_attachment(item, bid_by_no, bid_sections)
            result = self._evaluate_attachment(item, section, deadline, bidder_name)
            if not result["found"]:
                skipped_missing_attachments.append(result["title"])
                continue
            results.append(result)
            if result["signature_check"]["status"] == "fail":
                missing_signatures.append(result["title"])
            if result["signature_check"]["status"] == "pending":
                pending_signatures.append(result["title"])
            if result["seal_check"]["status"] == "fail":
                missing_seals.append(result["title"])
            if result["date_check"]["status"] == "missing_date":
                missing_dates.append(result["title"])
            if result["date_check"]["status"] == "late":
                late_dates.append(result["title"])

        checked_count = len(results)
        if checked_count <= 0 or deadline is None:
            compliance_status = "pending"
        elif any(x["status"] == "fail" for x in results):
            compliance_status = "fail"
        elif any(x["status"] == "pending" for x in results):
            compliance_status = "pending"
        else:
            compliance_status = "pass"

        date_status = "missing_deadline" if deadline is None else ("fail" if missing_dates or late_dates else "pass")
        position_status = "fail" if missing_signatures or missing_seals else ("pending" if pending_signatures else "pass")
        return {
            "mode": "tender_vs_bid",
            "summary": self._pair_summary(checked_count, deadline, compliance_status, [], missing_signatures, pending_signatures, missing_seals, missing_dates, late_dates),
            "seal_detected": seal_bundle["detected"],
            "seal_count": seal_bundle["count"],
            "seal_contents": seal_bundle["texts"],
            "signature_detected": signature_bundle["detected"],
            "signature_count": signature_bundle["count"],
            "signature_contents": signature_bundle["texts"],
            "bidder_name": bidder_name,
            "required_attachment_count": len(required),
            "checked_attachment_count": checked_count,
            "required_attachments": [x["title"] for x in required],
            "skipped_missing_attachments": skipped_missing_attachments,
            "attachment_results": results,
            "position_check": {"status": position_status, "missing_attachments": [], "missing_signature_attachments": missing_signatures, "pending_signature_attachments": pending_signatures, "missing_seal_attachments": missing_seals},
            "date_check": {"status": date_status, "deadline_date": deadline["date"].isoformat() if deadline else None, "matched_deadline_text": deadline["text"] if deadline else None, "missing_date_attachments": missing_dates, "late_date_attachments": late_dates},
            "deadline_check": {"status": date_status, "deadline_date": deadline["date"].isoformat() if deadline else None, "matched_deadline_text": deadline["text"] if deadline else None, "source": "tender_document"},
            "seal_company_check": self._seal_company_check(bidder_name, seal_bundle["texts"]),
            "compliance_status": compliance_status,
        }

    def _check_single(self, payload: Any) -> dict:
        document = self._as_document(payload) or {}
        text = self._text(document)
        seal_bundle = self._seal_bundle(document)
        signature_bundle = self._signature_bundle(document)
        signatures = self._signature_values(text)
        sign_date = self._section_date(text, self._sections(document))
        has_signature = bool(signatures or signature_bundle["detected"])
        signature_status = "pass" if has_signature else ("pending" if seal_bundle["detected"] and sign_date else "fail")
        return {
            "mode": "single_document",
            "summary": "仅基于单文档全文做兜底扫描，未执行招投标附件级联校验。",
            "seal_detected": seal_bundle["detected"],
            "seal_count": seal_bundle["count"],
            "seal_contents": seal_bundle["texts"],
            "signature_detected": has_signature,
            "signature_count": max(len(signatures), signature_bundle["count"]),
            "signature_contents": signature_bundle["texts"] or list(dict.fromkeys([x.get("value") for x in signatures if isinstance(x, dict) and x.get("value")])),
            "bidder_name": self._bidder_name(document, seal_bundle["texts"]),
            "required_attachment_count": 0,
            "required_attachments": [],
            "attachment_results": [],
            "position_check": {"status": signature_status if seal_bundle["detected"] or signature_status != "fail" else "fail", "missing_attachments": [], "missing_signature_attachments": [] if has_signature or signature_status == "pending" else ["全文未识别到有效签字内容或签字区域"], "pending_signature_attachments": ["全文识别到签字位或签字区域，但未回填出稳定签名文本，建议人工复核"] if signature_status == "pending" else [], "missing_seal_attachments": [] if seal_bundle["detected"] else ["全文未识别到有效盖章"]},
            "date_check": {"status": "pass" if sign_date else "missing_date", "deadline_date": None, "matched_deadline_text": None, "missing_date_attachments": [] if sign_date else ["全文未识别到落款日期"], "late_date_attachments": []},
            "deadline_check": {"status": "not_applicable", "deadline_date": None, "matched_deadline_text": None, "source": None},
            "seal_company_check": self._seal_company_check(self._bidder_name(document, seal_bundle["texts"]), seal_bundle["texts"]),
            "compliance_status": "pass" if text and seal_bundle["detected"] and has_signature and sign_date else ("pending" if text and seal_bundle["detected"] and sign_date else "fail"),
        }

    def _pair_summary(self, count: int, deadline: dict | None, status: str, missing_attachments: list[str], missing_signatures: list[str], pending_signatures: list[str], missing_seals: list[str], missing_dates: list[str], late_dates: list[str]) -> str:
        if count <= 0:
            return "未在招标模板中识别到明确要求签字、盖章或填写日期的附件。"
        if deadline is None:
            return "已识别到需核验的附件，但未能稳定提取招标文件中的最晚提交日期。"
        if status == "pass":
            return f"共核验 {count} 个附件，签字、盖章与日期均满足要求，落款日期不晚于 {deadline['date'].isoformat()}。"
        issues = []
        if missing_attachments: issues.append(f"缺少附件 {len(missing_attachments)} 个")
        if missing_signatures: issues.append(f"缺少有效签字 {len(missing_signatures)} 个")
        if pending_signatures: issues.append(f"签字待人工复核 {len(pending_signatures)} 个")
        if missing_seals: issues.append(f"缺少有效盖章 {len(missing_seals)} 个")
        if missing_dates: issues.append(f"缺少落款日期 {len(missing_dates)} 个")
        if late_dates: issues.append(f"落款晚于截止日期 {len(late_dates)} 个")
        return f"共核验 {count} 个附件，截止日期按 {deadline['date'].isoformat()} 比对，结果：{'；'.join(issues) if issues else '存在待人工复核项'}。"

    def _extract_document_pair(self, primary: Any, secondary: Any | None) -> tuple[dict | None, dict | None]:
        if secondary is not None:
            return self._as_document(primary), self._as_document(secondary)
        payload = self._as_document(primary)
        if not isinstance(payload, dict):
            return None, None
        for tender_key, bid_key in (("tender_document", "bid_document"), ("tender_document", "business_bid_document"), ("招标文件", "投标文件"), ("tender", "bid"), ("招标", "投标")):
            tender, bid = self._as_document(payload.get(tender_key)), self._as_document(payload.get(bid_key))
            if tender is not None and bid is not None:
                return tender, bid
        return None, None

    def _as_document(self, payload: Any) -> dict | None:
        if isinstance(payload, dict):
            return payload
        if not isinstance(payload, str) or not payload.strip().startswith("{"):
            return None
        try:
            parsed = json.loads(payload)
        except json.JSONDecodeError:
            return None
        return parsed if isinstance(parsed, dict) else None

    def _container(self, payload: dict | None) -> dict:
        return payload.get("data") if isinstance(payload, dict) and isinstance(payload.get("data"), dict) else (payload or {})

    def _sections(self, payload: dict | None) -> list[dict]:
        sections = self._container(payload).get("layout_sections")
        if not isinstance(sections, list):
            return []
        result = []
        for i, item in enumerate(sections):
            if not isinstance(item, dict):
                continue
            text = str(item.get("text") or item.get("raw_text") or "").strip()
            if text:
                section = {"index": i, "page": item.get("page") if isinstance(item.get("page"), int) else None, "type": str(item.get("type") or "text").strip().lower() or "text", "text": text}
                bbox = self._normalize_bbox(item.get("bbox") or item.get("box"))
                if bbox is not None:
                    section["bbox"] = bbox
                result.append(section)
        return result

    def _text(self, payload: dict | None) -> str:
        container = self._container(payload)
        for key in ("content", "text", "full_text"):
            value = container.get(key)
            if isinstance(value, str) and value.strip():
                return value
        return "\n".join(x["text"] for x in self._sections(payload))

    def _lines(self, text: str) -> list[str]:
        return [x.strip() for x in re.split(r"[\r\n]+", str(text or "")) if x and x.strip()]

    def _compact(self, text: str) -> str:
        return re.sub(r"\s+", "", str(text or ""))

    def _normalize_bbox(self, value: Any) -> list[int] | None:
        if value is None:
            return None
        if isinstance(value, (list, tuple)):
            if len(value) >= 4 and all(isinstance(item, (int, float)) for item in value[:4]):
                x, y, w, h = [int(round(float(item))) for item in value[:4]]
                if w >= 0 and h >= 0:
                    return [x, y, w, h]
                return [min(x, w), min(y, h), abs(w - x), abs(h - y)]
            if value and all(isinstance(item, (list, tuple)) and len(item) >= 2 and all(isinstance(part, (int, float)) for part in item[:2]) for item in value):
                xs = [float(item[0]) for item in value]
                ys = [float(item[1]) for item in value]
                left, top = int(round(min(xs))), int(round(min(ys)))
                right, bottom = int(round(max(xs))), int(round(max(ys)))
                return [left, top, max(right - left, 0), max(bottom - top, 0)]
        return None

    def _dedupe_locations(self, items: list[dict]) -> list[dict]:
        result, seen = [], set()
        for item in items:
            if not isinstance(item, dict):
                continue
            page = item.get("page") if isinstance(item.get("page"), int) else None
            box = self._normalize_bbox(item.get("box") or item.get("bbox"))
            key = (page, tuple(box) if box is not None else None)
            if key in seen:
                continue
            seen.add(key)
            normalized = {"page": page}
            if box is not None:
                normalized["box"] = box
            result.append(normalized)
        return result

    def _evidence_key(self, page: int | None, box: Any = None, text: Any = None) -> tuple[Any, tuple[int, ...], str | None]:
        normalized_box = tuple(self._normalize_bbox(box) or [])
        compact_text = self._compact(text) or None
        return (page, normalized_box, compact_text)

    def _seal_bundle(self, payload: dict | None) -> dict:
        container = self._container(payload)
        seal_node = container.get("seal") if isinstance(container.get("seal"), dict) else {}
        seal_texts = []
        seal_locations = []
        for source in (seal_node.get("texts"), container.get("seal_texts"), [s["text"] for s in self._sections(payload) if s["type"] == "seal"]):
            if isinstance(source, str):
                source = [source]
            if isinstance(source, (list, tuple, set)):
                seal_texts.extend(str(x).strip() for x in source if str(x).strip())
        for source in (
            seal_node.get("locations"),
            container.get("seal_locations"),
            [{"page": s.get("page"), "box": s.get("bbox")} for s in self._sections(payload) if s["type"] == "seal" and s.get("bbox")],
        ):
            if isinstance(source, dict):
                source = [source]
            if isinstance(source, (list, tuple, set)):
                for item in source:
                    if isinstance(item, dict):
                        seal_locations.append(item)
        deduped = list(dict.fromkeys(seal_texts))
        raw_count = seal_node.get("count", container.get("seal_count", len(deduped)))
        try:
            seal_count = int(raw_count)
        except (TypeError, ValueError):
            seal_count = len(deduped)
        seal_count = max(seal_count, len(deduped))
        detected = seal_node.get("detected", container.get("seal_detected"))
        detected = bool(detected) if detected is not None else bool(seal_count or deduped)
        return {"detected": detected or bool(deduped), "count": seal_count, "texts": deduped, "locations": self._dedupe_locations(seal_locations)}

    def _signature_bundle(self, payload: dict | None) -> dict:
        container = self._container(payload)
        signature_node = container.get("signature") if isinstance(container.get("signature"), dict) else {}
        signature_texts = []
        signature_locations = []
        for source in (signature_node.get("texts"), container.get("signature_texts"), [s["text"] for s in self._sections(payload) if s["type"] == "signature"]):
            if isinstance(source, str):
                source = [source]
            if isinstance(source, (list, tuple, set)):
                signature_texts.extend(str(x).strip() for x in source if str(x).strip())
        for source in (
            signature_node.get("locations"),
            container.get("signature_locations"),
            [{"page": s.get("page"), "box": s.get("bbox")} for s in self._sections(payload) if s["type"] == "signature" and s.get("bbox")],
        ):
            if isinstance(source, dict):
                source = [source]
            if isinstance(source, (list, tuple, set)):
                for item in source:
                    if isinstance(item, dict):
                        signature_locations.append(item)
        deduped_texts = list(dict.fromkeys(signature_texts))
        deduped_locations = self._dedupe_locations(signature_locations)
        raw_count = signature_node.get("count", container.get("signature_count", max(len(deduped_texts), len(deduped_locations))))
        try:
            signature_count = int(raw_count)
        except (TypeError, ValueError):
            signature_count = max(len(deduped_texts), len(deduped_locations))
        signature_count = max(signature_count, len(deduped_texts), len(deduped_locations))
        detected = signature_node.get("detected", container.get("signature_detected"))
        detected = bool(detected) if detected is not None else bool(signature_count or deduped_texts or deduped_locations)
        return {"detected": detected or bool(deduped_texts or deduped_locations), "count": signature_count, "texts": deduped_texts, "locations": deduped_locations}

    def _date_candidates(self, text: str) -> list[dict]:
        items, seen = [], set()
        raw_text = str(text or "")
        normalized_text = raw_text.replace("\\underline", "").replace("\\text{", "").replace("{", "").replace("}", "").replace("$", "")
        normalized_text = re.sub(r"[_\s]+", "", normalized_text)
        for candidate_text in dict.fromkeys(x for x in (raw_text, normalized_text) if x):
            for pattern in self.DATE_PATTERNS:
                for match in re.finditer(pattern, candidate_text):
                    try:
                        parsed = date(int(match.group("year")), int(match.group("month")), int(match.group("day")))
                    except ValueError:
                        continue
                    key = (parsed, match.group(0))
                    if key in seen:
                        continue
                    seen.add(key)
                    items.append({"date": parsed, "text": match.group(0), "start": match.start()})
        return items

    def _deadline_from_doc(self, payload: dict | None) -> dict | None:
        candidates = []
        sections = self._sections(payload)
        anchors = sorted(((a, self._compact(a)) for a in self.DEADLINE_ANCHORS), key=lambda x: len(x[1]), reverse=True)
        for sec_index, section in enumerate(sections):
            if section["type"] == "seal" or self._catalog_like(section["text"]):
                continue
            lines = self._lines(section["text"]) or [section["text"]]
            for line_index, line in enumerate(lines):
                compact_line = self._compact(line)
                if not compact_line:
                    continue
                for anchor_text, anchor in anchors:
                    start = 0
                    while True:
                        pos = compact_line.find(anchor, start)
                        if pos < 0:
                            break
                        window = compact_line[pos:pos + 72]
                        local_dates = self._date_candidates(window)
                        if local_dates:
                            nearest = min(local_dates, key=lambda x: x["start"])
                            candidates.append({"date": nearest["date"], "text": window[:72], "page": section.get("page"), "section_index": sec_index, "line_index": line_index, "distance": nearest["start"], "anchor": anchor_text})
                        start = pos + len(anchor)
        if not candidates:
            return None
        return min(candidates, key=lambda x: (x["date"], x["page"] if x["page"] is not None else 10**9, x["section_index"], x["line_index"], x["distance"]))

    def _catalog_like(self, text: str) -> bool:
        compact = self._compact(text)
        if not compact:
            return False
        if "目录" in compact:
            return True
        if re.search(r"(?:\.{2,}|…{2,}|。{2,})\d{1,4}$", compact):
            return True
        return len(re.findall(r"(?:\.{2,}|…{2,}|。{2,})\d{1,4}", compact)) >= 2

    def _attachment_number(self, text: str) -> str | None:
        match = self.ATTACHMENT_RE.search(str(text or ""))
        return re.sub(r"\s+", "", match.group("number")) if match else None

    def _attachment_title(self, text: str) -> str:
        text = str(text or "").strip()
        text = re.sub(r"^\s*第[一二三四五六七八九十百零\d]+[章节部分篇项]\s*", "", text)
        text = re.sub(r"^\s*(?:[（(]?\d+(?:\s*[-－]\s*\d+)?[）).、．]?|[一二三四五六七八九十百零]+[、.)）．])\s*", "", text)
        idx = text.find("附件")
        text = text[idx:] if idx >= 0 else text
        return re.sub(r"\s+", " ", text).strip("：:；;，,。")

    def _attachment_title_key(self, text: str) -> str:
        title = self._attachment_title(text)
        title = re.sub(r"^\s*附件\s*\d+(?:\s*[-－]\s*\d+)*[、.)）．]?\s*", "", title)
        title = re.sub(r"[（(][^）)]{0,30}(?:格式|自拟|如有|说明|盖章|签字|样式|模板|原件|复印件)[^）)]*[）)]", "", title)
        title = re.sub(r"\s+", "", title)
        return title.strip("：:；;，,。")

    def _leading_title_key(self, text: str) -> str:
        leading = str(text or "").strip()
        leading = re.sub(r"^\s*(?:[（(]?\d+(?:\s*[-－]\s*\d+)?[)）\.、]?\s*|[一二三四五六七八九十百]+[、.])", "", leading)
        leading = re.sub(r"\s+", "", leading)
        return leading.strip("：:；;，,。")

    def _attachment_title_score(self, left: str, right: str) -> float:
        left_key = self._attachment_title_key(left)
        right_key = self._attachment_title_key(right)
        if not left_key or not right_key:
            return 0.0
        if left_key == right_key:
            return 1.0
        ratio = SequenceMatcher(None, left_key, right_key).ratio()
        bonus = 0.35 if len(left_key) >= 5 and len(right_key) >= 5 and (left_key in right_key or right_key in left_key) else 0.0
        return min(ratio + bonus, 1.0)

    def _attachment_title_hints(self, attachments: list[dict] | None = None) -> list[dict]:
        result, seen = [], set()
        for item in attachments or []:
            title = self._attachment_title(item.get("title"))
            title_key = self._attachment_title_key(title)
            if not title_key or title_key in seen:
                continue
            seen.add(title_key)
            result.append({"attachment_number": item.get("attachment_number"), "title": title, "title_key": title_key})
        for title in self.COMMON_ATTACHMENT_TITLES:
            title_key = self._attachment_title_key(title)
            if not title_key or title_key in seen:
                continue
            seen.add(title_key)
            result.append({"attachment_number": None, "title": title, "title_key": title_key})
        return result

    def _best_expected_attachment(self, text: str, expected_attachments: list[dict] | None = None) -> dict | None:
        title_key = self._attachment_title_key(text)
        if len(title_key) < 5:
            return None
        best = None
        for item in expected_attachments or []:
            score = self._attachment_title_score(title_key, item.get("title_key") or item.get("title") or "")
            if best is None or score > best["score"]:
                best = {"score": score, "item": item}
        return best["item"] if best and best["score"] >= 0.78 else None

    def _is_attachment_heading(self, section: dict, expected_attachments: list[dict] | None = None) -> bool:
        if section.get("type") not in {"heading", "text"}:
            return False
        text = str(section.get("text") or "").strip()
        if not text or self._catalog_like(text):
            return False
        matched_expected = self._best_expected_attachment(text, expected_attachments)
        attachment_number = self._attachment_number(text)
        compact = self._compact(text)
        if attachment_number is not None:
            attachment_index = compact.find("附件")
            if attachment_index < 0 or attachment_index > 12:
                return False
            if len(compact) > 120 and section.get("type") != "heading":
                return False
            return matched_expected is not None if expected_attachments else True
        if expected_attachments is None:
            return False
        if matched_expected is not None and section.get("type") == "text":
            leading_title_key = self._leading_title_key(text)
            expected_title_key = str(matched_expected.get("title_key") or "").strip()
            if expected_title_key and not (
                leading_title_key.startswith("附件") or leading_title_key.startswith(expected_title_key)
            ):
                return False
        if len(compact) > (64 if section.get("type") == "heading" else 36):
            return False
        return matched_expected is not None

    def _attachment_sections(self, payload: dict | None, seal_locations: list[dict] | None = None, signature_locations: list[dict] | None = None, expected_attachments: list[dict] | None = None) -> list[dict]:
        sections = self._sections(payload)
        starts = [i for i, x in enumerate(sections) if self._is_attachment_heading(x, expected_attachments)]
        result = []
        seal_locations = seal_locations or []
        signature_locations = signature_locations or []
        for pos, start in enumerate(starts):
            end = starts[pos + 1] if pos + 1 < len(starts) else len(sections)
            chunk = sections[start:end]
            title = chunk[0]["text"]
            matched_expected = self._best_expected_attachment(title, expected_attachments)
            attachment_number = self._attachment_number(title)
            normalized_title = self._attachment_title(title)
            if matched_expected is not None and attachment_number is None:
                attachment_number = matched_expected.get("attachment_number")
                normalized_title = matched_expected.get("title") or normalized_title
            pages = list(dict.fromkeys(x["page"] for x in chunk if x.get("page") is not None))
            local_seal_locations = [{"page": x.get("page"), "box": x.get("bbox")} for x in chunk if x["type"] == "seal" and x.get("bbox")]
            local_seal_locations.extend(item for item in seal_locations if item.get("page") in pages)
            local_signature_locations = [{"page": x.get("page"), "box": x.get("bbox")} for x in chunk if x["type"] == "signature" and x.get("bbox")]
            local_signature_locations.extend(item for item in signature_locations if item.get("page") in pages)
            result.append({"attachment_number": attachment_number, "title": normalized_title, "pages": pages, "text": "\n".join(x["text"] for x in chunk if x["type"] != "seal"), "seal_texts": [x["text"] for x in chunk if x["type"] == "seal"], "signature_texts": [x["text"] for x in chunk if x["type"] == "signature"], "sections": chunk, "seal_locations": self._dedupe_locations(local_seal_locations), "signature_locations": self._dedupe_locations(local_signature_locations)})
        return result

    def _required_attachments(
        self,
        tender_payload: dict | None,
        bid_by_no: dict[str, dict] | None = None,
        bid_sections: list[dict] | None = None,
    ) -> list[dict]:
        result, seen = [], set()
        bid_by_no = bid_by_no or {}
        bid_sections = bid_sections or []
        for item in TemplateExtractor.extract_consistency_templates(tender_payload or {}):
            title = self._attachment_title(item.get("title"))
            text = "\n".join(item.get("content") or [])
            template_req = self._requirements(title, text)
            attachment_number = self._attachment_number(title)
            key = attachment_number or title
            if key in seen:
                continue
            seen.add(key)
            probe_attachment = {
                "attachment_number": attachment_number,
                "title": title,
                "text": text,
                "requirements": template_req,
            }
            bid_section = self._match_attachment(probe_attachment, bid_by_no, bid_sections)
            if bid_section is None and any(marker in title for marker in self.EXCLUDE_ATTACHMENTS):
                continue
            merged_req = self._merge_requirements(
                template_req,
                self._requirements(title, bid_section.get("text") or "") if bid_section else None,
            )
            if merged_req.get("is_optional") and not any(
                merged_req.get(flag) for flag in ("requires_signature", "requires_seal", "requires_date")
            ):
                continue
            result.append(
                {
                    "attachment_number": attachment_number,
                    "title": title,
                    "text": text,
                    "requirements": merged_req,
                }
            )
        return result

    def _merge_requirements(self, primary: dict | None, secondary: dict | None) -> dict:
        base = dict(primary or {})
        other = dict(secondary or {})

        signature_examples = list(dict.fromkeys((base.get("signature_field_examples") or []) + (other.get("signature_field_examples") or [])))
        seal_examples = list(dict.fromkeys((base.get("seal_field_examples") or []) + (other.get("seal_field_examples") or [])))
        date_examples = list(dict.fromkeys((base.get("date_field_examples") or []) + (other.get("date_field_examples") or [])))

        return {
            "requires_signature": bool(base.get("requires_signature")),
            "signature_field_count": int(base.get("signature_field_count") or 0),
            "signature_field_examples": signature_examples[:3],
            "requires_seal": bool(base.get("requires_seal")),
            "seal_field_examples": seal_examples[:3],
            "requires_date": bool(base.get("requires_date")),
            "date_field_examples": date_examples[:3],
            "is_optional": bool(base.get("is_optional")),
        }

    def _requirements(self, title: str, text: str) -> dict:
        lines = self._lines(text)
        sig = [x for x in lines if self._is_signature_requirement_line(x)]
        seal = [x for x in lines if self._is_seal_requirement_line(x)]
        dates = [x for x in lines if self._is_date_requirement_line(x)]
        return {"requires_signature": bool(sig), "signature_field_count": len(sig), "signature_field_examples": sig[:3], "requires_seal": bool(seal), "seal_field_examples": seal[:3], "requires_date": bool(dates), "date_field_examples": dates[:3], "is_optional": any(x in f'{title}\n{text[:200]}' for x in self.OPTIONAL_MARKERS)}

    def _is_signature_requirement_line(self, line: str) -> bool:
        text, compact = str(line or "").strip(), self._compact(line)
        if not compact or "签字代表" in compact or any(x in compact for x in ("本授权书声明", "全权办理", "投标活动", "合法代理人")):
            return False
        if not any(x in compact for x in self.SIGNATURE_MARKERS) or not any(x in compact for x in self.SIGNATURE_ANCHORS):
            return False
        if len(compact) > 80 or (any(x in text for x in ("\\underline", "\\text{", "$")) and len(compact) > 40):
            return False
        return "：" in text or ":" in text or "___" in text or "__" in text or len(compact) <= 24

    def _is_seal_requirement_line(self, line: str) -> bool:
        compact = self._compact(line)
        if not compact or not any(x in compact for x in self.SEAL_MARKERS):
            return False
        if any(x in compact for x in self.SIGNATURE_MARKERS) and not any(x in compact for x in self.COMPANY_ANCHORS):
            return False
        return len(compact) <= 100 or any(x in compact for x in self.COMPANY_ANCHORS)

    def _is_date_requirement_line(self, line: str) -> bool:
        text, compact = str(line or "").strip(), self._compact(line)
        blocked = ("成立日期", "注册日期", "有效期", "合同签订日期", "签订日期为准", "日期为准", "提问截止", "截止日期", "截止时间", "开标时间", "开标日期")
        if not compact or "打印日期" in compact or any(x in compact for x in blocked):
            return False
        if "签订日期" in compact and not compact.startswith("签订日期"):
            return False
        if not any(x in compact for x in ("日期", "填写日期", "签署日期", "落款日期", "签订日期")):
            return False
        starts_with_anchor = any(compact.startswith(anchor) for anchor in self.DATE_FIELD_ANCHORS)
        near_signing_context = any(anchor in compact for anchor in self.SIGNATURE_MARKERS + self.SIGNATURE_ANCHORS + self.SEAL_MARKERS + self.COMPANY_ANCHORS)
        if not starts_with_anchor and not near_signing_context:
            return False
        if "：" not in text and ":" not in text and not starts_with_anchor:
            return False
        return len(compact) <= 40 or "underline" in compact.lower() or bool(self._date_candidates(text))

    def _match_attachment(self, attachment: dict, bid_by_no: dict[str, dict], all_sections: list[dict]) -> dict | None:
        if attachment.get("attachment_number") in bid_by_no:
            return bid_by_no[attachment["attachment_number"]]
        attachment_number = attachment.get("attachment_number")
        title = self._attachment_title(attachment["title"])
        best = None
        for section in all_sections:
            section_number = section.get("attachment_number")
            if attachment_number is not None and section_number not in (None, attachment_number):
                continue
            score = self._attachment_title_score(title, section.get("title") or "")
            if best is None or score > best["score"]:
                best = {"score": score, "section": section}
        return best["section"] if best and best["score"] >= 0.68 else None

    def _evaluate_attachment(self, attachment: dict, bid_section: dict | None, deadline: dict | None, bidder_name: str | None) -> dict:
        seal_check = self._seal_check(attachment, bid_section, bidder_name)
        date_check = self._date_check(attachment, bid_section, deadline)
        signature_check = self._signature_check(attachment, bid_section, seal_check, date_check)
        if bid_section is None:
            status = "fail"
        elif any(x["status"] in {"fail", "missing_date", "late"} for x in (signature_check, seal_check, date_check)):
            status = "fail"
        elif any(x["status"] in {"pending", "missing_deadline"} for x in (signature_check, seal_check, date_check)):
            status = "pending"
        else:
            status = "pass"
        return {"title": attachment["title"], "attachment_number": attachment.get("attachment_number"), "found": bid_section is not None, "matched_bid_title": bid_section["title"] if bid_section else None, "pages": bid_section["pages"] if bid_section else [], "requirements": attachment["requirements"], "signature_check": signature_check, "seal_check": seal_check, "date_check": date_check, "status": status}

    def _signature_check(self, attachment: dict, bid_section: dict | None, seal_check: dict, date_check: dict) -> dict:
        required = int(attachment["requirements"].get("signature_field_count") or 0)
        if required <= 0:
            return {"status": "not_required", "required_count": 0, "filled_count": 0, "pending_count": 0, "filled_values": [], "pending_fields": [], "empty_fields": []}
        if bid_section is None:
            return {"status": "fail", "required_count": required, "filled_count": 0, "pending_count": 0, "filled_values": [], "pending_fields": [], "empty_fields": attachment["requirements"].get("signature_field_examples") or []}

        slots = self._collect_signature_slots(attachment, bid_section)
        filled, pending, empty = [], [], []
        used_signature_keys, used_seal_keys = set(), set()

        for slot in slots:
            direct_value = self._signature_value(slot["line"])
            if direct_value:
                filled.append({"line": slot["line"], "page": slot.get("page"), "mode": "placeholder_backfill" if self._is_signature_placeholder_value(direct_value) else "text_inline", "value": direct_value})
                continue

            nearby_signature = self._signature_nearby_detected_signature(slot, bid_section)
            if nearby_signature is not None:
                signature_key = self._evidence_key(nearby_signature.get("page"), nearby_signature.get("box"), nearby_signature.get("text") or nearby_signature.get("value"))
                if signature_key in used_signature_keys:
                    nearby_signature = None
                else:
                    used_signature_keys.add(signature_key)
            if nearby_signature is not None:
                filled.append({"line": slot["line"], "page": slot.get("page"), "mode": nearby_signature.get("mode") or "ocr_signature_region", "value": nearby_signature.get("value") or self._signature_placeholder_text(), "signature_page": nearby_signature.get("page"), "signature_box": nearby_signature.get("box"), "signature_text": nearby_signature.get("text")})
                continue

            nearby_personal_seal = self._signature_nearby_personal_seal(slot, bid_section)
            if slot["allows_seal"] and nearby_personal_seal is not None:
                seal_key = self._evidence_key(nearby_personal_seal.get("page"), nearby_personal_seal.get("box"), nearby_personal_seal.get("seal_text") or nearby_personal_seal.get("person_name"))
                if seal_key in used_seal_keys:
                    nearby_personal_seal = None
                else:
                    used_seal_keys.add(seal_key)
            if slot["allows_seal"] and nearby_personal_seal is not None:
                filled.append({"line": slot["line"], "page": slot.get("page"), "mode": "personal_seal_as_alternative", "value": nearby_personal_seal.get("person_name"), "seal_page": nearby_personal_seal.get("page"), "seal_box": nearby_personal_seal.get("box"), "seal_text": nearby_personal_seal.get("seal_text")})
                continue

            nearby_seal = self._signature_nearby_seal_evidence(slot, bid_section)
            if slot["allows_seal"] and nearby_seal is not None:
                seal_key = self._evidence_key(nearby_seal.get("page"), nearby_seal.get("box"), nearby_seal.get("seal_text"))
                if seal_key in used_seal_keys:
                    nearby_seal = None
                else:
                    used_seal_keys.add(seal_key)
            if slot["allows_seal"] and nearby_seal is not None:
                filled.append({"line": slot["line"], "page": slot.get("page"), "mode": "seal_region_as_alternative", "value": self._signature_placeholder_text(), "seal_page": nearby_seal.get("page"), "seal_box": nearby_seal.get("box"), "seal_text": nearby_seal.get("seal_text")})
                continue

            nearby_text = self._signature_nearby_text_evidence(slot, bid_section)
            if nearby_text is not None:
                filled.append({"line": slot["line"], "page": slot.get("page"), "mode": "nearby_text_mark", "value": nearby_text["value"], "evidence_text": nearby_text["text"], "evidence_page": nearby_text["page"]})
                continue

            if self._supports_signature_pending(slot, date_check):
                pending.append({"line": slot["line"], "page": slot.get("page"), "reason": "signature_anchor_found_but_no_stable_person_signature_or_personal_seal", "allows_seal": slot["allows_seal"]})
                continue

            empty.append(slot["line"])

        if not slots and self._supports_signature_pending({"line": (attachment["requirements"].get("signature_field_examples") or ["签字位"])[0], "page": (bid_section.get("pages") or [None])[-1], "allows_seal": False}, date_check):
            pending.append({"line": (attachment["requirements"].get("signature_field_examples") or ["签字位"])[0], "page": (bid_section.get("pages") or [None])[-1], "reason": "signature_template_found_but_slot_not_stably_extracted", "allows_seal": False})

        status = "pass" if len(filled) >= required else ("pending" if len(filled) + len(pending) >= required else "fail")
        return {"status": status, "required_count": required, "filled_count": len(filled), "pending_count": len(pending), "filled_values": filled[:5], "pending_fields": pending[:5], "empty_fields": empty[:5]}

    def _signature_value(self, line: str) -> str | None:
        text, compact = str(line or "").strip(), self._compact(line)
        if not compact or len(compact) > 80 or (any(x in text for x in ("\\underline", "\\text{", "$")) and len(compact) > 40):
            return None
        if self._is_signature_placeholder_value(text):
            return self._signature_placeholder_text()
        value = re.split(r"[：:]", text)[-1] if ("：" in text or ":" in text) else re.split(r"(?:签字或盖章|签字|签章|签名|手签)", text, maxsplit=1)[-1]
        if self._is_signature_placeholder_value(value):
            return self._signature_placeholder_text()
        value = value.replace("underline", "").replace("text", "")
        value = re.sub(r"\$|\\underline|\\text\{|\}", "", value)
        value = re.sub(r"(?:签字或盖章|签字|签章|签名|手签|盖章)", "", value)
        value = re.sub(r"[（）()【】\[\]_:：,，;；\.\-—/\\\s_]+", "", value)
        return value[:40] if value and re.search(r"[A-Za-z0-9\u4e00-\u9fa5]", value) else None

    def _signature_placeholder_text(self) -> str:
        return self.SIGNATURE_PLACEHOLDER_TEXTS[0]

    def _is_signature_placeholder_value(self, text: Any) -> bool:
        compact = self._compact(text)
        if not compact:
            return False
        return compact in {self._compact(item) for item in self.SIGNATURE_PLACEHOLDER_TEXTS}

    def _signature_line_allows_seal(self, line: str) -> bool:
        compact = self._compact(line)
        return "签字或盖章" in compact or "签章" in compact

    def _collect_signature_slots(self, attachment: dict, bid_section: dict) -> list[dict]:
        slots, seen = [], set()

        def append_slot(line: str, page: int | None = None, bbox: list[int] | None = None, source: str = "text") -> None:
            compact = self._compact(line)
            if not compact:
                return
            key = (page, compact)
            if key in seen:
                return
            seen.add(key)
            slots.append(
                {
                    "line": str(line or "").strip(),
                    "page": page,
                    "bbox": self._normalize_bbox(bbox),
                    "allows_seal": self._signature_line_allows_seal(line),
                    "source": source,
                }
            )

        for section in bid_section.get("sections") or []:
            line = str(section.get("text") or "").strip()
            if self._is_signature_requirement_line(line):
                append_slot(line, section.get("page"), section.get("bbox"), "section")

        if not slots:
            fallback_page = (bid_section.get("pages") or [None])[-1]
            for line in self._lines(bid_section.get("text") or ""):
                if self._is_signature_requirement_line(line):
                    append_slot(line, fallback_page, None, "fallback_text")

        if not slots:
            fallback_page = (bid_section.get("pages") or [None])[-1]
            for line in attachment["requirements"].get("signature_field_examples") or []:
                append_slot(line, fallback_page, None, "template")

        return slots

    def _normalize_signature_candidate(self, text: str) -> str | None:
        value = str(text or "").strip()
        compact = self._compact(value)
        if not compact or len(compact) > 32:
            return None
        if self._is_signature_placeholder_value(value):
            return self._signature_placeholder_text()
        if self._is_signature_requirement_line(value) or self._is_date_requirement_line(value) or self._is_seal_requirement_line(value):
            return None
        if any(anchor in compact for anchor in self.COMPANY_ANCHORS):
            return None
        if re.search(r"(有限责任公司|股份有限公司|集团有限公司|有限公司|公司)", compact):
            return None
        value = value.replace("underline", "").replace("text", "")
        value = re.sub(r"\$|\\underline|\\text\{|\}", "", value)
        value = re.sub(r"[（）()【】\[\]_:：,，;；\.\-—/\\\s_]+", "", value)
        if not value or len(value) > 16 or len(value) < 2 or re.fullmatch(r"\d+", value) or re.search(r"\d", value):
            return None
        blocked = ("电话", "传真", "注", "特此声明", "职务", "销售", "总监", "身份证", "地址", "邮编", "邮箱", "说明", "附件", "授权", "投标", "日期", "声明", "附", "项目", "编号")
        if any(token in value for token in blocked):
            return None
        if re.fullmatch(r"[\u4e00-\u9fa5]{2,6}", value):
            return value
        if re.fullmatch(r"[A-Za-z]{2,20}", value):
            return value
        if re.fullmatch(r"[\u4e00-\u9fa5A-Za-z]{2,8}", value) and len(re.findall(r"[\u4e00-\u9fa5]", value)) >= 2:
            return value
        return None

    def _box_near_signature_slot(self, slot_box: list[int] | None, candidate_box: list[int] | None, *, max_dx: int = 320, max_dy: int = 90) -> bool:
        if slot_box is None or candidate_box is None:
            return False
        slot_left, slot_top, slot_w, slot_h = slot_box
        cand_left, cand_top, cand_w, cand_h = candidate_box
        slot_right = slot_left + slot_w
        slot_bottom = slot_top + slot_h
        cand_right = cand_left + cand_w
        same_line = abs(cand_top - slot_top) <= max(max(slot_h, cand_h), 24)
        to_right = cand_left >= slot_left - 24 and cand_left - slot_right <= max_dx
        below = 0 <= cand_top - slot_bottom <= max_dy and cand_right >= slot_left - 24 and cand_left <= slot_right + 120
        return (same_line and to_right) or below

    def _signature_nearby_text_evidence(self, slot: dict, bid_section: dict) -> dict | None:
        slot_page = slot.get("page")
        slot_box = self._normalize_bbox(slot.get("bbox"))
        best = None

        if slot_box is not None and slot_page is not None:
            for section in bid_section.get("sections") or []:
                if section.get("page") != slot_page:
                    continue
                if str(section.get("text") or "").strip() == slot["line"]:
                    continue
                candidate_box = self._normalize_bbox(section.get("bbox"))
                if not self._box_near_signature_slot(slot_box, candidate_box):
                    continue
                candidate_text = str(section.get("text") or "").strip()
                candidate_value = self._normalize_signature_candidate(candidate_text)
                if candidate_value is None:
                    continue
                distance = max((candidate_box[0] if candidate_box else 0) - (slot_box[0] + slot_box[2]), 0) + abs((candidate_box[1] if candidate_box else 0) - slot_box[1])
                score = distance + len(candidate_value) * 4
                if best is None or score < best["score"]:
                    best = {"score": score, "text": candidate_text, "value": candidate_value, "page": section.get("page")}

        if best is not None:
            return best

        lines = self._lines(bid_section.get("text") or "")
        for idx, line in enumerate(lines):
            if line != slot["line"]:
                continue
            for offset in (1, -1, 2):
                neighbor_idx = idx + offset
                if not (0 <= neighbor_idx < len(lines)):
                    continue
                candidate_text = lines[neighbor_idx]
                candidate_value = self._normalize_signature_candidate(candidate_text)
                if candidate_value is not None:
                    return {"text": candidate_text, "value": candidate_value, "page": slot_page}
        return None

    def _signature_nearby_detected_signature(self, slot: dict, bid_section: dict) -> dict | None:
        slot_page = slot.get("page")
        slot_box = self._normalize_bbox(slot.get("bbox"))
        best = None

        for section in bid_section.get("sections") or []:
            if str(section.get("type") or "").strip().lower() != "signature":
                continue
            if slot_page is not None and section.get("page") != slot_page:
                continue
            candidate_box = self._normalize_bbox(section.get("bbox"))
            if slot_box is not None and candidate_box is not None and not self._box_near_signature_slot(slot_box, candidate_box, max_dx=420, max_dy=180):
                continue
            candidate_text = str(section.get("text") or "").strip()
            distance = 0
            if slot_box is not None and candidate_box is not None:
                distance = max(candidate_box[0] - (slot_box[0] + slot_box[2]), 0) + abs(candidate_box[1] - slot_box[1])
            score = distance - (60 if self._is_signature_placeholder_value(candidate_text) else 0)
            if best is None or score < best["score"]:
                best = {"score": score, "page": section.get("page"), "box": candidate_box, "text": candidate_text, "value": self._signature_placeholder_text(), "mode": "ocr_signature_section"}

        for item in bid_section.get("signature_locations") or []:
            if slot_page is not None and item.get("page") != slot_page:
                continue
            candidate_box = self._normalize_bbox(item.get("box"))
            if slot_box is not None and candidate_box is not None and not self._box_near_signature_slot(slot_box, candidate_box, max_dx=420, max_dy=180):
                continue
            distance = 0
            if slot_box is not None and candidate_box is not None:
                distance = max(candidate_box[0] - (slot_box[0] + slot_box[2]), 0) + abs(candidate_box[1] - slot_box[1])
            if best is None or distance < best["score"]:
                best = {"score": distance, "page": item.get("page"), "box": candidate_box, "text": None, "value": self._signature_placeholder_text(), "mode": "ocr_signature_location"}

        if best is not None:
            best.pop("score", None)
            return best

        if slot_page is None:
            return None
        fallback_item = next((item for item in (bid_section.get("signature_locations") or []) if item.get("page") in (bid_section.get("pages") or [])), None)
        if fallback_item is None:
            return None
        return {"page": fallback_item.get("page"), "box": self._normalize_bbox(fallback_item.get("box")), "text": None, "value": self._signature_placeholder_text(), "mode": "ocr_signature_location_fallback"}

    def _normalize_person_name(self, text: str) -> str | None:
        value = self._normalize_signature_candidate(text)
        if value is None:
            return None
        return value if re.fullmatch(r"[\u4e00-\u9fa5]{2,4}", value) or re.fullmatch(r"[A-Za-z]{2,20}", value) else None

    def _extract_person_name_candidates(self, bid_section: dict) -> list[str]:
        names: list[str] = []
        patterns = (
            r"[\(（]\s*([一-龥]{2,4})\s*[、,，/]",
            r"(?:法定代表人|授权代表|授权委托人|委托代理人|被授权人)[：:\s]{0,4}([一-龥]{2,4})",
        )
        for line in self._lines(bid_section.get("text") or ""):
            compact = self._compact(line)
            if "公司" in compact:
                continue
            for pattern in patterns:
                for match in re.finditer(pattern, line):
                    person = self._normalize_person_name(match.group(1))
                    if person:
                        names.append(person)
        return list(dict.fromkeys(names))

    def _is_company_like_seal_text(self, text: str) -> bool:
        compact = self._compact(text)
        return bool(re.search(r"(有限责任公司|股份有限公司|集团有限公司|有限公司|公司|专用章)$", compact) or "公司" in compact)

    def _signature_nearby_personal_seal(self, slot: dict, bid_section: dict) -> dict | None:
        slot_page = slot.get("page")
        slot_box = self._normalize_bbox(slot.get("bbox"))
        personal_seal_texts = []
        for seal_text in bid_section.get("seal_texts") or []:
            normalized = self._normalize_person_name(seal_text)
            if normalized is not None and not self._is_company_like_seal_text(seal_text):
                personal_seal_texts.append({"seal_text": seal_text, "person_name": normalized})

        if not personal_seal_texts:
            return None
        best = None
        for item in bid_section.get("seal_locations") or []:
            if slot_page is not None and item.get("page") != slot_page:
                continue
            candidate_box = self._normalize_bbox(item.get("box"))
            if slot_box is not None and not self._box_near_signature_slot(slot_box, candidate_box, max_dx=420, max_dy=180):
                continue
            distance = 0
            if slot_box is not None:
                distance = max((candidate_box[0] if candidate_box else 0) - (slot_box[0] + slot_box[2]), 0) + abs((candidate_box[1] if candidate_box else 0) - slot_box[1])
            if best is None or distance < best["distance"]:
                best = {"distance": distance, "page": item.get("page"), "box": candidate_box, **personal_seal_texts[0]}
        if best is not None:
            return best
        return {"page": slot_page, "box": None, **personal_seal_texts[0]}

    def _signature_nearby_seal_evidence(self, slot: dict, bid_section: dict) -> dict | None:
        slot_page = slot.get("page")
        slot_box = self._normalize_bbox(slot.get("bbox"))
        best = None
        for item in bid_section.get("seal_locations") or []:
            if slot_page is not None and item.get("page") != slot_page:
                continue
            candidate_box = self._normalize_bbox(item.get("box"))
            if slot_box is not None and candidate_box is not None and not self._box_near_signature_slot(slot_box, candidate_box, max_dx=420, max_dy=180):
                continue
            distance = 0
            if slot_box is not None and candidate_box is not None:
                distance = max(candidate_box[0] - (slot_box[0] + slot_box[2]), 0) + abs(candidate_box[1] - slot_box[1])
            seal_text = self._best_nearby_seal_text(slot_page, candidate_box, bid_section)
            if best is None or distance < best["distance"]:
                best = {"distance": distance, "page": item.get("page"), "box": candidate_box, "seal_text": seal_text}
        if best is not None:
            best.pop("distance", None)
            return best
        return None

    def _best_nearby_seal_text(self, page: int | None, seal_box: list[int] | None, bid_section: dict) -> str | None:
        best = None
        for section in bid_section.get("sections") or []:
            if str(section.get("type") or "").strip().lower() != "seal":
                continue
            if page is not None and section.get("page") != page:
                continue
            text = str(section.get("text") or "").strip()
            if not text:
                continue
            candidate_box = self._normalize_bbox(section.get("bbox"))
            distance = 0
            if seal_box is not None and candidate_box is not None:
                distance = abs(candidate_box[0] - seal_box[0]) + abs(candidate_box[1] - seal_box[1])
            if best is None or distance < best["distance"]:
                best = {"distance": distance, "text": text}
        if best is not None:
            return best["text"]
        seal_texts = [str(x).strip() for x in (bid_section.get("seal_texts") or []) if str(x).strip()]
        return seal_texts[0] if seal_texts else None

    def _supports_signature_pending(self, slot: dict, date_check: dict) -> bool:
        if not str(slot.get("line") or "").strip():
            return False
        return date_check.get("status") in {"pass", "not_required", "missing_deadline", "missing_date", "late"}

    def _seal_check(self, attachment: dict, bid_section: dict | None, bidder_name: str | None) -> dict:
        if not attachment["requirements"].get("requires_seal"):
            return {"status": "not_required", "detected": False, "matched": None, "seal_texts": [], "best_match": None}
        if bid_section is None:
            return {"status": "fail", "detected": False, "matched": False, "seal_texts": [], "best_match": None}
        seal_texts = list(dict.fromkeys(str(x).strip() for x in (bid_section.get("seal_texts") or []) if str(x).strip()))
        seal_locations = self._dedupe_locations(bid_section.get("seal_locations") or [])
        detected, matched, best_match = bool(seal_texts or seal_locations), bool(seal_texts or seal_locations), None
        if bidder_name and seal_texts:
            for seal_text in seal_texts:
                score = self._company_score(bidder_name, seal_text)
                if best_match is None or score > best_match["score"]:
                    best_match = {"bidder_name": bidder_name, "seal_text": seal_text, "score": round(score, 4)}
            matched = bool(best_match and best_match["score"] >= 0.45)
        return {"status": "pass" if detected and matched else "fail", "detected": detected, "matched": matched, "seal_texts": seal_texts, "seal_locations": seal_locations, "best_match": best_match}

    def _date_check(self, attachment: dict, bid_section: dict | None, deadline: dict | None) -> dict:
        if not attachment["requirements"].get("requires_date"):
            return {"status": "not_required", "sign_date": None, "deadline_date": deadline["date"].isoformat() if deadline else None, "matched_sign_text": None, "matched_deadline_text": deadline["text"] if deadline else None}
        if bid_section is None:
            return {"status": "missing_date", "sign_date": None, "deadline_date": deadline["date"].isoformat() if deadline else None, "matched_sign_text": None, "matched_deadline_text": deadline["text"] if deadline else None}
        if deadline is None:
            return {"status": "missing_deadline", "sign_date": None, "deadline_date": None, "matched_sign_text": None, "matched_deadline_text": None}
        sign_date = self._section_date(bid_section.get("text") or "", bid_section.get("sections") or [])
        if sign_date is None:
            return {"status": "missing_date", "sign_date": None, "deadline_date": deadline["date"].isoformat(), "matched_sign_text": None, "matched_deadline_text": deadline["text"]}
        ok = sign_date["date"] <= deadline["date"]
        return {"status": "pass" if ok else "late", "sign_date": sign_date["date"].isoformat(), "deadline_date": deadline["date"].isoformat(), "matched_sign_text": sign_date["text"], "matched_deadline_text": deadline["text"], "is_before_deadline": ok, "days_gap": (deadline["date"] - sign_date["date"]).days}

    def _section_date(self, text: str, sections: list[dict] | None = None) -> dict | None:
        items, lines = [], self._lines(text)
        for i, line in enumerate(lines):
            if not self._is_date_requirement_line(line):
                continue
            items.extend(self._date_candidates(line))
            if not items and i + 1 < len(lines):
                items.extend(self._date_candidates(lines[i + 1]))
        if items:
            return max(items, key=lambda x: x["date"])

        fallback_items = []
        for section in sections or []:
            section_text = str(section.get("text") or "").strip()
            compact = self._compact(section_text)
            if not compact or str(section.get("type") or "").strip().lower() == "seal":
                continue
            if any(anchor in compact for anchor in self.DATE_FIELD_ANCHORS):
                fallback_items.extend(self._date_candidates(section_text))
        if fallback_items:
            return max(fallback_items, key=lambda x: x["date"])

        contextual_items = []
        recent_sections = list(sections or [])[-8:]
        context_anchors = self.DATE_FIELD_ANCHORS + self.SIGNATURE_MARKERS + self.SEAL_MARKERS + self.COMPANY_ANCHORS
        for idx, section in enumerate(recent_sections):
            section_text = str(section.get("text") or "").strip()
            compact = self._compact(section_text)
            if not compact:
                continue
            section_candidates = self._date_candidates(section_text)
            has_context_anchor = any(anchor in compact for anchor in context_anchors)
            nearby_context = False
            if section_candidates:
                for neighbor in recent_sections[max(0, idx - 2): min(len(recent_sections), idx + 3)]:
                    neighbor_compact = self._compact(str(neighbor.get("text") or "").strip())
                    if any(anchor in neighbor_compact for anchor in context_anchors):
                        nearby_context = True
                        break
            if has_context_anchor or (section_candidates and nearby_context):
                contextual_items.extend(section_candidates)
        return max(contextual_items, key=lambda x: x["date"]) if contextual_items else None

    def _signature_values(self, text: str) -> list[dict]:
        values = []
        for line in self._lines(text):
            if self._is_signature_requirement_line(line):
                value = self._signature_value(line)
                if value:
                    values.append({"line": line, "value": value})
        return values

    def _normalize_company(self, text: str) -> str:
        text = re.sub(r"（.*?）|\(.*?\)", "", str(text or ""))
        text = re.sub(r"[^0-9A-Za-z\u4e00-\u9fa5]", "", text)
        for anchor in self.COMPANY_ANCHORS:
            text = text.replace(anchor, "")
        return text

    def _bidder_name(self, payload: dict | None, seal_texts: list[str]) -> str | None:
        container = self._container(payload)
        for key in ("bidder_name", "company_name", "supplier_name"):
            value = container.get(key)
            if isinstance(value, str) and value.strip():
                normalized = self._normalize_company(value)
                if len(normalized) >= 4:
                    return normalized
        for section in self._sections(payload):
            compact = self._compact(section["text"])
            for anchor in self.COMPANY_ANCHORS:
                if anchor in compact:
                    match = self.COMPANY_RE.search(compact.split(anchor, 1)[-1])
                    if match:
                        return self._normalize_company(match.group(1))
        for seal_text in seal_texts:
            match = self.COMPANY_RE.search(self._compact(seal_text))
            if match:
                return self._normalize_company(match.group(1))
        return None

    def _company_score(self, bidder_name: str, seal_text: str) -> float:
        bidder, seal = self._normalize_company(bidder_name), self._normalize_company(seal_text)
        if not bidder or not seal:
            return 0.0
        bonus = 0.35 if bidder in seal or seal in bidder else 0.0
        ratio = SequenceMatcher(None, bidder, seal).ratio()
        overlap = len(set(bidder) & set(seal)) / max(len(set(bidder)), 1)
        return min(max(ratio, overlap) + bonus, 1.0)

    def _seal_company_check(self, bidder_name: str | None, seal_texts: list[str]) -> dict:
        if not seal_texts:
            return {"status": "pending", "matched": False, "reason": "seal_text_not_found", "best_match": None}
        if not bidder_name:
            return {"status": "pending", "matched": False, "reason": "bidder_name_not_found", "best_match": None}
        best = None
        for seal_text in seal_texts:
            score = self._company_score(bidder_name, seal_text)
            if best is None or score > best["score"]:
                best = {"bidder_name": bidder_name, "seal_text": seal_text, "score": round(score, 4)}
        matched = bool(best and best["score"] >= 0.45)
        return {"status": "pass" if matched else "fail", "matched": matched, "reason": "matched" if matched else "low_similarity", "best_match": best}
