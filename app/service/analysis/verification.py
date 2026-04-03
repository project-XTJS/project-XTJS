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
    SEAL_MARKERS = ("盖章", "公章", "加盖公章")
    COMPANY_ANCHORS = ("投标人名称", "投标人", "供应商名称", "供应商", "单位名称", "公司名称", "企业名称", "声明人")
    OPTIONAL_MARKERS = ("如有", "可选", "如适用", "如需")
    EXCLUDE_ATTACHMENTS = ("拟派项目负责人情况表", "项目人员配置表", "人员配置表")
    ATTACHMENT_RE = re.compile(r"附件\s*(?P<number>\d+(?:\s*-\s*\d+)*)")
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
        bidder_name = self._bidder_name(bid, seal_bundle["texts"])
        deadline = self._deadline_from_doc(tender)
        bid_sections = self._attachment_sections(bid)
        bid_by_no = {x["attachment_number"]: x for x in bid_sections if x.get("attachment_number")}
        required = self._required_attachments(tender, bid_by_no, bid_sections)

        results, missing_attachments, missing_signatures, missing_seals, missing_dates, late_dates = [], [], [], [], [], []
        for item in required:
            section = self._match_attachment(item, bid_by_no, bid_sections)
            result = self._evaluate_attachment(item, section, deadline, bidder_name)
            results.append(result)
            if not result["found"]:
                missing_attachments.append(result["title"])
            if result["signature_check"]["status"] == "fail":
                missing_signatures.append(result["title"])
            if result["seal_check"]["status"] == "fail":
                missing_seals.append(result["title"])
            if result["date_check"]["status"] == "missing_date":
                missing_dates.append(result["title"])
            if result["date_check"]["status"] == "late":
                late_dates.append(result["title"])

        if not required or deadline is None:
            compliance_status = "pending"
        elif any(x["status"] == "fail" for x in results):
            compliance_status = "fail"
        else:
            compliance_status = "pass"

        date_status = "missing_deadline" if deadline is None else ("fail" if missing_dates or late_dates else "pass")
        position_status = "fail" if missing_attachments or missing_signatures or missing_seals else "pass"
        return {
            "mode": "tender_vs_bid",
            "summary": self._pair_summary(len(required), deadline, compliance_status, missing_attachments, missing_signatures, missing_seals, missing_dates, late_dates),
            "seal_detected": seal_bundle["detected"],
            "seal_count": seal_bundle["count"],
            "seal_contents": seal_bundle["texts"],
            "bidder_name": bidder_name,
            "required_attachment_count": len(required),
            "required_attachments": [x["title"] for x in required],
            "attachment_results": results,
            "position_check": {"status": position_status, "missing_attachments": missing_attachments, "missing_signature_attachments": missing_signatures, "missing_seal_attachments": missing_seals},
            "date_check": {"status": date_status, "deadline_date": deadline["date"].isoformat() if deadline else None, "matched_deadline_text": deadline["text"] if deadline else None, "missing_date_attachments": missing_dates, "late_date_attachments": late_dates},
            "deadline_check": {"status": date_status, "deadline_date": deadline["date"].isoformat() if deadline else None, "matched_deadline_text": deadline["text"] if deadline else None, "source": "tender_document"},
            "seal_company_check": self._seal_company_check(bidder_name, seal_bundle["texts"]),
            "compliance_status": compliance_status,
        }

    def _check_single(self, payload: Any) -> dict:
        document = self._as_document(payload) or {}
        text = self._text(document)
        seal_bundle = self._seal_bundle(document)
        signatures = self._signature_values(text)
        sign_date = self._section_date(text)
        return {
            "mode": "single_document",
            "summary": "仅基于单文档全文做兜底扫描，未执行招投标附件级联校验。",
            "seal_detected": seal_bundle["detected"],
            "seal_count": seal_bundle["count"],
            "seal_contents": seal_bundle["texts"],
            "bidder_name": self._bidder_name(document, seal_bundle["texts"]),
            "required_attachment_count": 0,
            "required_attachments": [],
            "attachment_results": [],
            "position_check": {"status": "pass" if signatures else "fail", "missing_attachments": [], "missing_signature_attachments": [] if signatures else ["全文未识别到有效签字内容"], "missing_seal_attachments": [] if seal_bundle["detected"] else ["全文未识别到有效盖章"]},
            "date_check": {"status": "pass" if sign_date else "missing_date", "deadline_date": None, "matched_deadline_text": None, "missing_date_attachments": [] if sign_date else ["全文未识别到落款日期"], "late_date_attachments": []},
            "deadline_check": {"status": "not_applicable", "deadline_date": None, "matched_deadline_text": None, "source": None},
            "seal_company_check": self._seal_company_check(self._bidder_name(document, seal_bundle["texts"]), seal_bundle["texts"]),
            "compliance_status": "pass" if text and seal_bundle["detected"] and signatures and sign_date else "pending",
        }

    def _pair_summary(self, count: int, deadline: dict | None, status: str, missing_attachments: list[str], missing_signatures: list[str], missing_seals: list[str], missing_dates: list[str], late_dates: list[str]) -> str:
        if count <= 0:
            return "未在招标模板中识别到明确要求签字、盖章或填写日期的附件。"
        if deadline is None:
            return "已识别到需核验的附件，但未能稳定提取招标文件中的最晚提交日期。"
        if status == "pass":
            return f"共核验 {count} 个附件，签字、盖章与日期均满足要求，落款日期不晚于 {deadline['date'].isoformat()}。"
        issues = []
        if missing_attachments: issues.append(f"缺少附件 {len(missing_attachments)} 个")
        if missing_signatures: issues.append(f"缺少有效签字 {len(missing_signatures)} 个")
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
                result.append({"index": i, "page": item.get("page") if isinstance(item.get("page"), int) else None, "type": str(item.get("type") or "text").strip().lower() or "text", "text": text})
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

    def _seal_bundle(self, payload: dict | None) -> dict:
        container = self._container(payload)
        seal_node = container.get("seal") if isinstance(container.get("seal"), dict) else {}
        seal_texts = []
        for source in (seal_node.get("texts"), container.get("seal_texts"), [s["text"] for s in self._sections(payload) if s["type"] == "seal"]):
            if isinstance(source, str):
                source = [source]
            if isinstance(source, (list, tuple, set)):
                seal_texts.extend(str(x).strip() for x in source if str(x).strip())
        deduped = list(dict.fromkeys(seal_texts))
        raw_count = seal_node.get("count", container.get("seal_count", len(deduped)))
        try:
            seal_count = int(raw_count)
        except (TypeError, ValueError):
            seal_count = len(deduped)
        seal_count = max(seal_count, len(deduped))
        detected = seal_node.get("detected", container.get("seal_detected"))
        detected = bool(detected) if detected is not None else bool(seal_count or deduped)
        return {"detected": detected or bool(deduped), "count": seal_count, "texts": deduped}

    def _date_candidates(self, text: str) -> list[dict]:
        items = []
        for pattern in self.DATE_PATTERNS:
            for match in re.finditer(pattern, text or ""):
                try:
                    parsed = date(int(match.group("year")), int(match.group("month")), int(match.group("day")))
                except ValueError:
                    continue
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
        text = re.sub(r"^\s*[\d一二三四五六七八九十]+(?:\.\d+)*[、.)．]?\s*", "", str(text or "").strip())
        idx = text.find("附件")
        text = text[idx:] if idx >= 0 else text
        return re.sub(r"\s+", " ", text).strip("：:；;，,。")

    def _is_attachment_heading(self, section: dict) -> bool:
        if section.get("type") not in {"heading", "text"}:
            return False
        text = str(section.get("text") or "").strip()
        if not text or self._catalog_like(text):
            return False
        attachment_number = self._attachment_number(text)
        if attachment_number is None:
            return False
        compact = self._compact(text)
        attachment_index = compact.find("附件")
        if attachment_index < 0:
            return False
        if attachment_index > 12:
            return False
        if len(compact) > 120 and section.get("type") != "heading":
            return False
        return True

    def _attachment_sections(self, payload: dict | None) -> list[dict]:
        sections = self._sections(payload)
        starts = [i for i, x in enumerate(sections) if self._is_attachment_heading(x)]
        result = []
        for pos, start in enumerate(starts):
            end = starts[pos + 1] if pos + 1 < len(starts) else len(sections)
            chunk = sections[start:end]
            title = chunk[0]["text"]
            result.append({"attachment_number": self._attachment_number(title), "title": self._attachment_title(title), "pages": list(dict.fromkeys(x["page"] for x in chunk if x.get("page") is not None)), "text": "\n".join(x["text"] for x in chunk if x["type"] != "seal"), "seal_texts": [x["text"] for x in chunk if x["type"] == "seal"]})
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
            "requires_signature": bool(base.get("requires_signature") or other.get("requires_signature")),
            "signature_field_count": max(int(base.get("signature_field_count") or 0), int(other.get("signature_field_count") or 0)),
            "signature_field_examples": signature_examples[:3],
            "requires_seal": bool(base.get("requires_seal") or other.get("requires_seal")),
            "seal_field_examples": seal_examples[:3],
            "requires_date": bool(base.get("requires_date") or other.get("requires_date")),
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
        if "：" not in text and ":" not in text and not compact.startswith("日期") and not compact.startswith("签订日期"):
            return False
        return len(compact) <= 40 or "underline" in compact.lower() or bool(self._date_candidates(text))

    def _match_attachment(self, attachment: dict, bid_by_no: dict[str, dict], all_sections: list[dict]) -> dict | None:
        if attachment.get("attachment_number") in bid_by_no:
            return bid_by_no[attachment["attachment_number"]]
        title = self._attachment_title(attachment["title"])
        best = None
        for section in all_sections:
            score = SequenceMatcher(None, title, section.get("title") or "").ratio()
            if best is None or score > best["score"]:
                best = {"score": score, "section": section}
        return best["section"] if best and best["score"] >= 0.6 else None

    def _evaluate_attachment(self, attachment: dict, bid_section: dict | None, deadline: dict | None, bidder_name: str | None) -> dict:
        signature_check = self._signature_check(attachment, bid_section)
        seal_check = self._seal_check(attachment, bid_section, bidder_name)
        date_check = self._date_check(attachment, bid_section, deadline)
        if bid_section is None:
            status = "fail"
        elif date_check["status"] == "missing_deadline":
            status = "pending"
        else:
            status = "fail" if any(x["status"] in {"fail", "missing_date", "late"} for x in (signature_check, seal_check, date_check)) else "pass"
        return {"title": attachment["title"], "attachment_number": attachment.get("attachment_number"), "found": bid_section is not None, "matched_bid_title": bid_section["title"] if bid_section else None, "pages": bid_section["pages"] if bid_section else [], "requirements": attachment["requirements"], "signature_check": signature_check, "seal_check": seal_check, "date_check": date_check, "status": status}

    def _signature_check(self, attachment: dict, bid_section: dict | None) -> dict:
        required = int(attachment["requirements"].get("signature_field_count") or 0)
        if required <= 0:
            return {"status": "not_required", "required_count": 0, "filled_count": 0, "filled_values": [], "empty_fields": []}
        if bid_section is None:
            return {"status": "fail", "required_count": required, "filled_count": 0, "filled_values": [], "empty_fields": attachment["requirements"].get("signature_field_examples") or []}
        filled, empty = [], []
        for line in self._lines(bid_section.get("text") or ""):
            if not self._is_signature_requirement_line(line):
                continue
            value = self._signature_value(line)
            (filled if value else empty).append({"line": line, "value": value} if value else line)
        return {"status": "pass" if len(filled) >= required else "fail", "required_count": required, "filled_count": len(filled), "filled_values": filled[:5], "empty_fields": empty[:5]}

    def _signature_value(self, line: str) -> str | None:
        text, compact = str(line or "").strip(), self._compact(line)
        if not compact or len(compact) > 80 or (any(x in text for x in ("\\underline", "\\text{", "$")) and len(compact) > 40):
            return None
        value = re.split(r"[：:]", text)[-1] if ("：" in text or ":" in text) else re.split(r"(?:签字或盖章|签字|签章|签名|手签)", text, maxsplit=1)[-1]
        value = value.replace("underline", "").replace("text", "")
        value = re.sub(r"\$|\\underline|\\text\{|\}", "", value)
        value = re.sub(r"(?:签字或盖章|签字|签章|签名|手签|盖章)", "", value)
        value = re.sub(r"[（）()【】\[\]_:：,，;；\.\-—/\\\s_]+", "", value)
        return value[:40] if value and re.search(r"[A-Za-z0-9\u4e00-\u9fa5]", value) else None

    def _seal_check(self, attachment: dict, bid_section: dict | None, bidder_name: str | None) -> dict:
        if not attachment["requirements"].get("requires_seal"):
            return {"status": "not_required", "detected": False, "matched": None, "seal_texts": [], "best_match": None}
        if bid_section is None:
            return {"status": "fail", "detected": False, "matched": False, "seal_texts": [], "best_match": None}
        seal_texts = list(dict.fromkeys(str(x).strip() for x in (bid_section.get("seal_texts") or []) if str(x).strip()))
        detected, matched, best_match = bool(seal_texts), bool(seal_texts), None
        if bidder_name and seal_texts:
            for seal_text in seal_texts:
                score = self._company_score(bidder_name, seal_text)
                if best_match is None or score > best_match["score"]:
                    best_match = {"bidder_name": bidder_name, "seal_text": seal_text, "score": round(score, 4)}
            matched = bool(best_match and best_match["score"] >= 0.45)
        return {"status": "pass" if detected and matched else "fail", "detected": detected, "matched": matched, "seal_texts": seal_texts, "best_match": best_match}

    def _date_check(self, attachment: dict, bid_section: dict | None, deadline: dict | None) -> dict:
        if not attachment["requirements"].get("requires_date"):
            return {"status": "not_required", "sign_date": None, "deadline_date": deadline["date"].isoformat() if deadline else None, "matched_sign_text": None, "matched_deadline_text": deadline["text"] if deadline else None}
        if bid_section is None:
            return {"status": "missing_date", "sign_date": None, "deadline_date": deadline["date"].isoformat() if deadline else None, "matched_sign_text": None, "matched_deadline_text": deadline["text"] if deadline else None}
        if deadline is None:
            return {"status": "missing_deadline", "sign_date": None, "deadline_date": None, "matched_sign_text": None, "matched_deadline_text": None}
        sign_date = self._section_date(bid_section.get("text") or "")
        if sign_date is None:
            return {"status": "missing_date", "sign_date": None, "deadline_date": deadline["date"].isoformat(), "matched_sign_text": None, "matched_deadline_text": deadline["text"]}
        ok = sign_date["date"] <= deadline["date"]
        return {"status": "pass" if ok else "late", "sign_date": sign_date["date"].isoformat(), "deadline_date": deadline["date"].isoformat(), "matched_sign_text": sign_date["text"], "matched_deadline_text": deadline["text"], "is_before_deadline": ok, "days_gap": (deadline["date"] - sign_date["date"]).days}

    def _section_date(self, text: str) -> dict | None:
        items, lines = [], self._lines(text)
        for i, line in enumerate(lines):
            if not self._is_date_requirement_line(line):
                continue
            items.extend(self._date_candidates(line))
            if not items and i + 1 < len(lines):
                items.extend(self._date_candidates(lines[i + 1]))
        return max(items, key=lambda x: x["date"]) if items else None

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
