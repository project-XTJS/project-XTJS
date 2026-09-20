from __future__ import annotations
from .verification_evidence import project_attachment, refresh_verification_summary
from . import bidder_identity

import json
import re
from datetime import date
from typing import Any

from .attachment_synonyms import (
    canonicalize_attachment_title,
    strip_attachment_title_parenthetical_noise,
)
from .compliance.template_extractor import TemplateExtractor


class VerificationChecker:
    DATE_PATTERNS = (
        r"(?P<year>20\d{2})[年/\-.](?P<month>1[0-2]|0?[1-9])[月/\-.](?P<day>3[01]|[12]\d|0?[1-9])(?:日)?",
        r"(?<!\d)(?P<year>20\d{2})(?P<month>0[1-9]|1[0-2])(?P<day>0[1-9]|[12]\d|3[01])(?!\d)",
    )
    DEADLINE_ANCHORS = (
        "投标截止时间", "投标截止日期", "递交截止时间", "递交截止日期",
        "投标文件递交截止时间", "投标文件递交截止日期", "提交截止时间", "提交截止日期",
        "响应文件递交截止时间", "响应文件递交截止日期", "响应截止时间", "响应截止日期",
        "提交响应文件截止时间", "提交响应文件截止日期",
        "递交响应文件截止时间", "递交响应文件截止日期",
        "提交投标文件截止时间", "提交投标文件截止日期",
        "开标时间", "开标日期",
    )
    DEADLINE_PRIMARY_ANCHOR_MARKERS = ("递交", "提交", "投标截止", "响应截止")
    SIGNATURE_MARKERS = ("签字或盖章", "签字", "签章", "签名", "手签")
    SIGNATURE_ANCHORS = ("法定代表人", "授权代表", "授权委托人", "委托代理人", "被授权人", "代表人", "项目经理", "拟任项目经理")
    SIGNATURE_PLACEHOLDER_TEXTS = ("已签字", "已盖章", "已签章")
    SEAL_MARKERS = ("盖章", "公章", "加盖公章")
    SEAL_COMPANY_MATCH_THRESHOLD = 0.75
    DATE_FIELD_ANCHORS = ("日期", "填写日期", "签署日期", "落款日期", "签订日期")
    COMPANY_ANCHORS = ("投标人名称", "投标人", "供应商名称", "供应商", "单位名称", "公司名称", "企业名称", "声明人")
    OPTIONAL_MARKERS = ("如有", "可选", "如适用", "如需")
    EXCLUDE_ATTACHMENTS = ("拟派项目负责人情况表", "项目人员配置表", "人员配置表")
    ATTACHMENT_SCOPE_STOP_MARKERS = (
        "营业执照",
        "信用中国",
        "中国政府采购网",
        "国家企业信用信息公示系统",
        "纳税证明",
        "完税证明",
        "税收完税",
        "税收缴款",
        "电子缴税",
        "缴款凭证",
        "缴费凭证",
        "社会保障资金",
        "社会保险",
        "社保",
        "审计报告",
        "财务报表",
        "银行资信",
        "开户许可证",
        "开户证明",
        "基本存款账户",
        "保证金缴纳",
        "投标保证金",
        "发票",
    )
    DATE_CONTEXT_EXCLUDE_MARKERS = (
        "打印日期",
        "生成日期",
        "申报日期",
        "缴款日期",
        "缴费日期",
        "开具日期",
        "填发日期",
        "所属期",
        "成立日期",
        "注册日期",
        "营业期限",
        "有效期限",
        "有效期",
        "合同签订日期",
        "签订日期为准",
        "日期为准",
        "截止日期",
        "截止时间",
        "开标日期",
        "开标时间",
    )
    ATTACHMENT_RE = re.compile(r"^\s*(?:[（(]?\d+(?:\s*[-－–—]\s*\d+)?[)）\.、]?\s*)?附件\s*(?P<number>\d+(?:\s*[-－–—]\s*\d+)*)")
    ATTACHMENT_TITLE_PREFIX_PATTERNS = (
        r'^\s*(?:附件|附表)\s*[A-Z\d一二三四五六七八九十百零]+(?:\s*[-－–—]\s*[A-Z\d一二三四五六七八九十百零]+)*[、.)）．]?\s*',
        r'^\s*第[一二三四五六七八九十百零\d]+[章节部分篇项]\s*',
        # 兼容 8-2 / 8-5 / 8-1-2 这类子编号标题
        r'^\s*\d+(?:\s*[-－–—]\s*\d+)+(?:[、.)）．]?\s*)',
        # 兼容 2.2 / 7.2 / 8.1.2 这类多级编号标题
        r'^\s*\d+(?:[．\.、]\d+)+(?:[．\.、])?\s*',
        r'^\s*(?:\d+|[A-Z]|[一二三四五六七八九十百零]+)[．\.、]\s*',
        r'^\s*[（(](?:\d+|[A-Z]|[一二三四五六七八九十百零]+)[）)]\s*',
        r'^\s*\d+[)）]\s*',
    )
    ATTACHMENT_TITLE_NOISE_PATTERNS = (
        r'附件|附表|附录|格式',
        r'按要求加盖公章',
        r'加盖公章',
        r'后附证明材料',
        r'直接投标的应提供',
        r'委托授权人投标的应提供',
        r'委托授权投标的应提供',
        r'及被授权人身份证',
        r'及身份证',
        r'如为分支机构投标则须总公司唯一授权函',
        # 引导动词不参与标题判定：招标“提供强制采购节能产品承诺书（格式）”
        # 与投标标题“强制采购节能产品承诺书”视为同一标题。
        r'^(?:需提供|须提供|应提供|应附|后附|提供|提交|具备|具有|出具|开具|携带)',
    )
    GENERIC_UNNUMBERED_ATTACHMENT_TITLE_KEYS = {
        "承诺函",
        "证书",
    }
    COMMON_ATTACHMENT_TITLES = (
        "投标保证书",
        "投标承诺书",
        "投标函",
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
    RESPONSE_FORMAT_ZONE_MARKERS = (
        "响应文件格式附件",
        "投标文件格式附件",
        "部分格式附件",
    )
    RESPONSE_FORMAT_CHAPTER_MARKERS = (
        "响应文件格式",
        "投标文件格式",
    )
    RESPONSE_FORMAT_EMBEDDED_TITLE_MARKERS = (
        "法定代表人资格证明书",
        "法定代表人证明书",
        "法定代表人身份证明",
        "法定代表人授权委托书",
        "单位负责人证明书",
        "单位负责人身份证明",
        "授权委托书",
        "声明函",
        "承诺书",
        "保证书",
    )
    RESPONSE_FORMAT_COMPOSITE_TITLE_MARKERS = (
        "组成及部分格式",
        "文件组成及部分格式",
        "资格证明文件组成",
    )
    CHAPTER_HEADING_RE = re.compile(r"^\s*第[一二三四五六七八九十百零\d]+章")
    COMPANY_RE = re.compile(r"([A-Za-z0-9\u4e00-\u9fa5]{4,60}(?:有限责任公司|股份有限公司|集团有限公司|有限公司|公司))")

    def __init__(self, ocr_service: Any):
        self.ocr_service = ocr_service

    def check_seal_and_date(self, extraction_result: Any, bid_document: Any | None = None, *, bidder_identity_result: dict | None = None) -> dict:
        tender_document, actual_bid_document = self._extract_document_pair(extraction_result, bid_document)
        if tender_document is not None and actual_bid_document is not None:
            return self._check_pair(tender_document, actual_bid_document, bidder_identity_result=bidder_identity_result)
        return self._check_single(extraction_result)

    def _check_pair(self, tender_document: Any, bid_document: Any, *, bidder_identity_result: dict | None = None) -> dict:
        tender = self._as_document(tender_document) or {}
        bid = self._as_document(bid_document) or {}
        seal_bundle = self._seal_bundle(bid)
        signature_bundle = self._signature_bundle(bid)
        identity = bidder_identity_result if bidder_identity_result is not None else self._bidder_identity(bid)
        bidder_name = identity["name"]
        deadline = self._deadline_from_doc(tender)
        deadline_resolution = self.resolve_deadline(tender)
        template_required = self._required_attachments(tender)
        expected_attachments = self._attachment_title_hints(template_required)
        bid_sections = self._attachment_sections(bid, seal_bundle["locations"], signature_bundle["locations"], expected_attachments)
        bid_by_no: dict[str, list[dict]] = {}
        for item in bid_sections:
            attachment_number = item.get("attachment_number")
            if not attachment_number:
                continue
            bid_by_no.setdefault(attachment_number, []).append(item)
        required = self._required_attachments(tender, bid_by_no, bid_sections)

        from .requirement_groups import parse_group, alternative_state
        group_details = {}
        scope_entries = TemplateExtractor.extract_business_attachment_scope(tender).get('item_entries', [])
        if any(parse_group(e.get('content', ''))['operator'] != 'single' for e in scope_entries):
            from .compliance.integrity import IntegrityChecker
            group_details = IntegrityChecker().check_integrity(tender, bid)['details']

        results, missing_attachment_results, skipped_missing_attachments, skipped_optional_attachments, missing_signatures, pending_signatures, missing_seals, missing_dates, late_dates = [], [], [], [], [], [], [], [], []
        for item in required:
            resolution = self._resolve_attachment(item, bid_sections)
            branch_state = alternative_state(self, item['title'], group_details)
            if branch_state == 'alternative_not_provided' and resolution['location_status'] == 'not_found':
                skipped_optional_attachments.append(item['title'])
                continue
            if branch_state == 'ambiguous' and resolution['location_status'] == 'not_found':
                resolution['location_status'] = 'ambiguous'
            section = resolution['section']
            result = self._evaluate_attachment(item, section, deadline, bidder_name)
            result['location_status'] = resolution['location_status']
            result['location_candidates'] = resolution.get('candidates', []) if section is None else []
            result["date_check"]["deadline_resolution"] = deadline_resolution
            if deadline is None:
                result["date_check"]["deadline_locations"] = deadline_resolution["locations"]
            if resolution['location_status'] == 'ambiguous':
                result['found'] = None
                result['status'] = 'pending'
                for component in ('date_check', 'seal_check', 'signature_check'):
                    result[component]['status'] = 'pending'
                results.append(result)
                continue
            if (
                not result["found"]
                and item.get('requirements', {}).get('applicability_status') == 'unclear'
            ):
                result['found'] = None
                result['status'] = 'pending'
                result['applicability_status'] = 'unclear'
                result['condition_text'] = item.get('requirements', {}).get('condition_text') or ''
                for component in ('date_check', 'seal_check', 'signature_check'):
                    result[component]['status'] = 'pending'
                results.append(result)
                continue
            if not result["found"]:
                if item.get('requirements', {}).get('optionality_conflict'):
                    results.append(result)
                    continue
                if bool(item.get("requirements", {}).get("is_optional")):
                    skipped_optional_attachments.append(result["title"])
                    continue
                skipped_missing_attachments.append(result["title"])
                missing_attachment_results.append(result)
                continue
            results.append(result)
            if result["signature_check"]["status"] in {"fail", "missing"}:
                missing_signatures.append(result["title"])
            if result["signature_check"]["status"] == "pending":
                pending_signatures.append(result["title"])
            if result["seal_check"]["status"] in {"missing"}:
                missing_seals.append(result["title"])
            if result["date_check"]["status"] == "missing_date":
                missing_dates.append(result["title"])
            if result["date_check"]["status"] == "late":
                late_dates.append(result["title"])

        checked_count = len(results)
        has_missing = bool(skipped_missing_attachments or missing_signatures or missing_seals or missing_dates)
        if late_dates:
            compliance_status = "fail"
        elif has_missing:
            compliance_status = "missing"
        elif checked_count <= 0 or deadline is None:
            compliance_status = "missing"
        elif any(x["status"] == "fail" for x in results):
            compliance_status = "fail"
        elif any(x["status"] == "pending" for x in results):
            compliance_status = "pending"
        else:
            compliance_status = "pass"

        date_status = "missing_deadline" if deadline is None else ("fail" if late_dates else ("missing" if missing_dates else "pass"))
        position_status = "missing" if skipped_missing_attachments or missing_signatures or missing_seals else ("pending" if pending_signatures else "pass")
        deadline_locations = deadline.get("locations") if isinstance(deadline, dict) else []
        return refresh_verification_summary({
            "deadline_resolution": deadline_resolution,
            "mode": "tender_vs_bid",
            "seal_detected": seal_bundle["detected"],
            "seal_count": seal_bundle["count"],
            "seal_contents": seal_bundle["texts"],
            "signature_detected": signature_bundle["detected"],
            "signature_count": signature_bundle["count"],
            "signature_contents": signature_bundle["texts"],
            "bidder_name": bidder_name,
            "bidder_identity": identity,
            "required_attachment_count": len(required),
            "checked_attachment_count": checked_count,
            "required_attachments": [x["title"] for x in required],
            "skipped_missing_attachments": skipped_missing_attachments,
            "skipped_optional_attachments": skipped_optional_attachments,
            "missing_attachment_results": missing_attachment_results,
            "attachment_results": results,
            "position_check": {"status": position_status, "missing_attachments": skipped_missing_attachments, "missing_signature_attachments": missing_signatures, "pending_signature_attachments": pending_signatures, "missing_seal_attachments": missing_seals},
            "date_check": {"status": date_status, "deadline_date": deadline["date"].isoformat() if deadline else None, "matched_deadline_text": deadline["text"] if deadline else None, "matched_deadline_page": deadline.get("page") if isinstance(deadline, dict) else None, "deadline_locations": deadline_locations, "missing_date_attachments": missing_dates, "late_date_attachments": late_dates},
            "deadline_check": {"status": date_status, "deadline_date": deadline["date"].isoformat() if deadline else None, "matched_deadline_text": deadline["text"] if deadline else None, "matched_deadline_page": deadline.get("page") if isinstance(deadline, dict) else None, "deadline_locations": deadline_locations, "source": "tender_document"},
            "seal_company_check": self._seal_company_check(bidder_name, seal_bundle["texts"]),
            "compliance_status": compliance_status,
        })

    def _check_single(self, payload: Any) -> dict:
        document = self._as_document(payload) or {}
        text = self._text(document)
        seal_bundle = self._seal_bundle(document)
        signature_bundle = self._signature_bundle(document)
        signatures = self._signature_values(text)
        sign_date = self._section_date(text, self._sections(document))
        has_signature = bool(signatures or signature_bundle["detected"])
        signature_status = "pending"  # No tender requirement: observation cannot establish compliance.
        seal_company_check = self._seal_company_check(self._bidder_name(document, seal_bundle["texts"]), seal_bundle["texts"])
        return {
            "mode": "single_document",
            "summary": "未提供招标要求，仅展示签字、盖章和日期线索，合规结论待复核。",
            "seal_detected": seal_bundle["detected"],
            "seal_count": seal_bundle["count"],
            "seal_contents": seal_bundle["texts"],
            "signature_detected": has_signature,
            "signature_count": max(len(signatures), signature_bundle["count"]),
            "signature_contents": signature_bundle["texts"] or list(dict.fromkeys([x.get("value") for x in signatures if isinstance(x, dict) and x.get("value")])),
            "bidder_name": self._bidder_name(document, seal_bundle["texts"]),
            "bidder_identity": self._bidder_identity(document),
            "required_attachment_count": 0,
            "required_attachments": [],
            "attachment_results": [],
            "position_check": {"status": signature_status if seal_bundle["detected"] or signature_status != "missing" else "missing", "missing_attachments": [], "missing_signature_attachments": [] if has_signature or signature_status == "pending" else ["全文未识别到有效签字内容或签字区域"], "pending_signature_attachments": ["全文识别到签字位或签字区域，但未回填出稳定签名文本，建议人工复核"] if signature_status == "pending" else [], "missing_seal_attachments": [], "pending_seal_attachments": [] if seal_bundle["detected"] else ["全文未定位到盖章，需对照原件核验"]},
            "date_check": {"status": "pass" if sign_date else "missing_date", "deadline_date": None, "matched_deadline_text": None, "missing_date_attachments": [] if sign_date else ["全文未识别到落款日期"], "late_date_attachments": []},
            "deadline_check": {"status": "not_applicable", "deadline_date": None, "matched_deadline_text": None, "source": None},
            "seal_company_check": seal_company_check,
            "compliance_status": "pending",
        }

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
                coordinate_system = str(item.get("coordinate_system") or "").strip()
                raw_bbox = item.get("bbox") or item.get("box")
                if coordinate_system.lower() == "pdf_point":
                    if raw_bbox is not None:
                        section["bbox"] = raw_bbox
                    section["coordinate_system"] = coordinate_system
                else:
                    bbox = self._normalize_bbox(raw_bbox)
                    if bbox is not None:
                        section["bbox"] = bbox
                section["_synthetic"] = bool(item.get("_synthetic"))
                if item.get("lines"):
                    section["lines"] = item["lines"]
                if item.get("bbox_format"):
                    section["bbox_format"] = item["bbox_format"]
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

    def _layout_box_xywh(self, section: dict) -> list[int] | None:
        """Layout bbox is xyxy; detection locations use xywh. Convert only for geometry."""
        bbox = section.get("bbox")
        if bbox is None:
            return self._normalize_bbox(section.get("box"))
        if (isinstance(bbox, (list, tuple)) and len(bbox) == 4
                and all(isinstance(x, (int, float)) for x in bbox)):
            left, top, right, bottom = [int(round(x)) for x in bbox]
            if section.get("bbox_format") == "xywh" or right < left or bottom < top:
                return self._normalize_bbox(bbox)
            return [left, top, right - left, bottom - top]
        return self._normalize_bbox(bbox)

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
        for source in (signature_node.get("texts"), container.get("signature_texts"), [s["text"] for s in self._sections(payload) if s["type"] == "signature" and not s.get("_synthetic")]):
            if isinstance(source, str):
                source = [source]
            if isinstance(source, (list, tuple, set)):
                signature_texts.extend(str(x).strip() for x in source if str(x).strip())
        for source in (
            signature_node.get("locations"),
            container.get("signature_locations"),
            [{"page": s.get("page"), "box": s.get("bbox")} for s in self._sections(payload) if s["type"] == "signature" and not s.get("_synthetic") and s.get("bbox")],
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

    def _is_pure_date_line(self, text: str) -> bool:
        """识别只包含日期本身的独立落款行。"""
        plain = str(text or "").strip()
        if not plain:
            return False
        normalized = re.sub(r"[\s　]+", "", plain)
        for pattern in self.DATE_PATTERNS:
            if re.fullmatch(pattern, normalized):
                return True
        return False

    def _is_ocr_damaged_date_field_line(self, text: str) -> bool:
        """识别印章遮挡后“日期”仅剩“期”的落款行。"""
        plain = str(text or "").strip()
        if not re.match(r"^期\s*[:：]", plain):
            return False
        return bool(self._date_candidates(plain))

    def _is_blocked_sign_date_context(self, text: str) -> bool:
        compact = self._compact(text)
        if not compact:
            return False
        return any(marker in compact for marker in self.DATE_CONTEXT_EXCLUDE_MARKERS)

    def resolve_deadline(self, payload: dict | None) -> dict:
        from .verification_evidence import resolve_deadline
        return resolve_deadline(self, payload)

    def _deadline_from_doc(self, payload: dict | None) -> dict | None:
        resolution = self.resolve_deadline(payload)
        selected = resolution.get("selected")
        if selected is None:
            return None
        return {**selected, "date": date.fromisoformat(selected["date"]),
                "locations": resolution["locations"], "resolution_evidence": resolution}

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
        return re.sub(r"[–—－]", "-", re.sub(r"\s+", "", match.group("number"))) if match else None

    def _attachment_title(self, text: str) -> str:
        text = str(text or "").strip()
        idx = text.find("附件")
        # 没有“附件X”时，也要把多级编号前缀去干净，避免 7.2/8.1.2 残留数字影响匹配。
        text = text[idx:] if idx >= 0 else self._strip_attachment_title_prefix(text)
        return re.sub(r"\s+", " ", text).strip("：:；;，,。")

    def _strip_attachment_title_prefix(self, text: str) -> str:
        value = str(text or "").strip()
        previous = None
        while value and value != previous:
            previous = value
            for pattern in self.ATTACHMENT_TITLE_PREFIX_PATTERNS:
                value = re.sub(pattern, "", value).strip()
        return value

    def _raw_attachment_title_key(self, text: str) -> str:
        title = self._strip_attachment_title_prefix(self._attachment_title(text))
        title = strip_attachment_title_parenthetical_noise(title)
        title = re.split(r"[；;。]", title, maxsplit=1)[0]
        title = re.sub(r"[（(][^）)]{0,30}(?:格式|自拟|如有|说明|盖章|签字|样式|模板|原件|复印件)[^）)]*[）)]", "", title)
        for pattern, repl in (
            (r"法定代表人资格证明书", "法定代表人证明书"),
            (r"法定代表人身份证明", "法定代表人证明书"),
            (r"单位负责人身份证明", "单位负责人证明书"),
            (r"授权委托书及被授权人身份证", "授权委托书"),
        ):
            title = re.sub(pattern, repl, title)
        for pattern in self.ATTACHMENT_TITLE_NOISE_PATTERNS:
            title = re.sub(pattern, "", title)
        title = re.sub(r"[^\u4e00-\u9fa5A-Za-z0-9]", "", title)
        return title.strip("：:；;，,。")

    def _attachment_title_key(self, text: str) -> str:
        title = self._raw_attachment_title_key(text)
        title = canonicalize_attachment_title(title)
        title = re.sub(r"[^\u4e00-\u9fa5A-Za-z0-9]", "", title)
        return title.strip("：:；;，,。")

    def _attachment_title_match_keys(self, text: str) -> set[str]:
        """生成标题匹配 key，兼容“声明函或授权书”这类二选一模板标题。"""
        raw_title = self._strip_attachment_title_prefix(self._attachment_title(text))
        raw_title = re.split(r"[；;。]", raw_title, maxsplit=1)[0]
        keys = {self._attachment_title_key(raw_title)}
        # 合并要求项（如“中小企业声明函（格式） 中小企业声明函（工程）”）：
        # 按空白拆出的子标题也各自生成 key，任一命中即视为匹配。
        for part in re.split(r"\s+", raw_title):
            # A spaced page/section number such as "一 24 公司" is not a
            # sub-title. Short fragments can otherwise match a common synonym.
            if len(self._raw_attachment_title_key(part)) < 4:
                continue
            part_key = self._attachment_title_key(part)
            if part_key:
                keys.add(part_key)
        if "或" in raw_title:
            for part in re.split(r"\s*或\s*", raw_title):
                part_key = self._attachment_title_key(part)
                if part_key:
                    keys.add(part_key)
        return {key for key in keys if key}

    def _attachment_titles_compatible(self, expected_title: str, actual_title: str) -> bool:
        from .attachment_resolution import compatible
        return compatible(self, expected_title, actual_title)

    def _attachment_title_hints(self, attachments: list[dict] | None = None) -> list[dict]:
        from .attachment_resolution import form_title
        result, seen = [], set()
        for item in attachments or []:
            title = form_title(self._attachment_title(item.get("title")))
            title_key = self._raw_attachment_title_key(title)
            if not title_key or title_key in seen:
                continue
            seen.add(title_key)
            result.append({"attachment_number": item.get("attachment_number"), "title": title, "title_key": title_key})
        for title in self.COMMON_ATTACHMENT_TITLES:
            title_key = self._raw_attachment_title_key(title)
            if not title_key or title_key in seen:
                continue
            seen.add(title_key)
            result.append({"attachment_number": None, "title": title, "title_key": title_key})
        return result

    def _expected_attachment(self, text: str, expected_attachments: list[dict] | None = None) -> dict | None:
        # 先按标题精确命中，避免后文合同/说明里的“附件1/附件2”把正式附件串号。
        attachment_number = self._attachment_number(text)
        raw_title_key = self._raw_attachment_title_key(text)
        if (
            attachment_number is None
            and raw_title_key in self.GENERIC_UNNUMBERED_ATTACHMENT_TITLE_KEYS
        ):
            return None
        title_key = self._attachment_title_key(text)
        matched_by_title = None
        matched_by_number = None
        for item in expected_attachments or []:
            expected_number = item.get("attachment_number")
            expected_title_key = str(item.get("title_key") or "").strip()
            expected_title = str(item.get("title") or "")
            if title_key and (
                self._attachment_titles_compatible(expected_title, text)
            ):
                if attachment_number and expected_number == attachment_number:
                    return item
                matched_by_title = matched_by_title or item
            if attachment_number and expected_number == attachment_number:
                matched_by_number = matched_by_number or item
        if matched_by_title is not None:
            return matched_by_title
        # 只有标题 key 缺失时，才退回纯附件编号匹配。
        if attachment_number and not title_key:
            return matched_by_number
        # 标题 key 不兼容但词法相近（表/介绍、一拆多等变体）也对应同一提示——与完整性口径一致。
        if matched_by_title is None and matched_by_number is None:
            from app.service.analysis.compliance.structured_consistency import lexical_similarity

            best, best_score = None, 0.0
            for item in expected_attachments or []:
                expected_title = str(item.get("title") or item.get("title_key") or "")
                if not expected_title:
                    continue
                from .attachment_resolution import form_kind
                if form_kind(text) and form_kind(expected_title) and form_kind(text) != form_kind(expected_title):
                    continue
                score = lexical_similarity(self._raw_attachment_title_key(expected_title), self._raw_attachment_title_key(text))
                if score >= 0.5 and score > best_score:
                    best_score, best = score, item
            if best is not None:
                return best
        return None

    def _is_attachment_heading(self, section: dict, expected_attachments: list[dict] | None = None) -> bool:
        if section.get("type") not in {"heading", "text"}:
            return False
        text = str(section.get("text") or "").strip()
        if not text or self._catalog_like(text):
            return False
        if re.match(r'^\s*(?:粘贴|黏贴|贴此处)', text):
            return False
        # 落款字段不能因与附件名称共享“响应/法定代表人”等词而切断表单。
        # 正式表名可带“（须加盖公章）”等填写要求，仍保留为标题候选。
        undecorated = re.sub(r"[（(][^）)]*[）)]", "", text).strip()
        if re.search(r"签字|签名|签章|盖章|公章", text) and not re.search(
            r"(?:书|表|函|证明|清单|报告|凭证|许可证)\s*$", undecorated
        ):
            return False
        matched_expected = self._expected_attachment(text, expected_attachments)
        attachment_number = self._attachment_number(text)
        compact = self._compact(text)
        if attachment_number is not None:
            if not TemplateExtractor._explicit_template_heading(self._attachment_title(text)):
                return False
            attachment_index = compact.find("附件")
            if attachment_index < 0 or attachment_index > 12:
                return False
            if len(compact) > 120 and section.get("type") != "heading":
                return False
            return matched_expected is not None if expected_attachments else True
        if expected_attachments is None:
            return False
        if len(compact) > (120 if section.get("type") == "heading" else 36):
            return False
        if matched_expected is None:
            return False
        if section.get("type") == "heading":
            return True
        return self._looks_like_attachment_text_title(text)

    def _looks_like_attachment_text_title(self, text: str) -> bool:
        compact = self._compact(text)
        if not compact or len(compact) > 36:
            return False
        if any(mark in compact for mark in ("根据", "应当提供", "应提供", "须提交", "须提供", "提交", "详见", "参见", "说明如下", "如果", "为准", "报价相等", "修正总价")):
            return False
        if any(mark in text for mark in ("。", "；", ";", "：", ":")):
            return False
        return True

    def _response_format_sections(self, tender_payload: dict | None) -> list[dict]:
        payload = self._as_document(tender_payload) or {}
        shared_sections = TemplateExtractor._response_format_sections(payload)
        if shared_sections:
            return shared_sections

        container = self._container(payload)
        sections, _ = TemplateExtractor.preprocess_sections(
            container.get("layout_sections", []),
            container.get("logical_tables", []),
            is_template=True,
        )
        if not sections:
            return []

        start_index = None
        for idx, section in enumerate(sections):
            compact = self._compact(section.get("text") or "")
            if any(marker in compact for marker in self.RESPONSE_FORMAT_ZONE_MARKERS):
                start_index = idx
                break
        if start_index is None:
            for idx, section in enumerate(sections):
                compact = self._compact(section.get("text") or "")
                if any(marker in compact for marker in self.RESPONSE_FORMAT_CHAPTER_MARKERS):
                    start_index = idx
                    break
        if start_index is None:
            return []

        end_index = len(sections)
        for idx in range(start_index + 1, len(sections)):
            section = sections[idx]
            if str(section.get("type") or "").strip().lower() != "heading":
                continue
            text = str(section.get("text") or "").strip()
            compact = self._compact(text)
            if self.CHAPTER_HEADING_RE.match(text) and not any(
                marker in compact for marker in self.RESPONSE_FORMAT_CHAPTER_MARKERS
            ):
                end_index = idx
                break
        return sections[start_index:end_index]

    def _is_embedded_response_format_heading(self, section: dict) -> bool:
        if str(section.get("type") or "").strip().lower() != "heading":
            return False
        text = str(section.get("text") or "").strip()
        compact = self._compact(text)
        if not compact or self._catalog_like(text):
            return False
        if "附件" in compact or len(compact) > 40:
            return False
        if "格式" not in compact:
            return False
        return any(marker in compact for marker in self.RESPONSE_FORMAT_EMBEDDED_TITLE_MARKERS)

    def _is_response_format_attachment_heading(self, section: dict) -> bool:
        if TemplateExtractor._is_response_format_attachment_heading(section):
            return True
        if self._is_attachment_heading(section):
            return True
        return self._is_embedded_response_format_heading(section)

    def _attachment_heading_identity(
        self,
        text: str,
        expected_attachments: list[dict] | None = None,
    ) -> str:
        """将附件标题归一为稳定标识，用于合并同一附件的多层标题。"""
        matched_expected = self._expected_attachment(text, expected_attachments)
        if matched_expected is not None:
            identity = (
                matched_expected.get("attachment_number")
                or matched_expected.get("title_key")
                or self._attachment_title_key(matched_expected.get("title") or "")
            )
            if identity:
                return str(identity)
        attachment_number = self._attachment_number(text)
        if attachment_number:
            return str(attachment_number)
        return self._attachment_title_key(text)

    def _can_merge_attachment_heading_gap(self, gap_sections: list[dict]) -> bool:
        """同一附件连续出现“章节标题 + 表单标题”时，视为同一附件起点。"""
        for section in gap_sections:
            text = str(section.get("text") or "").strip()
            section_type = str(section.get("type") or "").strip().lower()
            if not text:
                continue
            if section_type in {"heading", "seal", "signature"}:
                continue
            if self._catalog_like(text):
                continue
            return False
        return True

    def _is_page_title_like(self, section: dict) -> bool:
        text = str(section.get("text") or "").strip()
        compact = self._compact(text)
        if not compact:
            return False
        section_type = str(section.get("type") or "").strip().lower()
        if section_type == "heading":
            return True
        if section_type in {"seal", "signature"}:
            return False
        if len(compact) <= 42:
            return True
        if len(compact) > 96:
            return False
        bbox = self._normalize_bbox(section.get("bbox") or section.get("box"))
        return bool(bbox and bbox[1] <= 260)

    def _is_attachment_scope_stop_section(
        self,
        section: dict,
        current_title: str,
        expected_attachments: list[dict] | None = None,
    ) -> bool:
        text = str(section.get("text") or "").strip()
        compact = self._compact(text)
        if not compact or self._catalog_like(text):
            return False
        if str(section.get("type") or "").strip().lower() in {"seal", "signature"}:
            return False

        current_key = self._attachment_title_key(current_title)
        current_number = self._attachment_number(current_title)
        attachment_number = self._attachment_number(text)
        # Numbered continuation fields such as “4. 其他需要说明的情况：”
        # are body content, including when normal pagination moves them to a new page.
        if (section.get('type') == 'text' and attachment_number is None
                and re.search(r'[:：；;。]', text)
                and not self._is_attachment_heading(section, expected_attachments)):
            return False
        # A wrapped reference to another form is still body text.
        if '附件' in compact and any(marker in compact for marker in ('此表', '须与', '详见', '参见')):
            return False
        if attachment_number is not None and not TemplateExtractor._explicit_template_heading(self._attachment_title(text)):
            return False
        current_expected = self._expected_attachment(current_title, expected_attachments)
        candidate_expected = self._expected_attachment(text, expected_attachments)
        if current_expected is not None and candidate_expected is not None:
            current_identity = (
                current_expected.get("attachment_number")
                or current_expected.get("title_key")
            )
            candidate_identity = (
                candidate_expected.get("attachment_number")
                or candidate_expected.get("title_key")
            )
            # “章节说明标题 + 正式表单标题”可能措辞不同，但只要都明确指向
            # 同一招标附件，就仍属于当前附件，不能在正式标题处截断。
            if current_identity and current_identity == candidate_identity:
                return False
        if attachment_number is not None:
            matched_expected = candidate_expected
            if matched_expected is None:
                return True
            matched_number = matched_expected.get("attachment_number")
            matched_key = str(matched_expected.get("title_key") or "").strip()
            if current_number and matched_number == current_number:
                return False
            if current_key and matched_key == current_key:
                return False
            return True

        # A continued deviation row may mention an invoice/payment. It is still
        # table content, not a new invoice attachment that terminates this form.
        if '偏离表' in current_title and section.get('type') == 'table':
            return False
        if not self._is_page_title_like(section):
            return False

        title_key = self._attachment_title_key(text)
        if title_key and current_key and title_key == current_key:
            return False
        common_title_keys = {self._attachment_title_key(title) for title in self.COMMON_ATTACHMENT_TITLES}
        if title_key and title_key in common_title_keys:
            return True
        return any(marker in compact for marker in self.ATTACHMENT_SCOPE_STOP_MARKERS)

    def _effective_attachment_check_chunk(
        self,
        chunk: list[dict],
        expected_attachments: list[dict] | None = None,
    ) -> list[dict]:
        if not chunk:
            return []
        title = str(chunk[0].get("text") or "").strip()
        start_page = chunk[0].get("page") if isinstance(chunk[0].get("page"), int) else None
        effective: list[dict] = []
        for index, section in enumerate(chunk):
            page = section.get("page") if isinstance(section.get("page"), int) else None
            same_start_page = start_page is not None and page == start_page
            section_type = str(section.get("type") or "").strip().lower()
            if (
                index > 0
                and (not same_start_page or section_type == "heading")
                and self._is_attachment_scope_stop_section(section, title, expected_attachments)
            ):
                break
            effective.append(section)
        return effective or chunk[:1]

    def _scoped_bid_section_for_checks(self, bid_section: dict | None) -> dict | None:
        if not isinstance(bid_section, dict):
            return bid_section
        check_sections = bid_section.get("check_sections")
        if not isinstance(check_sections, list) or not check_sections:
            return bid_section
        scoped = dict(bid_section)
        scoped["sections"] = check_sections
        scoped["pages"] = bid_section.get("check_pages") or bid_section.get("pages") or []
        scoped["text"] = bid_section.get("check_text") or bid_section.get("text") or ""
        scoped["seal_texts"] = bid_section.get("check_seal_texts") or []
        scoped["signature_texts"] = bid_section.get("check_signature_texts") or []
        scoped["seal_locations"] = bid_section.get("check_seal_locations") or []
        scoped["signature_locations"] = bid_section.get("check_signature_locations") or []
        return scoped

    def _extract_response_format_attachments(self, tender_payload: dict | None) -> list[dict]:
        payload = self._as_document(tender_payload) or {}
        shared_attachments = TemplateExtractor.extract_response_format_attachments(payload)
        if shared_attachments:
            normalized: list[dict] = []
            for item in shared_attachments:
                if not isinstance(item, dict):
                    continue
                next_item = dict(item)
                next_item["title_locations"] = list(
                    item.get("title_locations") or item.get("locations") or []
                )
                normalized.append(next_item)
            return normalized

        sections = self._response_format_sections(payload)
        if not sections:
            return []

        starts = [
            idx
            for idx, section in enumerate(sections)
            if self._is_response_format_attachment_heading(section)
        ]
        attachments: list[dict] = []
        for pos, start in enumerate(starts):
            end = starts[pos + 1] if pos + 1 < len(starts) else len(sections)
            chunk = self._effective_attachment_check_chunk(sections[start:end])
            if not chunk:
                continue
            title = str(chunk[0].get("text") or "").strip()
            content = [
                str(section.get("text") or "").strip()
                for section in chunk
                if str(section.get("text") or "").strip()
            ]
            title_locations = TemplateExtractor._section_locations(chunk[0])
            locations = [
                location
                for section in chunk
                for location in TemplateExtractor._section_locations(section)
            ]
            attachments.append(
                {
                    "attachment_number": self._attachment_number(title),
                    "title": self._attachment_title(title),
                    "content": content,
                    "pages": list(
                        dict.fromkeys(
                            section.get("page")
                            for section in chunk
                            if section.get("page") is not None
                        )
                    ),
                    "title_locations": title_locations,
                    "locations": locations,
                }
            )
        return attachments

    def _is_composite_response_attachment(self, title: str) -> bool:
        compact = self._compact(title)
        return any(marker in compact for marker in self.RESPONSE_FORMAT_COMPOSITE_TITLE_MARKERS)

    def _attachment_sections(self, payload: dict | None, seal_locations: list[dict] | None = None, signature_locations: list[dict] | None = None, expected_attachments: list[dict] | None = None) -> list[dict]:
        from .attachment_resolution import candidate_view
        sections = candidate_view(self, TemplateExtractor._template_boundary_sections(self._sections(payload)), expected_attachments or [])
        raw_starts = [i for i, x in enumerate(sections) if self._is_attachment_heading(x, expected_attachments)]
        starts: list[int] = []
        for start in raw_starts:
            if not starts:
                starts.append(start)
                continue
            prev_start = starts[-1]
            prev_text = str(sections[prev_start].get("text") or "").strip()
            curr_text = str(sections[start].get("text") or "").strip()
            prev_identity = self._attachment_heading_identity(prev_text, expected_attachments)
            curr_identity = self._attachment_heading_identity(curr_text, expected_attachments)
            gap_sections = sections[prev_start + 1:start]
            # 同一附件的连续多层标题不应切成两个附件，否则正文会被截断成只剩标题。
            if prev_identity and prev_identity == curr_identity and self._can_merge_attachment_heading_gap(gap_sections):
                continue
            # Explicit numbered chapter/subform headings may repeat a shortened
            # name (7. 近三年…项目业绩清单 / 7.1 项目业绩清单).
            parent = re.match(r'^\s*(\d+(?:\.\d+)*)[.、．]\s*', prev_text)
            child = re.match(r'^\s*(\d+(?:\.\d+)+)\s*[.、．]?\s*', curr_text)
            if parent and child and child.group(1).startswith(parent.group(1) + '.'):
                from .attachment_resolution import form_kind
                prev_key, curr_key = self._raw_attachment_title_key(prev_text), self._raw_attachment_title_key(curr_text)
                if (len(curr_key) >= 4 and curr_key in prev_key
                        and form_kind(prev_text) == form_kind(curr_text)
                        and self._can_merge_attachment_heading_gap(gap_sections)):
                    continue
            starts.append(start)
        result = []
        seal_locations = seal_locations or []
        signature_locations = signature_locations or []
        for pos, start in enumerate(starts):
            end = starts[pos + 1] if pos + 1 < len(starts) else len(sections)
            chunk = sections[start:end]
            title = chunk[0]["text"]
            matched_expected = self._expected_attachment(title, expected_attachments)
            attachment_number = self._attachment_number(title)
            normalized_title = self._attachment_title(title)
            expected_title = None
            expected_attachment_number = None
            if matched_expected is not None:
                expected_title = matched_expected.get("title")
                expected_attachment_number = matched_expected.get("attachment_number")
                if attachment_number is None:
                    attachment_number = expected_attachment_number
            pages = list(dict.fromkeys(x["page"] for x in chunk if x.get("page") is not None))
            check_chunk = self._effective_attachment_check_chunk(chunk, expected_attachments)
            check_pages = list(dict.fromkeys(x["page"] for x in check_chunk if x.get("page") is not None))
            local_seal_locations = [{"page": x.get("page"), "box": self._layout_box_xywh(x)} for x in chunk if x["type"] == "seal" and x.get("bbox")]
            local_seal_locations.extend(item for item in seal_locations if item.get("page") in pages)
            local_signature_locations = [{"page": x.get("page"), "box": self._layout_box_xywh(x)} for x in chunk if x["type"] == "signature" and x.get("bbox")]
            local_signature_locations.extend(item for item in signature_locations if item.get("page") in pages)
            local_check_seal_locations = [{"page": x.get("page"), "box": self._layout_box_xywh(x)} for x in check_chunk if x["type"] == "seal" and x.get("bbox")]
            local_check_seal_locations.extend(item for item in seal_locations if item.get("page") in check_pages)
            local_check_signature_locations = [{"page": x.get("page"), "box": self._layout_box_xywh(x)} for x in check_chunk if x["type"] == "signature" and x.get("bbox")]
            local_check_signature_locations.extend(item for item in signature_locations if item.get("page") in check_pages)
            result.append(
                {
                    "attachment_number": attachment_number,
                    "title": normalized_title,
                    "expected_title": expected_title,
                    "is_container": pos + 1 < len(starts)
                    and (bool(re.match(r'^(?:第[一二三四五六七八九十]+章|[一二三四五六七八九十A-Z]+[、.])', title))
                         and bool(re.match(r'^[（(][一二三四五六七八九十\d]+[）)]', sections[starts[pos + 1]]['text']))
                         or chunk[0].get('source_block_index') is not None
                         and chunk[0].get('source_block_index') == sections[starts[pos + 1]].get('source_block_index'))
                    and all(
                        x.get('type') == 'heading' or not str(x.get('text') or '').strip()
                        for x in check_chunk),
                    "expected_attachment_number": expected_attachment_number,
                    "pages": pages,
                    "check_pages": check_pages,
                    "text": "\n".join(x["text"] for x in chunk if x["type"] != "seal"),
                    "check_text": "\n".join(x["text"] for x in check_chunk if x["type"] != "seal"),
                    "seal_texts": [x["text"] for x in chunk if x["type"] == "seal"],
                    "check_seal_texts": [x["text"] for x in check_chunk if x["type"] == "seal"],
                    "signature_texts": [x["text"] for x in chunk if x["type"] == "signature"],
                    "check_signature_texts": [x["text"] for x in check_chunk if x["type"] == "signature"],
                    "sections": chunk,
                    "check_sections": check_chunk,
                    "seal_locations": self._dedupe_locations(local_seal_locations),
                    "check_seal_locations": self._dedupe_locations(local_check_seal_locations),
                    "signature_locations": self._dedupe_locations(local_signature_locations),
                    "check_signature_locations": self._dedupe_locations(local_check_signature_locations),
                }
            )
        return result

    def _required_attachments(
        self,
        tender_payload: dict | None,
        bid_by_no: dict[str, list[dict]] | None = None,
        bid_sections: list[dict] | None = None,
    ) -> list[dict]:
        result, seen = [], set()
        bid_by_no = bid_by_no or {}
        bid_sections = bid_sections or []
        response_attachments, _ = TemplateExtractor.filter_business_response_attachments(
            tender_payload or {},
            self._extract_response_format_attachments(tender_payload),
        )
        for item in response_attachments:
            title = self._attachment_title(item.get("title"))
            text = "\n".join(item.get("content") or [])
            template_req = self._requirements(title, text)
            template_req['optionality_conflict'] = bool(item.get('optionality_conflict'))
            template_req['optionality_locations'] = list(item.get('optionality_locations') or [])
            template_req['is_optional'] = (template_req['is_optional'] or bool(item.get('is_optional'))) and not template_req['optionality_conflict']
            template_req['applicability_status'] = str(item.get('applicability_status') or 'required')
            template_req['condition_text'] = str(item.get('condition_text') or '')
            template_req['applicability_locations'] = list(item.get('applicability_locations') or [])
            attachment_number = self._attachment_number(title)
            key = attachment_number or title
            if key in seen:
                continue
            seen.add(key)
            # “组成及部分格式”这类总说明页不直接作为待检附件。
            if self._is_composite_response_attachment(title):
                continue
            probe_attachment = {
                "attachment_number": attachment_number,
                "title": title,
                "text": text,
                "requirements": template_req,
            }
            bid_section = self._match_attachment(probe_attachment, bid_by_no, bid_sections)
            if bid_section is None and any(marker in title for marker in self.EXCLUDE_ATTACHMENTS):
                continue
            result.append(
                {
                    "attachment_number": attachment_number,
                    "title": title,
                    "text": text,
                    "requirements": template_req,
                    "template_locations": list(
                        item.get("title_locations") or item.get("locations") or []
                    )[:6],
                }
            )
        return result

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
        if not compact or any(x in compact for x in blocked) or self._is_blocked_sign_date_context(compact):
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

    def _resolve_attachment(self, attachment: dict, all_sections: list[dict]) -> dict:
        from .attachment_resolution import resolve
        return resolve(self, attachment, all_sections)

    def _match_attachment(self, attachment: dict, bid_by_no: dict[str, list[dict]], all_sections: list[dict]) -> dict | None:
        return self._resolve_attachment(attachment, all_sections)['section']

    def _evaluate_attachment(self, attachment: dict, bid_section: dict | None, deadline: dict | None, bidder_name: str | None) -> dict:
        seal_check = self._seal_check(attachment, bid_section, bidder_name)
        date_check = self._date_check(attachment, bid_section, deadline)
        signature_check = self._signature_check(attachment, bid_section, seal_check, date_check)
        return project_attachment({
            "title": attachment["title"],
            "attachment_number": attachment.get("attachment_number"),
            "found": bid_section is not None,
            "matched_bid_title": bid_section["title"] if bid_section else None,
            "pages": bid_section["pages"] if bid_section else [],
            "check_pages": bid_section.get("check_pages") if bid_section else [],
            "locations": self._attachment_heading_locations(bid_section),
            "template_locations": list(attachment.get("template_locations") or [])[:6],
            "requirements": attachment["requirements"],
            "signature_check": signature_check,
            "seal_check": seal_check,
            "date_check": date_check,
        })

    def _attachment_heading_locations(self, bid_section: dict | None) -> list[dict]:
        if not isinstance(bid_section, dict):
            return []
        locations: list[dict] = []
        title = str(bid_section.get("title") or "").strip()
        for section in bid_section.get("sections") or []:
            if not isinstance(section, dict):
                continue
            text = str(section.get("text") or "").strip()
            page = section.get("page") if isinstance(section.get("page"), int) else None
            bbox = self._normalize_bbox(section.get("bbox") or section.get("box"))
            if page is None or not text:
                continue
            locations.append(
                {
                    "page": page,
                    "bbox": bbox,
                    "text": text,
                    "location_precision": section.get('location_precision', 'block'),
                    "source_block_index": section.get('source_block_index'),
                }
            )
            if title and self._attachment_titles_compatible(title, text):
                break
            if len(locations) >= 1:
                break
        return self._dedupe_locations(locations)


    def _signature_check(self, attachment: dict, bid_section: dict | None, seal_check: dict, date_check: dict) -> dict:
        required = int(attachment["requirements"].get("signature_field_count") or 0)
        if required <= 0:
            return {"status": "not_required", "required_count": 0, "filled_count": 0, "pending_count": 0, "filled_values": [], "pending_fields": [], "empty_fields": []}
        if bid_section is None:
            return {"status": "missing", "required_count": required, "filled_count": 0, "pending_count": 0, "filled_values": [], "pending_fields": [], "empty_fields": attachment["requirements"].get("signature_field_examples") or []}
        bid_section = self._scoped_bid_section_for_checks(bid_section) or bid_section

        slots = self._collect_signature_slots(attachment, bid_section)
        filled, pending, empty = [], [], []
        used_signature_keys, used_seal_keys = set(), set()

        for slot in slots:
            direct_value = self._signature_value(slot["line"])
            placeholder_direct_value = (
                direct_value
                if direct_value and self._is_signature_placeholder_value(direct_value)
                else None
            )
            placeholder_nearby_signature = None
            if direct_value and placeholder_direct_value is None:
                filled.append({"line": slot["line"], "page": slot.get("page"), "mode": "text_inline", "value": direct_value})
                continue

            nearby_signature = self._signature_nearby_detected_signature(slot, bid_section)
            if nearby_signature is not None:
                signature_key = self._evidence_key(nearby_signature.get("page"), nearby_signature.get("box"), nearby_signature.get("text") or nearby_signature.get("value"))
                if signature_key in used_signature_keys:
                    nearby_signature = None
                else:
                    used_signature_keys.add(signature_key)
            if nearby_signature is not None:
                if self._is_signature_placeholder_value(nearby_signature.get("text") or nearby_signature.get("value")):
                    placeholder_nearby_signature = nearby_signature
                else:
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
                key = self._evidence_key(nearby_text.get('page'), nearby_text.get('box'), nearby_text.get('text'))
                if key in used_signature_keys:
                    nearby_text = None
                else:
                    used_signature_keys.add(key)
            if nearby_text is not None:
                filled.append({"line": slot["line"], "page": slot.get("page"), "mode": "nearby_text_mark", "value": nearby_text["value"], "evidence_text": nearby_text["text"], "evidence_page": nearby_text["page"]})
                continue

            if placeholder_nearby_signature is not None:
                pending.append({"line": slot["line"], "page": slot.get("page"), "reason": "signature_region_text_unparsed", "presence": "detected", "evidence": placeholder_nearby_signature})
                continue
            pending.append({"line": slot["line"], "page": slot.get("page"), "reason": "signature_not_reliably_located", "presence": "unknown"})
        while len(filled) + len(pending) < required:
            pending.append({"reason": "signature_slot_not_reliably_located", "presence": "unknown"})
        status = "pass" if len(filled) >= required else "pending"
        detected = bool(filled or any(p.get('presence') == 'detected' for p in pending))
        return {"status": status, "presence": "detected" if detected else "unknown",
                "text_status": "parsed" if filled else "unparsed", "required_count": required,
                "filled_count": len(filled), "pending_count": len(pending), "filled_values": filled,
                "pending_fields": pending, "empty_fields": empty}

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
                append_slot(line, section.get("page"), self._layout_box_xywh(section), "section")

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
                if section.get("_synthetic") or section.get("type") not in {"text", "signature"}:
                    continue
                candidate_box = self._layout_box_xywh(section)
                if not self._box_near_signature_slot(slot_box, candidate_box):
                    continue
                candidate_text = str(section.get("text") or "").strip()
                candidate_value = self._normalize_signature_candidate(candidate_text)
                if candidate_text in {"手机", "银行账号", "单位", "人民币"}:
                    continue
                if candidate_value is None:
                    continue
                distance = max((candidate_box[0] if candidate_box else 0) - (slot_box[0] + slot_box[2]), 0) + abs((candidate_box[1] if candidate_box else 0) - slot_box[1])
                score = distance + len(candidate_value) * 4
                if best is None or score < best["score"]:
                    best = {"score": score, "text": candidate_text, "value": candidate_value, "page": section.get("page"), "box": candidate_box}

        if best is not None:
            return best

        # Never bypass a failed geometric match by scanning the flattened text.
        if slot_box is not None:
            return None

        # Without coordinates, only an adjacent value in the same field block is
        # usable; a title on another page is not evidence of a signature.
        field = next((s for s in bid_section.get("sections") or []
                      if s.get("page") == slot_page
                      and slot["line"] in self._lines(s.get("text") or "")), None)
        lines = self._lines((field or {}).get("text") or "")
        for idx, line in enumerate(lines):
            if line != slot["line"]:
                continue
            for offset in (1,):
                neighbor_idx = idx + offset
                if not (0 <= neighbor_idx < len(lines)):
                    continue
                candidate_text = lines[neighbor_idx]
                candidate_value = self._normalize_person_name(candidate_text)
                if candidate_text in {"手机", "银行账号", "单位", "人民币"}:
                    continue
                if candidate_value is not None:
                    return {"text": candidate_text, "value": candidate_value, "page": slot_page}
        return None

    def _signature_nearby_detected_signature(self, slot: dict, bid_section: dict) -> dict | None:
        slot_page = slot.get("page")
        slot_box = self._normalize_bbox(slot.get("bbox"))
        best = None

        for section in bid_section.get("sections") or []:
            if section.get("_synthetic") or str(section.get("type") or "").strip().lower() != "signature":
                continue
            if slot_page is not None and section.get("page") != slot_page:
                continue
            candidate_box = self._layout_box_xywh(section)
            if slot_box is not None and candidate_box is not None and not self._box_near_signature_slot(slot_box, candidate_box, max_dx=420, max_dy=180):
                continue
            candidate_text = str(section.get("text") or "").strip()
            distance = 0
            if slot_box is not None and candidate_box is not None:
                distance = max(candidate_box[0] - (slot_box[0] + slot_box[2]), 0) + abs(candidate_box[1] - slot_box[1])
            score = distance - (60 if self._is_signature_placeholder_value(candidate_text) else 0)
            signature_text = self._normalize_signature_candidate(candidate_text)
            if signature_text is None and candidate_text and not self._is_signature_placeholder_value(candidate_text):
                signature_text = candidate_text
            signature_value = signature_text or self._signature_placeholder_text()
            if best is None or score < best["score"]:
                best = {"score": score, "page": section.get("page"), "box": candidate_box, "text": signature_text, "value": signature_value, "mode": "ocr_signature_section"}

        synthetic_boxes = {(s.get("page"), tuple(self._layout_box_xywh(s) or [])) for s in bid_section.get("sections") or [] if s.get("_synthetic")}
        for item in bid_section.get("signature_locations") or []:
            if (item.get("page"), tuple(item.get("box") or item.get("bbox") or [])) in synthetic_boxes:
                continue
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

        return None

    def _normalize_person_name(self, text: str) -> str | None:
        value = self._normalize_signature_candidate(text)
        if value is None:
            return None
        return value if re.fullmatch(r"[\u4e00-\u9fa5]{2,4}", value) or re.fullmatch(r"[A-Za-z]{2,20}", value) else None


    def _is_company_like_seal_text(self, text: str) -> bool:
        compact = self._compact(text)
        return bool(re.search(r"(有限责任公司|股份有限公司|集团有限公司|有限公司|公司|专用章)$", compact) or "公司" in compact)

    def _official_seal_texts(self, seal_texts: list[str], bidder_name: str | None = None) -> list[str]:
        deduped = list(dict.fromkeys(str(text or "").strip() for text in seal_texts if str(text or "").strip()))
        if not deduped:
            return []
        company_texts = [
            text
            for text in deduped
            if self._is_company_like_seal_text(text)
            or self._extract_company_candidate(text)
            or (bidder_name and self._company_score(bidder_name, text) >= 0.45)
        ]
        if company_texts:
            return company_texts
        return [text for text in deduped if self._normalize_person_name(text) is None]

    def _signature_nearby_personal_seal(self, slot: dict, bid_section: dict) -> dict | None:
        slot_page = slot.get("page")
        slot_box = self._normalize_bbox(slot.get("bbox"))
        if slot_box is None:
            return None
        best = None
        for section in bid_section.get("sections") or []:
            if not isinstance(section, dict):
                continue
            if str(section.get("type") or "").strip().lower() != "seal":
                continue
            section_page = section.get("page")
            if slot_page is not None and section_page != slot_page:
                continue
            seal_text = str(section.get("text") or "").strip()
            person_name = self._normalize_person_name(seal_text)
            if person_name is None or self._is_company_like_seal_text(seal_text):
                continue
            candidate_box = self._layout_box_xywh(section)
            if candidate_box is None:
                continue
            if not self._box_near_signature_slot(slot_box, candidate_box, max_dx=420, max_dy=180):
                continue
            distance = max(candidate_box[0] - (slot_box[0] + slot_box[2]), 0) + abs(candidate_box[1] - slot_box[1])
            if best is None or distance < best["distance"]:
                best = {
                    "distance": distance,
                    "page": section_page,
                    "box": candidate_box,
                    "seal_text": seal_text,
                    "person_name": person_name,
                }
        if best is not None:
            best.pop("distance", None)
            return best
        return None

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
        requires_seal = bool(attachment["requirements"].get("requires_seal"))
        if not requires_seal:
            return {"status": "not_required", "detected": False, "matched": None, "seal_texts": [], "best_match": None}
        if bid_section is None:
            return {"status": "missing", "detected": False, "matched": False, "seal_texts": [], "best_match": None}
        bid_section = self._scoped_bid_section_for_checks(bid_section) or bid_section
        raw_seal_texts = list(dict.fromkeys(str(x).strip() for x in (bid_section.get("seal_texts") or []) if str(x).strip()))
        seal_texts = self._official_seal_texts(raw_seal_texts, bidder_name)
        seal_locations = self._dedupe_locations(bid_section.get("seal_locations") or [])
        textual_seals = self._textual_seal_evidences(bid_section, bidder_name)
        detected = bool(raw_seal_texts or seal_locations)
        matched = False
        best_match = None
        best_match_score = None
        if bidder_name and seal_texts:
            for seal_text in seal_texts:
                score = self._company_score(bidder_name, seal_text)
                if best_match_score is None or score > best_match_score:
                    best_match_score = score
                    best_match = {"bidder_name": bidder_name, "seal_text": seal_text, "score": round(score, 4)}
            matched = bool(
                best_match
                and best_match_score is not None
                and self._seal_company_matches(
                    bidder_name,
                    best_match["seal_text"],
                    best_match_score,
                )
            )
        return {
            "status": "pass" if detected and matched else "pending",
            "presence": "detected" if detected else "unknown",
            "text_status": "parsed" if seal_texts else "unparsed",
            "detected": detected,
            "matched": matched,
            "seal_texts": seal_texts,
            "seal_locations": seal_locations,
            "textual_seal_evidences": textual_seals,
            "best_match": best_match,
            "match_threshold": self.SEAL_COMPANY_MATCH_THRESHOLD,
            "match_operator": ">",
        }

    def _date_check(self, attachment: dict, bid_section: dict | None, deadline: dict | None) -> dict:
        deadline_locations = deadline.get("locations") if isinstance(deadline, dict) else []
        deadline_date_iso = deadline["date"].isoformat() if deadline else None
        deadline_text = deadline["text"] if deadline else None
        deadline_page = deadline.get("page") if isinstance(deadline, dict) else None
        requires_date = bool(attachment["requirements"].get("requires_date"))
        if not requires_date:
            return {"status": "not_required", "sign_date": None, "deadline_date": deadline_date_iso, "matched_sign_text": None, "matched_sign_page": None, "matched_deadline_text": deadline_text, "matched_deadline_page": deadline_page, "deadline_locations": deadline_locations}
        if bid_section is None:
            return {"status": "missing_date", "sign_date": None, "deadline_date": deadline_date_iso, "matched_sign_text": None, "matched_sign_page": None, "matched_deadline_text": deadline_text, "matched_deadline_page": deadline_page, "deadline_locations": deadline_locations}
        bid_section = self._scoped_bid_section_for_checks(bid_section) or bid_section
        section_text = bid_section.get("text") or ""
        section_nodes = bid_section.get("sections") or []
        all_dates = self._section_date_candidates(section_text, section_nodes)
        distinct_dates = {candidate["date"] for candidate in all_dates}
        if len(distinct_dates) == 1:
            # 附件范围内只有一个唯一日期时，不再要求“日期”字段或签章上下文。
            # 优先沿用严格规则的定位信息；严格规则未命中时使用唯一候选。
            sign_date = self._section_date(section_text, section_nodes) or all_dates[0]
            date_match_method = "single_date_fallback"
        elif len(distinct_dates) > 1:
            # 多个不同日期仍按原规则定位落款日期，避免把成立、合同、打印等日期混入。
            sign_date = self._section_date(section_text, section_nodes)
            date_match_method = "contextual_date"
        else:
            sign_date = None
            date_match_method = None
        if deadline is None:
            return {"status": "missing_deadline", "sign_date": sign_date["date"].isoformat() if sign_date else None, "deadline_date": None, "matched_sign_text": sign_date["text"] if sign_date else None, "matched_sign_page": sign_date.get("page") if sign_date else None, "matched_deadline_text": None, "matched_deadline_page": None, "deadline_locations": [], "match_method": date_match_method}
        if sign_date is None:
            return {"status": "missing_date", "sign_date": None, "deadline_date": deadline["date"].isoformat(), "matched_sign_text": None, "matched_sign_page": None, "matched_deadline_text": deadline["text"], "matched_deadline_page": deadline.get("page"), "deadline_locations": deadline_locations}
        ok = sign_date["date"] <= deadline["date"]
        return {"status": "pass" if ok else "late", "sign_date": sign_date["date"].isoformat(), "deadline_date": deadline["date"].isoformat(), "matched_sign_text": sign_date["text"], "matched_sign_page": sign_date.get("page"), "matched_deadline_text": deadline["text"], "matched_deadline_page": deadline.get("page"), "deadline_locations": deadline_locations, "is_before_deadline": ok, "days_gap": (deadline["date"] - sign_date["date"]).days, "match_method": date_match_method}

    def _section_date_candidates(
        self,
        text: str,
        sections: list[dict] | None = None,
    ) -> list[dict]:
        """提取附件检查范围内的日期，并按日期、页码去重。"""
        items: list[dict] = []
        seen: set[tuple[date, int | None]] = set()
        for section in sections or []:
            section_text = str(section.get("text") or "").strip()
            if not section_text:
                continue
            page = section.get("page") if isinstance(section.get("page"), int) else None
            for candidate in self._date_candidates(section_text):
                key = (candidate["date"], page)
                if key in seen:
                    continue
                seen.add(key)
                items.append({**candidate, "page": page})
        if items:
            return items
        for candidate in self._date_candidates(text):
            key = (candidate["date"], None)
            if key in seen:
                continue
            seen.add(key)
            items.append({**candidate, "page": None})
        return items

    def _section_date(self, text: str, sections: list[dict] | None = None) -> dict | None:
        items, lines = [], self._lines(text)
        for i, line in enumerate(lines):
            if self._is_blocked_sign_date_context(line):
                continue
            if not (
                self._is_date_requirement_line(line)
                or self._is_ocr_damaged_date_field_line(line)
            ):
                continue
            line_items = self._date_candidates(line)
            items.extend(line_items)
            if not line_items and i + 1 < len(lines) and not self._is_blocked_sign_date_context(lines[i + 1]):
                items.extend(self._date_candidates(lines[i + 1]))
        if items:
            best = max(items, key=lambda x: x["date"])
            for section in sections or []:
                section_text = str(section.get("text") or "").strip()
                if best.get("text") and best["text"] in section_text:
                    page = section.get("page") if isinstance(section.get("page"), int) else None
                    return {**best, "page": page}
            return best

        fallback_items = []
        for section in sections or []:
            section_text = str(section.get("text") or "").strip()
            compact = self._compact(section_text)
            if not compact or str(section.get("type") or "").strip().lower() == "seal":
                continue
            if self._is_blocked_sign_date_context(section_text):
                continue
            if any(anchor in compact for anchor in self.DATE_FIELD_ANCHORS):
                for candidate in self._date_candidates(section_text):
                    fallback_items.append({**candidate, "page": section.get("page") if isinstance(section.get("page"), int) else None})
        if fallback_items:
            return max(fallback_items, key=lambda x: x["date"])

        pure_date_items = []
        section_list = list(sections or [])
        context_anchors = self.DATE_FIELD_ANCHORS + self.SIGNATURE_MARKERS + self.SEAL_MARKERS + self.COMPANY_ANCHORS
        for idx, section in enumerate(section_list):
            section_text = str(section.get("text") or "").strip()
            compact = self._compact(section_text)
            if not compact or str(section.get("type") or "").strip().lower() == "seal":
                continue
            if self._is_blocked_sign_date_context(section_text):
                continue
            if not self._is_pure_date_line(section_text):
                continue
            current_page = section.get("page") if isinstance(section.get("page"), int) else None
            nearby_context = False
            for neighbor in section_list[max(0, idx - 4): min(len(section_list), idx + 5)]:
                if neighbor is section:
                    continue
                neighbor_text = str(neighbor.get("text") or "").strip()
                neighbor_compact = self._compact(neighbor_text)
                if not neighbor_compact:
                    continue
                if self._is_blocked_sign_date_context(neighbor_text):
                    continue
                neighbor_page = neighbor.get("page") if isinstance(neighbor.get("page"), int) else None
                if current_page is not None and neighbor_page is not None and neighbor_page != current_page:
                    continue
                if any(anchor in neighbor_compact for anchor in context_anchors):
                    nearby_context = True
                    break
            if not nearby_context:
                continue
            for candidate in self._date_candidates(section_text):
                pure_date_items.append({**candidate, "page": current_page})
        if pure_date_items:
            return max(pure_date_items, key=lambda x: x["date"])

        return None

    def _signature_values(self, text: str) -> list[dict]:
        values = []
        for line in self._lines(text):
            if self._is_signature_requirement_line(line):
                value = self._signature_value(line)
                if value:
                    values.append({"line": line, "value": value})
        return values

    def _normalize_company(self, text: str) -> str:
        return bidder_identity.comparison_key(text)



    def _extract_company_candidate(self, text: str) -> str | None:
        return bidder_identity.name_value(text)

    def _bidder_identity(self, payload: dict | None) -> dict:
        return bidder_identity.identify(self._container(payload), self._sections(payload))

    def _bidder_name(self, payload: dict | None, seal_texts: list[str]) -> str | None:
        return self._bidder_identity(payload)["name"]

    def _company_score(self, bidder_name: str, seal_text: str) -> float:
        return bidder_identity.company_score(bidder_name, seal_text)

    def _seal_company_matches(self, bidder_name: str, seal_text: str, score: float) -> bool:
        """Accept a complete organization-name match above the configured score."""
        seal_name = bidder_identity.name_value(seal_text) or seal_text
        return bool(
            score > self.SEAL_COMPANY_MATCH_THRESHOLD
            and bidder_identity.sufficient_name(bidder_name)
            and bidder_identity.sufficient_name(seal_name)
        )

    def _seal_company_check(self, bidder_name: str | None, seal_texts: list[str]) -> dict:
        if not seal_texts:
            return {"status": "pending", "matched": False, "reason": "seal_text_not_found", "best_match": None}
        if not bidder_name:
            return {"status": "pending", "matched": False, "reason": "bidder_name_not_found", "best_match": None}
        best = None
        best_score = None
        for seal_text in seal_texts:
            score = self._company_score(bidder_name, seal_text)
            if best_score is None or score > best_score:
                best_score = score
                best = {"bidder_name": bidder_name, "seal_text": seal_text, "score": round(score, 4)}
        matched = bool(
            best
            and best_score is not None
            and self._seal_company_matches(
                bidder_name,
                best["seal_text"],
                best_score,
            )
        )
        return {
            "status": "pass" if matched else "pending",
            "matched": matched,
            "reason": "matched" if matched else "identity_requires_confirmation",
            "best_match": best,
            "match_threshold": self.SEAL_COMPANY_MATCH_THRESHOLD,
            "match_operator": ">",
        }

    def _textual_seal_evidences(self, bid_section: dict | None, bidder_name: str | None) -> list[dict]:
        if not isinstance(bid_section, dict):
            return []
        bidder_norm = self._normalize_company(bidder_name) if bidder_name else ""
        evidences: list[dict] = []
        seen: set[tuple[int | None, str]] = set()
        for section in bid_section.get("sections") or []:
            page = section.get("page") if isinstance(section.get("page"), int) else None
            section_text = str(section.get("text") or "").strip()
            if not section_text:
                continue
            for line in self._lines(section_text):
                compact = self._compact(line)
                if not compact or not any(marker in compact for marker in self.SEAL_MARKERS):
                    continue
                normalized_company = self._normalize_company(line)
                if bidder_norm:
                    if bidder_norm not in normalized_company:
                        continue
                    company_value = bidder_norm
                else:
                    match = self.COMPANY_RE.search(compact)
                    if not match:
                        continue
                    company_value = self._normalize_company(match.group(1))
                key = (page, compact)
                if key in seen:
                    continue
                seen.add(key)
                evidences.append(
                    {
                        "page": page,
                        "box": self._normalize_bbox(section.get("bbox")),
                        "text": line,
                        "company": company_value,
                    }
                )
        return evidences
