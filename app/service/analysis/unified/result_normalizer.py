# unified/result_normalizer.py
"""
统一商务标审查 - 审查结果标准化 Mixin

将各审查模块的原始输出规范化为统一的问题列表、摘要和指标。
"""

from __future__ import annotations

from typing import Any


class ResultNormalizerMixin:
    """
    审查结果标准化 Mixin。

    依赖：
    - 其他 Mixin：_issue, _empty_issue_bucket, _join_text, _map_generic_status,
                 _map_price_result, _combine_review_status, _summarize_itemized_subcheck
    """

    @staticmethod
    def _locations_with_document_role(
        locations: Any,
        role: str,
    ) -> list[dict[str, Any]]:
        tagged: list[dict[str, Any]] = []
        if not isinstance(locations, list):
            return tagged
        for location in locations:
            if not isinstance(location, dict):
                continue
            next_location = dict(location)
            next_location.setdefault("document_role", role)
            tagged.append(next_location)
        return tagged

    @staticmethod
    def _single_tender_location(
        *,
        page: Any = None,
        bbox: Any = None,
        text: Any = None,
    ) -> list[dict[str, Any]]:
        if page in (None, "", []) and bbox in (None, "", []):
            return []
        location: dict[str, Any] = {
            "document_role": "tender",
            "document": "tender",
            "coordinate_system": "pdf_point",
        }
        if page not in (None, "", []):
            location["page"] = page
        if bbox not in (None, "", []):
            location["bbox"] = bbox
        if text not in (None, "", []):
            location["text"] = str(text)
        if len(location) <= 3:
            return []
        return [location]

    def _deviation_issue_evidence(self, item: dict[str, Any]) -> dict[str, Any]:
        evidence = dict(item)
        if not evidence.get("tender_star_locations"):
            tender_locations = self._single_tender_location(
                page=item.get("requirement_page"),
                bbox=item.get("requirement_bbox"),
                text=item.get("requirement"),
            )
            if tender_locations:
                evidence["tender_star_locations"] = tender_locations
        return evidence

    def _normalize_integrity(self, raw: dict[str, Any]) -> dict[str, Any]:
        """标准化完整性审查原始结果。"""
        details = raw.get("details", {}) if isinstance(raw, dict) else {}
        score = raw.get("integrity_score") if isinstance(raw, dict) else None
        ignored_count = raw.get("ignored_item_count", 0) if isinstance(raw, dict) else 0

        passed = []
        failed = []
        missing = []
        for item_name, detail in details.items():
            detail = detail or {}
            if not detail.get("scored", True):
                continue
            preview = str(detail.get("preview") or "-")
            category = str(detail.get("category") or "")
            evidence = {
                "status": detail.get("status"),
                "preview": preview,
                "category": category,
                "locations": self._locations_with_document_role(
                    detail.get("locations") or [],
                    "business_bid",
                ),
                "template_locations": self._locations_with_document_role(
                    detail.get("template_locations") or [],
                    "tender",
                ),
                "catalog_pages": raw.get("business_catalog_pages") or raw.get("toc_pages") or [],
                "catalog_locations": self._locations_with_document_role(
                    raw.get("business_catalog_locations") or [],
                    "business_bid",
                ),
            }
            if detail.get("is_passed"):
                passed.append(
                    self._issue(
                        status="pass",
                        title=item_name,
                        message=f"已找到，命中内容：{preview}",
                        evidence=evidence,
                    )
                )
            else:
                missing.append(
                    self._issue(
                        status="missing",
                        title=item_name,
                        message="未在商务标中找到该必备项。",
                        evidence=evidence,
                    )
                )

        total = (
            raw.get("scored_item_count")
            if isinstance(raw, dict) and isinstance(raw.get("scored_item_count"), int)
            else len(passed) + len(failed) + len(missing)
        )
        review_status = self._combine_review_status(
            [issue["status"] for issue in passed + failed + missing]
        )
        # 摘要使用“已命中/总数”的口径，避免直接使用“缺失 X 项”的表述。
        summary = f"共校验 {total} 项，已命中 {len(passed)}/{total} 项"
        if score is not None:
            summary += f"，完整性得分 {score}"
        summary += "。"
        if ignored_count:
            summary += f" 另有 {ignored_count} 个父级标题由子项覆盖，不单独计分。"
        return {
            "validation": {
                "status": "correct" if isinstance(details, dict) else "failed",
                "reason": "模块返回了完整性得分和逐项命中明细。",
            },
            "review": {
                "status": review_status,
                "summary": summary,
            },
            "metrics": {
                "integrity_score": score,
                "total_item_count": total,
                "passed_item_count": len(passed),
                "failed_item_count": len(failed),
                "missing_item_count": len(missing),
                "ignored_item_count": ignored_count,
            },
            "issues": {
                "passed": passed,
                "failed": failed,
                "missing": missing,
                "unclear": [],
            },
        }

    def _normalize_consistency(self, raw: Any) -> dict[str, Any]:
        """标准化一致性审查原始结果。"""
        skipped_segments = []
        original_segment_count = 0
        if isinstance(raw, dict):
            segments = raw.get("evaluated_segments", raw.get("segments", [])) or []
            skipped_segments = raw.get("skipped_segments", []) or []
            original_segment_count = int(raw.get("original_segment_count") or 0)
        else:
            segments = raw if isinstance(raw, list) else []
            original_segment_count = len(segments)
        passed = []
        failed = []
        missing_items = []
        short_body_skipped = 0
        attachment_not_found_skipped = 0
        integrity_skipped = 0
        self_defined_skipped = 0

        for skipped in skipped_segments:
            skip_reason = skipped.get("skip_reason") or {}
            skip_type = str(skip_reason.get("type") or "")
            if skip_type == "body_too_short":
                short_body_skipped += 1
            elif skip_type in {"attachment_not_found", "optional_attachment_not_provided"}:
                attachment_not_found_skipped += 1
            elif skip_type == "self_defined_format":
                self_defined_skipped += 1
            else:
                integrity_skipped += 1

        for segment in segments:
            title = str(segment.get("name") or "未命名模板段")
            missing = segment.get("missing_anchors") or []
            unfilled_fields = segment.get("unfilled_fields") or []
            evidence = {
                "missing_anchors": missing,
                "unfilled_fields": unfilled_fields,
                "template_body_length": segment.get("template_body_length"),
                "bid_body_length": segment.get("bid_body_length"),
                "locations": self._locations_with_document_role(
                    segment.get("locations") or [],
                    "business_bid",
                ),
                "template_locations": self._locations_with_document_role(
                    segment.get("template_locations") or [],
                    "tender",
                ),
            }
            if segment.get("is_passed"):
                passed.append(
                    self._issue(
                        status="pass",
                        title=title,
                        message="模板正文固定内容未发现改动。",
                        evidence=evidence,
                    )
                )
            else:
                parts = []
                if missing:
                    parts.append(f"缺少模板关键内容：{self._join_text(missing)}")
                issue_status = "missing" if missing or unfilled_fields else "fail"
                issue = self._issue(
                    status=issue_status,
                    title=title,
                    message="；".join(parts) or "模板正文固定内容疑似被修改。",
                    evidence=evidence,
                )
                if issue_status == "missing":
                    missing_items.append(issue)
                else:
                    failed.append(issue)

        has_results = bool(segments or skipped_segments)
        if skipped_segments:
            validation_status = "correct"
            validation_reason = "模块返回了逐模板段的一致性结果，并已跳过正文过短或完整性缺失的附件。"
        else:
            validation_status = "correct" if segments else "unclear"
            validation_reason = (
                "模块返回了逐模板段的正文固定内容比对结果。"
                if segments
                else "未提取到可比较的模板段，需人工复核模板抽取是否成功。"
            )

        if has_results:
            review_status = self._combine_review_status(
                [issue["status"] for issue in passed + failed + missing_items]
            )
        else:
            review_status = "unclear"

        if has_results:
            total_segments = original_segment_count or (len(segments) + len(skipped_segments))
            # 一致性摘要统一改成“已通过/已校验”的数量表达。
            summary = (
                f"共比对 {total_segments} 个模板段，实际校验 {len(segments)} 个，"
                f"已通过 {len(passed)}/{len(segments)} 个；"
                f"正文不足20字跳过 {short_body_skipped} 个，"
                f"格式自拟跳过 {self_defined_skipped} 个，"
                f"附件未稳定定位跳过 {attachment_not_found_skipped} 个，"
                f"因完整性结果跳过 {integrity_skipped} 个。"
            )
        else:
            summary = "未提取到可比较的模板段。"

        return {
            "validation": {"status": validation_status, "reason": validation_reason},
            "review": {"status": review_status, "summary": summary},
            "metrics": {
                "template_segment_count": original_segment_count or len(segments),
                "evaluated_segment_count": len(segments),
                "skipped_segment_count": len(skipped_segments),
                "short_body_skipped_count": short_body_skipped,
                "self_defined_skipped_count": self_defined_skipped,
                "attachment_not_found_skipped_count": attachment_not_found_skipped,
                "integrity_skipped_count": integrity_skipped,
                "passed_segment_count": len(passed),
                "failed_segment_count": len(failed),
                "missing_segment_count": len(missing_items),
                "fillable_field_count": 0,
                "unfilled_field_count": 0,
            },
            "issues": {"passed": passed, "failed": failed, "missing": missing_items, "unclear": []},
        }

    def _normalize_pricing(self, raw: dict[str, Any]) -> dict[str, Any]:
        """标准化报价审查的原始结果。"""
        self_check = raw.get("self_check", {}) if isinstance(raw, dict) else {}
        tender_limit_check = raw.get("tender_limit_check", {}) if isinstance(raw, dict) else {}

        passed: list[dict[str, Any]] = []
        failed: list[dict[str, Any]] = []
        missing: list[dict[str, Any]] = []
        unclear: list[dict[str, Any]] = []
        shared_tender_price_locations: list[dict[str, Any]] = []
        if isinstance(tender_limit_check, dict):
            shared_tender_price_locations = [
                dict(location)
                for location in tender_limit_check.get("locations") or []
                if isinstance(location, dict)
                and str(
                    location.get("document_role")
                    or location.get("document")
                    or location.get("role")
                    or ""
                ).lower()
                in {"tender", "鎷涙爣鏂囦欢", "招标文件"}
            ]

        for subcheck_code, title, payload in (
            ("price_reasonableness", "投标总价自检", self_check),
            ("tender_limit_check", "招标限价校验", tender_limit_check),
        ):
            summary_text = self._join_text(payload.get("summary"))
            status = self._map_price_result(payload.get("result"), summary_text)
            evidence = dict(payload) if isinstance(payload, dict) else {}
            evidence.update(
                {
                    "result": payload.get("result"),
                    "type": payload.get("type"),
                    "summary": payload.get("summary"),
                    "subcheck_code": subcheck_code,
                }
            )
            if subcheck_code == "tender_limit_check" and isinstance(payload, dict):
                tender_locations = [
                    dict(location)
                    for location in payload.get("locations") or []
                    if isinstance(location, dict)
                    and str(
                        location.get("document_role")
                        or location.get("document")
                        or location.get("role")
                        or ""
                    ).lower()
                    in {"tender", "招标文件"}
                ]
                if tender_locations:
                    evidence["tender_price_locations"] = self._locations_with_document_role(
                        tender_locations,
                        "tender",
                    )
            elif shared_tender_price_locations and not evidence.get("tender_price_locations"):
                evidence["tender_price_locations"] = self._locations_with_document_role(
                    shared_tender_price_locations,
                    "tender",
                )
            issue = self._issue(
                status=status,
                title=title,
                message=summary_text or "未返回明确结论。",
                evidence=evidence,
            )
            if status == "pass":
                passed.append(issue)
            elif status == "fail":
                failed.append(issue)
            elif status == "missing":
                missing.append(issue)
            else:
                unclear.append(issue)

        review_status = self._combine_review_status(
            [issue["status"] for issue in passed + failed + missing + unclear]
        )
        return {
            "validation": {
                "status": "correct" if self_check or tender_limit_check else "unclear",
                "reason": "模块返回了报价自检和招标限价校验两部分结果。",
            },
            "review": {
                "status": review_status,
                "summary": "；".join(issue["message"] for issue in passed + failed + unclear if issue["message"]),
            },
            "metrics": {
                "passed_subcheck_count": len(passed),
                "failed_subcheck_count": len(failed),
                "missing_subcheck_count": len(missing),
                "unclear_subcheck_count": len(unclear),
            },
            "issues": {"passed": passed, "failed": failed, "missing": missing, "unclear": unclear},
        }

    def _normalize_itemized(self, raw: dict[str, Any]) -> dict[str, Any]:
        """标准化分项报价审查的原始结果。"""
        checks = raw.get("checks", {}) if isinstance(raw, dict) else {}
        manual_review = raw.get("manual_review", {}) if isinstance(raw, dict) else {}
        raw_status = str(raw.get("status") or "").strip().lower() if isinstance(raw, dict) else ""
        itemized_table_detected = bool(raw.get("itemized_table_detected")) if isinstance(raw, dict) else False
        top_status = self._map_generic_status(raw_status)
        missing_itemized_table = raw_status in {"not_detected", "missing"} and not itemized_table_detected

        if missing_itemized_table:
            missing_issue = self._issue(
                status="missing",
                title="分项报价表",
                message=str(
                    raw.get("summary")
                    or "未识别到分项报价表，无法执行分项报价表一致性校验。"
                ),
                evidence={"itemized_table_detected": False, "raw_status": raw_status},
            )
            return {
                "validation": {
                    "status": "correct",
                    "reason": "模块已执行，但当前文件未检测到可用于一致性校验的分项报价表。",
                },
                "review": {
                    "status": "missing",
                    "summary": str(
                        raw.get("summary")
                        or "未识别到分项报价表，无法执行分项报价表一致性校验。"
                    ),
                },
                "metrics": {
                    "itemized_table_detected": False,
                    "passed_subcheck_count": 0,
                    "failed_subcheck_count": 0,
                    "missing_subcheck_count": 1,
                    "unclear_subcheck_count": 0,
                },
                "issues": {"passed": [], "failed": [], "missing": [missing_issue], "unclear": []},
            }

        passed: list[dict[str, Any]] = []
        failed: list[dict[str, Any]] = []
        missing: list[dict[str, Any]] = []
        unclear: list[dict[str, Any]] = []

        subcheck_labels = {
            "row_arithmetic": "分项行算术校验",
            "sum_consistency": "分项汇总一致性校验",
            "duplicate_items": "疑似重复报价校验",
            "missing_item": "招标列项缺失校验",
        }
        itemized_evidence = raw.get("evidence") or {}

        def itemized_locations_for_subcheck(
            subcheck_code: str,
            payload: dict[str, Any],
        ) -> list[dict[str, Any]]:
            locations: list[dict[str, Any]] = []

            def append_page_location(page: Any, text: Any = None) -> None:
                page_number = self._first_positive_page(page)
                if not page_number:
                    return
                locations.append(
                    {
                        "page": page_number,
                        "text": str(text or subcheck_labels.get(subcheck_code) or subcheck_code),
                        "document": "bidder",
                    }
                )

            if subcheck_code == "sum_consistency":
                matched_label = str(payload.get("matched_total_label") or "").strip()
                for total in itemized_evidence.get("total_candidates") or []:
                    if not isinstance(total, dict):
                        continue
                    label = str(total.get("label") or "").strip()
                    if matched_label and label and label != matched_label:
                        continue
                    append_page_location(total.get("section_pages"), label or matched_label)
                    if locations:
                        break
                if not locations:
                    for table in itemized_evidence.get("structured_tables") or []:
                        for location in table.get("locations") or []:
                            if isinstance(location, dict):
                                locations.append(location)
                        if locations:
                            break
            elif subcheck_code == "row_arithmetic":
                for item in payload.get("issues") or []:
                    if not isinstance(item, dict):
                        continue
                    append_page_location(item.get("section_pages"), item.get("label") or item.get("serial"))
            elif subcheck_code == "missing_item":
                for item in payload.get("missing_items") or []:
                    if not isinstance(item, dict):
                        continue
                    append_page_location(item.get("section_pages") or item.get("pages"), item.get("label") or item.get("name"))

            return locations

        for subcheck_code, payload in checks.items():
            sub_status = str(payload.get("status") or "").strip().lower()
            if sub_status == "not_applicable":
                continue

            normalized_status = self._map_generic_status(sub_status)
            if subcheck_code == "missing_item" and normalized_status == "fail":
                normalized_status = "missing"
            label = subcheck_labels.get(subcheck_code, subcheck_code)
            message = self._summarize_itemized_subcheck(subcheck_code, payload)
            evidence = dict(payload)
            if not evidence.get("locations"):
                locations = itemized_locations_for_subcheck(subcheck_code, evidence)
                if locations:
                    evidence["locations"] = locations
            issue = self._issue(status=normalized_status, title=label, message=message, evidence=evidence)
            if normalized_status == "pass":
                passed.append(issue)
            elif normalized_status == "fail":
                failed.append(issue)
            elif normalized_status == "missing":
                missing.append(issue)
            else:
                unclear.append(issue)

        if manual_review.get("required") and not missing_itemized_table:
            manual_evidence = dict(manual_review)
            if not manual_evidence.get("locations"):
                recognized_total = manual_evidence.get("recognized_total")
                if isinstance(recognized_total, dict):
                    page = self._first_positive_page(recognized_total.get("section_pages") or recognized_total.get("pages"))
                    if page:
                        manual_evidence["locations"] = [
                            {
                                "page": page,
                                "text": recognized_total.get("label") or "分项报价识别总价",
                                "document": "bidder",
                            }
                        ]
                if not manual_evidence.get("locations"):
                    for table in itemized_evidence.get("structured_tables") or []:
                        table_locations = [
                            location
                            for location in (table.get("locations") or [])
                            if isinstance(location, dict)
                        ]
                        if table_locations:
                            manual_evidence["locations"] = table_locations
                            break
            unclear.append(
                self._issue(
                    status="unclear",
                    title="人工复核提示",
                    message="分项报价识别存在歧义，建议人工核对识别总价和未完整识别行。",
                    evidence=manual_evidence,
                )
            )

        validation_status = "correct"
        validation_reason = "模块返回了分项报价校验明细。"
        issue_statuses = [issue["status"] for issue in passed + failed + missing + unclear]
        review_status = self._combine_review_status(issue_statuses) if issue_statuses else top_status
        review_summary = str(raw.get("summary") or "未返回分项报价结论。")
        if top_status == "unclear":
            validation_status = "unclear"
            validation_reason = "模块已执行，但当前样本存在未完整识别的分项行，结论需人工复核。"

        return {
            "validation": {"status": validation_status, "reason": validation_reason},
            "review": {
                "status": review_status,
                "summary": review_summary,
            },
            "metrics": {
                "itemized_table_detected": raw.get("itemized_table_detected"),
                "passed_subcheck_count": len(passed),
                "failed_subcheck_count": len(failed),
                "missing_subcheck_count": len(missing),
                "unclear_subcheck_count": len(unclear),
            },
            "issues": {"passed": passed, "failed": failed, "missing": missing, "unclear": unclear},
        }

    def _normalize_deviation(self, raw: dict[str, Any]) -> dict[str, Any]:
        """标准化偏离条款审查的原始结果。"""
        compliance_status = self._map_generic_status(raw.get("compliance_status"))
        missing_items = raw.get("missing_response_items", []) if isinstance(raw, dict) else []
        negative_items = raw.get("negative_deviation_items", []) if isinstance(raw, dict) else []
        unclear_items = raw.get("unclear_response_items", []) if isinstance(raw, dict) else []

        passed: list[dict[str, Any]] = []
        failed: list[dict[str, Any]] = []
        missing: list[dict[str, Any]] = []
        unclear: list[dict[str, Any]] = []

        if raw.get("deviation_status") == "no_star_requirements":
            passed.append(
                self._issue(
                    status="pass",
                    title="星标条款检查",
                    message=str(raw.get("summary") or "未发现带 ★ 的强制性要求。"),
                    evidence={
                        "core_requirements_count": raw.get("core_requirements_count"),
                        "deviation_tables": raw.get("deviation_tables"),
                    },
                )
            )

        for item in missing_items:
            evidence = self._deviation_issue_evidence(item)
            missing.append(
                self._issue(
                    status="missing",
                    title=item.get("requirement") or "缺失响应条款",
                    message="未找到对应响应内容。",
                    evidence=evidence,
                )
            )
        for item in negative_items:
            evidence = self._deviation_issue_evidence(item)
            failed.append(
                self._issue(
                    status="fail",
                    title=item.get("requirement") or "负偏离条款",
                    message=f"检测到负偏离：{item.get('response_evidence') or '未提供详细证据'}",
                    evidence=evidence,
                )
            )
        for item in unclear_items:
            evidence = self._deviation_issue_evidence(item)
            unclear.append(
                self._issue(
                    status="unclear",
                    title=item.get("requirement") or "响应不明确条款",
                    message=f"响应内容不明确：{item.get('response_evidence') or '未提供详细证据'}",
                    evidence=evidence,
                )
            )

        if compliance_status == "pass" and not passed:
            passed.append(
                self._issue(
                    status="pass",
                    title="偏离条款校验",
                    message=str(raw.get("summary") or "偏离条款审查通过。"),
                    evidence={"stats": raw.get("stats")},
                )
            )

        issue_statuses = [issue["status"] for issue in passed + failed + missing + unclear]
        review_status = self._combine_review_status(issue_statuses) if issue_statuses else compliance_status

        total_requirements = raw.get("core_requirements_count")
        if not isinstance(total_requirements, int):
            total_requirements = len(missing_items) + len(negative_items) + len(unclear_items)
        covered_count = max(0, int(total_requirements or 0) - len(missing_items))
        review_summary = (
            f"共核验 {int(total_requirements or 0)} 条带★要求，"
            f"已明确响应 {covered_count}/{int(total_requirements or 0)} 条，"
            f"负偏离 {len(negative_items)} 条，不明确 {len(unclear_items)} 条。"
        )

        return {
            "validation": {
                "status": "correct",
                "reason": "模块返回了星标条款抽取、响应匹配和偏离分类结果。",
            },
            "review": {
                "status": review_status,
                "summary": review_summary,
            },
            "metrics": {
                "core_requirements_count": raw.get("core_requirements_count"),
                "missing_count": len(missing_items),
                "negative_deviation_count": len(negative_items),
                "unclear_deviation_count": len(unclear_items),
            },
            "issues": {"passed": passed, "failed": failed, "missing": missing, "unclear": unclear},
        }

    def _verification_attachment_lookup(self, raw: dict[str, Any]) -> dict[str, dict[str, Any]]:
        lookup: dict[str, dict[str, Any]] = {}
        if not isinstance(raw, dict):
            return lookup
        for item in (raw.get("attachment_results") or []) + (raw.get("missing_attachment_results") or []):
            if not isinstance(item, dict):
                continue
            title = str(item.get("title") or "").strip()
            if title and title not in lookup:
                lookup[title] = item
        return lookup

    def _verification_attachment_evidence(
        self,
        attachment: Any,
        *,
        source: str,
        lookup: dict[str, dict[str, Any]],
    ) -> dict[str, Any]:
        evidence: dict[str, Any] = {"attachment": attachment, "source": source}
        item = lookup.get(str(attachment or "").strip())
        if not isinstance(item, dict):
            return evidence

        for key in ("pages", "locations", "attachment_number", "matched_bid_title", "template_locations"):
            value = item.get(key)
            if value not in (None, "", []):
                evidence[key] = (
                    self._locations_with_document_role(value, "tender")
                    if key == "template_locations"
                    else value
                )

        date_check = item.get("date_check")
        if source == "date_check" and isinstance(date_check, dict):
            evidence["date_check"] = date_check
            for source_key, target_key in (
                ("matched_sign_page", "matched_page"),
                ("matched_sign_text", "matched_text"),
            ):
                value = date_check.get(source_key)
                if value not in (None, "", []):
                    evidence[target_key] = value
            deadline_locations = date_check.get("deadline_locations") or []
            if deadline_locations:
                evidence["deadline_locations"] = self._locations_with_document_role(
                    deadline_locations,
                    "tender",
                )

        if source == "position_check":
            for key in ("signature_check", "seal_check"):
                value = item.get(key)
                if isinstance(value, dict):
                    evidence[key] = value
        return evidence

    def _normalize_verification(self, raw: dict[str, Any]) -> dict[str, Any]:
        """标准化签字盖章日期审查的原始结果。"""
        compliance_status = self._map_generic_status(raw.get("compliance_status"))
        position_check = raw.get("position_check", {}) if isinstance(raw, dict) else {}
        date_check = raw.get("date_check", {}) if isinstance(raw, dict) else {}
        seal_company_check = raw.get("seal_company_check", {}) if isinstance(raw, dict) else {}

        passed: list[dict[str, Any]] = []
        failed: list[dict[str, Any]] = []
        missing: list[dict[str, Any]] = []
        unclear: list[dict[str, Any]] = []

        missing_attachments = position_check.get("missing_attachments") or []
        missing_signature = position_check.get("missing_signature_attachments") or []
        pending_signature = position_check.get("pending_signature_attachments") or []
        missing_seal = position_check.get("missing_seal_attachments") or []
        missing_date = date_check.get("missing_date_attachments") or []
        late_date = date_check.get("late_date_attachments") or []
        attachment_lookup = self._verification_attachment_lookup(raw)

        if seal_company_check.get("status") == "pass":
            passed.append(
                self._issue(status="pass", title="公章与投标人匹配", message="检测到的公章与投标人名称匹配。", evidence=seal_company_check)
            )
        elif seal_company_check:
            failed.append(
                self._issue(status="fail", title="公章与投标人匹配", message="检测到的公章与投标人名称不匹配。", evidence=seal_company_check)
            )

        if position_check.get("status") == "pass":
            passed.append(
                self._issue(status="pass", title="签字盖章位置检查", message="所有必需附件均已找到，未发现缺失签字或盖章。", evidence=position_check)
            )
        else:
            for attachment in missing_attachments:
                missing.append(
                    self._issue(
                        status="missing",
                        title=attachment,
                        message="未找到要求签章的附件。",
                        evidence=self._verification_attachment_evidence(
                            attachment,
                            source="position_check",
                            lookup=attachment_lookup,
                        ),
                    )
                )
            for attachment in missing_signature:
                missing.append(
                    self._issue(
                        status="missing",
                        title=attachment,
                        message="附件缺少签字。",
                        evidence=self._verification_attachment_evidence(
                            attachment,
                            source="position_check",
                            lookup=attachment_lookup,
                        ),
                    )
                )
            for attachment in missing_seal:
                missing.append(
                    self._issue(
                        status="missing",
                        title=attachment,
                        message="附件缺少盖章。",
                        evidence=self._verification_attachment_evidence(
                            attachment,
                            source="position_check",
                            lookup=attachment_lookup,
                        ),
                    )
                )

        for attachment in pending_signature:
            unclear.append(
                self._issue(
                    status="unclear",
                    title=attachment,
                    message="签字字段处于待填写状态，建议人工复核。",
                    evidence=self._verification_attachment_evidence(
                        attachment,
                        source="position_check",
                        lookup=attachment_lookup,
                    ),
                )
            )

        if date_check.get("status") == "pass":
            passed.append(
                self._issue(status="pass", title="落款日期检查", message="已检测到落款日期，且均未晚于投标截止时间。", evidence=date_check)
            )
        else:
            for attachment in missing_date:
                missing.append(
                    self._issue(
                        status="missing",
                        title=attachment,
                        message="附件缺少落款日期。",
                        evidence=self._verification_attachment_evidence(
                            attachment,
                            source="date_check",
                            lookup=attachment_lookup,
                        ),
                    )
                )
            for attachment in late_date:
                failed.append(
                    self._issue(
                        status="fail",
                        title=attachment,
                        message="附件落款日期晚于招标截止时间。",
                        evidence=self._verification_attachment_evidence(
                            attachment,
                            source="date_check",
                            lookup=attachment_lookup,
                        ),
                    )
                )

        required_attachment_count = int(raw.get("required_attachment_count") or 0)
        position_pass_count = max(
            0,
            required_attachment_count - len(missing_attachments) - len(missing_signature) - len(missing_seal),
        )
        date_pass_count = max(
            0,
            required_attachment_count - len(missing_date) - len(late_date),
        )
        review_summary = (
            f"共核验 {required_attachment_count} 个必检附件，"
            f"签章要素已覆盖 {position_pass_count}/{required_attachment_count} 个，"
            f"日期校验通过 {date_pass_count}/{required_attachment_count} 个。"
        )
        if seal_company_check:
            review_summary += (
                " 公章单位匹配已确认。"
                if seal_company_check.get("status") == "pass"
                else " 公章单位匹配需进一步确认。"
            )

        issue_statuses = [issue["status"] for issue in passed + failed + missing + unclear]
        review_status = self._combine_review_status(issue_statuses) if issue_statuses else compliance_status

        return {
            "validation": {"status": "correct", "reason": "模块返回了附件级签字、盖章、日期和公章匹配结果。"},
            "review": {"status": review_status, "summary": review_summary},
            "metrics": {
                "required_attachment_count": raw.get("required_attachment_count"),
                "missing_attachment_count": len(missing_attachments),
                "missing_signature_count": len(missing_signature),
                "pending_signature_count": len(pending_signature),
                "missing_seal_count": len(missing_seal),
                "missing_date_count": len(missing_date),
                "late_date_count": len(late_date),
            },
            "issues": {"passed": passed, "failed": failed, "missing": missing, "unclear": unclear},
        }
