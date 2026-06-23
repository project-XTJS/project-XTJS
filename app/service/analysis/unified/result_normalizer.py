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
            if role == "tender":
                next_location.setdefault("document", "tender")
            tagged.append(next_location)
        return tagged

    def _missing_anchor_locations_with_document_role(
        self,
        details: Any,
        role: str,
    ) -> list[dict[str, Any]]:
        tagged: list[dict[str, Any]] = []
        if not isinstance(details, list):
            return tagged
        for detail in details:
            if not isinstance(detail, dict):
                continue
            next_detail = dict(detail)
            next_detail["locations"] = self._locations_with_document_role(
                detail.get("locations") or [],
                role,
            )
            tagged.append(next_detail)
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

    def _deviation_issue_evidence(self, item: dict[str, Any], raw: dict[str, Any] | None = None) -> dict[str, Any]:
        evidence = dict(item)
        raw = raw or {}
        response_status = str(evidence.get("response_status") or "")
        deviation_status = str(raw.get("deviation_status") or "")
        table_missing = "deviation_table_missing" in response_status or "deviation_table_missing" in deviation_status
        if table_missing:
            evidence["deviation_table_missing"] = True
            evidence["deviation_status"] = deviation_status or response_status
            evidence["catalog_pages"] = raw.get("business_catalog_pages") or raw.get("catalog_pages") or []
            evidence["catalog_locations"] = self._locations_with_document_role(
                raw.get("business_catalog_locations") or raw.get("catalog_locations") or [],
                "business_bid",
            )
        if not evidence.get("tender_star_locations"):
            tender_locations = self._single_tender_location(
                page=item.get("requirement_page"),
                bbox=item.get("requirement_bbox"),
                text=item.get("requirement"),
            )
            if tender_locations:
                evidence["tender_star_locations"] = tender_locations
        if not evidence.get("response_locations") and item.get("response_page"):
            response_role = str(item.get("response_document_role") or "business_bid")
            evidence["response_locations"] = self._locations_with_document_role(
                [
                    {
                        "page": item.get("response_page"),
                        "bbox": item.get("response_bbox"),
                        "text": item.get("response_evidence") or item.get("requirement"),
                        "coordinate_system": "pdf_point",
                    }
                ],
                response_role,
            )
        return evidence

    @staticmethod
    def _is_deviation_table_missing_evidence(evidence: dict[str, Any]) -> bool:
        if evidence.get("deviation_table_missing"):
            return True
        status_values = (
            evidence.get("response_status"),
            evidence.get("deviation_status"),
        )
        return any("deviation_table_missing" in str(value or "") for value in status_values)

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
        unclear_items = []
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
            template_locations = self._locations_with_document_role(
                segment.get("template_locations") or [],
                "tender",
            )
            template_attachment_locations = self._locations_with_document_role(
                segment.get("template_attachment_locations")
                or segment.get("template_locations")
                or [],
                "tender",
            )
            tender_highlight_locations = self._locations_with_document_role(
                segment.get("tender_highlight_locations")
                or segment.get("template_attachment_locations")
                or segment.get("template_locations")
                or [],
                "tender",
            )
            evidence = {
                "missing_anchors": missing,
                "missing_anchor_locations": self._missing_anchor_locations_with_document_role(
                    segment.get("missing_anchor_locations") or [],
                    "tender",
                ),
                "unfilled_fields": unfilled_fields,
                "template_body_length": segment.get("template_body_length"),
                "bid_body_length": segment.get("bid_body_length"),
                "locations": self._locations_with_document_role(
                    segment.get("locations") or [],
                    "business_bid",
                ),
                "template_locations": template_locations,
                "template_attachment_locations": template_attachment_locations,
                "tender_highlight_locations": tender_highlight_locations,
                "engine_version": segment.get("engine_version"),
                "model_status": segment.get("model_status") or {},
                "attachment_match": segment.get("attachment_match") or {},
                "element_results": segment.get("element_results") or [],
                "difference_category": segment.get("difference_category"),
                "difference_items": segment.get("difference_items") or [],
            }
            segment_status = str(segment.get("status") or "").strip().lower()
            if segment_status not in {"pass", "missing", "unclear", "skipped"}:
                segment_status = (
                    "pass"
                    if segment.get("is_passed")
                    else ("missing" if missing or unfilled_fields else "unclear")
                )
            if segment_status == "pass":
                passed.append(
                    self._issue(
                        status="pass",
                        title=title,
                        message="模板正文固定内容未发现改动。",
                        evidence=evidence,
                    )
                )
            elif segment_status == "missing":
                parts = []
                if missing:
                    parts.append(f"缺少模板关键内容：{self._join_text(missing)}")
                issue_status = "missing"
                issue = self._issue(
                    status=issue_status,
                    title=title,
                    message="；".join(parts) or "模板正文固定内容疑似被修改。",
                    evidence=evidence,
                )
                missing_items.append(issue)
            elif segment_status == "unclear":
                unclear_items.append(
                    self._issue(
                        status="unclear",
                        title=title,
                        message="模板骨架存在疑似改写或对齐不确定项，需要人工复核。",
                        evidence=evidence,
                    )
                )

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
                [
                    issue["status"]
                    for issue in passed + failed + missing_items + unclear_items
                ]
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
                "unclear_segment_count": len(unclear_items),
                "fillable_field_count": 0,
                "unfilled_field_count": 0,
            },
            "issues": {
                "passed": passed,
                "failed": failed,
                "missing": missing_items,
                "unclear": unclear_items,
            },
        }

    def _normalize_pricing(self, raw: dict[str, Any]) -> dict[str, Any]:
        """标准化报价审查的原始结果。"""
        self_check = raw.get("self_check", {}) if isinstance(raw, dict) else {}
        tender_limit_check = raw.get("tender_limit_check", {}) if isinstance(raw, dict) else {}

        def location_role(location: dict[str, Any]) -> str:
            raw_role = str(
                location.get("document_role")
                or location.get("document")
                or location.get("role")
                or ""
            ).lower()
            if (
                raw_role in {"tender", "招标文件"}
                or "tender" in raw_role
                or "招标" in raw_role
            ):
                return "tender"
            if (
                raw_role in {"bidder", "bid", "business", "business_bid", "投标文件", "商务标"}
                or "bidder" in raw_role
                or "business" in raw_role
                or "投标" in raw_role
                or "商务" in raw_role
            ):
                return "business_bid"
            return ""

        def positive_pages(values: Any) -> list[int]:
            pages: list[int] = []
            seen: set[int] = set()
            raw_values = values if isinstance(values, list) else [values]
            for value in raw_values:
                page = self._first_positive_page(value)
                if page and page not in seen:
                    seen.add(page)
                    pages.append(page)
            return pages

        subchecks: list[dict[str, Any]] = []
        message_parts: list[str] = []
        business_locations: list[dict[str, Any]] = []
        tender_locations: list[dict[str, Any]] = []
        business_pages: list[int] = []
        tender_pages: list[int] = []

        for subcheck_code, label, payload in (
            ("price_reasonableness", "直接报价大小写一致", self_check),
            ("tender_limit_check", "是否超过最高限价", tender_limit_check),
        ):
            payload = payload if isinstance(payload, dict) else {}
            summary_text = self._join_text(payload.get("summary"))
            status = self._map_price_result(payload.get("result"), summary_text)
            message = summary_text or "未返回明确结论。"
            message_parts.append(f"{label}：{message}")

            subcheck_locations = [
                dict(location)
                for location in payload.get("locations") or []
                if isinstance(location, dict)
            ]
            for location in subcheck_locations:
                role = location_role(location)
                if subcheck_code == "price_reasonableness" or role == "business_bid":
                    business_locations.append(location)
                elif role == "tender":
                    tender_locations.append(location)
            if subcheck_code == "price_reasonableness":
                business_pages.extend(positive_pages(payload.get("pages")))
            elif subcheck_code == "tender_limit_check":
                for page in positive_pages(payload.get("pages")):
                    tender_page = any(
                        self._first_positive_page(location) == page
                        for location in subcheck_locations
                        if location_role(location) == "tender"
                    )
                    if tender_page:
                        tender_pages.append(page)
                    else:
                        business_pages.append(page)

            subchecks.append(
                {
                    "subcheck_code": subcheck_code,
                    "label": label,
                    "status": status,
                    "result": payload.get("result"),
                    "type": payload.get("type"),
                    "summary": payload.get("summary"),
                    "message": message,
                    "pages": payload.get("pages") or [],
                    "locations": subcheck_locations,
                }
            )

        for location in tender_locations:
            page = self._first_positive_page(location)
            if page:
                tender_pages.append(page)
        for location in business_locations:
            page = self._first_positive_page(location)
            if page:
                business_pages.append(page)

        business_pages = positive_pages(business_pages)
        tender_pages = positive_pages(tender_pages)
        review_status = self._combine_review_status(
            [subcheck["status"] for subcheck in subchecks]
        )
        issue_evidence: dict[str, Any] = {
            "subcheck_code": "pricing_reasonableness",
            "self_check": self_check,
            "tender_limit_check": tender_limit_check,
            "subchecks": subchecks,
        }
        if business_locations:
            issue_evidence["locations"] = self._locations_with_document_role(
                business_locations,
                "business_bid",
            )
        if business_pages:
            issue_evidence["pages"] = business_pages
        if tender_locations:
            issue_evidence["tender_price_locations"] = self._locations_with_document_role(
                tender_locations,
                "tender",
            )
        if tender_pages:
            issue_evidence["tender_pages"] = tender_pages

        issue = self._issue(
            status=review_status,
            title="报价合理性",
            message="；".join(message_parts),
            evidence=issue_evidence,
        )
        passed = [issue] if review_status == "pass" else []
        failed = [issue] if review_status == "fail" else []
        missing = [issue] if review_status == "missing" else []
        unclear = [issue] if review_status == "unclear" else []
        status_counts = {
            "pass": sum(1 for subcheck in subchecks if subcheck["status"] == "pass"),
            "fail": sum(1 for subcheck in subchecks if subcheck["status"] == "fail"),
            "missing": sum(1 for subcheck in subchecks if subcheck["status"] == "missing"),
            "unclear": sum(1 for subcheck in subchecks if subcheck["status"] == "unclear"),
        }
        return {
            "validation": {
                "status": "correct" if self_check or tender_limit_check else "unclear",
                "reason": "模块返回了报价合理性的两个子项结果，已合并为报价合理性审查。",
            },
            "review": {
                "status": review_status,
                "summary": issue["message"],
            },
            "metrics": {
                "passed_subcheck_count": status_counts["pass"],
                "failed_subcheck_count": status_counts["fail"],
                "missing_subcheck_count": status_counts["missing"],
                "unclear_subcheck_count": status_counts["unclear"],
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

            def append_primary_itemized_location() -> None:
                for item in itemized_evidence.get("extracted_items") or []:
                    if not isinstance(item, dict):
                        continue
                    append_page_location(
                        item.get("section_pages") or item.get("pages"),
                        item.get("label") or item.get("serial") or "分项报价表",
                    )
                    if locations:
                        return

                for table in itemized_evidence.get("structured_tables") or []:
                    if not isinstance(table, dict):
                        continue
                    for location in table.get("locations") or []:
                        if isinstance(location, dict):
                            locations.append(location)
                    if locations:
                        return
                    append_page_location(
                        table.get("pages") or table.get("page"),
                        table.get("title") or "分项报价表",
                    )
                    if locations:
                        return

            if subcheck_code == "sum_consistency":
                append_primary_itemized_location()
                if locations:
                    return locations

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
                        if not isinstance(table, dict):
                            continue
                        for location in table.get("locations") or []:
                            if isinstance(location, dict):
                                locations.append(location)
                        if locations:
                            break
                        append_page_location(
                            table.get("pages") or table.get("page"),
                            table.get("title") or "分项报价表",
                        )
                        if locations:
                            break
            elif subcheck_code == "row_arithmetic":
                for item in payload.get("issues") or []:
                    if not isinstance(item, dict):
                        continue
                    append_page_location(item.get("section_pages"), item.get("label") or item.get("serial"))
                if not locations:
                    append_primary_itemized_location()
            elif subcheck_code == "missing_item":
                for item in payload.get("missing_items") or []:
                    if not isinstance(item, dict):
                        continue
                    append_page_location(item.get("section_pages") or item.get("pages"), item.get("label") or item.get("name"))

            return locations

        subchecks: list[dict[str, Any]] = []
        for subcheck_code in ("row_arithmetic", "sum_consistency"):
            payload = checks.get(subcheck_code) or {}
            if not isinstance(payload, dict) or not payload:
                continue
            sub_status = str(payload.get("status") or "").strip().lower()
            if sub_status == "not_applicable":
                continue

            normalized_status = self._map_generic_status(sub_status)
            label = subcheck_labels.get(subcheck_code, subcheck_code)
            message = self._summarize_itemized_subcheck(subcheck_code, payload)
            evidence = dict(payload)
            if not evidence.get("locations"):
                locations = itemized_locations_for_subcheck(subcheck_code, evidence)
                if locations:
                    evidence["locations"] = locations
            subchecks.append(
                {
                    "subcheck_code": subcheck_code,
                    "label": label,
                    "status": normalized_status,
                    "message": message,
                    "evidence": evidence,
                }
            )

        if not subchecks:
            fallback_status = top_status if top_status in {"pass", "fail", "missing", "unclear"} else "unclear"
            subchecks.append(
                {
                    "subcheck_code": "itemized_pricing_check",
                    "label": "分项报价表校验",
                    "status": fallback_status,
                    "message": str(raw.get("summary") or "未返回分项报价表校验明细。"),
                    "evidence": {"raw_status": raw_status, "itemized_table_detected": itemized_table_detected},
                }
            )

        issue_locations: list[dict[str, Any]] = []
        for subcheck in subchecks:
            evidence = subcheck.get("evidence") or {}
            for location in evidence.get("locations") or []:
                if isinstance(location, dict):
                    issue_locations.append(location)
        if not issue_locations:
            for table in itemized_evidence.get("structured_tables") or []:
                for location in table.get("locations") or []:
                    if isinstance(location, dict):
                        issue_locations.append(location)
                if issue_locations:
                    break

        issue_evidence: dict[str, Any] = {
            "subcheck_code": "itemized_pricing_check",
            "itemized_table_detected": raw.get("itemized_table_detected"),
            "subchecks": subchecks,
            "row_arithmetic": checks.get("row_arithmetic") or {},
            "sum_consistency": checks.get("sum_consistency") or {},
            "manual_review": manual_review,
        }
        if issue_locations:
            issue_evidence["locations"] = self._locations_with_document_role(
                issue_locations,
                "business_bid",
            )
        if itemized_evidence.get("extracted_item_count") is not None:
            issue_evidence["extracted_item_count"] = itemized_evidence.get("extracted_item_count")
        if itemized_evidence.get("total_candidates"):
            issue_evidence["total_candidates"] = itemized_evidence.get("total_candidates")

        review_status = self._combine_review_status([subcheck["status"] for subcheck in subchecks])
        message_parts = [
            f"{subcheck['label']}：{subcheck['message']}"
            for subcheck in subchecks
        ]
        if manual_review.get("required"):
            message_parts.append("人工复核提示：分项报价识别存在歧义，建议核对分项行和合计金额。")
            if review_status == "pass":
                review_status = "unclear"

        issue = self._issue(
            status=review_status,
            title="分项报价表校验",
            message="；".join(message_parts),
            evidence=issue_evidence,
        )
        passed = [issue] if review_status == "pass" else []
        failed = [issue] if review_status == "fail" else []
        missing = [issue] if review_status == "missing" else []
        unclear = [issue] if review_status == "unclear" else []

        validation_status = "correct"
        validation_reason = "模块返回了分项报价校验明细。"
        review_summary = issue["message"]
        if top_status == "unclear":
            validation_status = "unclear"
            validation_reason = "模块已执行，但当前样本存在未完整识别的分项行，结论需人工复核。"

        status_counts = {
            "pass": sum(1 for subcheck in subchecks if subcheck["status"] == "pass"),
            "fail": sum(1 for subcheck in subchecks if subcheck["status"] == "fail"),
            "missing": sum(1 for subcheck in subchecks if subcheck["status"] == "missing"),
            "unclear": sum(1 for subcheck in subchecks if subcheck["status"] == "unclear"),
        }

        return {
            "validation": {"status": validation_status, "reason": validation_reason},
            "review": {
                "status": review_status,
                "summary": review_summary,
            },
            "metrics": {
                "itemized_table_detected": raw.get("itemized_table_detected"),
                "passed_subcheck_count": status_counts["pass"],
                "failed_subcheck_count": status_counts["fail"],
                "missing_subcheck_count": status_counts["missing"],
                "unclear_subcheck_count": status_counts["unclear"],
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

        no_star_requirements = raw.get("deviation_status") == "no_star_requirements"

        for item in missing_items:
            evidence = self._deviation_issue_evidence(item, raw)
            missing.append(
                self._issue(
                    status="missing",
                    title=item.get("requirement") or "缺失响应条款",
                    message="缺少偏离表。" if self._is_deviation_table_missing_evidence(evidence) else "未找到对应响应内容。",
                    evidence=evidence,
                )
            )
        for item in negative_items:
            evidence = self._deviation_issue_evidence(item, raw)
            failed.append(
                self._issue(
                    status="fail",
                    title=item.get("requirement") or "负偏离条款",
                    message=f"检测到负偏离：{item.get('response_evidence') or '未提供详细证据'}",
                    evidence=evidence,
                )
            )
        for item in unclear_items:
            evidence = self._deviation_issue_evidence(item, raw)
            unclear.append(
                self._issue(
                    status="unclear",
                    title=item.get("requirement") or "响应不明确条款",
                    message=f"响应内容不明确：{item.get('response_evidence') or '未提供详细证据'}",
                    evidence=evidence,
                )
            )

        # 加分项(△)未达标：仅提示、不计入合规失败，但要逐条爆出供人工/模型确认。
        bonus: list[dict[str, Any]] = []
        for item in (raw.get("bonus_flagged_items") or []):
            evidence = self._deviation_issue_evidence(item, raw)
            bonus.append(
                self._issue(
                    status="warning",
                    title=item.get("requirement") or "加分项(△)未达标",
                    message="加分项(△)未响应或存在偏离，建议人工确认（不计入合规失败）。",
                    evidence=evidence,
                )
            )

        # 响应正确(无问题)的 ★/△ 项也逐条展示，便于逐项核对(招标★/△条款 ↔ 投标响应)。
        responded_ok_statuses = {"positive_deviation", "no_deviation", "listed_response"}
        for item in (raw.get("match_results") or []):
            if not isinstance(item, dict) or not item.get("responded"):
                continue
            if str(item.get("response_status") or "") not in responded_ok_statuses:
                continue
            evidence = self._deviation_issue_evidence(item, raw)
            marker = "△加分项" if item.get("requirement_kind") == "bonus" else "★必须项"
            message = f"[{marker}] 已响应"
            if item.get("semantic_status"):
                message += f"；语义判定：{item.get('semantic_status')}"
            passed.append(
                self._issue(
                    status="pass",
                    title=item.get("requirement") or "已响应条款",
                    message=message,
                    evidence=evidence,
                )
            )

        # 兜底：没有逐条通过项但整体通过(或无★要求)时，给一个汇总通过项。
        if compliance_status == "pass" and not passed and not no_star_requirements:
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
                "mandatory_requirements_count": raw.get("mandatory_requirements_count"),
                "bonus_requirements_count": raw.get("bonus_requirements_count"),
                "missing_count": len(missing_items),
                "negative_deviation_count": len(negative_items),
                "unclear_deviation_count": len(unclear_items),
                "bonus_flagged_count": len(bonus),
            },
            "issues": {
                "passed": passed,
                "failed": failed,
                "missing": missing,
                "unclear": unclear,
                "bonus": bonus,
            },
            "marker_items": self._build_deviation_marker_items(raw),
        }

    def _build_deviation_marker_items(self, raw: dict[str, Any]) -> list[dict[str, Any]]:
        """把每一个 ★/△ 标记项投影成精简列表，确保前端能逐条展示并人工/模型确认。"""
        items: list[dict[str, Any]] = []
        for match in (raw.get("match_results") or []):
            if not isinstance(match, dict):
                continue
            items.append(
                {
                    "requirement_id": match.get("requirement_id"),
                    "requirement": match.get("requirement"),
                    "marker_type": match.get("marker_type"),
                    "requirement_kind": match.get("requirement_kind"),
                    "responded": bool(match.get("responded")),
                    "response_status": match.get("response_status"),
                    "response_evidence": match.get("response_evidence"),
                    "response_page": match.get("response_page"),
                    "requirement_page": match.get("requirement_page"),
                    "semantic_score": match.get("semantic_score"),
                    "semantic_status": match.get("semantic_status"),
                    "needs_manual": match.get("needs_manual"),
                    "risk_level": match.get("risk_level"),
                }
            )
        return items

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

        if source in {"attachment_result", "position_check", "date_check"}:
            for key in ("requirements", "signature_check", "seal_check", "date_check"):
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
        attachment_results = [item for item in (raw.get("attachment_results") or []) if isinstance(item, dict)]
        handled_attachment_titles: set[str] = set()

        def status_of(value: Any) -> str:
            return str(value or "").strip().lower()

        def attachment_title(item: dict[str, Any]) -> str:
            return str(item.get("title") or item.get("matched_bid_title") or item.get("attachment_number") or "签字盖章日期审查").strip()

        def component_status(item: dict[str, Any], key: str) -> str:
            value = item.get(key)
            return status_of(value.get("status") if isinstance(value, dict) else None)

        def effective_attachment_status(item: dict[str, Any]) -> str:
            status = status_of(item.get("status"))
            statuses = [
                component_status(item, "signature_check"),
                component_status(item, "seal_check"),
                component_status(item, "date_check"),
            ]
            active_statuses = [value for value in statuses if value and value != "not_required"]
            if any(value in {"fail", "late"} for value in active_statuses):
                return "fail"
            if any(value in {"missing", "missing_date"} for value in active_statuses):
                return "missing"
            if any(value in {"pending", "missing_deadline", "unclear"} for value in active_statuses):
                return "pending"
            if status and status != "pass":
                return status
            if active_statuses and all(value == "pass" for value in active_statuses):
                return "pass"
            if status == "pass" and not active_statuses:
                return "pass"
            return compliance_status

        def attachment_status_details(item: dict[str, Any]) -> list[str]:
            details: list[str] = []
            signature_status = component_status(item, "signature_check")
            seal_status = component_status(item, "seal_check")
            item_date_status = component_status(item, "date_check")
            if signature_status in {"missing", "fail"}:
                details.append("缺少签字")
            elif signature_status == "pending":
                details.append("签字待复核")
            if seal_status in {"missing", "fail"}:
                details.append("缺少盖章")
            elif seal_status == "pending":
                details.append("盖章待复核")
            if item_date_status == "missing_date":
                details.append("缺少落款日期")
            elif item_date_status == "late":
                details.append("落款日期晚于招标截止时间")
            elif item_date_status == "missing_deadline":
                details.append("未识别到招标截止日期，需复核日期")
            return details

        for item in attachment_results:
            title = attachment_title(item)
            if not title:
                continue
            handled_attachment_titles.add(title)
            status = effective_attachment_status(item)
            evidence = self._verification_attachment_evidence(
                title,
                source="attachment_result",
                lookup=attachment_lookup,
            )
            details = attachment_status_details(item)
            if status == "pass":
                passed.append(
                    self._issue(
                        status="pass",
                        title=title,
                        message="附件要求的签字、盖章、落款日期均已满足。",
                        evidence=evidence,
                    )
                )
            elif status in {"fail", "late"}:
                failed.append(
                    self._issue(
                        status="fail",
                        title=title,
                        message="附件签字盖章日期要求未通过：" + ("；".join(details) if details else "存在不符合项") + "。",
                        evidence=evidence,
                    )
                )
            elif status in {"pending", "missing_deadline", "unclear"}:
                unclear.append(
                    self._issue(
                        status="unclear",
                        title=title,
                        message="附件签字盖章日期要求待复核：" + ("；".join(details) if details else "存在待确认项") + "。",
                        evidence=evidence,
                    )
                )
            elif status in {"missing", "missing_date"}:
                missing.append(
                    self._issue(
                        status="missing",
                        title=title,
                        message="附件签字盖章日期要求未全部满足：" + ("；".join(details) if details else "存在缺失项") + "。",
                        evidence=evidence,
                    )
                )

        seal_company_status = status_of(seal_company_check.get("status"))
        if seal_company_status == "fail":
            failed.append(
                self._issue(status="fail", title="公章与投标人匹配", message="检测到的公章与投标人名称不匹配。", evidence=seal_company_check)
            )

        for attachment in missing_attachments:
            if str(attachment or "").strip() in handled_attachment_titles:
                continue
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
            if str(attachment or "").strip() in handled_attachment_titles:
                continue
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
            if str(attachment or "").strip() in handled_attachment_titles:
                continue
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
            if str(attachment or "").strip() in handled_attachment_titles:
                continue
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

        for attachment in missing_date:
            if str(attachment or "").strip() in handled_attachment_titles:
                continue
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
            if str(attachment or "").strip() in handled_attachment_titles:
                continue
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
