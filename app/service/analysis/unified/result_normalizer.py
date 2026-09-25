# unified/result_normalizer.py
"""
统一商务标审查 - 审查结果标准化 Mixin

将各审查模块的原始输出规范化为统一的问题列表、摘要和指标。
"""

from __future__ import annotations

import re
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
        unclear = []
        optional_skipped = []
        applicability_unclear = []
        for item_name, detail in details.items():
            detail = detail or {}
            if detail.get('applicability_status') == 'unclear':
                evidence = {
                    'condition_text': detail.get('condition_text'),
                    'applicability_status': 'unclear',
                    'template_locations': self._locations_with_document_role(
                        detail.get('applicability_locations')
                        or detail.get('template_locations')
                        or [],
                        'tender',
                    ),
                }
                issue = self._issue(
                    status='unclear',
                    title=item_name,
                    message='该材料适用条件取决于参选方式或主体情况，需要人工确认。',
                    evidence=evidence,
                )
                unclear.append(issue)
                applicability_unclear.append(item_name)
                continue
            if not detail.get("scored", True):
                if detail.get('is_optional'):
                    optional_skipped.append(item_name)
                continue
            preview = str(detail.get("preview") or "-")
            category = str(detail.get("category") or "")
            evidence = {
                "status": detail.get("status"),
                "preview": preview,
                "category": category,
                "requirement_group": detail.get('requirement_group'),
                "resolution_status": detail.get('resolution_status'),
                "is_optional": detail.get('is_optional', False),
                "optionality_conflict": detail.get('optionality_conflict', False),
                "optionality_locations": self._locations_with_document_role(detail.get('optionality_locations') or [], 'tender'),
                "applicability_resolution": detail.get('applicability_resolution'),
                "material_resolution": detail.get('material_resolution'),
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
            if detail.get('optionality_conflict'):
                unclear.append(self._issue(status='unclear', title=item_name, message='招标对该材料的必交与可选声明冲突，需要人工确认。', evidence=evidence))
            elif detail.get('resolution_status') == 'unclear':
                unclear.append(self._issue(status='unclear', title=item_name, message='材料要求关系或定位依据不明确，需要人工复核。', evidence=evidence))
            elif detail.get("is_passed"):
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

        actual_check_count = (
            raw.get("scored_item_count")
            if isinstance(raw, dict) and isinstance(raw.get("scored_item_count"), int)
            else len(passed) + len(failed) + len(missing)
        )
        extracted_item_count = (
            raw.get('extracted_item_count')
            if isinstance(raw, dict) and isinstance(raw.get('extracted_item_count'), int)
            else len(details)
        )
        all_items_excluded = bool(raw.get('all_items_excluded'))
        excluded_optional_items = [
            str(item).strip()
            for item in raw.get('excluded_optional_items') or []
            if str(item).strip()
        ]
        extraction_failed = extracted_item_count == 0 and not all_items_excluded
        no_applicable_items = bool(
            extracted_item_count > 0
            and actual_check_count == 0
            and optional_skipped
            and not applicability_unclear
        )
        if extraction_failed:
            extraction_reason = str(
                raw.get('extraction_reason')
                or '未建立商务材料完整性检查项。'
            )
            unclear.append(
                self._issue(
                    status='unclear',
                    title='商务材料完整性检查项未建立',
                    message=extraction_reason,
                    evidence={
                        'extraction_status': raw.get('extraction_status') or 'unclear',
                        'template_locations': self._locations_with_document_role(
                            raw.get('structure_locations')
                            or raw.get('scope_locations')
                            or [],
                            'tender',
                        ),
                    },
                )
            )
        not_applicable = []
        review_status = self._combine_review_status([
            issue['status'] for issue in passed + failed + missing + unclear + not_applicable
        ])
        # 摘要使用“已命中/总数”的口径，避免直接使用“缺失 X 项”的表述。
        if extraction_failed:
            summary = '未建立完整性检查项，请复核招标文件商务材料组成提取结果。'
        elif all_items_excluded:
            summary = '招标文件中的商务材料均为可选项，已从完整性审查范围排除。'
        elif no_applicable_items:
            summary = '该项不适用：本次无必检项，已提取的材料均为明确可选且未提供。'
        else:
            summary = f"共提取 {extracted_item_count} 项，实际校验 {actual_check_count} 项，已命中 {len(passed)}/{actual_check_count} 项"
            if score is not None:
                summary += f"，完整性得分 {score}"
            summary += "。"
        excluded_names = list(dict.fromkeys([*excluded_optional_items, *optional_skipped]))
        if excluded_names and not all_items_excluded:
            summary += f" 另有 {len(excluded_names)} 个可选材料已从审查范围排除：" + '、'.join(excluded_names) + '。'
        if ignored_count > len(optional_skipped):
            summary += f" 另有 {ignored_count - len(optional_skipped)} 个条目不单独计分。"
        if applicability_unclear:
            summary += f' 另有 {len(applicability_unclear)} 项适用条件待确认。'
        return {
            "validation": {
                "status": "unclear" if extraction_failed else ("correct" if isinstance(details, dict) else "failed"),
                "reason": (
                    str(raw.get('extraction_reason') or '未建立商务材料完整性检查项。')
                    if extraction_failed
                    else "模块返回了完整性得分和逐项命中明细。"
                ),
            },
            "review": {
                "status": review_status,
                "summary": summary,
            },
            "metrics": {
                "integrity_score": score,
                "extracted_item_count": extracted_item_count,
                "applicable_item_count": actual_check_count,
                "actual_check_count": actual_check_count,
                "total_item_count": actual_check_count,
                "passed_item_count": len(passed),
                "failed_item_count": len(failed),
                "missing_item_count": len(missing),
                "ignored_item_count": ignored_count,
                "skipped_optional_item_count": len(excluded_names),
                "excluded_optional_item_count": len(excluded_names),
                "applicability_unclear_count": len(applicability_unclear),
                "unclear_item_count": len(unclear),
            },
            "issues": {
                "passed": passed,
                "failed": failed,
                "missing": missing,
                "unclear": unclear,
                "not_applicable": not_applicable,
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
        extraction_reason = str(
            (raw.get('extraction_reason') or '')
            if isinstance(raw, dict)
            else ''
        ).strip()
        all_items_excluded = bool(
            isinstance(raw, dict) and raw.get('all_items_excluded')
        )
        excluded_optional_items = [
            str(item).strip()
            for item in ((raw.get('excluded_optional_items') or []) if isinstance(raw, dict) else [])
            if str(item).strip()
        ]
        passed = []
        failed = []
        missing_items = []
        unclear_items = []
        not_applicable_items = []
        short_body_skipped = 0
        attachment_not_found_skipped = 0
        integrity_skipped = 0
        self_defined_skipped = 0
        fixed_item_counts = {"pass": 0, "fail": 0, "unclear": 0, "not_applicable": 0}
        unclear_reason_counts: dict[str, int] = {}

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
            if skip_type in {
                "self_defined_format",
                "alternative_not_provided",
                "integrity_attachment_missing",
            }:
                not_applicable_items.append(
                    self._issue(
                        status="not_applicable",
                        title=str(skipped.get("name") or "不适用模板附件"),
                        message="该项不适用：" + str(skip_reason.get("reason") or "该附件有明确依据不适用本次一致性比对。"),
                        evidence={
                            "skip_reason": skip_reason,
                            "engine_version": skipped.get("engine_version"),
                            "template_locations": self._locations_with_document_role(
                                skipped.get("template_locations") or [],
                                "tender",
                            ),
                        },
                    )
                )

        skip_types = {
            str((item.get('skip_reason') or {}).get('type') or '')
            for item in skipped_segments
            if isinstance(item, dict)
        }
        all_templates_explicitly_skipped = bool(skipped_segments) and not segments and skip_types.issubset({
            'self_defined_format',
            'optional_attachment_not_provided',
            'alternative_not_provided',
            'integrity_attachment_missing',
        })
        all_optional_templates_excluded = bool(skipped_segments) and not segments and skip_types == {
            'optional_attachment_not_provided'
        }

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
                "location_status": segment.get('location_status'),
                "location_candidates": segment.get('location_candidates') or [],
                "element_results": segment.get("element_results") or [],
                "difference_category": segment.get("difference_category"),
                "difference_items": segment.get("difference_items") or [],
                "coverage": segment.get("coverage") or {},
                "unclear_reasons": segment.get("unclear_reasons") or [],
                "unresolved_ranges": segment.get("unresolved_ranges") or [],
                "is_optional": bool(segment.get('is_optional')),
                "optionality_conflict": bool(segment.get('optionality_conflict')),
                "optionality_locations": self._locations_with_document_role(segment.get('optionality_locations') or [], 'tender'),
                "applicability_status": segment.get('applicability_status') or 'required',
                "condition_text": segment.get('condition_text') or '',
                "applicability_locations": self._locations_with_document_role(
                    segment.get('applicability_locations') or [],
                    'tender',
                ),
            }
            item_counts = (segment.get("coverage") or {}).get("item_status_counts") or {}
            if not item_counts:
                item_counts = {
                    status: sum(
                        str(item.get("status") or "").lower() == status
                        for item in segment.get("element_results") or []
                        if item.get("required", True) and item.get("enabled", True)
                    )
                    for status in fixed_item_counts
                }
            for status in fixed_item_counts:
                fixed_item_counts[status] += int(item_counts.get(status) or 0)
            for reason in segment.get("unclear_reasons") or []:
                code = str(reason.get("code") or "unknown")
                unclear_reason_counts[code] = (
                    unclear_reason_counts.get(code, 0)
                    + int(reason.get("affected_item_count") or 1)
                )
            segment_status = str(segment.get("status") or "").strip().lower()
            if segment_status not in {"pass", "fail", "missing", "unclear", "not_applicable", "skipped"}:
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
            elif segment_status in {"fail", "missing"}:
                parts = []
                if missing:
                    parts.append(f"缺少模板关键内容：{self._join_text(missing)}")
                difference_count = len({
                    str(item.get("item_id") or index)
                    for index, item in enumerate(segment.get("difference_items") or [])
                    if item.get("status") == "fail"
                })
                if difference_count:
                    parts.append(f"{difference_count} 个固定内容项存在差异")
                issue_status = "fail" if segment_status == "fail" else "missing"
                issue = self._issue(
                    status=issue_status,
                    title=title,
                    message="；".join(parts) or "模板正文固定内容疑似被修改。",
                    evidence=evidence,
                )
                (failed if segment_status == "fail" else missing_items).append(issue)
            elif segment_status == "unclear":
                reason_messages = [
                    str(reason.get("message") or "").strip()
                    for reason in segment.get("unclear_reasons") or []
                    if str(reason.get("message") or "").strip()
                ]
                unclear_items.append(
                    self._issue(
                        status="unclear",
                        title=title,
                        message=("招标对该附件的必交与可选声明冲突，需要人工确认。"
                                 if segment.get('optionality_conflict')
                                 else "该附件的适用条件尚未确认，且未定位到对应投标内容。"
                                 if segment.get('applicability_status') == 'unclear'
                                 else "存在多个附件候选，对应附件待确认。" if segment.get('location_status') == 'ambiguous'
                                 else "未定位到对应投标内容，需要人工复核。" if segment.get('location_status') == 'not_found'
                                 else "；".join(reason_messages[:3])
                                 if reason_messages
                                 else "模板骨架存在疑似改写或对齐不确定项，需要人工复核。"),
                        evidence=evidence,
                    )
                )
            elif segment_status in {"not_applicable", "skipped"}:
                not_applicable_items.append(
                    self._issue(
                        status="not_applicable",
                        title=title,
                        message="该项不适用：该模板段按适用性规则跳过。",
                        evidence=evidence,
                    )
                )

        has_results = bool(segments or skipped_segments)
        if not has_results and not all_items_excluded:
            unclear_items.append(
                self._issue(
                    status='unclear',
                    title='模板一致性比对段未建立',
                    message=extraction_reason or '未提取到可比较的模板段，需人工复核模板抽取是否成功。',
                    evidence={
                        'extraction_status': raw.get('extraction_status') if isinstance(raw, dict) else 'unclear',
                        'template_locations': self._locations_with_document_role(
                            raw.get('structure_locations') or [] if isinstance(raw, dict) else [],
                            'tender',
                        ),
                    },
                )
            )
        elif not segments and not all_templates_explicitly_skipped:
            unclear_items.append(
                self._issue(
                    status='unclear',
                    title='模板一致性未实际比对',
                    message='模板段已提取，但因正文不足或附件定位不稳定，当前没有可确认的比对结论。',
                    evidence={'skip_types': sorted(skip_types)},
                )
            )
        if all_items_excluded or all_optional_templates_excluded:
            validation_status = "correct"
            validation_reason = "可选材料已在建立模板比对段前从审查范围排除。"
        elif skipped_segments:
            validation_status = "correct"
            validation_reason = "模块返回了逐模板段的一致性结果，并已跳过正文过短或完整性缺失的附件。"
        else:
            validation_status = "correct" if segments else "unclear"
            validation_reason = (
                "模块返回了逐模板段的正文固定内容比对结果。"
                if segments
                else extraction_reason or "未提取到可比较的模板段，需人工复核模板抽取是否成功。"
            )

        if all_items_excluded or all_optional_templates_excluded:
            review_status = 'pass'
        elif all_templates_explicitly_skipped:
            review_status = 'not_applicable'
        elif has_results:
            review_status = self._combine_review_status(
                [
                    issue["status"]
                    for issue in passed + failed + missing_items + unclear_items + not_applicable_items
                ]
            )
        else:
            review_status = "unclear"

        if all_items_excluded or all_optional_templates_excluded:
            total_segments = 0
            summary = (
                f"{len(excluded_optional_items) or len(skipped_segments)} 个可选材料已从模板一致性审查范围排除。"
            )
        elif all_templates_explicitly_skipped:
            total_segments = original_segment_count or len(skipped_segments)
            summary = f"该项不适用：共提取 {total_segments} 个模板段，均按明确适用性规则跳过。"
        elif has_results:
            total_segments = original_segment_count or (len(segments) + len(skipped_segments))
            # 一致性摘要统一改成“已通过/已校验”的数量表达。
            summary = (
                f"共比对 {total_segments} 个模板段，实际校验 {len(segments)} 个，"
                f"一致 {len(passed)} 个，不一致 {len(failed) + len(missing_items)} 个，"
                f"待核验 {len(unclear_items)} 个，不适用 {len(not_applicable_items)} 个；"
                f"正文不足20字跳过 {short_body_skipped} 个，"
                f"格式自拟跳过 {self_defined_skipped} 个，"
                f"附件未稳定定位跳过 {attachment_not_found_skipped} 个，"
                f"因完整性结果跳过 {integrity_skipped} 个。"
            )
            summary += (
                f" 固定内容项一致 {fixed_item_counts['pass']} 项，"
                f"不一致 {fixed_item_counts['fail']} 项，"
                f"待核验 {fixed_item_counts['unclear']} 项，"
                f"不适用 {fixed_item_counts['not_applicable']} 项。"
            )
        else:
            summary = extraction_reason or "未提取到可比较的模板段。"

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
                "not_applicable_segment_count": len(not_applicable_items),
                "fixed_item_count": sum(fixed_item_counts.values()),
                "passed_fixed_item_count": fixed_item_counts["pass"],
                "failed_fixed_item_count": fixed_item_counts["fail"],
                "unclear_fixed_item_count": fixed_item_counts["unclear"],
                "not_applicable_fixed_item_count": fixed_item_counts["not_applicable"],
                "excluded_optional_item_count": len(excluded_optional_items),
                "unclear_reason_counts": unclear_reason_counts,
                "fillable_field_count": 0,
                "unfilled_field_count": 0,
            },
            "issues": {
                "passed": passed,
                "failed": failed,
                "missing": missing_items,
                "unclear": unclear_items,
                "not_applicable": not_applicable_items,
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
            if subcheck_code == "price_reasonableness" and payload.get("quote_mode") == "rate":
                label = "费率是否符合招标规则"
            summary_text = self._join_text(payload.get("summary"))
            raw_subcheck_status = payload.get("status")
            status = self._map_generic_status(raw_subcheck_status) if raw_subcheck_status in {"pass", "fail", "missing", "unclear", "not_applicable", "skipped", "optional"} else self._map_price_result(payload.get("result"), summary_text)
            message = summary_text or "未返回明确结论。"
            if str(raw_subcheck_status or "").strip().lower() in {"not_applicable", "skipped", "optional"}:
                message = "该项不适用：" + message
            message_parts.append(f"{label}：{message}")

            subcheck_locations = [
                dict(location)
                for location in payload.get("locations") or []
                if isinstance(location, dict)
            ]
            for location in subcheck_locations:
                role = location_role(location)
                if role == "business_bid" or (subcheck_code == "price_reasonableness" and role != "tender"):
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
        review_status = self._combine_review_status([subcheck["status"] for subcheck in subchecks])
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
                "not_applicable_subcheck_count": 0,
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
            normalized_status = self._map_generic_status(sub_status)
            label = subcheck_labels.get(subcheck_code, subcheck_code)
            message = self._summarize_itemized_subcheck(subcheck_code, payload)
            if sub_status in {"not_applicable", "skipped", "optional"}:
                message = "该项不适用：" + message
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
            is_self_declared = item.get("marker_type") == "self_declared"
            failed.append(
                self._issue(
                    status="fail",
                    title=item.get("requirement") or "负偏离条款",
                    message=(
                        "偏离表主动声明负偏离："
                        if is_self_declared
                        else "检测到负偏离："
                    ) + str(item.get("response_evidence") or "未提供详细证据"),
                    evidence=evidence,
                )
            )
        for item in unclear_items:
            evidence = self._deviation_issue_evidence(item, raw)
            is_self_declared = item.get("marker_type") == "self_declared"
            unclear.append(
                self._issue(
                    status="unclear",
                    title=item.get("requirement") or "响应不明确条款",
                    message=(
                        "偏离表已填写偏离说明，需确认偏离性质："
                        if is_self_declared
                        else "响应内容不明确："
                    ) + str(item.get("response_evidence") or "未提供详细证据"),
                    evidence=evidence,
                )
            )

        # 评分项(△/▲)未达标：仅提示、不计入合规失败，但要逐条列出供人工/模型确认。
        bonus: list[dict[str, Any]] = []
        for item in (raw.get("bonus_flagged_items") or []):
            evidence = self._deviation_issue_evidence(item, raw)
            bonus.append(
                self._issue(
                    status="warning",
                    title=item.get("requirement") or "评分项(△/▲)未达标",
                    message="评分项(△/▲)未响应或存在偏离，建议人工确认（不计入合规失败）。",
                    evidence=evidence,
                )
            )

        # 响应正确(无问题)的 ★/△/▲ 项也逐条展示，便于逐项核对。
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
        self_declared_count = int(raw.get("self_declared_deviation_count") or 0)
        star_count = int(raw.get("core_star_requirements_count") or 0)
        if self_declared_count:
            review_summary = (
                f"共核验 {star_count} 条★要求，并检查到投标人主动填写偏离 "
                f"{self_declared_count} 条；负偏离 {len(negative_items)} 条，"
                f"需复核 {len(unclear_items)} 条。"
            )
        else:
            review_summary = (
                f"共核验 {int(total_requirements or 0)} 条带★要求，"
                f"已明确响应 {covered_count}/{int(total_requirements or 0)} 条，"
                f"负偏离 {len(negative_items)} 条，不明确 {len(unclear_items)} 条。"
            )

        return {
            "validation": {
                "status": "correct",
                "reason": "模块返回了星标条款响应及投标人主动声明偏离的分类与定位结果。",
            },
            "review": {
                "status": review_status,
                "summary": review_summary,
            },
            "metrics": {
                "core_requirements_count": raw.get("core_requirements_count"),
                "mandatory_requirements_count": raw.get("mandatory_requirements_count"),
                "bonus_requirements_count": raw.get("bonus_requirements_count"),
                "self_declared_deviation_count": self_declared_count,
                "self_declared_negative_count": raw.get("self_declared_negative_count", 0),
                "self_declared_unclear_count": raw.get("self_declared_unclear_count", 0),
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
        """把每一个 ★/△/▲ 标记项投影成精简列表，确保前端能逐条展示并人工/模型确认。"""
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
        evidence['location_status'] = item.get('location_status')
        evidence['location_candidates'] = item.get('location_candidates') or []
        if isinstance(item.get('found'), bool):
            evidence['bid_content_found'] = item['found']

        for key in (
            "pages", "locations", "attachment_number", "matched_bid_title", "template_locations",
            "applicability_status", "applicability_reason_code", "applicability_evidence",
            "verification_rule_version",
        ):
            value = item.get(key)
            if value not in (None, "", []):
                evidence[key] = (
                    self._locations_with_document_role(value, "tender")
                    if key == "template_locations"
                    else self._locations_with_document_role(value, 'business_bid') if key == 'locations' else value
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
        not_applicable: list[dict[str, Any]] = []
        missing_attachments = position_check.get("missing_attachments") or []
        missing_signature = position_check.get("missing_signature_attachments") or []
        pending_signature = position_check.get("pending_signature_attachments") or []
        missing_seal = position_check.get("missing_seal_attachments") or []
        missing_date = date_check.get("missing_date_attachments") or []
        late_date = date_check.get("late_date_attachments") or []
        attachment_lookup = self._verification_attachment_lookup(raw)
        attachment_results_by_key: dict[str, dict[str, Any]] = {}

        def attachment_key(value: Any) -> str:
            if isinstance(value, dict):
                number = str(value.get('attachment_number') or '').strip()
                title_value = str(value.get('title') or value.get('matched_bid_title') or '').strip()
            else:
                number = ''
                title_value = str(value or '').strip()
            if not number:
                matched = re.search(r'(?:附件|附表)\s*(\d+(?:\s*[-－–—]\s*\d+)*)', title_value)
                number = re.sub(r'\s*[－–—]\s*', '-', matched.group(1)) if matched else ''
            if number:
                return 'number:' + number
            normalized = re.sub(r'[\s：:；;，,。()（）【】\[\]]+', '', title_value)
            normalized = re.sub(r'^(?:附件|附表)', '', normalized)
            return 'title:' + normalized

        # Legacy payloads can place the same attachment in both arrays.  Keep
        # one effective row, preferring the normal attachment result because it
        # carries the component-level evidence.
        for item in raw.get("missing_attachment_results") or []:
            if isinstance(item, dict):
                attachment_results_by_key[attachment_key(item)] = item
        for item in raw.get("attachment_results") or []:
            if isinstance(item, dict):
                attachment_results_by_key[attachment_key(item)] = item
        attachment_results = list(attachment_results_by_key.values())
        handled_attachment_keys: set[str] = set()

        def status_of(value: Any) -> str:
            return str(value or "").strip().lower()

        def attachment_title(item: dict[str, Any]) -> str:
            return str(item.get("title") or item.get("matched_bid_title") or item.get("attachment_number") or "签字盖章日期审查").strip()

        def component_status(item: dict[str, Any], key: str) -> str:
            value = item.get(key)
            return status_of(value.get("status") if isinstance(value, dict) else None)

        def effective_attachment_status(item: dict[str, Any]) -> str:
            if (item.get('requirements') or {}).get('optionality_conflict'):
                return 'pending'
            if (
                status_of(item.get('status')) in {'not_applicable', 'skipped', 'optional'}
                or item.get('applicability_status') == 'not_applicable'
                or (item.get('requirements') or {}).get('applicability_status') == 'not_applicable'
            ):
                return 'not_applicable'
            if (item.get('requirements') or {}).get('applicability_status') == 'unclear' and item.get('found') is None:
                return 'pending'
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
            if (
                status_of(item.get('status')) in {'not_applicable', 'skipped', 'optional'}
                or
                item.get('applicability_status') == 'not_applicable'
                or (item.get('requirements') or {}).get('applicability_status') == 'not_applicable'
            ):
                evidence = item.get('applicability_evidence') or {}
                reason = str(evidence.get('text') or evidence.get('reason') or item.get('message') or '当前材料不适用该检查').strip()
                return [reason if reason.startswith('该项不适用') else '该项不适用：' + reason]
            if item.get('location_status') == 'ambiguous':
                return ['存在多个附件候选，对应附件待确认']
            if item.get('location_status') == 'not_found':
                return ['未定位到对应附件，签字、盖章及日期待人工复核']
            if (item.get('requirements') or {}).get('optionality_conflict'):
                details.append('招标的必交与可选声明冲突')
            if (item.get('requirements') or {}).get('applicability_status') == 'unclear':
                details.append('该附件的适用条件尚未确认')
            from ..verification_evidence import component_message
            for kind in ('signature', 'seal', 'date'):
                check = item.get(kind + '_check') or {}
                if check.get('status') not in ('pass', 'not_required', 'not_applicable', 'skipped'):
                    details.append(component_message(kind, check))
            return details

        for item in attachment_results:
            title = attachment_title(item)
            if not title:
                continue
            handled_attachment_keys.add(attachment_key(item))
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
                        message=(
                            "未定位到对应附件，签字、盖章及日期待人工复核。"
                            if item.get('location_status') == 'not_found'
                            else "附件签字盖章日期要求待复核：" + ("；".join(details) if details else "存在待确认项") + "。"
                        ),
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
            elif status in {"not_applicable", "skipped", "optional"}:
                not_applicable.append(
                    self._issue(
                        status="not_applicable",
                        title=title,
                        message="；".join(details) or "该项不适用。",
                        evidence=evidence,
                    )
                )
        seal_company_status = status_of(seal_company_check.get("status"))
        if seal_company_status == "fail":
            failed.append(
                self._issue(status="fail", title="公章与投标人匹配", message="检测到的公章与投标人名称不匹配。", evidence=seal_company_check)
            )

        for attachment in missing_attachments:
            if attachment_key(attachment) in handled_attachment_keys:
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
            if attachment_key(attachment) in handled_attachment_keys:
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
            if attachment_key(attachment) in handled_attachment_keys:
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
            if attachment_key(attachment) in handled_attachment_keys:
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
            if attachment_key(attachment) in handled_attachment_keys:
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
            if attachment_key(attachment) in handled_attachment_keys:
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

        from ..verification_evidence import attachment_counts, attachment_summary
        verification_counts = attachment_counts(raw)
        review_summary = attachment_summary(verification_counts)
        if seal_company_check:
            review_summary += (
                " 公章单位匹配已确认。"
                if seal_company_check.get("status") == "pass"
                else " 公章单位匹配需进一步确认。"
            )

        issue_statuses = [issue["status"] for issue in passed + failed + missing + unclear + not_applicable]
        review_status = self._combine_review_status(issue_statuses) if issue_statuses else compliance_status

        return {
            "validation": {"status": "correct", "reason": "模块返回了附件级签字、盖章、日期和公章匹配结果。"},
            "review": {"status": review_status, "summary": review_summary},
            "metrics": {
                **verification_counts,
                "required_attachment_count": raw.get("required_attachment_count"),
                "missing_attachment_count": len(missing_attachments),
                "missing_signature_count": len(missing_signature),
                "pending_signature_count": len(pending_signature),
                "missing_seal_count": len(missing_seal),
                "missing_date_count": len(missing_date),
                "late_date_count": len(late_date),
            },
            "issues": {"passed": passed, "failed": failed, "missing": missing, "unclear": unclear, "not_applicable": not_applicable},
        }
