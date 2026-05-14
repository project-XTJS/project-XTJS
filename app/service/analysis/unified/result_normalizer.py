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

    def _normalize_integrity(self, raw: dict[str, Any]) -> dict[str, Any]:
        """标准化完整性审查原始结果。"""
        details = raw.get("details", {}) if isinstance(raw, dict) else {}
        score = raw.get("integrity_score") if isinstance(raw, dict) else None
        ignored_count = raw.get("ignored_item_count", 0) if isinstance(raw, dict) else 0

        passed = []
        failed = []
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
                failed.append(
                    self._issue(
                        status="fail",
                        title=item_name,
                        message="未在商务标中找到该必备项。",
                        evidence=evidence,
                    )
                )

        total = (
            raw.get("scored_item_count")
            if isinstance(raw, dict) and isinstance(raw.get("scored_item_count"), int)
            else len(passed) + len(failed)
        )
        review_status = "pass" if not failed else "fail"
        summary = f"完整性得分 {score}，共校验 {total} 项，缺失 {len(failed)} 项。"
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
                "ignored_item_count": ignored_count,
            },
            "issues": {
                "passed": passed,
                "failed": failed,
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
        short_body_skipped = 0
        attachment_not_found_skipped = 0
        integrity_skipped = 0

        for skipped in skipped_segments:
            skip_reason = skipped.get("skip_reason") or {}
            skip_type = str(skip_reason.get("type") or "")
            if skip_type == "body_too_short":
                short_body_skipped += 1
            elif skip_type in {"attachment_not_found", "optional_attachment_not_provided"}:
                attachment_not_found_skipped += 1
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
                failed.append(
                    self._issue(
                        status="fail",
                        title=title,
                        message="；".join(parts) or "模板正文固定内容疑似被修改。",
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

        if failed:
            review_status = "fail"
        elif has_results:
            review_status = "pass"
        else:
            review_status = "unclear"

        if has_results:
            total_segments = original_segment_count or (len(segments) + len(skipped_segments))
            summary = (
                f"共比对 {total_segments} 个模板段，实际校验 {len(segments)} 个，"
                f"因正文不足{20}字跳过 {short_body_skipped} 个，"
                f"因附件未稳定定位跳过 {attachment_not_found_skipped} 个，"
                f"因完整性缺失跳过 {integrity_skipped} 个，通过 {len(passed)} 个，"
                f"存在缺漏 {len(failed)} 个。"
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
                "attachment_not_found_skipped_count": attachment_not_found_skipped,
                "integrity_skipped_count": integrity_skipped,
                "passed_segment_count": len(passed),
                "failed_segment_count": len(failed),
                "fillable_field_count": 0,
                "unfilled_field_count": 0,
            },
            "issues": {"passed": passed, "failed": failed, "unclear": []},
        }

    def _normalize_pricing(self, raw: dict[str, Any]) -> dict[str, Any]:
        """标准化报价审查的原始结果。"""
        self_check = raw.get("self_check", {}) if isinstance(raw, dict) else {}
        tender_limit_check = raw.get("tender_limit_check", {}) if isinstance(raw, dict) else {}

        passed: list[dict[str, Any]] = []
        failed: list[dict[str, Any]] = []
        unclear: list[dict[str, Any]] = []

        for subcheck_code, title, payload in (
            ("price_reasonableness", "投标总价自检", self_check),
            ("tender_limit_check", "招标限价校验", tender_limit_check),
        ):
            summary_text = self._join_text(payload.get("summary"))
            status = self._map_price_result(payload.get("result"), summary_text)
            issue = self._issue(
                status=status,
                title=title,
                message=summary_text or "未返回明确结论。",
                evidence={
                    "result": payload.get("result"),
                    "type": payload.get("type"),
                    "summary": payload.get("summary"),
                    "subcheck_code": subcheck_code,
                },
            )
            if status == "pass":
                passed.append(issue)
            elif status == "fail":
                failed.append(issue)
            else:
                unclear.append(issue)

        review_status = self._combine_review_status([issue["status"] for issue in passed + failed + unclear])
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
                "unclear_subcheck_count": len(unclear),
            },
            "issues": {"passed": passed, "failed": failed, "unclear": unclear},
        }

    def _normalize_itemized(self, raw: dict[str, Any]) -> dict[str, Any]:
        """标准化分项报价审查的原始结果。"""
        top_status = self._map_generic_status(raw.get("status"))
        checks = raw.get("checks", {}) if isinstance(raw, dict) else {}
        manual_review = raw.get("manual_review", {}) if isinstance(raw, dict) else {}

        passed: list[dict[str, Any]] = []
        failed: list[dict[str, Any]] = []
        unclear: list[dict[str, Any]] = []

        subcheck_labels = {
            "row_arithmetic": "分项行算术校验",
            "sum_consistency": "分项汇总一致性校验",
            "duplicate_items": "疑似重复报价校验",
            "missing_item": "招标列项缺失校验",
        }

        for subcheck_code, payload in checks.items():
            sub_status = str(payload.get("status") or "").strip().lower()
            if sub_status == "not_applicable":
                continue

            normalized_status = self._map_generic_status(sub_status)
            label = subcheck_labels.get(subcheck_code, subcheck_code)
            message = self._summarize_itemized_subcheck(subcheck_code, payload)
            issue = self._issue(status=normalized_status, title=label, message=message, evidence=payload)
            if normalized_status == "pass":
                passed.append(issue)
            elif normalized_status == "fail":
                failed.append(issue)
            else:
                unclear.append(issue)

        if manual_review.get("required"):
            unclear.append(
                self._issue(
                    status="unclear",
                    title="人工复核提示",
                    message="分项报价识别存在歧义，建议人工核对识别总价和未完整识别行。",
                    evidence=manual_review,
                )
            )

        validation_status = "correct"
        validation_reason = "模块返回了分项报价校验明细。"
        if top_status == "unclear":
            validation_status = "unclear"
            validation_reason = "模块已执行，但当前样本存在未完整识别的分项行，结论需人工复核。"

        return {
            "validation": {"status": validation_status, "reason": validation_reason},
            "review": {
                "status": top_status,
                "summary": str(raw.get("summary") or "未返回分项报价结论。"),
            },
            "metrics": {
                "itemized_table_detected": raw.get("itemized_table_detected"),
                "passed_subcheck_count": len(passed),
                "failed_subcheck_count": len(failed),
                "unclear_subcheck_count": len(unclear),
            },
            "issues": {"passed": passed, "failed": failed, "unclear": unclear},
        }

    def _normalize_deviation(self, raw: dict[str, Any]) -> dict[str, Any]:
        """标准化偏离条款审查的原始结果。"""
        compliance_status = self._map_generic_status(raw.get("compliance_status"))
        missing_items = raw.get("missing_response_items", []) if isinstance(raw, dict) else []
        negative_items = raw.get("negative_deviation_items", []) if isinstance(raw, dict) else []
        unclear_items = raw.get("unclear_response_items", []) if isinstance(raw, dict) else []

        passed: list[dict[str, Any]] = []
        failed: list[dict[str, Any]] = []
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
            failed.append(
                self._issue(
                    status="fail",
                    title=item.get("requirement") or "缺失响应条款",
                    message="未找到对应响应内容。",
                    evidence=item,
                )
            )
        for item in negative_items:
            failed.append(
                self._issue(
                    status="fail",
                    title=item.get("requirement") or "负偏离条款",
                    message=f"检测到负偏离：{item.get('response_evidence') or '未提供详细证据'}",
                    evidence=item,
                )
            )
        for item in unclear_items:
            unclear.append(
                self._issue(
                    status="unclear",
                    title=item.get("requirement") or "响应不明确条款",
                    message=f"响应内容不明确：{item.get('response_evidence') or '未提供详细证据'}",
                    evidence=item,
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

        return {
            "validation": {
                "status": "correct",
                "reason": "模块返回了星标条款抽取、响应匹配和偏离分类结果。",
            },
            "review": {
                "status": compliance_status,
                "summary": str(raw.get("summary") or "未返回偏离条款结论。"),
            },
            "metrics": {
                "core_requirements_count": raw.get("core_requirements_count"),
                "missing_count": len(missing_items),
                "negative_deviation_count": len(negative_items),
                "unclear_deviation_count": len(unclear_items),
            },
            "issues": {"passed": passed, "failed": failed, "unclear": unclear},
        }

    def _normalize_verification(self, raw: dict[str, Any]) -> dict[str, Any]:
        """标准化签字盖章日期审查的原始结果。"""
        compliance_status = self._map_generic_status(raw.get("compliance_status"))
        position_check = raw.get("position_check", {}) if isinstance(raw, dict) else {}
        date_check = raw.get("date_check", {}) if isinstance(raw, dict) else {}
        seal_company_check = raw.get("seal_company_check", {}) if isinstance(raw, dict) else {}

        passed: list[dict[str, Any]] = []
        failed: list[dict[str, Any]] = []
        unclear: list[dict[str, Any]] = []

        missing_attachments = position_check.get("missing_attachments") or []
        missing_signature = position_check.get("missing_signature_attachments") or []
        pending_signature = position_check.get("pending_signature_attachments") or []
        missing_seal = position_check.get("missing_seal_attachments") or []
        missing_date = date_check.get("missing_date_attachments") or []
        late_date = date_check.get("late_date_attachments") or []

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
                failed.append(
                    self._issue(status="fail", title=attachment, message="未找到要求签章的附件。", evidence={"attachment": attachment, "source": "position_check"})
                )
            for attachment in missing_signature:
                failed.append(
                    self._issue(status="fail", title=attachment, message="附件缺少签字。", evidence={"attachment": attachment, "source": "position_check"})
                )
            for attachment in missing_seal:
                failed.append(
                    self._issue(status="fail", title=attachment, message="附件缺少盖章。", evidence={"attachment": attachment, "source": "position_check"})
                )

        for attachment in pending_signature:
            unclear.append(
                self._issue(status="unclear", title=attachment, message="签字字段处于待填写状态，建议人工复核。", evidence={"attachment": attachment, "source": "position_check"})
            )

        if date_check.get("status") == "pass":
            passed.append(
                self._issue(status="pass", title="落款日期检查", message="已检测到落款日期，且均未晚于投标截止时间。", evidence=date_check)
            )
        else:
            for attachment in missing_date:
                failed.append(
                    self._issue(status="fail", title=attachment, message="附件缺少落款日期。", evidence={"attachment": attachment, "source": "date_check"})
                )
            for attachment in late_date:
                failed.append(
                    self._issue(status="fail", title=attachment, message="附件落款日期晚于招标截止时间。", evidence={"attachment": attachment, "source": "date_check"})
                )

        return {
            "validation": {"status": "correct", "reason": "模块返回了附件级签字、盖章、日期和公章匹配结果。"},
            "review": {"status": compliance_status, "summary": str(raw.get("summary") or "未返回签字盖章日期结论。")},
            "metrics": {
                "required_attachment_count": raw.get("required_attachment_count"),
                "missing_attachment_count": len(missing_attachments),
                "missing_signature_count": len(missing_signature),
                "pending_signature_count": len(pending_signature),
                "missing_seal_count": len(missing_seal),
                "missing_date_count": len(missing_date),
                "late_date_count": len(late_date),
            },
            "issues": {"passed": passed, "failed": failed, "unclear": unclear},
        }
