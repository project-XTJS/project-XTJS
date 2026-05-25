# unified/consistency_filter.py
"""
统一商务标审查 - 一致性过滤与完整性去重 Mixin

提供一致性检查的过滤逻辑，以及根据完整性缺失结果对验证结果进行去重处理。
"""

from __future__ import annotations

import copy
import re
import time
from typing import Any


class ConsistencyFilterMixin:
    """
    一致性过滤与完整性去重相关的所有方法。

    依赖：
    - 实例属性：consistency_checker, ATTACHMENT_REF_RE
    - 其他 Mixin：_issue, _empty_issue_bucket, _normalize_consistency, _normalize_verification,
                 _normalize_match_text, _simplify_integrity_item_title, _extract_attachment_refs
    """

    consistency_checker: Any
    ATTACHMENT_REF_RE: Any

    # 一致性检查执行与过滤
    def _execute_consistency_check(
        self,
        *,
        tender_payload: dict[str, Any],
        business_payload: dict[str, Any],
        integrity_check: dict[str, Any],
    ) -> dict[str, Any]:
        """执行模板一致性审查，并依据完整性缺失对结果进行过滤。"""
        started = time.perf_counter()
        check_code = "consistency_check"
        check_name = "模板一致性审查"
        try:
            integrity_raw = integrity_check.get("raw_result")
            raw_segments = self.consistency_checker.compare_raw_data(
                tender_payload,
                business_payload,
                integrity_raw=integrity_raw,
            )
            evaluated_segments, skipped_segments = self._filter_consistency_segments(
                raw_segments,
                integrity_raw,
            )
            raw_result = {
                "evaluated_segments": evaluated_segments,
                "skipped_segments": skipped_segments,
                "original_segment_count": len(raw_segments) if isinstance(raw_segments, list) else 0,
            }
            normalized = self._normalize_consistency(raw_result)
            execution = {
                "status": "ok",
                "duration_ms": round((time.perf_counter() - started) * 1000, 2),
            }
            return {
                "check_code": check_code,
                "check_name": check_name,
                "execution": execution,
                "validation": normalized["validation"],
                "review": normalized["review"],
                "metrics": normalized.get("metrics", {}),
                "issues": normalized.get("issues", self._empty_issue_bucket()),
                "raw_result": raw_result,
            }
        except Exception as exc:
            duration_ms = round((time.perf_counter() - started) * 1000, 2)
            issue = self._issue(
                status="fail",
                title=f"{check_name}执行失败",
                message=str(exc),
                evidence={"error_type": exc.__class__.__name__},
            )
            return {
                "check_code": check_code,
                "check_name": check_name,
                "execution": {
                    "status": "error",
                    "duration_ms": duration_ms,
                    "error_type": exc.__class__.__name__,
                    "error_message": str(exc),
                },
                "validation": {
                    "status": "failed",
                    "reason": "审查模块执行异常，未产出可用结果。",
                },
                "review": {
                    "status": "unclear",
                    "summary": f"{check_name}执行失败，当前无法得出可靠结论。",
                },
                "metrics": {},
                "issues": {
                    "passed": [],
                    "failed": [issue],
                    "missing": [],
                    "unclear": [],
                },
                "raw_result": None,
            }

    def _filter_consistency_segments(
        self,
        raw_segments: Any,
        integrity_raw: Any,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """根据完整性缺失结果过滤一致性段，将缺失的附件标记为跳过。"""
        segments = raw_segments if isinstance(raw_segments, list) else []
        evaluated: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []

        for segment in segments:
            existing_skip_reason = segment.get("skip_reason") if isinstance(segment, dict) else None
            if existing_skip_reason is not None:
                skipped.append(copy.deepcopy(segment))
                continue
            skip_reason = self._match_integrity_failure_for_segment(segment, integrity_raw)
            if skip_reason is None:
                evaluated.append(segment)
                continue
            skipped.append(
                {
                    **copy.deepcopy(segment),
                    "skip_reason": skip_reason,
                }
            )

        return evaluated, skipped

    def _match_integrity_failure_for_segment(
        self,
        segment: dict[str, Any],
        integrity_raw: Any,
    ) -> dict[str, Any] | None:
        """判断一个一致性段是否对应一个完整性缺失项，若是则返回跳过原因。"""
        details = integrity_raw.get("details", {}) if isinstance(integrity_raw, dict) else {}
        segment_title = str(segment.get("name") or "")
        normalized_segment_title = self._normalize_match_text(segment_title)
        segment_ref_keys = self._attachment_ref_keys(segment_title)

        for item_name, detail in details.items():
            if not isinstance(detail, dict):
                continue
            if detail.get("is_passed") or not detail.get("scored", True):
                continue

            missing_ref_keys = self._attachment_ref_keys(item_name)
            if segment_ref_keys and missing_ref_keys:
                matched_refs = sorted(segment_ref_keys & missing_ref_keys)
                if matched_refs:
                    return {
                        "type": "integrity_attachment_missing",
                        "integrity_item": item_name,
                        "integrity_status": detail.get("status"),
                        "matched_tokens": matched_refs,
                    }
                continue

            missing_tokens = self._integrity_missing_tokens(item_name, detail)
            if not missing_tokens:
                continue

            if any(token in normalized_segment_title for token in missing_tokens):
                return {
                    "type": "integrity_attachment_missing",
                    "integrity_item": item_name,
                    "integrity_status": detail.get("status"),
                    "matched_tokens": sorted(missing_tokens),
                }

        return None

    def _integrity_missing_tokens(self, item_name: str, detail: dict[str, Any]) -> set[str]:
        """从完整性缺失项中提取用于匹配的 token 集合。"""
        tokens: set[str] = set()

        # 这里只保留当前完整性结果中的附件编号和标题，不再兼容历史合并附件状态。
        for attachment_ref in self._extract_attachment_refs(item_name):
            tokens.add(self._normalize_match_text(attachment_ref))

        simplified_title = self._simplify_integrity_item_title(item_name)
        if simplified_title:
            tokens.add(self._normalize_match_text(simplified_title))

        return {token for token in tokens if token}

    # 完整性去重（验证检查）
    def _suppress_integrity_duplicates_in_verification(
        self,
        *,
        verification_check: dict[str, Any],
        integrity_check: dict[str, Any],
    ) -> dict[str, Any]:
        """移除签字盖章审查中已在完整性阶段确认为缺失的附件，避免重复报错。"""
        raw_result = verification_check.get("raw_result")
        integrity_raw = integrity_check.get("raw_result")
        if not isinstance(raw_result, dict) or not isinstance(integrity_raw, dict):
            return verification_check

        filtered_raw = self._filter_verification_raw_result(raw_result, integrity_raw)
        if filtered_raw == raw_result:
            return verification_check

        normalized = self._normalize_verification(filtered_raw)
        return {
            **verification_check,
            "validation": normalized["validation"],
            "review": normalized["review"],
            "metrics": normalized.get("metrics", {}),
            "issues": normalized.get("issues", self._empty_issue_bucket()),
            "raw_result": filtered_raw,
        }

    def _filter_verification_raw_result(
        self,
        raw_result: dict[str, Any],
        integrity_raw: dict[str, Any],
    ) -> dict[str, Any]:
        """从验证结果中过滤掉已由完整性检查标记为缺失的附件标题。"""
        filtered = copy.deepcopy(raw_result)
        suppressed: list[dict[str, Any]] = []

        def filter_titles(values: Any, source: str) -> list[str]:
            kept: list[str] = []
            seen: set[str] = set()
            for value in values or []:
                title = str(value or "").strip()
                if not title or title in seen:
                    continue
                seen.add(title)
                skip_reason = self._verification_skip_reason_from_integrity(title, integrity_raw)
                if skip_reason is None:
                    kept.append(title)
                    continue
                suppressed.append(
                    {
                        "attachment": title,
                        "source": source,
                        **skip_reason,
                    }
                )
            return kept

        position_check = filtered.get("position_check")
        if not isinstance(position_check, dict):
            position_check = {}
            filtered["position_check"] = position_check
        date_check = filtered.get("date_check")
        if not isinstance(date_check, dict):
            date_check = {}
            filtered["date_check"] = date_check

        position_check["missing_attachments"] = filter_titles(
            position_check.get("missing_attachments"),
            "position_check.missing_attachments",
        )
        position_check["missing_signature_attachments"] = filter_titles(
            position_check.get("missing_signature_attachments"),
            "position_check.missing_signature_attachments",
        )
        position_check["pending_signature_attachments"] = filter_titles(
            position_check.get("pending_signature_attachments"),
            "position_check.pending_signature_attachments",
        )
        position_check["missing_seal_attachments"] = filter_titles(
            position_check.get("missing_seal_attachments"),
            "position_check.missing_seal_attachments",
        )
        date_check["missing_date_attachments"] = filter_titles(
            date_check.get("missing_date_attachments"),
            "date_check.missing_date_attachments",
        )
        date_check["late_date_attachments"] = filter_titles(
            date_check.get("late_date_attachments"),
            "date_check.late_date_attachments",
        )
        filtered["skipped_missing_attachments"] = filter_titles(
            filtered.get("skipped_missing_attachments"),
            "skipped_missing_attachments",
        )

        if not suppressed:
            return filtered

        position_check["status"] = self._recompute_verification_position_status(position_check)
        date_check["status"] = self._recompute_verification_date_status(date_check)
        filtered["compliance_status"] = self._recompute_verification_compliance_status(filtered)
        filtered["suppressed_by_integrity"] = suppressed

        summary_text = str(filtered.get("summary") or "").strip()
        suffix = f"已排除与完整性缺失重复的 {len(suppressed)} 项附件核验。"
        filtered["summary"] = f"{summary_text} {suffix}".strip() if summary_text else suffix
        return filtered

    def _verification_skip_reason_from_integrity(
        self,
        attachment_title: str,
        integrity_raw: dict[str, Any],
    ) -> dict[str, Any] | None:
        """检查某个附件标题是否与完整性缺失项匹配。"""
        return self._match_integrity_failure_for_segment({"name": attachment_title}, integrity_raw)

    def _recompute_verification_position_status(self, position_check: dict[str, Any]) -> str:
        """重新计算签章位置校验的状态。"""
        missing_attachments = position_check.get("missing_attachments") or []
        missing_signature = position_check.get("missing_signature_attachments") or []
        pending_signature = position_check.get("pending_signature_attachments") or []
        missing_seal = position_check.get("missing_seal_attachments") or []
        if missing_attachments or missing_signature or missing_seal:
            return "missing"
        if pending_signature:
            return "pending"
        return "pass"

    def _recompute_verification_date_status(self, date_check: dict[str, Any]) -> str:
        """重新计算日期校验的状态。"""
        missing_date = date_check.get("missing_date_attachments") or []
        late_date = date_check.get("late_date_attachments") or []
        if late_date:
            return "fail"
        if missing_date:
            return "missing"
        original_status = str(date_check.get("status") or "").strip().lower()
        if original_status in {"missing_deadline", "not_required"}:
            return original_status
        return "pass"

    def _recompute_verification_compliance_status(self, raw_result: dict[str, Any]) -> str:
        """重新计算签字盖章日期核查的整体合规状态。"""
        position_check = raw_result.get("position_check") or {}
        date_check = raw_result.get("date_check") or {}
        seal_company_check = raw_result.get("seal_company_check") or {}
        attachment_results = raw_result.get("attachment_results") or []

        attachment_statuses = {
            str(item.get("status") or "").strip().lower()
            for item in attachment_results
            if isinstance(item, dict)
        }
        if (
            position_check.get("status") == "fail"
            or date_check.get("status") == "fail"
            or seal_company_check.get("status") == "fail"
            or "fail" in attachment_statuses
        ):
            return "fail"
        if (
            position_check.get("status") == "missing"
            or date_check.get("status") == "missing"
            or date_check.get("status") == "missing_date"
            or "missing" in attachment_statuses
            or "missing_date" in attachment_statuses
        ):
            return "missing"
        if (
            position_check.get("status") == "pending"
            or date_check.get("status") == "missing_deadline"
            or seal_company_check.get("status") == "pending"
            or "pending" in attachment_statuses
        ):
            return "pending"
        return "pass"

    # 文本规范化辅助
    def _extract_attachment_refs(self, text: str) -> list[str]:
        """从文本中提取所有附件引用（如“附件 1”、“附件 7-1”）。"""
        refs = []
        for match in self.ATTACHMENT_REF_RE.findall(str(text or "")):
            refs.append(re.sub(r"\s+", " ", match).strip())
        return refs

    def _attachment_ref_keys(self, text: str) -> set[str]:
        """提取规范化附件引用，要求附件号精确一致，避免附件8误匹配附件8-1。"""
        return {
            self._normalize_match_text(ref)
            for ref in self._extract_attachment_refs(text)
            if ref
        }

    def _simplify_integrity_item_title(self, item_name: str) -> str:
        """简化完整性条目标题，去除编号和括号内容。"""
        text = str(item_name or "").strip()
        text = re.sub(r"^\s*(?:\d+|[A-Z]|[一二三四五六七八九十百]+)[.、]\s*", "", text)
        text = re.sub(r"（.*?）|\(.*?\)", "", text).strip()
        if not text or len(text) > 24:
            return ""
        if any(sep in text for sep in ("；", ";", "，", ",")):
            return ""
        return text

    def _normalize_match_text(self, text: str) -> str:
        """归一化文本用于匹配比较（仅保留字母、数字和中文）。"""
        return "".join(ch for ch in str(text or "") if ch.isalnum() or "\u4e00" <= ch <= "\u9fff")
