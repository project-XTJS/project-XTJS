# unified/helpers.py
"""
统一商务标审查 - 通用辅助工具 Mixin

提供检查执行包装器、问题格式化、文本与状态处理、汇总统计等。
"""

from __future__ import annotations

import re
import time
from typing import Any, Callable


class HelpersMixin:
    """通用小工具 Mixin。"""

    # 声明实例属性类型提示（实际值由 __init__ 赋值）
    RESULT_SCHEMA_VERSION: str
    BUSINESS_RESULT_KEY: str
    PAGE_REF_RE: Any

    # 检查执行包装
    def _execute_check(
        self,
        *,
        check_code: str,
        check_name: str,
        runner: Callable[[], Any],
        normalizer: Callable[[Any], dict[str, Any]],
    ) -> dict[str, Any]:
        """通用的检查执行包装器：计时、异常捕获、结果标准化。"""
        started = time.perf_counter()
        try:
            raw_result = runner()
            normalized = normalizer(raw_result)
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
        except Exception as exc:  # pragma: no cover - defensive wrapper
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

    # 问题条目构造
    def _issue(
        self,
        *,
        status: str,
        title: str,
        message: str,
        evidence: Any | None = None,
    ) -> dict[str, Any]:
        """生成一条标准化的问题/条目。"""
        severity = {
            "pass": "info",
            "fail": "error",
            "unclear": "warning",
            "missing": "warning",
        }[status]
        issue = {
            "status": status,
            "severity": severity,
            "title": str(title),
            "message": str(message),
        }
        if evidence is not None:
            issue["evidence"] = evidence
        return issue

    def _empty_issue_bucket(self) -> dict[str, list[dict[str, Any]]]:
        """返回空的三类问题桶。"""
        return {
            "passed": [],
            "failed": [],
            "missing": [],
            "unclear": [],
        }

    # 文本处理
    def _join_text(self, value: Any) -> str:
        """将列表或其他值拼接为字符串。"""
        if isinstance(value, list):
            return "；".join(str(item) for item in value if item is not None and str(item).strip())
        if value is None:
            return ""
        return str(value)

    def _trim_text(self, value: Any, *, max_length: int) -> str:
        """截断文本并在尾部添加省略号。"""
        text = str(value or "").strip()
        if len(text) <= max_length:
            return text
        return f"{text[: max_length - 3].rstrip()}..."

    def _unique_texts(self, values: list[str]) -> list[str]:
        """去重并保留原始顺序的文本列表。"""
        seen: set[str] = set()
        unique: list[str] = []
        for value in values:
            normalized = str(value or "").strip()
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            unique.append(normalized)
        return unique

    # 状态映射与合并
    def _map_generic_status(self, raw_status: Any) -> str:
        """将原始状态字符串映射到标准状态：pass / fail / missing / unclear。"""
        text = str(raw_status or "").strip().lower()
        if text in {"pass", "passed", "ok", "success", "合格"}:
            return "pass"
        if text in {
            "missing",
            "not_found",
            "not found",
            "not_detected",
            "not detected",
            "missing_date",
            "missing_deadline",
            "未找到",
            "未识别",
            "缺失",
            "缺少",
            "未提供",
        }:
            return "missing"
        if text in {"fail", "failed", "error", "不合格"}:
            return "fail"
        return "unclear"

    def _map_price_result(self, result_value: Any, summary_text: str) -> str:
        """专门针对报价结果的字符串映射。"""
        result_text = str(result_value or "").strip()
        normalized_result = result_text.lower()
        if normalized_result in {"合格", "通过", "pass", "passed", "ok", "success"}:
            return "pass"
        if normalized_result in {"不合格", "失败", "未通过", "fail", "failed", "error"}:
            return "fail"

        combined = f"{result_text} {summary_text}".strip()
        # 摘要里可能出现“未识别到投标总金额不作为失败项”这类否定语义，
        # 不能因为包含“失败”两个字就覆盖结构化的合格结论。
        benign_fail_phrases = (
            "不作为失败项",
            "不判失败",
            "不是失败项",
            "不按总价限价校验判失败",
            "不按总价限价判失败",
            "不作为总价限价校验失败项",
        )
        combined_for_fail = combined
        for phrase in benign_fail_phrases:
            combined_for_fail = combined_for_fail.replace(phrase, "")
        fail_keywords = ("不合格", "超出", "异常", "失败", "错误")
        if any(keyword in combined_for_fail for keyword in fail_keywords):
            return "fail"
        if "合格" in combined or re.search(r"\bpass\b", combined, re.IGNORECASE):
            return "pass"
        missing_keywords = ("未识别", "未找到", "缺少", "缺失", "未提供")
        if any(keyword in combined for keyword in missing_keywords):
            return "missing"
        unclear_keywords = ("无法", "暂无法", "人工复核", "待复核")
        if any(keyword in combined for keyword in unclear_keywords):
            return "unclear"
        return "unclear"

    def _combine_review_status(
        self,
        statuses: list[str],
        *,
        counts: dict[str, int] | None = None,
    ) -> str:
        """合并多个子检查的审查状态为最终的审查状态。"""
        if counts:
            if counts.get("fail", 0) > 0:
                return "fail"
            if counts.get("missing", 0) > 0:
                return "missing"
            if counts.get("unclear", 0) > 0:
                return "unclear"
            return "pass"

        if any(status == "fail" for status in statuses):
            return "fail"
        if any(status == "missing" for status in statuses):
            return "missing"
        if any(status == "unclear" for status in statuses):
            return "unclear"
        return "pass"

    def _combine_validation_status(self, counts: dict[str, int]) -> str:
        """合并验证状态。"""
        if counts.get("failed", 0) > 0:
            return "failed"
        if counts.get("unclear", 0) > 0:
            return "unclear"
        return "correct"

    #排序键
    def _review_status_sort_key(self, status: Any) -> int:
        """用于将状态字符串映射为排序权重，fail 优先。"""
        text = str(status or "").strip().lower()
        order = {"fail": 0, "missing": 1, "unclear": 2, "pass": 3}
        return order.get(text, 3)

    def _check_display_index(self, check_code: Any) -> int:
        """返回检查项在预定展示顺序中的索引，未知项排最后。"""
        from .constants import CHECK_DISPLAY_ORDER
        text = str(check_code or "")
        if text in CHECK_DISPLAY_ORDER:
            return CHECK_DISPLAY_ORDER.index(text)
        return len(CHECK_DISPLAY_ORDER)

    # 投标人名称提取 
    def _extract_bidder_name(self, checks: dict[str, Any], fallback: str) -> str:
        """从签字盖章检查结果中探测投标人名称，若缺失则使用 fallback。"""
        verification_raw = checks.get("verification_check", {}).get("raw_result") or {}
        bidder_name = str(verification_raw.get("bidder_name") or "").strip()
        return bidder_name or fallback

    # 分项报价子检查摘要
    def _summarize_itemized_subcheck(self, subcheck_code: str, payload: dict[str, Any]) -> str:
        """为分项报价的子检查生成简短描述。"""
        if subcheck_code == "row_arithmetic":
            return f"存在 {payload.get('issue_count', 0)} 条算术错误，另有 {payload.get('unresolved_count', 0)} 条未解析行。"
        if subcheck_code == "sum_consistency":
            return (
                f"计算合计 {payload.get('calculated_total')}，声明总价 {payload.get('declared_total')}，"
                f"差额 {payload.get('difference')}。"
            )
        if subcheck_code == "duplicate_items":
            return f"检测到 {payload.get('issue_count', 0)} 组疑似重复报价项。"
        if subcheck_code == "missing_item":
            missing_items = payload.get("missing_items") or []
            return f"检测到 {len(missing_items)} 个疑似缺失列项。"
        return str(payload.get("status") or "未返回详细说明。")

    # 投标人/全局汇总
    def _aggregate_bidder_issues(self, checks: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
        """将单个投标人下所有检查项的问题汇总为一个桶。"""
        bucket = self._empty_issue_bucket()
        for check_code, check in checks.items():
            check_name = check["check_name"]
            for status_key in ("passed", "failed", "missing", "unclear"):
                for issue in (check.get("issues") or {}).get(status_key, []):
                    bucket[status_key].append(
                        {
                            "check_code": check_code,
                            "check_name": check_name,
                            **issue,
                        }
                    )
        return bucket

    def _summarize_bidder_checks(self, checks: dict[str, Any]) -> dict[str, Any]:
        """汇总单个投标人各审查项的状态计数。"""
        review_status_counts = {"pass": 0, "fail": 0, "unclear": 0, "missing": 0}
        validation_status_counts = {"correct": 0, "failed": 0, "unclear": 0}
        execution_status_counts = {"ok": 0, "error": 0}

        for check in checks.values():
            review_status = check["review"]["status"]
            validation_status = check["validation"]["status"]
            execution_status = check["execution"]["status"]
            review_status_counts[review_status] = review_status_counts.get(review_status, 0) + 1
            validation_status_counts[validation_status] = validation_status_counts.get(validation_status, 0) + 1
            execution_status_counts[execution_status] = execution_status_counts.get(execution_status, 0) + 1

        return {
            "check_count": len(checks),
            "review_status_counts": review_status_counts,
            "validation_status_counts": validation_status_counts,
            "execution_status_counts": execution_status_counts,
            "overall_review_status": self._combine_review_status(
                list(review_status_counts.keys()), counts=review_status_counts
            ),
            "overall_validation_status": self._combine_validation_status(validation_status_counts),
        }

    def _summarize_review(self, bidders: list[dict[str, Any]]) -> dict[str, Any]:
        """汇总所有投标人的整体审查状态。"""
        review_status_counts = {"pass": 0, "fail": 0, "unclear": 0, "missing": 0}
        validation_status_counts = {"correct": 0, "failed": 0, "unclear": 0}
        execution_status_counts = {"ok": 0, "error": 0}

        for bidder in bidders:
            for check in bidder["checks"].values():
                review_status = check["review"]["status"]
                validation_status = check["validation"]["status"]
                execution_status = check["execution"]["status"]
                review_status_counts[review_status] = review_status_counts.get(review_status, 0) + 1
                validation_status_counts[validation_status] = validation_status_counts.get(validation_status, 0) + 1
                execution_status_counts[execution_status] = execution_status_counts.get(execution_status, 0) + 1

        return {
            "bidder_count": len(bidders),
            "total_check_count": sum(review_status_counts.values()),
            "review_status_counts": review_status_counts,
            "validation_status_counts": validation_status_counts,
            "execution_status_counts": execution_status_counts,
        }

    def _summarize_function_validation(self, bidders: list[dict[str, Any]]) -> dict[str, Any]:
        """按审查子模块汇总功能验证状态。"""
        function_summary: dict[str, Any] = {}
        for bidder in bidders:
            for check_code, check in bidder["checks"].items():
                entry = function_summary.setdefault(
                    check_code,
                    {
                        "check_name": check["check_name"],
                        "execution_status_counts": {"ok": 0, "error": 0},
                        "validation_status_counts": {"correct": 0, "failed": 0, "unclear": 0},
                        "review_status_counts": {"pass": 0, "fail": 0, "unclear": 0, "missing": 0},
                    },
                )
                entry["execution_status_counts"][check["execution"]["status"]] += 1
                entry["validation_status_counts"][check["validation"]["status"]] += 1
                review_status = check["review"]["status"]
                entry["review_status_counts"][review_status] = (
                    entry["review_status_counts"].get(review_status, 0) + 1
                )
        return function_summary

    # 响应概览
    def _build_response_overview(self, review: dict[str, Any]) -> dict[str, Any]:
        """从完整审查结果中提炼 API 响应所需的概览信息。"""
        guide = review.get("reading_guide") or {}
        bidder_overview = guide.get("bidder_overview") or []
        return {
            "review_type": review.get("review_type"),
            "project_identifier_id": review.get("project_identifier_id"),
            "tender_file_name": guide.get("tender_file_name"),
            "bidder_count": len(bidder_overview),
            "review_status_counts": (review.get("summary") or {}).get("review_status_counts"),
            "recommended_fields": [
                "overview.bidder_overview",
                "review.reading_guide",
                "review.extraction_tables.catalog",
                "review.extraction_tables.tender_table",
                "review.extraction_tables.bidder_tables[].rows",
                "review.bidders[].reading_guide.check_navigation",
                "review.bidders[].checks",
            ],
            "bidder_overview": bidder_overview,
        }
