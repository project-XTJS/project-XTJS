from __future__ import annotations

import copy
import hashlib
import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from app.service.analysis.consistency import ConsistencyChecker
from app.service.analysis.deviation import DeviationChecker
from app.service.analysis.integrity import IntegrityChecker
from app.service.analysis.itemized_pricing import ItemizedPricingChecker
from app.service.analysis.pricing_reasonableness import ReasonablenessChecker
from app.service.analysis.verification import VerificationChecker
from app.service.postgresql_service import PostgreSQLService


class UnifiedBusinessReviewService:
    RESULT_SCHEMA_VERSION = "1.0"
    DEFAULT_RESULT_KEY = "unified_business_review"

    PAGE_KEYS = {"page", "page_no", "page_num", "page_index"}
    PAGE_LIST_KEYS = {"pages", "page_numbers", "page_nos"}
    BUSINESS_FILE_RE = re.compile(r"[\s_-]*商务标\s*$")
    TECHNICAL_FILE_RE = re.compile(r"[\s_-]*技术标\s*$")

    def __init__(self, db_service: PostgreSQLService | None = None) -> None:
        self.db_service = db_service or PostgreSQLService()
        self.integrity_checker = IntegrityChecker()
        self.consistency_checker = ConsistencyChecker()
        self.reasonableness_checker = ReasonablenessChecker()
        self.itemized_checker = ItemizedPricingChecker()
        self.deviation_checker = DeviationChecker()
        self.verification_checker = VerificationChecker(None)

    def review_dataset(
        self,
        dataset_dir: str | Path,
        *,
        project_identifier: str | None = None,
    ) -> dict[str, Any]:
        dataset = self._discover_dataset(dataset_dir)
        resolved_project_identifier = project_identifier or self._default_project_identifier(dataset["base_dir"])

        bidders: list[dict[str, Any]] = []
        for bidder in dataset["bidders"]:
            bidders.append(
                self._review_bidder(
                    tender_payload=dataset["tender"]["content"],
                    tender_meta=dataset["tender"]["meta"],
                    bidder=bidder,
                )
            )

        return {
            "schema_version": self.RESULT_SCHEMA_VERSION,
            "review_type": "unified_business_review",
            "generated_at": self._utc_now_iso(),
            "project_identifier_id": resolved_project_identifier,
            "dataset": {
                "base_dir": str(dataset["base_dir"]),
                "tender": dataset["tender"]["meta"],
                "bidders": [
                    {
                        "bidder_key": bidder["bidder_key"],
                        "business": bidder["business"]["meta"],
                        "technical": bidder["technical"]["meta"],
                    }
                    for bidder in dataset["bidders"]
                ],
                "file_count": 1 + len(dataset["bidders"]) * 2,
            },
            "function_validation": self._summarize_function_validation(bidders),
            "summary": self._summarize_review(bidders),
            "bidders": bidders,
        }

    def persist_dataset_review(
        self,
        dataset_dir: str | Path,
        *,
        project_identifier: str | None = None,
        result_key: str = DEFAULT_RESULT_KEY,
    ) -> dict[str, Any]:
        review = self.review_dataset(
            dataset_dir,
            project_identifier=project_identifier,
        )
        project = self._get_or_create_project(review["project_identifier_id"])
        result_record = self.db_service.upsert_project_result_item(
            project["identifier_id"],
            result_key,
            review,
        )
        return {
            "project": project,
            "result_key": result_key,
            "review": review,
            "result_record": result_record,
        }

    def _discover_dataset(self, dataset_dir: str | Path) -> dict[str, Any]:
        base_dir = Path(dataset_dir).expanduser().resolve()
        if not base_dir.exists():
            raise FileNotFoundError(f"dataset_dir does not exist: {base_dir}")
        if not base_dir.is_dir():
            raise NotADirectoryError(f"dataset_dir is not a directory: {base_dir}")

        files = sorted(
            {
                path.resolve()
                for pattern in ("*.json", "*.JSON")
                for path in base_dir.glob(pattern)
                if path.is_file()
            },
            key=lambda item: item.name,
        )
        if not files:
            raise FileNotFoundError(f"no JSON files found under {base_dir}")

        tender_candidates: list[Path] = []
        bidder_docs: dict[str, dict[str, Any]] = {}

        for path in files:
            stem = path.stem.strip()
            if "招标" in stem:
                tender_candidates.append(path)
                continue

            role: str | None = None
            bidder_key = stem
            if "商务标" in stem:
                role = "business"
                bidder_key = self.BUSINESS_FILE_RE.sub("", stem).strip() or stem
            elif "技术标" in stem:
                role = "technical"
                bidder_key = self.TECHNICAL_FILE_RE.sub("", stem).strip() or stem

            if role is None:
                continue

            bidder_entry = bidder_docs.setdefault(
                bidder_key,
                {"bidder_key": bidder_key, "business_path": None, "technical_path": None},
            )
            bidder_entry[f"{role}_path"] = path

        if len(tender_candidates) != 1:
            raise ValueError(
                f"expected exactly one tender JSON file, found {len(tender_candidates)} under {base_dir}"
            )

        incomplete = [
            bidder_key
            for bidder_key, entry in bidder_docs.items()
            if not entry["business_path"] or not entry["technical_path"]
        ]
        if incomplete:
            raise ValueError(f"incomplete bidder document pairs: {', '.join(sorted(incomplete))}")

        tender_path = tender_candidates[0]
        dataset = {
            "base_dir": base_dir,
            "tender": self._load_document(tender_path, role="tender", bidder_key=None),
            "bidders": [],
        }

        for bidder_key in sorted(bidder_docs):
            entry = bidder_docs[bidder_key]
            dataset["bidders"].append(
                {
                    "bidder_key": bidder_key,
                    "business": self._load_document(entry["business_path"], role="business", bidder_key=bidder_key),
                    "technical": self._load_document(entry["technical_path"], role="technical", bidder_key=bidder_key),
                }
            )

        return dataset

    def _load_document(self, path: Path, *, role: str, bidder_key: str | None) -> dict[str, Any]:
        content = json.loads(path.read_text(encoding="utf-8-sig"))
        return {
            "content": content,
            "meta": self._build_file_meta(path, role=role, bidder_key=bidder_key),
        }

    def _build_file_meta(self, path: Path, *, role: str, bidder_key: str | None) -> dict[str, Any]:
        raw_bytes = path.read_bytes()
        stat = path.stat()
        data_node = self._data_node(json.loads(raw_bytes.decode("utf-8-sig")))
        return {
            "role": role,
            "bidder_key": bidder_key,
            "file_name": path.name,
            "file_path": str(path),
            "file_size": stat.st_size,
            "modified_at": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
            "sha256": hashlib.sha256(raw_bytes).hexdigest(),
            "layout_section_count": len(data_node.get("layout_sections", []) or []),
            "logical_table_count": len(data_node.get("logical_tables", []) or []),
            "native_table_count": len(data_node.get("native_tables", []) or []),
            "page_count": self._page_count(data_node),
        }

    def _review_bidder(
        self,
        *,
        tender_payload: dict[str, Any],
        tender_meta: dict[str, Any],
        bidder: dict[str, Any],
    ) -> dict[str, Any]:
        business_payload = bidder["business"]["content"]
        technical_payload = bidder["technical"]["content"]
        combined_payload = self._merge_bid_documents(business_payload, technical_payload)

        checks = {
            "integrity_check": self._execute_check(
                check_code="integrity_check",
                check_name="商务标完整性审查",
                runner=lambda: self.integrity_checker.check_integrity(tender_payload, business_payload),
                normalizer=self._normalize_integrity,
            ),
            "consistency_check": self._execute_check(
                check_code="consistency_check",
                check_name="模板一致性审查",
                runner=lambda: self.consistency_checker.compare_raw_data(tender_payload, business_payload),
                normalizer=self._normalize_consistency,
            ),
            "pricing_check": self._execute_check(
                check_code="pricing_check",
                check_name="报价合理性审查",
                runner=lambda: {
                    "self_check": self.reasonableness_checker.check_price_reasonableness(business_payload),
                    "tender_limit_check": self.reasonableness_checker.check_bid_price_against_tender_limit(
                        tender_payload,
                        business_payload,
                    ),
                },
                normalizer=self._normalize_pricing,
            ),
            "itemized_pricing_check": self._execute_check(
                check_code="itemized_pricing_check",
                check_name="分项报价表审查",
                runner=lambda: self.itemized_checker.check_itemized_logic(
                    business_payload,
                    tender_text=tender_payload,
                ),
                normalizer=self._normalize_itemized,
            ),
            "deviation_check": self._execute_check(
                check_code="deviation_check",
                check_name="偏离条款审查",
                runner=lambda: self.deviation_checker.check_technical_deviation(tender_payload, combined_payload),
                normalizer=self._normalize_deviation,
            ),
            "verification_check": self._execute_check(
                check_code="verification_check",
                check_name="签字盖章日期审查",
                runner=lambda: self.verification_checker.check_seal_and_date(tender_payload, business_payload),
                normalizer=self._normalize_verification,
            ),
        }

        bidder_name = self._extract_bidder_name(checks, bidder["bidder_key"])
        aggregate_issues = self._aggregate_bidder_issues(checks)
        summary = self._summarize_bidder_checks(checks)

        return {
            "bidder_key": bidder["bidder_key"],
            "bidder_name": bidder_name,
            "documents": {
                "tender": tender_meta,
                "business": bidder["business"]["meta"],
                "technical": bidder["technical"]["meta"],
            },
            "summary": summary,
            "checks": checks,
            "issues": aggregate_issues,
        }

    def _execute_check(
        self,
        *,
        check_code: str,
        check_name: str,
        runner: Callable[[], Any],
        normalizer: Callable[[Any], dict[str, Any]],
    ) -> dict[str, Any]:
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
                    "unclear": [],
                },
                "raw_result": None,
            }

    def _normalize_integrity(self, raw: dict[str, Any]) -> dict[str, Any]:
        details = raw.get("details", {}) if isinstance(raw, dict) else {}
        score = raw.get("integrity_score") if isinstance(raw, dict) else None

        passed = []
        failed = []
        for item_name, detail in details.items():
            detail = detail or {}
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

        total = len(details)
        review_status = "pass" if not failed else "fail"
        summary = f"完整性得分 {score}，共校验 {total} 项，缺失 {len(failed)} 项。"
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
            },
            "issues": {
                "passed": passed,
                "failed": failed,
                "unclear": [],
            },
        }

    def _normalize_consistency(self, raw: Any) -> dict[str, Any]:
        segments = raw if isinstance(raw, list) else []
        passed = []
        failed = []

        for segment in segments:
            title = str(segment.get("name") or "未命名模板段")
            missing = segment.get("missing_anchors") or []
            evidence = {
                "missing_anchors": missing,
            }
            if segment.get("is_passed"):
                passed.append(
                    self._issue(
                        status="pass",
                        title=title,
                        message="模板关键锚点齐全。",
                        evidence=evidence,
                    )
                )
            else:
                failed.append(
                    self._issue(
                        status="fail",
                        title=title,
                        message=f"缺少模板锚点：{self._join_text(missing)}",
                        evidence=evidence,
                    )
                )

        validation_status = "correct" if segments else "unclear"
        validation_reason = (
            "模块返回了逐模板段的缺漏锚点结果。"
            if segments
            else "未提取到可比较的模板段，需人工复核模板抽取是否成功。"
        )
        review_status = "pass" if segments and not failed else ("unclear" if not segments else "fail")
        summary = (
            f"共比对 {len(segments)} 个模板段，通过 {len(passed)} 个，存在缺漏 {len(failed)} 个。"
            if segments
            else "未提取到可比较的模板段。"
        )

        return {
            "validation": {
                "status": validation_status,
                "reason": validation_reason,
            },
            "review": {
                "status": review_status,
                "summary": summary,
            },
            "metrics": {
                "template_segment_count": len(segments),
                "passed_segment_count": len(passed),
                "failed_segment_count": len(failed),
            },
            "issues": {
                "passed": passed,
                "failed": failed,
                "unclear": [],
            },
        }

    def _normalize_pricing(self, raw: dict[str, Any]) -> dict[str, Any]:
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

        review_status = self._combine_review_status(
            [issue["status"] for issue in passed + failed + unclear]
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
                "unclear_subcheck_count": len(unclear),
            },
            "issues": {
                "passed": passed,
                "failed": failed,
                "unclear": unclear,
            },
        }

    def _normalize_itemized(self, raw: dict[str, Any]) -> dict[str, Any]:
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
            issue = self._issue(
                status=normalized_status,
                title=label,
                message=message,
                evidence=payload,
            )
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
            "validation": {
                "status": validation_status,
                "reason": validation_reason,
            },
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
            "issues": {
                "passed": passed,
                "failed": failed,
                "unclear": unclear,
            },
        }

    def _normalize_deviation(self, raw: dict[str, Any]) -> dict[str, Any]:
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
            "issues": {
                "passed": passed,
                "failed": failed,
                "unclear": unclear,
            },
        }

    def _normalize_verification(self, raw: dict[str, Any]) -> dict[str, Any]:
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
                self._issue(
                    status="pass",
                    title="公章与投标人匹配",
                    message="检测到的公章与投标人名称匹配。",
                    evidence=seal_company_check,
                )
            )
        elif seal_company_check:
            failed.append(
                self._issue(
                    status="fail",
                    title="公章与投标人匹配",
                    message="检测到的公章与投标人名称不匹配。",
                    evidence=seal_company_check,
                )
            )

        if position_check.get("status") == "pass":
            passed.append(
                self._issue(
                    status="pass",
                    title="签字盖章位置检查",
                    message="所有必需附件均已找到，未发现缺失签字或盖章。",
                    evidence=position_check,
                )
            )
        else:
            for attachment in missing_attachments:
                failed.append(
                    self._issue(
                        status="fail",
                        title=attachment,
                        message="未找到要求签章的附件。",
                        evidence={"attachment": attachment, "source": "position_check"},
                    )
                )
            for attachment in missing_signature:
                failed.append(
                    self._issue(
                        status="fail",
                        title=attachment,
                        message="附件缺少签字。",
                        evidence={"attachment": attachment, "source": "position_check"},
                    )
                )
            for attachment in missing_seal:
                failed.append(
                    self._issue(
                        status="fail",
                        title=attachment,
                        message="附件缺少盖章。",
                        evidence={"attachment": attachment, "source": "position_check"},
                    )
                )

        for attachment in pending_signature:
            unclear.append(
                self._issue(
                    status="unclear",
                    title=attachment,
                    message="签字字段处于待填写状态，建议人工复核。",
                    evidence={"attachment": attachment, "source": "position_check"},
                )
            )

        if date_check.get("status") == "pass":
            passed.append(
                self._issue(
                    status="pass",
                    title="落款日期检查",
                    message="已检测到落款日期，且均未晚于投标截止时间。",
                    evidence=date_check,
                )
            )
        else:
            for attachment in missing_date:
                failed.append(
                    self._issue(
                        status="fail",
                        title=attachment,
                        message="附件缺少落款日期。",
                        evidence={"attachment": attachment, "source": "date_check"},
                    )
                )
            for attachment in late_date:
                failed.append(
                    self._issue(
                        status="fail",
                        title=attachment,
                        message="附件落款日期晚于招标截止时间。",
                        evidence={"attachment": attachment, "source": "date_check"},
                    )
                )

        return {
            "validation": {
                "status": "correct",
                "reason": "模块返回了附件级签字、盖章、日期和公章匹配结果。",
            },
            "review": {
                "status": compliance_status,
                "summary": str(raw.get("summary") or "未返回签字盖章日期结论。"),
            },
            "metrics": {
                "required_attachment_count": raw.get("required_attachment_count"),
                "missing_attachment_count": len(missing_attachments),
                "missing_signature_count": len(missing_signature),
                "pending_signature_count": len(pending_signature),
                "missing_seal_count": len(missing_seal),
                "missing_date_count": len(missing_date),
                "late_date_count": len(late_date),
            },
            "issues": {
                "passed": passed,
                "failed": failed,
                "unclear": unclear,
            },
        }

    def _aggregate_bidder_issues(self, checks: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
        bucket = self._empty_issue_bucket()
        for check_code, check in checks.items():
            check_name = check["check_name"]
            for status_key in ("passed", "failed", "unclear"):
                for issue in check["issues"][status_key]:
                    bucket[status_key].append(
                        {
                            "check_code": check_code,
                            "check_name": check_name,
                            **issue,
                        }
                    )
        return bucket

    def _summarize_bidder_checks(self, checks: dict[str, Any]) -> dict[str, Any]:
        review_status_counts = {"pass": 0, "fail": 0, "unclear": 0}
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
            "overall_review_status": self._combine_review_status(list(review_status_counts.keys()), counts=review_status_counts),
            "overall_validation_status": self._combine_validation_status(validation_status_counts),
        }

    def _summarize_review(self, bidders: list[dict[str, Any]]) -> dict[str, Any]:
        review_status_counts = {"pass": 0, "fail": 0, "unclear": 0}
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
        function_summary: dict[str, Any] = {}
        for bidder in bidders:
            for check_code, check in bidder["checks"].items():
                entry = function_summary.setdefault(
                    check_code,
                    {
                        "check_name": check["check_name"],
                        "execution_status_counts": {"ok": 0, "error": 0},
                        "validation_status_counts": {"correct": 0, "failed": 0, "unclear": 0},
                        "review_status_counts": {"pass": 0, "fail": 0, "unclear": 0},
                    },
                )
                entry["execution_status_counts"][check["execution"]["status"]] += 1
                entry["validation_status_counts"][check["validation"]["status"]] += 1
                entry["review_status_counts"][check["review"]["status"]] += 1
        return function_summary

    def _merge_bid_documents(self, business_payload: dict[str, Any], technical_payload: dict[str, Any]) -> dict[str, Any]:
        business_data = self._data_node(business_payload)
        technical_data = self._data_node(technical_payload)

        page_offset = self._page_count(business_data)
        merged_data: dict[str, Any] = {}
        keys = set(business_data.keys()) | set(technical_data.keys())

        for key in keys:
            left = business_data.get(key)
            right = technical_data.get(key)
            if isinstance(left, list) or isinstance(right, list):
                left_list = left if isinstance(left, list) else []
                right_list = right if isinstance(right, list) else []
                merged_data[key] = copy.deepcopy(left_list) + self._offset_page_refs(right_list, page_offset, parent_key=key)
            elif left is not None:
                merged_data[key] = copy.deepcopy(left)
            else:
                merged_data[key] = self._offset_page_refs(right, page_offset, parent_key=key)

        return {"data": merged_data}

    def _offset_page_refs(self, value: Any, offset: int, *, parent_key: str | None = None) -> Any:
        if offset <= 0:
            return copy.deepcopy(value)

        if isinstance(value, dict):
            result = {}
            for key, item in value.items():
                if key in self.PAGE_KEYS and isinstance(item, int):
                    result[key] = item + offset
                elif key in self.PAGE_LIST_KEYS and isinstance(item, list):
                    result[key] = [member + offset if isinstance(member, int) else copy.deepcopy(member) for member in item]
                else:
                    result[key] = self._offset_page_refs(item, offset, parent_key=key)
            return result

        if isinstance(value, list):
            if parent_key in self.PAGE_LIST_KEYS:
                return [member + offset if isinstance(member, int) else copy.deepcopy(member) for member in value]
            return [self._offset_page_refs(member, offset, parent_key=parent_key) for member in value]

        return copy.deepcopy(value)

    def _page_count(self, data_node: dict[str, Any]) -> int:
        pages = data_node.get("pages") or []
        if isinstance(pages, list) and pages:
            return len(pages)

        max_page = 0
        for collection_key in ("layout_sections", "logical_tables", "native_tables"):
            for item in data_node.get(collection_key, []) or []:
                max_page = max(max_page, self._max_page_in_payload(item))
        return max_page

    def _max_page_in_payload(self, payload: Any) -> int:
        if isinstance(payload, dict):
            values = []
            for key, value in payload.items():
                if key in self.PAGE_KEYS and isinstance(value, int):
                    values.append(value)
                elif key in self.PAGE_LIST_KEYS and isinstance(value, list):
                    values.extend(member for member in value if isinstance(member, int))
                else:
                    values.append(self._max_page_in_payload(value))
            return max(values or [0])
        if isinstance(payload, list):
            return max((self._max_page_in_payload(item) for item in payload), default=0)
        return 0

    def _extract_bidder_name(self, checks: dict[str, Any], fallback: str) -> str:
        verification_raw = checks["verification_check"].get("raw_result") or {}
        bidder_name = str(verification_raw.get("bidder_name") or "").strip()
        return bidder_name or fallback

    def _summarize_itemized_subcheck(self, subcheck_code: str, payload: dict[str, Any]) -> str:
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

    def _map_generic_status(self, raw_status: Any) -> str:
        text = str(raw_status or "").strip().lower()
        if text in {"pass", "passed", "ok", "success", "合格"}:
            return "pass"
        if text in {"fail", "failed", "error", "不合格"}:
            return "fail"
        return "unclear"

    def _map_price_result(self, result_value: Any, summary_text: str) -> str:
        result_text = str(result_value or "").strip()
        combined = f"{result_text} {summary_text}".strip()
        if "合格" in combined or re.search(r"\bpass\b", combined, re.IGNORECASE):
            return "pass"
        fail_keywords = ("不合格", "超出", "异常", "失败", "错误")
        if any(keyword in combined for keyword in fail_keywords):
            return "fail"
        unclear_keywords = ("未识别", "未找到", "无法", "暂无法", "人工复核", "待复核")
        if any(keyword in combined for keyword in unclear_keywords):
            return "unclear"
        return "unclear"

    def _combine_review_status(
        self,
        statuses: list[str],
        *,
        counts: dict[str, int] | None = None,
    ) -> str:
        if counts:
            if counts.get("fail", 0) > 0:
                return "fail"
            if counts.get("unclear", 0) > 0:
                return "unclear"
            return "pass"

        if any(status == "fail" for status in statuses):
            return "fail"
        if any(status == "unclear" for status in statuses):
            return "unclear"
        return "pass"

    def _combine_validation_status(self, counts: dict[str, int]) -> str:
        if counts.get("failed", 0) > 0:
            return "failed"
        if counts.get("unclear", 0) > 0:
            return "unclear"
        return "correct"

    def _default_project_identifier(self, dataset_dir: Path) -> str:
        digest = hashlib.sha1(str(dataset_dir).encode("utf-8")).hexdigest()[:10]
        return f"unified_business_review_{digest}"

    def _get_or_create_project(self, identifier_id: str) -> dict[str, Any]:
        project = self.db_service.get_project_by_identifier(identifier_id)
        if project:
            return project
        return self.db_service.create_project(identifier_id)

    def _data_node(self, payload: dict[str, Any]) -> dict[str, Any]:
        data = payload.get("data")
        return data if isinstance(data, dict) else payload

    def _join_text(self, value: Any) -> str:
        if isinstance(value, list):
            return "；".join(str(item) for item in value if item is not None and str(item).strip())
        if value is None:
            return ""
        return str(value)

    def _issue(
        self,
        *,
        status: str,
        title: str,
        message: str,
        evidence: Any | None = None,
    ) -> dict[str, Any]:
        severity = {
            "pass": "info",
            "fail": "error",
            "unclear": "warning",
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
        return {
            "passed": [],
            "failed": [],
            "unclear": [],
        }

    @staticmethod
    def _utc_now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()
