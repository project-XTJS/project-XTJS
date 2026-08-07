# -*- coding: utf-8 -*-
"""招标文件规范检查规则引擎。

本模块只基于 OCR JSON 做确定性规则判断，不调用大模型，也不写入项目结果。
"""

from __future__ import annotations

import re
from typing import Any

from app.service.analysis.reasonableness import ReasonablenessChecker


class TenderComplianceChecker:
    """Rule-based tender document compliance checker."""

    SCHEMA_VERSION = "tender_compliance.v1"
    AMOUNT_TOLERANCE_YUAN = 0.01

    LIMIT_KEYWORDS = [
        "最高限价",
        "最高投标限价",
        "最高响应限价",
        "最高报价限价",
        "招标控制价",
        "最高控制价",
        "控制价",
        "最高总价",
        "总限价",
        "报价上限",
    ]
    BUDGET_KEYWORDS = [
        "项目预算",
        "采购预算",
        "预算金额",
        "预算价",
        "最高预算",
        "总预算",
        "预算",
    ]
    GUARANTEE_EXCLUDE_KEYWORDS = ["履约保证金", "质量保证金", "质保金"]
    NO_GUARANTEE_KEYWORDS = [
        "不收取", "免收", "无需缴纳", "不需缴纳", "无须缴纳", "不缴纳", "不设置",
        "不要求提交", "无需提交", "无须提交", "不提交",
    ]
    PAYMENT_KEYWORDS = ["付款方式", "支付方式", "付款", "支付", "预付款", "进度款", "尾款", "质保金"]
    SCORE_CATEGORY_HEADERS = ["评分大类", "评分类别", "评审类别", "评分部分", "评审项目", "类别", "大类"]
    SCORE_ITEM_HEADERS = ["评分项", "评分项目", "评审因素", "评审项", "评审内容", "评分因素", "项目"]
    SCORE_MAX_HEADERS = ["单项满分", "满分", "分值", "权重", "最高分", "标准分", "配分", "得分"]
    SCORE_CRITERIA_HEADERS = ["评分标准", "评审标准", "评分细则", "计分方法", "评分说明", "评分内容"]
    SCORE_DEDUCTION_HEADERS = ["扣分标准", "扣分规则", "扣分说明"]

    def __init__(self, reasonableness_checker: ReasonablenessChecker | None = None) -> None:
        self.reasonableness = reasonableness_checker or ReasonablenessChecker()

    def check(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Run all tender compliance checks against a normalized OCR payload."""
        sections = self._collect_sections(payload)
        context = {
            "payload": payload,
            "sections": sections,
            "limit_candidates": self._limit_candidates(payload, sections),
            "budget_candidates": self._money_candidates_by_keywords(
                sections,
                self.BUDGET_KEYWORDS,
                exclude_keywords=[
                    *self.LIMIT_KEYWORDS,
                    "投标保证金",
                    "履约保证金",
                    "报价",
                    "合同金额",
                    "中标金额",
                    "成交金额",
                    "业绩",
                ],
            ),
        }
        # "最高投标限价 同预算/等于预算" 这类声明没有具体数字：用预算金额作为限价。
        if not context["limit_candidates"] and context["budget_candidates"]:
            same_budget_section = next(
                (
                    section
                    for section in sections
                    if re.search(
                        r"(?:最高投标限价|最高限价|招标控制价|最高控制价|控制价)[^。；\n]{0,20}(?:同|等于|与|按|即)[^。；\n]{0,8}(?:预算|概算)",
                        str(section.get("text") or ""),
                    )
                ),
                None,
            )
            if same_budget_section:
                best_budget = self._best_candidate(context["budget_candidates"])
                if best_budget:
                    context["limit_candidates"].append(
                        {
                            **best_budget,
                            "keyword": "最高投标限价(同预算)",
                            "context": str(same_budget_section.get("text") or "")[:220],
                        }
                    )

        checks = [
            self._check_budget_vs_limit(context),
            self._check_limit_consistency(context),
            self._check_bid_security_ratio(context),
            self._check_performance_security_consistency(context),
            self._check_payment_consistency(context),
            self._check_evaluation_method(context),
        ]
        summary = self._summarize(checks, payload=payload, sections=sections)
        return {
            "schema_version": self.SCHEMA_VERSION,
            "summary": summary,
            "checks": checks,
            "extractions": {
                "highest_limit": self._serialize_candidate(self._best_candidate(context["limit_candidates"])),
                "budget": self._serialize_candidate(self._best_candidate(context["budget_candidates"])),
                "limit_candidates": [
                    self._serialize_candidate(item)
                    for item in context["limit_candidates"][:8]
                ],
            },
        }

    # ------------------------------------------------------------------
    # Check implementations
    # ------------------------------------------------------------------
    def _check_budget_vs_limit(self, context: dict[str, Any]) -> dict[str, Any]:
        budget = self._best_candidate(context["budget_candidates"])
        limit = self._best_candidate(context["limit_candidates"])

        if not budget and not limit:
            return self._make_check(
                "budget_vs_limit",
                "项目预算与最高限价",
                "unclear",
                "未识别到项目预算或最高限价，需人工复核。",
                values={
                    "budget": self._serialize_candidate(budget),
                    "highest_limit": self._serialize_candidate(limit),
                },
                evidence=self._evidence_from_candidates([budget, limit]),
            )

        # 业务约定：预算与最高限价视为同一概念（同义词）。
        # 因此"预算≥限价"不再要求两者同时出现；单一来源即视为该值。
        if budget and limit:
            budget_amount = float(budget["amount_yuan"])
            limit_amount = float(limit["amount_yuan"])
            passed = budget_amount + self.AMOUNT_TOLERANCE_YUAN >= limit_amount
            message = (
                f"项目预算 {self._format_yuan(budget_amount)} "
                f"{'不低于' if passed else '低于'}最高限价 {self._format_yuan(limit_amount)}。"
            )
            if not passed:
                message = "预算与限价视为同义，但文档中预算低于最高限价，存在矛盾：" + message
        elif budget:
            budget_amount = float(budget["amount_yuan"])
            passed = True
            message = (
                f"识别到项目预算 {self._format_yuan(budget_amount)}，"
                "预算与最高限价视为同一概念，未发现矛盾。"
            )
        else:
            limit_amount = float(limit["amount_yuan"])
            passed = True
            message = (
                f"识别到最高限价 {self._format_yuan(limit_amount)}，"
                "预算与最高限价视为同一概念，未发现矛盾。"
            )
        return self._make_check(
            "budget_vs_limit",
            "项目预算与最高限价",
            "pass" if passed else "fail",
            message,
            values={
                "budget": self._serialize_candidate(budget),
                "highest_limit": self._serialize_candidate(limit),
            },
            evidence=self._evidence_from_candidates([budget, limit]),
        )

    def _check_limit_consistency(self, context: dict[str, Any]) -> dict[str, Any]:
        analysis = self._analyze_limit_categories(context["sections"])
        if analysis["conflicts"]:
            status = "fail"
            message = "最高限价存在冲突：" + "；".join(analysis["conflicts"])
        elif analysis["multi_lot"]:
            status = "unclear"
            message = "识别到多包件/多标段限价（" + "、".join(analysis["multi_lot"]) + "），需按包件人工核对一致性。"
        elif analysis["missing"]:
            status = "unclear"
            message = f"未完整识别最高限价来源：{', '.join(analysis['missing'])}，需人工复核。"
        else:
            status = "pass"
            message = "招标公告、投标人须知、报价章节中的最高限价一致。"

        return self._make_check(
            "limit_consistency",
            "最高限价一致性",
            status,
            message,
            values=analysis["values"],
            evidence=self._evidence_from_candidates(analysis["all_candidates"]),
        )

    def _analyze_limit_categories(self, sections: list[dict[str, Any]]) -> dict[str, Any]:
        categories = [
            ("announcement", "招标公告", ["招标公告", "采购公告", "投标邀请", "比选公告", "竞争性磋商公告"]),
            ("instructions", "投标人须知", ["投标人须知", "投标须知", "响应人须知", "须知前附表"]),
            ("quotation", "报价章节", ["报价章节", "报价要求", "投标报价", "报价一览表", "开标一览表", "报价"]),
        ]
        selected: list[dict[str, Any]] = []
        all_candidates: list[dict[str, Any]] = []
        missing: list[str] = []
        conflicts: list[str] = []
        multi_lot: list[str] = []
        values: dict[str, Any] = {}

        for key, label, category_keywords in categories:
            candidates = self._money_candidates_by_keywords(
                sections,
                self.LIMIT_KEYWORDS,
                category_keywords=category_keywords,
                exclude_keywords=[
                    *self.BUDGET_KEYWORDS,
                    "投标保证金",
                    "履约保证金",
                    "报价明细",
                    "分项报价",
                    "单价",
                ],
            )
            all_candidates.extend(candidates)
            amount_groups: dict[float, list[dict[str, Any]]] = {}
            for candidate in candidates:
                amount = round(float(candidate.get("amount_yuan") or 0), 2)
                amount_groups.setdefault(amount, []).append(candidate)
            if not amount_groups:
                missing.append(label)
                values[key] = None
                continue
            if len(amount_groups) > 1:
                # 多包件/多标段项目各包件限价不同是正常现象，不应直接判冲突；
                # 交由人工按包件核对。无包件结构的多金额才视为真实冲突。
                if any(
                    self._contains_any(str(candidate.get("context") or ""), ["包件", "标段", "分包"])
                    for items in amount_groups.values()
                    for candidate in items
                ):
                    multi_lot.append(label)
                    values[key] = {
                        "status": "multi_lot",
                        "candidates": [
                            self._serialize_candidate(self._best_candidate(items))
                            for _, items in sorted(amount_groups.items())
                        ],
                    }
                else:
                    conflicts.append(
                        f"{label}出现多个金额："
                        + "、".join(self._format_yuan(amount) for amount in sorted(amount_groups))
                    )
                    values[key] = {
                        "status": "conflict",
                        "candidates": [
                            self._serialize_candidate(self._best_candidate(items))
                            for _, items in sorted(amount_groups.items())
                        ],
                    }
                continue
            best = self._best_candidate(next(iter(amount_groups.values())))
            if best:
                categorized = {**best, "category": label}
                selected.append(categorized)
                values[key] = self._serialize_candidate(categorized)

        selected_amounts = {
            round(float(item.get("amount_yuan") or 0), 2)
            for item in selected
        }
        if len(selected_amounts) > 1:
            conflicts.append("不同章节最高限价不一致")
        return {
            "selected": selected,
            "all_candidates": all_candidates,
            "missing": missing,
            "conflicts": list(dict.fromkeys(conflicts)),
            "multi_lot": list(dict.fromkeys(multi_lot)),
            "values": values,
        }

    def _check_bid_security_ratio(self, context: dict[str, Any]) -> dict[str, Any]:
        sections = context["sections"]
        keywords = ["投标保证金", "响应保证金", "应答保证金", "报价保证金"]
        none_context_index = self._first_not_required_index(sections, keywords)
        if none_context_index is not None and self._window_is_not_required(sections, none_context_index):
            none_context = sections[none_context_index]
            return self._make_check(
                "bid_security_ratio",
                "投标保证金比例",
                "pass",
                "识别到投标保证金不收取或免收描述。",
                values={"mode": "not_required"},
                evidence=self._evidence_from_sections([none_context]),
            )

        percent = self._best_percentage_candidate(
            self._percentage_candidates_by_keywords(
                sections,
                keywords,
                exclude_keywords=self.GUARANTEE_EXCLUDE_KEYWORDS,
            )
        )
        if percent:
            value = float(percent["percent"])
            passed = value <= 2 + 1e-9
            return self._make_check(
                "bid_security_ratio",
                "投标保证金比例",
                "pass" if passed else "fail",
                f"投标保证金比例 {value:g}% {'未超过' if passed else '超过'}最高限价 2%。",
                values={"percent": value, "raw_value": percent.get("raw_value")},
                evidence=self._evidence_from_candidates([percent]),
            )

        amount = self._best_candidate(
            self._money_candidates_by_keywords(
                sections,
                keywords,
                exclude_keywords=self.GUARANTEE_EXCLUDE_KEYWORDS,
            )
        )
        limit_analysis = self._analyze_limit_categories(sections)
        if amount and limit_analysis["conflicts"]:
            return self._make_check(
                "bid_security_ratio",
                "投标保证金比例",
                "unclear",
                "最高限价存在冲突，无法可靠计算投标保证金比例。",
                values={
                    "bid_security": self._serialize_candidate(amount),
                    "limit_conflicts": limit_analysis["conflicts"],
                },
                evidence=self._evidence_from_candidates(
                    [amount, *limit_analysis["selected"]]
                ),
            )
        limit = self._best_candidate(context["limit_candidates"])
        if amount and limit:
            ratio = float(amount["amount_yuan"]) / max(float(limit["amount_yuan"]), 1)
            passed = ratio <= 0.02 + 1e-9
            return self._make_check(
                "bid_security_ratio",
                "投标保证金比例",
                "pass" if passed else "fail",
                f"投标保证金约为最高限价的 {ratio * 100:.2f}%，{'未超过' if passed else '超过'} 2%。",
                values={
                    "bid_security": self._serialize_candidate(amount),
                    "highest_limit": self._serialize_candidate(limit),
                    "ratio": round(ratio, 6),
                    "ratio_percent": round(ratio * 100, 4),
                },
                evidence=self._evidence_from_candidates([amount, limit]),
            )
        if amount and not limit:
            # 预算与限价视为同义：无最高限价时，用预算金额作分母计算比例。
            budget = self._best_candidate(context["budget_candidates"])
            if budget:
                ratio = float(amount["amount_yuan"]) / max(float(budget["amount_yuan"]), 1)
                passed = ratio <= 0.02 + 1e-9
                return self._make_check(
                    "bid_security_ratio",
                    "投标保证金比例",
                    "pass" if passed else "fail",
                    f"投标保证金约为预算（限价同义）的 {ratio * 100:.2f}%，{'未超过' if passed else '超过'} 2%。",
                    values={
                        "bid_security": self._serialize_candidate(amount),
                        "budget": self._serialize_candidate(budget),
                        "ratio": round(ratio, 6),
                        "ratio_percent": round(ratio * 100, 4),
                        "denominator": "budget",
                    },
                    evidence=self._evidence_from_candidates([amount, budget]),
                )
            return self._make_check(
                "bid_security_ratio",
                "投标保证金比例",
                "unclear",
                "识别到投标保证金金额，但未识别到最高限价或预算，无法计算比例。",
                values={"bid_security": self._serialize_candidate(amount), "highest_limit": None, "budget": None},
                evidence=self._evidence_from_candidates([amount]),
            )
        return self._make_check(
            "bid_security_ratio",
            "投标保证金比例",
            "unclear",
            "未识别到明确的投标保证金金额、比例或免收描述。",
            values={},
            evidence={},
        )

    def _check_performance_security_consistency(self, context: dict[str, Any]) -> dict[str, Any]:
        # 文档明确"不设置/免收/无需缴纳"履约保证金时，直接判定一致（pass），无需再找前附表与合同描述。
        # 搜索履约保证金时不能排除"履约保证金"本身（排除词表默认含它，专用于投标保证金场景）。
        not_required_index = self._first_not_required_index(
            context["sections"],
            ["履约保证金"],
            exclude_keywords=["质量保证金", "质保金"],
        )
        if not_required_index is not None and self._window_is_not_required(context["sections"], not_required_index):
            not_required_context = context["sections"][not_required_index]
            return self._make_check(
                "performance_security_consistency",
                "履约保证金一致性",
                "pass",
                "识别到履约保证金不收取/免收/不设置描述。",
                values={"mode": "not_required"},
                evidence=self._evidence_from_sections([not_required_context]),
            )
        schedule = self._clause_signature(
            context["sections"],
            ["履约保证金"],
            ["前附表", "须知前附表", "投标人须知"],
        )
        contract = self._clause_signature(
            context["sections"],
            ["履约保证金"],
            ["合同", "合同条款", "合同主要条款"],
        )
        values = {"schedule": schedule, "contract": contract}

        if not schedule or not contract:
            return self._make_check(
                "performance_security_consistency",
                "履约保证金一致性",
                "unclear",
                "未同时识别前附表与合同中的履约保证金描述。",
                values=values,
                evidence=self._evidence_from_signatures([schedule, contract]),
            )
        if not schedule.get("structured") or not contract.get("structured"):
            return self._make_check(
                "performance_security_consistency",
                "履约保证金一致性",
                "unclear",
                "已定位履约保证金条款，但未提取出可比较的比例、基数、金额或免收描述。",
                values=values,
                evidence=self._evidence_from_signatures([schedule, contract]),
            )

        passed = self._signatures_match(schedule, contract)
        return self._make_check(
            "performance_security_consistency",
            "履约保证金一致性",
            "pass" if passed else "fail",
            "前附表与合同中的履约保证金描述一致。" if passed else "前附表与合同中的履约保证金描述不一致。",
            values=values,
            evidence=self._evidence_from_signatures([schedule, contract]),
        )

    def _check_payment_consistency(self, context: dict[str, Any]) -> dict[str, Any]:
        sections = context["sections"]
        signatures = {
            "schedule": self._payment_signature(sections, "前附表", ["前附表", "须知前附表", "投标人须知"]),
            "technical": self._payment_signature(sections, "技术需求", ["技术需求", "技术要求", "采购需求", "服务需求", "项目需求"]),
            "contract": self._payment_signature(sections, "合同", ["合同", "合同条款", "合同主要条款"]),
        }
        missing = [label for label, signature in signatures.items() if not signature]
        if missing:
            return self._make_check(
                "payment_consistency",
                "付款方式一致性",
                "unclear",
                "未完整识别前附表、技术需求及合同中的付款方式。",
                values=signatures,
                evidence=self._evidence_from_signatures(signatures.values()),
            )

        if any(not signature.get("structured") for signature in signatures.values()):
            return self._make_check(
                "payment_consistency",
                "付款方式一致性",
                "unclear",
                "已定位付款条款，但部分描述无法拆分为明确的付款节点、比例或期限。",
                values=signatures,
                evidence=self._evidence_from_signatures(signatures.values()),
            )

        canonical_values = [signature["canonical"] for signature in signatures.values()]
        passed = len(set(canonical_values)) == 1
        return self._make_check(
            "payment_consistency",
            "付款方式一致性",
            "pass" if passed else "fail",
            "前附表、技术需求及合同中的付款方式一致。" if passed else "前附表、技术需求及合同中的付款方式不一致。",
            values=signatures,
            evidence=self._evidence_from_signatures(signatures.values()),
        )

    def _check_evaluation_method(self, context: dict[str, Any]) -> dict[str, Any]:
        evaluation_sections = self._evaluation_sections(context["sections"])
        structured = self._extract_structured_scoring(context["payload"])
        scoring_items = structured["items"]
        if not evaluation_sections and not scoring_items:
            return self._make_check(
                "evaluation_method",
                "评标办法",
                "unclear",
                "未定位到评标办法或评分标准章节。",
                values={},
                evidence={},
            )

        evaluation_text = "\n".join(section["text"] for section in evaluation_sections if section.get("text"))
        table_text = structured.get("table_text") or ""
        combined_text = "\n".join(item for item in (evaluation_text, table_text) if item)
        total_score = self._extract_total_score(combined_text)
        category_scores = self._extract_category_scores(evaluation_text)
        declared_category_scores = structured.get("declared_category_scores") or {}

        issues: list[str] = []
        unclear: list[str] = []
        for category, declared_score in declared_category_scores.items():
            existing = category_scores.get(category)
            if existing is not None and abs(float(existing) - float(declared_score)) > 0.01:
                issues.append(
                    f"{self._category_label(category)}定义冲突：正文 {existing:g} 分，评分表 {declared_score:g} 分"
                )
            category_scores[category] = float(declared_score)

        category_sum = sum(category_scores.values()) if category_scores else None
        if total_score is None:
            if category_sum is not None:
                total_score = category_sum
                unclear.append(f"未识别显式总分，按大类合计 {category_sum:g} 分核对")
            else:
                unclear.append("未识别总分")
        missing_categories = [
            label
            for key, label in (("business", "商务"), ("technical", "技术"), ("price", "价格"))
            if key not in category_scores
        ]
        if missing_categories:
            if (
                total_score is not None
                and category_sum is not None
                and abs(category_sum - total_score) <= 0.01
                and category_scores
            ):
                # 部分项目只有商务+技术两类（价格以权值/系数计分，不设价格分大类），
                # 已识别大类合计与总分一致时不因缺类判 unclear，仅提示。
                unclear.append(
                    f"未识别到{'、'.join(missing_categories)}大类"
                    f"（已识别大类合计 {category_sum:g} 分与总分一致，可能为两类计分或价格按权值计）"
                )
            else:
                unclear.append("未完整识别商务分、技术分、价格分")
        if total_score is not None and category_sum is not None and abs(category_sum - total_score) > 0.01:
            if missing_categories:
                # 大类识别不完整时无法可靠核对"大类合计=总分"，按修订原则判待复核而非不通过。
                unclear.append(
                    f"大类识别不完整，已识别大类合计 {category_sum:g} 分无法与总分 {total_score:g} 分可靠核对，需人工复核"
                )
            else:
                issues.append(f"商务分+技术分+价格分={category_sum:g}，不等于总分 {total_score:g}")

        category_sums: dict[str, float | None] = {}
        detail_sums: dict[str, dict[str, Any]] = {}
        anomalies: list[str] = []
        if scoring_items:
            for category in ("business", "technical", "price"):
                category_items = [item for item in scoring_items if item.get("category") == category]
                if not category_items:
                    if category in category_scores:
                        unclear.append(f"未识别{self._category_label(category)}评分细项")
                    continue
                missing_max = [item for item in category_items if item.get("item_max_score") is None]
                if missing_max:
                    category_sums[category] = None
                    unclear.append(
                        f"{self._category_label(category)}有 {len(missing_max)} 个评分项无法确定单项满分"
                    )
                else:
                    item_sum = round(
                        sum(float(item.get("item_max_score") or 0) for item in category_items),
                        4,
                    )
                    category_sums[category] = item_sum
                    detail_sums[category] = {
                        "sum": item_sum,
                        "items": [item.get("item_max_score") for item in category_items],
                    }
                    expected = category_scores.get(category)
                    if expected is not None and abs(item_sum - float(expected)) > 0.01:
                        generic_name = re.compile(r"^(?:序号|编号|评分项目|设置分值|\d+(?:\.\d+)?)$")
                        # 评分表跨页/跨表拆分时，同一大类细项来自多张表，多半存在漏行，
                        # 按修订原则"解析不完整→待复核"，不据此判不通过。
                        cross_table_count = len(
                            {
                                table_id
                                for item in category_items
                                for table_id in (item.get("evidence") or {}).get("table_ids") or []
                            }
                        )
                        incomplete = (
                            any(
                                generic_name.match(str(item.get("item_name") or "").strip())
                                or "未命名评分项" in str(item.get("item_name") or "")
                                or (item.get("anomalies"))
                                for item in category_items
                            )
                            or (len(category_items) == 1 and float(expected) > 2 * item_sum)
                            or cross_table_count >= 2
                        )
                        if incomplete:
                            unclear.append(
                                f"{self._category_label(category)}评分细项解析不完整"
                                f"（仅识别 {len(category_items)} 项、合计 {item_sum:g} 分），"
                                "无法可靠校验细项合计，需人工复核"
                            )
                        else:
                            issues.append(
                                f"{self._category_label(category)}细项满分合计 {item_sum:g}，不等于大类分值 {float(expected):g}"
                            )
                for item in category_items:
                    if item.get("status") == "unclear":
                        unclear.append(f"评分项“{item.get('item_name') or '未命名'}”规则需复核")
                        continue
                    item_anomalies = [str(value) for value in item.get("anomalies") or []]
                    anomalies.extend(item_anomalies)
            issues.extend(anomalies)
        else:
            detail_sums = self._extract_category_detail_sums(combined_text, category_scores)
            for key, detail in detail_sums.items():
                expected = category_scores.get(key)
                if expected is None or detail.get("sum") is None:
                    continue
                if abs(float(detail["sum"]) - float(expected)) > 0.01:
                    issues.append(
                        f"{self._category_label(key)}细项合计 {float(detail['sum']):g}，不等于大类分值 {float(expected):g}"
                    )
            if not detail_sums:
                unclear.append("未识别可加总的评分细项")
            anomalies = self._detect_score_range_anomalies(combined_text)
            issues.extend(anomalies)

        if issues:
            status = "fail"
            message = "；".join(issues[:4])
        elif unclear:
            status = "unclear"
            message = "；".join(unclear)
        else:
            status = "pass"
            message = "评标总分、大类分值、细项分值和分值区间未发现异常。"

        return self._make_check(
            "evaluation_method",
            "评标办法",
            status,
            message,
            values={
                "total_score": total_score,
                "category_scores": category_scores,
                "category_sums": category_sums,
                "category_detail_sums": detail_sums,
                "scoring_items": scoring_items,
                "anomalies": list(dict.fromkeys(anomalies)),
                "range_anomalies": list(dict.fromkeys(anomalies)),
            },
            evidence=self._evidence_from_sections(evaluation_sections[:5]),
        )

    # ------------------------------------------------------------------
    # OCR payload parsing and candidate extraction
    # ------------------------------------------------------------------
    def _collect_sections(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        data = payload.get("data") if isinstance(payload.get("data"), dict) else payload
        sections: list[dict[str, Any]] = []

        def add_section(raw: Any, *, source: str) -> None:
            if not isinstance(raw, dict):
                return
            text = self._text_from_node(raw)
            if not text:
                return
            page = raw.get("page")
            bbox = raw.get("bbox") or raw.get("box") or raw.get("bbox_ocr")
            section_base = {
                "page": page if isinstance(page, int) and page > 0 else None,
                "bbox": bbox if isinstance(bbox, (list, tuple)) else None,
                "source": source,
                "type": str(raw.get("type") or source),
            }
            chunks = [chunk.strip() for chunk in re.split(r"[\n\r]+", text) if chunk.strip()]
            if len(chunks) > 1:
                for chunk in chunks:
                    sections.append({**section_base, "text": chunk})
                return
            sections.append({**section_base, "text": text})

        for source_key in ("layout_sections", "table_sections", "logical_tables", "native_tables", "pages"):
            for item in data.get(source_key) or []:
                add_section(item, source=source_key)

        if not sections:
            text = self._text_from_node(data)
            if text:
                sections.append({"page": None, "bbox": None, "text": text, "source": "payload", "type": "text"})

        return sections

    def _limit_candidates(self, payload: dict[str, Any], sections: list[dict[str, Any]]) -> list[dict[str, Any]]:
        candidates = self._money_candidates_by_keywords(
            sections,
            self.LIMIT_KEYWORDS,
            exclude_keywords=[
                *self.BUDGET_KEYWORDS,
                "投标保证金",
                "履约保证金",
                "报价明细",
                "分项报价",
                "单价",
            ],
        )
        try:
            tender_limit = self.reasonableness._extract_tender_max_limit(payload)
        except Exception:
            tender_limit = None
        if tender_limit:
            limit_keyword = str(tender_limit.get("keyword") or "")
            limit_context = str(tender_limit.get("context") or "")
            if candidates:
                # 关键词法已命中真实限价声明，兜底提取（reasonableness）可能把预算/控制价等
                # 非限价金额带入，直接丢弃，避免"预算当限价"的误判。
                tender_limit = None
            elif "预算" in limit_keyword and not re.search(
                r"预算[^。；\n]{0,20}(?:即|就是|作为|等于|同于|视为|按)[^。；\n]{0,10}最高限价"
                r"|最高限价[^。；\n]{0,20}(?:即|就是|等于|同于|按|为)[^。；\n]{0,10}预算",
                self._normalize(limit_context),
            ):
                # 文档只有预算、没有最高限价声明，也没有"预算=限价"的明确表述：
                # 不能把预算当作限价，否则"预算≥限价"恒真。
                tender_limit = None
        if tender_limit:
            candidates.append(
                {
                    "raw_amount": tender_limit.get("raw_amount"),
                    "amount_yuan": tender_limit.get("amount_yuan"),
                    "page": tender_limit.get("page"),
                    "keyword": tender_limit.get("keyword") or "最高限价",
                    "context": tender_limit.get("context"),
                    "locations": tender_limit.get("locations") or [],
                    "score": 1000 + int(tender_limit.get("score") or 0),
                }
            )
        return self._dedupe_amount_candidates(candidates)

    def _money_candidates_by_keywords(
        self,
        sections: list[dict[str, Any]],
        keywords: list[str],
        *,
        category_keywords: list[str] | None = None,
        exclude_keywords: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        candidates: list[dict[str, Any]] = []
        normalized_keywords = [self._normalize(keyword) for keyword in keywords]
        normalized_categories = [self._normalize(keyword) for keyword in category_keywords or []]
        normalized_excludes = [self._normalize(keyword) for keyword in exclude_keywords or []]

        for section in sections:
            text = str(section.get("text") or "")
            if not text.strip():
                continue
            normalized_section = self._normalize(text)
            if normalized_categories and not any(keyword in normalized_section for keyword in normalized_categories):
                continue
            if normalized_excludes and any(keyword in normalized_section for keyword in normalized_excludes):
                # 允许局部上下文再判断，避免整段合同同时出现多个保证金条款时误杀。
                section_has_excluded_signal = True
            else:
                section_has_excluded_signal = False

            for amount in self.reasonableness._extract_money_candidates_from_text(text):
                start = int(amount.get("start") or 0)
                end = int(amount.get("end") or 0)
                local = self._local_context(text, start, end, window=90)
                normalized_local = self._normalize(local)
                keyword_distance = self._nearest_keyword_distance(text, start, end, keywords)
                exclude_distance = self._nearest_keyword_distance(text, start, end, exclude_keywords or [])
                if not any(keyword in normalized_local or keyword in normalized_section for keyword in normalized_keywords):
                    continue
                if exclude_distance is not None and (
                    keyword_distance is None or exclude_distance <= keyword_distance
                ):
                    continue
                if normalized_excludes and any(keyword in normalized_local for keyword in normalized_excludes):
                    if keyword_distance is None or exclude_distance is None or exclude_distance <= keyword_distance:
                        continue
                    # 排除"为最高限价的 X%，即 ¥金额"这类由保证金比例推导出的金额：
                    # 金额与排除关键词（保证金等）同句且句内出现百分比时，视为衍生金额而非独立限价/金额声明。
                    if re.search(r"\d+(?:\.\d+)?\s*%", normalized_local):
                        continue
                if section_has_excluded_signal and not any(keyword in normalized_local for keyword in normalized_keywords):
                    continue
                keyword = self._matched_keyword(local, keywords) or self._matched_keyword(text, keywords)
                candidates.append(
                    {
                        "raw_amount": amount.get("raw_amount"),
                        "amount_yuan": amount.get("amount_yuan"),
                        "page": section.get("page"),
                        "keyword": keyword,
                        "context": local,
                        "locations": [self._location_from_section(section, text=local)],
                        "score": self._candidate_score(
                            local,
                            text,
                            keywords,
                            category_keywords or [],
                            keyword_distance=keyword_distance,
                        ),
                    }
                )

        return self._dedupe_amount_candidates(candidates)

    def _percentage_candidates_by_keywords(
        self,
        sections: list[dict[str, Any]],
        keywords: list[str],
        *,
        exclude_keywords: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        candidates: list[dict[str, Any]] = []
        normalized_keywords = [self._normalize(keyword) for keyword in keywords]
        normalized_excludes = [self._normalize(keyword) for keyword in exclude_keywords or []]
        pattern = re.compile(r"(百分之[零〇一二两三四五六七八九十百点\d\.]+|(?:\d+(?:\.\d+)?\s*%))")

        for section in sections:
            text = str(section.get("text") or "")
            normalized_section = self._normalize(text)
            if not any(keyword in normalized_section for keyword in normalized_keywords):
                continue
            for match in pattern.finditer(text):
                raw = match.group(1)
                if not raw:
                    continue
                local = self._local_context(text, match.start(), match.end(), window=70)
                normalized_local = self._normalize(local)
                if not any(keyword in normalized_local for keyword in normalized_keywords):
                    continue
                if normalized_excludes and any(keyword in normalized_local for keyword in normalized_excludes):
                    continue
                percent = self._parse_percent(raw)
                if percent is None:
                    continue
                candidates.append(
                    {
                        "percent": percent,
                        "raw_value": raw if raw.startswith("百分之") else raw.replace(" ", ""),
                        "page": section.get("page"),
                        "keyword": self._matched_keyword(local, keywords) or keywords[0],
                        "context": local,
                        "locations": [self._location_from_section(section, text=local)],
                        "score": self._candidate_score(local, text, keywords, []),
                    }
                )
        return candidates

    # ------------------------------------------------------------------
    # Clause signatures
    # ------------------------------------------------------------------
    def _clause_signature(
        self,
        sections: list[dict[str, Any]],
        clause_keywords: list[str],
        category_keywords: list[str],
    ) -> dict[str, Any] | None:
        contexts = self._contexts_for_category(sections, clause_keywords, category_keywords)
        if not contexts:
            return None
        combined = "；".join(section["text"][:260] for section in contexts[:3])
        percentages = [
            candidate["percent"]
            for candidate in self._percentage_candidates_by_keywords(contexts, clause_keywords)
        ]
        amounts = [
            candidate["amount_yuan"]
            for candidate in self._money_candidates_by_keywords(contexts, clause_keywords)
        ]
        no_required = any(self._contains_any(section["text"], self.NO_GUARANTEE_KEYWORDS) for section in contexts)
        base = self._guarantee_base(combined)
        if no_required:
            mode = "not_required"
        elif percentages and amounts:
            mode = "mixed"
        elif percentages:
            mode = "percent"
        elif amounts:
            mode = "amount"
        else:
            mode = "unstructured"
        canonical_parts: list[str] = [mode]
        if base:
            canonical_parts.append(base)
        if no_required:
            canonical_parts.append("not_required")
        canonical_parts.extend(f"{round(float(value), 4):g}%" for value in sorted(set(percentages)))
        canonical_parts.extend(f"{round(float(value), 2):g}元" for value in sorted(set(amounts)))
        if mode == "unstructured":
            canonical_parts.append(self._normalize_clause(combined))
        return {
            "canonical": "|".join(canonical_parts),
            "mode": mode,
            "base": base,
            "structured": mode != "unstructured",
            "percentages": sorted(set(round(float(value), 4) for value in percentages)),
            "amounts_yuan": sorted(set(round(float(value), 2) for value in amounts)),
            "not_required": no_required,
            "contexts": [self._section_brief(section) for section in contexts[:3]],
        }

    def _payment_signature(
        self,
        sections: list[dict[str, Any]],
        label: str,
        category_keywords: list[str],
    ) -> dict[str, Any] | None:
        contexts = self._contexts_for_category(sections, self.PAYMENT_KEYWORDS, category_keywords)
        if not contexts:
            return None
        combined = "；".join(section["text"][:300] for section in contexts[:4])
        stages = self._payment_stages_from_text(combined)
        # 同一类目下多段重复条款（如合同条款前后两处相同描述）只保留一份，避免自我不一致。
        seen_stages: set[str] = set()
        unique_stages: list[dict[str, Any]] = []
        for stage in stages:
            stage_key = str(stage.get("canonical") or "")
            if stage_key in seen_stages:
                continue
            seen_stages.add(stage_key)
            unique_stages.append(stage)
        stages = unique_stages
        percentages = sorted({value for stage in stages for value in stage.get("percentages") or []})
        milestones = sorted({trigger for stage in stages for trigger in stage.get("triggers") or []})
        canonical = "||".join(stage["canonical"] for stage in stages)
        if not canonical:
            canonical = self._normalize_clause(combined)
        # 付款方式签名须包含 1~5 个百分比（付款节点比例）。0 个说明只抓到标题/交付期描述；
        # 超过 5 个通常是把中标服务费阶梯费率表等非付款方式内容误当成了付款节点。
        # 此时视为无法可靠解析，交由检查逻辑按 unclear 处理，而非参与一致性比对。
        structured = bool(stages) and 1 <= len(percentages) <= 5
        return {
            "label": label,
            "canonical": canonical,
            "structured": structured,
            "percentages": percentages,
            "milestones": milestones,
            "stages": stages,
            "contexts": [self._section_brief(section) for section in contexts[:4]],
        }

    def _guarantee_base(self, text: str) -> str | None:
        normalized = self._normalize(text)
        for base, keywords in (
            ("contract_amount", ["合同金额", "合同价款", "合同总价", "合同价"]),
            ("winning_amount", ["中标金额", "中标价", "中选金额", "成交金额", "成交价"]),
            ("highest_limit", ["最高限价", "招标控制价"]),
        ):
            if any(self._normalize(keyword) in normalized for keyword in keywords):
                return base
        return None

    def _payment_stages_from_text(self, text: str) -> list[dict[str, Any]]:
        trigger_groups = (
            ("contract_signing", ["合同签订", "签订合同", "合同生效"]),
            ("advance", ["预付款"]),
            ("delivery", ["到货", "交付", "供货完成"]),
            ("progress", ["进度款", "进度支付", "阶段款"]),
            ("acceptance", ["验收", "验收合格"]),
            ("trial", ["试运行"]),
            ("final", ["尾款", "余款", "剩余款"]),
            ("warranty", ["质保期满", "质保金", "质量保证金"]),
            ("invoice", ["发票", "开票"]),
        )
        stages: list[dict[str, Any]] = []
        segments = [
            segment.strip()
            for segment in re.split(r"[；;。\n\r]+", str(text or ""))
            if segment.strip()
        ]
        for segment in segments:
            triggers = [
                key
                for key, keywords in trigger_groups
                if self._contains_any(segment, keywords)
            ]
            percentages = self._percent_values_from_text(segment)
            days = [
                int(value)
                for value in re.findall(r"(\d+)\s*(?:个)?(?:工作日|日|天)内", segment)
            ]
            if not triggers and not percentages and not days:
                continue
            if not triggers and self._contains_any(segment, self.PAYMENT_KEYWORDS):
                triggers = ["payment"]
            # "合同签订后" 属于付款方式的前置修饰，不参与一致性比对，避免措辞差异导致误报。
            canonical_triggers = [trigger for trigger in triggers if trigger != "contract_signing"]
            canonical = ":".join(
                [
                    "+".join(canonical_triggers) or "payment",
                    ",".join(f"{value:g}" for value in percentages) or "-",
                    ",".join(str(value) for value in days) or "-",
                ]
            )
            stages.append(
                {
                    "triggers": triggers or ["payment"],
                    "percentages": percentages,
                    "days": days,
                    "canonical": canonical,
                    "text": self._trim(segment, 220),
                }
            )
        return stages

    def _contexts_for_category(
        self,
        sections: list[dict[str, Any]],
        clause_keywords: list[str],
        category_keywords: list[str],
    ) -> list[dict[str, Any]]:
        contexts: list[dict[str, Any]] = []
        for section in sections:
            text = str(section.get("text") or "")
            chunks = [chunk.strip() for chunk in re.split(r"[\n\r。；;]+", text) if chunk.strip()]
            for chunk in chunks or [text]:
                normalized = self._normalize(chunk)
                if not any(self._normalize(keyword) in normalized for keyword in clause_keywords):
                    continue
                if not any(self._normalize(keyword) in normalized for keyword in category_keywords):
                    continue
                contexts.append({**section, "text": chunk})
        if contexts:
            return contexts

        # Fallback: when the category title and the clause are split across adjacent OCR blocks.
        for index, section in enumerate(sections):
            text = str(section.get("text") or "")
            normalized = self._normalize(text)
            if not any(self._normalize(keyword) in normalized for keyword in category_keywords):
                continue
            window = sections[index:index + 4]
            joined = "\n".join(str(item.get("text") or "") for item in window)
            if any(self._normalize(keyword) in self._normalize(joined) for keyword in clause_keywords):
                contexts.append({**section, "text": joined})
        return contexts

    # ------------------------------------------------------------------
    # Evaluation method parsing
    # ------------------------------------------------------------------
    def _evaluation_sections(self, sections: list[dict[str, Any]]) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []
        keywords = [
            "评标办法", "评审办法", "评分标准", "评分细则", "综合评分", "评审因素",
            "商务标评审", "技术标评审", "价格标评审", "商务评审", "技术评审", "评分表",
        ]
        # 目录行（"第三章 评标办法 ........."）不能当作正文来源，否则抓到的是一串目录。
        toc_pattern = re.compile(r"第[一二三四五六七八九十0-9]+[章节][^。；\n]{0,40}(?:[.．·]{3,}|\.{3,})")
        for index, section in enumerate(sections):
            text = str(section.get("text") or "")
            if toc_pattern.search(text):
                continue
            if self._contains_any(text, keywords):
                for extra in sections[index:index + 12]:
                    extra_text = str(extra.get("text") or "")
                    if not toc_pattern.search(extra_text):
                        result.append(extra)
        if result:
            deduped: list[dict[str, Any]] = []
            seen: set[tuple[Any, str]] = set()
            for section in result:
                key = (section.get("page"), section.get("text"))
                if key in seen:
                    continue
                seen.add(key)
                deduped.append(section)
            return deduped
        return [
            section
            for section in sections
            if self._contains_any(
                str(section.get("text") or ""),
                ["商务分", "技术分", "价格分", "总分", "满分", "合计", "商务标评审", "技术标评审"],
            )
        ]

    def _extract_structured_scoring(self, payload: dict[str, Any]) -> dict[str, Any]:
        data = payload.get("data") if isinstance(payload.get("data"), dict) else payload
        tables = [
            table
            for table in data.get("logical_tables") or []
            if isinstance(table, dict)
        ]
        raw_items: list[dict[str, Any]] = []
        declared_category_scores: dict[str, float] = {}
        table_text_parts: list[str] = []
        # OCR 常把跨页的大评分表拆成多个逻辑表：类别归属需要跨表延续，
        # 否则后续分表（如"样品情况/质量保证"等细项）会因无类别信息而被丢弃。
        last_category_across_tables: str | None = None
        last_column_map: dict[str, int | None] | None = None

        for table_index, table in enumerate(tables):
            headers = [str(value or "").strip() for value in table.get("headers") or []]
            rows = [list(row) for row in table.get("rows") or [] if isinstance(row, list)]
            title = str(table.get("title") or "").strip()
            table_text = "\n".join(
                [title, " | ".join(headers)]
                + [" | ".join(str(cell or "") for cell in row) for row in rows]
            ).strip()
            column_map = self._score_table_column_map(headers)
            if not self._looks_like_scoring_table(table_text, column_map):
                # 跨页续表：表头可能被 OCR 截断（如只剩"…意一种情形)"），
                # 若数据行与上一张评分表的列结构吻合，则沿用其列映射继续解析。
                if last_column_map is not None and self._table_matches_column_map(table, last_column_map):
                    column_map = last_column_map
                else:
                    continue
            last_column_map = column_map
            if not self._looks_like_scoring_table(table_text, column_map):
                continue
            table_text_parts.append(table_text)

            header_row_count = max(0, int(table.get("header_row_count") or 0))
            data_rows = rows[header_row_count:] if header_row_count < len(rows) else rows
            current_category_text = ""
            current_item_name = ""
            last_category: str | None = last_category_across_tables
            pages = [page for page in table.get("pages") or [] if isinstance(page, int)]
            table_id = str(table.get("id") or f"table_{table_index + 1}")

            for row_offset, row in enumerate(data_rows):
                padded = [str(cell or "").strip() for cell in row]
                if not any(padded):
                    continue
                row_text = " | ".join(value for value in padded if value)
                category_cell = self._cell_at(padded, column_map.get("category"))
                item_cell = self._cell_at(padded, column_map.get("item"))
                if category_cell:
                    current_category_text = category_cell
                if item_cell:
                    current_item_name = item_cell

                category_source = category_cell or current_category_text or row_text
                category = self._score_category_from_text(category_source)
                if category is None:
                    category = self._score_category_from_text(row_text)
                if category is None:
                    # 无类别列且当前行无类别信息时，延续上一行已识别的类别，
                    # 避免漏掉表格中紧随其后的细项行（如"技术水平评价 20"之后的"… 10"）。
                    category = last_category
                if category is None:
                    continue
                last_category = category
                last_category_across_tables = category

                declared_score = self._declared_category_score(category_source, category)
                if declared_score is not None:
                    existing = declared_category_scores.get(category)
                    if existing is None or abs(existing - declared_score) <= 0.01:
                        declared_category_scores[category] = declared_score

                # 类别总分行（如"（二）技术部分得分 | 75"）：该行分值就是大类分值，
                # 不应再作为评分细项累计（否则会重复计数）。
                if re.search(
                    r"(?:商务|资信|技术|价格|报价|经济)\s*部分\s*(?:得分|满分|分)",
                    self._normalize(item_cell or category_cell or ""),
                ) or self._normalize(item_cell or "") in ("序号", "编号", "评分项目", "设置分值"):
                    continue

                max_text = self._cell_at(padded, column_map.get("max"))
                # 跨页续表可能残留被截断的表头行（如"…意一种情形)"）：
                # 无分值且整行无数字的行按表头处理，不当作评分细项。
                if not max_text and not any(re.search(r"\d+", cell) for cell in padded):
                    continue
                criteria_parts = [
                    self._cell_at(padded, column_map.get("criteria")),
                    self._cell_at(padded, column_map.get("deduction")),
                ]
                criteria_text = "；".join(value for value in criteria_parts if value)
                if not criteria_text:
                    criteria_text = row_text

                item_name = item_cell or current_item_name
                if (
                    not item_name
                    or self._looks_like_score_category_label(item_name)
                    or re.fullmatch(r"\d+(?:\.\d+)?", item_name or "")
                ):
                    item_name = self._fallback_score_item_name(padded, column_map)
                if not item_name:
                    if self._contains_any(row_text, ["合计", "小计", "总分"]):
                        continue
                    item_name = f"未命名评分项 {row_offset + 1}"

                item = self._parse_scoring_item(
                    category=category,
                    item_name=item_name,
                    max_text=max_text,
                    criteria_text=criteria_text,
                    page=pages[0] if pages else None,
                    pages=pages,
                    table_id=table_id,
                    row_index=header_row_count + row_offset,
                    row_text=row_text,
                )
                if item:
                    raw_items.append(item)

        return {
            "items": self._merge_scoring_items(raw_items),
            "declared_category_scores": declared_category_scores,
            "table_text": "\n".join(table_text_parts),
        }

    def _score_table_column_map(self, headers: list[str]) -> dict[str, int | None]:
        normalized = [self._normalize(header) for header in headers]

        def find(aliases: list[str], *, excluded: set[int] | None = None) -> int | None:
            excluded = excluded or set()
            for index, header in enumerate(normalized):
                if index in excluded:
                    continue
                if any(self._normalize(alias) in header for alias in aliases):
                    return index
            return None

        category_index = find(self.SCORE_CATEGORY_HEADERS)
        item_index = find(self.SCORE_ITEM_HEADERS, excluded={category_index} if category_index is not None else set())
        max_index = find(self.SCORE_MAX_HEADERS, excluded={value for value in (category_index, item_index) if value is not None})
        criteria_index = find(self.SCORE_CRITERIA_HEADERS)
        deduction_index = find(self.SCORE_DEDUCTION_HEADERS)
        return {
            "category": category_index,
            "item": item_index,
            "max": max_index,
            "criteria": criteria_index,
            "deduction": deduction_index,
        }

    def _table_matches_column_map(self, table: dict[str, Any], column_map: dict[str, int | None]) -> bool:
        """判断表格的数据行是否与给定列映射吻合（用于跨页续表识别）。"""
        item_index = column_map.get("item")
        max_index = column_map.get("max")
        if item_index is None and max_index is None:
            return False
        for row in (table.get("rows") or []):
            if not isinstance(row, (list, tuple)):
                continue
            padded = [str(cell or "").strip() for cell in row]
            max_text = padded[max_index] if max_index is not None and max_index < len(padded) else ""
            if not re.fullmatch(r"(?:\d+(?:\.\d+)?|\d+\s*分)", max_text):
                continue
            if item_index is not None and item_index < len(padded) and padded[item_index]:
                return True
        return False

    def _looks_like_scoring_table(self, table_text: str, column_map: dict[str, int | None]) -> bool:
        normalized = self._normalize(table_text)
        if not any(keyword in normalized for keyword in ("评分", "评审", "得分", "分值", "满分")):
            return False
        mapped = sum(1 for value in column_map.values() if value is not None)
        return mapped >= 2 and (
            column_map.get("item") is not None
            or column_map.get("criteria") is not None
            or column_map.get("deduction") is not None
        )

    @staticmethod
    def _cell_at(row: list[str], index: int | None) -> str:
        if index is None or index < 0 or index >= len(row):
            return ""
        return str(row[index] or "").strip()

    def _score_category_from_text(self, text: str) -> str | None:
        normalized = self._normalize(text)
        if any(token in normalized for token in ("商务", "资信")):
            return "business"
        if "技术" in normalized:
            return "technical"
        if any(token in normalized for token in ("价格", "报价", "经济")):
            return "price"
        return None

    def _looks_like_score_category_label(self, text: str) -> bool:
        normalized = self._normalize(text)
        return bool(
            re.fullmatch(
                r"(?:商务|资信|技术|价格|报价|经济)(?:部分|评分|分)?(?:\d+(?:\.\d+)?分)?",
                normalized,
            )
        )

    def _declared_category_score(self, text: str, category: str) -> float | None:
        label_patterns = {
            "business": r"(?:商务|资信)(?:部分|标|评分|分)",
            "technical": r"技术(?:部分|标|评分|分)",
            "price": r"(?:价格|报价|经济)(?:部分|标|评分|分)",
        }
        pattern = label_patterns.get(category)
        if not pattern:
            return None
        # 仅当来源包含"类别级"标签（如"技术部分得分/技术标/技术评分"）时才视为大类声明，
        # 避免把细项行（"1 | 技术参数响应情况 | …满分38分"）误当成技术大类分值。
        match = re.search(rf"{pattern}[^\d]{{0,20}}(\d+(?:\.\d+)?)\s*分", str(text or ""))
        return float(match.group(1)) if match else None

    def _fallback_score_item_name(self, row: list[str], column_map: dict[str, int | None]) -> str:
        excluded = {value for value in column_map.values() if value is not None}
        for index, value in enumerate(row):
            text = str(value or "").strip()
            if index in excluded or not text:
                continue
            if re.fullmatch(r"\d+(?:\.\d+)?\s*分?", text):
                continue
            return self._trim(text, 80)
        return ""

    def _parse_scoring_item(
        self,
        *,
        category: str,
        item_name: str,
        max_text: str,
        criteria_text: str,
        page: int | None,
        pages: list[int],
        table_id: str,
        row_index: int,
        row_text: str,
    ) -> dict[str, Any] | None:
        combined = "；".join(value for value in (max_text, criteria_text) if value)
        ranges = self._score_ranges_from_text(criteria_text)
        deduction_rule = self._deduction_rule_from_text(criteria_text, max_text=max_text)
        explicit_max = self._explicit_item_max(max_text)
        if explicit_max is None:
            explicit_max = self._explicit_item_max(criteria_text, require_label=True)

        if deduction_rule:
            score_type = "deduction"
            item_max = explicit_max
            if item_max is None and deduction_rule.get("max_deduction") is not None:
                item_max = float(deduction_rule["max_deduction"])
        elif ranges:
            score_type = "interval"
            item_max = explicit_max if explicit_max is not None else max(value["end"] for value in ranges)
        else:
            fixed_scores = self._fixed_scores_from_text(combined)
            score_type = "fixed" if explicit_max is not None or fixed_scores else "unknown"
            item_max = explicit_max if explicit_max is not None else (max(fixed_scores) if fixed_scores else None)

        return {
            "category": category,
            "category_label": self._category_label(category),
            "item_name": self._trim(item_name, 100),
            "score_type": score_type,
            "item_max_score": round(float(item_max), 4) if item_max is not None else None,
            "ranges": ranges,
            "deduction_rule": deduction_rule,
            "status": "pending",
            "anomalies": [],
            "evidence": {
                "page": page,
                "pages": list(dict.fromkeys(pages)),
                "table_id": table_id,
                "row_index": row_index,
                "text": self._trim(row_text, 260),
            },
            "criteria": self._trim(criteria_text, 500),
        }

    def _merge_scoring_items(self, raw_items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        grouped: dict[tuple[str, str], dict[str, Any]] = {}
        for item in raw_items:
            key = (str(item.get("category") or ""), self._normalize(item.get("item_name")))
            current = grouped.get(key)
            if current is None:
                current = {
                    **item,
                    "ranges": list(item.get("ranges") or []),
                    "criteria_parts": [item.get("criteria")] if item.get("criteria") else [],
                    "evidence_list": [item.get("evidence")] if item.get("evidence") else [],
                    "max_candidates": [item.get("item_max_score")] if item.get("item_max_score") is not None else [],
                    "deduction_rules": [item.get("deduction_rule")] if item.get("deduction_rule") else [],
                }
                grouped[key] = current
                continue
            current["ranges"].extend(item.get("ranges") or [])
            if item.get("criteria"):
                current["criteria_parts"].append(item.get("criteria"))
            if item.get("evidence"):
                current["evidence_list"].append(item.get("evidence"))
            if item.get("item_max_score") is not None:
                current["max_candidates"].append(item.get("item_max_score"))
            if item.get("deduction_rule"):
                current["deduction_rules"].append(item.get("deduction_rule"))
            precedence = {"unknown": 0, "fixed": 1, "interval": 2, "deduction": 3}
            if precedence.get(str(item.get("score_type")), 0) > precedence.get(str(current.get("score_type")), 0):
                current["score_type"] = item.get("score_type")

        merged: list[dict[str, Any]] = []
        for current in grouped.values():
            anomalies: list[str] = []
            max_candidates = sorted({round(float(value), 4) for value in current.pop("max_candidates", [])})
            merged_multiple_max = len(max_candidates) > 1
            if len(max_candidates) > 1:
                anomalies.append(
                    f"评分项“{current['item_name']}”出现多个单项满分："
                    + "、".join(f"{value:g}" for value in max_candidates)
                )
            current["item_max_score"] = max_candidates[-1] if max_candidates else None
            current["criteria"] = "；".join(dict.fromkeys(current.pop("criteria_parts", [])))
            evidence_list = [value for value in current.pop("evidence_list", []) if value]
            pages = [page for evidence in evidence_list for page in evidence.get("pages") or [] if isinstance(page, int)]
            current["evidence"] = {
                "page": pages[0] if pages else next((value.get("page") for value in evidence_list if value.get("page")), None),
                "pages": list(dict.fromkeys(pages)),
                "table_ids": list(dict.fromkeys(value.get("table_id") for value in evidence_list if value.get("table_id"))),
                "rows": [value.get("row_index") for value in evidence_list],
                "texts": [value.get("text") for value in evidence_list if value.get("text")],
            }
            deduction_rules = current.pop("deduction_rules", [])
            if deduction_rules:
                caps = [rule.get("max_deduction") for rule in deduction_rules if rule.get("max_deduction") is not None]
                steps = [step for rule in deduction_rules for step in rule.get("step_scores") or []]
                current["deduction_rule"] = {
                    "step_scores": sorted(set(steps)),
                    "max_deduction": max(caps) if caps else None,
                    "deduct_to_zero": any(rule.get("deduct_to_zero") for rule in deduction_rules),
                    "text": "；".join(dict.fromkeys(rule.get("text") for rule in deduction_rules if rule.get("text"))),
                }
            # 多个不同满分合并到同一评分项，说明多个细项行被归并（常见于缺失细项名），
            # 此时区间/重叠分析不可靠，整体标记为 unclear 交人工复核，而非直接判 fail。
            if not merged_multiple_max:
                anomalies.extend(self._score_range_anomalies(current["item_name"], current.get("ranges") or []))
            if merged_multiple_max:
                current["status"] = "unclear"
            elif anomalies:
                current["status"] = "fail"
            elif current.get("item_max_score") is None:
                current["status"] = "unclear"
            elif current.get("score_type") == "deduction" and not (
                (current.get("deduction_rule") or {}).get("max_deduction") is not None
                or (current.get("deduction_rule") or {}).get("deduct_to_zero")
            ):
                current["status"] = "unclear"
            elif current.get("score_type") == "unknown":
                current["status"] = "unclear"
            else:
                current["status"] = "pass"
            current["anomalies"] = list(dict.fromkeys(anomalies))
            merged.append(current)
        return merged

    def _explicit_item_max(self, text: str, *, require_label: bool = False) -> float | None:
        raw = str(text or "").strip()
        if not raw:
            return None
        if not require_label and re.fullmatch(r"\s*\d+(?:\.\d+)?\s*(?:分)?\s*", raw):
            match = re.search(r"\d+(?:\.\d+)?", raw)
            return float(match.group(0)) if match else None
        patterns = [
            r"(?:单项满分|满分|最高得分|最高分|分值|配分|本项)[为：:\s]*(\d+(?:\.\d+)?)\s*分",
            r"(\d+(?:\.\d+)?)\s*分[（(]?(?:满分|最高)[）)]?",
        ]
        for pattern in patterns:
            match = re.search(pattern, raw)
            if match:
                return float(match.group(1))
        return None

    def _fixed_scores_from_text(self, text: str) -> list[float]:
        values = [
            float(value)
            for value in re.findall(r"(?:得|计|赋|给)[为：:\s]*(\d+(?:\.\d+)?)\s*分", str(text or ""))
        ]
        return values

    def _score_ranges_from_text(self, text: str) -> list[dict[str, float]]:
        pattern = re.compile(
            r"(\d+(?:\.\d+)?)\s*(?:-|－|—|–|~|～|至|到)\s*(\d+(?:\.\d+)?)\s*分?"
        )
        return [
            {"start": float(match.group(1)), "end": float(match.group(2))}
            for match in pattern.finditer(str(text or ""))
        ]

    def _deduction_rule_from_text(self, text: str, *, max_text: str = "") -> dict[str, Any] | None:
        raw = str(text or "")
        if "扣" not in raw:
            return None
        step_scores = [
            float(value)
            for value in re.findall(r"扣\s*(\d+(?:\.\d+)?)\s*分", raw)
        ]
        cap = None
        for pattern in (
            r"(?:最多|最高|累计最多|累计最高|上限)[^\d]{0,8}(\d+(?:\.\d+)?)\s*分",
            r"最多扣\s*(\d+(?:\.\d+)?)\s*分",
        ):
            match = re.search(pattern, raw)
            if match:
                cap = float(match.group(1))
                break
        deduct_to_zero = self._contains_any(raw, ["扣完为止", "扣至0分", "扣至零分", "本项分值扣完"])
        explicit_max = self._explicit_item_max(max_text)
        if cap is None and deduct_to_zero and explicit_max is not None:
            cap = explicit_max
        return {
            "step_scores": step_scores,
            "max_deduction": cap,
            "deduct_to_zero": deduct_to_zero,
            "text": self._trim(raw, 300),
        }

    def _score_range_anomalies(self, item_name: str, ranges: list[dict[str, float]]) -> list[str]:
        anomalies: list[str] = []
        seen: set[tuple[float, float]] = set()
        valid: list[tuple[float, float]] = []
        for value in ranges:
            start = float(value.get("start") or 0)
            end = float(value.get("end") or 0)
            pair = (start, end)
            if start > end:
                anomalies.append(f"评分项“{item_name}”分值区间 {start:g}-{end:g} 反向")
                continue
            if pair in seen:
                anomalies.append(f"评分项“{item_name}”分值区间 {start:g}-{end:g} 重复")
            seen.add(pair)
            valid.append(pair)
        ordered = sorted(set(valid))
        for current, following in zip(ordered, ordered[1:]):
            if following[0] < current[1]:
                anomalies.append(
                    f"评分项“{item_name}”分值区间 {current[0]:g}-{current[1]:g} 与 {following[0]:g}-{following[1]:g} 重叠"
                )
            elif following[0] > current[1] + 1:
                anomalies.append(
                    f"评分项“{item_name}”分值区间 {current[0]:g}-{current[1]:g} 与 {following[0]:g}-{following[1]:g} 存在断层"
                )
        return anomalies

    def _extract_total_score(self, text: str) -> float | None:
        patterns = [
            r"(?:总分|总评分|满分)[为：:\s]*([0-9]+(?:\.[0-9]+)?)\s*分",
            r"([0-9]+(?:\.[0-9]+)?)\s*分[，,。\s]*(?:总分|满分)",
        ]
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                return float(match.group(1))
        return None

    def _extract_category_scores(self, text: str) -> dict[str, float]:
        categories = {
            "business": r"(?:商务|资信)",
            "technical": r"技术",
            "price": r"(?:价格|报价|经济)",
        }
        scores: dict[str, float] = {}
        for key, category_pattern in categories.items():
            # 1) 类别级声明："商务部分…满分 30 分" / "（二）技术部分得分 | 类型 | 满分 70 分"
            match = re.search(
                rf"{category_pattern}(?:部分|标|评分)[^\d]{{0,20}}(?:满分|合计|得分)[为：:\s]*([0-9]+(?:\.[0-9]+)?)\s*分",
                text,
            )
            if match is None:
                match = re.search(
                    rf"{category_pattern}(?:部分|标|评分)[^\d]{{0,12}}([0-9]+(?:\.[0-9]+)?)\s*分",
                    text,
                )
            if match is None:
                # 2) 兜底：仅当没有类别级声明时，才接受"X…N分"的宽松匹配。
                match = re.search(
                    rf"{category_pattern}[^\d]{{0,12}}([0-9]+(?:\.[0-9]+)?)\s*分",
                    text,
                )
            if match:
                scores[key] = float(match.group(1))
        return scores

    def _extract_category_detail_sums(self, text: str, category_scores: dict[str, float]) -> dict[str, dict[str, Any]]:
        labels = {
            "business": ["商务评分细项", "商务评分标准", "商务细项", "商务部分"],
            "technical": ["技术评分细项", "技术评分标准", "技术细项", "技术部分"],
            "price": ["价格评分细项", "报价评分细项", "价格评分标准", "报价评分标准", "价格部分", "报价部分"],
        }
        detail_sums: dict[str, dict[str, Any]] = {}
        lines = [line.strip() for line in re.split(r"[\n\r]+", text) if line.strip()]
        if len(lines) <= 1:
            lines = [part.strip() for part in re.split(r"[。；;]", text) if part.strip()]

        for key, key_labels in labels.items():
            expected = category_scores.get(key)
            matched_scores: list[float] = []
            matched_lines: list[str] = []
            for line in lines:
                if not any(label in line for label in key_labels):
                    continue
                scores = [float(item) for item in re.findall(r"([0-9]+(?:\.[0-9]+)?)\s*分", line)]
                if not scores:
                    continue
                if expected is not None and len(scores) > 1:
                    for index, score in enumerate(scores):
                        if abs(score - expected) <= 0.01:
                            scores.pop(index)
                            break
                if scores:
                    matched_scores.extend(scores)
                    matched_lines.append(line[:220])
            if matched_scores:
                detail_sums[key] = {
                    "sum": round(sum(matched_scores), 4),
                    "items": matched_scores,
                    "contexts": matched_lines[:3],
                }
        return detail_sums

    def _detect_score_range_anomalies(self, text: str) -> list[str]:
        anomalies: list[str] = []
        range_pattern = re.compile(
            r"(?<![0-9A-Za-z])"
            r"([0-9]+(?:\.[0-9]+)?)\s*(?:-|－|—|–|~|～|至|到)\s*([0-9]+(?:\.[0-9]+)?)\s*分?"
        )
        lines = [line.strip() for line in re.split(r"[\n\r。；;]", text) if line.strip()]
        for line in lines:
            # 重复检测按"行"（单个评分项）内比较，避免不同评分项使用相同区间时误报重复。
            line_seen: set[tuple[float, float]] = set()
            ranges: list[tuple[float, float]] = []
            for match in range_pattern.finditer(line):
                start = float(match.group(1))
                end = float(match.group(2))
                after = line[match.end():]
                # 排除年份/编号/日期等非分值区间：如"2025-2027年度"、"XTJS2025-263"、
                # "2024年5月-2024年6月"（4 位年份，或紧跟"年/年度"）。
                if start >= 1000 and end >= 1000:
                    continue
                if re.match(r"^\s*(?:年|年度)", after):
                    continue
                if start > end:
                    anomalies.append(f"分值区间 {start:g}-{end:g} 反向")
                    continue
                pair = (start, end)
                if pair in line_seen:
                    anomalies.append(f"分值区间 {start:g}-{end:g} 重复")
                line_seen.add(pair)
                ranges.append(pair)
            ranges.sort()
            for current, following in zip(ranges, ranges[1:]):
                if following[0] < current[1]:
                    anomalies.append(f"分值区间 {current[0]:g}-{current[1]:g} 与 {following[0]:g}-{following[1]:g} 重叠")
                elif following[0] > current[1] + 1:
                    anomalies.append(f"分值区间 {current[0]:g}-{current[1]:g} 与 {following[0]:g}-{following[1]:g} 存在断层")
        return list(dict.fromkeys(anomalies))

    # ------------------------------------------------------------------
    # Formatting helpers
    # ------------------------------------------------------------------
    def _make_check(
        self,
        code: str,
        title: str,
        status: str,
        message: str,
        *,
        values: dict[str, Any],
        evidence: dict[str, Any],
    ) -> dict[str, Any]:
        return {
            "code": code,
            "title": title,
            "status": status,
            "severity": {"pass": "info", "fail": "error", "unclear": "warning"}.get(status, "warning"),
            "message": message,
            "values": values,
            "evidence": self._normalize_evidence_items(evidence),
        }

    def _normalize_evidence_items(self, evidence: Any) -> list[dict[str, Any]]:
        """将内部 locations/contexts 证据统一为可直接定位预览的数组。"""
        if isinstance(evidence, list):
            raw_items = evidence
        elif isinstance(evidence, dict):
            raw_items = [
                *(evidence.get("locations") or []),
                *(evidence.get("contexts") or []),
            ]
        else:
            raw_items = []

        normalized: list[dict[str, Any]] = []
        seen: set[tuple[Any, str, str]] = set()
        for item in raw_items:
            if not isinstance(item, dict):
                continue
            bbox = item.get("bbox") or item.get("box")
            text = self._trim(item.get("text") or item.get("context"), 320)
            page = item.get("page") or item.get("page_number")
            key = (page, str(bbox), text)
            if key in seen or (page in (None, "") and not text):
                continue
            seen.add(key)
            entry: dict[str, Any] = {
                "page": page,
                "text": text,
                "section": item.get("section") or item.get("category") or item.get("source"),
                "coordinate_system": item.get("coordinate_system") or "pdf_point",
            }
            if bbox is not None:
                entry["bbox"] = bbox
            normalized.append(entry)
        return normalized

    def _summarize(self, checks: list[dict[str, Any]], *, payload: dict[str, Any], sections: list[dict[str, Any]]) -> dict[str, Any]:
        pass_count = sum(1 for check in checks if check.get("status") == "pass")
        fail_count = sum(1 for check in checks if check.get("status") == "fail")
        unclear_count = sum(1 for check in checks if check.get("status") == "unclear")
        if fail_count:
            overall_status = "failed"
        elif unclear_count:
            overall_status = "unclear"
        else:
            overall_status = "passed"
        return {
            "overall_status": overall_status,
            "total": len(checks),
            "passed": pass_count,
            "failed": fail_count,
            "unclear": unclear_count,
            "suspicious": fail_count + unclear_count,
            "page_count": payload.get("page_count") or (payload.get("metadata") or {}).get("page_count"),
            "section_count": len(sections),
        }

    def _serialize_candidate(self, candidate: dict[str, Any] | None) -> dict[str, Any] | None:
        if not candidate:
            return None
        result: dict[str, Any] = {
            "raw_amount": candidate.get("raw_amount"),
            "amount_yuan": candidate.get("amount_yuan"),
            "page": candidate.get("page"),
            "keyword": candidate.get("keyword"),
            "context": self._trim(candidate.get("context"), 240),
        }
        if candidate.get("category"):
            result["category"] = candidate.get("category")
        return result

    def _evidence_from_candidates(self, candidates: list[dict[str, Any] | None]) -> dict[str, Any]:
        locations: list[dict[str, Any]] = []
        contexts: list[dict[str, Any]] = []
        for candidate in candidates:
            if not candidate:
                continue
            for location in candidate.get("locations") or []:
                if isinstance(location, dict):
                    locations.append(location)
            contexts.append(
                {
                    "page": candidate.get("page"),
                    "keyword": candidate.get("keyword"),
                    "text": self._trim(candidate.get("context"), 240),
                }
            )
        return {
            "locations": self._dedupe_locations(locations),
            "contexts": [item for item in contexts if item.get("text")],
        }

    def _evidence_from_sections(self, sections: list[dict[str, Any] | None]) -> dict[str, Any]:
        valid_sections = [section for section in sections if isinstance(section, dict)]
        return {
            "locations": self._dedupe_locations([self._location_from_section(section) for section in valid_sections]),
            "contexts": [self._section_brief(section) for section in valid_sections],
        }

    def _evidence_from_signatures(self, signatures: Any) -> dict[str, Any]:
        contexts: list[dict[str, Any]] = []
        for signature in signatures or []:
            if not signature:
                continue
            contexts.extend(signature.get("contexts") or [])
        return {"contexts": contexts}

    @staticmethod
    def _category_label(key: str) -> str:
        return {"business": "商务分", "technical": "技术分", "price": "价格分"}.get(key, key)

    @staticmethod
    def _format_yuan(value: float) -> str:
        if abs(value) >= 10000 and abs(value / 10000 - round(value / 10000)) < 0.0001:
            return f"{value / 10000:g}万元"
        return f"{value:.2f}元"

    # ------------------------------------------------------------------
    # General helpers
    # ------------------------------------------------------------------
    def _text_from_node(self, node: Any) -> str:
        if node is None:
            return ""
        if isinstance(node, str):
            return self._clean_text(node)
        if isinstance(node, (int, float, bool)):
            return str(node)
        if isinstance(node, list):
            return self._clean_text("\n".join(self._text_from_node(item) for item in node))
        if not isinstance(node, dict):
            return ""
        parts: list[str] = []
        for key in ("text", "raw_text", "content", "block_content", "markdown", "html"):
            value = node.get(key)
            if isinstance(value, str) and value.strip():
                parts.append(value)
        for key in ("rows", "records", "headers", "cells", "table", "data"):
            value = node.get(key)
            if isinstance(value, (list, dict)):
                parts.append(self._text_from_node(value))
        return self._clean_text("\n".join(parts))

    @staticmethod
    def _clean_text(value: Any) -> str:
        text = str(value or "")
        text = re.sub(r"\r\n?", "\n", text)
        text = text.replace("\u3000", " ").replace("\xa0", " ")
        text = re.sub(r"[ \t]+", " ", text)
        return text.strip()

    @staticmethod
    def _normalize(value: Any) -> str:
        return re.sub(r"[\s,，。；;：:、（）()\[\]【】<>《》\"'“”‘’]+", "", str(value or "")).lower()

    def _normalize_clause(self, value: Any) -> str:
        normalized = self._normalize(value)
        return normalized[:180]

    def _contains_any(self, text: Any, keywords: list[str]) -> bool:
        normalized = self._normalize(text)
        return any(self._normalize(keyword) in normalized for keyword in keywords)

    def _matched_keyword(self, text: Any, keywords: list[str]) -> str:
        normalized = self._normalize(text)
        for keyword in keywords:
            if self._normalize(keyword) in normalized:
                return keyword
        return ""

    def _local_context(self, text: str, start: int, end: int, *, window: int = 80) -> str:
        left = max(0, start - window)
        right = min(len(text), end + window)
        line_left = text.rfind("\n", 0, start)
        line_right = text.find("\n", end)
        if line_left != -1:
            left = max(left, line_left + 1)
        if line_right != -1:
            right = min(right, line_right)
        return self._clean_text(text[left:right])

    def _candidate_score(
        self,
        local_context: str,
        section_text: str,
        keywords: list[str],
        category_keywords: list[str],
        *,
        keyword_distance: int | None = None,
    ) -> int:
        normalized_local = self._normalize(local_context)
        normalized_section = self._normalize(section_text)
        score = 0
        score += 100 * sum(1 for keyword in keywords if self._normalize(keyword) in normalized_local)
        score += 20 * sum(1 for keyword in keywords if self._normalize(keyword) in normalized_section)
        score += 30 * sum(1 for keyword in category_keywords if self._normalize(keyword) in normalized_section)
        if keyword_distance is not None:
            score += max(0, 80 - keyword_distance)
        return score

    def _nearest_keyword_distance(
        self,
        text: str,
        start: int,
        end: int,
        keywords: list[str],
    ) -> int | None:
        distances: list[int] = []
        for keyword in keywords or []:
            raw_keyword = str(keyword or "")
            if not raw_keyword:
                continue
            for match in re.finditer(re.escape(raw_keyword), text):
                if match.end() <= start:
                    distances.append(start - match.end())
                elif match.start() >= end:
                    distances.append(match.start() - end)
                else:
                    distances.append(0)
        return min(distances) if distances else None

    @staticmethod
    def _trim(value: Any, max_length: int = 220) -> str:
        text = str(value or "").strip()
        if len(text) <= max_length:
            return text
        return text[:max_length].rstrip() + "..."

    def _section_brief(self, section: dict[str, Any]) -> dict[str, Any]:
        return {
            "page": section.get("page"),
            "source": section.get("source"),
            "text": self._trim(section.get("text"), 260),
        }

    def _location_from_section(self, section: dict[str, Any], *, text: str | None = None) -> dict[str, Any]:
        location: dict[str, Any] = {
            "page": section.get("page"),
            "text": self._trim(text if text is not None else section.get("text"), 240),
            "document": "tender",
            "document_role": "tender",
            "section": section.get("category") or section.get("source"),
            "coordinate_system": section.get("coordinate_system") or "pdf_point",
        }
        if section.get("bbox") is not None:
            location["bbox"] = section.get("bbox")
        return location

    def _first_context(
        self,
        sections: list[dict[str, Any]],
        keywords: list[str],
        *,
        include_keywords: list[str],
        exclude_keywords: list[str] | None = None,
    ) -> dict[str, Any] | None:
        for section in sections:
            text = str(section.get("text") or "")
            if not self._contains_any(text, keywords):
                continue
            if exclude_keywords and self._contains_any(text, exclude_keywords):
                continue
            if self._contains_any(text, include_keywords):
                return section
        return None

    def _first_not_required_index(
        self,
        sections: list[dict[str, Any]],
        keywords: list[str],
        *,
        exclude_keywords: list[str] | None = None,
    ) -> int | None:
        """定位首个含"免收/不设置"描述的保证金条款所在 section 下标。"""
        exclude_keywords = list(exclude_keywords or self.GUARANTEE_EXCLUDE_KEYWORDS)
        for index, section in enumerate(sections):
            text = str(section.get("text") or "")
            if not self._contains_any(text, keywords):
                continue
            if exclude_keywords and self._contains_any(text, exclude_keywords):
                continue
            if self._contains_any(text, self.NO_GUARANTEE_KEYWORDS):
                return index
        return None

    def _window_is_not_required(self, sections: list[dict[str, Any]], start_index: int) -> bool:
        """判断免收描述是否为最终勾选项。

        OCR 会把复选框拆成相邻多个段落（"□无须提交"和"■设置保证金"各成一段），
        因此需要合并匹配段及后续段落一起判断：
        - 存在勾选标记（■/√/☑）时，看勾选项后面的选项是免收还是收取；
        - 无勾选标记时，出现免收词且无非否定形式的收取/设置侧，才判免收。
        """
        window_text = " ".join(
            str(section.get("text") or "")
            for section in sections[start_index:start_index + 3]
        )
        checked = [match for match in re.finditer(r"[■√☑]", window_text)]
        if checked:
            # 勾选项的文本范围：从勾选标记到下一个复选框标记（□/■/√/☑）为止。
            marker_end = checked[-1].end()
            next_marker = re.search(r"[□■√☑]", window_text[marker_end:])
            span = next_marker.start() if next_marker else 25
            option = window_text[marker_end:marker_end + span]
            nr = re.search(
                r"(?:不收取|免收|无须提交|无需提交|不要求提交|不设置|无须缴纳|无需缴纳|不需缴纳|不缴纳)",
                option,
            )
            setw = re.search(r"(?<!无)(?<!不)(?:收取|缴纳|提交|设置)", option)
            if nr and not setw:
                return True
            if setw and not nr:
                return False
            if nr:
                return True
            if setw:
                return False
        has_not_required = self._contains_any(window_text, self.NO_GUARANTEE_KEYWORDS)
        has_set_side = re.search(r"(?<!无)(?<!不)(?:收取|缴纳|提交|设置)", window_text) is not None
        return has_not_required and not has_set_side

    def _percent_values_from_text(self, text: str) -> list[float]:
        values: list[float] = []
        for match in re.finditer(r"(百分之[零〇一二两三四五六七八九十百点\d\.]+|(?:\d+(?:\.\d+)?))\s*%?", text):
            local = self._local_context(text, match.start(), match.end(), window=18)
            if "%" not in local and "百分之" not in local:
                continue
            value = self._parse_percent(match.group(1))
            if value is not None:
                values.append(round(value, 4))
        return values

    def _parse_percent(self, raw_value: Any) -> float | None:
        raw = str(raw_value or "").strip().rstrip("%").strip()
        if raw.startswith("百分之"):
            return self._parse_chinese_number(raw.replace("百分之", "", 1))
        try:
            return float(raw)
        except ValueError:
            return None

    def _parse_chinese_number(self, value: str) -> float | None:
        value = str(value or "").strip()
        if not value:
            return None
        if re.fullmatch(r"\d+(?:\.\d+)?", value):
            return float(value)
        digits = {"零": 0, "〇": 0, "一": 1, "二": 2, "两": 2, "三": 3, "四": 4, "五": 5, "六": 6, "七": 7, "八": 8, "九": 9}
        if "点" in value:
            left, right = value.split("点", 1)
            left_value = self._parse_chinese_number(left)
            if left_value is None:
                return None
            decimal = "".join(str(digits.get(char, "")) for char in right)
            if not decimal:
                return left_value
            return float(f"{int(left_value)}.{decimal}")
        if value == "十":
            return 10.0
        if "十" in value:
            left, right = value.split("十", 1)
            tens = digits.get(left, 1 if left == "" else None)
            ones = digits.get(right, 0 if right == "" else None)
            if tens is None or ones is None:
                return None
            return float(tens * 10 + ones)
        if len(value) == 1 and value in digits:
            return float(digits[value])
        return None

    @staticmethod
    def _best_candidate(candidates: list[dict[str, Any]]) -> dict[str, Any] | None:
        valid = [
            item
            for item in candidates
            if item and item.get("amount_yuan") is not None
        ]
        if not valid:
            return None
        return sorted(valid, key=lambda item: (int(item.get("score") or 0), float(item.get("amount_yuan") or 0)), reverse=True)[0]

    @staticmethod
    def _best_percentage_candidate(candidates: list[dict[str, Any]]) -> dict[str, Any] | None:
        if not candidates:
            return None
        return sorted(candidates, key=lambda item: (int(item.get("score") or 0), float(item.get("percent") or 0)), reverse=True)[0]

    def _dedupe_amount_candidates(self, candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
        deduped: dict[tuple[Any, float, str], dict[str, Any]] = {}
        for candidate in candidates:
            try:
                amount = round(float(candidate.get("amount_yuan")), 2)
            except (TypeError, ValueError):
                continue
            key = (candidate.get("page"), amount, self._normalize(candidate.get("keyword")))
            if key not in deduped or int(candidate.get("score") or 0) > int(deduped[key].get("score") or 0):
                deduped[key] = candidate
        return sorted(deduped.values(), key=lambda item: int(item.get("score") or 0), reverse=True)

    @staticmethod
    def _dedupe_locations(locations: list[dict[str, Any]]) -> list[dict[str, Any]]:
        deduped: list[dict[str, Any]] = []
        seen: set[tuple[Any, str, str]] = set()
        for location in locations:
            if not isinstance(location, dict):
                continue
            key = (location.get("page"), str(location.get("bbox")), str(location.get("text")))
            if key in seen:
                continue
            seen.add(key)
            deduped.append(location)
        return deduped

    def _signatures_match(self, left: dict[str, Any], right: dict[str, Any]) -> bool:
        if left.get("canonical") == right.get("canonical"):
            return True
        if left.get("mode") != right.get("mode"):
            return False
        if left.get("not_required") or right.get("not_required"):
            return bool(left.get("not_required") and right.get("not_required"))
        if left.get("base") != right.get("base"):
            return False
        left_percentages = set(left.get("percentages") or [])
        right_percentages = set(right.get("percentages") or [])
        if left_percentages or right_percentages:
            return left_percentages == right_percentages
        left_amounts = set(left.get("amounts_yuan") or [])
        right_amounts = set(right.get("amounts_yuan") or [])
        if left_amounts or right_amounts:
            return left_amounts == right_amounts
        return False
