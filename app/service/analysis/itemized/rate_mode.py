# itemized/rate_mode.py
"""
分项报价 - 下浮率模式 Mixin

处理下浮率/优惠率报价模式的特殊逻辑：
模式检测、列项抽取、删减项比对、跳号提示等。
"""

from __future__ import annotations

import re
from collections import Counter
from difflib import SequenceMatcher


class RateModeMixin:

    # 依赖常量与其它 Mixin 提供的方法
    RATE_KEYWORDS: tuple
    ITEM_SECTION_ANCHORS: tuple
    TOTAL_KEYWORDS: tuple
    UNIT_KEYWORDS: tuple
    RATE_LABEL_MATCH_SIMILARITY_THRESHOLD = 0.78
    RATE_LABEL_MATCH_CONTAINMENT_MIN_LENGTH = 4

    # 下浮率模式入口
    def _check_downward_rate_mode(
        self,
        candidate_sections: list[dict],
        tender_document: dict | None = None,
        item_sections: list[dict] | None = None,
    ) -> dict:
        """执行下浮率报价模式下的列项抽取和删减项比对。"""
        # 下浮率报价常出现在“开标一览表”中；它只能证明有报价页，
        # 不能等同于已识别到“分项报价表/已标价工程量清单”。
        if not item_sections:
            return {
                "itemized_table_detected": False,
                "mode": "downward_rate",
                "status": "not_detected",
                "passed": None,
                "summary": "未识别到分项报价表，当前仅检测到下浮率报价页。",
                "checks": {
                    "row_arithmetic": {
                        "status": "not_detected",
                        "issue_count": 0,
                        "issues": [],
                    },
                    "sum_consistency": {
                        "status": "not_detected",
                        "calculated_total": None,
                        "declared_total": None,
                        "difference": None,
                        "matched_total_label": None,
                    },
                    "duplicate_items": {
                        "status": "not_detected",
                        "issue_count": 0,
                        "issues": [],
                    },
                    "missing_item": {
                        "status": "missing",
                        "missing_items": [],
                        "comparison_basis": None,
                        "matched_count": 0,
                        "reference_count": 0,
                        "bid_count": 0,
                        "match_strategies": {},
                        "hints": [],
                        "hint_level": None,
                    },
                },
                "evidence": {
                    "extracted_item_count": 0,
                    "extracted_items": [],
                    "total_candidates": [],
                    "comparison_items": [],
                    "reference_item_count": 0,
                    "reference_items": [],
                    "matched_items": [],
                    "unmatched_reference_items": [],
                },
                "details": [
                    "检测到下浮率模式，但未识别到分项报价表或已标价工程量清单。",
                ],
            }

        relevant_sections = [
            section
            for section in candidate_sections
            if any(
                keyword in "\n".join(section["lines"])
                for keyword in self.RATE_KEYWORDS
            )
        ]
        if not relevant_sections:
            relevant_sections = candidate_sections

        extracted_items = []
        for section in relevant_sections:
            extracted_items.extend(
                self._extract_rate_items(section["lines"], section_context=section)
            )

        extracted_items = self._dedupe_entries(extracted_items)
        serial_gap_hints = self._extract_serial_gap_hints(relevant_sections)
        comparison_items = self._extract_comparison_items_from_sections(
            relevant_sections,
            rate_mode=True,
        )
        reference_items = (
            self._extract_reference_items(tender_document)
            if tender_document
            else []
        )
        comparison_result = (
            self._compare_reference_items(reference_items, comparison_items)
            if reference_items
            else None
        )

        if comparison_result is None:
            missing_items = []
            missing_item_status = "unknown"
            comparison_basis = None
            status = "unknown"
            matched_items = []
            match_strategy_stats = {}
        else:
            missing_items = comparison_result["missing_items"]
            missing_item_status = "missing" if missing_items else "pass"
            comparison_basis = comparison_result["comparison_basis"]
            status = "missing" if missing_items else "pass"
            matched_items = list(comparison_result.get("matched_items") or [])
            match_strategy_stats = dict(
                comparison_result.get("match_strategy_stats") or {}
            )

        details = [
            "检测到下浮率模式，按业务规则跳过下浮率数值本身的校验。",
        ]
        if comparison_result is None:
            details.append(
                "当前未提供招标文件，无法完成招标列项与投标列项的删减项比对。"
            )
        elif missing_items:
            details.append(
                f"对比招标列项后发现疑似删减项：{', '.join(missing_items)}。"
            )
        else:
            details.append(
                "已对比招标文件与投标文件列项，暂未发现明显删减项。"
            )
        if match_strategy_stats.get("label_fallback"):
            details.append(
                f"其中 {match_strategy_stats['label_fallback']} 个列项通过标签回退匹配确认覆盖，"
                "说明投标文件的列项序号存在调整。"
            )
        if serial_gap_hints:
            details.append(
                f"提示：投标文件内部检测到序号可能跳号：{', '.join(serial_gap_hints)}。"
                "该提示仅供人工复核，不直接作为删减项判定依据。"
            )

        return {
            # Only a real itemized/priced BOQ section should mark this as detected.
            # Opening-bid/rate pages are handled above as missing itemized tables.
            "itemized_table_detected": bool(item_sections),
            "mode": "downward_rate",
            "status": status,
            "passed": self._status_to_passed(status),
            "summary": self._build_downward_rate_summary(missing_item_status),
            "checks": {
                "row_arithmetic": {
                    "status": "not_applicable",
                    "issue_count": 0,
                    "issues": [],
                },
                "sum_consistency": {
                    "status": "not_applicable",
                    "calculated_total": None,
                    "declared_total": None,
                    "difference": None,
                    "matched_total_label": None,
                },
                "duplicate_items": {
                    "status": "not_applicable",
                    "issue_count": 0,
                    "issues": [],
                },
                "missing_item": {
                    "status": missing_item_status,
                    "missing_items": missing_items,
                    "comparison_basis": comparison_basis,
                    "matched_count": len(matched_items),
                    "reference_count": len(reference_items),
                    "bid_count": len(comparison_items),
                    "match_strategies": match_strategy_stats,
                    "hints": serial_gap_hints,
                    "hint_level": "info" if serial_gap_hints else None,
                },
            },
            "evidence": {
                "extracted_item_count": len(extracted_items),
                "extracted_items": self._serialize_entries(extracted_items),
                "total_candidates": [],
                "comparison_items": self._serialize_entries(comparison_items),
                "reference_item_count": len(reference_items),
                "reference_items": self._serialize_entries(reference_items),
                "matched_items": matched_items,
                "unmatched_reference_items": missing_items,
            },
            "details": details,
        }

    def _detect_downward_rate_mode(self, sections: list[dict]) -> bool:
        """判断当前文档是否属于下浮率/优惠率报价模式。"""
        for section in sections:
            section_text = "\n".join(section["lines"])
            if not any(keyword in section_text for keyword in self.RATE_KEYWORDS):
                continue
            if "%" in section_text or "％" in section_text:
                return True
            if any(
                keyword in line
                for line in section["lines"]
                for keyword in self.RATE_KEYWORDS
            ):
                return True
        return False

    # 序号提取与缺失检测
    def _extract_serials(self, lines: list[str]) -> list[str]:
        """从候选区段中抽取所有可识别的序号。"""
        serials = []
        for line in lines:
            compact = re.sub(r"\s+", "", line)
            if not re.search(r"[\u4e00-\u9fff]", compact):
                continue
            if self._is_heading_line(compact):
                continue
            if not (
                self._looks_like_item_row(line)
                or any(keyword in line for keyword in self.RATE_KEYWORDS)
                or bool(self._extract_money_candidates(line))
            ):
                continue
            serial = self._extract_row_serial(line)
            if serial:
                serials.append(serial)
        return serials

    def _extract_serial_gap_hints(self, sections: list[dict]) -> list[str]:
        """汇总多个区段中的序号，并给出可能的跳号提示。"""
        serials = []
        for section in sections:
            serials.extend(self._extract_serials(section["lines"]))
        return self._find_missing_serials(serials)

    def _find_missing_serials(self, serials: list[str]) -> list[str]:
        """根据整数序号和子序号推断可能缺失的编号。"""
        if not serials:
            return []

        missing = []
        int_serials = sorted({int(serial) for serial in serials if serial.isdigit()})
        # 如果序号跨度异常大，可能不是连续编号，放弃整数推断
        if (
            len(int_serials) >= 3
            and int_serials[-1] - int_serials[0] > len(int_serials) + 5
        ):
            int_serials = []
        for left, right in zip(int_serials, int_serials[1:]):
            if right - left <= 1:
                continue
            missing.extend([str(number) for number in range(left + 1, right)])

        grouped_children = {}
        for serial in serials:
            if "." not in serial:
                continue
            prefix, child = serial.split(".", 1)
            if not prefix.isdigit() or not child.isdigit():
                continue
            grouped_children.setdefault(prefix, []).append(int(child))

        for prefix, children in grouped_children.items():
            ordered_children = sorted(set(children))
            for left, right in zip(ordered_children, ordered_children[1:]):
                if right - left <= 1:
                    continue
                missing.extend(
                    [f"{prefix}.{number}" for number in range(left + 1, right)]
                )
        return missing

    # 下浮率行项抽取
    def _extract_rate_items(
        self, lines: list[str], *, section_context: dict | None = None
    ) -> list[dict]:
        """在下浮率模式下提取可用于比对的列项标签。"""
        items = []
        row_blocks = self._build_table_row_blocks(lines)
        for block in row_blocks:
            block_text = " ".join(block.get("lines") or [])
            if not self._has_rate_signal(block_text):
                continue
            label = self._extract_comparison_label(
                block_text,
                int(block.get("start_index") or 0),
                rate_mode=True,
            )
            if not label:
                continue
            items.append(
                {
                    "label": label,
                    "amount": None,
                    "source": "downward_rate",
                    "raw_text": block_text[:160],
                    **self._build_entry_context(
                        section_context,
                        serial=block.get("serial") or self._extract_row_serial(block_text),
                        line_index=block.get("start_index"),
                    ),
                }
            )
        if items:
            return items

        for idx, line in enumerate(lines):
            if not self._has_rate_signal(line):
                continue
            if "序号" in line and "项目名称" in line:
                continue
            label = self._extract_comparison_label(line, idx, rate_mode=True)
            if not label:
                continue
            items.append(
                {
                    "label": label,
                    "amount": None,
                    "source": "downward_rate",
                    "raw_text": line[:160],
                    **self._build_entry_context(
                        section_context,
                        serial=self._extract_row_serial(line),
                        line_index=idx,
                    ),
                }
            )
        return items

    # 招标参考列项提取与比对
    def _extract_reference_items(self, document: dict | None) -> list[dict]:
        """从招标参考文档中提取标准列项集合。"""
        if not document:
            return []
        item_sections = document.get("item_sections") or []
        if not item_sections:
            lines = document.get("lines") or []
            item_sections = self._find_sections(
                lines, self.ITEM_SECTION_ANCHORS, require_score=False
            )
        return self._extract_comparison_items_from_sections(
            item_sections, rate_mode=False
        )

    def _extract_comparison_items_from_sections(
        self, sections: list[dict], *, rate_mode: bool
    ) -> list[dict]:
        """从多个区段中提取列项，并按序号与标签去重。"""
        items = []
        for section in sections:
            items.extend(
                self._extract_comparison_items(section["lines"], rate_mode=rate_mode)
            )

        deduped = []
        seen = set()
        for item in items:
            key = (item.get("serial"), item.get("label_key"))
            if key in seen:
                continue
            seen.add(key)
            deduped.append(item)
        return deduped

    def _extract_comparison_items(
        self, lines: list[str], *, rate_mode: bool
    ) -> list[dict]:
        """从单个区段中提取用于招投标比对的列项。"""
        row_blocks = self._build_table_row_blocks(lines)
        if row_blocks:
            block_items = self._extract_comparison_items_from_row_blocks(
                row_blocks,
                rate_mode=rate_mode,
            )
            if block_items:
                return block_items

        items = []
        for idx, line in enumerate(lines):
            compact = re.sub(r"\s+", "", line)
            if not compact:
                continue
            if compact.startswith("随机备品备件") or (
                "备件名称" in compact and "规格型号" in compact
            ):
                break
            if self._should_skip_line(line) or self._looks_like_total_line(line):
                if self._looks_like_total_line(line):
                    break
                continue
            if "序号" in line and "名称" in line:
                continue
            if not re.search(r"[\u4e00-\u9fff]", compact):
                continue

            serial = self._extract_row_serial(line)
            has_rate = self._has_rate_signal(line)
            if not (self._looks_like_item_row(line) or serial or has_rate):
                continue
            if rate_mode and not has_rate and not serial:
                continue

            label = self._extract_comparison_label(line, idx, rate_mode=rate_mode)
            if not label:
                continue
            if self._should_skip_comparison_item_label(label, line, rate_mode=rate_mode):
                continue
            items.append(
                {
                    "serial": serial,
                    "label": label,
                    "label_key": self._build_comparison_label_key(label),
                    "source": "rate_item" if rate_mode else "reference_item",
                }
            )
        return items

    def _extract_comparison_items_from_row_blocks(
        self, row_blocks: list[dict], *, rate_mode: bool
    ) -> list[dict]:
        """优先按重建后的行块抽取列项，降低 OCR 拆行对比对结果的干扰。"""
        items = []
        for block in row_blocks:
            block_text = " ".join(block.get("lines") or [])
            if not block_text:
                continue
            serial = str(
                block.get("serial") or self._extract_row_serial(block_text) or ""
            ).strip()
            has_rate = self._has_rate_signal(block_text)
            if not (self._looks_like_item_row(block_text) or serial or has_rate):
                continue
            if rate_mode and not (has_rate or serial):
                continue
            label = self._extract_comparison_label(
                block_text,
                int(block.get("start_index") or 0),
                rate_mode=rate_mode,
            )
            if not label or self._is_generic_comparison_label(label):
                continue
            if self._should_skip_comparison_item_label(
                label,
                block_text,
                rate_mode=rate_mode,
            ):
                continue
            items.append(
                {
                    "serial": serial or None,
                    "label": label,
                    "label_key": self._build_comparison_label_key(label),
                    "source": "rate_item" if rate_mode else "reference_item",
                }
            )
        return items

    def _extract_comparison_label(
        self, line: str, index: int, *, rate_mode: bool
    ) -> str | None:
        """清洗比较用标签，去掉金额、单位和下浮率尾巴。"""
        label = re.sub(r"^\s*\d+(?:\.\d+)?\s*", "", line)
        label = re.sub(r"^\s*\d+(?:\.\d+)?\s+", "", label)
        if rate_mode:
            label = re.split(
                r"(?:下浮率|优惠率|折扣率|折让率|下浮|%|％)",
                label,
                maxsplit=1,
            )[0]
        label = re.sub(
            r"\s*(?:￥|¥)?\s*\d[\d,]*(?:\.\d{1,2})?\s*$", "", label
        )
        label = re.sub(r"\b(?:免费)\b.*$", "", label)
        label = re.sub(
            r"\s*(?:台|套|项|个|批|次|人|年|月|日|米|吨|樘|组|m2|㎡)\s*\d+(?:\.\d+)?\s.*$",
            "",
            label,
        )
        label = re.sub(r"\s+", " ", label).strip("：: /")
        return label[:80] if label else None

    def _build_comparison_label_key(self, label: str | None) -> str:
        """构造列项比对用标签键，去掉空白和常见标点，保留核心词。"""
        normalized = self._normalize_label_key(label)
        return re.sub(r"[^\u4e00-\u9fffA-Za-z0-9]+", "", normalized)

    def _is_generic_comparison_label(self, label: str | None) -> bool:
        """判断标签是否只是兜底生成的泛化占位文本。"""
        return bool(re.fullmatch(r"第\d+行", str(label or "").strip()))

    def _should_skip_comparison_item_label(
        self,
        label: str | None,
        source_text: str | None,
        *,
        rate_mode: bool,
    ) -> bool:
        """过滤明显不是报价列项的招标/合同条款，避免当作缺失报价项。"""
        if rate_mode:
            return False
        compact_label = re.sub(r"\s+", "", str(label or ""))
        compact_source = re.sub(r"\s+", "", str(source_text or ""))
        combined = compact_label + compact_source
        if not compact_label:
            return True
        if compact_label in {"偏离", "响应", "说明", "条款", "目录"}:
            return True

        clause_markers = (
            "投标人",
            "中标人",
            "招标人",
            "本章第",
            "本项目",
            "应当",
            "应按",
            "须",
            "不得",
            "分包",
            "合同",
            "条款",
            "自行施工",
            "施工总承包",
            "接受",
        )
        strong_clause_markers = (
            "投标人中标后",
            "本章第",
            "分包",
            "偏离",
            "合同约定",
            "招标人负责",
        )
        price_markers = (
            "下浮率",
            "优惠率",
            "折扣率",
            "报价",
            "单价",
            "合价",
            "金额",
            "清单",
        )
        clause_hit_count = sum(1 for marker in clause_markers if marker in combined)
        if any(marker in combined for marker in strong_clause_markers):
            return True
        if clause_hit_count >= 2:
            return True
        if clause_hit_count and not any(marker in compact_label for marker in price_markers):
            return True
        return False

    def _has_reliable_comparison_label(self, item: dict) -> bool:
        """判断列项是否具备足够稳定的标签，可用于序号变化时的回退匹配。"""
        label = str(item.get("label") or "").strip()
        label_key = str(item.get("label_key") or "").strip()
        return bool(
            label_key
            and not self._is_generic_comparison_label(label)
            and len(label_key) >= 2
        )

    def _has_rate_signal(self, text: str) -> bool:
        """判断文本中是否包含下浮率/优惠率信号。"""
        return bool(text) and (
            any(keyword in text for keyword in self.RATE_KEYWORDS)
            or "%" in text
            or "％" in text
        )

    def _compare_reference_items(
        self, reference_items: list[dict], bid_items: list[dict]
    ) -> dict:
        """比较招标与投标列项，输出疑似缺失项。"""
        missing_items = []
        matched_items = []
        match_strategy_stats: Counter[str] = Counter()
        has_serial_reference = any(item.get("serial") for item in reference_items)
        has_serial_bid = any(item.get("serial") for item in bid_items)

        for item in reference_items:
            match_payload = self._match_reference_item(item, bid_items)
            if match_payload is None:
                missing_items.append(self._format_comparison_item(item))
                continue
            strategy = str(match_payload["strategy"])
            match_strategy_stats[strategy] += 1
            matched_items.append(
                {
                    "reference": self._format_comparison_item(item),
                    "bid": self._format_comparison_item(match_payload["bid_item"]),
                    "strategy": strategy,
                }
            )

        deduped_missing = []
        seen = set()
        for item in missing_items:
            if item in seen:
                continue
            seen.add(item)
            deduped_missing.append(item)
        comparison_basis = self._resolve_comparison_basis(
            has_serial_reference=has_serial_reference,
            has_serial_bid=has_serial_bid,
            match_strategy_stats=match_strategy_stats,
        )
        return {
            "comparison_basis": comparison_basis,
            "missing_items": deduped_missing,
            "matched_items": matched_items,
            "match_strategy_stats": dict(match_strategy_stats),
        }

    def _match_reference_item(
        self, reference_item: dict, bid_items: list[dict]
    ) -> dict | None:
        """为招标列项寻找最合适的投标列项匹配，优先序号+标签，再回退到标签。"""
        reference_serial = str(reference_item.get("serial") or "").strip()
        same_serial_candidates = (
            [
                item
                for item in bid_items
                if str(item.get("serial") or "").strip() == reference_serial
            ]
            if reference_serial
            else []
        )

        same_serial_label_match = self._find_best_label_match(
            reference_item,
            same_serial_candidates,
        )
        if same_serial_label_match is not None:
            return {
                "bid_item": same_serial_label_match,
                "strategy": "serial_and_label",
            }

        any_label_match = self._find_best_label_match(reference_item, bid_items)
        if any_label_match is not None:
            matched_serial = str(any_label_match.get("serial") or "").strip()
            strategy = (
                "label_fallback"
                if reference_serial and matched_serial and matched_serial != reference_serial
                else "label"
            )
            return {
                "bid_item": any_label_match,
                "strategy": strategy,
            }

        if same_serial_candidates and (
            not self._has_reliable_comparison_label(reference_item)
            or any(
                not self._has_reliable_comparison_label(candidate)
                for candidate in same_serial_candidates
            )
        ):
            return {
                "bid_item": same_serial_candidates[0],
                "strategy": "serial_only",
            }
        return None

    def _find_best_label_match(
        self, reference_item: dict, bid_items: list[dict]
    ) -> dict | None:
        """根据标签相似度为参考列项选择最佳候选。"""
        reference_label_key = str(reference_item.get("label_key") or "").strip()
        if not reference_label_key:
            return None

        best_item = None
        best_score = 0.0
        for bid_item in bid_items:
            candidate_label_key = str(bid_item.get("label_key") or "").strip()
            score = self._score_comparison_label_keys(
                reference_label_key,
                candidate_label_key,
            )
            if score > best_score:
                best_score = score
                best_item = bid_item

        threshold = float(
            getattr(self, "RATE_LABEL_MATCH_SIMILARITY_THRESHOLD", 0.78) or 0.78
        )
        return best_item if best_item is not None and best_score >= threshold else None

    def _score_comparison_label_keys(
        self, reference_label_key: str, candidate_label_key: str
    ) -> float:
        """计算两个列项标签键的匹配分数。"""
        if not reference_label_key or not candidate_label_key:
            return 0.0
        if reference_label_key == candidate_label_key:
            return 1.0

        threshold = float(
            getattr(self, "RATE_LABEL_MATCH_SIMILARITY_THRESHOLD", 0.78) or 0.78
        )
        min_length = int(
            getattr(self, "RATE_LABEL_MATCH_CONTAINMENT_MIN_LENGTH", 4) or 4
        )
        shorter, longer = sorted(
            (reference_label_key, candidate_label_key),
            key=len,
        )
        if len(shorter) >= min_length and shorter in longer:
            return max(threshold, len(shorter) / max(len(longer), 1))
        return SequenceMatcher(None, reference_label_key, candidate_label_key).ratio()

    def _resolve_comparison_basis(
        self,
        *,
        has_serial_reference: bool,
        has_serial_bid: bool,
        match_strategy_stats: Counter[str],
    ) -> str:
        """根据匹配过程归纳本次删减项比对的主要依据。"""
        if match_strategy_stats.get("label_fallback"):
            return "tender_vs_bid_serial_then_label"
        if has_serial_reference and has_serial_bid:
            return "tender_vs_bid_serial"
        return "tender_vs_bid_label"

    def _format_comparison_item(self, item: dict) -> str:
        """把列项格式化成便于展示和人工复核的字符串。"""
        serial = item.get("serial")
        label = item.get("label")
        if serial and label:
            return f"{serial}:{label}"
        return label or str(serial or "")

    # 下浮率模式摘要
    def _build_downward_rate_summary(self, missing_item_status: str) -> str:
        """生成下浮率模式下的摘要结论。"""
        if missing_item_status == "missing":
            return "检测到下浮率模式，并发现疑似删减项。"
        if missing_item_status == "pass":
            return "检测到下浮率模式，已完成招标列项与投标列项比对，暂未发现删减项。"
        return (
            "检测到下浮率模式，但当前缺少足够参考信息，无法完成删减项比对。"
        )
