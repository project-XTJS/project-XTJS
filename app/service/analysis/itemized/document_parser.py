# itemized/document_parser.py
"""
分项报价 - 文档解析 Mixin

负责输入解析、文本清洗、区段定位（纯文本和结构化表格）。
包含最核心的文本提取逻辑，支持处理复杂的 OCR 结构化 JSON。
"""

from __future__ import annotations

import json
import re
from typing import Any


def _extract_text_from_payload(payload: object) -> str:
    """
    从解析后的 OCR JSON 或字符串中提取完整文本。
    支持 layout_sections 分区合并、recognition 嵌套及多种键名适配。
    """
    if isinstance(payload, str):
        return payload

    if isinstance(payload, dict):
        # 1. 尝试获取核心数据容器
        container = payload.get("data") if isinstance(payload.get("data"), dict) else payload

        # 2. 优先通过版面分区 (layout_sections) 合并文本
        layout_sections = container.get("layout_sections")
        if isinstance(layout_sections, list):
            lines = []
            for section in layout_sections:
                if not isinstance(section, dict):
                    continue
                # 兼容 raw_text 或 text 键
                text = section.get("raw_text") or section.get("text")
                if isinstance(text, str) and text.strip():
                    lines.append(text.strip())
            if lines:
                return "\n".join(lines)

        # 3. 尝试从识别结果字段中提取
        recognition = container.get("recognition")
        if isinstance(recognition, dict):
            for key in ("content", "raw_text", "text", "full_text"):
                value = recognition.get(key)
                if isinstance(value, str) and value.strip():
                    return value

        # 4. 尝试从容器或根节点直接提取
        for key in ("content", "raw_text", "text", "full_text"):
            value = container.get(key)
            if isinstance(value, str) and value.strip():
                return value

        for key in ("content", "raw_text", "text", "full_text"):
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                return value

    return str(payload or "")


class DocumentParserMixin:
    # 声明类常量占位符，由组合后的主类提供
    ITEM_SECTION_ANCHORS: tuple
    PRIMARY_ITEM_SECTION_ANCHORS: tuple
    TOTAL_SECTION_ANCHORS: tuple
    TOTAL_KEYWORDS: tuple
    SUBTOTAL_KEYWORDS: tuple
    RATE_KEYWORDS: tuple
    UNIT_KEYWORDS: tuple
    ZERO_AMOUNT_KEYWORDS: tuple
    MONEY_TOLERANCE: Any
    PRIMARY_ITEM_SECTION_NEARBY_PAGE_GAP: int

    def _prepare_document(self, payload: object) -> dict:
        """统一整理输入，抽取文本、分项段落、总价段落和候选检查区间。"""
        parsed_payload = self._parse_payload(payload)
        source_text = (
            _extract_text_from_payload(parsed_payload)
            if parsed_payload is not None
            else str(payload or "")
        )
        normalized_text = self._normalize_text(source_text)
        lines = self._split_lines(normalized_text)

        # 尝试从结构化版面信息中寻找表格区段
        structured_item_sections = self._prioritize_item_sections(
            self._find_layout_table_sections(parsed_payload, self.ITEM_SECTION_ANCHORS)
        )
        structured_total_sections = self._find_layout_table_sections(
            parsed_payload, self.TOTAL_SECTION_ANCHORS
        )
        structured_item_sections = self._isolate_primary_item_sections(
            structured_item_sections,
            total_sections=structured_total_sections,
        )

        # 如果结构化识别失败，回退到纯文本锚点搜索
        fallback_item_sections = self._prioritize_item_sections(
            self._find_sections(lines, self.ITEM_SECTION_ANCHORS)
        )
        total_sections = structured_total_sections or self._find_sections(
            lines, self.TOTAL_SECTION_ANCHORS
        )
        item_sections = structured_item_sections or self._isolate_primary_item_sections(
            fallback_item_sections,
            total_sections=total_sections,
        )
        
        # 合并去重候选区段
        candidate_sections = self._dedupe_sections(item_sections + total_sections)
        if not candidate_sections:
            candidate_sections = [
                {
                    "anchor": "全文",
                    "lines": lines,
                    "source": "full_text",
                    "section_id": "full_text:0",
                }
            ]

        return {
            "payload": parsed_payload,
            "text": source_text,
            "normalized_text": normalized_text,
            "lines": lines,
            "item_sections": item_sections,
            "total_sections": total_sections,
            "candidate_sections": candidate_sections,
        }

    def _isolate_primary_item_sections(
        self,
        sections: list[dict],
        *,
        total_sections: list[dict] | None = None,
    ) -> list[dict]:
        """同一文件多次出现分项报价表时，优先保留靠近开标总价的主报价表。"""
        if len(sections or []) <= 1:
            return sections

        reference_pages = sorted(
            {
                page
                for section in total_sections or []
                for page in (section.get("pages") or [])
                if isinstance(page, int)
            }
        )

        def section_start_locator(section: dict) -> int:
            pages = [page for page in (section.get("pages") or []) if isinstance(page, int)]
            if pages:
                return min(pages)
            start = section.get("start")
            return int(start) if isinstance(start, int) else 10**9

        def section_score(section: dict) -> tuple:
            start_locator = section_start_locator(section)
            anchor = str(section.get("anchor") or "")
            distance = (
                min(abs(start_locator - page) for page in reference_pages)
                if reference_pages
                else start_locator
            )
            return (
                1 if anchor in self.PRIMARY_ITEM_SECTION_ANCHORS else 0,
                1 if section.get("logical_table_refs") else 0,
                -distance,
                -start_locator,
                len(section.get("lines") or []),
            )

        best_section = max(sections, key=section_score)
        best_anchor = str(best_section.get("anchor") or "")
        best_start = section_start_locator(best_section)
        nearby_gap = int(getattr(self, "PRIMARY_ITEM_SECTION_NEARBY_PAGE_GAP", 2) or 2)

        isolated = []
        for section in sections:
            start_locator = section_start_locator(section)
            if section is best_section:
                isolated.append(section)
                continue
            if (
                str(section.get("anchor") or "") == best_anchor
                and abs(start_locator - best_start) <= nearby_gap
            ):
                isolated.append(section)
        return isolated or [best_section]

    def _parse_payload(self, payload: object) -> dict | None:
        """将字符串或对象输入解析为 OCR JSON 字典。"""
        if isinstance(payload, dict):
            return payload
        if not isinstance(payload, str):
            return None
        raw_text = payload.strip()
        if not raw_text.startswith("{"):
            return None
        try:
            parsed = json.loads(raw_text)
        except json.JSONDecodeError:
            return None
        return parsed if isinstance(parsed, dict) else None

    def _normalize_text(self, text: str) -> str:
        """规范换行和空白。"""
        normalized = str(text or "")
        normalized = normalized.replace("\r\n", "\n").replace("\r", "\n")
        normalized = normalized.replace("\u3000", " ").replace("\xa0", " ")
        normalized = re.sub(r"[ \t\f\v]+", " ", normalized)
        normalized = re.sub(r"\n{3,}", "\n\n", normalized)
        # 处理没有换行的紧凑文本（针对某些 OCR 引擎优化）
        if "\n" not in normalized:
            normalized = re.sub(
                r"(小写[:：]\s*[¥￥]?\s*\d[\d,]*(?:\.\d{1,2})?\s*元?)", r"\1\n", normalized
            )
            normalized = re.sub(r"(大写[:：][^\s]{2,30})", r"\1\n", normalized)
            normalized = re.sub(
                r"((?:合计|总计|总价|投标总价|单价合计)[^。；]{0,20})", r"\n\1", normalized
            )
        return normalized.strip()

    def _split_lines(self, text: str) -> list[str]:
        """拆分为有效行。"""
        return [line.strip() for line in text.split("\n") if line and line.strip()]

    def _find_sections(
        self,
        lines: list[str],
        anchors: tuple[str, ...],
        window: int = 80,
        *,
        require_score: bool = True,
    ) -> list[dict]:
        """在纯文本中按锚点定位可能的报价区段。"""
        sections = []
        for idx, line in enumerate(lines):
            matched_anchor = next((anchor for anchor in anchors if anchor in line), None)
            if not matched_anchor or not self._is_anchor_line(line, matched_anchor):
                continue

            end = min(len(lines), idx + window)
            # 遇到下一个大标题提前结束
            for cursor in range(idx + 5, end):
                if self._is_heading_line(lines[cursor]) and not any(
                    anchor in lines[cursor] for anchor in anchors
                ):
                    end = cursor
                    break

            section_lines = lines[idx:end]
            score = self._score_section(section_lines, matched_anchor)
            if require_score and score <= 0:
                continue

            sections.append(
                {
                    "anchor": matched_anchor,
                    "start": idx,
                    "end": end,
                    "score": score,
                    "lines": section_lines,
                }
            )

        sections.sort(key=lambda item: (-item["score"], item["start"]))
        deduped = []
        seen = set()
        for section in sections:
            key = (section["start"], section["end"], section["anchor"])
            if key in seen:
                continue
            seen.add(key)
            deduped.append(
                {
                    "anchor": section["anchor"],
                    "lines": section["lines"],
                    "start": section["start"],
                    "end": section["end"],
                    "source": "text_section",
                    "section_id": f"text:{section['anchor']}:{section['start']}:{section['end']}",
                }
            )
        return deduped

    def _is_anchor_line(self, line: str, anchor: str) -> bool:
        """过滤目录或说明文字中的锚点。"""
        compact = re.sub(r"\s+", "", line)
        anchor_index = compact.find(anchor)
        if anchor_index < 0:
            return False
        if "目录" in compact or "..." in line or ".." in line:
            return False
        if len(compact) > 40 and anchor_index > 6:
            return False
        if len(compact) > 30 and any(
            hint in compact for hint in ("须与", "不一致", "计入", "中标价", "量化")
        ):
            return False
        return True

    def _score_section(self, lines: list[str], anchor: str) -> int:
        """区段打分。"""
        text = "\n".join(lines)
        amount_hits = sum(len(self._extract_money_candidates(line)) for line in lines)
        total_hits = sum(
            1 for line in lines if any(keyword in line for keyword in self.TOTAL_KEYWORDS)
        )
        score = amount_hits + total_hits
        if anchor in ("开标一览表", "报价一览表"):
            score += 2
        if "目录" in text and amount_hits == 0:
            return 0
        return score

    def _should_skip_line(self, line: str) -> bool:
        """过滤无意义行。"""
        compact = re.sub(r"\s+", "", line)
        if not compact:
            return True
        if compact in {"注：", "注"}:
            return True
        if re.fullmatch(r"[-—_·\.0-9/（）()]+", compact):
            return True
        if compact.startswith(("投标人名称", "日期", "大写")):
            return True
        return False

    def _is_heading_line(self, line: str) -> bool:
        """识别章节标题行。"""
        compact = re.sub(r"\s+", "", line)
        serial = self._extract_row_serial(line)
        if serial and (
            self._extract_money_candidates(line) or self._contains_quantity_unit(compact)
        ):
            return False
        return bool(
            re.match(
                r"^(第[一二三四五六七八九十百]+章|[一二三四五六七八九十]+、|\d+\.[\d\.]*|（[一二三四五六七八九十]+）)",
                compact,
            )
        )

    def _contains_quantity_unit(self, text: str) -> bool:
        """判断是否含数量单位。"""
        compact = re.sub(r"\s+", "", text)
        unit_pattern = "|".join(re.escape(unit) for unit in sorted(self.UNIT_KEYWORDS, key=len, reverse=True))
        return bool(
            re.search(
                rf"(?:\d+(?:\.\d+)?\s*(?:{unit_pattern})|(?:{unit_pattern})\s*\d+(?:\.\d+)?)",
                compact,
                re.IGNORECASE,
            )
        )

    def _looks_like_frequency_range_line(self, line: str) -> bool:
        """识别 GHz/MHz 技术参数行。"""
        compact = re.sub(r"\s+", "", line)
        return bool(
            re.match(
                r"^\d+(?:\.\d+)?(?:GHz|Ghz|MHz|kHz|Hz)[~～\-至]\d+(?:\.\d+)?(?:GHz|Ghz|MHz|kHz|Hz)?",
                compact,
                re.IGNORECASE,
            )
        )

    def _looks_like_total_line(self, line: str) -> bool:
        """汇总行识别。"""
        if self._is_table_header_line(line):
            return False
        compact = re.sub(r"\s+", "", line)
        return any(keyword in compact for keyword in self.TOTAL_KEYWORDS) or self._looks_like_subtotal_line(compact)

    def _looks_like_subtotal_line(self, line: str) -> bool:
        """小计行识别。"""
        compact = re.sub(r"\s+", "", line)
        return any(keyword in compact for keyword in self.SUBTOTAL_KEYWORDS)

    def _looks_like_item_row(self, line: str) -> bool:
        """报价明细行识别。"""
        compact = re.sub(r"\s+", "", line)
        if not re.search(r"[\u4e00-\u9fff]", compact):
            return False
        if self._looks_like_total_line(compact):
            return False
        if re.match(r"^\d+(?:\.\d+)?", compact):
            return True
        unit_pattern = "|".join(re.escape(unit) for unit in sorted(self.UNIT_KEYWORDS, key=len, reverse=True))
        return bool(re.search(rf"(?:{unit_pattern})\s*\d+(?:\.\d+)?", compact))

    def _extract_money_candidates(self, line: str) -> list[Any]:
        """金额候选值识别。"""
        candidates = []
        for match in re.finditer(
            r"(?:￥|¥)?\s*((?:\d+,\d{4,}|\d{1,3}(?:,\d{3})+|\d+)(?:\.\d{1,2})?)", line
        ):
            value = self._to_decimal(match.group(1))
            if value is None:
                continue
            around = line[max(0, match.start() - 3) : min(len(line), match.end() + 4)]
            suffix = line[match.end() : min(len(line), match.end() + 5)]
            if "%" in around or "％" in around: continue
            if re.match(r"\s*(?:℃|°C|mm|cm|kg|g|GHz|MHz|kW|dB)(?:\b|$)", suffix, re.IGNORECASE):
                continue
            if not self._looks_like_money_value(value):
                continue
            if (
                re.search(r"(年|月|日|页|GHz|MHz|kW|dB|mm|cm)", around, re.IGNORECASE)
                and value < self._to_decimal("1000")
            ):
                continue
            candidates.append(value)
        return candidates

    def _extract_zero_amount_candidate(self, line: str) -> Any | None:
        """0元项识别（免费/包含）。"""
        normalized = re.sub(r"\s+", " ", line).strip()
        if not normalized or not self._extract_row_serial(normalized):
            return None
        if not any(keyword in normalized for keyword in self.ZERO_AMOUNT_KEYWORDS):
            return None

        unit_pattern = "|".join(re.escape(unit) for unit in sorted(self.UNIT_KEYWORDS, key=len, reverse=True))
        if re.search(
            rf"(?:{unit_pattern})\s*\d+(?:\.\d+)?\s+0(?:\.\d{{1,2}})?(?:\s|$)",
            normalized,
            re.IGNORECASE,
        ):
            return self._to_decimal("0")
        if "免费" in normalized:
            return self._to_decimal("0")
        return None

    def _extract_row_amounts(self, line: str) -> list[Any]:
        """综合提取行内金额。"""
        amounts = self._extract_money_candidates(line)
        if amounts:
            return amounts
        zero_amount = self._extract_zero_amount_candidate(line)
        return [zero_amount] if zero_amount is not None else []

    def _looks_like_money_value(self, value: Any) -> bool:
        """金额合理性过滤。"""
        return value >= self._to_decimal("100")

    def _extract_row_serial(self, line: str) -> str | None:
        """序号提取。"""
        leading_match = re.match(r"^\s*(\d+(?:\.\d+)*)(?:\s+|[\.、．])", line)
        if leading_match:
            serial = leading_match.group(1)
            remain = line[leading_match.end() :].strip()
            if not re.match(r"^\d+(?:\.\d+)?\s*(?:GHz|Ghz|MHz|kHz|Hz|mm|cm|kg|g|dB)\b", f"{serial}{remain}", re.IGNORECASE):
                return serial

        trailing_match = re.search(r"(?:^|\s)(\d+(?:\.\d+)*)(?:[\.、．])\s*$", line)
        if trailing_match and re.search(r"[\u4e00-\u9fff]", line):
            return trailing_match.group(1)
        return None

    def _is_table_header_line(self, line: str) -> bool:
        """表头特征识别。"""
        compact = re.sub(r"\s+", "", line)
        return (
            ("序号" in compact and "单价" in compact and "合计" in compact)
            or ("序号" in compact and any(x in compact for x in ("名称", "内容", "类型")))
            or ("规格型号" in compact and "单位" in compact and "数量" in compact)
            or (
                any(x in compact for x in ("服务类型", "项目名称", "名称", "类型"))
                and "数量" in compact
                and "单价" in compact
                and any(x in compact for x in ("总价", "合计", "金额"))
            )
            or ("搴忓彿" in compact and "鍗曚环" in compact) # 乱码兼容
        )

    def _dedupe_sections(self, sections: list[dict]) -> list[dict]:
        """去重。"""
        deduped = []
        seen = set()
        for section in sections:
            key = (section.get("anchor"), tuple(section.get("lines") or []))
            if key in seen: continue
            seen.add(key)
            deduped.append(section)
        return deduped

    def _matches_other_anchor(self, text: str, anchors: tuple[str, ...]) -> bool:
        matched_anchor = next((anchor for anchor in anchors if anchor in text), None)
        return bool(matched_anchor and self._is_anchor_line(text, matched_anchor))

    def _is_skippable_layout_text(self, text: str) -> bool:
        lines = self._split_lines(self._normalize_text(text))
        return bool(lines) and all(self._should_skip_line(line) for line in lines)

    def _is_layout_bridge_text(self, text: str) -> bool:
        return (
            self._is_skippable_layout_text(text)
            or self._is_spare_parts_marker_text(text)
            or self._is_layout_page_marker_text(text)
            or self._is_layout_seal_text(text)
        )

    def _is_layout_page_marker_text(self, text: str) -> bool:
        compact = re.sub(r"\s+", "", text)
        return re.fullmatch(r"第\d+页", compact) or compact in {"商务部分", "技术部分"}

    def _should_attach_following_layout_table(self, text: str) -> bool:
        if self._is_spare_parts_marker_text(text): return False
        lines = self._split_lines(self._normalize_text(text))
        if not lines: return False
        if any(self._extract_money_candidates(line) for line in lines): return True
        if any(self._extract_zero_amount_candidate(line) is not None for line in lines): return True
        return False

    def _is_spare_parts_marker_text(self, text: str) -> bool:
        compact = re.sub(r"\s+", "", text)
        return "随机备品备件" in compact or ("备件名称" in compact and "规格型号" in compact)

    def _is_layout_seal_text(self, text: str) -> bool:
        compact = re.sub(r"\s+", "", text)
        return bool(compact) and ("公司" in compact or "有限" in compact) and bool(re.search(r"\d{6,}", compact))
