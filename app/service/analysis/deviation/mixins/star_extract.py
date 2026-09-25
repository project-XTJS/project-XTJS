# -*- coding: utf-8 -*-
"""星标条款提取 Mixin"""
import re
from typing import Any


class StarExtractMixin:
    """负责从招标文件中提取 ★、△、▲ 条款。"""

    _LEADING_MARKER_RE = re.compile(
        r"^\s*(?:(?:[（(]?\d{1,2}(?:[)）、.．]|\s+))|(?:[一二三四五六七八九十]+[、.．]))?\s*([★△▲])\s*"
    )
    _NUMBERED_MARKER_RE = re.compile(r"([★△▲])\s*(?:\d{1,2}[、.．)）](?!\d)|[（(]\d{1,2}[)）])")
    _NUMBERED_CHILD_RE = re.compile(r"(?<!\S)(?=(?:\d{1,2}[、.．)）](?!\d)|[（(]\d{1,2}[)）]))")
    _LEGEND_RE = re.compile(
        r"(?:标注|标记|标识|符号|图例|以上|本技术规格书中)[^。；;\n]{0,35}[★△▲]"
        r"|[★△▲][^。；;\n]{0,18}(?:代表|表示|为.{0,12}(?:指标|参数|条款|必须|加分))"
        r"|标[★△▲]条款"
    )
    _PRICE_TERMS = ("报价", "限价", "预算", "价格", "单价", "折扣率", "下浮率", "优惠率", "折让率", "结算金额")
    _SCORE_TERMS = ("评分", "得分", "扣分", "评标办法", "加分规则")
    _DELIVERY_PAYMENT_TERMS = ("交货期", "交付时间", "交付期限", "付款方式", "付款条件", "结算方式")
    _SUBMISSION_TERMS = (
        "证明材料", "证明文件", "资质证明", "资质证书", "资格证明", "身份证明",
        "承诺函", "配置清单", "明细清单", "提交材料", "提交资料", "检测报告",
        "许可证", "授权书", "截图", "格式自拟", "加盖公章", "签字盖章",
    )

    # 依赖常量
    STAR_RE: re.Pattern
    TRIANGLE_RE: re.Pattern
    IMPORTANT_RE: re.Pattern
    MARKER_RE: re.Pattern
    ITEM_MARKER_RE: re.Pattern
    REQUIREMENT_CHAPTER_STRONG_HINTS: tuple
    REQUIREMENT_CHAPTER_WEAK_HINTS: tuple
    REQUIREMENT_CHAPTER_EXCLUDE_HINTS: tuple
    STAR_REQUIREMENT_EXCLUDE_HINTS: tuple
    STOP_HINTS: tuple

    # 依赖工具方法
    _norm: Any
    _clean_req: Any
    _fragments: Any
    _split_lines: Any
    _page_lines: Any
    _is_boundary: Any
    _has_star_marker: Any
    _infer_section: Any

    def _extract_star_requirements(self, tender_payload: dict) -> list[dict[str, Any]]:
        """仅在需求章节内扫描带 ★、△、▲ 的具体要求。"""
        lines = self._page_lines(tender_payload)
        scopes = self._chapter_scopes_for_star(lines)
        return self._collect_star_requirements_from_scopes(lines, scopes, tender_payload=tender_payload)

    def _collect_star_requirements_from_scopes(
        self,
        lines: list[dict[str, Any]],
        scopes: list[tuple[int, int, str]],
        *,
        tender_payload: dict | None = None,
    ) -> list[dict[str, Any]]:
        """优先收集结构化表格行，再收集表格外的带标记文本。"""
        out: list[dict[str, Any]] = []
        seen = set()
        table_entries, table_regions = self._table_star_entries(tender_payload or {}, lines, scopes)
        entries = list(table_entries)
        for start_idx, end_idx, chapter_title in scopes:
            entries.extend(self._iter_star_requirement_entries(
                lines,
                start_idx=start_idx,
                end_idx=end_idx,
                chapter_title=chapter_title,
                table_regions=table_regions,
            ))
        entries.sort(key=lambda entry: (entry.get("page") or 0, entry.get("source_order", 0), entry.get("line_number") or 0))
        for entry in entries:
            req = self._clean_req(entry["text"])
            req_norm = self._norm(req)
            if len(req_norm) < 4 or req_norm in seen:
                continue
            if self._star_exclusion_reason(entry, req):
                continue
            seen.add(req_norm)
            marker_type = entry["marker_type"]
            requirement_kind = "mandatory" if marker_type == "star" else "bonus"
            prefix = {"star": "STAR", "triangle": "TRI", "important": "IMP"}[marker_type]
            out.append({
                "requirement_id": f"{prefix}-{len(out)+1:03d}",
                "requirement": req,
                "marker_type": marker_type,
                "requirement_kind": requirement_kind,
                "section_type": entry["section_type"],
                "page": entry["page"],
                "bbox": entry.get("bbox"),
                "line_number": entry["line_number"],
                "normalized_requirement": req_norm,
                "fragments": self._fragments(req),
                "chapter_title": entry["chapter_title"],
            })
        return out

    def _chapter_scopes_for_star(self, lines: list[dict[str, Any]]) -> list[tuple[int, int, str]]:
        """查找招标文件中“需求/要求/标准/任务书”类章节的范围。"""
        if not lines:
            return []

        def compact(text: str) -> str:
            return re.sub(r"\s+", "", str(text or "")).replace("：", "").replace(":", "")

        def is_chapter_heading(text: str) -> bool:
            t = compact(text)
            if not re.match(r"^第[一二三四五六七八九十百0-9]+章", t):
                return False
            if len(re.findall(r"第[一二三四五六七八九十百0-9]+章", t)) > 1:
                return False
            return len(t) <= 36

        def chapter_score(text: str) -> int:
            title = compact(text)
            if not title or not is_chapter_heading(title):
                return 0
            if any(token in title for token in self.REQUIREMENT_CHAPTER_EXCLUDE_HINTS):
                return 0

            score = 0
            for token in self.REQUIREMENT_CHAPTER_STRONG_HINTS:
                if token in title:
                    score += 6
            for token in self.REQUIREMENT_CHAPTER_WEAK_HINTS:
                if token in title:
                    score += 2
            if "技术" in title:
                score += 2
            return score

        chapter_starts = [
            idx for idx, item in enumerate(lines) if is_chapter_heading(str(item.get("text", "")))
        ]
        if not chapter_starts:
            return []

        scopes: list[tuple[int, int, str, int]] = []
        for position, start_idx in enumerate(chapter_starts):
            end_idx = (
                chapter_starts[position + 1] - 1
                if position + 1 < len(chapter_starts)
                else len(lines) - 1
            )
            title = str(lines[start_idx].get("text", ""))
            score = chapter_score(title)
            if score <= 0:
                continue
            scopes.append((start_idx, end_idx, title, score))

        if not scopes:
            return []

        best_score = max(score for _, _, _, score in scopes)
        selected = [
            (start_idx, end_idx, title)
            for start_idx, end_idx, title, score in scopes
            if score >= max(2, best_score - 2)
        ]
        return selected

    def _star_exclusion_reason(self, entry: dict[str, Any], requirement: str) -> str | None:
        """只让具体的技术或服务要求进入偏离检查。"""
        raw = str(entry.get("raw_text") or entry.get("text") or "")
        compact = re.sub(r"\s+", "", requirement)
        content = re.sub(
            r"^\s*(?:\d{1,2}[、.．)）]|[（(]\d{1,2}[)）])\s*",
            "",
            str(entry.get("content_text") or requirement),
        ).strip()
        content_compact = re.sub(r"\s+", "", content)
        if self._LEGEND_RE.search(raw):
            return "marker_legend"
        if any(term in compact for term in self._PRICE_TERMS) and not (
            entry.get("marker_type") == "important" and self._important_has_service_requirement(content_compact)
        ):
            return "pricing"
        if any(term in compact for term in self._SCORE_TERMS):
            return "scoring"
        if any(term in compact for term in self._DELIVERY_PAYMENT_TERMS):
            return "delivery_or_payment"
        proof_compact = compact
        proof_content = content_compact
        main_clause = re.split(r"[（(]", compact, maxsplit=1)[0]
        if len(main_clause) >= 8 and re.search(r"[：:≥≤]|(?:应|须|需|支持)", main_clause):
            proof_compact = main_clause
            proof_content = re.split(r"[（(]", content_compact, maxsplit=1)[0]
        if "资质" in proof_compact or "经营许可证" in proof_compact:
            return "submission"
        if re.search(r"(?:资质|资格|证明材料|证明文件|证书|清单|交付物).{0,20}(?:要求|提供|提交|须|应|：|:)", proof_compact):
            return "submission"
        if (
            re.match(r"^(?:投标人|供应商|投标供应商)?(?:应|须|需)?(?:提供|提交|出具|附上|上传)", proof_content)
            and any(term in proof_content for term in self._SUBMISSION_TERMS)
        ):
            return "submission"
        if any(term in compact for term in ("格式自拟", "加盖公章", "签字盖章")):
            return "submission"
        if re.search(r"^(?:投标人|供应商)(?:应|须|需)说明|^(?:投标人|供应商).{0,60}作出说明", proof_content):
            return "submission"
        if any(term in proof_content for term in ("工程设计资料", "技术资料", "调试资料", "人员配置名单")):
            return "submission"
        if self._is_star_heading(requirement):
            return "heading_without_requirement"
        return None

    @staticmethod
    def _important_has_service_requirement(text: str) -> bool:
        """含报价的 ▲ 条款只有同时规定服务或备件义务时才保留。"""
        return bool(
            re.search(r"(?:维保方案|维保服务|维修服务|售后服务|技术支持|备件.{0,8}供应)", text)
            and re.search(r"(?:提供|保障|保证|支持|供应|延续|不少于|至少)", text)
        )

    @staticmethod
    def _marker_type(marker: str) -> str:
        return {"★": "star", "△": "triangle", "▲": "important"}[marker]

    @staticmethod
    def _is_star_heading(text: str) -> bool:
        compact = re.sub(r"\s+", "", str(text or "")).strip("：:")
        return len(compact) <= 16 and bool(re.search(r"(?:要求|标准|条款|项目总体|服务周期|内容)$", compact))

    @staticmethod
    def _bbox_overlap_fraction(inner: Any, outer: Any) -> float:
        if not isinstance(inner, (list, tuple)) or not isinstance(outer, (list, tuple)):
            return 0.0
        if len(inner) < 4 or len(outer) < 4:
            return 0.0
        try:
            a, b = [float(v) for v in inner[:4]], [float(v) for v in outer[:4]]
            area = max(0.0, a[2] - a[0]) * max(0.0, a[3] - a[1])
            overlap = max(0.0, min(a[2], b[2]) - max(a[0], b[0])) * max(0.0, min(a[3], b[3]) - max(a[1], b[1]))
            return overlap / area if area > 0 else 0.0
        except (TypeError, ValueError):
            return 0.0

    def _table_scope(
        self,
        page: int | None,
        bbox: Any,
        lines: list[dict[str, Any]],
        scopes: list[tuple[int, int, str]],
    ) -> str | None:
        if page is None:
            return None
        for start_idx, end_idx, title in scopes:
            start, end = lines[start_idx], lines[end_idx]
            if not (isinstance(start.get("page"), int) and isinstance(end.get("page"), int)):
                continue
            if not start["page"] <= page <= end["page"]:
                continue
            if page == start["page"] and bbox and start.get("bbox"):
                if float(bbox[1]) < float(start["bbox"][1]):
                    continue
            next_idx = end_idx + 1
            if next_idx < len(lines) and page == lines[next_idx].get("page") and bbox and lines[next_idx].get("bbox"):
                if float(bbox[1]) >= float(lines[next_idx]["bbox"][1]):
                    continue
            return title
        return None

    def _table_star_entries(
        self,
        tender_payload: dict,
        lines: list[dict[str, Any]],
        scopes: list[tuple[int, int, str]],
    ) -> tuple[list[dict[str, Any]], dict[int, list[dict[str, Any]]]]:
        doc = self._doc_container(tender_payload)
        entries: list[dict[str, Any]] = []
        regions: dict[int, list[dict[str, Any]]] = {}
        structured_keys: set[tuple[int, Any]] = set()
        logical_tables = doc.get("logical_tables") or []
        for table_index, table in enumerate(logical_tables):
            if not isinstance(table, dict) or not isinstance(table.get("rows"), list):
                continue
            try:
                page = int(table.get("page") or (table.get("pages") or [None])[0])
            except (TypeError, ValueError, IndexError):
                continue
            bbox = table.get("bbox")
            title = self._table_scope(page, bbox, lines, scopes)
            if not title:
                continue
            structured_keys.add((page, table.get("table_index", table_index)))
            table_text = str(table.get("text") or "") or " ".join(
                " ".join(str(cell or "") for cell in row)
                for row in table["rows"] if isinstance(row, list)
            )
            regions.setdefault(page, []).append({"bbox": bbox, "norm": self._norm(table_text)})
            headers = [str(item or "") for item in (table.get("headers") or [])]
            header_is_row = bool(
                headers
                and re.fullmatch(r"\d{1,3}[.、．]?", headers[0].strip())
                and any(cell.strip() in ("★", "△", "▲") or self._LEADING_MARKER_RE.match(cell) for cell in headers)
            )
            if header_is_row:
                entries.extend(self._star_entries_from_table_row(
                    headers, [], page=page, bbox=bbox, chapter_title=title,
                    table_index=table_index, row_index=0,
                ))
            for row_index, row in enumerate(table["rows"], start=int(header_is_row)):
                if isinstance(row, list):
                    entries.extend(self._star_entries_from_table_row(
                        row, headers, page=page, bbox=bbox, chapter_title=title,
                        table_index=table_index, row_index=row_index,
                    ))

        for table_index, table in enumerate(doc.get("table_sections") or []):
            if not isinstance(table, dict):
                continue
            try:
                page = int(table.get("page") or (table.get("pages") or [None])[0])
            except (TypeError, ValueError, IndexError):
                continue
            if (page, table.get("table_index", table_index)) in structured_keys:
                continue
            bbox = table.get("bbox")
            title = self._table_scope(page, bbox, lines, scopes)
            if not title:
                continue
            raw_lines = self._split_lines(str(table.get("text") or ""))
            if not raw_lines:
                continue
            regions.setdefault(page, []).append({"bbox": bbox, "norm": self._norm(" ".join(raw_lines))})
            for row_index, line in enumerate(raw_lines):
                cells = re.split(r"\s*\|\s*|\t", line)
                entries.extend(self._star_entries_from_table_row(
                    cells, [], page=page, bbox=bbox, chapter_title=title,
                    table_index=table_index, row_index=row_index,
                ))
        return entries, regions

    def _star_entries_from_table_row(
        self,
        row: list[Any],
        headers: list[str],
        *,
        page: int,
        bbox: Any,
        chapter_title: str,
        table_index: int,
        row_index: int,
    ) -> list[dict[str, Any]]:
        cells = [re.sub(r"\s+", " ", str(cell or "")).strip() for cell in row]
        multi_entries = self._multiple_markers_in_table_row(
            cells, headers, page=page, bbox=bbox, chapter_title=chapter_title,
            table_index=table_index, row_index=row_index,
        )
        if multi_entries is not None:
            return multi_entries
        marker_idx: int | None = None
        marker: str | None = None
        for idx, cell in enumerate(cells):
            header = headers[idx].strip() if idx < len(headers) else ""
            is_marker_column = idx <= 1 or any(
                token in header for token in ("重要性", "重要程度", "标记", "标识", "星号", "关键性", "实质性")
            )
            if cell in ("★", "△", "▲") and is_marker_column:
                marker_idx, marker = idx, cell
                break
        if marker is None:
            for idx, cell in enumerate(cells):
                if cell in ("★", "△", "▲"):
                    continue
                header = headers[idx].strip() if idx < len(headers) else ""
                if header and any(token in header for token in ("备注", "注释", "补充说明")):
                    continue
                match = self._LEADING_MARKER_RE.match(cell)
                if match:
                    marker_idx, marker = idx, match.group(1)
                    break
        if marker is None:
            return []

        content_cells: list[str] = []
        for idx, cell in enumerate(cells):
            if idx == marker_idx:
                cell = self._LEADING_MARKER_RE.sub("", cell).strip()
            if not cell or cell in ("★", "△", "▲"):
                continue
            if idx == 0 and re.fullmatch(r"\d{1,3}[.、．]?", cell):
                continue
            if idx < len(headers) and re.fullmatch(r"(?:序号|编号)", headers[idx].strip()):
                continue
            content_cells.append(cell)
        if not content_cells:
            return []
        label, body = content_cells[0], " ".join(content_cells[1:])
        if not body and self._is_star_heading(label):
            return []
        children = self._split_star_row_children(body) if body else [""]
        raw_text = " | ".join(cells)
        marker_type = self._marker_type(marker)
        entries: list[dict[str, Any]] = []
        for child in children:
            text = f"{label}：{child}" if child else label
            entries.append({
                "text": text,
                "raw_text": raw_text,
                "page": page,
                "bbox": bbox,
                "line_number": row_index + 1,
                "table_index": table_index,
                "row_index": row_index,
                "marker_offset": raw_text.find(marker),
                "marker_type": marker_type,
                "content_text": child or label,
                "chapter_title": chapter_title,
                "section_type": self._table_section_type(headers, text),
                "source_order": 0,
            })
        return entries

    def _multiple_markers_in_table_row(
        self,
        cells: list[str],
        headers: list[str],
        *,
        page: int,
        bbox: Any,
        chapter_title: str,
        table_index: int,
        row_index: int,
    ) -> list[dict[str, Any]] | None:
        """一个单元格连写多个带标记的编号条款时逐项创建候选。"""
        raw_row = " | ".join(cells)
        for cell_index, cell in enumerate(cells):
            header = headers[cell_index].strip() if cell_index < len(headers) else ""
            if any(token in header for token in ("备注", "注释", "补充说明")):
                continue
            markers = list(self._NUMBERED_MARKER_RE.finditer(cell))
            if len(markers) < 2:
                continue
            labels = []
            for preceding in cells[:cell_index]:
                label = self._LEADING_MARKER_RE.sub("", preceding).strip()
                if label and label not in ("★", "△", "▲") and not re.fullmatch(r"\d{1,3}[.、．]?", label):
                    labels.append(label)
            label = " ".join(labels)
            cell_offset = raw_row.find(cell)
            entries = []
            for idx, match in enumerate(markers):
                end = markers[idx + 1].start() if idx + 1 < len(markers) else len(cell)
                segment = cell[match.start():end].strip()
                content = segment[1:].strip()
                text = f"{label}：{content}" if label else content
                entries.append({
                    "text": text,
                    "raw_text": segment,
                    "content_text": content,
                    "page": page,
                    "bbox": bbox,
                    "line_number": row_index + 1,
                    "table_index": table_index,
                    "row_index": row_index,
                    "marker_offset": cell_offset + match.start(),
                    "marker_type": self._marker_type(match.group(1)),
                    "chapter_title": chapter_title,
                    "section_type": self._table_section_type(headers, text),
                    "source_order": 0,
                })
            return entries
        return None

    def _split_star_row_children(self, body: str) -> list[str]:
        matches = list(self._NUMBERED_CHILD_RE.finditer(body))
        if len(matches) < 2:
            return [body]
        prefix = body[:matches[0].start()].strip(" ：:")
        parts = []
        for index, match in enumerate(matches):
            end = matches[index + 1].start() if index + 1 < len(matches) else len(body)
            child = body[match.start():end].strip()
            if child:
                parts.append(f"{prefix} {child}".strip() if prefix else child)
        return parts or [body]

    @staticmethod
    def _table_section_type(headers: list[str], text: str) -> str:
        context = " ".join(headers) + " " + text
        if any(token in context for token in ("技术", "指标", "参数", "性能", "配置", "功能", "CPU", "内存")):
            return "technical"
        if any(token in context for token in ("商务", "合同", "付款", "交付", "工期", "资质", "资格")):
            return "business"
        return "unknown"

    def _iter_star_requirement_entries(
        self,
        lines: list[dict[str, Any]],
        *,
        start_idx: int,
        end_idx: int,
        chapter_title: str,
        table_regions: dict[int, list[dict[str, Any]]] | None = None,
    ) -> list[dict[str, Any]]:
        """只从表格外的明确标记行提取，避免跨行吞掉无关段落。"""
        entries: list[dict[str, Any]] = []
        heading: tuple[str, str, int | None] | None = None
        for idx in range(start_idx, end_idx + 1):
            item = lines[idx]
            line = str(item.get("text") or "").strip()
            if not line:
                continue
            if self._line_covered_by_table(item, table_regions or {}):
                continue
            match = self._LEADING_MARKER_RE.match(line)
            if match:
                marker = match.group(1)
                cleaned = self._clean_req(line)
                heading = (cleaned, marker, item.get("page")) if self._is_star_heading(cleaned) else None
                if heading is not None:
                    continue
                text = self._join_line_continuations(lines, idx, end_idx, table_regions or {})
                entries.append({
                    "text": text,
                    "raw_text": text,
                    "page": item.get("page"),
                    "line_number": item.get("line_number"),
                    "bbox": item.get("bbox"),
                    "marker_offset": text.find(marker),
                    "marker_type": self._marker_type(marker),
                    "section_type": self._infer_section(lines, idx),
                    "chapter_title": chapter_title,
                    "source_order": 1,
                })
                continue
            if heading is None:
                continue
            title, marker, heading_page = heading
            if item.get("page") != heading_page:
                heading = None
                continue
            if not re.match(r"^\s*(?:\d{1,2}[、.．)）]|[（(]\d{1,2}[)）])", line):
                if self._is_boundary(line):
                    heading = None
                continue
            if self._looks_like_numbered_heading(line):
                heading = None
                continue
            child = self._join_line_continuations(lines, idx, end_idx, table_regions or {})
            text = f"{title}：{child}"
            entries.append({
                "text": text,
                "raw_text": f"{marker}{text}",
                "content_text": child,
                "page": item.get("page"),
                "line_number": item.get("line_number"),
                "bbox": item.get("bbox"),
                "marker_offset": 0,
                "marker_type": self._marker_type(marker),
                "section_type": self._infer_section(lines, idx),
                "chapter_title": chapter_title,
                "source_order": 1,
            })
        return entries

    @staticmethod
    def _looks_like_numbered_heading(line: str) -> bool:
        body = re.sub(r"^\s*(?:\d{1,2}[、.．)）]|[（(]\d{1,2}[)）])\s*", "", line).strip()
        return (
            len(body) <= 12
            and not re.search(r"[。！？!?；;，,：:≥≤=]", body)
            and bool(re.search(r"(?:要求|保障|方案|标准|服务|建设|维护|管理|配置)$", body))
        )

    def _join_line_continuations(
        self,
        lines: list[dict[str, Any]],
        start_idx: int,
        end_idx: int,
        table_regions: dict[int, list[dict[str, Any]]],
    ) -> str:
        """只合并同页、紧邻且不以新条款开头的 OCR 断行。"""
        item = lines[start_idx]
        parts = [str(item.get("text") or "").strip()]
        for next_idx in range(start_idx + 1, min(start_idx + 7, end_idx + 1)):
            following = lines[next_idx]
            next_line = str(following.get("text") or "").strip()
            if (
                not next_line
                or following.get("page") != item.get("page")
                or self._line_covered_by_table(following, table_regions)
                or self._LEADING_MARKER_RE.match(next_line)
                or re.match(r"^\s*(?:\d{1,2}[、.．)）]|[（(]\d{1,2}[)）])", next_line)
                or self._is_boundary(next_line)
                or re.search(r"[。！？!?；;]\s*$", parts[-1])
            ):
                break
            parts.append(next_line)
        return " ".join(parts)

    def _line_covered_by_table(
        self,
        item: dict[str, Any],
        table_regions: dict[int, list[dict[str, Any]]],
    ) -> bool:
        page = item.get("page")
        normalized = self._norm(str(item.get("text") or ""))
        if not normalized:
            return False
        for region in table_regions.get(page, []):
            bbox = region.get("bbox")
            if self._bbox_overlap_fraction(item.get("bbox"), bbox) >= 0.8:
                return True
            if len(normalized) >= 8 and normalized in str(region.get("norm") or ""):
                return True
        return False

    def _split_numbered_segments(self, text: str) -> tuple[str, list[str]]:
        """将一行文本按条目编号（如 (1)、(2)）拆分为多个段落。"""
        raw = str(text or "").strip()
        if not raw:
            return "", []

        matches = list(self.ITEM_MARKER_RE.finditer(raw))
        if not matches:
            return raw, []

        prefix = raw[: matches[0].start()].strip()
        segments: list[str] = []
        for idx, match in enumerate(matches):
            start = match.start()
            end = matches[idx + 1].start() if idx + 1 < len(matches) else len(raw)
            segment = raw[start:end].strip()
            if segment:
                segments.append(segment)
        return prefix, segments

    def _can_append_requirement_line(
        self,
        current: dict[str, Any],
        line: str,
        page_no: int | None,
    ) -> bool:
        """判断当前行是否可以追加到正在构建的要求条目中。"""
        if self._is_boundary(line):
            return False
        merged = " ".join(str(part or "").strip() for part in current.get("parts", []) if str(part or "").strip())
        merged = re.sub(r"\s+", " ", merged).strip()
        if merged and re.search(r"[。！？!?]\s*$", merged):
            return False
        if current.get("page") != page_no and merged and re.search(r"[；;]\s*$", merged):
            return False
        return True

    def _infer_section(self, lines: list[dict[str, Any]], idx: int) -> str:
        """根据上下文推断当前要求的类型（商务或技术）。"""
        ctx = "\n".join(x["text"] for x in lines[max(0, idx - 6) : idx + 1])
        if any(k in ctx for k in ("技术", "参数", "指标", "性能", "配置", "功能")):
            return "technical"
        if any(k in ctx for k in ("商务", "合同", "付款", "交付", "工期", "资质", "资格")):
            return "business"
        return "unknown"

    def _has_star_marker(self, text: str) -> bool:
        """检查文本中是否包含 ★、△ 或 ▲ 标记（兼容旧命名）。"""
        return self._has_marker(text)

    def _has_marker(self, text: str) -> bool:
        """检查文本中是否包含任一标记（★ 必须项 / △、▲ 评分项）。"""
        return bool(self.MARKER_RE.search(text or ""))

    def _marker_kind(self, text: str) -> str | None:
        """返回文本中的标记类型；★ 优先，其次 △、▲。"""
        t = text or ""
        if self.STAR_RE.search(t):
            return "star"
        if self.TRIANGLE_RE.search(t):
            return "triangle"
        if self.IMPORTANT_RE.search(t):
            return "important"
        return None

    def _is_boundary(self, line: str) -> bool:
        """判断当前行是否为章节/标题等边界。"""
        c = re.sub(r"\s+", "", str(line or ""))
        if not c:
            return False
        if any(h in c for h in self.STOP_HINTS) and "偏离" not in c:
            return True
        return bool(re.match(r"^(第[一二三四五六七八九十百]+[章节部分]|[一二三四五六七八九十]+[、.．]|[0-9]{1,2}[、.．])", c) and len(c) <= 40)
