# -*- coding: utf-8 -*-
"""偏离表提取 Mixin"""
import re
from difflib import SequenceMatcher
from typing import Any


class DeviationTableMixin:
    """负责从投标文件中提取商务/技术偏离表及行数据。"""

    # 依赖常量
    BUSINESS_TITLES: tuple
    TECH_TITLES: tuple
    STOP_HINTS: tuple
    TABLE_ROW_MARKER_RE: re.Pattern
    NO_DEV_PATTERNS: tuple
    POS_DEV_PATTERNS: tuple
    NEG_DEV_PATTERNS: tuple

    # 依赖工具方法
    _norm: Any
    _clean_req: Any
    _fragments: Any
    _split_lines: Any
    _page_lines: Any
    _match_patterns: Any
    _clip: Any
    _has_star_marker: Any
    _normalize_markup_text: Any
    _merge_unique_parts: Any
    _section_text: Any
    _is_catalog_like_line: Any

    def _extract_bid_deviation_sections(self, bid_payload: dict) -> dict[str, Any]:
        """从投标文件中提取商务偏离表、技术偏离表等区段。"""
        line_items = self._page_lines(bid_payload)
        business = self._collect_sections(line_items, self.BUSINESS_TITLES)
        technical = self._collect_sections(line_items, self.TECH_TITLES)
        if not business and not technical:
            generic = self._collect_sections(line_items, ("偏离表",))
            for sec in generic:
                head = "\n".join((sec.get("lines") or [])[:3])
                if "技术" in head:
                    technical.append(sec)
                elif "商务" in head:
                    business.append(sec)
                else:
                    business.append(sec)
                    technical.append(sec)

        business = self._dedupe_sections(business)
        technical = self._dedupe_sections(technical)
        rows = self._extract_deviation_rows(bid_payload, business, technical)
        combined = "\n\n".join(x["text"] for x in business + technical if x.get("text"))
        return {"business": business, "technical": technical, "combined_text": combined, "rows": rows}

    def _collect_table_coverage(self, sections: dict[str, Any]) -> dict[str, dict[str, Any]]:
        """统计商务/技术偏离表的覆盖情况。"""
        coverage: dict[str, dict[str, Any]] = {}
        for group in ("business", "technical"):
            best = {
                "covered": False,
                "title": "",
                "row_count": 0,
                "response_row_count": 0,
                "sample": "",
            }
            best_score = (0, 0, 0)
            for sec in sections.get(group) or []:
                lines = sec.get("lines") or self._split_lines(sec.get("text", ""))
                row_lines = [ln for ln in lines if self._is_table_row_start(ln)]
                response_lines = [ln for ln in lines if self._looks_like_response_row(ln)]
                row_count = len(row_lines)
                response_row_count = len(response_lines)
                covered = bool(response_row_count > 0 or row_count >= 2)
                score = (1 if covered else 0, response_row_count, row_count)
                if score > best_score:
                    best_score = score
                    best = {
                        "covered": covered,
                        "title": str(sec.get("title") or ""),
                        "row_count": row_count,
                        "response_row_count": response_row_count,
                        "sample": str((response_lines or row_lines or [""])[0]).strip(),
                    }
            coverage[group] = best
        return coverage

    def _is_table_row_start(self, line: str) -> bool:
        """判断一行文本是否为表格行的起始（以数字开头）。"""
        return bool(re.match(r"^\s*\d{1,3}(?:\s*[.,)\u3001\uff0e\uff09]|\s+)", str(line or "")))

    def _looks_like_response_row(self, line: str) -> bool:
        """判断一行文本是否包含偏离响应信息。"""
        text = str(line or "")
        if not self._is_table_row_start(text):
            return False
        if re.search(r"\bP\d{1,3}(?:-P?\d{1,3})?\b", text, re.IGNORECASE):
            return True
        if "偏离" in text or "响应" in text:
            return True
        return False

    def _extract_deviation_rows(
        self,
        bid_payload: dict,
        business_sections: list[dict[str, Any]],
        technical_sections: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """从投标文件的逻辑表格和段落中提取所有可能的偏离响应行。"""
        rows: list[dict[str, Any]] = []
        doc = self._doc_container(bid_payload)
        section_hints = technical_sections + business_sections

        logical_tables = doc.get("logical_tables")
        if isinstance(logical_tables, list):
            for table in logical_tables:
                if isinstance(table, dict):
                    rows.extend(self._extract_rows_from_logical_table(table, section_hints=section_hints))

        for section in technical_sections:
            rows.extend(self._extract_rows_from_section(section, "technical"))
        for section in business_sections:
            rows.extend(self._extract_rows_from_section(section, "business"))

        out: list[dict[str, Any]] = []
        seen = set()
        for row in rows:
            joined_key = self._norm(row.get("joined_text", ""))[:260]
            if not joined_key or joined_key in seen:
                continue
            seen.add(joined_key)
            out.append(row)
        return out

    def _extract_rows_from_logical_table(
        self,
        table: dict[str, Any],
        *,
        section_hints: list[dict[str, Any]] | None = None,
    ) -> list[dict[str, Any]]:
        """解析一个逻辑表格，将其记录转换为偏离行结构。"""
        out: list[dict[str, Any]] = []
        headers = [str(x or "").strip() for x in (table.get("headers") or [])]
        pages = [x for x in (table.get("pages") or []) if isinstance(x, int)]
        page_no = pages[0] if pages else None
        if page_no is None:
            raw_page = table.get("page")
            try:
                page_no = int(raw_page) if raw_page is not None else None
            except (TypeError, ValueError):
                page_no = None
        title = self._resolve_logical_table_title(table, page_no=page_no, section_hints=section_hints)

        records = table.get("records")
        if isinstance(records, list):
            for record in records:
                if isinstance(record, dict):
                    row = self._build_row_from_record(record, headers=headers, page_no=page_no, title=title)
                    if row:
                        out.append(row)
        if out:
            return out

        rows = table.get("rows")
        if not isinstance(rows, list):
            native_headers, native_records = self._extract_native_table_records(table)
            for record in native_records:
                row = self._build_row_from_record(record, headers=native_headers, page_no=page_no, title=title)
                if row:
                    out.append(row)
            return out

        for values in rows:
            if not isinstance(values, list):
                continue
            record = {}
            for idx, value in enumerate(values):
                key = headers[idx] if idx < len(headers) else f"col_{idx + 1}"
                record[str(key)] = value
            row = self._build_row_from_record(record, headers=headers, page_no=page_no, title=title)
            if row:
                out.append(row)

        if out:
            return out

        native_headers, native_records = self._extract_native_table_records(table)
        for record in native_records:
            row = self._build_row_from_record(record, headers=native_headers, page_no=page_no, title=title)
            if row:
                out.append(row)
        return out

    def _extract_native_table_records(self, table: dict[str, Any]) -> tuple[list[str], list[dict[str, Any]]]:
        """从表格的 HTML 或文本内容中解析出表头和数据行。"""
        raw_html = table.get("block_content") or table.get("html") or table.get("text") or ""
        if not isinstance(raw_html, str) or not raw_html.strip():
            return [], []

        html_rows = self._parse_html_table_rows(raw_html)
        if not html_rows:
            return [], []

        header_like = self._looks_like_header_row(html_rows[0])
        headers = html_rows[0] if header_like else [f"col_{idx + 1}" for idx in range(len(html_rows[0]))]
        data_rows = html_rows[1:] if header_like else html_rows

        records: list[dict[str, Any]] = []
        for values in data_rows:
            if not isinstance(values, list):
                continue
            record = {
                headers[idx] if idx < len(headers) else f"col_{idx + 1}": str(value or "").strip()
                for idx, value in enumerate(values)
                if str(value or "").strip()
            }
            if record:
                records.append(record)

        return headers, records

    def _parse_html_table_rows(self, raw_html: str) -> list[list[str]]:
        """简易 HTML 表格解析，提取每行每列的文本。"""
        html = str(raw_html or "")
        if "<tr" not in html.lower() or ("<td" not in html.lower() and "<th" not in html.lower()):
            return []

        rows: list[list[str]] = []
        for row_html in re.findall(r"(?is)<tr\b[^>]*>(.*?)</tr>", html):
            cells: list[str] = []
            for cell_html in re.findall(r"(?is)<t[dh]\b[^>]*>(.*?)</t[dh]>", row_html):
                cell_text = self._normalize_markup_text(cell_html, preserve_lines=False)
                if cell_text:
                    cells.append(cell_text)
                else:
                    cells.append("")
            if any(cell.strip() for cell in cells):
                rows.append(cells)
        return rows

    def _looks_like_header_row(self, values: list[str]) -> bool:
        """判断一行文本是否为表头。"""
        if not isinstance(values, list) or not values:
            return False
        joined = "".join(str(value or "").strip() for value in values)
        return any(token in joined for token in ("需求", "要求", "条款", "响应", "应答", "偏离", "说明", "备注"))

    def _resolve_logical_table_title(
        self,
        table: dict[str, Any],
        *,
        page_no: int | None,
        section_hints: list[dict[str, Any]] | None = None,
    ) -> str:
        """通过周围区段的标题或表格自身属性推断表格标题。"""
        best_nearby_title = ""
        best_nearby_rank: tuple[int, int] | None = None
        for section in section_hints or []:
            if not isinstance(section, dict):
                continue
            title = str(section.get("title") or "").strip()
            if not title:
                continue
            section_page = section.get("page")
            if page_no is not None and section_page == page_no:
                return title
            if not isinstance(page_no, int) or not isinstance(section_page, int):
                continue
            distance = abs(section_page - page_no)
            if distance > 4:
                continue
            rank = (distance, 0 if section_page <= page_no else 1)
            if best_nearby_rank is None or rank < best_nearby_rank:
                best_nearby_rank = rank
                best_nearby_title = title

        if best_nearby_title:
            return best_nearby_title

        for candidate in (
            table.get("title"),
            table.get("caption"),
            table.get("name"),
            table.get("id"),
            table.get("block_id"),
            table.get("block_label"),
        ):
            title = str(candidate or "").strip()
            if title and not self._is_generic_table_title(title):
                return title

        return f"第{page_no}页表格" if page_no is not None else "logical_table"

    def _is_generic_table_title(self, title: str) -> bool:
        """检查是否为通用表格标题。"""
        compact = re.sub(r"\s+", "", str(title or ""))
        if not compact:
            return True
        if compact.lower() in {"table", "logical_table"}:
            return True
        return bool(re.fullmatch(r"(?:table_)?\d+", compact, re.IGNORECASE))

    def _build_row_from_record(
        self,
        record: dict[str, Any],
        *,
        headers: list[str],
        page_no: int | None,
        title: str,
    ) -> dict[str, Any] | None:
        """将表格中的一行记录转换为偏离分析所需的结构。"""
        ordered_keys = list(record.keys())
        if headers and all(str(h or "").strip() in record for h in headers):
            ordered_keys = [str(h or "").strip() for h in headers]

        requirement_parts: list[str] = []
        response_parts: list[str] = []
        deviation_parts: list[str] = []
        ordered_cells: list[tuple[str, str]] = []

        for key in ordered_keys:
            value = str(record.get(key) or "").strip()
            if not value:
                continue
            ordered_cells.append((str(key or "").strip(), value))
            role = self._column_role(key)
            if role == "requirement":
                requirement_parts.append(value)
            elif role == "response":
                response_parts.append(value)
            elif role == "deviation":
                deviation_parts.append(value)

        if not ordered_cells:
            return None

        if not requirement_parts:
            inferred = self._infer_generic_row_columns(ordered_cells)
            if inferred is None:
                return None
            requirement, response, deviation = inferred
        else:
            requirement = " ".join(requirement_parts).strip()
            response = " ".join(response_parts).strip()
            deviation = " ".join(deviation_parts).strip()

        joined_text = " ".join(part for part in (requirement, response, deviation) if part).strip()
        if len(self._norm(requirement or joined_text)) < 4:
            return None

        return {
            "group": self._guess_row_group(title, joined_text),
            "source": "logical_table",
            "page": page_no,
            "title": title,
            "requirement_text": requirement,
            "response_text": response,
            "deviation_text": deviation,
            "joined_text": joined_text,
            "requirement_norm": self._norm(requirement),
            "response_norm": self._norm(response),
            "deviation_norm": self._norm(deviation),
            "joined_norm": self._norm(joined_text),
        }

    def _infer_generic_row_columns(self, ordered_cells: list[tuple[str, str]]) -> tuple[str, str, str] | None:
        """无表头时通过内容特征推断需求/响应/偏离列。"""
        values = [value for _, value in ordered_cells if value]
        if len(values) < 2:
            return None

        dev_idx: int | None = None
        page_idx: int | None = None
        req_idx: int | None = None

        for idx, value in enumerate(values):
            if dev_idx is None and self._match_patterns(value, self.NO_DEV_PATTERNS + self.POS_DEV_PATTERNS + self.NEG_DEV_PATTERNS):
                dev_idx = idx
            if page_idx is None and re.search(r"\bP?\d{1,4}(?:\s*[-~]\s*P?\d{1,4})?\b", value, re.IGNORECASE):
                page_idx = idx
            if req_idx is None and ("★" in value or len(self._norm(value)) >= 10):
                req_idx = idx

        if req_idx is None:
            return None

        search_limit = dev_idx if dev_idx is not None else (page_idx if page_idx is not None else len(values))
        if req_idx >= search_limit:
            return None

        requirement = values[req_idx]
        response = ""
        for idx in range(req_idx + 1, search_limit):
            candidate = values[idx]
            if candidate and candidate != requirement:
                response = candidate
                break

        deviation = values[dev_idx] if dev_idx is not None else ""
        if not response and not deviation:
            return None
        return requirement, response, deviation

    def _column_role(self, label: str) -> str | None:
        """根据列标题推断其角色（需求/响应/偏离）。"""
        text = str(label or "").strip().lower()
        if not text:
            return None
        if any(token in text for token in ("招标文件的招标需求", "招标需求", "招标文件需求", "需求", "要求", "条款")):
            return "requirement"
        if any(token in text for token in ("投标文件的响应", "投标响应", "响应内容", "响应", "应答", "回复")):
            return "response"
        if any(token in text for token in ("偏离说明", "偏离", "说明", "备注")):
            return "deviation"
        return None

    def _extract_rows_from_section(self, section: dict[str, Any], group: str) -> list[dict[str, Any]]:
        """从纯文本区段中提取标记为★或含偏离响应的行。"""
        out: list[dict[str, Any]] = []
        title = str(section.get("title") or "")
        raw_text = re.sub(r"\s+", " ", str(section.get("text") or "")).strip()
        for segment in self._split_table_row_segments(raw_text):
            normalized = self._norm(segment)
            if len(normalized) < 8:
                continue
            has_response_marker = bool(
                "响应" in segment
                or self._match_patterns(segment, self.NO_DEV_PATTERNS + self.POS_DEV_PATTERNS + self.NEG_DEV_PATTERNS)
                or re.search(r"\bP\d{1,4}(?:\s*[-~]\s*P?\d{1,4})?\b", segment, re.IGNORECASE)
            )
            if not ("★" in segment or has_response_marker):
                continue
            out.append(
                {
                    "group": group,
                    "source": "section_text",
                    "page": section.get("page"),
                    "title": title,
                    "requirement_text": segment,
                    "response_text": segment if has_response_marker else "",
                    "deviation_text": segment if self._match_patterns(segment, self.NO_DEV_PATTERNS + self.POS_DEV_PATTERNS + self.NEG_DEV_PATTERNS) else "",
                    "joined_text": segment,
                    "requirement_norm": normalized,
                    "response_norm": self._norm(segment) if has_response_marker else "",
                    "deviation_norm": self._norm(segment) if self._match_patterns(segment, self.NO_DEV_PATTERNS + self.POS_DEV_PATTERNS + self.NEG_DEV_PATTERNS) else "",
                    "joined_norm": normalized,
                }
            )
        return out

    def _split_table_row_segments(self, text: str) -> list[str]:
        """将偏离表文本按条号分割为独立的段落。"""
        raw = re.sub(r"\s+", " ", str(text or "")).strip()
        if not raw:
            return []

        segments: list[str] = []
        _, numbered_segments = self._split_numbered_segments(raw)
        if numbered_segments:
            segments.extend(numbered_segments)

        if not segments:
            matches = list(self.TABLE_ROW_MARKER_RE.finditer(raw))
            if len(matches) >= 2:
                for idx, match in enumerate(matches):
                    start = match.start()
                    end = matches[idx + 1].start() if idx + 1 < len(matches) else len(raw)
                    segment = raw[start:end].strip()
                    if segment:
                        segments.append(segment)

        if not segments and ("★" in raw or "偏离" in raw or "响应" in raw):
            segments = [raw]

        merged_segments: list[str] = []
        idx = 0
        while idx < len(segments):
            segment = segments[idx]
            if re.match(r"^\s*\d{1,3}(?:[、.．)]?)\s*$", segment) and idx + 1 < len(segments):
                segment = f"{segment} {segments[idx + 1]}".strip()
                idx += 1
            if idx + 1 < len(segments):
                current_sub = re.search(r"[（(]\d{1,2}[)）]", segment)
                next_sub = re.match(r"^\s*[（(]\d{1,2}[)）]", segments[idx + 1])
                if (
                    current_sub
                    and next_sub
                    and current_sub.group(0).replace("(", "（").replace(")", "）")
                    == next_sub.group(0).strip().replace("(", "（").replace(")", "）")
                    and "响应" not in segment
                    and not self._match_patterns(segment, self.NO_DEV_PATTERNS + self.POS_DEV_PATTERNS + self.NEG_DEV_PATTERNS)
                    and not re.search(r"\bP\d{1,4}(?:\s*[-~]\s*P?\d{1,4})?\b", segment, re.IGNORECASE)
                ):
                    segment = f"{segment} {segments[idx + 1]}".strip()
                    idx += 1
            merged_segments.append(segment)
            idx += 1

        out: list[str] = []
        seen = set()
        for segment in merged_segments:
            key = self._norm(segment)[:220]
            if key and key not in seen:
                seen.add(key)
                out.append(segment)
        return out

    def _guess_row_group(self, title: str, text: str) -> str:
        """根据标题和内容猜测该偏离行属于商务组还是技术组。"""
        joined = f"{title}\n{text}"
        if any(token in joined for token in ("商务", "合同", "付款", "交货", "质保", "资质", "售后", "工期")):
            return "business"
        if any(token in joined for token in ("技术", "指标", "参数", "性能", "功能", "配置", "温度", "增益", "频率")):
            return "technical"
        return "unknown"

    def _collect_sections(
        self,
        line_items: list[dict[str, Any]],
        anchors: tuple[str, ...],
        window: int = 220,
    ) -> list[dict[str, Any]]:
        """在行列表中收集以给定 anchor 字符串开头的段落。"""
        out: list[dict[str, Any]] = []
        texts = [str(item.get("text") or "") for item in line_items]
        for i, item in enumerate(line_items):
            line = texts[i]
            anchor = next((x for x in anchors if x in line), None)
            if not anchor:
                continue
            if self._is_catalog_like_line(line):
                continue
            end = min(len(line_items), i + window)
            table_mode = any(
                "招标文件" in probe and "投标文件" in probe and ("响应" in probe or "偏离" in probe)
                for probe in texts[i : min(i + 12, len(texts))]
            )
            for c in range(i + 1, end):
                if c - i < 8:
                    continue
                now = texts[c]
                if any(t in now for t in self.BUSINESS_TITLES + self.TECH_TITLES):
                    end = c
                    break
                if table_mode:
                    now_compact = re.sub(r"\s+", "", now)
                    if any(h in now for h in self.STOP_HINTS) and "偏离" not in now and len(now_compact) <= 40:
                        end = c
                        break
                elif self._is_section_boundary(now):
                    end = c
                    break
            chunk_items = line_items[i:end]
            chunk = [str(chunk_item.get("text") or "") for chunk_item in chunk_items]
            text = "\n".join(chunk).strip()
            if len(self._norm(text)) >= 20:
                out.append(
                    {
                        "title": anchor,
                        "page": item.get("page"),
                        "start_line": item.get("line_number", i + 1),
                        "lines": chunk,
                        "line_items": chunk_items,
                        "text": text,
                    }
                )
        return out

    def _dedupe_sections(self, sections: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """移除内容重复的区段。"""
        out = []
        seen = set()
        for s in sections:
            key = self._norm(s.get("text", ""))[:240]
            if key and key not in seen:
                seen.add(key)
                out.append(s)
        return out

    def _detect_global_no_deviation(self, text: str) -> dict:
        """检测投标文件中是否有整体性的“无偏离”声明。"""
        pats = (
            r"(全部|所有).{0,8}(响应|满足).{0,18}(无偏离|未偏离|没有偏离)",
            r"(无偏离|未偏离).{0,18}(全部|所有).{0,8}(响应|满足)",
            r"完全响应.{0,20}(要求|条款).{0,20}(无偏离|未偏离|没有偏离)",
        )
        for p in pats:
            m = re.search(p, text or "", re.IGNORECASE | re.DOTALL)
            if m:
                return {"detected": True, "matched_text": self._clip(m.group(0), 120), "coverage_type": "global_no_deviation_statement"}
        return {"detected": False, "matched_text": "", "coverage_type": "none"}

    def _is_section_boundary(self, line: str) -> bool:
        """更严格的分节边界判断。"""
        c = re.sub(r"\s+", "", str(line or ""))
        if not c:
            return False
        if any(h in c for h in self.STOP_HINTS) and "偏离" not in c:
            return True
        return bool(re.match(r"^(第[一二三四五六七八九十百]+[章节部分]|[一二三四五六七八九十]+[、.．])", c) and len(c) <= 40)

    def _is_catalog_like_line(self, line: str) -> bool:
        """判断文本是否类似目录行。"""
        compact = re.sub(r"\s+", "", str(line or ""))
        if not compact:
            return False
        if "目录" in compact:
            return True
        if re.search(r"(?:\.{2,}|…{2,}|。{2,})\d{1,4}$", compact):
            return True
        return len(re.findall(r"(?:\.{2,}|…{2,}|。{2,})\d{1,4}", compact)) >= 2

    # 依赖 mixins/parse.py 中的 _doc_container
    _doc_container: Any
    # 依赖 mixins/star_extract.py 中的 _split_numbered_segments
    _split_numbered_segments: Any