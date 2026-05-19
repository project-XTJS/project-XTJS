# -*- coding: utf-8 -*-
"""星标条款提取 Mixin"""
import re
from typing import Any


class StarExtractMixin:
    """负责从招标文件中提取 ★ 条款。"""

    # 依赖常量
    STAR_RE: re.Pattern
    ITEM_MARKER_RE: re.Pattern
    REQUIREMENT_CHAPTER_STRONG_HINTS: tuple
    REQUIREMENT_CHAPTER_WEAK_HINTS: tuple
    REQUIREMENT_CHAPTER_EXCLUDE_HINTS: tuple
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
        """仅在需求章节内扫描，抽取带 ★ 的强制性要求条目。"""
        lines = self._page_lines(tender_payload)
        scopes = self._chapter_scopes_for_star(lines)
        return self._collect_star_requirements_from_scopes(lines, scopes)

    def _collect_star_requirements_from_scopes(
        self,
        lines: list[dict[str, Any]],
        scopes: list[tuple[int, int, str]],
    ) -> list[dict[str, Any]]:
        """按章节范围收集星标条款，并做去重。"""
        out: list[dict[str, Any]] = []
        seen = set()
        for start_idx, end_idx, chapter_title in scopes:
            for entry in self._iter_star_requirement_entries(
                lines,
                start_idx=start_idx,
                end_idx=end_idx,
                chapter_title=chapter_title,
            ):
                req = self._clean_req(entry["text"])
                req_norm = self._norm(req)
                if len(req_norm) < 4 or req_norm in seen:
                    continue
                seen.add(req_norm)
                out.append(
                    {
                        "requirement_id": f"STAR-{len(out)+1:03d}",
                        "requirement": req,
                        "section_type": entry["section_type"],
                        "page": entry["page"],
                        "line_number": entry["line_number"],
                        "normalized_requirement": req_norm,
                        "fragments": self._fragments(req),
                        "chapter_title": entry["chapter_title"],
                    }
                )
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

    def _iter_star_requirement_entries(
        self,
        lines: list[dict[str, Any]],
        *,
        start_idx: int,
        end_idx: int,
        chapter_title: str,
    ) -> list[dict[str, Any]]:
        """在指定行范围内遍历并提取每一条带星标的要求条目。"""
        entries: list[dict[str, Any]] = []
        current: dict[str, Any] | None = None

        def flush_current() -> None:
            nonlocal current
            if not current:
                return
            merged = " ".join(str(part or "").strip() for part in current["parts"] if str(part or "").strip())
            merged = re.sub(r"\s+", " ", merged).strip()
            if merged and self._has_star_marker(merged):
                entries.append(
                    {
                        "text": merged,
                        "page": current["page"],
                        "line_number": current["line_number"],
                        "section_type": current["section_type"],
                        "chapter_title": current["chapter_title"],
                    }
                )
            current = None

        for idx in range(start_idx, end_idx + 1):
            item = lines[idx]
            line = str(item.get("text") or "").strip()
            if not line:
                continue

            prefix, segments = self._split_numbered_segments(line)
            if segments:
                if prefix and current is not None:
                    current["parts"].append(prefix)
                elif prefix and current is None and self._has_star_marker(prefix):
                    segments[0] = f"{prefix} {segments[0]}".strip()

                is_boundary_line = self._is_boundary(line)
                for segment in segments:
                    flush_current()
                    if is_boundary_line and not self._has_star_marker(segment):
                        continue
                    current = {
                        "page": item["page"],
                        "line_number": item["line_number"],
                        "section_type": self._infer_section(lines, idx),
                        "chapter_title": chapter_title,
                        "parts": [segment],
                    }
                continue

            if current is not None and self._can_append_requirement_line(current, line, item["page"]):
                current["parts"].append(line)
                continue

            flush_current()
            if self._has_star_marker(line):
                current = {
                    "page": item["page"],
                    "line_number": item["line_number"],
                    "section_type": self._infer_section(lines, idx),
                    "chapter_title": chapter_title,
                    "parts": [line],
                }

        flush_current()
        return entries

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
        """检查文本中是否包含星标符号。"""
        return bool(self.STAR_RE.search(text or ""))

    def _is_boundary(self, line: str) -> bool:
        """判断当前行是否为章节/标题等边界。"""
        c = re.sub(r"\s+", "", str(line or ""))
        if not c:
            return False
        if any(h in c for h in self.STOP_HINTS) and "偏离" not in c:
            return True
        return bool(re.match(r"^(第[一二三四五六七八九十百]+[章节部分]|[一二三四五六七八九十]+[、.．]|[0-9]{1,2}[、.．])", c) and len(c) <= 40)
