# -*- coding: utf-8 -*-
"""文本清洗与归一化工具 Mixin"""
import re
from html import unescape
from typing import Any


class TextUtilsMixin:
    # 依赖常量
    STAR_RE: re.Pattern

    def _normalize_markup_text(self, value: Any, *, preserve_lines: bool) -> str:
        """清洗 HTML 标记，保留换行或纯文本。"""
        text = unescape(str(value or ""))
        if not text.strip():
            return ""

        text = re.sub(r"(?is)<img\b[^>]*alt=['\"]([^'\"]*)['\"][^>]*>", r" \1 ", text)
        text = re.sub(r"(?is)<img\b[^>]*>", " ", text)

        if preserve_lines:
            text = re.sub(r"(?i)<br\s*/?>", "\n", text)
            text = re.sub(r"(?i)</t[dh]>", "\t", text)
            text = re.sub(r"(?i)</tr>", "\n", text)
            text = re.sub(r"(?i)</?(table|thead|tbody|tfoot|tr|p|div|section|article)[^>]*>", "\n", text)
            text = re.sub(r"(?i)</?(td|th)[^>]*>", " ", text)
            text = re.sub(r"<[^>]+>", " ", text)
            text = text.replace("\r\n", "\n").replace("\r", "\n")
            text = re.sub(r"[^\S\n\t]+", " ", text)
            text = re.sub(r" *\t *", "\t", text)
            text = re.sub(r"[ \t]*\n[ \t]*", "\n", text)
            text = re.sub(r"\n{3,}", "\n\n", text)
            lines: list[str] = []
            for raw_line in text.splitlines():
                cells = [re.sub(r" {2,}", " ", cell).strip() for cell in raw_line.split("\t")]
                cleaned = "\t".join(cell for cell in cells if cell).strip()
                if cleaned:
                    lines.append(cleaned)
            return "\n".join(lines)

        text = re.sub(r"<[^>]+>", " ", text)
        text = re.sub(r"\s+", " ", text).strip()
        return text

    def _norm(self, text: str) -> str:
        """文本归一化：去星号、数学符号、标点、空白，转为小写。"""
        t = self.STAR_RE.sub("", str(text or ""))
        t = self._normalize_math_text(t)
        t = re.sub(r"[\s\u3000\xa0]+", "", t)
        t = t.replace("℃", "c").replace("°c", "c").replace("°C", "c").replace("°", "")
        t = t.replace("×", "x").replace("∗", "*")
        t = re.sub(r"[，,。；;：:！？!?（）()【】\[\]《》<>“”\"'‘’、\-_/\\]", "", t)
        return t.lower()

    def _normalize_math_text(self, text: str) -> str:
        """将 LaTeX 风格的数学符号转为普通字符。"""
        t = str(text or "")
        replacements = (
            ("\\leq", "≤"),
            ("\\geq", "≥"),
            ("\\pm", "±"),
            ("\\times", "×"),
            ("\\sim", "~"),
            ("\\cdot", "·"),
            ("\\mu", "μ"),
        )
        for source, target in replacements:
            t = t.replace(source, target)
        t = re.sub(r"\\mathrm\s*\{\s*c\s*\}", "℃", t, flags=re.IGNORECASE)
        t = re.sub(r"\^\s*\{\s*\\circ\s*\}", "°", t, flags=re.IGNORECASE)
        t = re.sub(r"\^\s*\{\s*([0-9]+)\s*\}", r"\1", t)
        t = re.sub(r"\\(?:text|mathrm|operatorname)\s*\{([^{}]*)\}", r"\1", t)
        t = re.sub(r"[$^{}]", "", t)
        return re.sub(r"\s+", " ", t).strip()

    def _clean_req(self, text: str) -> str:
        """清洗星标条款文本：去掉星号、数学标记、编号前缀。"""
        t = self.STAR_RE.sub("", str(text or ""))
        t = self._normalize_math_text(t)
        t = re.sub(
            r"^\s*(?:第[一二三四五六七八九十百]+[条章节项点]|[一二三四五六七八九十]+[、.．]|[0-9]+[、.．)]|[（(]\d{1,2}[)）])\s*",
            "",
            t,
        )
        return re.sub(r"\s+", " ", t).strip("，,；; ")

    def _split_lines(self, text: str) -> list[str]:
        """将文本按换行符（或句号等）分割成行。"""
        t = (
            str(text or "")
            .replace("\\r\\n", "\n")
            .replace("\\n", "\n")
            .replace("\\r", "\n")
            .replace("\r\n", "\n")
            .replace("\r", "\n")
            .replace("\u3000", " ")
            .replace("\xa0", " ")
        )
        if "\n" not in t:
            t = re.sub(r"([。；;！？!?])", r"\1\n", t)
        return [re.sub(r"[ \t\f\v]+", " ", x).strip() for x in t.split("\n") if x and x.strip()]

    def _clip(self, text: str, max_chars: int) -> str:
        """将文本截断到指定长度，并添加省略号。"""
        t = re.sub(r"\s+", " ", str(text or "").strip())
        return t if len(t) <= max_chars else f"{t[:max_chars].rstrip()}..."

    def _match_patterns(self, text: str, patterns: tuple[str, ...]) -> bool:
        """检查文本是否匹配给定的任一正则模式。"""
        return any(re.search(p, text or "", re.IGNORECASE) for p in patterns)

    def _fragments(self, text: str) -> list[str]:
        """将要求文本拆分为用于匹配的关键片段（长度>=4）。"""
        segs = re.split(r"[，,。；;：:\s（）()【】《》\"'‘’、\-]+", self._clean_req(text))
        vals = []
        for s in segs:
            n = self._norm(s)
            if len(n) >= 4:
                vals.append(n)
        if not vals:
            n = self._norm(text)
            if len(n) >= 4:
                vals = [n[: min(12, len(n))]] + ([n[-10:]] if len(n) > 14 else [])
        out, seen = [], set()
        for v in sorted(vals, key=len, reverse=True):
            if v not in seen:
                seen.add(v)
                out.append(v)
            if len(out) >= 6:
                break
        return out

    def _merge_unique_parts(self, parts: list[str], *, norm_cap: int = 240) -> list[str]:
        """合并并去重文本片段。"""
        merged: list[str] = []
        seen = set()
        for item in parts:
            text = str(item or "").strip()
            key = self._norm(text)[:norm_cap]
            if text and key and key not in seen:
                seen.add(key)
                merged.append(text)
        return merged