"""
??????????

??????????????????????????????????
???????????
"""

import re
from typing import List, Dict

from .template_extractor import TemplateExtractor 
from ..verification import VerificationChecker



class ConsistencyChecker:
    """一致性校验器：比对招标模型段落与投标文件段落的内容差异。"""

    # 正式标题行模式（如“附件1”、“第一章”）
    FORMAL_TITLE_LINE_RE = re.compile(
        r"^\s*(?:" r"(?:[（(]?\d+(?:\s*[-－]\s*\d+)?[)）\.、]?\s*)?(?:附件|附表)\s*\d+(?:\s*[-－]\s*\d+)*"
        r"|第[一二三四五六七八九十百0-9]+[章节部分]" r")"
    )
    # 非正文区块起始标记
    NON_BODY_BLOCK_MARKERS = (
        "与本项目有关的一切正式往来通讯请寄",
        "正式往来通讯请寄",
    )
    # 非正文行标记（如落款处的签名栏）
    NON_BODY_LINE_MARKERS = (
        "参选人法定代表人",
        "法定代表人或授权代表签字或盖章",
        "法定代表人签字或盖章",
        "授权委托人签字或盖章",
        "参选人名称",
        "供应商名称",
        "日期",
        "已签字",
    )
    # 注释/说明引导行
    NOTE_LEAD_RE = re.compile(r"^\s*(?:注|说明)\s*[:：]?\s*$")
    PLACEHOLDER_SPAN_RE = re.compile(
        r"_{2,}(?:[（(][^()（）\n]{0,40}[）)])?_{2,}"
        r"|_{2,}"
        r"|(?:…|\.|·){3,}"
        r"|[（(]\s*[)）]"
    )

    def __init__(self):
        self.NORM_PATTERN = re.compile(r'[\u4e00-\u9fa5a-zA-Z0-9]+')
        self.GAP_PATTERN = re.compile(r'[^\u4e00-\u9fa5a-zA-Z0-9]+')
        self._verification_checker = VerificationChecker(None)

    def _normalize(self, text: str) -> str:
        """提取文本中的中英文字母和数字，去除所有其他字符。"""
        if not text:
            return ""
        normalized = str(text)
        for token in ("underline", "underset", "cdot", "text"):
            normalized = normalized.replace(token, "")
        normalized = normalized.replace("需要说明的", "")
        return "".join(self.NORM_PATTERN.findall(normalized))

    def _normalize_title(self, text: str) -> str:
        """归一化标题：去括号、去噪声、去序号。"""
        if not text:
            return ""
        no_brackets = re.sub(r'\(.*?\)|（.*?）', '', text)
        clean = re.sub(r'[^\u4e00-\u9fa5A-Za-z0-9]|附件|附表|附录|格式', '', no_brackets)
        return re.sub(r'^[\d一二三四五六七八九十百]+', '', clean)

    def _is_formal_title_line(self, text: str) -> bool:
        """判断是否为正式的标题行（附件号或章节）。"""
        return bool(self.FORMAL_TITLE_LINE_RE.match(str(text or "").strip()))

    def _strip_title_line(self, text: str, title: str) -> str:
        """若文本首行与标题重复则移除首行。"""
        if not text:
            return ""
        lines = [line for line in text.splitlines() if line.strip()]
        if not lines:
            return ""

        first_line = lines[0].strip()
        if not self._is_formal_title_line(first_line):
            return "\n".join(lines).strip()

        first_norm = self._normalize_title(first_line)
        title_norm = self._normalize_title(title)
        if first_norm and title_norm and (first_norm in title_norm or title_norm in first_norm):
            lines = lines[1:]
        return "\n".join(lines).strip()

    def _is_non_body_line(self, normalized_line: str) -> bool:
        """检查归一化后的行是否是落款等非正文行。"""
        return any(marker in normalized_line for marker in self.NON_BODY_LINE_MARKERS)

    def _trim_non_body_lines(self, text: str) -> str:
        """移除正文中的通讯地址块和落款行。"""
        if not text:
            return ""

        kept: list[str] = []
        in_non_body_block = False

        for line in text.splitlines():
            stripped = line.strip()
            if not stripped:
                continue

            normalized_line = self._normalize(stripped)
            if not normalized_line:
                continue

            if any(marker in normalized_line for marker in self.NON_BODY_BLOCK_MARKERS):
                in_non_body_block = True
                continue

            if in_non_body_block:
                continue

            if self._is_non_body_line(normalized_line):
                continue

            kept.append(stripped)

        return "\n".join(kept)

    def _trim_instruction_note_block(self, text: str) -> str:
        """移除正文中的注释/说明块（以“注：”或“说明：”开头的内容）。"""
        if not text:
            return ""

        kept: list[str] = []
        in_note_block = False
        note_end_markers = self.NON_BODY_LINE_MARKERS + ("盖章", "签字", "参选人名称")

        for line in text.splitlines():
            stripped = line.strip()
            if not stripped:
                continue

            normalized_line = self._normalize(stripped)
            if not normalized_line:
                continue

            if self.NOTE_LEAD_RE.match(stripped):
                in_note_block = True
                continue

            if in_note_block:
                if (
                    self._is_formal_title_line(stripped)
                    or self._is_non_body_line(normalized_line)
                    or any(marker in normalized_line for marker in note_end_markers)
                ):
                    in_note_block = False
                else:
                    continue

            kept.append(stripped)

        return "\n".join(kept)

    def _build_attachment_lookup(self, test_json: dict, templates: List[Dict]) -> tuple[dict[str, dict], list[dict]]:
        """
        从投标文件 JSON 中提取所有附件（如附件1、附件2等），
        返回按附件号索引的字典和合并后的附件列表。
        """
        expected_attachments = []
        seen = set()
        for temp in templates:
            title = str(temp.get("title") or "").strip()
            attachment_number = self._verification_checker._attachment_number(title)
            normalized_title = self._verification_checker._attachment_title(title)
            key = attachment_number or normalized_title
            if not key or key in seen:
                continue
            seen.add(key)
            expected_attachments.append(
                {
                    "attachment_number": attachment_number,
                    "title": normalized_title,
                    "title_key": self._verification_checker._attachment_title_key(normalized_title),
                }
            )

        sections = self._verification_checker._attachment_sections(
            test_json, [], [], expected_attachments,
        )
        merged_sections: list[dict] = []
        by_number: dict[str, dict] = {}
        for item in sections:
            attachment_number = item.get("attachment_number")
            if not attachment_number:
                merged_sections.append(item)
                continue
            existing = by_number.get(attachment_number)
            if existing is None:
                copied = {
                    **item,
                    "pages": list(item.get("pages") or []),
                    "seal_texts": list(item.get("seal_texts") or []),
                    "signature_texts": list(item.get("signature_texts") or []),
                    "sections": list(item.get("sections") or []),
                    "seal_locations": list(item.get("seal_locations") or []),
                    "signature_locations": list(item.get("signature_locations") or []),
                }
                by_number[attachment_number] = copied
                merged_sections.append(copied)
                continue

            # 合并相同附件号下的内容
            existing["pages"] = list(dict.fromkeys((existing.get("pages") or []) + (item.get("pages") or [])))
            existing["seal_texts"] = list(dict.fromkeys((existing.get("seal_texts") or []) + (item.get("seal_texts") or [])))
            existing["signature_texts"] = list(dict.fromkeys((existing.get("signature_texts") or []) + (item.get("signature_texts") or [])))
            existing["sections"] = list(existing.get("sections") or []) + list(item.get("sections") or [])
            existing["seal_locations"] = self._verification_checker._dedupe_locations(
                list(existing.get("seal_locations") or []) + list(item.get("seal_locations") or [])
            )
            existing["signature_locations"] = self._verification_checker._dedupe_locations(
                list(existing.get("signature_locations") or []) + list(item.get("signature_locations") or [])
            )
            part = str(item.get("text") or "").strip()
            if part:
                prefix = str(existing.get("text") or "").strip()
                existing["text"] = f"{prefix}\n{part}".strip() if prefix else part
        return by_number, merged_sections

    def _serialize_section_locations(self, section: dict | None) -> List[Dict]:
        """将附件区段中的页面/位置信息序列化为列表。"""
        if not isinstance(section, dict):
            return []
        locations: List[Dict] = []
        for item in section.get("sections") or []:
            if not isinstance(item, dict):
                continue
            page = item.get("page") if isinstance(item.get("page"), int) else None
            bbox = item.get("bbox")
            normalized_bbox = None
            if isinstance(bbox, (list, tuple)) and len(bbox) >= 4 and all(isinstance(x, (int, float)) for x in bbox[:4]):
                normalized_bbox = [int(round(float(x))) for x in bbox[:4]]
            text = str(item.get("text") or "").strip()
            if page is None and normalized_bbox is None and not text:
                continue
            locations.append(
                {
                    "page": page,
                    "bbox": normalized_bbox,
                    "text": text[:120] if text else "",
                    "type": str(item.get("type") or "text"),
                }
            )
        return locations

    def _plain_text(self, text: str) -> str:
        value = str(text or "").replace("\u3000", " ").replace("\xa0", " ")
        value = value.replace("\r\n", "\n").replace("\r", "\n")
        value = re.sub(r"[ \t\f\v]+", " ", value)
        return value.strip()

    def _compact_text(self, text: str) -> str:
        return re.sub(r"\s+", "", self._plain_text(text))

    def _body_lines(self, text: str) -> List[str]:
        return [self._plain_text(line) for line in str(text or "").splitlines() if self._plain_text(line)]

    def _strip_placeholder_hints(self, line: str) -> str:
        stripped = self._plain_text(line)
        stripped = self.PLACEHOLDER_SPAN_RE.sub("", stripped)
        stripped = re.sub(r"[（(]\s*[)）]", "", stripped)
        stripped = re.sub(r"\s+", " ", stripped)
        return stripped.strip()

    def _looks_like_table_line(self, line: str) -> bool:
        return "|" in str(line or "") and str(line or "").count("|") >= 2

    def _is_effectively_filled_value(self, value: str, hint: str = "") -> bool:
        raw = self._plain_text(value)
        if not raw:
            return False
        normalized = raw
        for token in ("\\underline", "\\text", "underline", "text"):
            normalized = normalized.replace(token, "")
        normalized = re.sub(r"_{2,}|(?:…|\.|·){3,}", "", normalized)
        normalized = re.sub(r"[（()）\[\]{}<>$\\]", "", normalized)
        compact = self._normalize(normalized)
        if not compact:
            return False
        hint_key = self._normalize(hint)
        if hint_key and compact == hint_key:
            return False
        if compact in {"年月日", "年月", "月日", "日期", "签字", "盖章", "签字盖章"}:
            return False
        return True

    def _is_fillable_line(self, line: str) -> bool:
        text = self._plain_text(line)
        if not text or self._looks_like_table_line(text):
            return False
        compact = self._compact_text(text)
        if not compact or self.NOTE_LEAD_RE.match(text):
            return False
        if self.PLACEHOLDER_SPAN_RE.search(compact):
            return True
        if re.search(r"[：:]\s*$", text):
            return True
        if "：" in text or ":" in text:
            _, value = re.split(r"[:：]", text, maxsplit=1)
            return not self._is_effectively_filled_value(value)
        return False

    def _build_fill_spec(self, line: str) -> dict | None:
        text = self._plain_text(line)
        if not self._is_fillable_line(text):
            return None

        compact = self._compact_text(text)
        placeholder_matches = list(self.PLACEHOLDER_SPAN_RE.finditer(compact))
        anchor_text = self._strip_placeholder_hints(text) or text
        anchor_key = self._normalize(anchor_text)
        if any(
            marker in anchor_key
            for marker in (
                "总报价", "不含税总价", "含税总价", "人民币",
                "盖章", "签字", "签章", "比选响应单位", "参选人名称", "供应商名称",
                "法定代表人签字", "授权代表签字",
            )
        ):
            return None

        if placeholder_matches:
            # 长段落中的占位符更适合只做“固定正文”比对，不宜按逐空位强校验。
            if len(compact) > 80 or len(placeholder_matches) > 2:
                return None
            pattern_parts: list[str] = []
            placeholders: list[dict] = []
            cursor = 0
            for index, match in enumerate(placeholder_matches):
                pattern_parts.append(re.escape(compact[cursor:match.start()]))
                group_name = f"fill_{index}"
                pattern_parts.append(f"(?P<{group_name}>.*?)")
                raw_placeholder = match.group(0)
                hint = re.sub(r"_{2,}|[.…·（）()]", "", raw_placeholder)
                placeholders.append(
                    {
                        "name": group_name,
                        "hint": self._plain_text(hint),
                    }
                )
                cursor = match.end()
            pattern_parts.append(re.escape(compact[cursor:]))
            return {
                "kind": "placeholder",
                "template_line": text,
                "display_label": anchor_text,
                "anchor_text": anchor_text,
                "anchor_key": anchor_key,
                "pattern": re.compile("^" + "".join(pattern_parts) + "$"),
                "placeholders": placeholders,
            }

        bracket_hints = re.findall(r"[（(]([^()（）]{1,40})[）)]", text)
        if bracket_hints and re.search(r"[：:]\s*$", text):
            pattern_source = re.escape(compact)
            placeholders: list[dict] = []
            for index, hint in enumerate(bracket_hints):
                token = re.escape(self._compact_text(f"（{hint}）"))
                if token not in pattern_source:
                    token = re.escape(self._compact_text(f"({hint})"))
                group_name = f"fill_{index}"
                pattern_source = pattern_source.replace(token, f"(?P<{group_name}>.*?)", 1)
                placeholders.append({"name": group_name, "hint": hint})
            return {
                "kind": "placeholder",
                "template_line": text,
                "display_label": re.sub(r"[（(][^()（）]{1,40}[）)]", "", anchor_text).strip() or anchor_text,
                "anchor_text": re.sub(r"[（(][^()（）]{1,40}[）)]", "", anchor_text).strip() or anchor_text,
                "anchor_key": self._normalize(re.sub(r"[（(][^()（）]{1,40}[）)]", "", anchor_text)),
                "pattern": re.compile("^" + pattern_source + "$"),
                "placeholders": placeholders,
            }

        label, _ = re.split(r"[:：]", text, maxsplit=1)
        label = re.sub(r"[（(][^()（）]{0,40}[）)]", "", label)
        label = self._plain_text(label)
        normalized_label = self._normalize(label)
        if re.match(r"^(?:[（(]?[一二三四五六七八九十\d]+[)）]?)", label) and len(normalized_label) <= 8:
            return None
        if normalized_label in {"基本情况", "基本经济指标", "其他情况"}:
            return None
        return {
            "kind": "suffix",
            "template_line": text,
            "display_label": label or text,
            "anchor_text": anchor_text,
            "anchor_key": anchor_key,
            "label_key": self._normalize(label or text),
        }

    def _analyze_template_segment(self, title: str, text: str) -> dict:
        body = self._trim_instruction_note_block(
            self._trim_non_body_lines(self._strip_title_line(text, title))
        )
        fill_specs: list[dict] = []
        anchor_lines: list[str] = []
        for line in self._body_lines(body):
            fill_spec = self._build_fill_spec(line)
            if fill_spec is not None:
                fill_specs.append(fill_spec)
                anchor_lines.append(fill_spec.get("anchor_text") or line)
            else:
                anchor_lines.append(line)
        anchor_source = "\n".join(anchor_lines)
        return {
            "body": body,
            "anchor_source": anchor_source,
            "anchors": self._get_anchors(anchor_source),
            "fill_specs": fill_specs,
        }

    def _evaluate_placeholder_fill_spec(self, fill_spec: dict, bid_lines: List[str]) -> dict | None:
        best_match: dict | None = None
        for line in bid_lines:
            compact_line = self._compact_text(line)
            if not compact_line:
                continue
            match = fill_spec["pattern"].fullmatch(compact_line)
            if match is not None:
                missing_labels = []
                for index, placeholder in enumerate(fill_spec.get("placeholders") or [], start=1):
                    value = match.group(placeholder["name"])
                    if self._is_effectively_filled_value(value, placeholder.get("hint") or ""):
                        continue
                    missing_labels.append(placeholder.get("hint") or f"填写项{index}")
                candidate = {
                    "line": line,
                    "missing_labels": missing_labels,
                    "matched": True,
                }
                if best_match is None or len(missing_labels) < len(best_match.get("missing_labels") or []):
                    best_match = candidate
                if not missing_labels:
                    break
                continue

            anchor_key = str(fill_spec.get("anchor_key") or "").strip()
            if anchor_key and anchor_key in self._normalize(line) and best_match is None:
                best_match = {
                    "line": line,
                    "missing_labels": [],
                    "matched": False,
                }

        if best_match is None:
            return {
                "label": fill_spec.get("display_label") or fill_spec.get("template_line") or "填写项",
                "reason": "field_line_not_found",
                "template_line": fill_spec.get("template_line"),
            }
        if best_match["matched"] and not best_match["missing_labels"]:
            return None
        return {
            "label": fill_spec.get("display_label") or fill_spec.get("template_line") or "填写项",
            "reason": "field_value_missing" if best_match["matched"] else "field_line_not_stably_matched",
            "template_line": fill_spec.get("template_line"),
            "bid_line": best_match.get("line"),
            "missing_labels": best_match.get("missing_labels") or [],
        }

    def _evaluate_suffix_fill_spec(self, fill_spec: dict, bid_lines: List[str]) -> dict | None:
        label_key = str(fill_spec.get("label_key") or "").strip()
        for index, line in enumerate(bid_lines):
            normalized_line = self._normalize(line)
            if label_key and label_key not in normalized_line:
                continue

            value = ""
            if "：" in line or ":" in line:
                _, value = re.split(r"[:：]", line, maxsplit=1)
            if self._is_effectively_filled_value(value):
                return None

            next_line = bid_lines[index + 1] if index + 1 < len(bid_lines) else ""
            if next_line and not self._is_fillable_line(next_line) and self._is_effectively_filled_value(next_line):
                return None

            return {
                "label": fill_spec.get("display_label") or fill_spec.get("template_line") or "填写项",
                "reason": "field_value_missing",
                "template_line": fill_spec.get("template_line"),
                "bid_line": line,
            }

        return {
            "label": fill_spec.get("display_label") or fill_spec.get("template_line") or "填写项",
            "reason": "field_line_not_found",
            "template_line": fill_spec.get("template_line"),
        }

    def _evaluate_fill_specs(self, fill_specs: List[dict], bid_body: str) -> List[Dict]:
        if not fill_specs:
            return []
        bid_lines = self._body_lines(bid_body)
        unfilled_fields: List[Dict] = []
        for fill_spec in fill_specs:
            if fill_spec.get("kind") == "placeholder":
                result = self._evaluate_placeholder_fill_spec(fill_spec, bid_lines)
            else:
                result = self._evaluate_suffix_fill_spec(fill_spec, bid_lines)
            if result is not None:
                unfilled_fields.append(result)
        return unfilled_fields

    def _get_anchors(self, text: str) -> List[str]:
        """从文本中提取用于比对的锚点词（长度>=2的中英文词）。"""
        text = re.sub(r'\(.*?\)|（.*?）', ' ', text)
        text = text.replace('年月日', '年 月 日')
        parts = self.GAP_PATTERN.split(text)
        anchors = []
        for p in parts:
            norm = self._normalize(p)
            if '粘贴' in norm or ('签字' in norm and '盖章' in norm) or norm.isdigit(): 
                continue
            if len(norm) >= 2 or norm in ['年', '月', '日']: 
                anchors.append(norm)
        return anchors

    def compare_raw_data(self, model_json: dict, test_json: dict) -> List[Dict]:
        """
        主比对方法：将招标文件模板与投标文件段落进行比对，
        返回每个模板的通过状态及缺失锚点列表。
        """
        temps = TemplateExtractor.extract_consistency_templates(model_json)
        model_segments = [
            {
                "title": t["title"],
                "text": "\n".join(t.get("content") or []),
                "is_optional": bool(t.get("is_optional")),
            }
            for t in temps
        ]
        bid_by_no, bid_sections = self._build_attachment_lookup(test_json, model_segments)

        results = []
        for m_seg in model_segments:
            m_txt = m_seg["text"]
            title = m_seg["title"]
            is_optional = bool(m_seg.get("is_optional"))

            attachment_probe = {
                "attachment_number": self._verification_checker._attachment_number(title),
                "title": self._verification_checker._attachment_title(title),
            }
            matched_section = self._verification_checker._match_attachment(attachment_probe, bid_by_no, bid_sections)
            if matched_section is None:
                results.append(
                    {
                        "name": title,
                        "is_passed": bool(is_optional),
                        "missing_anchors": [],
                        "unfilled_fields": [],
                        "pages": [],
                        "locations": [],
                        "skip_reason": (
                            {"type": "optional_attachment_not_provided"}
                            if is_optional
                            else {
                                "type": "attachment_not_found",
                                "attachment_number": attachment_probe["attachment_number"],
                            }
                        ),
                    }
                )
                continue
            t_txt = matched_section.get("text") or ""

            matched_pages = list(matched_section.get("pages") or []) if isinstance(matched_section, dict) else []
            matched_locations = self._serialize_section_locations(matched_section)
            template_analysis = self._analyze_template_segment(title, m_txt)
            t_body = self._trim_instruction_note_block(
                self._trim_non_body_lines(self._strip_title_line(t_txt, title))
            )

            norm_t = self._normalize(t_body)
            anchors = template_analysis["anchors"]
            missing = [a for a in anchors if a not in norm_t]
            unfilled_fields = self._evaluate_fill_specs(template_analysis["fill_specs"], t_body)

            results.append(
                {
                    "name": title,
                    "is_passed": len(missing) == 0 and not unfilled_fields,
                    "missing_anchors": missing,
                    "unfilled_fields": unfilled_fields,
                    "fillable_field_count": len(template_analysis["fill_specs"]),
                    "pages": matched_pages,
                    "locations": matched_locations,
                }
            )
        return results
