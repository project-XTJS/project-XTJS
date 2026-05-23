"""
格式模板一致性检查模块。

对照招标文件中的商务标格式附件，检查投标文件中对应模板的固定正文
是否被删除或改动。
"""

import re
from typing import Any, List, Dict

from .template_extractor import TemplateExtractor 
from ..verification import VerificationChecker



class ConsistencyChecker:
    """一致性校验器：比对招标模型段落与投标文件段落的内容差异。"""

    # 正文过短的附件不参与一致性判断，避免只靠标题/零散字段误报
    MIN_BODY_LENGTH = 20
    INTEGRITY_ATTACHMENT_REF_RE = re.compile(r"附件\s*\d+(?:\s*[-－—]\s*\d+)?")

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
    NON_BODY_PREFIX_MARKERS = tuple(marker for marker in NON_BODY_LINE_MARKERS if marker not in {"日期", "已签字"})
    SHORT_NON_BODY_LABELS = ("日期", "已签字")
    # 注释/说明引导行
    NOTE_LEAD_RE = re.compile(r"^\s*(?:注|说明)\s*[:：]?\s*$")
    PLACEHOLDER_SPAN_RE = re.compile(
        r"_{2,}(?:[（(][^()（）\n]{0,40}[）)])?_{2,}"
        r"|_{2,}"
        r"|(?:…|\.|·){3,}"
        r"|[（(]\s*[)）]"
    )
    # 已填写的动态内容常出现在下划线、括号或 LaTeX underline 中，一致性比较时应剥离
    FILLED_UNDERLINE_RE = re.compile(r"\$?\s*\\underline\{\s*(?:\\text\{)?[^{}\n]{0,200}(?:\})?\s*\}\s*\$?")
    FILLED_UNDERLINE_CAPTURE_RE = re.compile(
        r"\$?\s*\\underline\{\s*(?:\\text\{)?(?P<content>[^{}\n]{0,200})(?:\})?\s*\}\s*\$?"
    )
    URL_TEXT_RE = re.compile(r"https?://|www\.", re.IGNORECASE)
    BRACKET_PLACEHOLDER_RE = re.compile(r"【[^】\n]{1,80}】|\[[^\]\n]{1,80}\]|「[^」\n]{1,80}」")
    FILLED_BLANK_SPAN_RE = re.compile(r"_{2,}\s*[^_\n]{0,120}?\s*_{2,}")
    SHORT_PAREN_RE = re.compile(r"[（(][^()（）\n]{0,80}[）)]")
    FOOTER_PAGE_RE = re.compile(r"^\s*(?:第\s*\d+\s*页(?:\s*共\s*\d+\s*页)?|\d+\s*/\s*\d+|\d+)\s*$")
    FIELD_LABEL_PREFIXES = ("地址", "邮政编码", "电话号码", "传真号码", "电子邮件", "电子邮箱", "电话", "传真")
    AMOUNT_LINE_MARKERS = ("总报价为", "不含税总价", "含税总价", "人民币")
    UNDERLINE_PRESERVE_MARKERS = (
        "副本",
        "电子文件",
        "u盘",
        "授权代表",
        "宣布如下",
        "投标文件所在页",
        "偏离说明",
        "劳动合同",
        "退休人员",
    )
    TEMPLATE_PLACEHOLDER_HINT_MARKERS = (
        "招标人名称",
        "采购人名称",
        "项目名称",
        "项目编号",
        "包件名称",
        "标段名称",
        "投标人名称",
        "供应商名称",
        "公司名称",
        "单位名称",
        "法定代表人姓名",
        "法定代表人姓名职务",
        "被授权人的姓名",
        "被授权人的姓名职务",
        "被授权人姓名",
        "被授权人姓名职务",
        "公司地址",
    )
    DYNAMIC_BRACKET_PLACEHOLDER_MARKERS = TEMPLATE_PLACEHOLDER_HINT_MARKERS + (
        "名称",
        "姓名",
        "职务",
        "地址",
        "编号",
    )
    NON_COMPARABLE_SLOT_MARKERS = (
        "单位公章",
        "盖章",
        "签章",
        "签字",
        "身份证号码",
        "手机",
    )
    ANCHOR_EQUIVALENT_REPLACEMENTS = (
        ("招标公告", "投标邀请书"),
        ("投标邀请书", "招标公告"),
    )
    # 表格型附件只校验表头和固定说明，不把数据行数值带入一致性比较。
    TABLE_HEADER_GROUPS = (
        (
            ("报价项", ("报价项",)),
            ("产品", ("产品/设备名称", "产品名称", "产品",)),
            ("设备名称", ("产品/设备名称", "设备名称",)),
            ("不含税总价", ("不含税总价",)),
            ("增值税税率", ("增值税税率",)),
            ("含税总价", ("含税总价",)),
            ("交货进度", ("交货进度",)),
            ("备注", ("备注",)),
        ),
        (
            ("设备名称", ("设备名称",)),
            ("数量", ("数量",)),
            ("单位", ("单位",)),
            ("增值税税率", ("增值税税率",)),
            ("含税单价", ("含税单价",)),
            ("含税总价", ("含税总价",)),
            ("备注", ("备注",)),
        ),
        (
            ("采购文件商务条款", ("采购文件商务条款", "采购文件的商务条款", "采购文件商务",)),
            ("响应文件的商务条款", ("响应文件的商务条款", "响应文件商务条款", "响应文件的商务",)),
            ("偏离", ("偏离",)),
            ("说明", ("说明",)),
        ),
        (
            ("序号", ("序号",)),
            ("年份", ("年份",)),
            ("项目名称", ("项目名称",)),
            ("合同金额", ("合同金额",)),
            ("委托内容", ("委托内容",)),
            ("委托单位", ("委托单位",)),
            (
                "所附证明材料在本响应文件的所在页码",
                (
                    "所附证明材料在本响应文件的所在页码",
                    "所附证明材料在本投标文件的所在页码",
                    "所附证明材料在本比选文件的所在页码",
                    "所附证明材料在本响应文件",
                    "所附证明材料在本投标文件",
                    "所附证明材料在本比选文件",
                    "所在页码",
                ),
            ),
        ),
    )
    INSTRUCTIONAL_LINE_MARKERS = (
        "我方同意根据采购人进一步要求出示有关资料予以证实",
        "如为联合体",
        "此附件联合体各方均应提供",
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

    def _normalize_match_text(self, text: str) -> str:
        """归一化文本用于跨模块标题匹配。"""
        return "".join(ch for ch in str(text or "") if ch.isalnum() or "\u4e00" <= ch <= "\u9fff")

    def _normalize_attachment_number(self, number: Any) -> str:
        """统一附件号写法，避免空格和全角横线造成不匹配。"""
        text = str(number or "").strip()
        text = text.replace("－", "-").replace("—", "-").replace("–", "-")
        return re.sub(r"\s+", "", text)

    def _attachment_numbers_from_text(self, text: str) -> set[str]:
        """从标题/完整性条目中提取附件号。"""
        numbers: set[str] = set()
        direct_number = self._verification_checker._attachment_number(str(text or ""))
        normalized_direct = self._normalize_attachment_number(direct_number)
        if normalized_direct:
            numbers.add(normalized_direct)

        for attachment_ref in self.INTEGRITY_ATTACHMENT_REF_RE.findall(str(text or "")):
            ref_number = self._verification_checker._attachment_number(attachment_ref)
            normalized_ref = self._normalize_attachment_number(ref_number)
            if normalized_ref:
                numbers.add(normalized_ref)
        return numbers

    def _simplify_integrity_item_title(self, item_name: str) -> str:
        """简化完整性条目标题，去除编号和括号内容。"""
        text = str(item_name or "").strip()
        text = re.sub(r"^\s*(?:\d+|[A-Z]|[一二三四五六七八九十百]+)[.、]\s*", "", text)
        text = re.sub(r"（.*?）|\(.*?\)", "", text).strip()
        if not text or len(text) > 36:
            return ""
        if any(sep in text for sep in ("；", ";", "，", ",")):
            return ""
        return text

    def _integrity_title_tokens(self, item_name: str) -> set[str]:
        """提取完整性缺失项可用于匹配一致性模板标题的标题 token。"""
        tokens: set[str] = set()
        simplified_title = self._simplify_integrity_item_title(item_name)
        if simplified_title:
            tokens.add(self._normalize_match_text(simplified_title))

        attachment_title = self._verification_checker._attachment_title(str(item_name or ""))
        attachment_title = re.sub(r"（.*?）|\(.*?\)", "", attachment_title).strip()
        normalized_attachment_title = self._normalize_match_text(attachment_title)
        if 4 <= len(normalized_attachment_title) <= 80:
            tokens.add(normalized_attachment_title)

        return {token for token in tokens if token}

    def _integrity_skip_reason_for_title(
        self, title: str, integrity_raw: dict | None
    ) -> dict | None:
        """若完整性已判定该模板附件缺失，则返回一致性前置跳过原因。"""
        if not isinstance(integrity_raw, dict):
            return None

        details = integrity_raw.get("details", {})
        if not isinstance(details, dict):
            return None

        segment_title = str(title or "")
        normalized_segment_title = self._normalize_match_text(segment_title)
        segment_numbers = self._attachment_numbers_from_text(segment_title)

        for item_name, detail in details.items():
            if not isinstance(detail, dict):
                continue
            if detail.get("is_passed") or not detail.get("scored", True):
                continue

            item_numbers = self._attachment_numbers_from_text(str(item_name or ""))
            matched_number = sorted(segment_numbers & item_numbers)
            if matched_number:
                return {
                    "type": "integrity_attachment_missing",
                    "integrity_item": item_name,
                    "integrity_status": detail.get("status"),
                    "matched_attachment_number": matched_number[0],
                }

            if segment_numbers and item_numbers:
                continue

            matched_tokens = [
                token
                for token in self._integrity_title_tokens(str(item_name or ""))
                if token and token in normalized_segment_title
            ]
            if matched_tokens:
                return {
                    "type": "integrity_attachment_missing",
                    "integrity_item": item_name,
                    "integrity_status": detail.get("status"),
                    "matched_tokens": sorted(matched_tokens),
                }

        return None

    def _integrity_pass_reason_for_title(
        self, title: str, integrity_raw: dict | None
    ) -> dict | None:
        """若完整性已确认该模板附件存在，则返回可用于跳过格式自拟正文检查的依据。"""
        if not isinstance(integrity_raw, dict):
            return None

        details = integrity_raw.get("details", {})
        if not isinstance(details, dict):
            return None

        segment_title = str(title or "")
        normalized_segment_title = self._normalize_match_text(segment_title)
        segment_numbers = self._attachment_numbers_from_text(segment_title)

        for item_name, detail in details.items():
            if not isinstance(detail, dict):
                continue
            if not detail.get("is_passed") or not detail.get("scored", True):
                continue

            item_numbers = self._attachment_numbers_from_text(str(item_name or ""))
            matched_number = sorted(segment_numbers & item_numbers)
            if matched_number:
                return {
                    "integrity_item": item_name,
                    "integrity_status": detail.get("status"),
                    "matched_attachment_number": matched_number[0],
                }

            if segment_numbers and item_numbers:
                continue

            matched_tokens = [
                token
                for token in self._integrity_title_tokens(str(item_name or ""))
                if token and token in normalized_segment_title
            ]
            if matched_tokens:
                return {
                    "integrity_item": item_name,
                    "integrity_status": detail.get("status"),
                    "matched_tokens": sorted(matched_tokens),
                }

        return None

    def _is_self_defined_format_template(self, title: str, text: str) -> bool:
        """格式自拟附件不做固定正文逐字一致性检查。"""
        return "格式自拟" in f"{title}\n{text}"

    def _self_defined_skip_reason(
        self, integrity_reason: dict | None = None, *, source: str = "attachment"
    ) -> dict:
        reason = {
            "type": "self_defined_format",
            "reason": "格式自拟附件不进行固定正文一致性检查",
            "source": source,
        }
        if integrity_reason:
            reason.update(integrity_reason)
        return reason

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

    def _is_non_body_line(self, text: str, normalized_line: str) -> bool:
        """检查归一化后的行是否是落款等非正文行。"""
        plain = self._plain_text(text)
        compact_plain = re.sub(r"\s+", "", plain)
        if not compact_plain:
            return False
        if any(compact_plain.startswith(marker) for marker in self.NON_BODY_PREFIX_MARKERS):
            return True
        for marker in self.SHORT_NON_BODY_LABELS:
            if compact_plain.startswith(marker) and len(compact_plain) <= 24:
                return True
        return compact_plain in self.NON_BODY_LINE_MARKERS or normalized_line in self.NON_BODY_LINE_MARKERS

    def _is_header_footer_line(self, text: str, normalized_line: str) -> bool:
        """过滤页眉页脚和纯页码。"""
        plain = self._plain_text(text)
        if not plain:
            return True
        if self.FOOTER_PAGE_RE.match(plain):
            return True
        if normalized_line.isdigit() and len(normalized_line) <= 4:
            return True
        if "招标编号" in plain and len(normalized_line) <= 32:
            return True
        return False

    def _trim_non_body_lines(self, text: str) -> str:
        """移除正文中的标题、页眉页脚、通讯地址块和落款行。"""
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

            if self._is_formal_title_line(stripped):
                continue
            if self._is_header_footer_line(stripped, normalized_line):
                continue

            if any(marker in normalized_line for marker in self.NON_BODY_BLOCK_MARKERS):
                in_non_body_block = True
                continue

            if in_non_body_block:
                continue

            if self._is_non_body_line(stripped, normalized_line):
                continue

            kept.append(stripped)

        return "\n".join(kept)

    def _table_header_projection(self, text: str) -> str:
        """从混合了表头和数据行的 OCR 文本里，只抽取稳定的表头字段。"""
        normalized_text = self._normalize(text)
        best_fields: list[str] = []
        for group in self.TABLE_HEADER_GROUPS:
            present_fields: list[str] = []
            for canonical, variants in group:
                normalized_variants = [self._normalize(item) for item in variants]
                if any(variant and variant in normalized_text for variant in normalized_variants):
                    present_fields.append(canonical)
            if len(present_fields) >= 3 and len(present_fields) > len(best_fields):
                best_fields = present_fields
        return " ".join(best_fields)

    def _should_preserve_underlined_text(self, content: str) -> bool:
        """仅在下划线内容明显属于固定句子骨架时保留，避免把纯填写值带入一致性比较。"""
        plain = self._plain_text(content)
        normalized = self._normalize(plain)
        if len(normalized) < 6:
            return False
        if self.URL_TEXT_RE.search(plain):
            return True
        if any(marker in normalized for marker in self.UNDERLINE_PRESERVE_MARKERS):
            return True
        if re.search(r"[，。；：、】【、]", plain) and len(normalized) >= 10:
            if re.fullmatch(r"[0-9零一二三四五六七八九十百千万年月日份元圆整]+", normalized):
                return False
            return True
        return False

    def _strip_or_preserve_filled_underlines(self, text: str) -> str:
        """保留被 OCR 包进 underline 的固定句，继续剥离纯填写值。"""

        def repl(match: re.Match[str]) -> str:
            content = match.group("content") or ""
            if self._should_preserve_underlined_text(content):
                return content
            return " "

        return self.FILLED_UNDERLINE_CAPTURE_RE.sub(repl, text)

    def _fixed_body_line(self, line: str) -> str:
        """提取一行中的固定正文，只保留不随填写内容变化的部分。"""
        text = self._plain_text(line)
        if not text:
            return ""
        if re.fullmatch(r"[a-z]{1,3}", self._compact_text(text)):
            return ""
        if self._is_non_comparable_slot_line(text):
            return ""
        table_header_projection = self._table_header_projection(text)
        if table_header_projection:
            return table_header_projection
        normalized_text = self._normalize(text)
        compact_text = self._compact_text(text)
        if "已签字" in compact_text and any(
            marker in compact_text for marker in ("法定代表人", "委托代理人", "项目经理", "投标人")
        ):
            return ""
        has_dynamic_marker = bool(
            self.FILLED_UNDERLINE_RE.search(text)
            or self.FILLED_BLANK_SPAN_RE.search(text)
            or self.PLACEHOLDER_SPAN_RE.search(compact_text)
        )
        # 招标模板里的说明性提示不是正文，避免与投标文件填写内容混在一起误判。
        if any(marker in normalized_text for marker in self.INSTRUCTIONAL_LINE_MARKERS):
            return ""
        # 报价金额句属于填写项，模板和投标文件的书写方式差异较大，不纳入固定正文一致性比较。
        if any(marker in normalized_text for marker in self.AMOUNT_LINE_MARKERS):
            if "总报价为" in normalized_text and (has_dynamic_marker or re.search(r"[¥￥]|\d[\d,，.]*", text)):
                return ""
        fixed_line = self._strip_or_preserve_filled_underlines(text)
        fixed_line = self.FILLED_BLANK_SPAN_RE.sub(" ", fixed_line)
        fixed_line = self.SHORT_PAREN_RE.sub(" ", fixed_line)
        normalized_fixed_candidate = self._normalize(self._strip_placeholder_hints(fixed_line))
        # 长填写骨架句先剥离填写值，再根据剩余固定正文判断；只有固定信息几乎为空时才跳过。
        if has_dynamic_marker and len(normalized_text) > 30 and len(normalized_fixed_candidate) < 10:
            return ""
        fill_spec = self._build_fill_spec(text)
        if fill_spec is not None:
            # 填写行不应整行删除，而是保留固定标签部分参与一致性比较。
            preserved_label = self._plain_text(
                fill_spec.get("display_label")
                or fill_spec.get("anchor_text")
                or fill_spec.get("template_line")
                or ""
            )
            if "：" in preserved_label or ":" in preserved_label:
                preserved_label = re.split(r"[:：]", preserved_label, maxsplit=1)[0]
            preserved_label = self._strip_placeholder_hints(preserved_label)
            preserved_label = re.sub(r"\s+", " ", preserved_label).strip("：:；;，,。 ")
            return preserved_label if len(self._normalize(preserved_label)) >= 2 else ""
        # 已填写的地址、邮箱等行只保留字段名，不把具体值带入一致性比较
        if "：" in fixed_line or ":" in fixed_line:
            label, _ = re.split(r"[:：]", fixed_line, maxsplit=1)
            plain_label = self._plain_text(label)
            if plain_label and any(marker in plain_label for marker in self.FIELD_LABEL_PREFIXES):
                fixed_line = plain_label
        fixed_line = self._strip_placeholder_hints(fixed_line)
        fixed_line = re.sub(r"\s+", " ", fixed_line).strip("：:；;，,。 ")
        return fixed_line if self._normalize(fixed_line) else ""

    def _build_fixed_body(self, text: str) -> str:
        """从正文中提取固定内容，填写项和落款等内容不参与一致性判断。"""
        fixed_lines: list[str] = []
        for line in self._body_lines(text):
            fixed_line = self._fixed_body_line(line)
            if fixed_line:
                fixed_lines.append(fixed_line)
        return "\n".join(fixed_lines)

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
                    or self._is_non_body_line(stripped, normalized_line)
                    or any(marker in normalized_line for marker in note_end_markers)
                ):
                    in_note_block = False
                else:
                    continue

            kept.append(stripped)

        return "\n".join(kept)

    def _build_attachment_lookup(self, test_json: dict, templates: List[Dict]) -> tuple[dict[str, list[dict]], list[dict]]:
        """
        从投标文件 JSON 中提取所有附件（如附件1、附件2等），
        返回按附件号索引的候选列表和原始附件列表。
        """
        expected_attachments = []
        seen = set()
        for temp in templates:
            title = str(temp.get("title") or "").strip()
            attachment_number = self._verification_checker._attachment_number(title)
            normalized_title = self._verification_checker._attachment_title(title)
            title_key = self._verification_checker._attachment_title_key(normalized_title)
            key = title_key or attachment_number or normalized_title
            if not key or key in seen:
                continue
            seen.add(key)
            expected_attachments.append(
                {
                    "attachment_number": attachment_number,
                    "title": normalized_title,
                    "title_key": title_key,
                }
            )

        sections = self._verification_checker._attachment_sections(
            test_json, [], [], expected_attachments,
        )
        by_number: dict[str, list[dict]] = {}
        for item in sections:
            attachment_number = item.get("attachment_number")
            if attachment_number:
                by_number.setdefault(attachment_number, []).append(item)
        return by_number, sections

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
        stripped = self._strip_named_placeholder_hints(stripped)
        stripped = self.PLACEHOLDER_SPAN_RE.sub("", stripped)
        stripped = re.sub(r"[（(]\s*[)）]", "", stripped)
        stripped = re.sub(r"\s+", " ", stripped)
        return stripped.strip()

    def _strip_named_placeholder_hints(self, line: str) -> str:
        """移除 【项目名称】 这类模板变量提示，避免把变量名当固定正文。"""

        def repl(match: re.Match[str]) -> str:
            content = match.group(0)
            normalized = self._normalize(content)
            if any(self._normalize(marker) in normalized for marker in self.TEMPLATE_PLACEHOLDER_HINT_MARKERS):
                return ""
            return content

        return self.BRACKET_PLACEHOLDER_RE.sub(repl, line)

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

    def _meaningful_fixed_parts(self, parts: List[str]) -> List[str]:
        """过滤掉过短或纯序号片段，只保留可用于槽位匹配的固定骨架。"""
        result: List[str] = []
        for part in parts:
            normalized = self._normalize(part)
            if not normalized or normalized.isdigit():
                continue
            if len(normalized) < 2 and normalized not in {"年", "月", "日"}:
                continue
            result.append(normalized)
        return result

    def _build_dynamic_slot_spec(self, line: str) -> dict | None:
        """根据招标模板中的下划线/空格槽位，生成动态句匹配规则。"""
        text = self._plain_text(line)
        if not text or self._looks_like_table_line(text):
            return None
        if self._is_non_comparable_slot_line(text):
            return None

        compact = self._compact_text(text)
        placeholder_matches = list(self.PLACEHOLDER_SPAN_RE.finditer(compact))
        placeholder_matches.extend(
            match
            for match in self.SHORT_PAREN_RE.finditer(compact)
            if self._is_dynamic_bracket_placeholder(match.group(0))
        )
        placeholder_matches = sorted(placeholder_matches, key=lambda item: item.start())
        if not placeholder_matches:
            return None

        pattern_parts: list[str] = []
        fixed_parts_raw: list[str] = []
        cursor = 0
        for match in placeholder_matches:
            fixed_fragment = compact[cursor:match.start()]
            if fixed_fragment:
                pattern_parts.append(re.escape(fixed_fragment))
                fixed_parts_raw.append(fixed_fragment)
            pattern_parts.append(r".{0,160}?")
            cursor = match.end()
        tail = compact[cursor:]
        if tail:
            pattern_parts.append(re.escape(tail))
            fixed_parts_raw.append(tail)

        fixed_parts = self._meaningful_fixed_parts(fixed_parts_raw)
        fallback_text = self._fixed_body_line(text)
        fallback_anchors = self._get_anchors(fallback_text)
        if not fixed_parts and not fallback_anchors:
            return None

        return {
            "template_line": text,
            "pattern": re.compile("".join(pattern_parts)),
            "fixed_parts": fixed_parts,
            "fallback_anchors": fallback_anchors,
        }

    def _is_dynamic_bracket_placeholder(self, text: str) -> bool:
        """判断括号内容是否是模板填写项，而不是“格式”等普通说明。"""
        content = str(text or "").strip("（）() ")
        if not content or len(content) > 40:
            return False
        normalized = self._normalize(content)
        if not normalized:
            return False
        if normalized in {"格式", "自拟", "如有", "说明", "盖章", "签字", "复印件", "原件"}:
            return False
        return any(self._normalize(marker) in normalized for marker in self.DYNAMIC_BRACKET_PLACEHOLDER_MARKERS)

    def _is_non_comparable_slot_line(self, line: str) -> bool:
        """签章、联系方式、纯日期等填空行由签章/日期模块处理，不参与模板一致性。"""
        compact = self._compact_text(line)
        if not compact:
            return False
        stripped = self._strip_placeholder_hints(line)
        normalized = self._normalize(stripped)
        if normalized and re.fullmatch(r"[年月日]+", normalized):
            return True
        return self.PLACEHOLDER_SPAN_RE.search(compact) is not None and any(
            marker in compact for marker in self.NON_COMPARABLE_SLOT_MARKERS
        )

    def _candidate_line_windows(self, bid_lines: List[str], max_window: int = 3) -> List[str]:
        """生成单行和相邻多行窗口，兼容 OCR 将一条模板句拆成多行的情况。"""
        windows: list[str] = []
        seen: set[str] = set()
        for size in range(1, max_window + 1):
            for start in range(0, len(bid_lines) - size + 1):
                candidate = "".join(bid_lines[start:start + size]).strip()
                if not candidate or candidate in seen:
                    continue
                seen.add(candidate)
                windows.append(candidate)
        return windows

    def _ordered_parts_present(self, fixed_parts: List[str], candidate_text: str) -> bool:
        """检查固定骨架是否按顺序出现在候选文本中。"""
        normalized_candidate = self._normalize(candidate_text)
        cursor = 0
        for part in fixed_parts:
            if not part:
                continue
            position = normalized_candidate.find(part, cursor)
            if position < 0:
                return False
            cursor = position + len(part)
        return True

    def _dynamic_slot_spec_matches(self, slot_spec: dict, bid_lines: List[str]) -> bool:
        """按模板槽位匹配投标句子，允许填写值替换下划线位置。"""
        fixed_parts = slot_spec.get("fixed_parts") or []
        fallback_anchors = self._meaningful_fixed_parts(slot_spec.get("fallback_anchors") or [])
        pattern = slot_spec.get("pattern")
        for candidate in self._candidate_line_windows(bid_lines):
            compact_candidate = self._compact_text(candidate)
            if not compact_candidate:
                continue
            if self._participation_declaration_slot_matches(slot_spec, candidate):
                return True
            if pattern is not None and pattern.search(compact_candidate):
                return True
            if fixed_parts and self._ordered_parts_present(fixed_parts, candidate):
                return True
            # 模板句就算下划线被投标文件删掉，只要固定骨架仍按顺序保留，也视为命中。
            if fallback_anchors and self._ordered_parts_present(fallback_anchors, candidate):
                return True
        return False

    def _participation_declaration_slot_matches(self, slot_spec: dict, candidate_text: str) -> bool:
        """识别“参与某项目/包件/招标编号”的已填写声明句。"""
        template_key = self._normalize(slot_spec.get("template_line") or "")
        if "作为投标人参与" not in template_key or "招标编号" not in template_key:
            return False

        candidate_key = self._normalize(candidate_text)
        if "作为投标人参与" not in candidate_key or "招标编号" not in candidate_key:
            return False
        if "投标" not in candidate_key:
            return False

        bid_number_match = re.search(r"招标编号[A-Za-z0-9]{3,}", candidate_key)
        package_match = re.search(r"包(?:件)?[A-Za-z0-9一二三四五六七八九十]+", candidate_key)
        if bid_number_match is None or package_match is None:
            return False

        participation_part = candidate_key.split("作为投标人参与", 1)[-1]
        participation_part = participation_part.split("的投标", 1)[0]
        project_part = participation_part.split("招标编号", 1)[0]
        project_part = re.sub(r"包(?:件)?[A-Za-z0-9一二三四五六七八九十]+", "", project_part)
        project_part = project_part.replace("项目", "").replace("包件号", "")
        return len(project_part) >= 4

    def _evaluate_dynamic_slot_specs(
        self, slot_specs: List[dict], bid_body: str, fixed_bid_body: str, title: str = ""
    ) -> List[str]:
        """先用模板槽位规则匹配，未命中时再回退到严格锚点比对。"""
        if not slot_specs:
            return []
        bid_lines = self._body_lines(bid_body)
        strict_bid_norm = self._normalize(fixed_bid_body)
        missing: List[str] = []
        for slot_spec in slot_specs:
            if self._dynamic_slot_spec_matches(slot_spec, bid_lines):
                continue
            fallback_anchors = slot_spec.get("fallback_anchors") or []
            slot_missing = [
                anchor
                for anchor in fallback_anchors
                if not self._anchor_present_in_bid(anchor, strict_bid_norm, title=title)
            ]
            missing.extend(slot_missing)
        return missing

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
        fixed_lines: List[str] = []
        dynamic_slot_specs: List[dict] = []
        for line in self._body_lines(body):
            slot_spec = self._build_dynamic_slot_spec(line)
            if slot_spec is not None:
                dynamic_slot_specs.append(slot_spec)
                continue
            fixed_line = self._fixed_body_line(line)
            if fixed_line:
                fixed_lines.append(fixed_line)
        fixed_body = "\n".join(fixed_lines)
        fixed_body_length = len(self._normalize(fixed_body))
        return {
            "body": body,
            "fixed_body": fixed_body,
            "fixed_body_length": fixed_body_length,
            "anchor_source": fixed_body,
            "anchors": self._get_anchors(fixed_body),
            "dynamic_slot_specs": dynamic_slot_specs,
            "fill_specs": [],
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

    def _anchor_variants(self, anchor: str) -> List[str]:
        variants = {anchor}
        for source, target in self.ANCHOR_EQUIVALENT_REPLACEMENTS:
            source_key = self._normalize(source)
            target_key = self._normalize(target)
            if source_key and target_key and source_key in anchor:
                variants.add(anchor.replace(source_key, target_key))
        return [variant for variant in variants if variant]

    def _manufacturer_either_or_anchor_present(
        self, title: str, anchor: str, normalized_bid_body: str
    ) -> bool:
        normalized_title = self._normalize(title)
        if "制造商声明函" not in normalized_title or "制造商授权书" not in normalized_title:
            return False
        declaration_markers = (
            self._normalize("我公司为本次项目所投产品制造商"),
            self._normalize("所投产品制造商"),
            self._normalize("制造商声明"),
        )
        authorization_markers = (
            self._normalize("授权投标人参与本项目"),
            self._normalize("授权投标人"),
            self._normalize("制造商授权"),
            self._normalize("授权书"),
        )
        anchor_is_declaration = any(marker in anchor for marker in declaration_markers)
        anchor_is_authorization = any(marker in anchor for marker in authorization_markers)
        has_declaration = any(marker in normalized_bid_body for marker in declaration_markers)
        has_authorization = any(marker in normalized_bid_body for marker in authorization_markers)
        return (anchor_is_authorization and has_declaration) or (
            anchor_is_declaration and has_authorization
        )

    def _anchor_present_in_bid(
        self, anchor: str, normalized_bid_body: str, *, title: str = ""
    ) -> bool:
        if not anchor:
            return True
        if anchor in normalized_bid_body:
            return True
        if any(variant in normalized_bid_body for variant in self._anchor_variants(anchor)):
            return True
        return self._manufacturer_either_or_anchor_present(
            title, anchor, normalized_bid_body
        )

    def compare_raw_data(
        self,
        model_json: dict,
        test_json: dict,
        integrity_raw: dict | None = None,
    ) -> List[Dict]:
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

        integrity_skip_reasons: dict[int, dict] = {}
        active_model_segments: List[Dict] = []
        for index, m_seg in enumerate(model_segments):
            skip_reason = self._integrity_skip_reason_for_title(m_seg["title"], integrity_raw)
            if skip_reason is not None:
                integrity_skip_reasons[index] = skip_reason
                continue
            active_model_segments.append(m_seg)

        bid_by_no: dict[str, list[dict]] = {}
        bid_sections: list[dict] = []
        if active_model_segments:
            bid_by_no, bid_sections = self._build_attachment_lookup(test_json, active_model_segments)

        results = []
        for index, m_seg in enumerate(model_segments):
            m_txt = m_seg["text"]
            title = m_seg["title"]
            is_optional = bool(m_seg.get("is_optional"))
            is_self_defined_format = self._is_self_defined_format_template(title, m_txt)
            self_defined_integrity_reason = (
                self._integrity_pass_reason_for_title(title, integrity_raw)
                if is_self_defined_format
                else None
            )

            integrity_skip_reason = integrity_skip_reasons.get(index)
            if integrity_skip_reason is not None:
                results.append(
                    {
                        "name": title,
                        "is_passed": True,
                        "missing_anchors": [],
                        "unfilled_fields": [],
                        "pages": [],
                        "locations": [],
                        "skip_reason": integrity_skip_reason,
                    }
                )
                continue

            attachment_probe = {
                "attachment_number": self._verification_checker._attachment_number(title),
                "title": self._verification_checker._attachment_title(title),
            }
            matched_section = self._verification_checker._match_attachment(attachment_probe, bid_by_no, bid_sections)
            if matched_section is None:
                if is_self_defined_format and self_defined_integrity_reason is not None:
                    results.append(
                        {
                            "name": title,
                            "is_passed": True,
                            "missing_anchors": [],
                            "unfilled_fields": [],
                            "pages": [],
                            "locations": [],
                            "skip_reason": self._self_defined_skip_reason(
                                self_defined_integrity_reason,
                                source="integrity",
                            ),
                        }
                    )
                    continue
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
            if is_self_defined_format:
                results.append(
                    {
                        "name": title,
                        "is_passed": True,
                        "missing_anchors": [],
                        "unfilled_fields": [],
                        "pages": matched_pages,
                        "locations": matched_locations,
                        "skip_reason": self._self_defined_skip_reason(
                            self_defined_integrity_reason,
                            source="attachment",
                        ),
                    }
                )
                continue
            template_analysis = self._analyze_template_segment(title, m_txt)
            dynamic_slot_specs = template_analysis.get("dynamic_slot_specs") or []
            if int(template_analysis.get("fixed_body_length") or 0) <= self.MIN_BODY_LENGTH and not dynamic_slot_specs:
                results.append(
                    {
                        "name": title,
                        "is_passed": True,
                        "missing_anchors": [],
                        "unfilled_fields": [],
                        "fillable_field_count": 0,
                        "template_body_length": int(template_analysis.get("fixed_body_length") or 0),
                        "pages": matched_pages,
                        "locations": matched_locations,
                        "skip_reason": {
                            "type": "body_too_short",
                            "body_length": int(template_analysis.get("fixed_body_length") or 0),
                            "min_body_length": self.MIN_BODY_LENGTH,
                        },
                    }
                )
                continue

            t_body = self._trim_instruction_note_block(
                self._trim_non_body_lines(self._strip_title_line(t_txt, title))
            )
            fixed_bid_body = self._build_fixed_body(t_body)

            norm_t = self._normalize(fixed_bid_body)
            anchors = template_analysis["anchors"]
            normalized_title = self._normalize(title)
            has_equivalent_final_quote = (
                any(marker in normalized_title for marker in ("开标一览表", "报价一览表"))
                and bool(re.search(r"[¥￥]|\d[\d,，.]*", t_body or fixed_bid_body or ""))
                and any(
                    marker in self._normalize(f"{fixed_bid_body}\n{t_body}")
                    for marker in ("最终报价总价元", "最终报价总价", "最终报价")
                )
            )
            missing = [
                a
                for a in anchors
                if not self._anchor_present_in_bid(a, norm_t, title=title)
                and not (has_equivalent_final_quote and a in {"投标总价", "报价总价", "总报价", "小写"})
            ]
            dynamic_missing = self._evaluate_dynamic_slot_specs(
                dynamic_slot_specs,
                t_body,
                fixed_bid_body,
                title=title,
            )
            if has_equivalent_final_quote and dynamic_missing:
                equivalent_total_fields = {
                    self._normalize(field)
                    for field in ("投标总价", "报价总价", "总报价", "小写", "投标总价（小写）", "报价总价（小写）")
                }
                dynamic_missing = [
                    field
                    for field in dynamic_missing
                    if self._normalize(field) not in equivalent_total_fields
                ]
            if dynamic_missing:
                combined_missing: List[str] = []
                seen_missing: set[str] = set()
                for anchor in missing + dynamic_missing:
                    normalized_anchor = self._normalize(anchor)
                    if not normalized_anchor or normalized_anchor in seen_missing:
                        continue
                    seen_missing.add(normalized_anchor)
                    combined_missing.append(anchor)
                missing = combined_missing

            results.append(
                {
                    "name": title,
                    "is_passed": len(missing) == 0,
                    "missing_anchors": missing,
                    "unfilled_fields": [],
                    "fillable_field_count": 0,
                    "template_body_length": int(template_analysis.get("fixed_body_length") or 0),
                    "bid_body_length": len(self._normalize(fixed_bid_body)),
                    "pages": matched_pages,
                    "locations": matched_locations,
                }
            )
        return results
