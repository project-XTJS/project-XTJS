# -*- coding: utf-8 -*-
"""
模板提取与文本分类模块。

负责从招标文件 OCR 结果中：
- 识别标题、目录行、附件标题等文本类型（SectionClassifier）
- 提取完整性检查所需的条款清单（extract_requirements）
- 提取一致性比对所需的模板基准（extract_consistency_templates）
"""

import re


class SectionClassifier:
    """文本类型识别器，提供标题识别和目录排除等功能。"""

    # 目录特征：连续点号、省略号、下划线后跟页码
    RE_TOC = re.compile(r'\.{3,}|…{2,}|_{3,}\s*\d+$')
    RE_TOC_TRAILING_PAGE = re.compile(r'\s\d+$')

    # 标题起始模式（附件、第X章、A.、一、等）
    RE_HEADING_START = re.compile(
        r'^\s*(?:'
        r'附件|附表|格式'
        r'|第[一二三四五六七八九十百零\d]+[章节部分篇项]'
        r'|[A-Z][、．\.]'
        r'|[一二三四五六七八九十百零]+[、．\.]'
        r'|[（(](?:[A-Z]|\d+|[一二三四五六七八九十百零]+)[）)]'
        r'|\d+[)）\.、]'
        r')'
    )
    # 特殊关键词标题（如“文件组成”、“商务文件”等）
    RE_KEYWORD_TITLE = re.compile(
        r'文件[的]?组成|商务文件|技术文件|部分格式附件|营业执照|招标文件|采购文件'
    )
    # 附件标题模式（可匹配“附件1”、“附表2-1”等）
    RE_ATTACHMENT_TITLE = re.compile(
        r'^\s*(?:[（(]?\d+(?:\s*[-－]\s*\d+)?[)）\.、]?\s*)?(?:附件|附表)\s*\d+(?:\s*[-－]\s*\d+)*'
    )
    # 附件标题关键词（用于判断附件标题的合法性）
    ATTACHMENT_TITLE_KEYWORDS = (
        "保证书",
        "报价表",
        "一览表",
        "偏离表",
        "情况表",
        "业绩清单",
        "清单",
        "证明书",
        "授权委托书",
        "授权书",
        "声明函",
        "承诺书",
        "营业执照",
        "资格证明文件",
    )
    # 附件正文标记（出现这些词的文本不太可能是纯粹的附件标题）
    ATTACHMENT_BODY_MARKERS = (
        "根据",
        "此表",
        "须与",
        "一致",
        "详见",
        "提交",
        "说明如下",
        "我方",
        "贵方",
        "响应文件",
    )
    # 含有这些符号的行，不视为目录噪声（避免误判带冒号或斜杠的正文行）
    NON_TOC_FIELD_MARKERS = ("：", ":", "/", "／")

    @classmethod
    def is_heading(cls, text: str, is_strict=False) -> bool:
        """判断文本是否为有效标题。

        Args:
            text: 待判断的文本。
            is_strict: 是否采用严格模式（必须是标准的标题起首）。

        Returns:
            True 如果认为是标题。
        """
        if cls.is_toc_noise(text):
            return False
        if is_strict:
            return bool(cls.RE_HEADING_START.search(text) or cls.RE_KEYWORD_TITLE.search(text))
        return bool(
            cls.RE_HEADING_START.search(text)
            or (cls.RE_KEYWORD_TITLE.search(text) and len(text) < 60)
        )

    @classmethod
    def is_attachment_heading_text(cls, text: str) -> bool:
        """判断文本是否为附件标题（如“附件1 投标保证书”）。"""
        if cls.is_toc_noise(text):
            return False
        raw_text = str(text or "").strip()
        if not raw_text or len(raw_text) > 80:
            return False
        if not cls.RE_ATTACHMENT_TITLE.search(raw_text):
            return False

        compact = re.sub(r"\s+", "", raw_text)
        # 包含正文标记的，不是纯标题
        if any(marker in compact for marker in cls.ATTACHMENT_BODY_MARKERS):
            return False
        if "。" in raw_text or "；" in raw_text or ";" in raw_text:
            return False

        # 剥离编号后，判断剩余主体部分是否像附件名称
        body = cls.RE_ATTACHMENT_TITLE.sub("", raw_text, count=1).strip("：:（）()、.． ")
        body = re.sub(r"[（(][^）)]{0,30}[）)]", "", body).strip()
        compact_body = re.sub(r"\s+", "", body)
        if not compact_body or len(compact_body) > 40:
            return False

        if any(keyword in compact_body for keyword in cls.ATTACHMENT_TITLE_KEYWORDS):
            return True
        return compact_body.endswith(("表", "书", "函", "照"))

    @classmethod
    def is_toc_noise(cls, text: str) -> bool:
        """判断文本是否为目录噪声（目录行或尾部带页码的行）。"""
        raw_text = str(text or "").strip()
        if not raw_text:
            return False

        if cls.RE_TOC.search(raw_text):
            return True

        # 仅对短行进行尾部页码判断，避免误伤带字段名和数值的正文
        if len(raw_text) > 40:
            return False
        if any(marker in raw_text for marker in cls.NON_TOC_FIELD_MARKERS):
            return False
        return bool(cls.RE_TOC_TRAILING_PAGE.search(raw_text))


class TemplateExtractor:
    """招标文件模板提取器，负责提取要求清单和一致性模板基准。"""

    # 移除 LaTeX 标签
    RE_TAGS = re.compile(r'\\[a-zA-Z]+|[{}$]')
    # 要求条款的切分正则（按编号拆分）
    REQUIREMENT_SPLIT_RE = re.compile(
        r'(?<![\dA-Z一二三四五六七八九十百零])'
        r'(?=(?:\d+[．\.、)]|[A-Z][．\.、)]|[一二三四五六七八九十百零]+[、．\.]|[（(](?:\d+|[A-Z]|[一二三四五六七八九十百零]+)[）)]))'
    )
    RESPONSE_FORMAT_ZONE_MARKERS = ("响应文件格式附件", "部分格式附件")
    RESPONSE_FORMAT_CHAPTER_MARKERS = ("响应文件格式",)
    RESPONSE_FORMAT_EMBEDDED_TITLE_MARKERS = (
        "法定代表人资格证明书",
        "法定代表人证明书",
        "法定代表人身份证明",
        "法定代表人授权委托书",
        "单位负责人证明书",
        "单位负责人身份证明",
        "授权委托书",
        "声明函",
        "承诺书",
        "保证书",
    )
    RESPONSE_FORMAT_COMPOSITE_TITLE_MARKERS = (
        "组成及部分格式",
        "文件组成及部分格式",
        "资格证明文件组成",
    )
    CHAPTER_HEADING_RE = re.compile(r'^\s*第[一二三四五六七八九十百零\d]+章')
    ATTACHMENT_NUMBER_RE = re.compile(
        r'^\s*(?:[（(]?\d+(?:\s*[-－]\s*\d+)?[)）\.、]?\s*)?(?:附件|附表)\s*(?P<number>\d+(?:\s*[-－]\s*\d+)*)'
    )

    @classmethod
    def _format_logical_table(cls, tb: dict, is_template: bool = False) -> str:
        """将逻辑表格格式化为文本。若为模板提取，则仅保留首行（表头）作为一致性锚点。

        Args:
            tb: 逻辑表格字典，可能含 rows、records、headers 等。
            is_template: 是否处于模板提取模式，仅提取表头。
        """
        lines = []
        rows = tb.get('rows', tb.get('body', tb.get('data', [])))

        if not rows and tb.get('headers'):
            lines.append(" | ".join(str(x) for x in tb['headers']))

        # 模板模式下只取首行（表头），避免将大量数据行写入模板
        if is_template and rows:
            rows = rows[:1]

        for row in rows:
            if isinstance(row, dict):
                lines.append(" | ".join(str(v) for v in row.values() if v is not None))
            elif isinstance(row, list):
                cell_texts = [
                    str(cell['text']) if isinstance(cell, dict) and 'text' in cell else str(cell)
                    for cell in row
                ]
                lines.append(" | ".join(cell_texts))
        return "\n".join(lines)

    @classmethod
    def preprocess_sections(
        cls, layout_sections: list, logical_tables: list = None, is_template: bool = False
    ) -> tuple:
        """统一预处理区段：清洗文本、提升标题类型、识别全局页眉。

        Args:
            layout_sections: OCR 产生的版面区段列表。
            logical_tables: 可选，逻辑表格列表。传入后将尝试将表格内容嵌入对应区段。
            is_template: 是否处于模板提取模式（影响表格行的选取）。

        Returns:
            (处理后的区段列表, 全局重复标题集合)
        """
        tables_by_page = {}
        if logical_tables:
            for tb in logical_tables:
                for p in tb.get('pages', []):
                    tables_by_page.setdefault(p, []).append(tb)

        consumed_tables = {}
        overall_consumed = set()

        processed = []
        page_has_heading = {}
        heading_counts = {}

        # 第一遍：清洗文本并识别原生标题
        for sec in layout_sections:
            if not isinstance(sec, dict) or 'text' not in sec:
                continue

            new_sec = sec.copy()
            page = new_sec.get('page', -1)

            if logical_tables and new_sec.get('type') == 'table':
                tbs = tables_by_page.get(page, [])
                idx = consumed_tables.get(page, 0)
                if idx < len(tbs):
                    tb = tbs[idx]
                    consumed_tables[page] = idx + 1
                    tb_id = tb.get('id', id(tb))
                    # 跨页表格避免重复提取
                    if tb_id not in overall_consumed:
                        overall_consumed.add(tb_id)
                        new_sec['text'] = cls._format_logical_table(tb, is_template)
                    else:
                        new_sec['text'] = ""
                else:
                    new_sec['text'] = cls.RE_TAGS.sub('', str(new_sec.get('text', ''))).strip()
            else:
                new_sec['text'] = cls.RE_TAGS.sub('', str(new_sec.get('text', ''))).strip()

            text = new_sec['text']
            if not text:
                continue

            # 如果该区段原本就是 heading 且符合严格标题定义，记录下来
            if new_sec.get('type') == 'heading' and SectionClassifier.is_heading(text, is_strict=True):
                page_has_heading[page] = True
                clean_h = text.replace(' ', '')
                heading_counts[clean_h] = heading_counts.get(clean_h, 0) + 1

            processed.append(new_sec)

        final_processed = []
        # 第二遍：提升文本类型（某些 text 段落实际是标题）
        for sec in processed:
            text = sec['text']
            page = sec.get('page')
            if sec.get('type') == 'text':
                should_promote = False
                if not page_has_heading.get(page, False) and SectionClassifier.is_heading(text):
                    should_promote = True
                elif SectionClassifier.is_attachment_heading_text(text):
                    should_promote = True

                if should_promote:
                    sec['type'] = 'heading'
                    page_has_heading[page] = True
                    clean_h = text.replace(' ', '')
                    heading_counts[clean_h] = heading_counts.get(clean_h, 0) + 1
            final_processed.append(sec)

        global_headers = {t for t, c in heading_counts.items() if c > 2}
        return final_processed, global_headers

    @classmethod
    def _is_noise(cls, text: str, headers: set, section_type: str | None = None) -> bool:
        """判断文本是否为噪声（目录行或重复出现的全局页眉）。"""
        clean = text.replace(' ', '')
        if clean in headers:
            return True
        if section_type == 'table':
            return False
        return SectionClassifier.is_toc_noise(text)

    @classmethod
    def _split_requirement_parts(cls, text: str) -> list[str]:
        """将文本按条款编号切分为多个部分。"""
        if not text:
            return []
        return [
            part.strip()
            for part in re.split(cls.REQUIREMENT_SPLIT_RE, text)
            if part and part.strip()
        ]

    @classmethod
    def _parse_requirement_item(cls, text: str) -> tuple[str | None, str]:
        """解析一个条款，返回 (序号, 内容)。"""
        raw = str(text or "").strip()
        if not raw:
            return None, ""

        patterns = (
            r'^(\d+)[．\.、)]\s*(.+)',
            r'^([A-Z])[．\.、)]\s*(.+)',
            r'^[（(](\d+|[A-Z]|[一二三四五六七八九十百零]+)[）)]\s*(.+)',
            r'^([一二三四五六七八九十百零]+)[、．\.]\s*(.+)',
        )
        for pattern in patterns:
            match = re.match(pattern, raw)
            if match:
                return match.group(1), match.group(2)
        return None, ""

    @classmethod
    def _looks_like_requirement_leaf(cls, text: str) -> bool:
        """判断一条内容是否像具体的材料要求（表、书、函等）。"""
        compact = re.sub(r'\s+', '', cls._clean_label(text or ""))
        if not compact:
            return False
        return any(
            marker in compact
            for marker in (
                "表", "书", "函", "清单", "凭证", "介绍", "执照", "证明",
                "委托", "授权", "承诺", "声明", "偏离", "合同",
            )
        )

    @classmethod
    def _compact(cls, text: str) -> str:
        return re.sub(r'\s+', '', str(text or ''))

    @classmethod
    def _attachment_number(cls, text: str) -> str | None:
        match = cls.ATTACHMENT_NUMBER_RE.search(str(text or ""))
        return re.sub(r"\s+", "", match.group("number")) if match else None

    @classmethod
    def _attachment_title(cls, text: str) -> str:
        value = str(text or "").strip()
        value = re.sub(r'^\s*第[一二三四五六七八九十百零\d]+[章节部分篇项]\s*', '', value)
        value = re.sub(r'^\s*(?:[（(]?\d+(?:\s*[-－]\s*\d+)?[）).、．]?|[一二三四五六七八九十百零]+[、.)）．])\s*', '', value)
        idx = value.find("附件")
        value = value[idx:] if idx >= 0 else value
        return re.sub(r'\s+', ' ', value).strip('：:；;，,。')

    @classmethod
    def _is_embedded_response_format_heading(cls, section: dict) -> bool:
        if str(section.get('type') or '').strip().lower() != 'heading':
            return False
        text = str(section.get('text') or '').strip()
        compact = cls._compact(text)
        if not compact or SectionClassifier.is_toc_noise(text):
            return False
        if "附件" in compact or len(compact) > 40:
            return False
        if "格式" not in compact:
            return False
        return any(marker in compact for marker in cls.RESPONSE_FORMAT_EMBEDDED_TITLE_MARKERS)

    @classmethod
    def _is_response_format_attachment_heading(cls, section: dict) -> bool:
        text = str(section.get('text') or '').strip()
        compact = cls._compact(text)
        attachment_number = cls._attachment_number(text)
        if attachment_number is not None:
            attachment_index = compact.find("附件")
            if attachment_index >= 0 and attachment_index <= 12:
                return True
        return (
            SectionClassifier.is_attachment_heading_text(text)
            or cls._is_embedded_response_format_heading(section)
        )

    @classmethod
    def _response_format_sections(cls, model_raw_json: dict) -> list[dict]:
        data_node = model_raw_json.get('data', model_raw_json)
        sections, _ = cls.preprocess_sections(
            data_node.get('layout_sections', []),
            data_node.get('logical_tables', []),
            is_template=True,
        )
        if not sections:
            return []

        start_index = None
        for idx, section in enumerate(sections):
            compact = cls._compact(section.get('text') or '')
            if any(marker in compact for marker in cls.RESPONSE_FORMAT_ZONE_MARKERS):
                start_index = idx
                break
        if start_index is None:
            for idx, section in enumerate(sections):
                compact = cls._compact(section.get('text') or '')
                if any(marker in compact for marker in cls.RESPONSE_FORMAT_CHAPTER_MARKERS):
                    start_index = idx
                    break
        if start_index is None:
            return []

        end_index = len(sections)
        for idx in range(start_index + 1, len(sections)):
            section = sections[idx]
            if str(section.get('type') or '').strip().lower() != 'heading':
                continue
            text = str(section.get('text') or '').strip()
            compact = cls._compact(text)
            if cls.CHAPTER_HEADING_RE.match(text) and not any(
                marker in compact for marker in cls.RESPONSE_FORMAT_CHAPTER_MARKERS
            ):
                end_index = idx
                break
        return sections[start_index:end_index]

    @classmethod
    def extract_response_format_attachments(cls, model_raw_json: dict) -> list[dict]:
        """从“响应文件格式/响应文件格式附件”区域提取附件标题与内容。"""
        sections = cls._response_format_sections(model_raw_json)
        if not sections:
            return []

        starts = [
            idx
            for idx, section in enumerate(sections)
            if cls._is_response_format_attachment_heading(section)
        ]
        attachments: list[dict] = []
        current_top_level_number: str | None = None
        for pos, start in enumerate(starts):
            end = starts[pos + 1] if pos + 1 < len(starts) else len(sections)
            chunk = sections[start:end]
            if not chunk:
                continue
            title = str(chunk[0].get('text') or '').strip()
            attachment_number = cls._attachment_number(title)
            if attachment_number is not None:
                current_top_level_number = attachment_number
            elif current_top_level_number is not None:
                attachment_number = current_top_level_number
            normalized_title = cls._attachment_title(title)
            compact_title = cls._compact(normalized_title)
            attachments.append(
                {
                    "attachment_number": attachment_number,
                    "title": normalized_title,
                    "content": [
                        str(section.get('text') or '').strip()
                        for section in chunk
                        if str(section.get('text') or '').strip()
                    ],
                    "is_composite": any(
                        marker in compact_title
                        for marker in cls.RESPONSE_FORMAT_COMPOSITE_TITLE_MARKERS
                    ),
                }
            )
        return attachments

    @classmethod
    def extract_requirements(cls, model_raw_json: dict) -> tuple:
        """提取完整性检查所需的条款清单，返回 (有序列表, 序号到附件编号映射)。

        Args:
            model_raw_json: 招标文件 OCR JSON（可能包含 data 包裹）。

        Returns:
            (ordered_list, attachment_mapping)
            - ordered_list: 格式为 "序号. 内容" 的字符串列表。
            - attachment_mapping: 序号 → 附件编号列表。
        """
        data_node = model_raw_json.get('data', model_raw_json)
        sections, headers = cls.preprocess_sections(data_node.get('layout_sections', []))

        ordered_list = []
        attachment_mapping = {}
        stage = 0

        for sec in sections:
            text = sec['text']
            if not text or cls._is_noise(text, headers, sec.get('type')):
                continue

            if sec['type'] == 'heading':
                if stage == 0 and '文件' in text and '组成' in text:
                    stage = 1
                    continue
                if stage == 1 and "商务" in text:
                    stage = 2
                    continue
                if stage == 2 and "技术" in text:
                    break

            if stage == 2:
                # 将条款文本按编号拆分，依次处理
                for part in cls._split_requirement_parts(text):
                    seq, content = cls._parse_requirement_item(part)
                    if not seq or not content:
                        continue
                    if not cls._looks_like_requirement_leaf(content):
                        continue
                    ordered_list.append(f"{seq}. {cls._clean_label(content)}")

                    # 提取“格式参见本章附件X”中的附件编号
                    attach_refs = re.findall(r'格式参见本章附件\s*([\d\-—，,、\s]+)', content)
                    attach_numbers = []
                    if attach_refs:
                        raw = attach_refs[0]
                        nums = re.split(r'[，,、\s]+', raw)
                        for num in nums:
                            num = num.strip().strip('—－-')
                            if num:
                                attach_numbers.append(num)
                    if attach_numbers:
                        attachment_mapping[seq] = attach_numbers

        response_attachments = cls.extract_response_format_attachments(model_raw_json)
        for attachment in response_attachments:
            title = str(attachment.get("title") or "").strip()
            if not title or attachment.get("is_composite"):
                continue
            if title not in ordered_list:
                ordered_list.append(title)
            attachment_number = str(attachment.get("attachment_number") or "").strip()
            if attachment_number:
                attachment_mapping[title] = [attachment_number]

        return ordered_list, attachment_mapping

    @classmethod
    def extract_consistency_templates(cls, model_raw_json: dict) -> list:
        """提取一致性比对所需的模板基准，来源与完整性检查保持一致。"""
        requirements, attachment_mapping = cls.extract_requirements(model_raw_json)
        response_attachments = cls.extract_response_format_attachments(model_raw_json)
        if not response_attachments:
            return []

        attachments_by_title: dict[str, dict] = {}
        attachments_by_number: dict[str, dict] = {}
        for attachment in response_attachments:
            title = str(attachment.get("title") or "").strip()
            if not title or attachment.get("is_composite"):
                continue
            compact_title = cls._compact(title)
            if compact_title and compact_title not in attachments_by_title:
                attachments_by_title[compact_title] = attachment
            attachment_number = str(attachment.get("attachment_number") or "").strip()
            if attachment_number and attachment_number not in attachments_by_number:
                attachments_by_number[attachment_number] = attachment

        templates: list[dict] = []
        seen_titles: set[str] = set()
        for item in requirements:
            item_title = str(item or "").strip()
            if not item_title:
                continue

            attachment = attachments_by_title.get(cls._compact(item_title))
            if attachment is None:
                for ref in attachment_mapping.get(item_title) or []:
                    attachment = attachments_by_number.get(str(ref).strip())
                    if attachment is not None:
                        break
            if attachment is None:
                continue

            normalized_title = str(attachment.get("title") or item_title).strip()
            compact_title = cls._compact(normalized_title)
            if not compact_title or compact_title in seen_titles:
                continue
            seen_titles.add(compact_title)

            templates.append(
                {
                    "title": normalized_title,
                    "content": list(attachment.get("content") or []),
                    "is_optional": "如有" in normalized_title,
                }
            )

        return templates

    @staticmethod
    def _clean_label(text: str) -> str:
        """清理标签文本：去掉分号、句号结尾，去掉格式参见说明，去掉末尾页码。"""
        return re.sub(
            r'[；;:：。]$|[(（].*?格式.*?参见.*?[)）]|[\.…]+\s*\d+$',
            '',
            text
        ).strip()
