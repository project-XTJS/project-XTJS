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
    def _is_consistency_template_heading(cls, text: str) -> bool:
        """判断文本是否为一致性模板的章节标题。"""
        raw = str(text or "").strip()
        if not raw:
            return False
        if SectionClassifier.RE_ATTACHMENT_TITLE.search(raw):
            return True
        if re.match(r'^第[一二三四五六七八九十百零\d]+[章节部分篇项]', raw):
            compact = re.sub(r'\s+', '', raw)
            return len(compact) <= 48 and any(
                keyword in compact
                for keyword in SectionClassifier.ATTACHMENT_TITLE_KEYWORDS
            )
        return False

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

        return ordered_list, attachment_mapping

    @classmethod
    def extract_consistency_templates(cls, model_raw_json: dict) -> list:
        """提取一致性比对所需的模板基准（含表格表头），返回模板列表。

        Args:
            model_raw_json: 招标文件 OCR JSON。

        Returns:
            模板字典列表，每个包含 "title" 和 "content"（文本行列表）。
        """
        data_node = model_raw_json.get('data', model_raw_json)

        # 传入 logical_tables 以启用表格结构化，is_template=True 表示仅提取表头
        sections, headers = cls.preprocess_sections(
            data_node.get('layout_sections', []),
            data_node.get('logical_tables', []),
            is_template=True,
        )

        templates, current, in_zone = [], None, False
        for sec in sections:
            text = sec['text']
            if not text or cls._is_noise(text, headers, sec.get('type')):
                continue

            if sec['type'] == 'heading' and not in_zone:
                if "部分格式附件" in text:
                    in_zone = True
                    continue

            if in_zone:
                if sec['type'] == 'heading':
                    # 遇到大章节或特定关键词则停止收集
                    if re.match(r'^第[一二三四五六七八九十百]+章', text) or "营业执照" in text:
                        break
                    if cls._is_consistency_template_heading(text):
                        if current:
                            templates.append(current)
                        current = {"title": cls._clean_label(text), "content": [text]}
                        continue
                if current:
                    current["content"].append(text)

        if current:
            templates.append(current)
        return templates

    @staticmethod
    def _clean_label(text: str) -> str:
        """清理标签文本：去掉分号、句号结尾，去掉格式参见说明，去掉末尾页码。"""
        return re.sub(
            r'[；;:：。]$|[(（].*?格式.*?参见.*?[)）]|[\.…]+\s*\d+$',
            '',
            text
        ).strip()