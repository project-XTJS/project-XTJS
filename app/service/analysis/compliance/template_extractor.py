# -*- coding: utf-8 -*-
"""
模板提取与文本分类模块。

负责从招标文件 OCR 结果中：
- 识别标题、目录行、附件标题等文本类型（SectionClassifier）
- 提取完整性检查所需的条款清单（extract_requirements）
- 提取一致性比对所需的模板基准（extract_consistency_templates）
"""

import re

from ..attachment_synonyms import (
    canonicalize_attachment_title,
    strip_attachment_title_parenthetical_noise,
)


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
        r'(?<![\dA-Z一二三四五六七八九十百零\-－])'
        r'(?=(?:\d+[．\.、)]|[A-Z][．\.、)]|[一二三四五六七八九十百零]+[、．\.]|[（(](?:\d+|[A-Z]|[一二三四五六七八九十百零]+)[）)]))'
    )
    RESPONSE_FORMAT_ZONE_MARKERS = ("响应文件格式附件", "投标文件格式附件", "部分格式附件")
    RESPONSE_FORMAT_CHAPTER_MARKERS = ("响应文件格式", "投标文件格式")
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
    RESPONSE_FORMAT_ATTACHMENT_TITLE_MARKERS = (
        "投标承诺书",
        "投标函",
        "开标一览表",
        "法定代表人证明",
        "法定代表人授权委托书",
        "授权委托书",
        "项目管理机构人员情况表",
        "项目管理机构人员组成表",
        "项目经理简历表",
        "主要项目管理人员简历表",
        "投标人基本资料",
        "投标人基本情况表",
        "近年完成的类似项目情况表",
        "投标保证金",
        "开具增值税专用发票承诺书",
        "施工组织设计",
        "其他材料",
    )
    CHAPTER_HEADING_RE = re.compile(r'^\s*第[一二三四五六七八九十百零\d]+章')
    ATTACHMENT_NUMBER_RE = re.compile(
        r'^\s*(?:[（(]?\d+(?:\s*[-－]\s*\d+)?[)）\.、]?\s*)?(?:附件|附表)\s*(?P<number>\d+(?:\s*[-－]\s*\d+)*)'
    )
    ATTACHMENT_SCOPE_STOP_MARKERS = (
        "营业执照",
        "信用中国",
        "中国政府采购网",
        "国家企业信用信息公示系统",
        "纳税证明",
        "完税证明",
        "税收完税",
        "税收缴款",
        "电子缴税",
        "缴款凭证",
        "缴费凭证",
        "社会保障资金",
        "社会保险",
        "社保",
        "审计报告",
        "财务报表",
        "银行资信",
        "开户许可证",
        "开户证明",
        "基本存款账户",
        "保证金缴纳",
        "投标保证金",
        "发票",
    )
    COMMON_ATTACHMENT_TITLES = (
        "投标保证书",
        "投标承诺书",
        "投标函",
        "开标一览表",
        "分项报价表",
        "商务条款偏离表",
        "技术条款偏离表",
        "投标人基本情况表",
        "类似项目业绩清单",
        "法定代表人资格证明书",
        "法定代表人授权委托书",
        "投标人承诺声明函",
        "不参与围标串标承诺书",
        "保证金缴纳凭证",
        "财务状况及税收、社会保障资金缴纳情况声明函",
        "制造商声明函",
        "制造商授权书",
        "投标人认为需加以说明的其他内容",
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
    def _section_location(cls, section: dict) -> dict | None:
        text = str(section.get('text') or '').strip()
        page = section.get('page') if isinstance(section.get('page'), int) else None
        bbox = section.get('bbox') or section.get('box')
        if not text and page is None and bbox is None:
            return None
        return {
            "page": page,
            "bbox": bbox,
            "text": text[:240] if text else "",
            "type": str(section.get('type') or "text"),
            "coordinate_system": str(section.get('coordinate_system') or "pdf_point"),
        }

    @classmethod
    def _section_locations(cls, section: dict) -> list[dict]:
        if not isinstance(section, dict):
            return []

        page = section.get('page') if isinstance(section.get('page'), int) else None
        section_type = str(section.get('type') or "text")
        coordinate_system = str(section.get('coordinate_system') or "pdf_point")
        block_index = section.get('block_index')
        locations: list[dict] = []
        seen: set[tuple] = set()

        for line in section.get('lines') or []:
            if not isinstance(line, dict):
                continue
            text = str(line.get('text') or '').strip()
            line_page = line.get('page') if isinstance(line.get('page'), int) else page
            bbox = line.get('bbox') or line.get('box')
            if not text and line_page is None and bbox is None:
                continue
            key = (line_page, tuple(bbox or []), text)
            if key in seen:
                continue
            seen.add(key)
            location = {
                "page": line_page,
                "bbox": bbox,
                "text": text[:240] if text else "",
                "type": "line",
                "section_type": section_type,
                "coordinate_system": str(line.get('coordinate_system') or coordinate_system),
            }
            if block_index is not None:
                location["block_index"] = block_index
            if line.get('line_index') is not None:
                location["line_index"] = line.get('line_index')
            locations.append(location)

        if locations:
            return locations

        fallback = cls._section_location(section)
        return [fallback] if fallback is not None else []

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
        edge_text_pages: dict[str, set[int]] = {}
        document_pages: set[int] = set()

        # 第一遍：清洗文本并识别原生标题
        for sec in layout_sections:
            if not isinstance(sec, dict) or 'text' not in sec:
                continue

            new_sec = sec.copy()
            page = new_sec.get('page', -1)
            if isinstance(page, int) and page >= 0:
                document_pages.add(page)

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

            bbox = new_sec.get('bbox') or new_sec.get('box')
            page_height = (
                new_sec.get('page_height')
                or new_sec.get('image_height')
                or ((new_sec.get('page_size') or [None, None])[1]
                    if isinstance(new_sec.get('page_size'), (list, tuple))
                    and len(new_sec.get('page_size')) >= 2
                    else None)
            )
            near_page_edge = False
            if (
                isinstance(bbox, (list, tuple))
                and len(bbox) >= 4
                and all(isinstance(value, (int, float)) for value in bbox[:4])
            ):
                top = float(bbox[1])
                bottom = float(bbox[3])
                near_page_edge = top <= 180
                if isinstance(page_height, (int, float)) and page_height > 0:
                    near_page_edge = near_page_edge or bottom >= float(page_height) * 0.88
            compact_text = text.replace(' ', '')
            if (
                near_page_edge
                and isinstance(page, int)
                and 0 < len(compact_text) <= 80
            ):
                edge_text_pages.setdefault(compact_text, set()).add(page)

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

        page_count = max(1, len(document_pages))
        repeated_edge_texts = {
            text
            for text, pages in edge_text_pages.items()
            if len(pages) >= 3 and len(pages) / page_count >= 0.35
        }
        global_headers = {
            t for t, c in heading_counts.items() if c > 2
        } | repeated_edge_texts
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
                "委托", "授权", "承诺", "声明", "偏离", "合同", "资料",
                "许可证", "证书", "保证金", "合格证",
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
    def _attachment_title_key(cls, text: str) -> str:
        title = cls._attachment_title(text)
        title = strip_attachment_title_parenthetical_noise(title)
        title = re.split(r'[（(【\[]', title, maxsplit=1)[0]
        title = re.sub(r'(附件|附表|附录|格式|模板)', '', title)
        title = canonicalize_attachment_title(title)
        title = re.sub(r'[^\u4e00-\u9fa5A-Za-z0-9]', '', title)
        return title.strip()

    @classmethod
    def _catalog_like(cls, text: str) -> bool:
        compact = cls._compact(text)
        if not compact:
            return False
        if "目录" in compact:
            return True
        if re.search(r'(?:\.{2,}|…{2,}|_{2,})\d{1,4}$', compact):
            return True
        return len(re.findall(r'(?:\.{2,}|…{2,}|_{2,})\d{1,4}', compact)) >= 2

    @classmethod
    def _normalize_bbox(cls, value) -> list[int] | None:
        if value is None:
            return None
        if isinstance(value, (list, tuple)):
            if len(value) >= 4 and all(isinstance(item, (int, float)) for item in value[:4]):
                x, y, w, h = [int(round(float(item))) for item in value[:4]]
                if w >= 0 and h >= 0:
                    return [x, y, w, h]
                return [min(x, w), min(y, h), abs(w - x), abs(h - y)]
            if value and all(
                isinstance(item, (list, tuple))
                and len(item) >= 2
                and all(isinstance(part, (int, float)) for part in item[:2])
                for item in value
            ):
                xs = [float(item[0]) for item in value]
                ys = [float(item[1]) for item in value]
                left, top = int(round(min(xs))), int(round(min(ys)))
                right, bottom = int(round(max(xs))), int(round(max(ys)))
                return [left, top, max(right - left, 0), max(bottom - top, 0)]
        return None

    @classmethod
    def _is_page_title_like(cls, section: dict) -> bool:
        text = str(section.get('text') or '').strip()
        compact = cls._compact(text)
        if not compact:
            return False
        section_type = str(section.get('type') or '').strip().lower()
        if section_type == 'heading':
            return True
        if section_type in {'seal', 'signature'}:
            return False
        if len(compact) <= 42:
            return True
        if len(compact) > 96:
            return False
        bbox = cls._normalize_bbox(section.get('bbox') or section.get('box'))
        return bool(bbox and bbox[1] <= 260)

    @classmethod
    def _is_attachment_scope_stop_section(cls, section: dict, current_title: str) -> bool:
        text = str(section.get('text') or '').strip()
        compact = cls._compact(text)
        if not compact or cls._catalog_like(text):
            return False
        if str(section.get('type') or '').strip().lower() in {'seal', 'signature'}:
            return False

        current_key = cls._attachment_title_key(current_title)
        current_number = cls._attachment_number(current_title)
        attachment_number = cls._attachment_number(text)
        if attachment_number is not None:
            if current_number and attachment_number == current_number:
                return False
            return True

        if cls.CHAPTER_HEADING_RE.match(text) and not cls._is_response_format_chapter_heading(text):
            return True

        if not cls._is_page_title_like(section):
            return False

        title_key = cls._attachment_title_key(text)
        if title_key and current_key and title_key == current_key:
            return False
        if cls._is_response_format_attachment_heading(section):
            return True
        common_title_keys = {cls._attachment_title_key(title) for title in cls.COMMON_ATTACHMENT_TITLES}
        if title_key and title_key in common_title_keys:
            return True
        return any(marker in compact for marker in cls.ATTACHMENT_SCOPE_STOP_MARKERS)

    @classmethod
    def _effective_attachment_chunk(cls, chunk: list[dict]) -> list[dict]:
        if not chunk:
            return []
        title = str(chunk[0].get('text') or '').strip()
        start_page = chunk[0].get('page') if isinstance(chunk[0].get('page'), int) else None
        effective: list[dict] = []
        for index, section in enumerate(chunk):
            page = section.get('page') if isinstance(section.get('page'), int) else None
            same_start_page = start_page is not None and page == start_page
            section_type = str(section.get('type') or '').strip().lower()
            if (
                index > 0
                and (not same_start_page or section_type == 'heading')
                and cls._is_attachment_scope_stop_section(section, title)
            ):
                break
            effective.append(section)
        return effective or chunk[:1]

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
        if str(section.get('type') or '').strip().lower() != 'heading':
            return False
        if not compact or compact in {"目录", "附"}:
            return False
        if "包括" in compact and ("商务标" in compact or "技术标" in compact):
            return False
        if "及其" in compact and ("、" in compact or "附录" in compact):
            return False
        attachment_number = cls._attachment_number(text)
        if attachment_number is not None:
            attachment_index = compact.find("附件")
            if attachment_index >= 0 and attachment_index <= 12:
                return True
        if any(marker in compact for marker in cls.RESPONSE_FORMAT_ATTACHMENT_TITLE_MARKERS):
            return True
        return (
            SectionClassifier.is_attachment_heading_text(text)
            or cls._is_embedded_response_format_heading(section)
        )

    @classmethod
    def _find_business_format_start_index(cls, sections: list[dict]) -> int | None:
        """跳过投标文件格式章内的目录，定位真正的“商务标”模板正文。"""
        for idx, section in enumerate(sections):
            if str(section.get('type') or '').strip().lower() != 'heading':
                continue
            compact = cls._compact(section.get('text') or '')
            if not compact:
                continue
            if "商务标" not in compact or "技术" in compact or "包括" in compact:
                continue
            if "：" in compact or ":" in compact or compact.endswith("商务标"):
                return idx
        return None

    @classmethod
    def _is_response_format_chapter_heading(cls, text: str) -> bool:
        """判断标题是否正是“响应文件格式/投标文件格式”章名。"""
        compact = cls._compact(text).strip("：:；;。")
        compact = re.sub(r'^第[一二三四五六七八九十百零\d]+章', '', compact)
        return compact in cls.RESPONSE_FORMAT_CHAPTER_MARKERS

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
            text = str(section.get('text') or '').strip()
            if cls._is_noise(text, set(), section.get('type')):
                continue
            compact = cls._compact(text)
            if any(
                compact == marker or (compact.endswith(marker) and len(compact) <= len(marker) + 8)
                for marker in cls.RESPONSE_FORMAT_ZONE_MARKERS
            ):
                start_index = idx
                break
        if start_index is None:
            for idx, section in enumerate(sections):
                text = str(section.get('text') or '').strip()
                if str(section.get('type') or '').strip().lower() != 'heading':
                    continue
                if cls._is_noise(text, set(), section.get('type')):
                    continue
                if cls._is_response_format_chapter_heading(text):
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

        effective_start = cls._find_business_format_start_index(sections)
        section_offset = (effective_start + 1) if effective_start is not None else 0
        candidate_sections = sections[section_offset:]
        starts = [
            idx
            for idx, section in enumerate(candidate_sections)
            if cls._is_response_format_attachment_heading(section)
        ]
        attachments: list[dict] = []
        current_top_level_number: str | None = None
        for pos, start in enumerate(starts):
            end = starts[pos + 1] if pos + 1 < len(starts) else len(candidate_sections)
            chunk = cls._effective_attachment_chunk(candidate_sections[start:end])
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
            title_locations = cls._section_locations(chunk[0])
            attachments.append(
                {
                    "attachment_number": attachment_number,
                    "title": normalized_title,
                    "content": [
                        str(section.get('text') or '').strip()
                        for section in chunk
                        if str(section.get('text') or '').strip()
                    ],
                    "title_locations": title_locations,
                    "locations": [
                        location
                        for section in chunk
                        for location in cls._section_locations(section)
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
        business_scope = cls.extract_business_attachment_scope(model_raw_json)
        response_attachments, scoped = cls.filter_business_response_attachments(model_raw_json)

        attachments_by_number: dict[str, dict] = {}
        for attachment in response_attachments:
            attachment_number = str(attachment.get("attachment_number") or "").strip()
            if attachment_number and attachment_number not in attachments_by_number:
                attachments_by_number[attachment_number] = attachment

        ordered_list: list[str] = []
        attachment_mapping: dict[str, list[str]] = {}
        dedupe_seen: set[str] = set()

        def push_requirement(title: str, numbers: list[str] | None = None) -> None:
            normalized_title = str(title or "").strip()
            if not normalized_title:
                return
            dedupe_key = cls._requirement_core_title(normalized_title) or cls._compact(normalized_title)
            if not dedupe_key or dedupe_key in dedupe_seen:
                return
            dedupe_seen.add(dedupe_key)
            ordered_list.append(normalized_title)
            if numbers:
                clean_numbers = [str(num).strip() for num in numbers if str(num).strip()]
                if clean_numbers:
                    attachment_mapping[normalized_title] = clean_numbers

        # 有附件引用的组成条目优先落到附件模板标题；没有附件引用的材料项直接保留。
        for entry in business_scope.get("item_entries") or []:
            numbers = [str(num).strip() for num in entry.get("attachment_numbers") or [] if str(num).strip()]
            if numbers:
                matched_any = False
                for number in numbers:
                    attachment = attachments_by_number.get(number)
                    if attachment is None:
                        continue
                    push_requirement(str(attachment.get("title") or "").strip(), [number])
                    matched_any = True
                if not matched_any:
                    push_requirement(str(entry.get("content") or "").strip(), numbers)
                continue
            push_requirement(str(entry.get("content") or "").strip())

        # 只有未能识别商务标组成范围时才回退全量附件；已识别范围时避免把技术标模板混入商务完整性。
        if not scoped:
            for attachment in response_attachments:
                title = str(attachment.get("title") or "").strip()
                if not title or attachment.get("is_composite"):
                    continue
                attachment_number = str(attachment.get("attachment_number") or "").strip()
                push_requirement(title, [attachment_number] if attachment_number else None)

        return ordered_list, attachment_mapping

    @classmethod
    def extract_requirement_locations(cls, model_raw_json: dict) -> dict[str, list[dict]]:
        """Return tender-side source locations for business integrity requirements."""
        business_scope = cls.extract_business_attachment_scope(model_raw_json)
        response_attachments, _scoped = cls.filter_business_response_attachments(model_raw_json)

        attachments_by_number: dict[str, dict] = {}
        for attachment in response_attachments:
            attachment_number = str(attachment.get("attachment_number") or "").strip()
            if attachment_number and attachment_number not in attachments_by_number:
                attachments_by_number[attachment_number] = attachment

        locations_by_title: dict[str, list[dict]] = {}

        def add_locations(title: str, locations: list[dict] | None) -> None:
            normalized_title = str(title or "").strip()
            if not normalized_title or not locations:
                return
            clean_locations = [location for location in locations if isinstance(location, dict)]
            if not clean_locations:
                return
            locations_by_title.setdefault(normalized_title, clean_locations)

        for entry in business_scope.get("item_entries") or []:
            if not isinstance(entry, dict):
                continue
            entry_locations = [dict(location) for location in entry.get("locations") or [] if isinstance(location, dict)]
            if not entry_locations:
                continue
            numbers = [str(num).strip() for num in entry.get("attachment_numbers") or [] if str(num).strip()]
            matched_any = False
            for number in numbers:
                attachment = attachments_by_number.get(number)
                title = str((attachment or {}).get("title") or "").strip()
                if not title:
                    continue
                add_locations(title, entry_locations)
                matched_any = True
            if not matched_any:
                add_locations(str(entry.get("content") or "").strip(), entry_locations)

        return locations_by_title

    @classmethod
    def extract_consistency_templates(cls, model_raw_json: dict) -> list:
        """提取一致性比对所需的模板基准，来源与完整性检查保持一致。"""
        requirements, attachment_mapping = cls.extract_requirements(model_raw_json)
        response_attachments = cls.extract_response_format_attachments(model_raw_json)
        if not response_attachments:
            return []

        attachments_by_title: dict[str, dict] = {}
        attachments_by_core_title: dict[str, dict] = {}
        attachments_by_number: dict[str, dict] = {}
        for attachment in response_attachments:
            title = str(attachment.get("title") or "").strip()
            if not title or attachment.get("is_composite"):
                continue
            compact_title = cls._compact(title)
            if compact_title and compact_title not in attachments_by_title:
                attachments_by_title[compact_title] = attachment
            compact_core_title = cls._compact(cls._attachment_core_title(title))
            if compact_core_title and compact_core_title not in attachments_by_core_title:
                attachments_by_core_title[compact_core_title] = attachment
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
                attachment = attachments_by_core_title.get(
                    cls._compact(cls._requirement_core_title(item_title))
                )
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
                    # 一致性预览应定位到当前附件标题页；整段正文 locations 在 OCR
                    # 切块跨页或混入下一附件时容易把预览带到下一份模板。
                    "locations": list(attachment.get("title_locations") or attachment.get("locations") or []),
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

    @classmethod
    def _attachment_core_title(cls, text: str) -> str:
        """提取附件标题主体，便于与“投标文件的组成”中的商务标条目比对。"""
        value = cls._attachment_title(text)
        value = strip_attachment_title_parenthetical_noise(value)
        value = re.sub(
            r'^\s*(?:附件|附表)\s*\d+(?:\s*[-－]\s*\d+)*[、.)）．]?\s*',
            '',
            value,
        )
        value = re.sub(r'[(（].*?格式.*?[)）]', '', value)
        value = re.sub(r'格式', '', value)
        value = canonicalize_attachment_title(value)
        return re.sub(r'\s+', ' ', value).strip('：:；;，,。 ')

    @classmethod
    def _requirement_core_title(cls, text: str) -> str:
        """提取要求条目的标题主体，用于和附件标题做统一去重。"""
        value = cls._clean_label(text or "")
        value = strip_attachment_title_parenthetical_noise(value)
        value = re.sub(r'^\s*(?:[A-Z]|\d+|[一二三四五六七八九十百零]+)\s*[．\.、)]\s*', '', value)
        value = re.sub(r'^[（(](?:\d+|[A-Z]|[一二三四五六七八九十百零]+)[）)]\s*', '', value)
        value = re.sub(r'格式参见本章附件\s*[\d\-—，,、\s]+', '', value)
        value = value.replace('（须加盖公章）', '').replace('(须加盖公章)', '').replace('须加盖公章', '')
        value = value.replace('后附证明材料', '')
        value = value.replace('可另外再附公司简介（如有）', '').replace('可另外再附公司简介', '')
        value = value.replace('（如有）', '').replace('(如有)', '')
        value = value.replace('完成的', '').replace('近三年以来', '').replace('近三年内', '').replace('近三年', '')
        value = value.replace('情况介绍', '情况').replace('情况表', '情况').replace('介绍', '')
        value = canonicalize_attachment_title(value)
        return re.sub(r'\s+', '', value).strip('：:；;，,。 ')

    @classmethod
    def _extract_attachment_refs(cls, text: str) -> list[str]:
        """提取“格式参见本章附件X”中的附件编号列表。"""
        refs = re.findall(r'格式参见本章附件\s*([\d\-—，,、\s]+)', str(text or ""))
        numbers: list[str] = []
        for raw in refs:
            for num in re.split(r'[，,、\s]+', raw):
                cleaned = num.strip().strip('—－-')
                if cleaned:
                    numbers.append(cleaned)
        return numbers

    @classmethod
    def extract_business_attachment_scope(cls, model_raw_json: dict) -> dict:
        """提取“投标文件的组成”中明确归入商务标的条目和附件范围。"""
        data_node = model_raw_json.get('data', model_raw_json)
        sections, headers = cls.preprocess_sections(data_node.get('layout_sections', []))

        ordered_items: list[str] = []
        item_entries: list[dict] = []
        attachment_mapping: dict[str, list[str]] = {}
        required_numbers: set[str] = set()
        required_titles: set[str] = set()
        stage = 0
        found_composition = False
        entered_business = False

        for sec in sections:
            text = sec['text']
            if not text or cls._is_noise(text, headers, sec.get('type')):
                continue

            compact = cls._compact(text)
            if sec['type'] == 'heading':
                if stage == 0 and '投标文件' in text and '组成' in text:
                    stage = 1
                    found_composition = True
                    continue

            direct_business_heading = bool(
                re.match(r"^(?:\d+(?:\.\d+)*|[（(]?[一二三四五六七八九十]+[)）]?、?)?商务标", compact)
            )
            if (
                stage == 1
                and (sec.get('type') == 'heading' or direct_business_heading)
                and '商务' in compact
                and '技术' not in compact
            ):
                stage = 2
                entered_business = True
                continue
            direct_technical_heading = bool(
                re.match(r"^(?:\d+(?:\.\d+)*|[（(]?[一二三四五六七八九十]+[)）]?、?)?技术标", compact)
            )
            if (
                stage == 2
                and (sec.get('type') == 'heading' or direct_technical_heading)
                and '技术' in compact
                and '商务' not in compact
            ):
                break

            if stage != 2:
                continue

            def requirement_locations(requirement_text: str) -> list[dict]:
                locations = []
                for location in cls._section_locations(sec):
                    if not isinstance(location, dict):
                        continue
                    next_location = dict(location)
                    if requirement_text:
                        next_location["text"] = requirement_text
                    locations.append(next_location)
                return locations[:1]

            parsed_any = False
            for part in cls._split_requirement_parts(text):
                seq, content = cls._parse_requirement_item(part)
                if not seq or not content:
                    continue
                if not cls._looks_like_requirement_leaf(content):
                    continue

                cleaned = cls._clean_label(content)
                ordered_items.append(f"{seq}. {cleaned}")
                required_titles.add(cls._compact(cleaned))

                attach_numbers = cls._extract_attachment_refs(content)
                item_entries.append(
                    {
                        "seq": seq,
                        "content": cleaned,
                        "attachment_numbers": attach_numbers,
                        "title_core": cls._requirement_core_title(cleaned),
                        "locations": requirement_locations(cleaned),
                    }
                )
                if attach_numbers:
                    attachment_mapping[seq] = attach_numbers
                    required_numbers.update(attach_numbers)
                parsed_any = True

            if parsed_any:
                continue

            if cls._looks_like_requirement_leaf(text) and len(compact) <= 80:
                cleaned = cls._clean_label(text)
                if cleaned:
                    seq = str(len(item_entries) + 1)
                    ordered_items.append(f"{seq}. {cleaned}")
                    required_titles.add(cls._compact(cleaned))
                    item_entries.append(
                        {
                            "seq": seq,
                            "content": cleaned,
                            "attachment_numbers": [],
                            "title_core": cls._requirement_core_title(cleaned),
                            "locations": requirement_locations(cleaned),
                        }
                    )

        return {
            "has_business_composition": found_composition and entered_business,
            "ordered_items": ordered_items,
            "item_entries": item_entries,
            "attachment_mapping": attachment_mapping,
            "required_numbers": required_numbers,
            "required_titles": required_titles,
        }

    @classmethod
    def filter_business_response_attachments(
        cls,
        model_raw_json: dict,
        attachments: list[dict] | None = None,
    ) -> tuple[list[dict], bool]:
        """按“投标文件的组成”过滤商务标应包含的附件；无组成时回退全量附件。"""
        source_attachments = list(attachments or cls.extract_response_format_attachments(model_raw_json))
        scope = cls.extract_business_attachment_scope(model_raw_json)
        if not scope.get("has_business_composition"):
            return source_attachments, False

        required_numbers = {str(x).strip() for x in scope.get("required_numbers") or set() if str(x).strip()}
        required_titles = {str(x).strip() for x in scope.get("required_titles") or set() if str(x).strip()}
        if not required_numbers and not required_titles:
            return source_attachments, False

        filtered: list[dict] = []
        for attachment in source_attachments:
            attachment_number = str(attachment.get("attachment_number") or "").strip()
            title = str(attachment.get("title") or "").strip()
            compact_title = cls._compact(title)
            compact_core_title = cls._compact(cls._attachment_core_title(title))

            if attachment_number and attachment_number in required_numbers:
                filtered.append(attachment)
                continue
            if compact_title and compact_title in required_titles:
                filtered.append(attachment)
                continue
            if compact_core_title and any(
                compact_core_title == token
                or compact_core_title in token
                or token in compact_core_title
                for token in required_titles
            ):
                filtered.append(attachment)

        return filtered, True
