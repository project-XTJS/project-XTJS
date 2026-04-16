import re

class SectionClassifier:
    """专门负责识别文本类型的工具类，提供标题识别和目录排除功能"""
    # 增加对目录页宽容度的排除：匹配省略号、连续点、下划线或末尾带孤立数字
    RE_TOC = re.compile(r'\.{3,}|…{2,}|_{3,}\s*\d+$')
    RE_TOC_TRAILING_PAGE = re.compile(r'\s\d+$')
    
    # 标题识别标准
    RE_HEADING_START = re.compile(r'^\s*[\(（]?(附件|附表|格式|第[一二三四五六七八九十百]+[章节部分]|[一二三四五六七八九十]+[、.]|[\(（]?\d+[\)）\.、])')
    RE_KEYWORD_TITLE = re.compile(r'文件[的]?组成|商务文件|技术文件|部分格式附件|营业执照|招标文件|采购文件')
    RE_ATTACHMENT_TITLE = re.compile(r'^\s*(?:[（(]?\d+(?:\s*[-－]\s*\d+)?[)）\.、]?\s*)?(?:附件|附表)\s*\d+(?:\s*[-－]\s*\d+)*')
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
    NON_TOC_FIELD_MARKERS = ("：", ":", "/", "／")

    @classmethod
    def is_heading(cls, text: str, is_strict=False) -> bool:
        """判断是否为有效标题"""
        if cls.is_toc_noise(text):
            return False
        if is_strict:
            return bool(cls.RE_HEADING_START.search(text) or cls.RE_KEYWORD_TITLE.search(text))
        return bool(cls.RE_HEADING_START.search(text) or (cls.RE_KEYWORD_TITLE.search(text) and len(text) < 60))

    @classmethod
    def is_attachment_heading_text(cls, text: str) -> bool:
        if cls.is_toc_noise(text):
            return False
        raw_text = str(text or "").strip()
        if not raw_text or len(raw_text) > 80:
            return False
        if not cls.RE_ATTACHMENT_TITLE.search(raw_text):
            return False

        compact = re.sub(r"\s+", "", raw_text)
        if any(marker in compact for marker in cls.ATTACHMENT_BODY_MARKERS):
            return False
        if "。" in raw_text or "；" in raw_text or ";" in raw_text:
            return False

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
        raw_text = str(text or "").strip()
        if not raw_text:
            return False

        if cls.RE_TOC.search(raw_text):
            return True

        # 尾部页码判断只用于明显像目录的短行，避免误伤带字段名和数值的正文。
        if len(raw_text) > 40:
            return False
        if any(marker in raw_text for marker in cls.NON_TOC_FIELD_MARKERS):
            return False
        return bool(cls.RE_TOC_TRAILING_PAGE.search(raw_text))


class TemplateExtractor:
    """双雷达导航提取器：负责从招标文件中提取清单与模板基准"""

    RE_TAGS = re.compile(r'\\[a-zA-Z]+|[{}$]')

    @classmethod
    def _format_logical_table(cls, tb: dict, is_template: bool = False) -> str:
        """将逻辑表格格式化为文本
        :param is_template: 如果为 True，则只提取表格的第一行（表头）作为一致性锚点
        """
        lines = []
        rows = tb.get('rows', tb.get('body', tb.get('data', [])))
        
        if not rows and tb.get('headers'):
            lines.append(" | ".join(str(x) for x in tb['headers']))
            
        # 【核心修改点】：如果是模版文件且存在多行，我们只截取 rows[0]（通常为表头要求）
        if is_template and rows:
            rows = rows[:1]
            
        for row in rows:
            if isinstance(row, dict):
                lines.append(" | ".join(str(v) for v in row.values() if v is not None))
            elif isinstance(row, list):
                cell_texts = [str(cell['text']) if isinstance(cell, dict) and 'text' in cell else str(cell) for cell in row]
                lines.append(" | ".join(cell_texts))
        return "\n".join(lines)
    
    @classmethod
    def preprocess_sections(cls, layout_sections: list, logical_tables: list = None, is_template: bool = False) -> tuple:
        """统一预处理：提权、清洗、识别全局页眉
        如果传入 logical_tables，则执行表格结构解析植入
        """
        # 预处理：构建表格跨页映射字典（只有在传入 logical_tables 时才执行）
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

        # 第一遍扫描：清洗文本并使用分类器识别原生标题
        for sec in layout_sections:
            if not isinstance(sec, dict) or 'text' not in sec: continue
            
            new_sec = sec.copy()
            page = new_sec.get('page', -1)
            
            # 如果启用了表格处理，且当前是 table 类型
            if logical_tables and new_sec.get('type') == 'table':
                tbs = tables_by_page.get(page, [])
                idx = consumed_tables.get(page, 0)
                if idx < len(tbs):
                    tb = tbs[idx]
                    consumed_tables[page] = idx + 1
                    tb_id = tb.get('id', id(tb))
                    # 避免跨页表格内容重复提取
                    if tb_id not in overall_consumed:
                        overall_consumed.add(tb_id)
                        # 【核心修改点】：将 is_template 标志传递给表格格式化函数
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

            if new_sec.get('type') == 'heading' and SectionClassifier.is_heading(text, is_strict=True):
                page_has_heading[page] = True
                clean_h = text.replace(' ', '')
                heading_counts[clean_h] = heading_counts.get(clean_h, 0) + 1
                
            processed.append(new_sec)

        final_processed = []
        # 第二遍扫描：执行提权逻辑 (Text -> Heading)
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
        """判断是否为目录行或重复页眉"""
        clean = text.replace(' ', '')
        if clean in headers:
            return True
        if section_type == 'table':
            return False
        return SectionClassifier.is_toc_noise(text)

    @classmethod
    def extract_requirements(cls, model_raw_json: dict) -> tuple:
        """提取完整性清单：返回有序列表和序号到附件编号的映射"""
        data_node = model_raw_json.get('data', model_raw_json)
        sections, headers = cls.preprocess_sections(data_node.get('layout_sections', []))
    
        ordered_list = []
        attachment_mapping = {}  # 序号 -> [附件编号列表]
        stage = 0

        for sec in sections:
            text = sec['text']
            if not text or cls._is_noise(text, headers, sec.get('type')): continue

            if sec['type'] == 'heading':
                if stage == 0 and '文件' in text and '组成' in text: stage = 1; continue
                if stage == 1 and "商务" in text: stage = 2; continue
                if stage == 2 and "技术" in text: break

            if stage == 2:
                # 按照文本出现的先后顺序切分并压入同一个列表
                parts = re.split(r'(?<![\dA-Z])(?=(?:\d+|[A-Z])[．\.]\s*)', text)
                for part in filter(None, [p.strip() for p in parts]):
                    # 提取序号
                    m = re.match(r'^(\d+)[．\.]\s*(.+)', part)
                    s = re.match(r'^([A-Z])[．\.]\s*(.+)', part)
                    seq = None
                    content = ""
                    if m:
                        seq = m.group(1)
                        content = m.group(2)
                        ordered_list.append(f"{seq}. {cls._clean_label(content)}")
                    elif s:
                        seq = s.group(1)
                        content = s.group(2)
                        ordered_list.append(f"{seq}. {cls._clean_label(content)}")
                    else:
                        continue

                    # 从内容中提取“格式参见本章附件X”中的附件编号
                    attach_refs = re.findall(r'格式参见本章附件\s*([\d\-—，,、\s]+)', content)
                    attach_numbers = []
                    if attach_refs:
                        # 处理可能多个编号，如 "8-1、8-2" 或 "8-1,8-2"
                        raw = attach_refs[0]
                        # 按逗号、顿号、空格分割
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
        """提取一致性模板基准（传入 logical_tables 启动表格结构化）"""
        data_node = model_raw_json.get('data', model_raw_json)
        
        # 【核心修改点】：通过传递 is_template=True 激活表头单行提取逻辑
        sections, headers = cls.preprocess_sections(
            data_node.get('layout_sections', []), 
            data_node.get('logical_tables', []),
            is_template=True
        )

        templates, current, in_zone = [], None, False
        for sec in sections:
            text = sec['text']
            if not text or cls._is_noise(text, headers, sec.get('type')): continue
            
            if sec['type'] == 'heading' and not in_zone:
                if "部分格式附件" in text: in_zone = True; continue

            if in_zone:
                if sec['type'] == 'heading':
                    if re.match(r'^第[一二三四五六七八九十百]+章', text) or "营业执照" in text: break
                    if SectionClassifier.RE_HEADING_START.search(text):
                        if current: templates.append(current)
                        current = {"title": cls._clean_label(text), "content": [text]}
                        continue
                if current: current["content"].append(text)

        if current: templates.append(current)
        return templates

    @staticmethod
    def _clean_label(text: str) -> str:
        return re.sub(r'[；;:：。]$|[(（].*?格式.*?参见.*?[)）]|[\.…]+\s*\d+$', '', text).strip()
