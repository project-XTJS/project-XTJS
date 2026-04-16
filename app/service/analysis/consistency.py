import re
from typing import List, Dict

# 假设 TemplateExtractor 已在同目录下定义
from .template_extractor import TemplateExtractor 

class DocumentProcessor:
    """段落处理器：将文档切分为与模板对应的段落"""

    @classmethod
    def _compile_template_patterns(cls, templates: List[Dict]) -> None:
        """抽取独立方法：预编译模板匹配模式"""
        for temp in templates:
            # 1. 剔除括号内的补充说明
            title_no_brackets = re.sub(r'\(.*?\)|（.*?）', '', temp['title'])
            # 2. 清除干扰词与标点
            clean = re.sub(r'[^\u4e00-\u9fa5A-Za-z0-9]|附件|附表|附录|格式', '', title_no_brackets)
            # 3. 剥离前缀序号
            core = re.sub(r'^[\d一二三四五六七八九十百]+', '', clean)
            
            # 取核心前6个字符，允许中间穿插空白或横线
            # 【核心修复点】：增加 ^.{0,25}? 限制，强制要求标题核心词必须出现在文本前25个字符内
            # 防止在长段落（如说明须知）中偶然包含模板关键字（如"开标一览表"）导致被错误截断
            temp['pattern'] = re.compile(r'^.{0,25}?' + r'[\s\-_]*'.join(list(core[:6])))
            temp['buffer'] = []
            temp['extracted_text'] = ""

    @classmethod
    def _find_matching_template_idx(cls, clean_text: str, templates: List[Dict], current_idx: int) -> int:
        """抽取独立方法：寻找匹配的模板索引（包含乱序回退逻辑）"""
        # 优先向后找
        search_order = list(range(current_idx + 1, len(templates)))
        # 如果后面没找到，再回头从前面找（兼容乱序）
        search_order.extend(range(0, current_idx + 1))
        
        for j in search_order:
            if templates[j]['pattern'].search(clean_text):
                return j
        return -1

    @classmethod
    def segment_document(cls, raw_json: dict, templates: list, is_test_file: bool = False) -> List[Dict]:
        data_node = raw_json.get('data', raw_json)
        
        # 解析 logical_tables 并在预处理时传递，保留正文中的表格结构
        logical_tables = data_node.get('logical_tables', [])
        sections, headers = TemplateExtractor.preprocess_sections(
            data_node.get('layout_sections', []), 
            logical_tables
        )
        
        cls._compile_template_patterns(templates)
        current_idx = -1

        for sec in sections:
            text = sec['text']
            if not text or TemplateExtractor._is_noise(text, headers, sec.get('type')) or (sec.get('type') == 'text' and text.strip().isdigit()): 
                continue
                
            if sec['type'] == 'heading':
                clean_text = text.replace(' ', '')
                is_potential_title = True if is_test_file else TemplateExtractor.RE_HEADING_START.search(text)

                if is_potential_title:
                    matched_idx = cls._find_matching_template_idx(clean_text, templates, current_idx)
                
                    if matched_idx != -1:
                        # 忽略后文重复出现的标题模板 (防重入)
                        if len(templates[matched_idx]['buffer']) > 3: 
                            if current_idx != -1: 
                                templates[current_idx]['extracted_text'] = "\n".join(templates[current_idx]['buffer'])
                            current_idx = -1
                            continue
                        
                        # 成功匹配新标题：保存上一个的状态，并切换到新状态
                        if current_idx != -1: 
                            templates[current_idx]['extracted_text'] = "\n".join(templates[current_idx]['buffer'])
                        
                        current_idx = matched_idx
                        templates[current_idx]['buffer'] = [text]
                        continue
                    
                    elif current_idx != -1 and re.match(r'^[一二三四五六七八九十百]+[、．]', text.strip()):
                        templates[current_idx]['extracted_text'] = "\n".join(templates[current_idx]['buffer'])
                        current_idx = -1
                        continue
                
                # 遇到大章节断点，主动终止当前收集状态
                is_chapter_break = re.search(r'^第[一二三四五六七八九十百]+[章节部分]', text) or "技术文件" in text
                if current_idx != -1 and is_chapter_break:
                    templates[current_idx]['extracted_text'] = "\n".join(templates[current_idx]['buffer'])
                    current_idx = -1

            # 如果当前处于“收集状态”，则将正文追加到对应模板的缓冲区
            if current_idx != -1: 
                templates[current_idx]['buffer'].append(text)

        # 循环结束，收尾最后一个模板
        if current_idx != -1: 
            templates[current_idx]['extracted_text'] = "\n".join(templates[current_idx]['buffer'])
            
        return [{"title": t['title'], "text": t['extracted_text']} for t in templates]


class ConsistencyChecker:
    """一致性校验器：比对正文细节差异"""

    FORMAL_TITLE_LINE_RE = re.compile(
        r"^\s*(?:"
        r"(?:[（(]?\d+(?:\s*[-－]\s*\d+)?[)）\.、]?\s*)?(?:附件|附表)\s*\d+(?:\s*[-－]\s*\d+)*"
        r"|第[一二三四五六七八九十百0-9]+[章节部分]"
        r")"
    )
    NON_BODY_BLOCK_MARKERS = (
        "与本项目有关的一切正式往来通讯请寄",
        "正式往来通讯请寄",
    )
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

    def __init__(self):
        # NORM_PATTERN: 用于最后比对时，过滤一切非核心字符
        self.NORM_PATTERN = re.compile(r'[\u4e00-\u9fa5a-zA-Z0-9]+')
        
        # GAP_PATTERN: 匹配一切非中文、非字母、非数字的字符
        self.GAP_PATTERN = re.compile(r'[^\u4e00-\u9fa5a-zA-Z0-9]+')

    def _normalize(self, text: str) -> str:
        if not text: return ""
        return "".join(self.NORM_PATTERN.findall(text))

    def _normalize_title(self, text: str) -> str:
        if not text:
            return ""
        no_brackets = re.sub(r'\(.*?\)|（.*?）', '', text)
        clean = re.sub(r'[^\u4e00-\u9fa5A-Za-z0-9]|附件|附表|附录|格式', '', no_brackets)
        return re.sub(r'^[\d一二三四五六七八九十百]+', '', clean)

    def _is_formal_title_line(self, text: str) -> bool:
        return bool(self.FORMAL_TITLE_LINE_RE.match(str(text or "").strip()))

    def _strip_title_line(self, text: str, title: str) -> str:
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
        return any(marker in normalized_line for marker in self.NON_BODY_LINE_MARKERS)

    def _trim_non_body_lines(self, text: str) -> str:
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

    def _get_anchors(self, text: str) -> List[str]:
        # 1. 抹平括号（防止文本粘连）
        text = re.sub(r'\(.*?\)|（.*?）', ' ', text)
        
        # 2. 核心业务逻辑保留
        text = text.replace('年月日', '年 月 日')
        
        # 3. 使用预编译的非空白/字母/数字正则进行切割
        parts = self.GAP_PATTERN.split(text)
        
        anchors = []
        for p in parts:
            norm = self._normalize(p)
            # 业务跳过规则
            if '粘贴' in norm or ('签字' in norm and '盖章' in norm) or norm.isdigit(): 
                continue
            # 保留长度>=2的词汇，以及单字的“年月日”
            if len(norm) >= 2 or norm in ['年', '月', '日']: 
                anchors.append(norm)
                
        return anchors

    # 在 compare_raw_data 方法中增加特殊附件判断
    def compare_raw_data(self, model_json: dict, test_json: dict) -> List[Dict]:
        temps = TemplateExtractor.extract_consistency_templates(model_json)
        model_segments = [{"title": t['title'], "text": "\n".join(t['content'])} for t in temps]
        test_segments = DocumentProcessor.segment_document(test_json, temps, is_test_file=True)

        results = []
        for i, m_seg in enumerate(model_segments):
            m_txt = m_seg['text']
            t_txt = test_segments[i]['text']

            m_body = self._trim_non_body_lines(self._strip_title_line(m_txt, m_seg['title']))
            t_body = self._trim_non_body_lines(self._strip_title_line(t_txt, m_seg['title']))

            title = m_seg['title']
            # 特殊附件：只要存在内容即通过
            if "制造商声明函" in title or "制造商授权书" in title or "原厂授权函" in title:
                passed = bool(t_body.strip())
                results.append({
                    "name": title,
                    "is_passed": passed,
                    "missing_anchors": [] if passed else ["[未检测到内容]"]
                })
                continue

            norm_t = self._normalize(t_body)
            anchors = self._get_anchors(m_body)
            missing = [a for a in anchors if a not in norm_t]

            results.append({
                "name": title,
                "is_passed": len(missing) == 0,
                "missing_anchors": missing
            })
        return results
