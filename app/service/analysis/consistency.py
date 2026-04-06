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
            if not text or TemplateExtractor._is_noise(text, headers) or (sec.get('type') == 'text' and text.strip().isdigit()): 
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

    def __init__(self):
        # NORM_PATTERN: 用于最后比对时，过滤一切非核心字符
        self.NORM_PATTERN = re.compile(r'[\u4e00-\u9fa5a-zA-Z0-9]+')
        
        # GAP_PATTERN: 匹配一切非中文、非字母、非数字的字符
        self.GAP_PATTERN = re.compile(r'[^\u4e00-\u9fa5a-zA-Z0-9]+')

    def _normalize(self, text: str) -> str:
        if not text: return ""
        return "".join(self.NORM_PATTERN.findall(text))

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

    def compare_raw_data(self, model_json: dict, test_json: dict) -> List[Dict]:
        # 获取模板文本
        temps = TemplateExtractor.extract_consistency_templates(model_json)
        model_segments = [{"title": t['title'], "text": "\n".join(t['content'])} for t in temps]
        
        # 获取测试文件文本
        test_segments = DocumentProcessor.segment_document(test_json, temps, is_test_file=True)

        results = []
        for i, m_seg in enumerate(model_segments):
            m_txt = m_seg['text']
            t_txt = test_segments[i]['text']

            # 剔除标题行（如果存在换行）
            t_body = t_txt.split('\n', 1)[1] if '\n' in t_txt else ""
            m_body = m_txt.split('\n', 1)[1] if '\n' in m_txt else ""

            # 将测试正文高度清洗压缩
            norm_t = self._normalize(t_body)
            # 提取模板锚点
            anchors = self._get_anchors(m_body)
            
            # O(N) 级别的高效比对
            missing = [a for a in anchors if a not in norm_t]
            
            results.append({
                "name": m_seg['title'], 
                "is_passed": len(missing) == 0, 
                "missing_anchors": missing
            })
            
        return results