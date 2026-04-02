import re
from .template_extractor import TemplateExtractor

class DocumentProcessor:
    """精准切分工具：支持单次顺序遍历、章节刹车及页码提权容错"""

    @classmethod
    def _promote_sections(cls, sections: list) -> list:
        """页码提权容错逻辑"""
        PATTERN_START = re.compile(r'^\s*[\(（]?(附件|附表|格式|第[一二三四五六七八九十百]+[章节部分]|[一二三四五六七八九十]+[、.])')
        PATTERN_KEYWORD = re.compile(r'文件[的]?组成|商务文件|技术文件|部分格式附件|营业执照')
        
        def is_target(text: str) -> bool:
            if PATTERN_START.search(text): return True
            if PATTERN_KEYWORD.search(text) and len(text) < 40: return True
            return False

        page_has_native = {}
        for sec in sections:
            if not isinstance(sec, dict): continue
            page = sec.get('page', -1)
            if sec.get('type') == 'heading' and is_target(str(sec.get('text', '')).strip()):
                page_has_native[page] = True
                
        processed = []
        promoted = set()
        for sec in sections:
            if not isinstance(sec, dict): 
                processed.append(sec)
                continue
            page = sec.get('page', -1)
            sec_type = sec.get('type', '')
            text = str(sec.get('text', '')).strip()
            
            if sec_type == 'text' and not page_has_native.get(page, False) and page not in promoted:
                if is_target(text):
                    new_sec = sec.copy()
                    new_sec['type'] = 'heading'
                    processed.append(new_sec)
                    promoted.add(page)
                    continue
            processed.append(sec)
        return processed

    @classmethod
    def segment_document(cls, raw_json: dict, templates: list) -> list:
        data_node = raw_json.get('data', raw_json)
        sections = cls._promote_sections(data_node.get('layout_sections', []))
        
        for temp in templates:
            clean_title = re.sub(r'[^\u4e00-\u9fa5A-Za-z0-9]|附件|附表|附录|格式', '', temp['title'])
            temp['pattern'] = re.compile(f'.*?'.join(list(clean_title[:5])))
            temp['extracted_text'] = ""

        current_idx, captured = -1, []
        for sec in sections:
            text = str(sec.get('text', '')).strip()
            sec_type = sec.get('type', '')
            if not text: continue
            
            is_break_text = (
                re.search(r'^\s*第[一二三四五六七八九十百]+[章节部分]', text) or 
                re.search(r'^\s*[一二三四五六七八九十]+[、.]', text) or
                "营业执照" in text or
                "技术文件" in text
            )
            
            if sec_type == 'heading' and is_break_text:
                if current_idx != -1:
                    templates[current_idx]['extracted_text'] = "\n".join(captured)
                    current_idx, captured = -1, []
                continue

            if sec_type == 'heading' and re.search(r'^\s*[\(（]?(附件|格式|附表)', text):
                matched_idx = -1
                for i in range(current_idx + 1, len(templates)):
                    if templates[i]['pattern'].search(text):
                        matched_idx = i; break
                
                if matched_idx != -1:
                    if current_idx != -1: templates[current_idx]['extracted_text'] = "\n".join(captured)
                    current_idx, captured = matched_idx, [text]
                    continue
                elif current_idx != -1:
                    templates[current_idx]['extracted_text'] = "\n".join(captured)
                    current_idx, captured = -1, []
                    continue

            if current_idx != -1: captured.append(text)

        if current_idx != -1: templates[current_idx]['extracted_text'] = "\n".join(captured)

        results = []
        for temp in templates:
            final_text = re.sub(r'[\s\n]+\d+$', '', temp['extracted_text'].strip())
            results.append({"title": temp['title'], "text": final_text})
        return results

class ConsistencyChecker:
    """一致性审查服务：标点括号净化 + 粘贴锚点过滤 + 日期智能拆分"""
    def __init__(self):
        self.BRACKET_PATTERN = re.compile(r'（[^）]*）|【[^】]*】|\[[^\]]*\]|<[^>]*>')
        self.SPLIT_PATTERN = re.compile(r'（[^）]*）|【[^】]*】|\[[^\]]*\]|<[^>]*>|[:：_+，,。．.；;！!？?：“”"\'、\s\n\r\\@·・·]+')
        self.CONTENT_PATTERN = re.compile(r'[\u4e00-\u9fa5a-zA-Z0-9]+')

    def _normalize(self, text: str) -> str:
        if not text: return ""
        text = text.replace('(', '（').replace(')', '）')
        text = self.BRACKET_PATTERN.sub('', text)
        return "".join(self.CONTENT_PATTERN.findall(text))

    def _get_anchors(self, text: str):
        text = text.replace('(', '（').replace(')', '）')
        
        segments = self.SPLIT_PATTERN.split(text)
        anchors = []
        for s in segments:
            if not s or "粘贴" in s: continue
            norm_s = self._normalize(s)

            if len(norm_s) >= 2 : 
                anchors.append(norm_s)
        return anchors

    def compare_raw_data(self, model_json: dict, test_json: dict, found_sections: list = None):
        templates = TemplateExtractor.extract_consistency_templates(model_json)
        m_segs = DocumentProcessor.segment_document(model_json, templates)
        b_segs = DocumentProcessor.segment_document(test_json, templates)

        reports = []
        for i, m_seg in enumerate(m_segs):
            m_text, b_text = m_seg['text'], b_segs[i]['text']
            if len(m_text) < 20:
                reports.append({"name": m_seg['title'], "is_passed": True, "missing_anchors": ["【纯图/短文本】"]})
                continue

            norm_bidder = self._normalize(b_text)
            anchors = self._get_anchors(m_text)
            issues = [a for a in anchors if norm_bidder.find(a) == -1]

            reports.append({"name": m_seg['title'], "is_passed": len(issues) == 0, "missing_anchors": issues})
        return reports