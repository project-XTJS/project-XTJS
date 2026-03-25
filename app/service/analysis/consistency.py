import re
import difflib

# =====================================================================
# 1. 业务标准配置：定义 13 个核心附件的起止锚点
# =====================================================================
MODULE_CONFIG = [
    {"id": 1, "title": "投标保证书", "start": "投标保证书", "end": "通讯请寄"},
    {"id": 2, "title": "开标一览表", "start": "开标一览表", "end": "计算错误"},
    {"id": 3, "title": "商务条款偏离表", "start": "商务条款偏离表", "end": "投标人法定代表人"},
    {"id": 4, "title": "技术条款偏离表", "start": "技术条款偏离表", "end": "投标人名称"},
    {"id": 5, "title": "投标人基本情况表", "start": "投标人基本情况", "end": "予以证实"},
    {"id": 6, "title": "类似项目业绩清单", "start": "类似项目业绩清单", "end": "复印件或影印件"},
    {"id": 7, "title": "法定代表人资格证明书", "start": "法定代表人资格证明书", "end": "负责人"},
    {"id": 8, "title": "法定代表人授权委托书", "start": "法定代表人授权委托书", "end": "法定代表人签字"},
    {"id": 9, "title": "供应商承诺声明函", "start": "供应商承诺声明函", "end": "特此声明"},
    {"id": 10, "title": "不参与围标串标承诺书", "start": "不参与围标串标承诺书", "end": "特此承诺"},
    {"id": 11, "title": "拟派项目负责人情况表", "start": "拟派项目负责人情况表", "end": "须征得采购人同意"},
    {"id": 12, "title": "项目人员配置表", "start": "项目人员配置表", "end": "征得采购人同意"},
    {"id": 13, "title": "财务状况声明函", "start": "财务状况及税收、社会保障资金缴纳情况声明函", "end": "相应责任"},
]

class DocumentProcessor:
    """针对新版 OCR (layout_sections) 的内存提取器"""
    @staticmethod
    def _get_full_text(raw_json_data):
        data_node = raw_json_data.get('data', raw_json_data)
        full_text = data_node.get('text') or data_node.get('content', '')
        if not full_text and 'layout_sections' in data_node:
            full_text = "\n".join([str(sec.get('text') or sec.get('content') or '') for sec in data_node['layout_sections'] if isinstance(sec, dict)])
        if not full_text and 'pages' in data_node:
            full_text = "\n".join([p.get('text', '') for p in data_node['pages'] if isinstance(p, dict)])
        return full_text

    @staticmethod
    def _find_nth(text, pattern, n):
        matches = list(re.finditer(re.escape(pattern), text))
        return matches[n-1].start() if len(matches) >= n else -1

    @classmethod
    def extract_segments(cls, raw_json_data):
        full_text = cls._get_full_text(raw_json_data)
        if not full_text: return []

        body_start = cls._find_nth(full_text, "商务标文件", 2)
        search_area = full_text[body_start:] if body_start != -1 else full_text

        extracted = []
        for cfg in MODULE_CONFIG:
            start_idx = cls._find_nth(search_area, cfg['start'], 2)
            if start_idx == -1:
                match = re.search(re.escape(cfg['start']), search_area)
                start_idx = match.start() if match else -1

            text_content = ""
            if start_idx != -1:
                chunk = search_area[start_idx:]
                end_match = re.search(re.escape(cfg['end']), chunk, re.S)
                text_content = chunk[:end_match.end()] if end_match else chunk[:5000]
            
            extracted.append({"id": cfg['id'], "title": cfg['title'], "text": text_content.strip()})
        return extracted

class TemplateAnalysisService:
    """格式一致性审查服务"""
    def __init__(self):
        # 🟢 新增：专门匹配各类中英文括号及内部全部内容
        self.BRACKET_PATTERN = re.compile(r'（[^）]*）|\([^\)]*\)|【[^】]*】|\[[^\]]*\]|<[^>]*>')
        # 标点符号用作骨架切割的缝隙
        self.GAP_PATTERN = re.compile(r'[，,。．.；;！!？?：:“”"\'、\s\n\r_\\@·・·]+')
        self.CONTENT_PATTERN = re.compile(r'[\u4e00-\u9fa5a-zA-Z0-9]+')

    def _remove_brackets(self, text: str) -> str:
        """【核心方法】：去除文本中所有括号及内部内容，实现彻底的填空豁免"""
        if not text: return ""
        return self.BRACKET_PATTERN.sub('', text)

    def _normalize(self, text: str) -> str:
        """获取纯文字骨架"""
        if not text: return ""
        # 提取骨架前，先将括号里的内容一网打尽
        clean_text = self._remove_brackets(text)
        return "".join(self.CONTENT_PATTERN.findall(clean_text))

    def _get_anchors(self, template_text: str):
        """获取模板的固定锚点片段"""
        clean_template = self._remove_brackets(template_text)
        segments = self.GAP_PATTERN.split(clean_template)
        return [self._normalize(s) for s in segments if len(self._normalize(s)) >= 2]

    def _generate_diff_snippets(self, m_text: str, b_text: str):
        """核心高亮对比算法：提取上下文并标注删改内容"""
        # 对比前也先脱去括号内容，避免括号填空的正常差异被算作红绿篡改
        clean_m = self._remove_brackets(m_text)
        clean_b = self._remove_brackets(b_text)
        
        t1 = re.sub(r'\s+', '', clean_m)
        t2 = re.sub(r'\s+', '', clean_b)
        matcher = difflib.SequenceMatcher(None, t1, t2)
        
        snippets = []
        for tag, i1, i2, j1, j2 in matcher.get_opcodes():
            if tag in ('replace', 'delete'):
                deleted = t1[i1:i2]
                
                # 过滤无意义差异
                if not re.search(r'[\u4e00-\u9fa5a-zA-Z0-9]', deleted):
                    continue
                if len(self._normalize(deleted)) < 2: 
                    continue
                    
                ctx_start = max(0, i1 - 10)
                ctx_end = min(len(t1), i2 + 10)
                prefix = t1[ctx_start:i1]
                suffix = t1[i2:ctx_end]
                
                if tag == 'replace':
                    added = t2[j1:j2]
                    snippets.append(f"{prefix}\033[91m[-{deleted}-]\033[0m\033[92m[+{added}+]\033[0m{suffix}")
                elif tag == 'delete':
                    snippets.append(f"{prefix}\033[91m[-{deleted} (恶意删减)-]\033[0m{suffix}")
                    
        return snippets

    def compare_raw_data(self, model_raw_json, test_raw_json):
        m_segs = DocumentProcessor.extract_segments(model_raw_json)
        b_segs = DocumentProcessor.extract_segments(test_raw_json)
        if not m_segs or not b_segs: return []

        reports = []
        for i in range(len(MODULE_CONFIG)):
            m_text = m_segs[i]['text']
            b_text = b_segs[i]['text']
            
            # _normalize 内部已经包含了去除括号逻辑
            norm_bidder = self._normalize(b_text)
            anchors = self._get_anchors(m_text)
            
            issues, last_pos = [], 0
            for anchor in anchors:
                pos = norm_bidder.find(anchor, last_pos)
                if pos == -1: issues.append(anchor)
                else: last_pos = pos + len(anchor)
            
            is_passed = len(issues) == 0 and len(norm_bidder) > 0
            
            # 只有不过关时，才去生成高亮对比片段
            diff_snippets = self._generate_diff_snippets(m_text, b_text) if not is_passed else []

            reports.append({
                "index": i + 1,
                "name": MODULE_CONFIG[i]['title'],
                "is_passed": is_passed,
                "match_rate": (len(anchors) - len(issues)) / len(anchors) if anchors else 1.0,
                "diff_snippets": diff_snippets,
                "missing_segments": issues
            })
        return reports