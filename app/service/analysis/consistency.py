import re
import difflib
import json

# 1. 业务标准配置：定义 13 个核心附件的起止锚点
MODULE_CONFIG = [
    {"id": 1, "title": "投标保证书", "start": "投标保证书", "end": "通讯请寄"},
    {"id": 2, "title": "开标一览表", "start": "开标一览表", "end": "计算错误"},
    {"id": 3, "title": "商务条款偏离表", "start": "商务条款偏离表", "end": "对应投标文件所在页"}, 
    {"id": 4, "title": "技术条款偏离表", "start": "技术条款偏离表", "end": "对应材料投标文件所在页"},
    {"id": 5, "title": "投标人基本情况表", "start": "投标人基本情况", "end": "予以证实"},
    {"id": 6, "title": "类似项目业绩清单", "start": "类似项目业绩清单", "end": "履约期合同"},
    {"id": 7, "title": "法定代表人资格证明书", "start": "法定代表人资格证明书", "end": "负责人"},
    {"id": 8, "title": "法定代表人授权委托书", "start": "法定代表人授权委托书", "end": "转委托权"}, 
    {"id": 9, "title": "供应商承诺声明函", "start": "供应商承诺声明函", "end": "特此声明"},
    {"id": 10, "title": "不参与围标串标承诺书", "start": "不参与围标串标承诺书", "end": "特此承诺"},
    {"id": 11, "title": "拟派项目负责人情况表", "start": "拟派项目负责人情况表", "end": "须征得采购人同意"},
    {"id": 12, "title": "项目人员配置表", "start": "项目人员配置表", "end": "征得采购人同意"},
    {"id": 13, "title": "财务状况声明函", "start": "财务状况及税收", "end": "相应责任"}, 
]

class DocumentProcessor:
    """内存提取器"""
    @staticmethod
    def _get_full_text(raw_json_data):
        data_node = raw_json_data.get('data', raw_json_data)
        full_text = data_node.get('text') or data_node.get('content', '')
        if not full_text and 'layout_sections' in data_node:
            full_text = "\n".join([str(sec.get('text') or sec.get('content') or '') for sec in data_node['layout_sections'] if isinstance(sec, dict)])
        if not full_text and 'pages' in data_node:
            full_text = "\n".join([p.get('text', '') for p in data_node['pages'] if isinstance(p, dict)])
        return full_text

    @classmethod
    def extract_segments(cls, raw_json_data):
        full_text = cls._get_full_text(raw_json_data)
        if not full_text: return []

        extracted = []
        for cfg in MODULE_CONFIG:
            start_matches = list(re.finditer(re.escape(cfg['start']), full_text))
            text_content = ""
            
            if start_matches:
                for match in reversed(start_matches):
                    start_idx = match.start()
                    chunk = full_text[start_idx:start_idx+8000]
                    end_match = re.search(re.escape(cfg['end']), chunk, re.S)
                    if end_match:
                        text_content = chunk[:end_match.end()]
                        break
                
                if not text_content:
                    start_idx = start_matches[-1].start()
                    text_content = full_text[start_idx:start_idx+3000]

            extracted.append({"id": cfg['id'], "title": cfg['title'], "text": text_content.strip()})
        return extracted

class TemplateAnalysisService:
    """格式一致性审查服务"""
    def __init__(self):
        # 1. 括号正则：抹除诸如"(项目名称)"这样的占位符
        self.BRACKET_PATTERN = re.compile(r'（[^）]*）|\([^\)]*\)|【[^】]*】|\[[^\]]*\]|<[^>]*>')
        
        # 2. 缝隙正则：标点符号用作骨架切割的缝隙
        self.GAP_PATTERN = re.compile(r'[，,。．.；;！!？?：“”"\'、\s\n\r_\\@·・·]+')
        
        # 3. 切割正则：将标点、换行、冒号、括号等全当做模板切分的缝隙
        self.TEMPLATE_SPLIT_PATTERN = re.compile(
            r'（[^）]*）|\([^\)]*\)|【[^】]*】|\[[^\]]*\]|<[^>]*>|'  
            r'[:：]|'                                               
            r'_+|'                                                  
            r'[，,。．.；;！!？?：“”"\'、\s\n\r\\@·・·]+'           
        )
        
        # 4. 文字正则：保留中文和英文字母，剔除数字和特殊符号，构建纯文字骨架
        self.CONTENT_PATTERN = re.compile(r'[\u4e00-\u9fa5a-zA-Z]+')

        # 5. 近义词库
        self.SYNONYMS = {
            "投标承诺书": "投标保证书",
            "邀请招标": "公开招标",
            "询价": "公开招标",
            "竞争性谈判": "公开招标"
        }

    def _replace_synonyms(self, text: str) -> str:
        for syn, standard in self.SYNONYMS.items():
            text = text.replace(syn, standard)
        return text

    def _clean_text(self, text: str) -> str:
        """清洗逻辑：只去近义词、去括号和去下划线，保留所有正文文字"""
        if not text: return ""
        text = text.replace('（', '(').replace('）', ')').replace('：', ':')
        text = self._replace_synonyms(text)
        text = self.BRACKET_PATTERN.sub('', text)
        text = re.sub(r'_+', '', text)
        return text

    def _normalize(self, text: str) -> str:
        """获取全文本的无数字纯文字骨架"""
        if not text: return ""
        clean_text = self._clean_text(text)
        return "".join(self.CONTENT_PATTERN.findall(clean_text))

    def _get_anchors(self, template_text: str):
        """核心代码：按括号、冒号、下划线和标点全方位切割模板"""
        template_text = template_text.replace('（', '(').replace('）', ')').replace('：', ':')
        template_text = re.sub(r'(项目|编号|金额|日期|地址)', r' \1 ', template_text)
        template_text = self._replace_synonyms(template_text)
        segments = self.TEMPLATE_SPLIT_PATTERN.split(template_text)
        
        anchors = []
        for s in segments:
            norm_s = self._normalize(s)
            if len(norm_s) >= 2:
                anchors.append(norm_s)
        return anchors

    def _generate_diff_snippets(self, m_text: str, b_text: str):
        clean_m = self._clean_text(m_text)
        clean_b = self._clean_text(b_text)
        
        t1 = "".join(self.CONTENT_PATTERN.findall(clean_m))
        t2 = "".join(self.CONTENT_PATTERN.findall(clean_b))
        matcher = difflib.SequenceMatcher(None, t1, t2)
        
        snippets = []
        for tag, i1, i2, j1, j2 in matcher.get_opcodes():
            if tag in ('replace', 'delete'):
                deleted = t1[i1:i2]
                if len(deleted) < 2: continue
                    
                ctx_start = max(0, i1 - 6)
                ctx_end = min(len(t1), i2 + 6)
                prefix = t1[ctx_start:i1]
                suffix = t1[i2:ctx_end]
                
                if tag == 'replace':
                    added = t2[j1:j2]
                    snippets.append(f"...{prefix}\033[91m[-{deleted}-]\033[0m\033[92m[+{added}+]\033[0m{suffix}...")
                elif tag == 'delete':
                    snippets.append(f"...{prefix}\033[91m[-{deleted} (删减)-]\033[0m{suffix}...")
                    
        return snippets

    def compare_raw_data(self, model_raw_json, test_raw_json):
        m_segs = DocumentProcessor.extract_segments(model_raw_json)
        b_segs = DocumentProcessor.extract_segments(test_raw_json)
        if not m_segs or not b_segs: return []

        reports = []
        for i in range(len(MODULE_CONFIG)):
            m_text = m_segs[i]['text']
            b_text = b_segs[i]['text']
            
            norm_bidder = self._normalize(b_text)
            anchors = self._get_anchors(m_text)
            
            issues, last_pos = [], 0
            for anchor in anchors:
                pos = norm_bidder.find(anchor, last_pos)
                if pos == -1: 
                    issues.append(anchor)
                else: 
                    last_pos = pos + len(anchor)
            
            missing_count = len(issues)
            is_passed = (missing_count == 0) and (len(norm_bidder) > 0)

            reports.append({
                "index": i + 1,
                "name": MODULE_CONFIG[i]['title'],
                "is_passed": is_passed,
                "missing_segments_count": missing_count,    # 记录未匹配个数
                "missing_anchors": issues,                  # 记录具体缺失内容
                "debug_model_raw": m_text,
                "debug_model_anchors": anchors,
                "debug_bidder_raw": b_text,
                "debug_bidder_normalized": norm_bidder
            })
            
        return reports