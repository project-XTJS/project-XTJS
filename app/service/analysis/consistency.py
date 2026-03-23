import re
import json

# 1. 业务标准配置：定义 13 个核心附件的起止锚点
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
    """内部处理类：负责在内存中提取指定片段"""
    @staticmethod
    def _find_nth(text, pattern, n):
        matches = list(re.finditer(re.escape(pattern), text))
        return matches[n-1].start() if len(matches) >= n else -1

    @classmethod
    def extract_segments(cls, raw_json_data):
        """从原始 JSON 字典中提取内容"""
        full_text = raw_json_data.get('data', {}).get('content', '')
        if not full_text:
            pages = raw_json_data.get('data', {}).get('pages', [])
            full_text = "\n".join([p.get('text', '') for p in pages])

        # 定位正文：跳过目录区
        body_start = cls._find_nth(full_text, "商务标文件", 2)
        search_area = full_text[body_start:] if body_start != -1 else full_text

        extracted = []
        for cfg in MODULE_CONFIG:
            start_idx = cls._find_nth(search_area, cfg['start'], 2)
            if start_idx == -1:
                match = re.search(re.escape(cfg['start']), search_area)
                start_idx = match.start() if match else -1

            content = ""
            if start_idx != -1:
                chunk = search_area[start_idx:]
                end_match = re.search(re.escape(cfg['end']), chunk, re.S)
                content = chunk[:end_match.end()].strip() if end_match else chunk[:4000].strip()
            
            extracted.append({"id": cfg['id'], "title": cfg['title'], "text": content})
        return extracted

class TemplateAnalysisService:
    """一致性审查服务"""
    def __init__(self):
        # 骨架比对模式：豁免括号内容、忽略标点
        self.GAP_PATTERN = re.compile(r'（[^）]*）|\([^\)]*\)|[，,。．.；;！!？?：:【】\[\]“”"\'、\s\n\r_\\@<@>·・·]+')
        self.CONTENT_PATTERN = re.compile(r'[\u4e00-\u9fa5a-zA-Z0-9]+')

    def _normalize(self, text: str) -> str:
        if not text: return ""
        return "".join(self.CONTENT_PATTERN.findall(text))

    def _get_anchors(self, template_text: str):
        segments = self.GAP_PATTERN.split(template_text)
        return [self._normalize(s) for s in segments if len(self._normalize(s)) >= 2]

    def compare_raw_data(self, model_raw_json, test_raw_json):
        """输入原始 JSON，返回一致性报告"""
        m_segs = DocumentProcessor.extract_segments(model_raw_json)
        b_segs = DocumentProcessor.extract_segments(test_raw_json)

        reports = []
        for i in range(len(MODULE_CONFIG)):
            m_text = m_segs[i]['text']
            b_text = b_segs[i]['text']
            
            norm_bidder = self._normalize(b_text)
            anchors = self._get_anchors(m_text)
            
            issues, last_pos = [], 0
            for anchor in anchors:
                pos = norm_bidder.find(anchor, last_pos)
                if pos == -1: issues.append(anchor)
                else: last_pos = pos + len(anchor)
            
            match_rate = (len(anchors) - len(issues)) / len(anchors) if anchors else 1.0
            reports.append({
                "index": i + 1,
                "name": MODULE_CONFIG[i]['title'],
                "is_passed": len(issues) == 0,
                "match_rate": match_rate,
                "missing_segments": issues
            })
        return reports