import re
from .template_extractor import TemplateExtractor

class IntegrityChecker:
    """完整性校验器：检查必要章节是否缺失"""

    # 集中化同义词映射字典，便于维护和扩展
    SENSITIVE_MAPPING = {
        "基本情况": ["基本情况"],
        "类似项目业绩": ["类似项目业绩清单", "业绩证明"],
        "营业执照": ["营业执照", "经营许可"],
        "制造商声明函": ["制造商声明", "制造商授权", "原厂授权"],
        "原厂授权函": ["制造商声明", "制造商授权", "原厂授权"],
        "缴纳社保": ["社会保险个人权益记录", "社保缴纳证明", "劳动合同证明"]
    }

    def __init__(self):
        self.VALID_PREFIX = re.compile(r'^\s*([\(（]?附件|[一二三四五六七八九十]+[、.]|[\(（]?\d+[\)）\.、])')

    def _normalize_target(self, name: str) -> str:
        name = re.sub(r'^(\d+|[A-Z])[．\.]\s*', '', name)
        
        # 优先通过字典映射标准化名称
        for key, mapped_vals in self.SENSITIVE_MAPPING.items():
            if key in name: return mapped_vals[0]
            
        name = re.sub(r'^(参选人|投标人|应答人)(认为|的)?|可另外再附.*|后附.*材料$|[(（].*?[））]', '', name)
        return name.strip('。，；;,. ')

    def _smart_match(self, clean_text: str, keyword: str, clean_target: str) -> bool:
        """基于字典映射的智能模糊匹配"""
        if clean_target in clean_text: 
            return True
        
        # 检查同义词映射
        for key, aliases in self.SENSITIVE_MAPPING.items():
            if key in keyword:
                if any(alias in clean_text for alias in aliases):
                    return True
        return False

    def _find_heading(self, sections: list, headers: set, keyword: str) -> str:
        clean_target = keyword.replace(' ', '')
        EXEMPT_KEYWORDS = ["营业执照", "社会保险"] # 特例：这两类资质常见无编号或前缀，且具有较强的文本特征，可以放宽前缀要求

        for sec in sections:
            if sec.get('type') != 'heading': continue
            text = sec['text']
            if TemplateExtractor._is_noise(text, headers): continue
            
            clean_text = text.replace(' ', '')
            
            if self._smart_match(clean_text, keyword, clean_target):
                # 特殊资质可以不依赖前缀编号
                is_exempt = any(k in keyword for k in EXEMPT_KEYWORDS)
                if is_exempt or self.VALID_PREFIX.search(text):
                    return text
        return None

    def check_integrity(self, model_json: dict, test_json: dict) -> dict:
        # reqs 现在接收的是一个按文档物理顺序排列的单一列表 List[str]
        reqs = TemplateExtractor.extract_requirements(model_json)
        data_node = test_json.get('data', test_json)
        sections, headers = TemplateExtractor.preprocess_sections(data_node.get('layout_sections', []))
        
        all_details = {}
        # 直接遍历有序列表，字典 all_details 将天然保持原文档的主子层级顺序
        for item in reqs:
            # 动态判断当前项是子项还是主项
            is_sub = re.match(r'^[A-Z][．\.]|^[\(（]\d+[\)）]', item)
            cat = "资格证明子项" if is_sub else "商务标主项"

            # 特例处理：法定代表人证明书和授权委托书通常成对出现，且文本特征明显，单独设计逻辑进行匹配
            if "法定代表人" in item and "证明书" in item and "授权委托书" in item:
                zm_match, sq_match = None, None
                
                for sec in sections:
                    if sec.get('type') != 'heading': continue
                    text = sec['text']
                    if TemplateExtractor._is_noise(text, headers): continue
                    clean_text = text.replace(' ', '')
                    
                    if ("法定代表人" in clean_text or "法人" in clean_text) and "证明" in clean_text:
                        zm_match = text
                    if ("法定代表人" in clean_text or "法人" in clean_text or "委托" in clean_text) and "授权" in clean_text:
                        sq_match = text
                        
                if zm_match and sq_match:
                    status, preview, is_passed = "已找到", f"{zm_match} | {sq_match}", True
                elif zm_match:
                    status, preview, is_passed = "缺失授权委托书", zm_match, False
                elif sq_match:
                    status, preview, is_passed = "缺失证明书", sq_match, False
                else:
                    status, preview, is_passed = "缺失证明书及授权书", "-", False
                    
                all_details[item] = {
                    "status": status, "preview": preview, "is_passed": is_passed, "category": cat
                }
                continue

            norm_item = self._normalize_target(item)
            match = self._find_heading(sections, headers, norm_item)
            all_details[item] = {
                "status": "已找到" if match else "缺失",
                "preview": match or "-",
                "is_passed": bool(match) or "如有" in item,
                "category": cat
            }
                
        passed = len([v for v in all_details.values() if v['is_passed']])
        score = round((passed / len(all_details)) * 100, 2) if all_details else 0
        return {"integrity_score": score, "details": all_details}