import re
from .template_extractor import TemplateExtractor

class IntegrityChecker:
    """针对结构化标题、逻辑依赖及特殊业务规则的投标文件完整性检查器"""

    def __init__(self):
        self.ATTACHMENT_PREFIX = re.compile(r'^\s*[\(（]?附件')

    def _promote_sections(self, sections: list) -> list:
        """同上的页码提权容错逻辑"""
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

    def _normalize_target_name(self, item_name: str) -> str:
        """提炼核心词，用于标题匹配"""
        if "基本情况" in item_name: return "基本情况"
        if "类似项目业绩清单" in item_name: return "类似项目业绩清单"
        if "财务状况" in item_name and "社会保障" in item_name: return "社会保障资金缴纳情况声明函"
        if "营业执照" in item_name: return "营业执照"
            
        item_name = re.sub(r'^(参选人的|投标人的|应答人的|参选人认为|投标人认为|应答人认为|参选人|投标人|应答人)', '', item_name)
        item_name = re.sub(r'(可另外再附.*|后附.*材料)$', '', item_name)
        item_name = re.sub(r'[（\(].*?(公章|原件|复印件|如有|格式).*?[）\)]', '', item_name)
        return item_name.strip('。，；;,. ')

    def _find_heading(self, sections: list, target_keyword: str) -> str:
        """在 headings 中寻找核心关键词"""
        is_license = "营业执照" in target_keyword
        for sec in sections:
            if not isinstance(sec, dict) or sec.get('type') != 'heading':
                continue
            text = str(sec.get('text') or '').strip()
            
            if is_license:
                if target_keyword in text: return text
            else:
                if self.ATTACHMENT_PREFIX.search(text) and target_keyword in text:
                    return text
        return None

    def check_integrity(self, model_raw_json: dict, test_raw_json: dict) -> dict:
        reqs = TemplateExtractor.extract_requirements(model_raw_json)
        test_data = test_raw_json.get('data', {})
        # 🌟 核心：注入页面提权过滤
        sections = self._promote_sections(test_data.get('layout_sections', []))

        sub_details = {}
        for sub_item in reqs['sub']:
            clean_name = self._normalize_target_name(sub_item)
            
            if "法定代表人" in sub_item:
                targets = ["法定代表人授权委托书", "法定代表人资格证明书"]
                found_parts = []
                missing_parts = []
                for t in targets:
                    match = self._find_heading(sections, t)
                    if match: found_parts.append(match)
                    else: missing_parts.append(t)
                
                if not missing_parts:
                    sub_details[sub_item] = {"status": "已找到", "preview": " + ".join(found_parts), "is_passed": True}
                else:
                    missing_desc = f"部分缺失 (缺: {', '.join(missing_parts)})" if found_parts else "缺失"
                    sub_details[sub_item] = {
                        "status": missing_desc, 
                        "preview": " + ".join(found_parts) if found_parts else "-",
                        "is_passed": False
                    }
            else:
                match = self._find_heading(sections, clean_name)
                if match:
                    sub_details[sub_item] = {"status": "已找到", "preview": match, "is_passed": True}
                else:
                    sub_details[sub_item] = {"status": "缺失", "preview": "-", "is_passed": False}

        main_details = {}
        for main_item in reqs['main']:
            if "资格" in main_item or "证明文件" in main_item:
                missing_subs = [self._normalize_target_name(s) for s, d in sub_details.items() if not d['is_passed']]
                if not missing_subs:
                    main_details[main_item] = {"status": "已找到", "preview": "所有子项均已通过校验", "is_passed": True}
                else:
                    main_details[main_item] = {
                        "status": f"不全 (缺: {', '.join(missing_subs)})", 
                        "preview": "部分子项缺失或内容不合规",
                        "is_passed": False
                    }
            else:
                clean_name = self._normalize_target_name(main_item)
                match = self._find_heading(sections, clean_name)
                if match:
                    main_details[main_item] = {"status": "已找到", "preview": match, "is_passed": True}
                else:
                    if any(kw in main_item for kw in ["其他内容", "如有"]):
                        main_details[main_item] = {"status": "未提供 (可选附加项)", "preview": "-", "is_passed": True}
                    else:
                        main_details[main_item] = {"status": "缺失", "preview": "-", "is_passed": False}

        all_details = {}
        found_sections = []
        missing_sections = []

        for name, info in main_details.items():
            all_details[name] = {**info, "category": "商务标主项文件"}
            if info['is_passed']: found_sections.append(name)
            else: missing_sections.append(name)

        for name, info in sub_details.items():
            key = f"子项: {name}"
            all_details[key] = {**info, "category": "资格证明子项材料"}
            if info['is_passed']: found_sections.append(key)
            else: missing_sections.append(key)

        total_items = len(reqs['main']) + len(reqs['sub'])
        passed_items = len([v for v in all_details.values() if v.get('is_passed')])
        score = round((passed_items / total_items) * 100, 2) if total_items > 0 else 100.0

        return {
            "integrity_score": score,
            "found_count": passed_items,
            "missing_count": len(missing_sections),
            "found_sections": found_sections, 
            "missing_sections": missing_sections,
            "details": all_details,
        }