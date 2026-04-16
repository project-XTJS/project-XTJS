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
        "缴纳社保": ["社会保险个人权益记录", "社保缴纳证明", "劳动合同证明"],
        "财务状况，依法缴纳税收和社会保障资金的声明函": [
            "财务状况，依法缴纳税收和社会保障资金的声明函",
            "财务状况及税收、社会保障资金缴纳情况声明函",
            "财务状况及税收和社会保障资金缴纳情况声明函",
            "依法缴纳税收和社会保障资金的声明函",
            "社会保障资金缴纳情况声明函",
        ],
    }
    OPTIONAL_ITEM_KEYWORDS = (
        "参选人认为需加以说明的其他内容",
        "投标人认为需加以说明的其他内容",
        "应答人认为需加以说明的其他内容",
    )
    PREFIX_PATTERNS = (
        r'^\s*(?:附件|附表)\s*[A-Z\d]+(?:\s*[-－]\s*[A-Z\d]+)*[、.)）．]?\s*',
        r'^\s*第[一二三四五六七八九十百零\d]+[章节部分篇项]\s*',
        r'^\s*(?:\d+|[A-Z]|[一二三四五六七八九十百零]+)[．\.、]\s*',
        r'^\s*[（(](?:\d+|[A-Z]|[一二三四五六七八九十百零]+)[）)]\s*',
        r'^\s*\d+[)）]\s*',
    )

    def __init__(self):
        self.VALID_PREFIX = re.compile(
            r'^\s*(?:'
            r'(?:附件|附表)\s*[A-Z\d]+(?:\s*[-－]\s*[A-Z\d]+)*'
            r'|第[一二三四五六七八九十百零\d]+[章节部分篇项]'
            r'|[A-Z][．\.、]'
            r'|[一二三四五六七八九十百零]+[、．\.]'
            r'|[（(](?:\d+|[A-Z]|[一二三四五六七八九十百零]+)[）)]'
            r'|\d+[)）\.、]'
            r')'
        )

    def _strip_heading_prefix(self, name: str) -> str:
        text = str(name or "").strip()
        previous = None
        while text and text != previous:
            previous = text
            for pattern in self.PREFIX_PATTERNS:
                text = re.sub(pattern, '', text).strip()
        return text.strip()

    def _normalize_title_text(self, name: str) -> str:
        text = self._strip_heading_prefix(name)
        return ''.join(ch for ch in text if ch.isalnum() or '\u4e00' <= ch <= '\u9fff')

    def _candidate_titles(self, keyword: str) -> list[str]:
        keyword_norm = self._normalize_title_text(keyword)
        titles = [keyword]
        for key, aliases in self.SENSITIVE_MAPPING.items():
            key_norm = self._normalize_title_text(key)
            if keyword_norm == key_norm or keyword_norm in key_norm or key_norm in keyword_norm:
                titles.extend([key, *aliases])
                break
        return list(dict.fromkeys(titles))

    def _normalize_target(self, name: str) -> str:
        stripped_name = self._strip_heading_prefix(name)
        normalized_name = self._normalize_title_text(stripped_name)

        # 优先通过标题主体做标准化，而不是依赖附件编号或条目编号
        for key, mapped_vals in self.SENSITIVE_MAPPING.items():
            candidates = [key, *mapped_vals]
            if any(self._normalize_title_text(candidate) in normalized_name for candidate in candidates):
                return key

        stripped_name = re.sub(r'^(参选人|投标人|应答人)(认为|的)?|可另外再附.*|后附.*材料$|[(（].*?[））]', '', stripped_name)
        return stripped_name.strip('。，；;,. ')

    def _smart_match(self, text: str, keyword: str) -> bool:
        """基于字典映射的智能模糊匹配"""
        normalized_text = self._normalize_title_text(text)
        for candidate in self._candidate_titles(keyword):
            normalized_candidate = self._normalize_title_text(candidate)
            if normalized_candidate and normalized_candidate in normalized_text:
                return True
        return False

    def _is_sub_item(self, item: str) -> bool:
        return bool(re.match(r'^(?:[A-Z][．\.、]|[\(（](?:\d+|[A-Z]|[一二三四五六七八九十百零]+)[\)）])', item or ""))

    def _is_optional_item(self, item: str) -> bool:
        normalized = str(item or "").strip()
        return "如有" in normalized or any(keyword in normalized for keyword in self.OPTIONAL_ITEM_KEYWORDS)

    def _apply_parent_relaxation(self, ordered_items: list[str], details: dict[str, dict]) -> None:
        current_main = None
        child_items: list[str] = []

        def finalize_current() -> None:
            if not current_main or not child_items:
                return
            main_detail = details.get(current_main) or {}
            child_all_passed = all((details.get(child) or {}).get("is_passed") for child in child_items)
            details[current_main] = {
                **main_detail,
                "status": "子附件齐全" if child_all_passed else "子附件不齐全",
                "preview": "；".join(child_items),
                "is_passed": child_all_passed,
                "scored": False,
                "covered_by_child_items": True,
                "child_items": child_items[:],
            }

        for item in ordered_items:
            if self._is_sub_item(item):
                if current_main is not None:
                    child_items.append(item)
                continue
            finalize_current()
            current_main = item
            child_items = []

        finalize_current()

    def _find_heading(self, sections: list, headers: set, keyword: str) -> str:
        EXEMPT_KEYWORDS = ["营业执照", "社会保险"] # 特例：这两类资质常见无编号或前缀，且具有较强的文本特征，可以放宽前缀要求

        for sec in sections:
            if sec.get('type') != 'heading': continue
            text = sec['text']
            if TemplateExtractor._is_noise(text, headers, sec.get('type')): continue

            compact = re.sub(r'\s+', '', text)
            if self._smart_match(text, keyword):
                # 特殊资质可以不依赖前缀编号
                is_exempt = any(k in keyword for k in EXEMPT_KEYWORDS)
                is_short_text_title = sec.get('type') == 'text' and len(compact) <= 60
                if is_exempt or self.VALID_PREFIX.search(text) or is_short_text_title:
                    return text
        return None

    def check_integrity(self, model_json: dict, test_json: dict) -> dict:
        # reqs 现在接收的是一个按文档物理顺序排列的单一列表 List[str]
        reqs, attachment_mapping = TemplateExtractor.extract_requirements(model_json)
        data_node = test_json.get('data', test_json)
        sections, headers = TemplateExtractor.preprocess_sections(data_node.get('layout_sections', []))
        
        all_details = {}
        # 直接遍历有序列表，字典 all_details 将天然保持原文档的主子层级顺序
        for item in reqs:
            # 动态判断当前项是子项还是主项
            is_sub = self._is_sub_item(item)
            cat = "资格证明子项" if is_sub else "商务标主项"

            # 特例处理：法定代表人证明书和授权委托书通常成对出现，且文本特征明显，单独设计逻辑进行匹配
            if "法定代表人" in item and "证明书" in item and "授权委托书" in item:
                zm_match, sq_match = None, None
                
                for sec in sections:
                    if sec.get('type') != 'heading': continue
                    text = sec['text']
                    if TemplateExtractor._is_noise(text, headers, sec.get('type')): continue
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
                    "status": status, "preview": preview, "is_passed": is_passed, "category": cat, "scored": True
                }
                continue

            norm_item = self._normalize_target(item)
            match = self._find_heading(sections, headers, norm_item)
            is_optional = self._is_optional_item(item)
            all_details[item] = {
                "status": (
                    "已找到"
                    if match
                    else ("可选项未提供" if is_optional else "缺失")
                ),
                "preview": match or "-",
                "is_passed": bool(match) or is_optional,
                "category": cat,
                "scored": True,
            }

        self._apply_parent_relaxation(reqs, all_details)
        scored_details = [v for v in all_details.values() if v.get("scored", True)]
        passed = len([v for v in scored_details if v['is_passed']])
        total = len(scored_details)
        score = round((passed / total) * 100, 2) if total else 0
        return {
            "integrity_score": score,
            "details": all_details,
            "scored_item_count": total,
            "ignored_item_count": len(all_details) - total,
            "attachment_mapping": attachment_mapping,  # 新增字段
        }
