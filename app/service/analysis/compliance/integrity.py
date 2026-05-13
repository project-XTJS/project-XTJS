# -*- coding: utf-8 -*-
"""
完整性校验模块。

检查投标文件中必要的章节、附件是否齐全。
依赖模板提取器获取要求列表，并基于标题和关键词匹配进行校验。
"""

import re
from typing import Any
from .template_extractor import TemplateExtractor


class IntegrityChecker:
    """完整性校验器：检查必要章节是否缺失。"""

    # 同义词映射：标准化项到别名的映射，用于智能匹配
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

    # 标题前缀模式（用于去除编号）
    PREFIX_PATTERNS = (
        r'^\s*(?:附件|附表)\s*[A-Z\d]+(?:\s*[-－]\s*[A-Z\d]+)*[、.)）．]?\s*',
        r'^\s*第[一二三四五六七八九十百零\d]+[章节部分篇项]\s*',
        r'^\s*(?:\d+|[A-Z]|[一二三四五六七八九十百零]+)[．\.、]\s*',
        r'^\s*[（(](?:\d+|[A-Z]|[一二三四五六七八九十百零]+)[）)]\s*',
        r'^\s*\d+[)）]\s*',
    )

    def __init__(self):
        # 预编译合法的标题前缀正则
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

    # 去标题前缀
    def _strip_heading_prefix(self, name: str) -> str:
        text = str(name or "").strip()
        previous = None
        while text and text != previous:
            previous = text
            for pattern in self.PREFIX_PATTERNS:
                text = re.sub(pattern, '', text).strip()
        return text.strip()

    # 文本归一化：仅保留字母数字和中文
    def _normalize_title_text(self, name: str) -> str:
        text = self._strip_heading_prefix(name)
        return ''.join(ch for ch in text if ch.isalnum() or '\u4e00' <= ch <= '\u9fff')

    # 根据关键词扩展候选标题列表
    def _candidate_titles(self, keyword: str) -> list[str]:
        keyword_norm = self._normalize_title_text(keyword)
        titles = [keyword]
        for key, aliases in self.SENSITIVE_MAPPING.items():
            key_norm = self._normalize_title_text(key)
            if keyword_norm == key_norm or keyword_norm in key_norm or key_norm in keyword_norm:
                titles.extend([key, *aliases])
                break
        return list(dict.fromkeys(titles))

    # 将任意标题归一化为标准描述
    def _normalize_target(self, name: str) -> str:
        stripped_name = self._strip_heading_prefix(name)
        normalized_name = self._normalize_title_text(stripped_name)

        # 优先通过字典映射到标准名称
        for key, mapped_vals in self.SENSITIVE_MAPPING.items():
            candidates = [key, *mapped_vals]
            if any(self._normalize_title_text(candidate) in normalized_name for candidate in candidates):
                return key

        # 移除类似于“参选人认为...”、“后附材料”等噪声
        stripped_name = re.sub(
            r'^(参选人|投标人|应答人)(认为|的)?|可另外再附.*|后附.*材料$|[(（].*?[））]',
            '',
            stripped_name,
        )
        return stripped_name.strip('。，；;,. ')

    # 基于字典的模糊匹配
    def _smart_match(self, text: str, keyword: str) -> bool:
        normalized_text = self._normalize_title_text(text)
        for candidate in self._candidate_titles(keyword):
            normalized_candidate = self._normalize_title_text(candidate)
            if normalized_candidate and normalized_candidate in normalized_text:
                return True
        return False

    # 判断是否为子项（如 A.、B. 或 (1) 等）
    def _is_sub_item(self, item: str) -> bool:
        return bool(re.match(
            r'^(?:[A-Z][．\.、]|[\(（](?:\d+|[A-Z]|[一二三四五六七八九十百零]+)[\)）])',
            item or "",
        ))

    # 判断是否为可选条目
    def _is_optional_item(self, item: str) -> bool:
        normalized = str(item or "").strip()
        # 完整性阶段只有标题本身带“如有”才允许缺失。
        return "如有" in normalized

    # 从 section 中提取位置信息
    def _location_from_section(self, section: dict | None) -> dict[str, Any] | None:
        if not isinstance(section, dict):
            return None
        bbox = section.get("bbox") or section.get("box")
        normalized_bbox = None
        if isinstance(bbox, (list, tuple)) and len(bbox) >= 4 and all(isinstance(item, (int, float)) for item in bbox[:4]):
            normalized_bbox = [int(round(float(item))) for item in bbox[:4]]
        page = section.get("page") if isinstance(section.get("page"), int) else None
        text = str(section.get("text") or "").strip()
        return {
            "page": page,
            "bbox": normalized_bbox,
            "text": text[:120] if text else "",
        }

    # 在区段列表中查找指定关键词的标题区段
    def _find_heading_section(self, sections: list, headers: set, keyword: str) -> dict | None:
        # 特例：营业执照和社保可以不依赖编号前缀
        EXEMPT_KEYWORDS = ["营业执照", "社会保险"]

        for sec in sections:
            if sec.get('type') != 'heading':
                continue
            text = sec['text']
            if TemplateExtractor._is_noise(text, headers, sec.get('type')):
                continue

            compact = re.sub(r'\s+', '', text)
            if self._smart_match(text, keyword):
                is_exempt = any(k in keyword for k in EXEMPT_KEYWORDS)
                is_short_text_title = sec.get('type') == 'text' and len(compact) <= 60
                if is_exempt or self.VALID_PREFIX.search(text) or is_short_text_title:
                    return sec
        return None

    # 主校验入口
    def check_integrity(self, model_json: dict, test_json: dict) -> dict:
        """
        根据招标文件模型检查投标文件的完整性。
        返回完整性评分、各项详情及位置信息。
        """
        reqs, attachment_mapping = TemplateExtractor.extract_requirements(model_json)
        data_node = test_json.get('data', test_json)
        sections, headers = TemplateExtractor.preprocess_sections(data_node.get('layout_sections', []))

        all_details = {}
        for item in reqs:
            is_sub = self._is_sub_item(item)
            cat = "资格证明子项" if is_sub else "商务标主项"

            # 每个附件单独判断，不再允许证明书/授权委托书互替，也不再做父子项放宽。
            norm_item = self._normalize_target(item)
            match_section = self._find_heading_section(sections, headers, norm_item)
            match = str(match_section.get("text") or "") if isinstance(match_section, dict) else None
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
                "locations": [self._location_from_section(match_section)] if match_section else [],
            }

        scored_details = [v for v in all_details.values() if v.get("scored", True)]
        passed = len([v for v in scored_details if v['is_passed']])
        total = len(scored_details)
        score = round((passed / total) * 100, 2) if total else 0

        return {
            "integrity_score": score,
            "details": all_details,
            "scored_item_count": total,
            "ignored_item_count": len(all_details) - total,
            "attachment_mapping": attachment_mapping,
        }
