# -*- coding: utf-8 -*-
"""
完整性校验模块。

检查投标文件中必要的章节、附件是否齐全。
依赖模板提取器获取要求列表，并基于标题和关键词匹配进行校验。
"""

import re
from typing import Any
from .template_extractor import TemplateExtractor
from ..attachment_synonyms import (
    attachment_title_variants,
    canonicalize_attachment_title,
)


class IntegrityChecker:
    """完整性校验器：检查必要章节是否缺失。"""

    # 同义词映射：标准化项到别名的映射，用于智能匹配
    SENSITIVE_MAPPING = {
        "基本情况": ["基本情况"],
        "类似项目业绩": ["类似项目业绩清单", "业绩证明"],
        "营业执照": ["营业执照", "经营许可"],
        "安全生产许可证": ["安全生产许可证", "有效的安全生产许可证"],
        "投标保证金": ["投标保证金", "保证金缴纳凭证", "投标保证金汇款凭证"],
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

    BODY_EVIDENCE_MAPPING = {
        "法定代表人资格证明书": [
            "法定代表人资格证明书",
            "法定代表人证明书",
            "法定代表人身份证明",
            "单位负责人身份证明",
        ],
        "法定代表人授权委托书": [
            "法定代表人授权委托书",
            "授权委托书",
            "委托代理人",
            "被授权人",
        ],
        "缴纳社保": [
            "被授权人社保缴纳证明",
            "社保缴纳证明",
            "社会保险个人权益记录",
            "劳动合同",
            "劳动合同书",
        ],
        "财务状况，依法缴纳税收和社会保障资金的声明函": [
            "财务状况",
            "税收",
            "社会保障资金",
            "声明函",
        ],
        "信用中国及中国裁判文书网证明材料": [
            "信用中国",
            "失信被执行人",
            "重大税收违法",
            "中国裁判文书网",
        ],
        "拟派项目经理有效的注册建造师、安全生产考核证书": [
            "项目经理",
            "注册建造师",
            "安全生产考核合格证书",
        ],
        "制造商声明函": ["制造商声明", "制造商授权", "原厂授权"],
        "原厂授权函": ["制造商声明", "制造商授权", "原厂授权"],
        "资格证明文件": [
            "营业执照",
            "法定代表人资格证明书",
            "法定代表人授权委托书",
            "被授权人社保缴纳证明",
            "劳动合同书",
            "承诺声明函",
            "不参与围标串标承诺书",
            "财务状况",
            "制造商声明函",
            "制造商授权书",
            "原厂授权",
        ],
    }

    COMPOSITE_REQUIREMENT_MARKERS = {
        "资格证明文件": {
            "markers": [
                "营业执照",
                "法定代表人资格证明书",
                "法定代表人授权委托书",
                "被授权人社保缴纳证明",
                "劳动合同书",
                "承诺声明函",
                "不参与围标串标承诺书",
                "财务状况",
                "制造商声明函",
                "制造商授权书",
                "原厂授权",
            ],
            "min_hits": 2,
        },
        "信用中国及中国裁判文书网证明材料": {
            "markers": [
                "失信被执行人",
                "重大税收违法",
                "中国裁判文书网",
            ],
            "min_hits": 2,
        },
        "拟派项目经理有效的注册建造师、安全生产考核证书": {
            "markers": [
                "项目经理",
                "注册建造师",
                "安全生产考核合格证书",
            ],
            "min_hits": 2,
        },
    }

    BODY_MATCH_FRAGMENTS = (
        "法定代表人",
        "资格证明",
        "身份证明",
        "授权委托书",
        "授权委托",
        "委托代理人",
        "被授权人",
        "营业执照",
        "社保缴纳",
        "社会保险",
        "劳动合同",
        "财务状况",
        "税收",
        "社会保障资金",
        "声明函",
        "承诺书",
        "制造商声明",
        "制造商授权",
        "原厂授权",
        "投标保证书",
        "偏离表",
        "报价表",
        "一览表",
        "信用中国",
        "失信被执行人",
        "裁判文书",
        "注册建造师",
        "安全生产考核",
    )

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
        titles = [keyword, *attachment_title_variants(keyword)]
        for key, aliases in self.SENSITIVE_MAPPING.items():
            key_norm = self._normalize_title_text(key)
            if keyword_norm == key_norm or keyword_norm in key_norm or key_norm in keyword_norm:
                titles.extend([key, *aliases])
                break
        return list(dict.fromkeys(titles))

    def _body_evidence_titles(self, keyword: str) -> list[str]:
        keyword_norm = self._normalize_title_text(keyword)
        titles = [
            keyword,
            canonicalize_attachment_title(keyword),
            self._normalize_target(keyword),
            *self._candidate_titles(keyword),
        ]
        for key, aliases in self.BODY_EVIDENCE_MAPPING.items():
            key_norm = self._normalize_title_text(key)
            if keyword_norm == key_norm or keyword_norm in key_norm or key_norm in keyword_norm:
                titles.extend([key, *aliases])
                break

        cleaned_titles = []
        for title in titles:
            clean = re.sub(r"[（(][^()（）]{0,40}[）)]", "", self._strip_heading_prefix(title)).strip()
            if clean and clean not in cleaned_titles:
                cleaned_titles.append(clean)
        return cleaned_titles

    def _body_evidence_parts(self, candidate: str) -> list[str]:
        normalized_candidate = self._normalize_title_text(candidate)
        if not normalized_candidate:
            return []

        parts = []
        split_parts = re.split(r"[、，,；;：:\s/]+", candidate)
        for part in split_parts:
            normalized_part = self._normalize_title_text(part)
            if len(normalized_part) >= 2 and normalized_part not in parts:
                parts.append(normalized_part)

        for fragment in self.BODY_MATCH_FRAGMENTS:
            normalized_fragment = self._normalize_title_text(fragment)
            if normalized_fragment and normalized_fragment in normalized_candidate and normalized_fragment not in parts:
                parts.append(normalized_fragment)

        if normalized_candidate not in parts:
            parts.append(normalized_candidate)
        return parts

    # 将任意标题归一化为标准描述
    def _normalize_target(self, name: str) -> str:
        stripped_name = self._strip_heading_prefix(name)
        stripped_name = canonicalize_attachment_title(stripped_name)
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
        compact = re.sub(r"\s+", "", normalized)
        return (
            "如有" in normalized
            or "认为需要补充" in compact
            or ("其他内容" in compact and "前附表规定" in compact)
        )

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
                is_short_heading_title = sec.get('type') == 'heading' and len(compact) <= 36
                if is_exempt or self.VALID_PREFIX.search(text) or is_short_text_title or is_short_heading_title:
                    return sec
        return None

    # 第二遍只回查 text 区段，并跳过目录页，避免把目录项当成正文附件
    def _collect_toc_pages(self, sections: list) -> set[int]:
        toc_pages: set[int] = set()
        toc_line_counts: dict[int, int] = {}

        for sec in sections:
            if not isinstance(sec, dict):
                continue
            text = str(sec.get("text") or "").strip()
            page = sec.get("page")
            if not text or not isinstance(page, int):
                continue

            compact = re.sub(r"\s+", "", text)
            if compact == "目录" or (compact.startswith("目录") and len(compact) <= 8):
                toc_pages.add(page)

            if TemplateExtractor._is_noise(text, set(), sec.get("type")):
                toc_line_counts[page] = toc_line_counts.get(page, 0) + 1

        for page, count in toc_line_counts.items():
            if count >= 3:
                toc_pages.add(page)

        return toc_pages

    def _is_usable_body_section(
        self, section: dict, headers: set, toc_pages: set[int]
    ) -> bool:
        if not isinstance(section, dict):
            return False
        if section.get("page") in toc_pages:
            return False
        if section.get("type") not in {"heading", "text", "table"}:
            return False
        text = str(section.get("text") or "")
        if not text:
            return False
        if TemplateExtractor._is_noise(text, headers, section.get("type")):
            return False
        return True

    def _content_match_score(
        self, text: str, keyword: str
    ) -> tuple[int, str | None, list[str]]:
        normalized_text = self._normalize_title_text(text)
        if not normalized_text:
            return 0, None, []

        best_score = 0
        best_title = None
        best_hits: list[str] = []
        for candidate in self._body_evidence_titles(keyword):
            normalized_candidate = self._normalize_title_text(candidate)
            if not normalized_candidate:
                continue
            if len(normalized_candidate) >= 4 and normalized_candidate in normalized_text:
                score = 100 + min(len(normalized_candidate), 20)
                if score > best_score:
                    best_score = score
                    best_title = candidate
                    best_hits = [candidate]
                continue

            parts = self._body_evidence_parts(candidate)
            hits = []
            for part in parts:
                if part and part in normalized_text and part not in hits:
                    hits.append(part)
            longest_hit = max((len(part) for part in hits), default=0)
            if len(hits) >= 2:
                score = 60 + len(hits) * 10 + min(longest_hit, 20)
            elif longest_hit >= 6:
                score = 40 + min(longest_hit, 20)
            else:
                continue

            if score > best_score:
                best_score = score
                best_title = candidate
                best_hits = hits

        return best_score, best_title, best_hits

    def _find_body_section(
        self, sections: list, headers: set, keyword: str, toc_pages: set[int]
    ) -> dict | None:
        best_section = None
        best_score = 0
        best_match_title = None
        best_hits: list[str] = []

        for sec in sections:
            if not self._is_usable_body_section(sec, headers, toc_pages):
                continue
            score, matched_title, hits = self._content_match_score(
                str(sec.get("text") or ""), keyword
            )
            if score <= best_score:
                continue
            best_section = sec
            best_score = score
            best_match_title = matched_title
            best_hits = hits

        if best_section is None or best_score < 60:
            return None

        matched = dict(best_section)
        matched["match_mode"] = "body"
        if best_match_title:
            matched["matched_keyword"] = best_match_title
        if best_hits:
            matched["matched_parts"] = best_hits
        return matched

    def _find_composite_body_section(
        self, sections: list, headers: set, keyword: str, toc_pages: set[int]
    ) -> dict | None:
        keyword_norm = self._normalize_title_text(keyword)
        matched_profile = None
        for profile_key, profile in self.COMPOSITE_REQUIREMENT_MARKERS.items():
            profile_key_norm = self._normalize_title_text(profile_key)
            if (
                keyword_norm == profile_key_norm
                or keyword_norm in profile_key_norm
                or profile_key_norm in keyword_norm
            ):
                matched_profile = profile
                break
        if matched_profile is None:
            return None

        marker_hits: dict[str, dict] = {}
        for sec in sections:
            if not self._is_usable_body_section(sec, headers, toc_pages):
                continue
            normalized_text = self._normalize_title_text(str(sec.get("text") or ""))
            if not normalized_text:
                continue
            for marker in matched_profile.get("markers") or []:
                normalized_marker = self._normalize_title_text(marker)
                if normalized_marker and normalized_marker in normalized_text and marker not in marker_hits:
                    marker_hits[marker] = sec

        min_hits = int(matched_profile.get("min_hits") or 1)
        if len(marker_hits) < min_hits:
            return None

        preview_hits = list(marker_hits.keys())[:4]
        first_section = next(iter(marker_hits.values()))
        synthetic = {
            "type": "text",
            "page": first_section.get("page"),
            "text": f"正文聚合命中：{', '.join(preview_hits)}",
            "match_mode": "composite_body",
            "matched_parts": list(marker_hits.keys()),
        }
        if first_section.get("bbox") is not None:
            synthetic["bbox"] = first_section.get("bbox")
        return synthetic

    # text 回查只接受标题样式的短文本，不把正文句子误当成附件标题
    def _looks_like_text_title(self, text: str, keyword: str) -> bool:
        compact = re.sub(r"\s+", "", str(text or ""))
        if not compact or len(compact) > 80:
            return False
        if any(mark in compact for mark in ("根据", "提交", "详见", "说明如下", "应提供", "应附", "后附", "附后")):
            return False
        if any(mark in text for mark in ("。", "；", ";")):
            return False

        normalized_text = self._normalize_title_text(text)
        candidate_lengths = [
            len(candidate_norm)
            for candidate in self._candidate_titles(keyword)
            if (candidate_norm := self._normalize_title_text(candidate))
            and candidate_norm in normalized_text
        ]
        if not candidate_lengths:
            return False

        matched_length = max(candidate_lengths)
        return len(normalized_text) <= max(matched_length + 24, matched_length * 2)

    # heading 没找到时，再按附件名回查 text，确认是否只是 OCR 把标题切成了正文
    def _find_text_section(self, sections: list, headers: set, keyword: str, toc_pages: set[int]) -> dict | None:
        for sec in sections:
            if sec.get("type") != "text":
                continue
            if sec.get("page") in toc_pages:
                continue

            text = str(sec.get("text") or "")
            if not text:
                continue
            if TemplateExtractor._is_noise(text, headers, sec.get("type")):
                continue
            if not self._looks_like_text_title(text, keyword):
                continue
            return sec
        return None

    # 完整性检查先认 heading，只有缺失项才做 text 二次确认
    def _find_required_section(self, sections: list, headers: set, keyword: str, toc_pages: set[int]) -> dict | None:
        match_section = self._find_heading_section(sections, headers, keyword)
        if match_section:
            return match_section
        match_section = self._find_text_section(sections, headers, keyword, toc_pages)
        if match_section:
            return match_section
        match_section = self._find_composite_body_section(sections, headers, keyword, toc_pages)
        if match_section:
            return match_section
        return self._find_body_section(sections, headers, keyword, toc_pages)

    # 主校验入口
    def check_integrity(self, model_json: dict, test_json: dict) -> dict:
        """
        根据招标文件模型检查投标文件的完整性。
        返回完整性评分、各项详情及位置信息。
        """
        reqs, attachment_mapping = TemplateExtractor.extract_requirements(model_json)
        data_node = test_json.get('data', test_json)
        sections, headers = TemplateExtractor.preprocess_sections(data_node.get('layout_sections', []))
        toc_pages = self._collect_toc_pages(sections)

        all_details = {}
        for item in reqs:
            is_sub = self._is_sub_item(item)
            cat = "资格证明子项" if is_sub else "商务标主项"

            # 每个附件单独判断，不再允许证明书/授权委托书互替，也不再做父子项放宽。
            norm_item = self._normalize_target(item)
            match_section = self._find_required_section(sections, headers, norm_item, toc_pages)
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
