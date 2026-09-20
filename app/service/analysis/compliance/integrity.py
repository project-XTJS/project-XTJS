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
from ..verification import VerificationChecker


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
        "缴纳社保": [
            "社会保险个人权益记录",
            "社会保险缴费记录",
            "社会保险缴纳证明",
            "社会保险缴费证明",
            "社保缴纳证明",
            "社保缴纳记录",
            "社保证明",
            "社保证明材料",
            "被授权人社保缴纳证明",
            "被授权人社保缴纳记录",
            "授权代表社保缴纳证明",
            "授权代表社保缴纳记录",
            "个人参保证明",
            "参保证明",
            "劳动合同证明",
            "劳动合同",
            "劳动合同书",
            "被授权人的劳动合同书",
            "聘用合同",
            "退休证",
        ],
        "财务状况，依法缴纳税收和社会保障资金的声明函": [
            "财务状况，依法缴纳税收和社会保障资金的声明函",
            "财务状况及税收、社会保障资金缴纳情况声明函",
            "财务状况及税收和社会保障资金缴纳情况声明函",
            "财务状况及税收、社会保障资金缴纳情况、没有重大违法记录声明函",
            "财务状况及税收和社会保障资金缴纳情况、没有重大违法记录声明函",
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
            "授权代表",
        ],
        "缴纳社保": [
            "被授权人社保缴纳证明",
            "被授权人社保缴纳记录",
            "授权代表社保缴纳证明",
            "授权代表社保缴纳记录",
            "社保缴纳证明",
            "社保缴纳记录",
            "社保证明",
            "社保证明材料",
            "社会保险个人权益记录",
            "社会保险缴费记录",
            "社会保险缴纳证明",
            "社会保险缴费证明",
            "个人参保证明",
            "参保证明",
            "劳动合同",
            "劳动合同书",
            "聘用合同",
            "退休证",
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
            "授权代表社保缴纳记录",
            "社会保险个人权益记录",
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
                "授权代表社保缴纳记录",
                "社会保险个人权益记录",
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
        "授权代表",
        "被授权人",
        "营业执照",
        "社保缴纳",
        "社保缴费",
        "社保缴纳记录",
        "社保证明",
        "社会保险",
        "社会保险个人权益记录",
        "社会保险缴费记录",
        "劳动合同",
        "聘用合同",
        "退休证",
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

    LEGAL_REP_PROOF_TARGET_MARKERS = (
        "法定代表人资格证明书",
        "法定代表人证明书",
        "法定代表人身份证明",
        "单位负责人证明书",
        "单位负责人身份证明",
    )

    LEGAL_REP_PROOF_EVIDENCE_MARKERS = (
        "法定代表人资格证明书",
        "法定代表人证明书",
        "法定代表人身份证明",
        "法定代表人证明",
        "单位负责人证明书",
        "单位负责人身份证明",
        "法定代表人或单位负责人证明书",
        "法定代表人单位负责人证明书",
    )

    SOCIAL_SECURITY_TARGET_MARKERS = (
        "缴纳社保",
        "社保缴纳",
        "社保证明",
        "社会保险",
        "劳动合同",
        "聘用合同",
        "退休证",
    )

    SOCIAL_SECURITY_ROLE_MARKERS = (
        "被授权人",
        "授权代表",
        "授权委托人",
        "委托代理人",
        "代理人",
    )

    SOCIAL_SECURITY_EVIDENCE_MARKERS = (
        "被授权人社保缴纳证明",
        "被授权人社保缴纳记录",
        "授权代表社保缴纳证明",
        "授权代表社保缴纳记录",
        "社保缴纳证明",
        "社保缴纳记录",
        "社保缴费证明",
        "社保缴费记录",
        "社保证明材料",
        "社保证明",
        "社会保险个人权益记录",
        "社会保险缴费记录",
        "社会保险缴纳证明",
        "社会保险缴费证明",
        "个人权益记录",
        "个人参保证明",
        "参保证明",
        "劳动合同证明",
        "劳动合同书",
        "劳动合同",
        "聘用合同",
        "退休证",
    )

    # 标题前缀模式（用于去除编号）
    PREFIX_PATTERNS = (
        r'^\s*(?:附件|附表)\s*[A-Z\d]+(?:\s*[-－]\s*[A-Z\d]+)*[、.)）．]?\s*',
        r'^\s*第[一二三四五六七八九十百零\d]+[章节部分篇项]\s*',
        r'^\s*\d+(?:\s*[.．]\s*\d+)+[.．、]?\s*',
        r'^\s*(?:\d+|[A-Z]|[一二三四五六七八九十百零]+)[．\.、]\s*',
        r'^\s*[（(](?:\d+|[A-Z]|[一二三四五六七八九十百零]+)[）)]\s*',
        r'^\s*\d+[)）]\s*',
    )

    # 要求项开头的引导动词（“提供X承诺书”→“X承诺书”），不参与标题判定
    LEADING_TITLE_VERB_RE = re.compile(
        r'^(?:需提供|须提供|应提供|应附|后附|提供|提交|具备|具有|出具|开具|携带)\s*'
    )

    def __init__(self):
        # 用于完整性命中时的“标题 key 兼容/词法相似度”判定（覆盖“或”备选、同义、编号差异）。
        self._verification_checker = VerificationChecker(None)
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
        text = re.sub(r"\\([.．、)）])", r"\1", text)
        previous = None
        while text and text != previous:
            previous = text
            for pattern in self.PREFIX_PATTERNS:
                text = re.sub(pattern, '', text).strip()
        return text.strip()

    # 文本归一化：仅保留字母数字和中文
    def _normalize_title_text(self, name: str) -> str:
        text = self._strip_heading_prefix(name)
        # 大标题后括号内容不参与标题存在性判断：
        # “中小企业声明函（工程）”与“中小企业声明函（格式）”视同同一标题。
        text = self._strip_parenthetical_content(text)
        return ''.join(ch for ch in text if ch.isalnum() or '\u4e00' <= ch <= '\u9fff')

    @staticmethod
    def _strip_parenthetical_content(text: str) -> str:
        """迭代剥离全/半角括号内容，嵌套括号也能完全去除（如“声明函（工程（一））”）。"""
        pattern = re.compile(r"[（(][^（）()]*[）)]")
        while True:
            stripped = pattern.sub("", text)
            if stripped == text:
                return text
            text = stripped

    # 根据关键词扩展候选标题列表
    def _candidate_titles(self, keyword: str) -> list[str]:
        keyword_norm = self._normalize_title_text(keyword)
        titles = [keyword, *attachment_title_variants(keyword)]
        for key, aliases in self.SENSITIVE_MAPPING.items():
            key_norm = self._normalize_title_text(key)
            if keyword_norm == key_norm or keyword_norm in key_norm or key_norm in keyword_norm:
                titles.extend([key, *aliases])
                break
        # 合并要求项（如“中小企业声明函（格式） 中小企业声明函（工程）”）：
        # 整串无法命中时，按空格拆出的子标题（通常是大标题的括号变体）也参与匹配。
        for part in self._keyword_parts(keyword):
            part_norm = self._normalize_title_text(part)
            if part_norm and part_norm != keyword_norm:
                titles.append(part)
                titles.extend(attachment_title_variants(part))
        # 要求项带“提供/提交”等引导动词时，去动词后的干净标题也作为候选：
        # 招标“提供强制采购节能产品承诺书（格式）”可命中投标“强制采购节能产品承诺书”。
        verb_stripped = self.LEADING_TITLE_VERB_RE.sub('', str(keyword or "").strip()).strip()
        if verb_stripped and self._normalize_title_text(verb_stripped) != keyword_norm:
            titles.append(verb_stripped)
            titles.extend(attachment_title_variants(verb_stripped))
        return list(dict.fromkeys(titles))

    @staticmethod
    def _keyword_parts(keyword: str) -> list[str]:
        """把合并要求项按空白拆成多个子标题（如“声明函（格式） 声明函（工程）”）。"""
        parts: list[str] = []
        for chunk in re.split(r"\s+", str(keyword or "").strip()):
            chunk = chunk.strip("。；，,;:.．、 ")
            if chunk and chunk not in parts:
                parts.append(chunk)
        return parts

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
            clean = self._strip_parenthetical_content(self._strip_heading_prefix(title)).strip()
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

    def _is_legal_representative_proof_target(self, keyword: str) -> bool:
        normalized_keyword = self._normalize_title_text(keyword)
        return any(
            self._normalize_title_text(marker) in normalized_keyword
            for marker in self.LEGAL_REP_PROOF_TARGET_MARKERS
        )

    def _is_social_security_target(self, keyword: str) -> bool:
        normalized_keyword = self._normalize_title_text(keyword)
        return any(
            self._normalize_title_text(marker) in normalized_keyword
            for marker in self.SOCIAL_SECURITY_TARGET_MARKERS
        )

    def _legal_representative_proof_match_score(self, text: str) -> tuple[int, str | None, list[str]]:
        normalized_text = self._normalize_title_text(text)
        if not normalized_text:
            return 0, None, []

        hits = []
        for marker in self.LEGAL_REP_PROOF_EVIDENCE_MARKERS:
            normalized_marker = self._normalize_title_text(marker)
            if normalized_marker and normalized_marker in normalized_text and marker not in hits:
                hits.append(marker)

        if hits:
            longest_hit = max(len(self._normalize_title_text(hit)) for hit in hits)
            return 100 + min(longest_hit, 20), hits[0], hits

        if (
            "兹证明" in normalized_text
            and "系" in normalized_text
            and ("法定代表人" in normalized_text or "负责人" in normalized_text)
        ):
            hits = ["兹证明", "法定代表人/负责人"]
            return 90, "兹证明...系法定代表人/负责人", hits

        return 0, None, []

    def _social_security_match_score(self, text: str) -> tuple[int, str | None, list[str]]:
        normalized_text = self._normalize_title_text(text)
        if not normalized_text:
            return 0, None, []

        evidence_hits = []
        for marker in self.SOCIAL_SECURITY_EVIDENCE_MARKERS:
            normalized_marker = self._normalize_title_text(marker)
            if normalized_marker and normalized_marker in normalized_text and marker not in evidence_hits:
                evidence_hits.append(marker)

        role_hits = []
        for marker in self.SOCIAL_SECURITY_ROLE_MARKERS:
            normalized_marker = self._normalize_title_text(marker)
            if normalized_marker and normalized_marker in normalized_text and marker not in role_hits:
                role_hits.append(marker)

        has_social_security_phrase = (
            "社保" in normalized_text
            and any(token in normalized_text for token in ("缴纳", "缴费", "证明", "记录", "参保"))
        ) or (
            "社会保险" in normalized_text
            and any(token in normalized_text for token in ("缴纳", "缴费", "证明", "记录", "权益", "参保"))
        )
        if has_social_security_phrase and "社保缴纳/社会保险记录" not in evidence_hits:
            evidence_hits.append("社保缴纳/社会保险记录")

        has_employment_evidence = any(token in normalized_text for token in ("劳动合同", "聘用合同", "退休证"))
        if has_employment_evidence and "劳动合同/聘用合同/退休证" not in evidence_hits:
            evidence_hits.append("劳动合同/聘用合同/退休证")

        if not evidence_hits:
            return 0, None, []

        longest_hit = max(len(self._normalize_title_text(hit)) for hit in evidence_hits)
        score = 95 + min(longest_hit, 20)
        if role_hits:
            score += 10
        return score, evidence_hits[0], [*role_hits, *evidence_hits]

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
        # 去掉“提供/提交/具备/出具/开具/携带”等引导动词，只保留实际标题：
        # 招标要求“提供强制采购节能产品承诺书（格式）”→ 投标标题“强制采购节能产品承诺书”视为同一标题。
        stripped_name = self.LEADING_TITLE_VERB_RE.sub('', stripped_name)
        return stripped_name.strip('。，；;,. ')

    # 基于字典的模糊匹配
    def _smart_match(self, text: str, keyword: str) -> bool:
        normalized_text = self._normalize_title_text(text)
        for candidate in self._candidate_titles(keyword):
            normalized_candidate = self._normalize_title_text(candidate)
            if normalized_candidate and normalized_candidate in normalized_text:
                return True
        # 标题 key 兼容：覆盖“生产厂家授权书或投标人为生产厂家的证明”这类“或”备选、
        # 同义词（承诺函/承诺书）、编号差异（附件2-1 开标一览表 vs 2 开标一览表）等。
        if self._verification_checker._attachment_titles_compatible(keyword, text):
            return True
        # 词法相似度兜底：标题相近（如去掉“或”字后主体一致）也视为命中。
        try:
            from app.service.analysis.compliance.structured_consistency import lexical_similarity

            if lexical_similarity(keyword, text) >= 0.5:
                return True
        except Exception:  # pragma: no cover - 依赖缺失时退化为仅子串/兼容匹配
            pass
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
        if (
            "如有" in normalized
            or "认为需要补充" in compact
            or "其他内容" in compact
            or "其它内容" in compact
        ):
            return True
        # “其他材料”等兜底类目不认定为必须材料，无需在投标文件中查找。
        stripped = self._strip_heading_prefix(normalized).strip().rstrip("。；，;,. ")
        if "其他材料" in compact or "其它材料" in compact:
            return True
        if stripped in ("其他", "其它"):
            return True
        return False

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
    def _catalog_locations(self, sections: list, toc_pages: set[int]) -> list[dict[str, Any]]:
        locations: list[dict[str, Any]] = []
        if not toc_pages:
            return locations

        for section in sections:
            if not isinstance(section, dict) or section.get("page") not in toc_pages:
                continue
            text = str(section.get("text") or "").strip()
            if not text:
                continue
            location = self._location_from_section(section)
            if location:
                locations.append(location)

        return locations[:24]

    def _template_locations_by_requirement(
        self,
        model_json: dict,
        requirements: list[str],
        attachment_mapping: dict[str, list[str]],
        *,
        business_scope: dict | None = None,
        response_attachments: list[dict] | None = None,
    ) -> dict[str, list[dict[str, Any]]]:
        requirement_locations = TemplateExtractor.extract_requirement_locations(
            model_json,
            business_scope=business_scope,
            response_attachments=response_attachments,
        )
        if response_attachments is None:
            response_attachments = TemplateExtractor.extract_response_format_attachments(model_json)
        attachments_by_title: dict[str, dict[str, Any]] = {}
        attachments_by_number: dict[str, dict[str, Any]] = {}

        for attachment in response_attachments:
            if not isinstance(attachment, dict) or attachment.get("is_composite"):
                continue
            title = str(attachment.get("title") or "").strip()
            compact_title = TemplateExtractor._compact(title)
            if compact_title and compact_title not in attachments_by_title:
                attachments_by_title[compact_title] = attachment
            number = str(attachment.get("attachment_number") or "").strip()
            if number and number not in attachments_by_number:
                attachments_by_number[number] = attachment

        locations_by_requirement: dict[str, list[dict[str, Any]]] = {}

        def source_locations_for(item_title: str) -> list[dict[str, Any]]:
            direct = requirement_locations.get(item_title)
            if direct:
                return [location for location in direct if isinstance(location, dict)]
            compact_item = TemplateExtractor._compact(item_title)
            for title, locations in requirement_locations.items():
                if TemplateExtractor._compact(title) == compact_item:
                    return [location for location in locations if isinstance(location, dict)]
            return []

        for item in requirements:
            item_title = str(item or "").strip()
            if not item_title:
                continue
            source_locations = source_locations_for(item_title)
            if source_locations:
                locations_by_requirement[item_title] = source_locations
                continue
            attachment = attachments_by_title.get(TemplateExtractor._compact(item_title))
            if attachment is None:
                for ref in attachment_mapping.get(item_title) or []:
                    attachment = attachments_by_number.get(str(ref).strip())
                    if attachment is not None:
                        break
            if attachment is None:
                continue
            locations = [
                location
                for location in attachment.get("locations") or []
                if isinstance(location, dict)
            ]
            locations_by_requirement[item_title] = locations

        return locations_by_requirement

    def _heading_match_score(self, text: str, keyword: str) -> int:
        """标题匹配评分：优先选择更完整、更具体的附件标题。"""
        normalized_text = self._normalize_title_text(text)
        if not normalized_text:
            return 0

        best_score = 0
        normalized_keyword = self._normalize_title_text(keyword)
        if normalized_keyword and normalized_keyword in normalized_text:
            best_score = max(best_score, 120 + min(len(normalized_keyword), 30))

        for candidate in self._candidate_titles(keyword):
            normalized_candidate = self._normalize_title_text(candidate)
            if not normalized_candidate or normalized_candidate not in normalized_text:
                continue
            score = 80 + min(len(normalized_candidate), 30)
            # 泛化标题“承诺函/声明函”可作为兜底，但不能抢掉更完整标题。
            if normalized_candidate in {"承诺函", "声明函", "证书"}:
                score = 30 + len(normalized_candidate)
            best_score = max(best_score, score)

        return best_score

    def _find_heading_section(
        self,
        sections: list,
        headers: set,
        keyword: str,
        toc_pages: set[int] | None = None,
    ) -> dict | None:
        # 特例：营业执照和社保可以不依赖编号前缀
        EXEMPT_KEYWORDS = ["营业执照", "社会保险"]
        best_section = None
        best_score = 0

        for sec in sections:
            if sec.get('type') != 'heading':
                continue
            if toc_pages and sec.get("page") in toc_pages:
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
                    score = self._heading_match_score(text, keyword)
                    # 标题 key 兼容/词法相似度已命中（如“或”备选、同义、编号差异），
                    # 但 _heading_match_score 只认整串导致得分为 0：给基础分避免被拦截。
                    if score <= 0:
                        score = 1
                    if score > best_score:
                        best_section = sec
                        best_score = score
        return best_section

    # 第二遍只回查 text 区段，并跳过目录页，避免把目录项当成正文附件
    def _collect_toc_pages(self, sections: list) -> set[int]:
        toc_pages: set[int] = set()

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

        if self._is_legal_representative_proof_target(keyword):
            return self._legal_representative_proof_match_score(text)

        if self._is_social_security_target(keyword):
            social_score, social_title, social_hits = self._social_security_match_score(text)
            if social_score:
                return social_score, social_title, social_hits

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
        allow_body_fallback = (
            self._is_legal_representative_proof_target(keyword)
            or self._is_social_security_target(keyword)
        )

        for sec in sections:
            if not self._is_usable_body_section(sec, headers, toc_pages):
                continue
            text = str(sec.get("text") or "")
            if not allow_body_fallback and not self._looks_like_text_title(text, keyword):
                continue
            score, matched_title, hits = self._content_match_score(
                text, keyword
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
        match_section = self._find_heading_section(sections, headers, keyword, toc_pages)
        if match_section:
            return match_section
        match_section = self._find_text_section(sections, headers, keyword, toc_pages)
        if match_section:
            return match_section
        match_section = self._find_composite_body_section(sections, headers, keyword, toc_pages)
        if match_section:
            return match_section
        return self._find_body_section(sections, headers, keyword, toc_pages)

    @staticmethod
    def _is_direct_or_delegated_participation_choice(item: str) -> bool:
        """识别把直接参加和委托参加写在同一条中的选择条件。"""
        compact = re.sub(r"\s+", "", str(item or ""))
        return bool(
            ("法定代表人" in compact or "单位负责人" in compact)
            and ("直接参加" in compact or "直接投标" in compact)
            and "委托" in compact
            and ("授权委托书" in compact or "被授权人" in compact)
        )

    def _find_participation_choice_section(
        self,
        sections: list,
        headers: set,
        item: str,
        toc_pages: set[int],
    ) -> tuple[dict | None, str | None]:
        """选择条件只需命中资格证明或授权委托中的一个实际分支。"""
        if not self._is_direct_or_delegated_participation_choice(item):
            return None, None
        branch_targets = (
            ("direct", "法定代表人资格证明书"),
            ("delegated", "法定代表人授权委托书"),
        )
        hits: list[tuple[str, dict]] = []
        for branch, target in branch_targets:
            hit = self._find_required_section(sections, headers, target, toc_pages)
            if hit:
                hits.append((branch, hit))
        if not hits:
            return None, None
        branches = "+".join(branch for branch, _ in hits)
        first = dict(hits[0][1])
        first["text"] = "；".join(str(hit.get("text") or "") for _, hit in hits)
        return first, branches

    def _find_entity_proof_choice_sections(
        self,
        sections: list,
        headers: set,
        item: str,
        toc_pages: set[int],
    ) -> list[tuple[str, dict]]:
        """识别“营业执照或法人证书”等明确主体证明选择组。"""
        compact = re.sub(r"\s+", "", str(item or ""))
        if (
            "营业执照" not in compact
            or not any(token in compact for token in ("或", "或者"))
            or not any(token in compact for token in ("事业单位法人证书", "法人登记证书"))
        ):
            return []
        targets = ["营业执照"]
        targets.extend(
            target
            for target in ("事业单位法人证书", "法人登记证书")
            if target in compact
        )
        hits: list[tuple[str, dict]] = []
        for target in targets:
            hit = self._find_required_section(sections, headers, target, toc_pages)
            if hit:
                hits.append((target, hit))
        return hits

    # 主校验入口
    def check_integrity(self, model_json: dict, test_json: dict) -> dict:
        """
        根据招标文件模型检查投标文件的完整性。
        返回完整性评分、各项详情及位置信息。
        """
        scope = TemplateExtractor.extract_business_attachment_scope(model_json)
        response_bundle = TemplateExtractor.extract_response_format_bundle(model_json)
        attachments, _ = TemplateExtractor.filter_business_response_attachments(
            model_json,
            list(response_bundle.get('attachments') or []),
            scope=scope,
        )
        reqs, attachment_mapping = TemplateExtractor.extract_requirements(
            model_json,
            business_scope=scope,
            response_attachments=attachments,
        )
        attributes = {str(a.get('attachment_number')): a for a in attachments if a.get('attachment_number')}
        scope_entries = [
            entry for entry in scope.get('item_entries') or []
            if isinstance(entry, dict)
        ]
        data_node = test_json.get('data', test_json)
        sections, headers = TemplateExtractor.preprocess_sections(data_node.get('layout_sections', []))
        toc_pages = self._collect_toc_pages(sections)
        catalog_locations = self._catalog_locations(sections, toc_pages)
        template_locations_by_item = self._template_locations_by_requirement(
            model_json,
            reqs,
            attachment_mapping,
            business_scope=scope,
            response_attachments=attachments,
        )

        all_details = {}
        for item in reqs:
            is_sub = self._is_sub_item(item)
            cat = "资格证明子项" if is_sub else "商务标主项"

            # 每个附件单独判断，不再允许证明书/授权委托书互替，也不再做父子项放宽。
            norm_item = self._normalize_target(item)
            match_section = self._find_required_section(sections, headers, norm_item, toc_pages)
            entity_choice_hits = self._find_entity_proof_choice_sections(
                sections,
                headers,
                item,
                toc_pages,
            )
            if entity_choice_hits:
                match_section = dict(entity_choice_hits[0][1])
                match_section['text'] = '；'.join(
                    str(hit.get('text') or '') for _, hit in entity_choice_hits
                )
            choice_section, applicability_resolution = self._find_participation_choice_section(
                sections,
                headers,
                item,
                toc_pages,
            )
            if choice_section is not None:
                match_section = choice_section
            from ..requirement_groups import parse_group
            group = parse_group(item)
            group_locations = []
            resolution_status = None
            if entity_choice_hits:
                branches = []
                entity_branch_titles = ["营业执照"] + [
                    title
                    for title in ("事业单位法人证书", "法人登记证书")
                    if title in re.sub(r'\s+', '', str(item or ''))
                ]
                for title in entity_branch_titles:
                    hit = next((section for name, section in entity_choice_hits if name == title), None)
                    locations = [self._location_from_section(hit)] if hit else []
                    group_locations.extend(location for location in locations if location)
                    branches.append({
                        'title': title,
                        'matched': bool(hit),
                        'locations': [location for location in locations if location],
                    })
                group = {'operator': 'any_of', 'source_text': item, 'branches': branches}
                resolution_status = 'matched'
            elif applicability_resolution:
                location = self._location_from_section(match_section)
                if location:
                    group_locations.append(location)
                matched_branches = set(applicability_resolution.split('+'))
                group = {
                    'operator': 'any_of',
                    'source_text': item,
                    'branches': [
                        {
                            'title': '法定代表人/单位负责人资格证明书及身份证',
                            'matched': 'direct' in matched_branches,
                            'locations': group_locations if 'direct' in matched_branches else [],
                        },
                        {
                            'title': '法定代表人/单位负责人授权委托书及被授权人身份证',
                            'matched': 'delegated' in matched_branches,
                            'locations': group_locations if 'delegated' in matched_branches else [],
                        },
                    ],
                }
                resolution_status = 'matched'
            elif group['operator'] != 'single':
                for branch in group['branches']:
                    hit = self._find_required_section(sections, headers, self._normalize_target(branch['title']), toc_pages)
                    # A staff certificate or a short contained synonym does not
                    # establish the presence of the enterprise material named in an OR branch.
                    if hit:
                        from ..verification import VerificationChecker
                        from ..attachment_resolution import title_keys
                        verifier = VerificationChecker(None)
                        hit_key = verifier._raw_attachment_title_key(hit.get('text', ''))
                        if not any(len(key) >= 4 and key in hit_key for key in title_keys(verifier, branch['title'])):
                            hit = None
                    branch['matched'] = bool(hit)
                    branch['locations'] = [self._location_from_section(hit)] if hit else []
                    group_locations.extend(branch['locations'])
                hits = [b for b in group['branches'] if b['matched']]
                passed_group = bool(hits) if group['operator'] == 'any_of' else len(hits) == len(group['branches'])
                resolution_status = 'unclear' if group['operator'] == 'unclear' else 'matched' if passed_group else 'not_found'
                match_section = {'text': '；'.join(b['title'] for b in hits), **(group_locations[0] if group_locations else {})} if passed_group and resolution_status != 'unclear' else None
            match = str(match_section.get("text") or "") if isinstance(match_section, dict) else None
            is_optional = self._is_optional_item(item)
            referenced = [attributes[n] for n in attachment_mapping.get(item, []) if n in attributes]
            item_refs = {str(value).strip() for value in attachment_mapping.get(item, []) if str(value).strip()}
            item_core = TemplateExtractor._requirement_core_title(item)
            related_entries = [
                entry
                for entry in scope_entries
                if (
                    item_refs.intersection({str(value).strip() for value in entry.get('attachment_numbers') or []})
                    or (
                        not item_refs
                        and TemplateExtractor._requirement_core_title(entry.get('content') or '') == item_core
                    )
                )
            ]
            conflict = any(a.get('optionality_conflict') for a in referenced)
            if referenced:
                is_optional = all(bool(a.get('is_optional')) or self._is_optional_item(a.get('title', '')) for a in referenced)
            is_optional = is_optional and not conflict
            applicability_condition_unclear = (
                any(
                    entry.get('applicability_status') == 'unclear'
                    for entry in related_entries
                )
                and not any(
                    entry.get('applicability_status') == 'required'
                    for entry in related_entries
                )
            ) or (
                any(a.get('applicability_status') == 'unclear' for a in referenced)
                and not any(a.get('applicability_status') == 'required' for a in referenced)
            )
            # 条件材料已经实际提交时，“是否必须提交”不再影响完整性结论：
            # 该材料客观存在，仍应继续做模板和签章检查。只有材料未定位到时，
            # 才需要人工确认参选方式或主体条件，避免把所有含授权分支的项目
            # 一律降为待复核。
            applicability_unclear = applicability_condition_unclear and not bool(match)
            condition_text = '；'.join(dict.fromkeys(
                str(value).strip()
                for value in [
                    *(entry.get('condition_text') for entry in related_entries),
                    *(attachment.get('condition_text') for attachment in referenced),
                ]
                if str(value or '').strip()
            ))
            applicability_locations = [
                location
                for entry in related_entries
                if entry.get('applicability_status') == 'unclear'
                for location in entry.get('locations') or []
            ] or [
                location
                for attachment in referenced
                for location in attachment.get('applicability_locations') or []
            ]

            all_details[item] = {
                "status": (
                    "待复核" if conflict or applicability_unclear or resolution_status == 'unclear' else "已找到"
                    if match
                    else ("待复核" if conflict or applicability_unclear else ("可选项未提供" if is_optional else "缺失"))
                ),
                "preview": match or "-",
                "is_passed": bool(match) and not conflict and not applicability_unclear and resolution_status != 'unclear',
                "resolution_status": 'unclear' if applicability_unclear else resolution_status,
                "requirement_group": group if group['operator'] != 'single' else None,
                "is_optional": is_optional,
                "optionality_conflict": conflict,
                "optionality_locations": [loc for a in referenced for loc in a.get('optionality_locations') or []],
                "applicability_status": (
                    "unclear" if applicability_unclear
                    else ("conditional_satisfied" if applicability_condition_unclear else ("optional" if is_optional else "required"))
                ),
                "condition_text": condition_text,
                "applicability_resolution": applicability_resolution,
                "material_resolution": (
                    'entity_proof_any_of' if entity_choice_hits else None
                ),
                "applicability_locations": applicability_locations,
                "category": cat,
                "scored": not ((is_optional and not match) or applicability_unclear),
                "locations": group_locations if group['operator'] != 'single' else [self._location_from_section(match_section)] if match_section else [],
                "template_locations": template_locations_by_item.get(item) or [],
            }

        scored_details = [v for v in all_details.values() if v.get("scored", True)]
        optional_skipped_count = len([
            value for value in all_details.values()
            if value.get('is_optional') and not value.get('scored', True)
        ])
        applicability_unclear_count = len([
            value for value in all_details.values()
            if value.get('applicability_status') == 'unclear'
        ])
        passed = len([v for v in scored_details if v['is_passed']])
        total = len(scored_details)
        score = round((passed / total) * 100, 2) if total else 0

        data_node = model_json.get('data', model_json)
        has_tender_text = any(
            str(section.get('text') or '').strip()
            for section in data_node.get('layout_sections') or []
            if isinstance(section, dict)
        )
        extracted_item_count = len(all_details)
        if extracted_item_count:
            extraction_status = 'resolved'
            extraction_reason = f'已提取 {extracted_item_count} 个商务材料要求。'
            structure_locations = scope.get('scope_locations') or []
        else:
            extraction_status = 'unclear' if has_tender_text else 'failed'
            extraction_reason = str(
                scope.get('extraction_reason')
                if scope.get('extraction_status') == 'unclear'
                else response_bundle.get('extraction_reason')
                or scope.get('extraction_reason')
                or '未建立商务材料完整性检查项。'
            )
            structure_locations = (
                scope.get('scope_locations')
                or response_bundle.get('structure_locations')
                or []
            )

        return {
            "scope_status": scope.get('scope_status', 'legacy'),
            "scope_locations": scope.get('scope_locations') or [],
            "extraction_status": extraction_status,
            "extraction_reason": extraction_reason,
            "structure_locations": structure_locations,
            "template_extraction_status": response_bundle.get('extraction_status'),
            "template_extraction_reason": response_bundle.get('extraction_reason'),
            "template_structure_locations": response_bundle.get('structure_locations') or [],
            "integrity_score": score,
            "details": all_details,
            "extracted_item_count": extracted_item_count,
            "applicable_item_count": total,
            "actual_check_count": total,
            "passed_item_count": passed,
            "skipped_item_count": optional_skipped_count,
            "applicability_unclear_count": applicability_unclear_count,
            "scored_item_count": total,
            "ignored_item_count": len(all_details) - total,
            "attachment_mapping": attachment_mapping,
            "toc_pages": sorted(toc_pages),
            "business_catalog_pages": sorted(toc_pages),
            "business_catalog_locations": catalog_locations,
            "template_locations_by_item": template_locations_by_item,
        }
