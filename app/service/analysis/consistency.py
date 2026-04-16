import re
from difflib import SequenceMatcher
from typing import List, Dict

# 假设 TemplateExtractor 已在同目录下定义
from .template_extractor import TemplateExtractor 

class DocumentProcessor:
    """段落处理器：将文档切分为与模板对应的段落"""

    TITLE_TOKEN_PATTERNS = (
        "法定代表人授权委托书",
        "法定代表人资格证明书",
        "法定代表人证明书",
        "法定代表人身份证明",
        "单位负责人证明书",
        "单位负责人身份证明",
        "类似项目业绩清单",
        "投标人基本情况介绍",
        "供应商承诺声明函",
        "不参与围标串标承诺书",
        "保证金缴纳凭证",
        "财务状况及税收社会保障资金缴纳情况声明函",
        "财务状况声明函",
        "社会保障资金缴纳情况声明函",
        "制造商声明函",
        "制造商授权书",
        "原厂授权函",
        "营业执照",
        "法人登记证书",
        "分项报价表",
        "商务条款偏离表",
        "技术条款偏离表",
        "开标一览表",
        "投标保证书",
        "拟派项目负责人情况表",
        "项目人员配置表",
        "授权委托书",
        "证明书",
        "声明函",
        "承诺书",
        "保证书",
        "一览表",
        "报价表",
        "偏离表",
        "情况表",
        "配置表",
        "清单",
        "凭证",
        "执照",
    )
    TITLE_PREFIX_PATTERNS = (
        r'^\s*(?:附件|附表)\s*[A-Z\d]+(?:\s*[-－]\s*[A-Z\d]+)*[、.)）．]?\s*',
        r'^\s*第[一二三四五六七八九十百零\d]+[章节部分篇项]\s*',
        r'^\s*(?:\d+|[A-Z]|[一二三四五六七八九十百零]+)[．\.、]\s*',
        r'^\s*[（(](?:\d+|[A-Z]|[一二三四五六七八九十百零]+)[）)]\s*',
        r'^\s*\d+[)）]\s*',
    )
    TITLE_NOISE_PATTERNS = (
        r'附件|附表|附录|格式',
        r'按要求加盖公章',
        r'加盖公章',
        r'后附证明材料',
        r'直接投标的应提供',
        r'委托授权人投标的应提供',
        r'委托授权投标的应提供',
        r'及被授权人身份证',
        r'及身份证',
        r'如为分支机构投标则须总公司唯一授权函',
    )
    TITLE_FRAGMENT_PATTERNS = (
        "法定代表人",
        "单位负责人",
        "授权委托书",
        "证明书",
        "声明函",
        "承诺书",
        "保证书",
        "一览表",
        "报价表",
        "偏离表",
        "情况表",
        "配置表",
        "清单",
        "凭证",
        "执照",
        "财务状况",
        "社会保障资金",
        "税收",
        "保证金",
        "投标人基本情况",
        "类似项目业绩",
        "营业执照",
        "劳动合同",
        "社保",
    )

    @classmethod
    def _strip_title_prefix(cls, text: str) -> str:
        value = str(text or "").strip()
        previous = None
        while value and value != previous:
            previous = value
            for pattern in cls.TITLE_PREFIX_PATTERNS:
                value = re.sub(pattern, '', value).strip()
        return value

    @classmethod
    def _normalize_title_key(cls, text: str) -> str:
        value = cls._strip_title_prefix(text)
        value = re.sub(r'\(.*?\)|（.*?）', ' ', value)
        value = re.split(r'[；;。]', value, maxsplit=1)[0]
        for pattern, repl in (
            (r'法定代表人资格证明书', '法定代表人证明书'),
            (r'法定代表人身份证明', '法定代表人证明书'),
            (r'单位负责人身份证明', '单位负责人证明书'),
            (r'授权委托书及被授权人身份证', '授权委托书'),
        ):
            value = re.sub(pattern, repl, value)
        for pattern in cls.TITLE_NOISE_PATTERNS:
            value = re.sub(pattern, '', value)
        return re.sub(r'[^\u4e00-\u9fa5A-Za-z0-9]', '', value)

    @classmethod
    def _title_tokens(cls, text: str) -> List[str]:
        title_key = cls._normalize_title_key(text)
        if not title_key:
            return []
        tokens = []
        for token in cls.TITLE_TOKEN_PATTERNS:
            compact = re.sub(r'[^\u4e00-\u9fa5A-Za-z0-9]', '', token)
            if compact and compact in title_key and compact not in tokens:
                tokens.append(compact)
        for token in cls.TITLE_FRAGMENT_PATTERNS:
            compact = re.sub(r'[^\u4e00-\u9fa5A-Za-z0-9]', '', token)
            if compact and compact in title_key and compact not in tokens:
                tokens.append(compact)
        return tokens or [title_key]

    @classmethod
    def _title_match_score(cls, left: str, right: str) -> float:
        left_key = cls._normalize_title_key(left)
        right_key = cls._normalize_title_key(right)
        if not left_key or not right_key:
            return 0.0
        if left_key == right_key:
            return 1.0

        score = SequenceMatcher(None, left_key, right_key).ratio()
        if len(left_key) >= 4 and (left_key in right_key or right_key in left_key):
            score = max(score, 0.92)

        left_tokens = cls._title_tokens(left)
        if left_tokens:
            matched = sum(
                1
                for token in left_tokens
                if token in right_key or (len(token) >= 4 and right_key in token)
            )
            coverage = matched / len(left_tokens)
            if coverage >= 1:
                score = max(score, 0.9 if len(left_tokens) >= 2 else 0.82)
            elif coverage >= 0.6:
                score = max(score, 0.76)
        return min(score, 1.0)

    @classmethod
    def _compile_template_patterns(cls, templates: List[Dict]) -> None:
        """抽取独立方法：预编译模板匹配模式"""
        for temp in templates:
            core = cls._normalize_title_key(temp['title'])
            temp['title_key'] = core
            temp['title_tokens'] = cls._title_tokens(temp['title'])
            if core:
                prefix = r'[\s\-_]*'.join(list(core[:min(6, len(core))]))
                temp['pattern'] = re.compile(r'^.{0,30}?' + prefix)
            else:
                temp['pattern'] = None
            temp['buffer'] = []
            temp['extracted_text'] = ""

    @classmethod
    def _find_matching_template_idx(cls, clean_text: str, templates: List[Dict], current_idx: int) -> int:
        """抽取独立方法：寻找匹配的模板索引（包含乱序回退逻辑）"""
        # 优先向后找
        search_order = list(range(current_idx + 1, len(templates)))
        # 如果后面没找到，再回头从前面找（兼容乱序）
        search_order.extend(range(0, current_idx + 1))

        best = None
        for j in search_order:
            score = cls._title_match_score(templates[j].get('title') or "", clean_text)
            pattern = templates[j].get('pattern')
            if pattern is not None and pattern.search(clean_text):
                score = max(score, 0.88)
            if best is None or score > best['score']:
                best = {"score": score, "index": j}
        return best['index'] if best and best['score'] >= 0.76 else -1

    @classmethod
    def segment_document(cls, raw_json: dict, templates: list, is_test_file: bool = False) -> List[Dict]:
        data_node = raw_json.get('data', raw_json)
        
        # 解析 logical_tables 并在预处理时传递，保留正文中的表格结构
        logical_tables = data_node.get('logical_tables', [])
        sections, headers = TemplateExtractor.preprocess_sections(
            data_node.get('layout_sections', []), 
            logical_tables
        )
        
        cls._compile_template_patterns(templates)
        current_idx = -1

        for sec in sections:
            text = sec['text']
            if not text or TemplateExtractor._is_noise(text, headers, sec.get('type')) or (sec.get('type') == 'text' and text.strip().isdigit()): 
                continue
                
            clean_text = re.sub(r'\s+', '', text)
            short_text_candidate = (
                sec['type'] == 'text'
                and len(clean_text) <= 60
                and any(
                    marker in clean_text
                    for marker in ("一览表", "报价表", "偏离表", "情况表", "配置表", "保证书", "证明书", "委托书", "声明函", "承诺书", "清单", "凭证", "执照", "介绍", "授权")
                )
            )

            if sec['type'] == 'heading' or short_text_candidate:
                is_potential_title = True if sec['type'] == 'heading' and is_test_file else (
                    short_text_candidate or bool(TemplateExtractor.RE_HEADING_START.search(text))
                )

                if is_potential_title:
                    matched_idx = cls._find_matching_template_idx(clean_text, templates, current_idx)
                
                    if matched_idx != -1:
                        # 忽略后文重复出现的标题模板 (防重入)
                        if len(templates[matched_idx]['buffer']) > 3: 
                            if current_idx != -1: 
                                templates[current_idx]['extracted_text'] = "\n".join(templates[current_idx]['buffer'])
                            current_idx = -1
                            continue
                        
                        # 成功匹配新标题：保存上一个的状态，并切换到新状态
                        if current_idx != -1: 
                            templates[current_idx]['extracted_text'] = "\n".join(templates[current_idx]['buffer'])
                        
                        current_idx = matched_idx
                        templates[current_idx]['buffer'] = [text]
                        continue
                    
                    elif current_idx != -1 and re.match(r'^[一二三四五六七八九十百]+[、．]', text.strip()):
                        templates[current_idx]['extracted_text'] = "\n".join(templates[current_idx]['buffer'])
                        current_idx = -1
                        continue
                
                # 遇到大章节断点，主动终止当前收集状态
                is_chapter_break = re.search(r'^第[一二三四五六七八九十百]+[章节部分]', text) or "技术文件" in text
                if current_idx != -1 and is_chapter_break:
                    templates[current_idx]['extracted_text'] = "\n".join(templates[current_idx]['buffer'])
                    current_idx = -1

            # 如果当前处于“收集状态”，则将正文追加到对应模板的缓冲区
            if current_idx != -1: 
                templates[current_idx]['buffer'].append(text)

        # 循环结束，收尾最后一个模板
        if current_idx != -1: 
            templates[current_idx]['extracted_text'] = "\n".join(templates[current_idx]['buffer'])
            
        return [{"title": t['title'], "text": t['extracted_text']} for t in templates]


class ConsistencyChecker:
    """一致性校验器：比对正文细节差异"""

    FORMAL_TITLE_LINE_RE = re.compile(
        r"^\s*(?:"
        r"(?:[（(]?\d+(?:\s*[-－]\s*\d+)?[)）\.、]?\s*)?(?:附件|附表)\s*\d+(?:\s*[-－]\s*\d+)*"
        r"|第[一二三四五六七八九十百0-9]+[章节部分]"
        r")"
    )
    NON_BODY_BLOCK_MARKERS = (
        "与本项目有关的一切正式往来通讯请寄",
        "正式往来通讯请寄",
    )
    NON_BODY_LINE_MARKERS = (
        "参选人法定代表人",
        "法定代表人或授权代表签字或盖章",
        "法定代表人签字或盖章",
        "授权委托人签字或盖章",
        "参选人名称",
        "供应商名称",
        "日期",
        "已签字",
    )

    def __init__(self):
        # NORM_PATTERN: 用于最后比对时，过滤一切非核心字符
        self.NORM_PATTERN = re.compile(r'[\u4e00-\u9fa5a-zA-Z0-9]+')
        
        # GAP_PATTERN: 匹配一切非中文、非字母、非数字的字符
        self.GAP_PATTERN = re.compile(r'[^\u4e00-\u9fa5a-zA-Z0-9]+')

    def _normalize(self, text: str) -> str:
        if not text: return ""
        return "".join(self.NORM_PATTERN.findall(text))

    def _normalize_title(self, text: str) -> str:
        if not text:
            return ""
        no_brackets = re.sub(r'\(.*?\)|（.*?）', '', text)
        clean = re.sub(r'[^\u4e00-\u9fa5A-Za-z0-9]|附件|附表|附录|格式', '', no_brackets)
        return re.sub(r'^[\d一二三四五六七八九十百]+', '', clean)

    def _is_formal_title_line(self, text: str) -> bool:
        return bool(self.FORMAL_TITLE_LINE_RE.match(str(text or "").strip()))

    def _strip_title_line(self, text: str, title: str) -> str:
        if not text:
            return ""
        lines = [line for line in text.splitlines() if line.strip()]
        if not lines:
            return ""

        first_line = lines[0].strip()
        if not self._is_formal_title_line(first_line):
            return "\n".join(lines).strip()

        first_norm = self._normalize_title(first_line)
        title_norm = self._normalize_title(title)
        if first_norm and title_norm and (first_norm in title_norm or title_norm in first_norm):
            lines = lines[1:]
        return "\n".join(lines).strip()

    def _is_non_body_line(self, normalized_line: str) -> bool:
        return any(marker in normalized_line for marker in self.NON_BODY_LINE_MARKERS)

    def _trim_non_body_lines(self, text: str) -> str:
        if not text:
            return ""

        kept: list[str] = []
        in_non_body_block = False

        for line in text.splitlines():
            stripped = line.strip()
            if not stripped:
                continue

            normalized_line = self._normalize(stripped)
            if not normalized_line:
                continue

            if any(marker in normalized_line for marker in self.NON_BODY_BLOCK_MARKERS):
                in_non_body_block = True
                continue

            if in_non_body_block:
                continue

            if self._is_non_body_line(normalized_line):
                continue

            kept.append(stripped)

        return "\n".join(kept)

    def _get_anchors(self, text: str) -> List[str]:
        # 1. 抹平括号（防止文本粘连）
        text = re.sub(r'\(.*?\)|（.*?）', ' ', text)
        
        # 2. 核心业务逻辑保留
        text = text.replace('年月日', '年 月 日')
        
        # 3. 使用预编译的非空白/字母/数字正则进行切割
        parts = self.GAP_PATTERN.split(text)
        
        anchors = []
        for p in parts:
            norm = self._normalize(p)
            # 业务跳过规则
            if '粘贴' in norm or ('签字' in norm and '盖章' in norm) or norm.isdigit(): 
                continue
            # 保留长度>=2的词汇，以及单字的“年月日”
            if len(norm) >= 2 or norm in ['年', '月', '日']: 
                anchors.append(norm)
                
        return anchors

    # 在 compare_raw_data 方法中增加特殊附件判断
    def compare_raw_data(self, model_json: dict, test_json: dict) -> List[Dict]:
        temps = TemplateExtractor.extract_consistency_templates(model_json)
        model_segments = [{"title": t['title'], "text": "\n".join(t['content'])} for t in temps]
        test_segments = DocumentProcessor.segment_document(test_json, temps, is_test_file=True)

        results = []
        for i, m_seg in enumerate(model_segments):
            m_txt = m_seg['text']
            t_txt = test_segments[i]['text']

            m_body = self._trim_non_body_lines(self._strip_title_line(m_txt, m_seg['title']))
            t_body = self._trim_non_body_lines(self._strip_title_line(t_txt, m_seg['title']))

            title = m_seg['title']
            # 特殊附件：只要存在内容即通过
            if "制造商声明函" in title or "制造商授权书" in title or "原厂授权函" in title:
                passed = bool(t_body.strip())
                results.append({
                    "name": title,
                    "is_passed": passed,
                    "missing_anchors": [] if passed else ["[未检测到内容]"]
                })
                continue

            if "法定代表人" in title and ("证明书" in title or "授权委托书" in title):
                relaxed_anchors = ["法定代表人"]
                if "证明书" in title:
                    relaxed_anchors.append("证明书")
                if "授权委托书" in title:
                    relaxed_anchors.append("授权委托书")

                norm_t = self._normalize(t_body)
                missing = [a for a in relaxed_anchors if a not in norm_t]
                results.append({
                    "name": title,
                    "is_passed": bool(t_body.strip()) and len(missing) == 0,
                    "missing_anchors": missing if t_body.strip() else ["[未检测到内容]"]
                })
                continue

            norm_t = self._normalize(t_body)
            anchors = self._get_anchors(m_body)
            missing = [a for a in anchors if a not in norm_t]

            results.append({
                "name": title,
                "is_passed": len(missing) == 0,
                "missing_anchors": missing
            })
        return results
