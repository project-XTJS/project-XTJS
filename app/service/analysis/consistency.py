import re

class DocumentProcessor:
    """利用 Model 模板锚点精准切分投标人文件（支持页码剔除与章节刹车）"""

    @classmethod
    def segment_document(cls, raw_json: dict, templates: list) -> list:
        data_node = raw_json.get('data', raw_json)
        sections = data_node.get('layout_sections', [])
        
        # 1. 预处理正则：保留数字，去除高频干扰词
        for temp in templates:
            title = temp['title']
            # 保留数字以区分附件5和附件8
            clean_title = re.sub(r'[^\u4e00-\u9fa5A-Za-z0-9]|附件|附表|附录|格式', '', title)
            core_keyword = clean_title[:5] # 取前5个字符做特征
            temp['pattern'] = re.compile(f'.*?'.join(list(core_keyword)))
            temp['extracted_text'] = ""

        results = []
        current_temp_idx = -1
        captured = []

        # 2. 单次顺序遍历，状态机式模板匹配与内容捕获
        for sec in sections:
            text = str(sec.get('text', '')).strip()
            sec_type = sec.get('type', '')
            if not text: continue
            
            # 识别附件标题特征
            is_attachment_heading = (sec_type == 'heading' and re.search(r'^(附件|格式|附表)', text))
            
            # 章节刹车特征：识别大章节标题作为终止符号
            is_chapter_break = (sec_type == 'heading' and re.match(r'^第[一二三四五六七八九十百]+[章节部分]', text))

            # --- 逻辑处理 ---
            
            # 如果遇到了下一章，立刻结算并彻底关闭录制
            if is_chapter_break:
                if current_temp_idx != -1:
                    templates[current_temp_idx]['extracted_text'] = "\n".join(captured)
                    current_temp_idx = -1
                    captured = []
                continue 

            if is_attachment_heading:
                matched_idx = -1
                for i in range(current_temp_idx + 1, len(templates)):
                    if templates[i]['pattern'].search(text):
                        matched_idx = i
                        break
                
                if matched_idx != -1:
                    # 匹配成功：结算上一个
                    if current_temp_idx != -1:
                        templates[current_temp_idx]['extracted_text'] = "\n".join(captured)
                    
                    # 开启新的录制
                    current_temp_idx = matched_idx
                    captured = [text]
                    continue
                else:
                    # 遇到了陌生附件 Heading，刹车结算
                    if current_temp_idx != -1:
                        templates[current_temp_idx]['extracted_text'] = "\n".join(captured)
                        current_temp_idx = -1
                        captured = []
                    continue

            # 正在录制状态下吸入内容
            if current_temp_idx != -1:
                captured.append(text)

        # 文档结束结算最后一个
        if current_temp_idx != -1:
            templates[current_temp_idx]['extracted_text'] = "\n".join(captured)

        # 组装返回并剔除末尾页码
        for temp in templates:
            final_text = temp['extracted_text'].strip()
            # 规则：如果整段文字末尾出现“换行+数字”或“空格+数字”，则判定为页码并删除
            final_text = re.sub(r'[\s\n]+\d+$', '', final_text)
            results.append({
                "title": temp['title'],
                "text": final_text
            })
            
        return results


class ConsistencyChecker:
    """格式一致性审查服务"""
    def __init__(self):
        # 1. 统一处理：将所有括号及内部内容作为正则匹配目标
        self.BRACKET_PATTERN = re.compile(r'（[^）]*）|【[^】]*】|\[[^\]]*\]|<[^>]*>')
        
        # 2. 分段切割点：标点、空白，以及括号本身也作为天然的句子切分断点
        self.TEMPLATE_SPLIT_PATTERN = re.compile(
            r'（[^）]*）|【[^】]*】|\[[^\]]*\]|<[^>]*>|[:：_+，,。．.；;！!？?：“”"\'、\s\n\r\\@·・·]+'           
        )
        
        # 3. 纯净内容提取：仅保留中文、大小写英文和数字
        self.CONTENT_PATTERN = re.compile(r'[\u4e00-\u9fa5a-zA-Z0-9]+')

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
        if not text: return ""
        # 将所有英文括号转为中文括号，统一抹除
        text = text.replace('(', '（').replace(')', '）')
        text = self._replace_synonyms(text)
        # 删掉所有括号及其内部内容
        text = self.BRACKET_PATTERN.sub('', text)
        text = re.sub(r'_+', '', text)
        return text

    def _normalize(self, text: str) -> str:
        if not text: return ""
        clean_text = self._clean_text(text)
        # 提取汉字、字母和数字，抛弃标点符号
        return "".join(self.CONTENT_PATTERN.findall(clean_text))

    def _get_anchors(self, template_text: str):
        if not template_text: return []
        
        # 预处理括号和近义词
        template_text = template_text.replace('(', '（').replace(')', '）')
        template_text = re.sub(r'(项目)', r' \1 ', template_text)
        template_text = self._replace_synonyms(template_text)
        
        # 使用标点和括号切分片段
        segments = self.TEMPLATE_SPLIT_PATTERN.split(template_text)
        
        anchors = []
        for s in segments:
            # 如果片段中出现“粘贴”，则跳过该锚点
            if "粘贴" in s:
                continue
                
            norm_s = self._normalize(s)
            if len(norm_s) >= 2:
                anchors.append(norm_s)
        return anchors

    def compare_raw_data(self, model_json: dict, test_json: dict, found_sections: list = None):
        from app.service.analysis.template_extractor import TemplateExtractor
        
        # 1. 提取 Model 的真实模板
        templates = TemplateExtractor.extract_consistency_templates(model_json)
        
        # 2. 用 Model 模板去切分文档
        m_segs = DocumentProcessor.segment_document(model_json, templates)
        b_segs = DocumentProcessor.segment_document(test_json, templates)

        reports = []
        for i, m_seg in enumerate(m_segs):
            title = m_seg['title']
            b_text = b_segs[i]['text']
            m_text = m_seg['text']

            # 3. 严格执行锚点比对
            norm_bidder = self._normalize(b_text)
            anchors = self._get_anchors(m_text)
            issues, last_pos = [], 0
            
            for a in anchors:
                pos = norm_bidder.find(a, last_pos)
                if pos == -1: 
                    issues.append(a)
                else: 
                    last_pos = pos + len(a)

            reports.append({
                "name": title,
                "is_passed": len(issues) == 0,
                "missing_segments_count": len(issues),
                "missing_anchors": issues
            })
            
        return reports