import re

class TemplateExtractor:
    """双雷达导航提取器：独立提取完整性清单与一致性模板"""

    @classmethod
    def _promote_sections(cls, sections: list) -> list:
        """
        页码匹配提升逻辑：
        每页先查找是否有匹配特征的 heading。如果找到了，跳过该页剩下的所有寻找。
        如果没有找到匹配的 heading，就在该页的 text 里找，找到匹配的将其强行提升为 heading。
        """
        PATTERN_START = re.compile(r'^\s*[\(（]?(附件|附表|格式|第[一二三四五六七八九十百]+[章节部分]|[一二三四五六七八九十]+[、.])')
        PATTERN_KEYWORD = re.compile(r'文件[的]?组成|商务文件|技术文件|部分格式附件|营业执照')
        
        def is_target(text: str) -> bool:
            if PATTERN_START.search(text): return True
            # 短文本关键词匹配，防止将正文中的长句误认为标题
            if PATTERN_KEYWORD.search(text) and len(text) < 40: return True
            return False

        # 1. 记录拥有原生合法 heading 的页码
        page_has_native = {}
        for sec in sections:
            if not isinstance(sec, dict): continue
            page = sec.get('page', -1)
            if sec.get('type') == 'heading' and is_target(str(sec.get('text', '')).strip()):
                page_has_native[page] = True
                
        # 2. 依次提权兜底
        processed = []
        promoted = set() # 记录已经被提升过 text 的页码
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
                    new_sec['type'] = 'heading' # 提权为 heading
                    processed.append(new_sec)
                    promoted.add(page) # 找到了一个就跳过当前page的后续查找
                    continue
            processed.append(sec)
        return processed

    @classmethod
    def extract_requirements(cls, model_raw_json: dict) -> dict:
        """为完整性检查提取商务文件清单"""
        data_node = model_raw_json.get('data', model_raw_json)
        sections = cls._promote_sections(data_node.get('layout_sections', []))
        
        main_list, sub_list = [], []
        STAGE_FIND_COMPOSE = 0
        STAGE_FIND_BUSINESS = 1
        STAGE_RECORDING = 2
        current_stage = STAGE_FIND_COMPOSE

        for sec in sections:
            text = str(sec.get('text', '')).strip()
            sec_type = sec.get('type', '')
            if not text: continue

            if sec_type == 'heading':
                if current_stage == STAGE_FIND_COMPOSE and re.search(r'[一二三四五]、.*文件[的]?组成', text):
                    current_stage = STAGE_FIND_BUSINESS
                    continue
                if current_stage == STAGE_FIND_BUSINESS and "商务文件" in text:
                    current_stage = STAGE_RECORDING
                    continue
                if current_stage == STAGE_RECORDING and "技术文件" in text:
                    break

            if current_stage == STAGE_RECORDING:
                main_match = re.match(r'^(\d+)[．\.]\s*(.+)', text)
                sub_match = re.match(r'^([A-Z])[．\.]\s*(.+)', text)
                if main_match:
                    main_list.append(cls._clean_text(main_match.group(2)))
                elif sub_match:
                    sub_list.append(cls._clean_text(sub_match.group(2)))

        return {"main": main_list, "sub": sub_list}

    @classmethod
    def extract_consistency_templates(cls, model_raw_json: dict) -> list:
        """为一致性检查提取“附件X”标准模板"""
        data_node = model_raw_json.get('data', model_raw_json)
        sections = cls._promote_sections(data_node.get('layout_sections', []))
        
        templates = []
        in_attachment_zone = False
        current_attachment = None

        for sec in sections:
            text = str(sec.get('text', '')).strip()
            sec_type = sec.get('type', '')
            
            if sec_type == 'heading' and not in_attachment_zone:
                if re.search(r'文件.*部分格式附件', text):
                    in_attachment_zone = True
                    continue

            if in_attachment_zone:
                if sec_type == 'heading':
                    if re.match(r'^第[一二三四五六七八九十百]+章', text) or "营业执照" in text:
                        break

                if sec_type == 'heading' and re.search(r'^\s*[\(（]?(附件|附表|格式)\s*[\d\-]+', text):
                    if current_attachment:
                        templates.append(current_attachment)
                        
                    if "人员配置表" in text or "项目人员配置表" in text:
                        current_attachment = None
                        continue
                        
                    current_attachment = {"title": cls._clean_text(text), "content": [text]}
                elif current_attachment:
                    current_attachment["content"].append(text)

        if current_attachment:
            templates.append(current_attachment)
            
        return templates

    @classmethod
    def _clean_text(cls, text: str) -> str:
        if "营业执照" in text: return "营业执照"
        text = re.sub(r'[；;:：。]$', '', text).strip()
        text = re.sub(r'[(（].*?格式.*?参见.*?[)）]', '', text).strip()
        text = re.sub(r'[\.…]+\s*\d+$', '', text).strip()
        return text