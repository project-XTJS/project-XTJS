import re

class TemplateExtractor:
    """双雷达导航提取器：独立提取完整性清单与一致性模板"""

    @classmethod
    def extract_requirements(cls, model_raw_json: dict) -> dict:
        """为完整性检查提取商务文件清单"""
        sections = model_raw_json.get('data', {}).get('layout_sections', [])
        main_list, sub_list = [], []
        
        # 状态机：寻找“文件的组成”
        STAGE_FIND_COMPOSE = 0
        STAGE_FIND_BUSINESS = 1
        STAGE_RECORDING = 2
        current_stage = STAGE_FIND_COMPOSE

        for sec in sections:
            text = str(sec.get('text', '')).strip()
            sec_type = sec.get('type', '')
            if not text: continue

            if sec_type == 'heading':
                # 兼容“一、响应文件的组成”或“应答文件的组成”
                if current_stage == STAGE_FIND_COMPOSE and re.search(r'[一二三四五]、.*文件[的]?组成', text):
                    current_stage = STAGE_FIND_BUSINESS
                    continue
                # 寻找“（一）商务文件”
                if current_stage == STAGE_FIND_BUSINESS and "商务文件" in text:
                    current_stage = STAGE_RECORDING
                    continue
                # 遇到“（二）技术文件”停止
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
        sections = data_node.get('layout_sections', [])
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
                # 增强版终止条件：增加“营业执照”作为刹车点
                if sec_type == 'heading':
                    if re.match(r'^第[一二三四五六七八九十]+章', text) or "营业执照" in text:
                        break

                if sec_type == 'heading' and re.match(r'^(附件|附表|格式)\s*[\d\-]+', text):
                    if current_attachment:
                        templates.append(current_attachment)
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