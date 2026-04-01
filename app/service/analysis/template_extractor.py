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
        sections = model_raw_json.get('data', {}).get('layout_sections', [])
        templates = []
        
        # 状态机：寻找“文件部分格式附件”
        in_attachment_zone = False
        current_attachment = None

        for sec in sections:
            text = str(sec.get('text', '')).strip()
            sec_type = sec.get('type', '')
            
            # 1. 进入大区：包含“文件”且包含“部分格式附件”
            if sec_type == 'heading' and not in_attachment_zone:
                if re.search(r'文件.*部分格式附件', text):
                    in_attachment_zone = True
                    continue

            if in_attachment_zone:
                # 2. 走出大区：遇到下一章（第X章）
                if sec_type == 'heading' and re.match(r'^第[一二三四五六七八九十]+章', text):
                    break

                # 3. 识别附件标题：以“附件”或“格式”开头的 Heading
                # 附件7-1, 附件12 等
                if sec_type == 'heading' and re.match(r'^(附件|附表|格式)\s*[\d\-]+', text):
                    # 如果当前已有录制的，存入列表
                    if current_attachment:
                        templates.append(current_attachment)
                    
                    current_attachment = {
                        "title": cls._clean_text(text),
                        "content": [text] # 包含标题本身
                    }
                elif current_attachment:
                    # 录制正文或表格
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