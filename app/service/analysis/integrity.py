import re
from .template_extractor import TemplateExtractor

class IntegrityChecker:
    """投标文件完整性检查器"""

    def __init__(self):
        pass

    @classmethod
    def extract_text(cls, raw_json_data: dict) -> str:
        """提取 PDF 转化后的全文本"""
        data_node = raw_json_data.get('data', raw_json_data)
        parts = []
        if 'layout_sections' in data_node:
            for sec in data_node['layout_sections']:
                if isinstance(sec, dict):
                    text_val = sec.get('text') or sec.get('content') or ''
                    parts.append(str(text_val))
        return "\n".join(parts)

    def _normalize_target_name(self, item_name: str) -> str:
        """核心词提取与强映射"""
        # 1. 强映射
        if "基本情况" in item_name:
            return "基本情况"
        if "类似项目业绩清单" in item_name:
            return "类似项目业绩清单"
        if "财务状况" in item_name and "社会保障" in item_name:
            return "社会保障资金缴纳情况声明函"
            
        # 2. 清洗无用主语
        item_name = re.sub(r'^(参选人的|投标人的|应答人的|参选人认为|投标人认为|应答人认为|参选人|投标人|应答人)', '', item_name)
        
        # 3. 清洗补充说明
        item_name = re.sub(r'(可另外再附.*|后附.*材料)$', '', item_name)
        
        # 4. 清洗括号内的格式要求
        item_name = re.sub(r'[（\(].*?(公章|原件|复印件|如有|格式).*?[）\)]', '', item_name)
        
        # 5. 清除首尾标点
        item_name = item_name.strip('。，；;,. ')
        
        return item_name

    def _check_items(self, text: str, items_list: list, category_name: str, header_prefix: str) -> tuple:
        """在长文本中搜索清单项"""
        found, missing, details = [], [], {}
        
        for original_item in items_list:
            search_target = self._normalize_target_name(original_item)
            
            # 取清洗后核心词的前4个字作为模糊匹配的雷达特征
            fuzzy_core = ".*?".join(list(search_target[:4]))
            
            strict_pattern = re.compile(rf'^{header_prefix}.*?{fuzzy_core}.*?$', re.MULTILINE)
            loose_pattern = re.compile(rf'^\s*.*?{fuzzy_core}.*?\s*$', re.MULTILINE)

            strict_matches = strict_pattern.findall(text)
            loose_matches = loose_pattern.findall(text)

            if strict_matches or loose_matches:
                found.append(original_item)
                details[original_item] = {
                    "status": "已找到", 
                    "preview": (strict_matches + loose_matches)[0].strip(), 
                    "category": category_name,
                    "search_target_used": search_target 
                }
            else:
                missing.append(original_item)
                details[original_item] = {
                    "status": "缺失", 
                    "category": category_name,
                    "search_target_used": search_target
                }
                
        return found, missing, details

    def check_integrity(self, model_raw_json: dict, test_raw_data) -> dict:
        """执行完整性检查的主入口"""
        # 1. 独立调用提取器：只提取“应交清单”（雷达A）
        reqs = TemplateExtractor.extract_requirements(model_raw_json)
        dynamic_main_sections = reqs['main']
        dynamic_sub_sections = reqs['sub']

        # 2. 获取投标人文件的全文本
        text = self.extract_text(test_raw_data) if isinstance(test_raw_data, dict) else test_raw_data
        
        # 定义可能出现的标题序号正则
        header_prefix = r'(?:第[一二三四五六七八九十百]+[章部分]|附件[一二三四五六七八九十]+|[一二三四五六七八九十]、|\d{1,2}\.[\d\.]*|[A-G]\.|[（\(][一二三四五六七八九十][）\)]|\([A-G]\))\s*'

        # 3. 基础审查
        main_found, main_missing, main_details = self._check_items(
            text, dynamic_main_sections, "商务标主项文件", header_prefix
        )
        sub_found, sub_missing, sub_details = self._check_items(
            text, dynamic_sub_sections, "资格证明子项材料", header_prefix
        )

        # 只要找到了任何一个资格证明子项 (A. B. C.)，父项就自动算作“已找到”
        qualification_parents = [m for m in main_missing if "资格" in m or "证明文件" in m]
        for qp in qualification_parents:
            if len(sub_found) > 0:
                main_missing.remove(qp)
                main_found.append(qp)
                main_details[qp]["status"] = "已找到 (因子项存在至少一个证明材料)"

        # 包含这些关键字的项目，如果没有找到，不计入缺失，也不扣分
        optional_keywords = ["其他内容"]
        actual_main_missing = []
        
        for m in main_missing:
            is_optional = any(kw in m for kw in optional_keywords)
            if is_optional:
                main_details[m]["status"] = "未提供 (可选附加项，不影响完整性)"
            else:
                actual_main_missing.append(m)
                
        main_missing = actual_main_missing

        # 4. 组装最终结果
        found_sections = main_found + [f"子项: {s}" for s in sub_found]
        missing_sections = main_missing + [f"子项: {s}" for s in sub_missing]
        
        details = {**main_details}
        for k, v in sub_details.items():
            details[f"子项: {k}"] = v

        # 5. 计算动态得分（剔除选填项对满分分母的干扰）
        total_required = len(dynamic_main_sections) + len(dynamic_sub_sections)
        optional_count = len([m for m in dynamic_main_sections if any(kw in m for kw in optional_keywords)])
        effective_required = max(1, total_required - optional_count)
        
        total_found = len(main_found) + len(sub_found)
        score = min(100.0, (total_found / effective_required) * 100) if effective_required > 0 else 100.0

        return {
            "integrity_score": round(score, 2),
            "found_count": total_found,
            "missing_count": len(missing_sections),
            # 这里的 found_sections 可以传给 ConsistencyChecker 充当免查白名单
            "found_sections": found_sections, 
            "missing_sections": missing_sections,
            "details": details,
        }