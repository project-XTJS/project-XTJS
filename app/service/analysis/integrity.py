"""
投标文件商务标合规性审查模块
包含功能：
1. 完整性检查 (IntegrityChecker) - 聚焦核心材料的缺失与漏报核查
2. 格式模板一致性检查 (TemplateConsistencyChecker) - 核心条款防篡改与近义词容错
负责人：虞光勇、陶明宇
"""
import re
import difflib

# 1. 投标文件完整性检查
class IntegrityChecker:
    # 商务标核心材料 10 大项白名单 (按需求文档严格定义)
    BUSINESS_REQUIRED_SECTIONS = [
        "投标保证书",
        "开标一览表",
        "分项报价表",
        "商务条款偏离表",
        "技术条款偏离表",
        "投标人基本情况介绍",
        "类似项目业绩清单",
        "投标人的资格证明文件",
        "保证金缴纳凭证",
        "投标人认为需加以说明的其他内容"
    ]

    # 第 8 项“投标人的资格证明文件”下的核心子项材料白名单
    QUALIFICATION_SUB_SECTIONS = [
        "营业执照",
        "法定代表人/单位负责人证明书",
        "缴纳社保的证明材料",
        "投标人承诺声明函",
        "不参与围标串标承诺书",
        "财务状况及税收、社会保障资金缴纳情况声明函",
        "制造商声明函"
    ]

    def check_integrity(self, text: str) -> dict:
        """
        投标文件完整性检查
        """
        found_sections = []
        missing_sections = []
        details = {}

        # 匹配常见的标书标题前缀：一、 / 1. / A. / (一) / 附件 等
        header_prefix = r'(?:第[一二三四五六七八九十百]+[章部分]|附件[一二三四五六七八九十]+|[一二三四五六七八九十]、|\d{1,2}\.[\d\.]*|[A-G]\.|（[一二三四五六七八九十]）|\([A-G]\))\s*'

        # 1. 检查 10 大主项目录
        for section in self.BUSINESS_REQUIRED_SECTIONS:
            fuzzy_section = ".*?".join(list(section))
            
            strict_pattern = re.compile(rf'^{header_prefix}.*?{fuzzy_section}.*?$', re.MULTILINE)
            loose_pattern = re.compile(rf'^\s*.*?{fuzzy_section}.*?\s*$', re.MULTILINE)

            strict_matches = strict_pattern.findall(text)
            loose_matches = loose_pattern.findall(text)

            if strict_matches or loose_matches:
                found_sections.append(section)
                details[section] = {
                    "status": "已找到",
                    "preview": (strict_matches + loose_matches)[0].strip(),
                    "category": "主项文件"
                }
            else:
                if section == "投标人认为需加以说明的其他内容":
                    details[section] = {"status": "可选项目，未提供", "category": "可选文件"}
                else:
                    missing_sections.append(section)
                    details[section] = {"status": "缺失", "category": "主项文件"}

        # 2. 检查第 8 项里面的 7 个子项目录
        if "投标人的资格证明文件" in found_sections:
            for sub_section in self.QUALIFICATION_SUB_SECTIONS:
                if "社保" in sub_section:
                    search_term = "(?:社保|劳动合同|聘用合同|退休证)"
                else:
                    search_term = ".*?".join(list(sub_section.split('/')[0][:4]))
                
                sub_pattern = re.compile(rf'^{header_prefix}.*?{search_term}.*?$', re.MULTILINE)
                sub_loose_pattern = re.compile(rf'^\s*.*?{search_term}.*?\s*$', re.MULTILINE)
                
                if sub_pattern.findall(text) or sub_loose_pattern.findall(text):
                    found_sections.append(f"子项: {sub_section}")
                    details[f"子项: {sub_section}"] = {"status": "已找到", "category": "资格证明文件子项"}
                else:
                    missing_sections.append(f"子项: {sub_section}")
                    details[f"子项: {sub_section}"] = {"status": "缺失 (判定：未按规范标注标题)", "category": "资格证明文件子项"}

        # 3. 计算合规性得分
        required_main_count = len(self.BUSINESS_REQUIRED_SECTIONS) - 1
        found_main_count = len([s for s in found_sections if not s.startswith("子项:")])
        score = (found_main_count / required_main_count) * 100 if required_main_count > 0 else 0

        return {
            "integrity_score": round(score, 2),
            "found_count": len(found_sections),
            "missing_count": len(missing_sections),
            "found_sections": found_sections,
            "missing_sections": missing_sections,
            "details": details
        }