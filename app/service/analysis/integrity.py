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

# 2. 模板一致性检查
class TemplateConsistencyChecker:
    def __init__(self):
        # 需求说明：公司提供的可能用到的近义词库（后续可从数据库动态加载）
        self.synonyms_dict = {
            "投标承诺书": "投标保证书",
            "公开招标": "招标",
            "邀请招标": "招标",
            "询价": "招标",
            "竞争性谈判": "招标"
        }

    def _normalize_text(self, text: str) -> str:
        """
        文本高强度清洗与预处理
        """
        if not text:
            return ""
        
        # 1. 替换近义词
        for variant, standard in self.synonyms_dict.items():
            text = text.replace(variant, standard)

        # 2. 统一中英文标点符号
        text = text.replace(',', '，').replace('(', '（').replace(')', '）').replace(':', '：')
        
        # 3. 去除所有空白字符（空格、换行、制表），将文本“压扁”为纯字符流
        text = re.sub(r'\s+', '', text)
        return text

    def _extract_template_skeleton(self, template_text: str) -> str:
        """
        提取模板骨架：将模板中的填空处剔除，只保留核心固定条款。
        """
        # 移除下划线填空区
        skeleton = re.sub(r'_{2,}', '', template_text)
        # 移除括号指示区，例如“（项目名称）”、“（招标人）”等
        skeleton = re.sub(r'（[^）]{1,15}）', '', skeleton)
        return self._normalize_text(skeleton)

    def check_consistency(self, template_text: str, bidder_text: str) -> dict:
        """
        模板一致性深度比对
        """
        norm_template = self._extract_template_skeleton(template_text)
        norm_bidder = self._normalize_text(bidder_text)

        matcher = difflib.SequenceMatcher(None, norm_template, norm_bidder)
        
        issues = []
        tampered_chars = 0

        for tag, i1, i2, j1, j2 in matcher.get_opcodes():
            if tag == 'delete':
                # 模板中有的字，投标书里没有（恶意删除约束条款）
                deleted_str = norm_template[i1:i2]
                if len(deleted_str) >= 4:
                    issues.append({
                        "issue_type": "核心条款被删除",
                        "content_preview": deleted_str
                    })
                    tampered_chars += len(deleted_str)
                    
            elif tag == 'replace':
                # 模板中的字被替换成了别的字（私自篡改条款）
                replaced_template = norm_template[i1:i2]
                replaced_bidder = norm_bidder[j1:j2]
                if len(replaced_template) >= 4:
                    issues.append({
                        "issue_type": "核心条款被篡改",
                        "original_template": replaced_template,
                        "modified_to": replaced_bidder
                    })
                    tampered_chars += len(replaced_template)
            
            # 不处理 'insert' (新增)，允许乙方在模板上正常填空

        is_standard = len(issues) == 0
        score = 100 if is_standard else max(0, 100 - len(issues) * 5)

        return {
            "format_score": score,
            "is_consistent": is_standard,
            "tampered_issues_count": len(issues),
            "detected_issues": issues,
            "suggestion": "未发现模板条款被篡改，格式一致" if is_standard else "检测到模板核心条款被删除或修改，将在系统中标红！"
        }