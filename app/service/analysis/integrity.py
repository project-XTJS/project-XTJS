"""
投标文件完整性与格式检查模块
负责人：虞光勇、陶明宇
"""
import re

class IntegrityChecker:
    # 商务标关键章节白名单
    BUSINESS_REQUIRED_SECTIONS = ["报价函", "商务偏离表", "资格证明文件", "售后服务承诺", "报价明细"]

    def check_integrity(self, text: str) -> dict:
        """
        虞光勇：检查必备章节是否齐全
        升级亮点：使用多行正则匹配，彻底排除“目录”或“正文提及”造成的误判。
        必须是独立成行，且带有类似“一、”、“1.”、“附件一”前缀的才算真正的章节标题。
        """
        found_sections = []
        missing_sections = []
        details = {}

        # 匹配常见的标书标题前缀：一、 / 1. / 1.1 / 第一章 / 附件一 等
        # \s* 代表允许有空格
        header_prefix = r'(?:第[一二三四五六七八九十百]+[章部分]|附件[一二三四五六七八九十]+|[一二三四五六七八九十]、|\d{1,2}\.[\d\.]*)\s*'

        for section in self.BUSINESS_REQUIRED_SECTIONS:
            # 策略1：匹配带编号的正规标题 (必须是在一行的开头)
            # 例如： "一、报价函" 或 "附件一 报价函"
            strict_pattern = re.compile(rf'^{header_prefix}{section}', re.MULTILINE)
            
            # 策略2：匹配不带编号，但独占一行的居中标题
            # 例如： "      报价函      "
            loose_pattern = re.compile(rf'^\s*{section}\s*$', re.MULTILINE)

            # 执行检索
            strict_matches = strict_pattern.findall(text)
            loose_matches = loose_pattern.findall(text)

            if strict_matches or loose_matches:
                found_sections.append(section)
                details[section] = {
                    "status": "✅ 已找到",
                    "match_type": "正规编号标题" if strict_matches else "独立成行标题",
                    "preview": (strict_matches + loose_matches)[0].strip() # 截取第一个匹配项供前端展示
                }
            else:
                missing_sections.append(section)
                details[section] = {"status": "❌ 缺失"}

        # 评分算法：百分制，每缺一项扣对应的分数
        score = (len(found_sections) / len(self.BUSINESS_REQUIRED_SECTIONS)) * 100

        return {
            "integrity_score": round(score, 2),
            "found_count": len(found_sections),
            "missing_count": len(missing_sections),
            "found_sections": found_sections,
            "missing_sections": missing_sections,
            "details": details # 详细的审查报告
        }

    def check_format_consistency(self, text: str) -> dict:
        """
        陶明宇：实现格式模板一致性检查
        注意：由于入参是纯文本(text: str)，无法检查字体(如宋体/黑体)。
        升级亮点：利用纯文本特征，进行标点符号规范、空白行泛滥、中英文混杂等结构性排版检查。
        """
        issues = []
        deduct_score = 0

        # 1. 空行泛滥检查（排版松散）
        if re.search(r'\n{4,}', text):
            issues.append("文档存在连续4个以上的空行，排版可能过于松散或存在异常分页。")
            deduct_score += 5

        # 2. 中英文标点严重混用检查（专业性校验）
        # 统计英文逗号和中文逗号的数量
        en_comma_count = text.count(',')
        zh_comma_count = text.count('，')
        
        # 如果总逗号数大于20，且英文逗号占比超过30%（在中文标书里很不正常）
        total_commas = en_comma_count + zh_comma_count
        if total_commas > 20 and (en_comma_count / total_commas) > 0.3:
            issues.append(f"中英文标点混杂：检测到大量英文半角逗号({en_comma_count}个)，建议统一使用中文全角标点。")
            deduct_score += 10

        # 3. 序号混乱检查（比如出现了 "1." 后紧跟 "3."）
        # 这里仅作简单预警示例，复杂逻辑可由陶明宇继续扩展
        if "一、" in text and "三、" in text and "二、" not in text:
             issues.append("大纲编号可能存在跳跃：检测到'一、'和'三、'，但缺失'二、'。")
             deduct_score += 15

        final_score = max(0, 100 - deduct_score)

        return {
            "format_score": final_score,
            "is_standard": len(issues) == 0,
            "detected_issues": issues,
            "suggestion": "排版规范，符合投标要求" if final_score == 100 else "建议根据检查出的问题重新排版"
        }