from typing import Dict, List, Any

class BusinessFormatService:
    @staticmethod
    def check_format(text: str) -> Dict[str, Any]:
        """检查商务标格式"""
        # 检查是否包含必要的章节
        required_sections = [
            "报价函",
            "商务偏离表",
            "资格证明文件",
            "售后服务承诺",
            "报价明细"
        ]
        
        found_sections = []
        missing_sections = []
        
        for section in required_sections:
            if section in text:
                found_sections.append(section)
            else:
                missing_sections.append(section)
        
        # 检查报价格式
        has_price = "报价" in text or "价格" in text
        has_validity = "有效期" in text
        
        return {
            "status": "success",
            "found_sections": found_sections,
            "missing_sections": missing_sections,
            "has_price": has_price,
            "has_validity": has_validity,
            "format_score": len(found_sections) / len(required_sections) * 100
        }
    
    @staticmethod
    def validate_sections(text: str) -> Dict[str, Any]:
        """验证各章节内容"""
        sections = {
            "报价函": "报价函" in text,
            "商务偏离表": "商务偏离表" in text,
            "资格证明文件": "资格证明文件" in text,
            "售后服务承诺": "售后服务承诺" in text,
            "报价明细": "报价明细" in text
        }
        
        # 检查章节内容完整性
        section_details = {}
        for section, found in sections.items():
            if found:
                # 简单检查章节内容长度
                start_idx = text.find(section)
                if start_idx != -1:
                    # 找到下一个章节的开始位置
                    next_section_idx = float('inf')
                    for other_section in sections:
                        if other_section != section:
                            idx = text.find(other_section, start_idx + len(section))
                            if idx != -1 and idx < next_section_idx:
                                next_section_idx = idx
                    
                    section_content = text[start_idx:next_section_idx]
                    section_details[section] = {
                        "found": True,
                        "length": len(section_content),
                        "has_content": len(section_content) > len(section) + 10
                    }
                else:
                    section_details[section] = {
                        "found": True,
                        "length": 0,
                        "has_content": False
                    }
            else:
                section_details[section] = {
                    "found": False,
                    "length": 0,
                    "has_content": False
                }
        
        return {
            "status": "success",
            "section_details": section_details,
            "overall_score": sum(1 for s in section_details.values() if s["has_content"]) / len(sections) * 100
        }