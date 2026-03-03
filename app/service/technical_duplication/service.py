from typing import Dict, List, Any
import re

class TechnicalDuplicationService:
    @staticmethod
    def check_duplication(text: str, historical_texts: List[str] = None) -> Dict[str, Any]:
        """检查技术标查重"""
        if historical_texts is None:
            historical_texts = []
        
        similarities = []
        for i, historical_text in enumerate(historical_texts):
            similarity = TechnicalDuplicationService.calculate_similarity(text, historical_text)
            similarities.append({
                "document_id": i + 1,
                "similarity": similarity
            })
        
        # 按相似度排序
        similarities.sort(key=lambda x: x["similarity"], reverse=True)
        
        # 检查是否有高度相似的文档
        high_similarity_docs = [doc for doc in similarities if doc["similarity"] > 0.7]
        
        return {
            "status": "success",
            "similarity_checks": similarities,
            "high_similarity_docs": high_similarity_docs,
            "has_high_duplication": len(high_similarity_docs) > 0
        }
    
    @staticmethod
    def check_technical_content(text: str) -> Dict[str, Any]:
        """检查技术标内容"""
        # 检查技术标必要内容
        technical_sections = [
            "施工方案",
            "技术措施",
            "质量保证",
            "安全措施",
            "进度计划"
        ]
        
        found_sections = []
        missing_sections = []
        
        for section in technical_sections:
            if section in text:
                found_sections.append(section)
            else:
                missing_sections.append(section)
        
        # 检查技术参数
        has_technical_params = "参数" in text or "技术指标" in text
        has_implementation_plan = "实施方案" in text or "实施计划" in text
        has_risk_management = "风险管理" in text or "风险控制" in text
        
        return {
            "status": "success",
            "found_sections": found_sections,
            "missing_sections": missing_sections,
            "has_technical_params": has_technical_params,
            "has_implementation_plan": has_implementation_plan,
            "has_risk_management": has_risk_management,
            "content_score": len(found_sections) / len(technical_sections) * 100
        }
    
    @staticmethod
    def calculate_similarity(text1: str, text2: str) -> float:
        """计算文本相似度"""
        # 简单的相似度计算：共同词的比例
        words1 = set(re.findall(r'\b\w+\b', text1.lower()))
        words2 = set(re.findall(r'\b\w+\b', text2.lower()))
        
        if not words1 or not words2:
            return 0.0
        
        common_words = words1.intersection(words2)
        return len(common_words) / len(words1.union(words2))
    
    @staticmethod
    def extract_technical_parameters(text: str) -> List[str]:
        """提取技术参数"""
        # 简单的技术参数提取逻辑
        param_patterns = [
            r'[A-Za-z0-9]+[参数指标].*?[0-9]+[.，,]*[0-9]*',
            r'[参数指标][：:].*?[0-9]+[.，,]*[0-9]*',
            r'[0-9]+[.，,]*[0-9]*[A-Za-z%]+'
        ]
        
        params = []
        for pattern in param_patterns:
            matches = re.findall(pattern, text, re.DOTALL)
            params.extend(matches)
        
        return params