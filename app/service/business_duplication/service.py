from typing import Dict, List, Any
import re

class BusinessDuplicationService:
    @staticmethod
    def check_duplication(text: str, historical_texts: List[str] = None) -> Dict[str, Any]:
        """检查商务标查重"""
        if historical_texts is None:
            historical_texts = []
        
        similarities = []
        for i, historical_text in enumerate(historical_texts):
            similarity = BusinessDuplicationService.calculate_similarity(text, historical_text)
            similarities.append({
                "document_id": i + 1,
                "similarity": similarity
            })
        
        # 按相似度排序
        similarities.sort(key=lambda x: x["similarity"], reverse=True)
        
        # 检查是否有高度相似的文档
        high_similarity_docs = [doc for doc in similarities if doc["similarity"] > 0.8]
        
        return {
            "status": "success",
            "similarity_checks": similarities,
            "high_similarity_docs": high_similarity_docs,
            "has_high_duplication": len(high_similarity_docs) > 0
        }
    
    @staticmethod
    def check_quote_duplication(text: str, historical_quotes: List[str] = None) -> Dict[str, Any]:
        """检查报价查重"""
        if historical_quotes is None:
            historical_quotes = []
        
        # 提取报价信息
        quotes = BusinessDuplicationService.extract_quotes(text)
        
        quote_similarities = []
        for i, historical_quote in enumerate(historical_quotes):
            historical_quote_items = BusinessDuplicationService.extract_quotes(historical_quote)
            for quote in quotes:
                for historical_quote_item in historical_quote_items:
                    similarity = BusinessDuplicationService.calculate_similarity(quote, historical_quote_item)
                    if similarity > 0.7:
                        quote_similarities.append({
                            "document_id": i + 1,
                            "quote": quote,
                            "historical_quote": historical_quote_item,
                            "similarity": similarity
                        })
        
        # 按相似度排序
        quote_similarities.sort(key=lambda x: x["similarity"], reverse=True)
        
        return {
            "status": "success",
            "quotes": quotes,
            "quote_duplication_checks": quote_similarities,
            "has_quote_duplication": len(quote_similarities) > 0
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
    def extract_quotes(text: str) -> List[str]:
        """提取报价信息"""
        # 简单的报价提取逻辑
        quote_patterns = [
            r'报价[：:].*?[0-9]+[.，,]*[0-9]*',
            r'价格[：:].*?[0-9]+[.，,]*[0-9]*',
            r'总[价计].*?[0-9]+[.，,]*[0-9]*',
            r'[0-9]+[.，,]*[0-9]*[元万元]'
        ]
        
        quotes = []
        for pattern in quote_patterns:
            matches = re.findall(pattern, text, re.DOTALL)
            quotes.extend(matches)
        
        return quotes