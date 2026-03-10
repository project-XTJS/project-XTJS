import re
from typing import Dict, List


def tokenize(text: str) -> List[str]:
    """中英文混合分词，供简易 Jaccard 相似度计算。"""
    if not text:
        return []
    return re.findall(r"[\u4e00-\u9fff]+|[A-Za-z0-9_]+", text.lower())


def jaccard_similarity(text1: str, text2: str) -> float:
    """计算两段文本的 Jaccard 相似度。"""
    words1 = set(tokenize(text1))
    words2 = set(tokenize(text2))
    if not words1 or not words2:
        return 0.0
    return len(words1.intersection(words2)) / len(words1.union(words2))


def calculate_similarity_list(
    source_text: str,
    historical_texts: List[str],
    threshold: float,
) -> Dict[str, object]:
    """批量计算当前文本与历史文本的相似度并按阈值筛选。"""
    similarities = []
    for index, historical_text in enumerate(historical_texts):
        score = jaccard_similarity(source_text, historical_text)
        similarities.append(
            {
                "document_id": index + 1,
                "similarity": score,
            }
        )

    similarities.sort(key=lambda item: item["similarity"], reverse=True)
    high_similarity_docs = [
        item for item in similarities if item["similarity"] >= threshold
    ]
    return {
        "similarity_checks": similarities,
        "high_similarity_docs": high_similarity_docs,
        "has_high_duplication": len(high_similarity_docs) > 0,
    }


def extract_quotes(text: str) -> List[str]:
    """按规则提取报价相关片段。"""
    patterns = [
        r"报价[：:].*?[0-9]+[.，,]*[0-9]*",
        r"价格[：:].*?[0-9]+[.，,]*[0-9]*",
        r"总[价计].*?[0-9]+[.，,]*[0-9]*",
        r"[0-9]+[.，,]*[0-9]*[元万元]",
    ]
    quotes: List[str] = []
    for pattern in patterns:
        quotes.extend(re.findall(pattern, text, re.DOTALL))
    return quotes


def extract_technical_parameters(text: str) -> List[str]:
    """按规则提取技术参数片段。"""
    patterns = [
        r"[A-Za-z0-9]+[参数指标].*?[0-9]+[.，,]*[0-9]*",
        r"[参数指标][：:].*?[0-9]+[.，,]*[0-9]*",
        r"[0-9]+[.，,]*[0-9]*[A-Za-z%]+",
    ]
    parameters: List[str] = []
    for pattern in patterns:
        parameters.extend(re.findall(pattern, text, re.DOTALL))
    return parameters
