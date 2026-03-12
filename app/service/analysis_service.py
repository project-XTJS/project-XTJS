from functools import lru_cache
from typing import Any, Dict, List

from app.service.ocr_service import OCRService
from app.utils.similarity_utils import (
    calculate_similarity_list,
    extract_quotes,
    extract_technical_parameters,
    jaccard_similarity,
)
from app.utils.text_utils import extract_text, preprocess_text


class AnalysisService:
    """统一分析服务：聚合格式检查、查重、参数提取等能力。"""

    # 商务标关键章节白名单。
    BUSINESS_REQUIRED_SECTIONS = [
        "报价函",
        "商务偏离表",
        "资格证明文件",
        "售后服务承诺",
        "报价明细",
    ]
    TECHNICAL_REQUIRED_SECTIONS = [
        "施工方案",
        "技术措施",
        "质量保证",
        "安全措施",
        "进度计划",
    ]

    def __init__(self) -> None:
        # OCR 仅在需要识别扫描 PDF 时启用。
        self.ocr_service = OCRService()

    def extract_text_with_ocr(self, file_path: str, file_type: str) -> str:
        """先做常规抽取，必要时回退 OCR。"""
        text = extract_text(file_path, file_type)
        if file_type == "pdf" and not text.strip():
            ocr_text = self.ocr_service.recognize_pdf(file_path)
            if ocr_text.startswith("PaddleOCR is not available") or ocr_text.startswith(
                "Error during"
            ):
                raise RuntimeError(ocr_text)
            text = ocr_text
        return preprocess_text(text)

    def summarize_text(self, text: str) -> Dict[str, Any]:
        """返回文本长度、词数与预览内容。"""
        return {
            "text_length": len(text),
            "word_count": len(text.split()),
            "sample_content": f"{text[:500]}..." if len(text) > 500 else text,
        }

    def check_business_format(self, text: str) -> Dict[str, Any]:
        """商务标格式检查：章节齐全性 + 关键字段存在性。"""
        found_sections = [s for s in self.BUSINESS_REQUIRED_SECTIONS if s in text]
        missing_sections = [s for s in self.BUSINESS_REQUIRED_SECTIONS if s not in text]
        has_price = "报价" in text or "价格" in text
        has_validity = "有效期" in text
        return {
            "status": "success",
            "found_sections": found_sections,
            "missing_sections": missing_sections,
            "has_price": has_price,
            "has_validity": has_validity,
            "format_score": len(found_sections) / len(self.BUSINESS_REQUIRED_SECTIONS) * 100,
        }

    def check_technical_content(self, text: str) -> Dict[str, Any]:
        """技术标内容检查：章节覆盖与核心要素。"""
        found_sections = [s for s in self.TECHNICAL_REQUIRED_SECTIONS if s in text]
        missing_sections = [s for s in self.TECHNICAL_REQUIRED_SECTIONS if s not in text]
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
            "content_score": len(found_sections) / len(self.TECHNICAL_REQUIRED_SECTIONS) * 100,
        }

    def validate_business_sections(self, text: str) -> Dict[str, Any]:
        """商务章节校验：定位章节并估算内容完整度。"""
        section_details: Dict[str, Dict[str, Any]] = {}
        for section in self.BUSINESS_REQUIRED_SECTIONS:
            found = section in text
            if not found:
                section_details[section] = {"found": False, "length": 0, "has_content": False}
                continue

            start_index = text.find(section)
            next_index = len(text)
            for other_section in self.BUSINESS_REQUIRED_SECTIONS:
                if other_section == section:
                    continue
                candidate = text.find(other_section, start_index + len(section))
                if candidate != -1 and candidate < next_index:
                    next_index = candidate

            # 取当前章节到下一个章节之间的内容长度作为近似指标。
            content = text[start_index:next_index]
            section_details[section] = {
                "found": True,
                "length": len(content),
                "has_content": len(content) > len(section) + 10,
            }

        overall_score = (
            sum(1 for detail in section_details.values() if detail["has_content"])
            / len(self.BUSINESS_REQUIRED_SECTIONS)
            * 100
        )
        return {"status": "success", "section_details": section_details, "overall_score": overall_score}

    def check_duplication(
        self, text: str, historical_texts: List[str], mode: str = "business"
    ) -> Dict[str, Any]:
        """统一查重入口：根据模式切换相似度阈值。"""
        threshold = 0.8 if mode == "business" else 0.7
        result = calculate_similarity_list(text, historical_texts, threshold)
        result["status"] = "success"
        result["threshold"] = threshold
        return result

    def check_quote_duplication(self, text: str, historical_quotes: List[str]) -> Dict[str, Any]:
        """报价查重：从文本中提取报价片段后逐条比对。"""
        quotes = extract_quotes(text)
        quote_similarities: List[Dict[str, Any]] = []

        for index, historical_quote in enumerate(historical_quotes):
            historical_quote_items = extract_quotes(historical_quote)
            for quote in quotes:
                for historical_item in historical_quote_items:
                    similarity = jaccard_similarity(quote, historical_item)
                    if similarity >= 0.7:
                        quote_similarities.append(
                            {
                                "document_id": index + 1,
                                "quote": quote,
                                "historical_quote": historical_item,
                                "similarity": similarity,
                            }
                        )

        quote_similarities.sort(key=lambda item: item["similarity"], reverse=True)
        return {
            "status": "success",
            "quotes": quotes,
            "quote_duplication_checks": quote_similarities,
            "has_quote_duplication": len(quote_similarities) > 0,
        }

    def extract_parameters(self, text: str) -> List[str]:
        """提取技术参数字段。"""
        return extract_technical_parameters(text)


@lru_cache(maxsize=1)
def get_analysis_service() -> AnalysisService:
    """返回单例分析服务，避免 OCR 模型重复初始化。"""
    return AnalysisService()
