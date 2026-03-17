# app/service/analysis_service.py
from functools import lru_cache
from app.service.ocr_service import OCRService
from app.utils.text_utils import preprocess_text

# 导入拆分后的子模块（确保路径正确）
from app.service.analysis.integrity import IntegrityChecker
from app.service.analysis.pricing_reasonableness import ReasonablenessChecker
from app.service.analysis.itemized_pricing import ItemizedPricingChecker
from app.service.analysis.deviation import DeviationChecker
from app.service.analysis.verification import VerificationChecker

class AnalysisService:
    def __init__(self, ocr_service: OCRService) -> None:
        self.ocr_service = ocr_service
        # 初始化各成员负责的业务组件
        self.integrity = IntegrityChecker()
        self.reasonableness = ReasonablenessChecker()
        self.itemized = ItemizedPricingChecker()
        self.deviation = DeviationChecker()
        self.verification = VerificationChecker(ocr_service)

    def run_full_analysis(self, text: str, extraction_meta: dict) -> dict:
        """聚合分析逻辑，返回完整结果。"""
        clean_text = preprocess_text(text)
        return {
            "integrity_result": self.integrity.check_integrity(clean_text),
            "pricing_reasonableness": self.reasonableness.check_price_reasonableness(clean_text),
            "itemized_check": self.itemized.check_itemized_logic(clean_text),
            "deviation_result": self.deviation.check_technical_deviation(clean_text),
            "verification_result": self.verification.check_seal_and_date(extraction_meta)
        }

@lru_cache(maxsize=1)
def get_analysis_service() -> AnalysisService:
    """提供给 dependencies.py 调用的单例工厂函数"""
    return AnalysisService(ocr_service=OCRService())