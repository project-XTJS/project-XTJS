# app/service/analysis_service.py
from functools import lru_cache
from app.service.ocr_service import OCRService
from app.utils.text_utils import preprocess_text, extract_file_data 

from app.service.analysis.integrity import IntegrityChecker
from app.service.analysis.pricing_reasonableness import ReasonablenessChecker
from app.service.analysis.itemized_pricing import ItemizedPricingChecker
from app.service.analysis.deviation import DeviationChecker
from app.service.analysis.verification import VerificationChecker

class AnalysisService:
    def __init__(self, ocr_service: OCRService) -> None:
        self.ocr_service = ocr_service
        self.integrity = IntegrityChecker()
        self.reasonableness = ReasonablenessChecker()
        self.itemized = ItemizedPricingChecker()
        self.deviation = DeviationChecker()
        self.verification = VerificationChecker(ocr_service)
    
    def get_supported_extensions(self) -> list:
        return ["pdf", "docx", "jpg", "jpeg", "png"]
    
    def extract_text_result(self, file_path: str, file_extension: str) -> dict:
        """
        核心调度方法：获取真实的物理文件数据，动态返回元数据。
        """
        # 1. 获取底层真实的物理文件解析数据
        file_data = extract_file_data(file_path, file_extension)
        raw_text = file_data.get("content", "")
        
        # 2. 判断是否需要 OCR 介入（例如纯图片，或者 PDF 解析出文字极少被判定为扫描件）
        ocr_used = False
        seal_data = {"count": 0, "texts": []}
        
        # 如果是图片，或者提取的文字小于 50 个字符（大概率是纯图片 PDF）
        if file_extension in ["jpg", "jpeg", "png"] or len(raw_text.strip()) < 50:
            try:
                # TODO: 依赖高海斌的 ocr_service.extract_all() 完成图片内容提取
                # 假设高海斌的接口返回: {"text": "识别结果", "seals": {"count": 1, "texts": ["公章"]}}
                if hasattr(self.ocr_service, "extract_all"):
                    ocr_result = self.ocr_service.extract_all(file_path)
                    raw_text = ocr_result.get("text", raw_text)
                    seal_data = ocr_result.get("seals", seal_data)
                    ocr_used = True
                    
                    # 更新图片识别的页级数据
                    file_data["pages"] = [{"page": 1, "text": raw_text}]
            except Exception as e:
                print(f"调用 OCR 服务异常: {e}")
        
        # 3. 动态组装所有真实字段
        return {
            "content": raw_text,                         
            "text_length": len(raw_text),
            "pages": file_data["pages"],               # 真实的每一页内容
            "page_count": file_data["page_count"],     # 真实的物理页数
            
            # --- 引擎与模式相关 ---
            "parser_engine": file_data["parser_engine"], # 动态引擎 (PyMuPDF/docx 等)
            "source_mode": "local",
            "active_device": "cuda" if getattr(self.ocr_service, 'use_gpu', False) else "cpu",
            
            # --- OCR 相关 ---
            "ocr_engine": "RapidOCR" if ocr_used else "none",
            "ocr_used": ocr_used,
            "ocr_available": self.ocr_service is not None,
            
            # --- 印章相关 ---
            "seal_enabled": True, # 假设全局开启
            "seal_removed": False,
            "seal_detected": seal_data.get("count", 0) > 0,
            "seal_count": seal_data.get("count", 0),
            "seal_texts": seal_data.get("texts", [])
        }
    
    def run_full_analysis(self, text: str, extraction_meta: dict) -> dict:
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
    return AnalysisService(ocr_service=OCRService())