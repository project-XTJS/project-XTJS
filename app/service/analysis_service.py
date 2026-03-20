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
    SUPPORTED_EXTENSIONS = ["pdf", "jpg", "jpeg", "png"]

    def __init__(self, ocr_service: OCRService) -> None:
        self.ocr_service = ocr_service
        self.integrity = IntegrityChecker()
        self.reasonableness = ReasonablenessChecker()
        self.itemized = ItemizedPricingChecker()
        self.deviation = DeviationChecker()
        self.verification = VerificationChecker(ocr_service)
    
    def get_supported_extensions(self) -> list:
        return self.SUPPORTED_EXTENSIONS.copy()
    
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
        
        if file_extension in ["jpg", "jpeg", "png"] or len(raw_text.strip()) < 50:
            try:
                if hasattr(self.ocr_service, "extract_all"):
                    # 传入 file_extension，确保 OCR 服务知道是图片还是 PDF
                    ocr_result = self.ocr_service.extract_all(file_path, file_extension)
                    
                    raw_text = ocr_result.get("text", raw_text)
                    seal_data = ocr_result.get("seals", seal_data)
                    ocr_used = True
                    
                    # 使用 PaddleOCR 逐页解析出的完美数据
                    if "pages" in ocr_result and ocr_result["pages"]:
                        file_data["pages"] = ocr_result["pages"]
                    else:
                        file_data["pages"] = [{"page": 1, "text": raw_text}]
            except Exception as e:
                print(f"调用 OCR 服务异常: {e}")
        
        # 3. 动态组装所有真实字段
        return {
            "content": raw_text,                         
            "text_length": len(raw_text),
            "pages": file_data.get("pages", []),               
            "page_count": file_data.get("page_count", 1),     
            
            # --- 引擎与模式相关 ---
            "parser_engine": file_data.get("parser_engine", "unknown"), 
            "source_mode": "local",
            "active_device": getattr(self.ocr_service, "active_device", "cpu"),
            
            # --- OCR 相关 ---
            "ocr_engine": "PaddleOCR" if ocr_used else "none",
            "ocr_used": ocr_used,
            "ocr_available": getattr(self.ocr_service, 'available', False),
            
            # --- 印章相关 ---
            "seal_enabled": True, 
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
