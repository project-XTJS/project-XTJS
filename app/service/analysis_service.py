import re
from functools import lru_cache
from typing import Any, Dict, List

from app.config.ocr import OCRConfig
from app.service.ocr_service import OCRService
from app.utils.text_utils import extract_text, preprocess_text


class AnalysisService:
    """统一分析服务：聚合文本抽取、格式检查与参数提取能力。"""

    OCR_IMAGE_EXTENSIONS = {"png", "jpg", "jpeg", "bmp", "tif", "tiff"}
    DIRECT_TEXT_EXTENSIONS = {"pdf", "docx", "doc"}
    SUPPORTED_EXTENSIONS = OCR_IMAGE_EXTENSIONS | DIRECT_TEXT_EXTENSIONS

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
    TECHNICAL_PARAMETER_PATTERNS = [
        r"[A-Za-z0-9]+[参数技术指标].*?[0-9]+[.]?[0-9]*",
        r"[参数技术指标][:：].*?[0-9]+[.]?[0-9]*",
        r"[0-9]+[.]?[0-9]*[A-Za-z%]+",
    ]

    def __init__(self) -> None:
        # OCR 仅在需要识别扫描 PDF 时启用。
        self.ocr_service = OCRService()

    @classmethod
    def get_supported_extensions(cls) -> List[str]:
        return sorted(cls.SUPPORTED_EXTENSIONS)

    def extract_text_result(self, file_path: str, file_type: str) -> Dict[str, Any]:
        """统一抽取文本并返回 OCR 元信息。"""
        normalized_file_type = (file_type or "").strip().lower()
        if normalized_file_type not in self.SUPPORTED_EXTENSIONS:
            supported = ",".join(self.get_supported_extensions())
            raise ValueError(
                f"Unsupported file type: {normalized_file_type}. Supported types: {supported}."
            )

        if normalized_file_type in self.OCR_IMAGE_EXTENSIONS:
            return self._extract_image_result(file_path, normalized_file_type)

        if normalized_file_type == "pdf":
            return self._extract_pdf_result(file_path)

        text = preprocess_text(extract_text(file_path, normalized_file_type))
        return self._build_result(
            content=text,
            file_type=normalized_file_type,
            parser_engine=normalized_file_type,
            source_mode="native_parser",
            ocr_engine="",
            ocr_used=False,
        )

    def extract_text_with_ocr(self, file_path: str, file_type: str) -> str:
        """兼容旧调用：返回纯文本内容。"""
        return self.extract_text_result(file_path, file_type)["content"]

    def _extract_pdf_result(self, file_path: str) -> Dict[str, Any]:
        if not OCRConfig.FORCE_PDF_OCR:
            text = extract_text(file_path, "pdf")
            if text.strip():
                return self._build_result(
                    content=preprocess_text(text),
                    file_type="pdf",
                    parser_engine="pdfplumber",
                    source_mode="text_layer",
                    ocr_engine="",
                    ocr_used=False,
                )

        ocr_result = self.ocr_service.recognize_pdf_result(file_path)
        return self._normalize_ocr_result(ocr_result, file_type="pdf")

    def _extract_image_result(self, file_path: str, file_type: str) -> Dict[str, Any]:
        ocr_result = self.ocr_service.recognize_image_result(file_path)
        return self._normalize_ocr_result(ocr_result, file_type=file_type)

    def _normalize_ocr_result(self, ocr_result: Dict[str, Any], *, file_type: str) -> Dict[str, Any]:
        content = ocr_result["content"]
        if not ocr_result["success"]:
            raise RuntimeError(content)

        return self._build_result(
            content=preprocess_text(content),
            file_type=file_type,
            parser_engine=ocr_result["ocr_engine"] or "PaddleOCR 3.x",
            source_mode="ocr",
            ocr_engine=ocr_result["ocr_engine"],
            ocr_used=True,
            active_device=ocr_result["active_device"],
            seal_enabled=ocr_result.get("seal_enabled", False),
            seal_removed=ocr_result.get("seal_removed", False),
            seal_detected=ocr_result.get("seal_detected", False),
            seal_count=ocr_result.get("seal_count", 0),
            seal_texts=ocr_result.get("seal_texts", []),
        )

    def _build_result(
        self,
        *,
        content: str,
        file_type: str,
        parser_engine: str,
        source_mode: str,
        ocr_engine: str,
        ocr_used: bool,
        active_device: str | None = None,
        seal_enabled: bool = False,
        seal_removed: bool = False,
        seal_detected: bool = False,
        seal_count: int = 0,
        seal_texts: List[str] | None = None,
    ) -> Dict[str, Any]:
        return {
            "file_type": file_type,
            "content": content,
            "text_length": len(content),
            "parser_engine": parser_engine,
            "source_mode": source_mode,
            "ocr_engine": ocr_engine,
            "ocr_used": ocr_used,
            "active_device": active_device or self.ocr_service.active_device,
            "ocr_available": self.ocr_service.is_available(),
            "seal_enabled": seal_enabled,
            "seal_removed": seal_removed,
            "seal_detected": seal_detected,
            "seal_count": seal_count,
            "seal_texts": seal_texts or [],
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

    def extract_parameters(self, text: str) -> List[str]:
        """提取技术参数字段。"""
        parameters: List[str] = []
        for pattern in self.TECHNICAL_PARAMETER_PATTERNS:
            parameters.extend(re.findall(pattern, text, re.DOTALL))
        return parameters


@lru_cache(maxsize=1)
def get_analysis_service() -> AnalysisService:
    """返回单例分析服务，避免 OCR 模型重复初始化。"""
    return AnalysisService()
