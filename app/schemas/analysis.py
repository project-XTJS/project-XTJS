from typing import Any, Literal
from pydantic import BaseModel, Field

class TextAnalysisRequest(BaseModel):
    """统一文本分析请求模型。"""
    task_type: Literal[
        "integrity_check",     # 完整性审查
        "pricing_reason",      # 报价合理性
        "itemized_pricing",    # 分项报价
        "deviation_check",     # 偏离检查
        "full_analysis"        # 全量分析
    ]
    text: str = Field(..., min_length=1)


class SignatureCropExportRequest(BaseModel):
    """导出签字位截图请求模型。"""

    tender_document: dict[str, Any] | str
    bid_document: dict[str, Any] | str
    bid_pdf_path: str | None = None
    output_dir: str | None = None
