from typing import Literal
from pydantic import BaseModel, Field

class TextAnalysisRequest(BaseModel):
    """统一文本分析请求模型。"""
    # task_type 决定后端执行的分析分支。
    task_type: Literal[
        "business_format",
        "business_sections",
        "technical_content",
        "extract_parameters",
    ]
    # 待分析文本，入参前置做最小长度限制。
    text: str = Field(..., min_length=1)