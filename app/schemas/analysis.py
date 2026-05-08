from typing import Literal

from pydantic import BaseModel, Field

TextTaskType = Literal[
    "integrity_check",
    "pricing_reason",
    "itemized_pricing",
    "deviation_check",
    "full_analysis",
]

ProjectAnalysisService = Literal[
    "business_bid_format_review",
    "business_bid_duplicate_check",
    "technical_bid_duplicate_check",
    "personnel_reuse_check",
    "typo_check",
]


class TextAnalysisRequest(BaseModel):
    """统一分析请求模型。"""

    task_type: TextTaskType | None = Field(
        default=None,
        description="文本分析任务类型。",
    )
    text: str | None = Field(
        default=None,
        min_length=1,
        description="待分析文本；用于 task_type 文本分析模式。",
    )
    project_identifier: str | None = Field(
        default=None,
        description="项目标识；用于按项目执行业务分析服务。",
    )
    services: list[ProjectAnalysisService] | None = Field(
        default=None,
        description=(
            "需要执行的项目分析服务，可选：business_bid_format_review、"
            "business_bid_duplicate_check、technical_bid_duplicate_check、"
            "personnel_reuse_check、typo_check。"
        ),
    )
    max_evidence_sections: int = Field(
        default=5,
        ge=1,
        le=20,
        description="查重类服务每组最多返回的证据章节数。",
    )
    max_pairs_per_type: int = Field(
        default=0,
        ge=0,
        le=500,
        description="查重类服务每类文档最多返回的对比对数量，0 表示不截断。",
    )
