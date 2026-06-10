# -*- coding: utf-8 -*-
"""
统一分析请求的 Pydantic 模型。

定义文本分析任务类型、项目分析服务类型，以及 TextAnalysisRequest。
"""

from typing import Any, Literal

from pydantic import BaseModel, Field

# 文本分析任务类型
TextTaskType = Literal[
    "integrity_check",
    "consistency_check",
    "verification_check",
    "pricing_reason",
    "itemized_pricing",
    "deviation_check",
    "full_analysis",
]

# 项目级分析服务类型
ProjectAnalysisService = Literal[
    "business_bid_format_review",
    "deviation_check",
    "business_bid_duplicate_check",
    "business_itemized_duplicate_check",
    "bid_response_duplicate_check",
    "technical_bid_duplicate_check",
    "personnel_reuse_check",
    "typo_check",
]


class TextAnalysisRequest(BaseModel):
    """统一分析请求模型，可同时用于文本分析或项目级业务分析。"""

    task_type: TextTaskType | None = Field(
        default=None,
        description="文本分析任务类型（文本分析模式使用）。",
    )
    text: str | None = Field(
        default=None,
        min_length=1,
        description=(
            "待分析文本；用于 task_type 文本分析模式。"
            "其中 consistency_check 需要传入包含模板文档与待检文档的 JSON 字符串，"
            "verification_check 可传单文档/招投标文档对的 JSON 字符串，"
            "也可直接传纯文本做单文档兜底检查。"
        ),
    )
    project_identifier: str | None = Field(
        default=None,
        description="项目标识（项目分析模式使用）。",
    )
    services: list[ProjectAnalysisService] | None = Field(
        default=None,
        description=(
            "需要执行的项目分析服务，可选：business_bid_format_review、"
            "deviation_check、business_bid_duplicate_check、technical_bid_duplicate_check、"
            "personnel_reuse_check、typo_check。"
        ),
    )
    confirmed_personnel_names: list[Any] | None = Field(
        default=None,
        description="人员复用检查使用的业务确认后人名列表；为空时返回待确认的抽取名单。",
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
