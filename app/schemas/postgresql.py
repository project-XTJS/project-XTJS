# -*- coding: utf-8 -*-
"""
PostgreSQL 相关请求模型定义。

包含项目、文档、关系、分析结果等 CRUD 操作所需的 Pydantic 模型，
以及查重范围枚举。
"""

from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field


class DocumentDataModel(BaseModel):
    """文档基础数据模型（UUID 标识、文件名、URL）。"""
    identifier_id: str
    file_name: str
    file_url: str


class ProjectCreateRequest(BaseModel):
    """创建项目请求，项目 UUID 由系统自动生成。"""
    project_name: str = Field(..., min_length=1, description="用户输入的项目名称。")


class DocumentCreateRequest(BaseModel):
    """创建文档请求，文档 UUID 由系统自动生成。"""
    file_name: str
    file_url: str = Field(
        ...,
        description="持久化对象存储地址，例如 minio://bucket/object。",
    )


class ProjectBindDocumentsRequest(BaseModel):
    """绑定招标、商务标、技术标到项目。"""
    tender_document_identifier: str
    business_bid_document_identifier: str
    technical_bid_document_identifier: Optional[str] = None


class ProjectUpdateRequest(BaseModel):
    """更新项目名称请求。"""
    project_name: Optional[str] = None


class DocumentUpdateRequest(BaseModel):
    """更新文档信息请求。"""
    file_name: Optional[str] = None
    file_url: Optional[str] = Field(
        default=None,
        description="持久化对象存储地址，例如 minio://bucket/object。",
    )


class DocumentPreviewRequest(BaseModel):
    """POST preview payload for long highlight parameters."""
    model_config = ConfigDict(extra="ignore")

    highlight: Optional[list[str] | str] = Field(default=None)
    highlight_bbox: Optional[list[float] | str] = Field(default=None)
    highlight_rects: Optional[list[list[float]] | str] = Field(default=None)
    highlight_coordinate_space: Optional[str] = Field(default="auto")


class ProjectRelationUpdateRequest(BaseModel):
    """更新项目文档绑定关系请求。"""
    tender_document_identifier: str
    business_bid_document_identifier: str
    technical_bid_document_identifier: Optional[str] = None


class IdentifierBatchDeleteRequest(BaseModel):
    """批量删除 UUID 标识请求（适用于项目或文档）。"""
    identifier_ids: list[str] = Field(
        ...,
        min_length=1,
        description="批量删除的 UUID 标识列表。",
    )


class RelationBatchDeleteRequest(BaseModel):
    """批量删除关联关系请求。"""
    relation_ids: list[int] = Field(
        ...,
        min_length=1,
        description="批量删除的关联主键列表。",
    )


class ProjectResultUpsertRequest(BaseModel):
    """创建或覆盖项目分析结果请求。"""
    model_config = ConfigDict(extra="forbid")

    project_identifier_id: str = Field(..., description="项目 UUID 标识。")
    result: dict[str, Any] = Field(..., description="完整结果 JSON 对象。")


class ProjectResultUpdateRequest(BaseModel):
    """更新项目分析结果请求。"""
    model_config = ConfigDict(extra="forbid")

    result: dict[str, Any] = Field(..., description="完整结果 JSON 对象。")


class BusinessBidManualReviewInputItem(BaseModel):
    """A single editable business-bid review value confirmed by a user."""
    model_config = ConfigDict(extra="allow")

    editable_id: str = Field(..., min_length=1, description="Stable editable item id.")
    result_path: str = Field(..., min_length=1, description="JSON path of the source result value.")
    bidder_key: Optional[str] = Field(default=None, description="Bidder key in the review result.")
    check_code: str = Field(..., min_length=1, description="Review check code.")
    field_group: str = Field(..., min_length=1, description="Editable field group.")
    field_name: str = Field(..., min_length=1, description="Editable field name.")
    original_value: Any = Field(default=None, description="Original OCR/review value.")
    manual_value: Any = Field(default=None, description="Manual correction value.")
    page_refs: list[int] = Field(default_factory=list, description="Related page numbers.")
    document_identifier_id: Optional[str] = Field(default=None, description="Related document UUID.")
    updated_at: Optional[str] = Field(default=None, description="Client-side update timestamp.")


class BusinessBidManualReviewInputsRequest(BaseModel):
    """Manual editable values for business-bid format review."""
    model_config = ConfigDict(extra="forbid")

    items: list[BusinessBidManualReviewInputItem] = Field(
        default_factory=list,
        description="Manual correction items.",
    )


class DocumentReviewContentUpdateRequest(BaseModel):
    """Update a document manual OCR working copy without changing raw OCR content."""
    model_config = ConfigDict(extra="forbid")

    effective_content: dict[str, Any] = Field(
        ...,
        description="Latest effective OCR content used by subsequent analysis.",
    )
    inputs: dict[str, Any] = Field(
        default_factory=dict,
        description="Manual inputs grouped by business domain.",
    )


class ProjectManualReviewResultInputsRequest(BaseModel):
    """Manual judgment inputs applied to one latest review result key."""
    model_config = ConfigDict(extra="allow")

    inputs: dict[str, Any] = Field(default_factory=dict, description="Manual correction payload.")


class ProjectReportExportRequest(BaseModel):
    """Stateless report export payload based on the current display result selection."""
    model_config = ConfigDict(extra="forbid")

    result: dict[str, Any] = Field(..., description="Filtered display result used only for this export.")


class ProjectWorkflowScopeRequest(BaseModel):
    """Workflow scope controls such as soft-excluded bidders."""
    model_config = ConfigDict(extra="allow")

    excluded_bidders: list[dict[str, Any]] = Field(
        default_factory=list,
        description="Soft-excluded bidder records.",
    )


class ProjectManualReviewRerunRequest(BaseModel):
    """Explicitly rerun selected services and save them as latest manual results."""
    model_config = ConfigDict(extra="forbid")

    services: list[str] = Field(default_factory=list, min_length=1)


class DuplicateCheckScope(str, Enum):
    """查重范围枚举。"""
    ALL = "all"
    BUSINESS_BID = "business_bid"
    TECHNICAL_BID = "technical_bid"


class ProjectDuplicateCheckRequest(BaseModel):
    """项目查重请求参数。"""
    document_types: Optional[list[str]] = Field(
        default=None,
        description="仅检查指定文档类型，允许值：business_bid、technical_bid；为空时同时检查两类。",
    )
    max_evidence_sections: int = Field(
        default=5,
        ge=1,
        le=20,
        description="每组最多返回的证据章节数。",
    )
    max_pairs_per_type: int = Field(
        default=0,
        ge=0,
        le=500,
        description="每类文档最多返回的对比对数，0 表示不截断。",
    )


class PersonnelReuseCheckRequest(BaseModel):
    """人员抽取确认后的一人多用检查请求。"""
    confirmed_names: Optional[list[Any]] = Field(
        default=None,
        description="业务人员确认后的人名列表；为空时返回抽取名单供确认。",
    )


class PersonnelReuseDraftDocument(BaseModel):
    """前端确认/编辑后的单份投标文件人员草稿。"""
    model_config = ConfigDict(extra="allow")

    document_identifier_id: Optional[str] = Field(default=None, description="文档 UUID。")
    identifier_id: Optional[str] = Field(default=None, description="兼容字段：文档 UUID。")
    document_type: Optional[str] = Field(default=None, description="文档类型：business_bid/technical_bid。")
    file_name: Optional[str] = Field(default=None, description="文档文件名。")
    relation_id: Optional[int] = Field(default=None, description="项目文档关系 ID。")
    personnel_entries: list[dict[str, Any]] = Field(default_factory=list, description="确认后的人员条目。")


class PersonnelReuseDraftRequest(BaseModel):
    """结果审核页保存/确认人员抽取草稿请求。"""
    model_config = ConfigDict(extra="forbid")

    documents: list[PersonnelReuseDraftDocument] = Field(
        default_factory=list,
        description="按投标文件分组的人员草稿。",
    )
