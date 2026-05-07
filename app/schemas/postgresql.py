from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field


class DocumentDataModel(BaseModel):
    identifier_id: str
    file_name: str
    file_url: str


class ProjectCreateRequest(BaseModel):
    identifier_id: Optional[str] = Field(
        default=None,
        description="项目业务标识，省略时自动生成。",
    )


class DocumentCreateRequest(BaseModel):
    identifier_id: Optional[str] = Field(
        default=None,
        description="文档业务标识，省略时自动生成。",
    )
    file_name: str
    file_url: str = Field(
        ...,
        description="持久化对象存储地址，例如 minio://bucket/object。",
    )


class ProjectBindDocumentsRequest(BaseModel):
    tender_document_identifier: str
    business_bid_document_identifier: str
    technical_bid_document_identifier: Optional[str] = None


class ProjectUpdateRequest(BaseModel):
    new_identifier_id: Optional[str] = None


class DocumentUpdateRequest(BaseModel):
    file_name: Optional[str] = None
    file_url: Optional[str] = Field(
        default=None,
        description="持久化对象存储地址，例如 minio://bucket/object。",
    )


class ProjectRelationUpdateRequest(BaseModel):
    tender_document_identifier: str
    business_bid_document_identifier: str
    technical_bid_document_identifier: Optional[str] = None


class IdentifierBatchDeleteRequest(BaseModel):
    identifier_ids: list[str] = Field(
        ...,
        min_length=1,
        description="批量删除的业务标识列表。",
    )


class RelationBatchDeleteRequest(BaseModel):
    relation_ids: list[int] = Field(
        ...,
        min_length=1,
        description="批量删除的关联主键列表。",
    )


class ProjectResultUpsertRequest(BaseModel):
    project_identifier_id: str = Field(..., description="项目业务标识。")
    result: dict[str, Any] = Field(..., description="完整结果 JSON 对象。")


class ProjectResultUpdateRequest(BaseModel):
    result: dict[str, Any] = Field(..., description="完整结果 JSON 对象。")


class DuplicateCheckScope(str, Enum):
    ALL = "all"
    BUSINESS_BID = "business_bid"
    TECHNICAL_BID = "technical_bid"


class ProjectDuplicateCheckRequest(BaseModel):
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
