from typing import Optional

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
    technical_bid_document_identifier: str


class ProjectUpdateRequest(BaseModel):
    new_identifier_id: str


class DocumentUpdateRequest(BaseModel):
    file_name: Optional[str] = None
    file_url: Optional[str] = Field(
        default=None,
        description="持久化对象存储地址，例如 minio://bucket/object。",
    )


class ProjectRelationUpdateRequest(BaseModel):
    tender_document_identifier: str
    business_bid_document_identifier: str
    technical_bid_document_identifier: str


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
