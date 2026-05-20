# -*- coding: utf-8 -*-
"""
PostgreSQL 相关请求模型定义。

包含项目、文档、关系、分析结果等 CRUD 操作所需的 Pydantic 模型，
以及查重范围枚举。
"""

from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field


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
    project_identifier_id: str = Field(..., description="项目 UUID 标识。")
    result: dict[str, Any] = Field(..., description="完整结果 JSON 对象。")


class ProjectResultUpdateRequest(BaseModel):
    """更新项目分析结果请求。"""
    result: dict[str, Any] = Field(..., description="完整结果 JSON 对象。")


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
