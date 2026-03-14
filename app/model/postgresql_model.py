from typing import Any, Optional
from uuid import uuid4

from pydantic import BaseModel, Field


class ResponseModel(BaseModel):
    """通用响应模型（可用于后续统一响应结构）。"""

    code: int = 200
    message: str = "success"
    # 每次响应生成一个请求追踪 ID，便于日志关联。
    rid: str = Field(default_factory=lambda: str(uuid4()))
    data: Any = None


class DocumentDataModel(BaseModel):
    """文档元数据模型。"""

    identifier_id: str
    file_name: str
    file_url: str


class ProjectCreateRequest(BaseModel):
    """项目创建请求。"""

    identifier_id: Optional[str] = Field(
        default=None, description="业务项目标识。为空时自动生成。"
    )


class DocumentCreateRequest(BaseModel):
    """文档创建请求。"""

    identifier_id: Optional[str] = Field(
        default=None, description="业务文档标识。为空时自动生成。"
    )
    file_name: str
    file_url: str = Field(
        ...,
        description="建议传持久化存储地址（如 minio://bucket/object），不要传预签名 URL。",
    )


class ProjectBindDocumentsRequest(BaseModel):
    """项目绑定招标/投标文档请求。"""

    tender_document_identifier: str
    bid_document_identifier: str


class ProjectUpdateRequest(BaseModel):
    """项目更新请求。"""

    new_identifier_id: str


class DocumentUpdateRequest(BaseModel):
    """文档更新请求。"""

    file_name: Optional[str] = None
    file_url: Optional[str] = Field(
        default=None,
        description="文档存储地址（minio://bucket/object）。",
    )


class ProjectRelationUpdateRequest(BaseModel):
    """项目文档关联更新请求。"""

    tender_document_identifier: str
    bid_document_identifier: str
