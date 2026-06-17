# -*- coding: utf-8 -*-
"""
认证与用户管理相关的 Pydantic 模型。
"""

from typing import Optional

from pydantic import BaseModel, Field


class LoginRequest(BaseModel):
    """登录请求。"""

    username: str = Field(min_length=1, max_length=64, description="登录用户名")
    password: str = Field(min_length=1, max_length=128, description="登录密码")


class ChangePasswordRequest(BaseModel):
    """修改自己密码的请求。"""

    old_password: str = Field(min_length=1, max_length=128, description="原密码")
    new_password: str = Field(min_length=1, max_length=128, description="新密码")


class CreateUserRequest(BaseModel):
    """管理员创建用户。"""

    username: str = Field(min_length=1, max_length=64, description="登录用户名")
    password: str = Field(min_length=1, max_length=128, description="初始密码")
    role_level: int = Field(
        default=1, ge=1, le=4,
        description="权限等级：1-普通用户，2-中级用户，3-高级用户，4-管理员",
    )
    display_name: Optional[str] = Field(default=None, max_length=64, description="展示名称")


class UpdateUserRequest(BaseModel):
    """管理员更新用户角色 / 启停 / 展示名。"""

    role_level: Optional[int] = Field(default=None, ge=1, le=4, description="权限等级 1-4")
    is_active: Optional[bool] = Field(default=None, description="是否启用")
    display_name: Optional[str] = Field(default=None, max_length=64, description="展示名称")


class ResetPasswordRequest(BaseModel):
    """管理员重置某用户密码。"""

    new_password: str = Field(min_length=1, max_length=128, description="新密码")
