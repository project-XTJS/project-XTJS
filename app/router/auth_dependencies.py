# -*- coding: utf-8 -*-
"""
认证相关的路由依赖。

提供从 Authorization Bearer 解析 JWT 并加载当前用户的依赖，
以及按权限等级守卫接口的 require_role 工厂。
"""

from typing import Any, Dict

from fastapi import Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from app.core.security import decode_access_token
from app.router.dependencies import get_user_service
from app.service.user_service import UserService

# auto_error=False：未带令牌时由我们统一抛 401，保证响应走 UnifiedResponse。
_bearer_scheme = HTTPBearer(auto_error=False)

# 等级 -> 中文，用于 403 提示
_ROLE_NAMES = {1: "普通用户", 2: "中级用户", 3: "高级用户", 4: "管理员"}


def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(_bearer_scheme),
    user_service: UserService = Depends(get_user_service),
) -> Dict[str, Any]:
    """
    解析并校验 Bearer 令牌，返回当前登录用户（对外视图）。

    令牌缺失/无效/过期 → 401；用户不存在或已停用 → 401。
    """
    if credentials is None or not credentials.credentials:
        raise HTTPException(status_code=401, detail="未提供认证令牌")

    payload = decode_access_token(credentials.credentials)
    if not payload or not payload.get("sub"):
        raise HTTPException(status_code=401, detail="认证令牌无效或已过期")

    user = user_service.get_public_by_identifier(payload["sub"])
    if user is None:
        raise HTTPException(status_code=401, detail="账号不存在")
    if not user.get("is_active"):
        raise HTTPException(status_code=401, detail="账号已被停用")
    return user


def require_role(min_level: int):
    """
    生成一个依赖：要求当前用户的 role_level 不低于 min_level，否则抛 403。

    用法：dependencies=[Depends(require_role(4))] 或 current=Depends(require_role(2))。
    """

    def _checker(current_user: Dict[str, Any] = Depends(get_current_user)) -> Dict[str, Any]:
        if int(current_user.get("role_level", 0)) < min_level:
            required = _ROLE_NAMES.get(min_level, f"等级{min_level}")
            raise HTTPException(status_code=403, detail=f"需要{required}及以上权限")
        return current_user

    return _checker
