# -*- coding: utf-8 -*-
"""
认证与用户管理路由。

提供登录、获取当前用户、修改密码，以及管理员专用的用户管理接口。
登录失败采用模糊提示并触发失败计数/锁定，抵御暴力破解与用户名枚举。
"""

from datetime import datetime, timezone
from typing import Any, Dict

from fastapi import APIRouter, Depends, HTTPException

from app.core.security import (
    create_access_token,
    validate_password_strength,
    verify_password,
)
from app.router.auth_dependencies import get_current_user, require_role
from app.router.dependencies import get_user_service
from app.schemas.auth import (
    ChangePasswordRequest,
    CreateUserRequest,
    LoginRequest,
    ResetPasswordRequest,
    UpdateUserRequest,
)
from app.service.user_service import (
    ROLE_LEVEL_ADMIN,
    UsernameAlreadyExistsError,
    UserService,
)

router = APIRouter()

# 模糊的登录失败提示，避免区分“用户不存在/密码错误”造成用户名枚举。
_LOGIN_FAILED_MESSAGE = "用户名或密码错误"


def _is_locked(record: Dict[str, Any]) -> bool:
    """判断账号当前是否处于锁定期内。"""
    locked_until = record.get("locked_until")
    if not locked_until:
        return False
    if locked_until.tzinfo is None:
        locked_until = locked_until.replace(tzinfo=timezone.utc)
    return locked_until > datetime.now(timezone.utc)


@router.post("/login", summary="用户登录", tags=["认证"])
def login(
    payload: LoginRequest,
    user_service: UserService = Depends(get_user_service),
):
    """校验用户名口令，成功则签发 JWT 访问令牌。"""
    record = user_service.get_auth_record_by_username(payload.username)

    # 用户不存在：返回模糊错误，不暴露账号是否存在。
    if record is None:
        raise HTTPException(status_code=401, detail=_LOGIN_FAILED_MESSAGE)

    if not record.get("is_active"):
        raise HTTPException(status_code=403, detail="账号已被停用，请联系管理员")

    if _is_locked(record):
        raise HTTPException(
            status_code=403,
            detail="账号因多次登录失败已被临时锁定，请稍后再试",
        )

    if not verify_password(payload.password, record["hashed_password"]):
        user_service.record_login_failure(record["identifier_id"])
        raise HTTPException(status_code=401, detail=_LOGIN_FAILED_MESSAGE)

    user_service.record_login_success(record["identifier_id"])
    token = create_access_token(record["identifier_id"], record["role_level"])
    return {
        "access_token": token,
        "token_type": "bearer",
        "user": {
            "identifier_id": str(record["identifier_id"]),
            "username": record["username"],
            "role_level": int(record["role_level"]),
            "role_label": UserService.role_label(record["role_level"]),
            "display_name": record.get("display_name"),
        },
    }


@router.get("/me", summary="获取当前登录用户", tags=["认证"])
def read_me(current_user: Dict[str, Any] = Depends(get_current_user)):
    """返回当前令牌对应的用户信息。"""
    return current_user


@router.post("/change-password", summary="修改本人密码", tags=["认证"])
def change_password(
    payload: ChangePasswordRequest,
    current_user: Dict[str, Any] = Depends(get_current_user),
    user_service: UserService = Depends(get_user_service),
):
    """校验原密码后修改为新密码。"""
    record = user_service.get_auth_record_by_username(current_user["username"])
    if record is None or not verify_password(payload.old_password, record["hashed_password"]):
        raise HTTPException(status_code=400, detail="原密码不正确")

    strength_error = validate_password_strength(payload.new_password)
    if strength_error:
        raise HTTPException(status_code=400, detail=strength_error)

    user_service.reset_password(current_user["identifier_id"], payload.new_password)
    return {"message": "密码修改成功"}


# —— 管理员专用：用户管理 ——


@router.get("/users", summary="用户列表（管理员）", tags=["认证"])
def list_users(
    _admin: Dict[str, Any] = Depends(require_role(ROLE_LEVEL_ADMIN)),
    user_service: UserService = Depends(get_user_service),
):
    """列出所有未删除用户。"""
    return user_service.list_users()


@router.post("/users", summary="创建用户（管理员）", tags=["认证"])
def create_user(
    payload: CreateUserRequest,
    _admin: Dict[str, Any] = Depends(require_role(ROLE_LEVEL_ADMIN)),
    user_service: UserService = Depends(get_user_service),
):
    """创建新用户并指定权限等级。"""
    strength_error = validate_password_strength(payload.password)
    if strength_error:
        raise HTTPException(status_code=400, detail=strength_error)
    try:
        return user_service.create_user(
            username=payload.username,
            password=payload.password,
            role_level=payload.role_level,
            display_name=payload.display_name,
        )
    except UsernameAlreadyExistsError:
        raise HTTPException(status_code=409, detail="用户名已存在")


@router.put("/users/{identifier_id}", summary="更新用户（管理员）", tags=["认证"])
def update_user(
    identifier_id: str,
    payload: UpdateUserRequest,
    admin: Dict[str, Any] = Depends(require_role(ROLE_LEVEL_ADMIN)),
    user_service: UserService = Depends(get_user_service),
):
    """更新用户的角色等级 / 启停状态 / 展示名。"""
    target = user_service.get_public_by_identifier(identifier_id)
    if target is None:
        raise HTTPException(status_code=404, detail="用户不存在")

    # 保护：不允许把最后一个启用的管理员降级或停用，避免锁死系统。
    _guard_last_admin(user_service, target, payload)

    updated = user_service.update_user(
        identifier_id=identifier_id,
        role_level=payload.role_level,
        is_active=payload.is_active,
        display_name=payload.display_name,
    )
    if updated is None:
        raise HTTPException(status_code=404, detail="用户不存在")
    return updated


@router.post(
    "/users/{identifier_id}/reset-password",
    summary="重置用户密码（管理员）",
    tags=["认证"],
)
def reset_user_password(
    identifier_id: str,
    payload: ResetPasswordRequest,
    _admin: Dict[str, Any] = Depends(require_role(ROLE_LEVEL_ADMIN)),
    user_service: UserService = Depends(get_user_service),
):
    """管理员重置指定用户密码。"""
    strength_error = validate_password_strength(payload.new_password)
    if strength_error:
        raise HTTPException(status_code=400, detail=strength_error)
    if not user_service.reset_password(identifier_id, payload.new_password):
        raise HTTPException(status_code=404, detail="用户不存在")
    return {"message": "密码已重置"}


@router.delete("/users/{identifier_id}", summary="删除用户（管理员）", tags=["认证"])
def delete_user(
    identifier_id: str,
    admin: Dict[str, Any] = Depends(require_role(ROLE_LEVEL_ADMIN)),
    user_service: UserService = Depends(get_user_service),
):
    """逻辑删除用户。"""
    target = user_service.get_public_by_identifier(identifier_id)
    if target is None:
        raise HTTPException(status_code=404, detail="用户不存在")
    if str(target["identifier_id"]) == str(admin["identifier_id"]):
        raise HTTPException(status_code=400, detail="不能删除当前登录的账号")
    if (
        int(target["role_level"]) == ROLE_LEVEL_ADMIN
        and target["is_active"]
        and user_service.count_admins() <= 1
    ):
        raise HTTPException(status_code=400, detail="不能删除最后一个管理员")
    if not user_service.delete_user(identifier_id):
        raise HTTPException(status_code=404, detail="用户不存在")
    return {"message": "用户已删除"}


def _guard_last_admin(
    user_service: UserService,
    target: Dict[str, Any],
    payload: UpdateUserRequest,
) -> None:
    """阻止把唯一启用的管理员降级或停用。"""
    if int(target["role_level"]) != ROLE_LEVEL_ADMIN or not target["is_active"]:
        return
    demoting = payload.role_level is not None and payload.role_level < ROLE_LEVEL_ADMIN
    deactivating = payload.is_active is False
    if (demoting or deactivating) and user_service.count_admins() <= 1:
        raise HTTPException(status_code=400, detail="不能降级或停用最后一个管理员")
