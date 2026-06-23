# -*- coding: utf-8 -*-
"""
认证安全核心模块。

提供密码哈希/校验（bcrypt）、JWT 访问令牌的签发与解析，以及密码强度校验。
所有口令一律以 bcrypt 哈希形式存储，绝不保存明文。
"""

from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import bcrypt
from jose import JWTError, jwt

from app.config.settings import settings

# bcrypt 仅使用密码的前 72 字节；新版 bcrypt 对超长输入会直接报错，
# 因此统一手动截断（哈希与校验用同样的截断，结果一致）。
_BCRYPT_MAX_BYTES = 72


def _to_bcrypt_bytes(password: str) -> bytes:
    """把明文口令编码为 bcrypt 可用字节，超过 72 字节按 bcrypt 规则截断。"""
    return str(password or "").encode("utf-8")[:_BCRYPT_MAX_BYTES]


def hash_password(plain_password: str) -> str:
    """返回明文密码的 bcrypt 哈希值。"""
    return bcrypt.hashpw(_to_bcrypt_bytes(plain_password), bcrypt.gensalt()).decode("utf-8")


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """校验明文密码与哈希是否匹配，哈希异常时安全返回 False。"""
    try:
        return bcrypt.checkpw(
            _to_bcrypt_bytes(plain_password),
            str(hashed_password or "").encode("utf-8"),
        )
    except (ValueError, TypeError):
        return False


def validate_password_strength(password: str) -> Optional[str]:
    """
    校验密码强度，符合要求返回 None，否则返回中文错误描述。

    规则：长度不少于配置的最小值，且至少包含字母与数字。
    """
    if not isinstance(password, str) or len(password) < settings.AUTH_PASSWORD_MIN_LENGTH:
        return f"密码长度至少为 {settings.AUTH_PASSWORD_MIN_LENGTH} 位"
    has_alpha = any(ch.isalpha() for ch in password)
    has_digit = any(ch.isdigit() for ch in password)
    if not (has_alpha and has_digit):
        return "密码需同时包含字母和数字"
    return None


def create_access_token(subject: str, role_level: int) -> str:
    """
    签发 JWT 访问令牌。

    参数：
        subject: 令牌主体，使用用户的 identifier_id。
        role_level: 用户权限等级（1-4），写入载荷便于快速判断。
    """
    now = datetime.now(timezone.utc)
    expire = now + timedelta(minutes=settings.JWT_ACCESS_TOKEN_EXPIRE_MINUTES)
    payload = {
        "sub": str(subject),
        "role_level": int(role_level),
        "iat": int(now.timestamp()),
        "exp": int(expire.timestamp()),
    }
    return jwt.encode(payload, settings.JWT_SECRET_KEY, algorithm=settings.JWT_ALGORITHM)


def decode_access_token(token: str) -> Optional[dict[str, Any]]:
    """解析并校验 JWT 令牌，失败（过期/签名错误等）返回 None。"""
    try:
        return jwt.decode(
            token,
            settings.JWT_SECRET_KEY,
            algorithms=[settings.JWT_ALGORITHM],
        )
    except JWTError:
        return None
