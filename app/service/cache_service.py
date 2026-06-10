# -*- coding: utf-8 -*-
"""Production cache helpers backed by Redis.

The cache is intentionally fail-fast when enabled. Production deployments should
surface Redis outages clearly instead of silently running with stale or missing
cache invalidation.
"""

from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass
from typing import Any, Callable

import redis
from fastapi.encoders import jsonable_encoder

from app.config.settings import settings

logger = logging.getLogger(__name__)


class CacheUnavailableError(RuntimeError):
    """Raised when the configured production cache cannot be used."""


@dataclass(frozen=True)
class CacheResult:
    hit: bool
    value: Any = None


class RedisCacheService:
    """Small Redis wrapper for JSON payload caching and prefix invalidation."""

    def __init__(self) -> None:
        self.enabled = bool(settings.XTJS_CACHE_ENABLED)
        self.required = bool(settings.XTJS_CACHE_REQUIRED)
        self.prefix = str(settings.XTJS_CACHE_KEY_PREFIX or "xtjs").strip() or "xtjs"
        self._client: redis.Redis | None = None

    def _redis(self) -> redis.Redis:
        if not self.enabled:
            raise CacheUnavailableError("缓存服务未启用。")
        if self._client is None:
            try:
                self._client = redis.Redis.from_url(
                    settings.XTJS_CACHE_REDIS_URL,
                    decode_responses=True,
                    socket_connect_timeout=2,
                    socket_timeout=3,
                )
            except Exception as exc:  # pragma: no cover - constructor failures are environment-specific
                raise CacheUnavailableError("缓存服务初始化失败，请检查 Redis 配置。") from exc
        return self._client

    def ping(self) -> bool:
        if not self.enabled:
            return False
        try:
            return bool(self._redis().ping())
        except Exception as exc:
            self._client = None
            raise CacheUnavailableError("缓存服务不可用，请检查 Redis。") from exc

    def key(self, *parts: Any) -> str:
        normalized = [self.prefix]
        normalized.extend(str(part).strip().replace(" ", "_") for part in parts if str(part).strip())
        return ":".join(normalized)

    @staticmethod
    def digest(value: Any) -> str:
        payload = json.dumps(jsonable_encoder(value), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        return hashlib.sha1(payload.encode("utf-8")).hexdigest()

    def get_json(self, key: str) -> CacheResult:
        try:
            raw = self._redis().get(key)
            if raw is None:
                return CacheResult(hit=False)
            return CacheResult(hit=True, value=json.loads(raw))
        except CacheUnavailableError:
            raise
        except Exception as exc:
            self._client = None
            logger.exception("cache get failed key=%s", key)
            raise CacheUnavailableError("缓存读取失败，请检查 Redis。") from exc

    def set_json(self, key: str, value: Any, ttl_seconds: int) -> None:
        try:
            payload = json.dumps(jsonable_encoder(value), ensure_ascii=False, separators=(",", ":"))
            self._redis().setex(key, max(1, int(ttl_seconds)), payload)
        except CacheUnavailableError:
            raise
        except Exception as exc:
            self._client = None
            logger.exception("cache set failed key=%s", key)
            raise CacheUnavailableError("缓存写入失败，请检查 Redis。") from exc

    def get_or_set_json(self, key: str, ttl_seconds: int, factory: Callable[[], Any]) -> tuple[Any, str]:
        cached = self.get_json(key)
        if cached.hit:
            return cached.value, "hit"
        value = factory()
        self.set_json(key, value, ttl_seconds)
        return value, "miss"

    def delete_patterns(self, patterns: list[str]) -> int:
        deleted = 0
        try:
            client = self._redis()
            for pattern in patterns:
                cursor = 0
                while True:
                    cursor, keys = client.scan(
                        cursor=cursor,
                        match=pattern,
                        count=max(1, int(settings.XTJS_CACHE_SCAN_BATCH_SIZE)),
                    )
                    if keys:
                        deleted += int(client.delete(*keys) or 0)
                    if cursor == 0:
                        break
            return deleted
        except CacheUnavailableError:
            raise
        except Exception as exc:
            self._client = None
            logger.exception("cache delete failed patterns=%s", patterns)
            raise CacheUnavailableError("缓存失效失败，请检查 Redis。") from exc

    def project_list_key(self, *, limit: int, offset: int, keyword: str | None) -> str:
        return self.key("projects", "list", self.digest({"limit": limit, "offset": offset, "keyword": keyword or ""}))

    def project_detail_key(self, identifier_id: str) -> str:
        return self.key("project", str(identifier_id), "detail")

    def project_results_key(
        self,
        identifier_id: str,
        *,
        view: str,
        include_raw_results: bool,
        include_result_record: bool,
    ) -> str:
        return self.key(
            "project",
            str(identifier_id),
            "results",
            self.digest({
                "view": view,
                "include_raw_results": bool(include_raw_results),
                "include_result_record": bool(include_result_record),
            }),
        )

    def project_ocr_status_key(self, identifier_id: str) -> str:
        return self.key("project", str(identifier_id), "ocr-status")

    def preview_meta_key(self, document_id: str, version: str, page: int) -> str:
        return self.key("preview", str(document_id), str(version), f"p{int(page)}")

    def invalidate_project(self, identifier_id: str | None = None) -> int:
        patterns = [self.key("projects", "list", "*")]
        normalized = str(identifier_id or "").strip()
        if normalized:
            patterns.append(self.key("project", normalized, "*"))
        else:
            patterns.append(self.key("project", "*"))
        return self.delete_patterns(patterns)

    def invalidate_document_preview(self, document_id: str | None = None) -> int:
        normalized = str(document_id or "").strip()
        pattern = self.key("preview", normalized if normalized else "*", "*")
        return self.delete_patterns([pattern])


_CACHE_SERVICE: RedisCacheService | None = None


def get_cache_service() -> RedisCacheService:
    global _CACHE_SERVICE
    if _CACHE_SERVICE is None:
        _CACHE_SERVICE = RedisCacheService()
    return _CACHE_SERVICE


def invalidate_project_cache(identifier_id: str | None = None) -> int:
    return get_cache_service().invalidate_project(identifier_id)


def invalidate_document_preview_cache(document_id: str | None = None) -> int:
    return get_cache_service().invalidate_document_preview(document_id)
