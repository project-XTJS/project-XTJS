# -*- coding: utf-8 -*-
"""统一日志配置：JSON 结构化输出 + 按天/按大小双滚动 + 保留 N 天。

- stdout 与文件均输出 JSON 单行，便于检索；
- 文件日志按天滚动，且单文件超过 max_bytes 也会滚动；
- backupCount 即保留天数（默认 90 天）。
"""

import json
import logging
import time
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

from app.config.settings import settings


class JsonFormatter(logging.Formatter):
    """将日志记录输出为单行 JSON。"""

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(record.created))
            + f".{int(record.msecs):03d}",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        extra_fields = getattr(record, "extra_fields", None)
        if isinstance(extra_fields, dict):
            payload.update(extra_fields)
        return json.dumps(payload, ensure_ascii=False)


class DailySizeRotatingFileHandler(TimedRotatingFileHandler):
    """按天滚动，同时单文件超过大小上限时也滚动。"""

    def __init__(
        self,
        filename: str,
        *,
        when: str = "midnight",
        backup_count: int = 90,
        max_bytes: int = 200 * 1024 * 1024,
        encoding: str = "utf-8",
    ) -> None:
        self.max_bytes = max(0, int(max_bytes or 0))
        super().__init__(
            filename,
            when=when,
            backupCount=backup_count,
            encoding=encoding,
        )

    def shouldRollover(self, record: logging.LogRecord) -> bool:
        if (
            self.max_bytes > 0
            and self.stream is not None
            and self.stream.tell() + len(record.getMessage()) + 256 >= self.max_bytes
        ):
            return True
        return super().shouldRollover(record)


def setup_logging() -> None:
    """配置根 logger 与 uvicorn 相关 logger 为 JSON 输出（可重复调用，幂等）。"""
    level_name = str(getattr(settings, "LOG_LEVEL", "INFO") or "INFO").strip().upper()
    level = getattr(logging, level_name, logging.INFO)
    log_dir = Path(str(getattr(settings, "LOG_DIR", "/app/logs") or "/app/logs"))
    retention_days = max(1, int(getattr(settings, "LOG_RETENTION_DAYS", 90) or 90))
    max_bytes = max(0, int(getattr(settings, "LOG_MAX_BYTES", 200 * 1024 * 1024) or 0))

    handlers: list[logging.Handler] = [logging.StreamHandler()]
    try:
        log_dir.mkdir(parents=True, exist_ok=True)
        file_handler = DailySizeRotatingFileHandler(
            str(log_dir / "app.jsonl"),
            backup_count=retention_days,
            max_bytes=max_bytes,
        )
        handlers.append(file_handler)
    except Exception:
        # 文件日志不可用时（如只读目录）退回仅 stdout，不影响服务。
        pass

    formatter = JsonFormatter()
    for handler in handlers:
        handler.setFormatter(formatter)

    root = logging.getLogger()
    root.setLevel(level)
    root.handlers = handlers
    root.propagate = False

    # uvicorn 相关 logger 交由根 logger 统一处理，避免重复输出。
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access", "uvicorn.asgi"):
        logger = logging.getLogger(name)
        logger.handlers = []
        logger.propagate = True
        logger.setLevel(level)
    logging.getLogger("uvicorn.access").setLevel(max(level, logging.INFO))
