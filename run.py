# -*- coding: utf-8 -*-
"""
应用启动入口。

负责解析运行配置，启动 Uvicorn 服务器并可选自动打开浏览器。
"""

import os
import sys
import threading
import webbrowser

import uvicorn

# 确保项目根目录在 sys.path 中
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.append(current_dir)


def _env_flag(name: str, default: bool = False) -> bool:
    """读取布尔型环境变量（1/true/yes/on 视为 True）。"""
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _resolve_server_config() -> tuple[str, int, bool]:
    """从环境变量解析服务器监听地址、端口及热重载开关。"""
    host = os.getenv("UVICORN_HOST", "127.0.0.1").strip() or "127.0.0.1"
    port = int(os.getenv("UVICORN_PORT", "8080"))
    reload_enabled = _env_flag("UVICORN_RELOAD", default=False)

    # Windows 下 reload 可能导致端口冲突，自动禁用
    if sys.platform == "win32" and reload_enabled:
        print("Detected Windows local launch, disabling reload to avoid WinError 10013.")
        reload_enabled = False

    return host, port, reload_enabled


def open_browser(host: str, port: int) -> None:
    """在默认浏览器中打开 Swagger UI 文档页面。"""
    browser_host = "127.0.0.1" if host == "0.0.0.0" else host
    webbrowser.open(f"http://{browser_host}:{port}/docs")


if __name__ == "__main__":
    host, port, reload_enabled = _resolve_server_config()

    # 延迟 2 秒自动打开浏览器
    browser_timer = threading.Timer(2, open_browser, args=(host, port))
    browser_timer.daemon = True
    browser_timer.start()

    uvicorn.run(
        "app.main:app",
        host=host,
        port=port,
        reload=reload_enabled,
        log_level="info",
    )