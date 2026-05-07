import os
import sys
import threading
import webbrowser

import uvicorn

current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.append(current_dir)


def _env_flag(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _resolve_server_config() -> tuple[str, int, bool]:
    host = os.getenv("UVICORN_HOST", "127.0.0.1").strip() or "127.0.0.1"
    port = int(os.getenv("UVICORN_PORT", "8080"))
    reload_enabled = _env_flag("UVICORN_RELOAD", default=False)

    if sys.platform == "win32" and reload_enabled:
        print("Detected Windows local launch, disabling reload to avoid WinError 10013.")
        reload_enabled = False

    return host, port, reload_enabled


def open_browser(host: str, port: int) -> None:
    browser_host = "127.0.0.1" if host == "0.0.0.0" else host
    webbrowser.open(f"http://{browser_host}:{port}/docs")


if __name__ == "__main__":
    host, port, reload_enabled = _resolve_server_config()

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
