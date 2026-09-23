"""Demand-started MacBERT CSC manager; idle time unloads the model process."""

import atexit
import hashlib
import json
import os
from pathlib import Path
import signal
import sqlite3
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from .contract import (
    DEFAULT_AUTO_MIN_PROBABILITY,
    DEFAULT_AUTO_MIN_PROBABILITY_RATIO,
    VERSION,
    TypoUnavailable,
    classify_candidates,
    validate_candidates,
    word_rule_version,
)


class ModelManager:
    def __init__(
        self,
        command,
        *,
        worker_url,
        model_id,
        cache_path,
        min_probability=0.60,
        min_probability_ratio=10.0,
        auto_min_probability=DEFAULT_AUTO_MIN_PROBABILITY,
        auto_min_probability_ratio=DEFAULT_AUTO_MIN_PROBABILITY_RATIO,
        idle_seconds=1800,
        startup_seconds=300,
        request_seconds=30,
        clock=time.monotonic,
        popen=subprocess.Popen,
    ):
        self.command = command
        self.worker_url = worker_url
        self.model_id = model_id
        self.min_probability = float(min_probability)
        self.min_probability_ratio = float(min_probability_ratio)
        self.auto_min_probability = float(auto_min_probability)
        self.auto_min_probability_ratio = float(auto_min_probability_ratio)
        self.idle_seconds = idle_seconds
        self.startup_seconds = startup_seconds
        self.request_seconds = request_seconds
        self.clock = clock
        self.popen = popen
        self.lock = threading.RLock()
        self.serial = threading.Lock()
        self.worker = None
        self.state = "unloaded"
        self.pending = 0
        self.last_used = None
        self.error = None
        self.metrics = {
            "checks": 0,
            "completed": 0,
            "incomplete": 0,
            "cache_hits": 0,
            "inference_checks": 0,
            "confirmed_results": 0,
            "review_candidates": 0,
            "inference_seconds": 0.0,
        }
        self.cache_path = str(cache_path)
        Path(self.cache_path).parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.cache_path) as db:
            db.execute(
                "CREATE TABLE IF NOT EXISTS checks (key TEXT PRIMARY KEY, result TEXT NOT NULL)"
            )

    def status(self):
        with self.lock:
            if self.worker is not None and self.worker.poll() is not None:
                self._stop()
                self.state = "failed"
                self.error = "纠错推理进程已退出"
            return {
                "state": self.state,
                "model": self.model_id,
                "min_probability": self.min_probability,
                "min_probability_ratio": self.min_probability_ratio,
                "auto_min_probability": self.auto_min_probability,
                "auto_min_probability_ratio": self.auto_min_probability_ratio,
                "rule_version": VERSION,
                "word_rule_version": word_rule_version(),
                "pending": self.pending,
                "idle_seconds": self.idle_seconds,
                "error": self.error,
                "metrics": dict(self.metrics),
            }

    def _record_completed(self, result, *, cache_hit):
        with self.lock:
            self.metrics["completed"] += 1
            self.metrics["cache_hits"] += int(cache_hit)
            self.metrics["confirmed_results"] += int(result.get("confirmed_count") or 0)
            self.metrics["review_candidates"] += int(result.get("review_candidate_count") or 0)

    def _request(self, path, payload=None, timeout=3):
        request = urllib.request.Request(
            self.worker_url + path,
            data=None
            if payload is None
            else json.dumps(payload, ensure_ascii=False).encode(),
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return json.loads(response.read() or b"{}")

    def _start(self):
        with self.lock:
            if (
                self.worker is not None
                and self.worker.poll() is None
                and self.state == "ready"
            ):
                return
            if self.worker is not None:
                self._stop()
            self.state = "starting"
            self.error = None
            self.worker = self.popen(self.command, start_new_session=True)
        deadline = self.clock() + self.startup_seconds
        while self.clock() < deadline:
            if self.worker.poll() is not None:
                break
            try:
                health = self._request("/health")
                if health.get("status") != "ready":
                    raise ValueError("worker_not_ready")
                with self.lock:
                    self.state = "ready"
                return
            except (OSError, ValueError):
                time.sleep(0.5)
        self._stop()
        raise TypoUnavailable("纠错模型启动失败或超时，请重试")

    def _stop(self):
        with self.lock:
            process = self.worker
            self.state = "stopping"
            if process is not None:
                try:
                    os.killpg(process.pid, signal.SIGTERM)
                    process.wait(timeout=15)
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait(timeout=10)
                except ProcessLookupError:
                    pass
            self.worker = None
            self.state = "unloaded"
            self.last_used = None

    def reap_idle(self):
        with self.lock:
            if (
                self.worker is not None
                and self.pending == 0
                and self.last_used is not None
                and self.clock() - self.last_used >= self.idle_seconds
            ):
                self._stop()
                return True
        return False

    def check(self, text):
        with self.lock:
            self.metrics["checks"] += 1
        key_material = (
            f"{self.model_id}\0{VERSION}\0{self.min_probability:.6f}"
            f"\0{self.min_probability_ratio:.6f}"
            f"\0{self.auto_min_probability:.6f}"
            f"\0{self.auto_min_probability_ratio:.6f}"
            f"\0{word_rule_version()}\0{text}"
        )
        key = hashlib.sha256(key_material.encode()).hexdigest()
        with sqlite3.connect(self.cache_path) as db:
            row = db.execute(
                "SELECT result FROM checks WHERE key=?", (key,)
            ).fetchone()
        if row:
            result = json.loads(row[0])
            self._record_completed(result, cache_hit=True)
            return dict(result, cache_hit=True)
        with self.lock:
            if self.pending >= 16:
                raise TypoUnavailable("纠错服务繁忙，请稍后重试")
            self.pending += 1
        inference_started = False
        inference_started_at = None
        try:
            with self.serial:
                with sqlite3.connect(self.cache_path) as db:
                    row = db.execute(
                        "SELECT result FROM checks WHERE key=?", (key,)
                    ).fetchone()
                if row:
                    result = json.loads(row[0])
                    self._record_completed(result, cache_hit=True)
                    return dict(result, cache_hit=True)
                self._start()
                for attempt in range(2):
                    try:
                        if not inference_started:
                            inference_started = True
                            inference_started_at = self.clock()
                            with self.lock:
                                self.metrics["inference_checks"] += 1
                        response = self._request(
                            "/correct",
                            {
                                "text": text,
                                "min_probability": self.min_probability,
                                "min_probability_ratio": self.min_probability_ratio,
                            },
                            timeout=self.request_seconds,
                        )
                        if "candidates" not in response:
                            if response.get("corrected_text") != text:
                                raise TypoUnavailable("纠错输出缺少字符候选")
                            response = {**response, "candidates": []}
                        candidates = validate_candidates(text, response)
                        issues, review_candidates = classify_candidates(
                            text,
                            candidates,
                            auto_min_probability=self.auto_min_probability,
                            auto_min_probability_ratio=self.auto_min_probability_ratio,
                        )
                        result = {
                            "status": "completed",
                            "issues": issues,
                            "review_candidates": review_candidates,
                            "candidate_count": len(candidates),
                            "confirmed_count": len(issues),
                            "review_candidate_count": len(review_candidates),
                            "model": self.model_id,
                            "min_probability": self.min_probability,
                            "min_probability_ratio": self.min_probability_ratio,
                            "auto_min_probability": self.auto_min_probability,
                            "auto_min_probability_ratio": self.auto_min_probability_ratio,
                            "rule_version": VERSION,
                            "word_rule_version": word_rule_version(),
                        }
                        with sqlite3.connect(self.cache_path) as db:
                            db.execute(
                                "INSERT OR REPLACE INTO checks VALUES (?,?)",
                                (key, json.dumps(result, ensure_ascii=False)),
                            )
                        self._record_completed(result, cache_hit=False)
                        return dict(result, cache_hit=False)
                    except (urllib.error.URLError, TimeoutError) as exc:
                        self._stop()
                        if attempt:
                            raise TypoUnavailable("纠错模型暂不可用") from exc
                        self._start()
                raise TypoUnavailable("纠错未完成")
        except Exception as exc:
            with self.lock:
                self.error = str(exc)
                self.metrics["incomplete"] += 1
            if isinstance(exc, TypoUnavailable):
                raise
            raise TypoUnavailable("纠错输出无效或执行失败") from exc
        finally:
            with self.lock:
                self.pending -= 1
                if inference_started and self.worker is not None:
                    self.last_used = self.clock()
                if inference_started_at is not None:
                    self.metrics["inference_seconds"] += max(
                        0.0, self.clock() - inference_started_at
                    )


def main():
    path = Path(os.environ.get("TYPO_MODEL_PATH", "/models/current"))
    manifest = json.loads((path / "model-manifest.json").read_text())
    model_id = manifest["model"] + "@" + manifest["revision"]
    worker_url = "http://127.0.0.1:8001"
    command = [
        sys.executable,
        "-m",
        "typo_runtime.worker",
        "--model",
        str(path),
        "--host",
        "127.0.0.1",
        "--port",
        "8001",
        "--device",
        os.environ.get("TYPO_DEVICE", "auto"),
    ]
    manager = ModelManager(
        command,
        worker_url=worker_url,
        model_id=model_id,
        cache_path=os.environ.get("TYPO_CACHE_PATH", "/cache/checks.sqlite"),
        min_probability=float(os.environ.get("TYPO_MIN_PROBABILITY", "0.60")),
        min_probability_ratio=float(
            os.environ.get("TYPO_MIN_PROBABILITY_RATIO", "10.0")
        ),
        auto_min_probability=float(
            os.environ.get("TYPO_AUTO_MIN_PROBABILITY", "0.90")
        ),
        auto_min_probability_ratio=float(
            os.environ.get("TYPO_AUTO_MIN_PROBABILITY_RATIO", "20.0")
        ),
        idle_seconds=int(os.environ.get("TYPO_IDLE_SECONDS", "1800")),
        startup_seconds=int(os.environ.get("TYPO_STARTUP_SECONDS", "300")),
        request_seconds=int(os.environ.get("TYPO_REQUEST_SECONDS", "30")),
    )
    atexit.register(manager._stop)

    def shutdown(*_):
        manager._stop()
        os._exit(0)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    def reaper():
        while True:
            time.sleep(1)
            manager.reap_idle()

    threading.Thread(target=reaper, daemon=True).start()

    class Handler(BaseHTTPRequestHandler):
        def reply(self, status, data):
            body = json.dumps(data, ensure_ascii=False).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            if self.path == "/health":
                self.reply(200, manager.status())
            else:
                self.reply(404, {"error": "not_found"})

        def do_POST(self):
            if self.path != "/check":
                return self.reply(404, {"error": "not_found"})
            try:
                size = int(self.headers.get("Content-Length", "0"))
                if not 0 < size <= 20000:
                    raise ValueError("invalid_request_size")
                body = json.loads(self.rfile.read(size))
                text = body["text"]
                if not isinstance(text, str) or not 0 < len(text) <= 300:
                    raise ValueError("invalid_text")
                self.reply(200, manager.check(text))
            except (ValueError, KeyError):
                self.reply(400, {"status": "failed", "error": "纠错输入无效"})
            except TypoUnavailable as exc:
                self.reply(503, {"status": "incomplete", "error": str(exc)})

        def log_message(self, format, *args):
            pass

    ThreadingHTTPServer(("0.0.0.0", 8090), Handler).serve_forever()


if __name__ == "__main__":
    main()
