"""Demand-started MacBERT CSC manager; idle time unloads the model process."""

import atexit
import hashlib
import importlib.metadata
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
    TypoUnavailable,
    validate_candidates,
)
from .v5 import VERSION as V5_VERSION, ShapeSoundGate, classify_v5, select_candidates

PROMPT_VERSION = "cec3-spelling-only-v1"
V5_FONT_SHA256 = "a3041811a78c361b1de50f953c805e0244951c21c5bd412f7232ef0d899af0da"


def _file_sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


class ModelManager:
    def __init__(
        self,
        command,
        *,
        worker_url,
        model_id,
        cec3_model_id,
        detector_model_id,
        pipeline_version=V5_VERSION,
        eval_trace=False,
        detector_threshold=0.99,
        glyph_threshold=0.75,
        font_path,
        font_sha256,
        pinyin_version,
        pillow_version=None,
        jieba_version="0.42.1",
        cache_path,
        idle_seconds=1800,
        startup_seconds=300,
        request_seconds=30,
        clock=time.monotonic,
        popen=subprocess.Popen,
    ):
        self.command = command
        self.worker_url = worker_url
        self.model_id = model_id
        self.cec3_model_id = cec3_model_id
        if pipeline_version != V5_VERSION:
            raise ValueError("unsupported_typo_pipeline")
        self.pipeline_version = pipeline_version
        self.detector_model_id = detector_model_id
        self.detector_threshold = float(detector_threshold)
        self.glyph_threshold = float(glyph_threshold)
        self.font_sha256 = font_sha256
        self.pinyin_version = pinyin_version
        self.pillow_version = pillow_version
        self.shape_sound_gate = ShapeSoundGate(font_path, glyph_threshold=self.glyph_threshold)
        self.eval_trace = bool(eval_trace)
        self.jieba_version = jieba_version
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
            "eligible_candidates": 0,
            "hidden_candidates": 0,
            "budget_skipped_candidates": 0,
            "cec3_unsupported_candidates": 0,
            "word_invalid_candidates": 0,
            "position_invalid_candidates": 0,
            "detector_rejected_candidates": 0,
            "source_valid_rejected_candidates": 0,
            "similarity_rejected_candidates": 0,
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
                "reference_model": self.cec3_model_id,
                "prompt_version": PROMPT_VERSION,
                "jieba_version": self.jieba_version,
                "min_probability": 0.10,
                "min_probability_ratio": 0.30,
                "rule_version": self.pipeline_version,
                "detector_model": self.detector_model_id,
                "detector_threshold": self.detector_threshold,
                "glyph_threshold": self.glyph_threshold,
                "font_sha256": self.font_sha256,
                "pinyin_version": self.pinyin_version,
                "pillow_version": self.pillow_version,
                "word_rule_version": None,
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
            self.metrics["eligible_candidates"] += int(result.get("eligible_count") or 0)
            self.metrics["hidden_candidates"] += int(result.get("hidden_count") or 0)
            for metric, result_key in (
                ("budget_skipped_candidates", "budget_skipped_count"),
                ("cec3_unsupported_candidates", "cec3_unsupported_count"),
                ("word_invalid_candidates", "word_invalid_count"),
                ("position_invalid_candidates", "position_invalid_count"),
                ("detector_rejected_candidates", "detector_rejected_count"),
                ("source_valid_rejected_candidates", "source_valid_rejected_count"),
                ("similarity_rejected_candidates", "similarity_rejected_count"),
            ):
                self.metrics[metric] += int(result.get(result_key) or 0)

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
            f"{self.model_id}\0{self.pipeline_version}\0min_probability=0.10"
            f"\0min_probability_ratio=0.30\0top_k=2"
            f"\0{self.cec3_model_id}\0{PROMPT_VERSION}"
            f"\0{self.jieba_version}"
            f"\0{self.detector_model_id}\0{self.detector_threshold:.6f}"
            f"\0{self.glyph_threshold:.6f}\0{self.font_sha256}\0{self.pinyin_version}\0{self.pillow_version}"
            f"\0trace={int(self.eval_trace)}\0{text}"
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
                try:
                    if not inference_started:
                        inference_started = True
                        inference_started_at = self.clock()
                        with self.lock:
                            self.metrics["inference_checks"] += 1
                    detected = self._request("/detect", {"text": text}, timeout=self.request_seconds)
                    detector_scores = detected.get("scores")
                    if not isinstance(detector_scores, list) or len(detector_scores) != len(text) or any(
                        score is not None and (not isinstance(score, (int, float)) or not 0 <= score <= 1)
                        for score in detector_scores
                    ):
                        raise TypoUnavailable("原文检测器输出无效")
                    detector_positive = any(
                        score is not None and score >= self.detector_threshold for score in detector_scores
                    )
                    candidates = []
                    if detector_positive or self.eval_trace:
                        response = self._request(
                            "/correct",
                            {"text": text, "min_probability": 0.10,
                             "min_probability_ratio": 0.30, "top_k": 2},
                            timeout=self.request_seconds,
                        )
                        candidates = validate_candidates(text, response, allow_subunit_ratio=True)
                    eligible, detector_rejected, budget_skipped, source_valid_rejected = select_candidates(
                        candidates, detector_scores, threshold=self.detector_threshold,
                        source_text=text, limit=len(candidates) if self.eval_trace else 8,
                    )
                    corrected = roundtrip = text
                    if eligible:
                        first_pass = self._request("/cec3", {"text": text}, timeout=self.request_seconds)
                        corrected = first_pass.get("corrected_text")
                        if not isinstance(corrected, str) or not corrected or any(
                            marker in corrected for marker in ("输入文本：", "纠正后的文本：", "```", "<|im_start|>")
                        ):
                            raise TypoUnavailable("CEC3 纠错输出无效")
                        if corrected != text:
                            second_pass = self._request("/cec3", {"text": corrected}, timeout=self.request_seconds)
                            roundtrip = second_pass.get("corrected_text")
                            if not isinstance(roundtrip, str) or not roundtrip or any(
                                marker in roundtrip for marker in ("输入文本：", "纠正后的文本：", "```", "<|im_start|>")
                            ):
                                raise TypoUnavailable("CEC3 回程输出无效")
                    trace_reasons = {} if self.eval_trace else None
                    issues, counts = classify_v5(
                        text, eligible, corrected, roundtrip,
                        gate=self.shape_sound_gate, reasons=trace_reasons,
                    )
                    counts["source_valid_rejected_count"] += source_valid_rejected
                    result = {
                        "status": "completed", "issues": issues, "review_candidates": [],
                        **counts, "candidate_count": len(candidates), "eligible_count": len(eligible),
                        "hidden_count": len(candidates) - len(issues),
                        "detector_rejected_count": detector_rejected,
                        "budget_skipped_count": budget_skipped,
                        "confirmed_count": len(issues), "review_candidate_count": 0,
                        "model": self.model_id, "reference_model": self.cec3_model_id,
                        "detector_model": self.detector_model_id,
                        "detector_threshold": self.detector_threshold,
                        "glyph_threshold": self.glyph_threshold,
                        "font_sha256": self.font_sha256, "pinyin_version": self.pinyin_version,
                        "pillow_version": self.pillow_version,
                        "verification_method": "original_detector_macbert_cec3",
                        "prompt_version": PROMPT_VERSION, "jieba_version": self.jieba_version,
                        "min_probability": 0.10, "min_probability_ratio": 0.30,
                        "rule_version": self.pipeline_version, "word_rule_version": None,
                    }
                    if self.eval_trace:
                        result["eval_trace"] = True
                        result["trace_score_floor"] = self.detector_threshold
                        result["raw_candidates"] = [
                            {**item, "detector_score": detector_scores[item["start"]]}
                            for item in candidates
                        ]
                        result["budget_candidates"] = eligible
                        result["calibration_candidates"] = issues
                        result["detector_scores"] = detector_scores
                        result["calibration_reasons"] = [
                            {"start": start, "replacement": replacement, "reason": reason}
                            for (start, replacement), reason in trace_reasons.items()
                        ]
                    with sqlite3.connect(self.cache_path) as db:
                        db.execute("INSERT OR REPLACE INTO checks VALUES (?,?)", (key, json.dumps(result, ensure_ascii=False)))
                    self._record_completed(result, cache_hit=False)
                    return dict(result, cache_hit=False)
                except (urllib.error.URLError, TimeoutError) as exc:
                    self._stop()
                    raise TypoUnavailable("纠错模型暂不可用") from exc
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
    pipeline_version = os.environ.get("TYPO_PIPELINE_VERSION", V5_VERSION)
    if pipeline_version != V5_VERSION:
        raise RuntimeError("仅支持 v5 错别字管线")
    path = Path(os.environ["TYPO_MODEL_PATH"])
    manifest = json.loads((path / "model-manifest.json").read_text())
    model_id = manifest["model"] + "@" + manifest["revision"] + "#" + manifest["model_sha256"]
    cec3_path = Path(os.environ["TYPO_CEC3_MODEL_PATH"])
    cec3_manifest = json.loads((cec3_path / "model-manifest.json").read_text())
    cec3_model_id = (
        cec3_manifest["model"] + "@" + cec3_manifest["revision"]
        + "#" + cec3_manifest["model_sha256"]
    )
    worker_url = "http://127.0.0.1:8001"
    command = [
        sys.executable,
        "-m",
        "typo_runtime.worker",
        "--model",
        str(path),
        "--cec3-model",
        str(cec3_path),
        "--detector-model",
        os.environ["TYPO_DETECTOR_MODEL_PATH"],
        "--host",
        "127.0.0.1",
        "--port",
        "8001",
        "--device",
        os.environ.get("TYPO_DEVICE", "auto"),
    ]
    detector_path = Path(os.environ["TYPO_DETECTOR_MODEL_PATH"])
    detector_manifest = json.loads((detector_path / "model-manifest.json").read_text())
    if _file_sha256(detector_path / "model.safetensors") != detector_manifest["model_sha256"]:
        raise RuntimeError("原文检测器权重校验失败")
    tokenizer_hash = _file_sha256(detector_path / "tokenizer.json")
    detector_model_id = (
        detector_manifest["model"] + "@" + detector_manifest["revision"]
        + "#" + detector_manifest["model_sha256"] + "+" + tokenizer_hash
    )
    font_path = Path(os.environ["TYPO_V5_FONT_PATH"])
    font_sha256 = _file_sha256(font_path)
    if font_sha256 != V5_FONT_SHA256:
        raise RuntimeError("v5 字体文件与固定版本不一致")
    if not (font_path.parent / "OFL-LICENSE.txt").is_file():
        raise RuntimeError("v5 字体许可证缺失")
    pinyin_version = importlib.metadata.version("pypinyin")
    pillow_version = importlib.metadata.version("Pillow")
    manager = ModelManager(
        command,
        worker_url=worker_url,
        model_id=model_id,
        cec3_model_id=cec3_model_id,
        detector_model_id=detector_model_id,
        detector_threshold=float(os.environ.get("TYPO_DETECTOR_THRESHOLD", "0.99")),
        glyph_threshold=float(os.environ.get("TYPO_GLYPH_THRESHOLD", "0.75")),
        font_path=font_path,
        font_sha256=font_sha256,
        pinyin_version=pinyin_version,
        pillow_version=pillow_version,
        pipeline_version=pipeline_version,
        eval_trace=os.environ.get("TYPO_V5_EVAL_TRACE", "false").lower() == "true",
        cache_path=os.environ.get("TYPO_CACHE_PATH", "/cache/checks.sqlite"),
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
