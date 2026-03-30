import os
import subprocess
import threading
import time
from collections import deque
from pathlib import Path
from typing import Any

try:
    import psutil
except Exception:  # pragma: no cover - optional dependency fallback
    psutil = None


class OCRProgressMonitor:
    _STAGE_RANGES: dict[str, tuple[float, float]] = {
        "prepare": (0.0, 0.08),
        "predict": (0.08, 0.72),
        "restructure": (0.72, 0.80),
        "postprocess": (0.80, 0.97),
        "tables": (0.97, 1.0),
        "done": (1.0, 1.0),
    }

    def __init__(
        self,
        *,
        file_path: str,
        file_type: str,
        device: str,
        total_pages: int,
        enabled: bool,
        bar_width: int,
        keep_recent_updates: int,
    ) -> None:
        self.file_path = file_path
        self.file_type = file_type
        self.device = str(device or "cpu")
        self.total_pages = max(0, int(total_pages or 0))
        self.enabled = bool(enabled)
        self.bar_width = max(12, int(bar_width or 24))
        self._recent_updates = deque(maxlen=max(1, int(keep_recent_updates or 12)))

        self._job_name = f"{Path(file_path).name}@{self.device}"
        self._gpu_index = self._parse_gpu_index(self.device)
        self._process = psutil.Process(os.getpid()) if psutil else None
        self._lock = threading.Lock()

        self._started_at = 0.0
        self._finished_at = 0.0
        self._stage = "prepare"
        self._stage_entered_at = 0.0
        self._stage_durations: dict[str, float] = {}
        self._progress = 0.0
        self._detail = "initializing"
        self._predict_pages = 0
        self._postprocess_pages = 0
        self._status = "running"
        self._error_message = ""
        self._latest_snapshot: dict[str, Any] = {}
        self._aggregates: dict[str, dict[str, float]] = {}

    def start(self) -> None:
        if self._started_at:
            return

        self._started_at = time.perf_counter()
        self._stage_entered_at = self._started_at

        if psutil:
            try:
                psutil.cpu_percent(interval=None)
            except Exception:
                pass
        self.update(stage="prepare", current=0, total=max(self.total_pages, 1), detail="preparing", emit=False)

    def update(
        self,
        *,
        stage: str,
        current: int = 0,
        total: int | None = None,
        detail: str = "",
        emit: bool = False,
    ) -> None:
        line: str | None = None
        with self._lock:
            self._switch_stage_locked(stage)
            if detail:
                self._detail = detail

            if total is not None and total > 0:
                self.total_pages = max(self.total_pages, int(total))

            if stage == "predict":
                self._predict_pages = max(self._predict_pages, int(current))
            elif stage == "postprocess":
                self._postprocess_pages = max(self._postprocess_pages, int(current))

            stage_total = int(total) if total else max(self.total_pages, int(current), 1)
            self._progress = max(self._progress, self._compute_progress(stage, int(current), stage_total))
            snapshot = self._collect_snapshot()
            self._latest_snapshot = snapshot
            self._record_snapshot_locked(snapshot)
            if emit:
                line = self._emit_locked()

        if line:
            print(line, flush=True)

    def finish(self, *, success: bool, error_message: str = "") -> dict[str, Any]:
        line: str | None = None
        with self._lock:
            self._latest_snapshot = self._collect_snapshot()
            self._record_snapshot_locked(self._latest_snapshot)
            self._switch_stage_locked("done")
            self._progress = 1.0
            self._status = "success" if success else "failed"
            self._error_message = str(error_message or "")
            self._detail = "completed" if success else "failed"
            self._finished_at = time.perf_counter()
            line = self._emit_locked()

        if line:
            print(line, flush=True)
        return self.build_summary()

    def build_summary(self) -> dict[str, Any]:
        with self._lock:
            elapsed = self._elapsed_locked()
            total_pages = max(self.total_pages, self._postprocess_pages, self._predict_pages)
            predict_duration = self._stage_duration_locked("predict")
            postprocess_duration = self._stage_duration_locked("postprocess")
            summary = {
                "enabled": self.enabled,
                "job_name": self._job_name,
                "device": self.device,
                "file_type": self.file_type,
                "status": self._status,
                "error_message": self._error_message,
                "total_pages": total_pages,
                "predict_pages": self._predict_pages,
                "processed_pages": self._postprocess_pages,
                "total_elapsed_seconds": self._round(elapsed),
                "predict_elapsed_seconds": self._round(predict_duration),
                "postprocess_elapsed_seconds": self._round(postprocess_duration),
                "ocr_speed_pages_per_second": self._rate(total_pages, elapsed),
                "predict_speed_pages_per_second": self._rate(self._predict_pages, predict_duration),
                "progress_percent": round(self._progress * 100.0, 1),
                "stage_durations": {
                    key: self._round(value)
                    for key, value in sorted(self._resolved_stage_durations_locked().items())
                    if value > 0
                },
                "system": self._build_system_summary_locked(),
                "recent_updates": list(self._recent_updates),
            }
            return summary

    def _collect_snapshot(self) -> dict[str, Any]:
        snapshot: dict[str, Any] = {
            "timestamp": self._round(self._elapsed_now()),
            "cpu_percent": None,
            "memory_percent": None,
            "process_rss_mb": None,
            "gpu_util_percent": None,
            "gpu_memory_percent": None,
            "gpu_memory_used_mb": None,
            "gpu_memory_total_mb": None,
            "gpu_temperature_c": None,
        }

        if psutil:
            try:
                snapshot["cpu_percent"] = float(psutil.cpu_percent(interval=None))
            except Exception:
                pass
            try:
                snapshot["memory_percent"] = float(psutil.virtual_memory().percent)
            except Exception:
                pass
            if self._process is not None:
                try:
                    rss = self._process.memory_info().rss / (1024 * 1024)
                    snapshot["process_rss_mb"] = float(rss)
                except Exception:
                    pass

        gpu_snapshot = self._collect_gpu_snapshot()
        snapshot.update(gpu_snapshot)
        return snapshot

    def _collect_gpu_snapshot(self) -> dict[str, Any]:
        if self._gpu_index is None:
            return {}

        command = [
            "nvidia-smi",
            "-i",
            str(self._gpu_index),
            "--query-gpu=index,utilization.gpu,utilization.memory,memory.used,memory.total,temperature.gpu",
            "--format=csv,noheader,nounits",
        ]

        startupinfo = None
        creationflags = 0
        if os.name == "nt":
            startupinfo = subprocess.STARTUPINFO()
            startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
            creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)

        try:
            completed = subprocess.run(
                command,
                capture_output=True,
                text=True,
                timeout=2,
                check=False,
                startupinfo=startupinfo,
                creationflags=creationflags,
            )
        except Exception:
            return {}

        if completed.returncode != 0 or not completed.stdout.strip():
            return {}

        row = completed.stdout.strip().splitlines()[0]
        parts = [part.strip() for part in row.split(",")]
        if len(parts) < 6:
            return {}

        gpu_memory_used = self._to_float(parts[3])
        gpu_memory_total = self._to_float(parts[4])
        gpu_memory_percent = None
        if gpu_memory_used is not None and gpu_memory_total and gpu_memory_total > 0:
            gpu_memory_percent = (gpu_memory_used / gpu_memory_total) * 100.0

        return {
            "gpu_util_percent": self._to_float(parts[1]),
            "gpu_memory_percent": gpu_memory_percent,
            "gpu_memory_used_mb": gpu_memory_used,
            "gpu_memory_total_mb": gpu_memory_total,
            "gpu_temperature_c": self._to_float(parts[5]),
        }

    def _record_snapshot_locked(self, snapshot: dict[str, Any]) -> None:
        for key, value in snapshot.items():
            if key == "timestamp" or value is None:
                continue
            aggregate = self._aggregates.setdefault(
                key,
                {"sum": 0.0, "count": 0.0, "max": float("-inf")},
            )
            numeric_value = float(value)
            aggregate["sum"] += numeric_value
            aggregate["count"] += 1.0
            aggregate["max"] = max(aggregate["max"], numeric_value)

    def _emit_locked(self) -> str | None:
        if not self.enabled:
            return None
        event = self._build_event_locked()
        self._recent_updates.append(event)
        return self._format_line(event)

    def _build_event_locked(self) -> dict[str, Any]:
        total_pages = max(self.total_pages, self._predict_pages, self._postprocess_pages)
        latest = dict(self._latest_snapshot)
        event = {
            "elapsed_seconds": self._round(self._elapsed_locked()),
            "stage": self._stage,
            "detail": self._detail,
            "progress_percent": round(self._progress * 100.0, 1),
            "total_pages": total_pages,
            "pages_completed": max(self._predict_pages, self._postprocess_pages),
            "predict_pages": self._predict_pages,
            "processed_pages": self._postprocess_pages,
            "ocr_speed_pages_per_second": self._rate(
                max(self._predict_pages, self._postprocess_pages),
                self._elapsed_locked(),
            ),
        }
        event.update(
            {
                "cpu_percent": self._round(latest.get("cpu_percent")),
                "memory_percent": self._round(latest.get("memory_percent")),
                "process_rss_mb": self._round(latest.get("process_rss_mb")),
                "gpu_util_percent": self._round(latest.get("gpu_util_percent")),
                "gpu_memory_percent": self._round(latest.get("gpu_memory_percent")),
                "gpu_memory_used_mb": self._round(latest.get("gpu_memory_used_mb")),
                "gpu_memory_total_mb": self._round(latest.get("gpu_memory_total_mb")),
                "gpu_temperature_c": self._round(latest.get("gpu_temperature_c")),
            }
        )
        return event

    def _format_line(self, event: dict[str, Any]) -> str:
        progress = max(0.0, min(100.0, float(event.get("progress_percent") or 0.0)))
        filled = int(round((progress / 100.0) * self.bar_width))
        bar = "#" * filled + "-" * max(0, self.bar_width - filled)

        parts = [
            f"[OCR] {self._job_name}",
            f"[{bar}]",
            f"{progress:5.1f}%",
            f"stage={event.get('stage')}",
        ]

        if event.get("total_pages"):
            parts.append(
                f"pages={event.get('pages_completed')}/{event.get('total_pages')}"
            )

        if event.get("ocr_speed_pages_per_second") is not None:
            parts.append(f"speed={event['ocr_speed_pages_per_second']:.2f} p/s")

        if event.get("cpu_percent") is not None:
            parts.append(f"cpu={event['cpu_percent']:.1f}%")
        if event.get("memory_percent") is not None:
            parts.append(f"mem={event['memory_percent']:.1f}%")
        if event.get("process_rss_mb") is not None:
            parts.append(f"rss={event['process_rss_mb']:.0f}MB")
        if event.get("gpu_util_percent") is not None:
            parts.append(f"gpu={event['gpu_util_percent']:.1f}%")
        if event.get("gpu_memory_used_mb") is not None and event.get("gpu_memory_total_mb") is not None:
            parts.append(
                f"gpu_mem={event['gpu_memory_used_mb']:.0f}/{event['gpu_memory_total_mb']:.0f}MB"
            )
        if event.get("gpu_temperature_c") is not None:
            parts.append(f"gpu_temp={event['gpu_temperature_c']:.0f}C")

        parts.append(f"elapsed={event.get('elapsed_seconds', 0.0):.1f}s")
        if event.get("detail"):
            parts.append(f"detail={event['detail']}")
        return " ".join(parts)

    def _compute_progress(self, stage: str, current: int, total: int) -> float:
        start, end = self._STAGE_RANGES.get(stage, (0.0, 1.0))
        if end <= start:
            return end

        safe_total = max(1, int(total or 1))
        ratio = max(0.0, min(1.0, float(current) / float(safe_total)))
        return start + ((end - start) * ratio)

    def _switch_stage_locked(self, stage: str) -> None:
        if stage == self._stage:
            return
        now = time.perf_counter()
        if self._stage_entered_at > 0:
            self._stage_durations[self._stage] = self._stage_durations.get(self._stage, 0.0) + (
                now - self._stage_entered_at
            )
        self._stage = stage
        self._stage_entered_at = now

    def _resolved_stage_durations_locked(self) -> dict[str, float]:
        durations = dict(self._stage_durations)
        if self._stage_entered_at > 0:
            durations[self._stage] = durations.get(self._stage, 0.0) + (
                time.perf_counter() - self._stage_entered_at
            )
        return durations

    def _stage_duration_locked(self, stage: str) -> float:
        return self._resolved_stage_durations_locked().get(stage, 0.0)

    def _build_system_summary_locked(self) -> dict[str, Any]:
        summary: dict[str, Any] = {}
        for key, aggregate in self._aggregates.items():
            count = aggregate.get("count", 0.0)
            if count <= 0:
                continue
            summary[key] = {
                "avg": self._round(aggregate["sum"] / count),
                "max": self._round(aggregate["max"]),
            }
        if self._latest_snapshot:
            summary["latest"] = {
                key: self._round(value)
                for key, value in self._latest_snapshot.items()
                if key != "timestamp" and value is not None
            }
        return summary

    def _elapsed_locked(self) -> float:
        if not self._started_at:
            return 0.0
        end = self._finished_at or time.perf_counter()
        return max(0.0, end - self._started_at)

    def _elapsed_now(self) -> float:
        if not self._started_at:
            return 0.0
        return max(0.0, time.perf_counter() - self._started_at)

    @staticmethod
    def _parse_gpu_index(device: str) -> int | None:
        normalized = str(device or "").strip().lower()
        if not normalized.startswith("gpu"):
            return None
        if ":" not in normalized:
            return 0
        _, raw_index = normalized.split(":", 1)
        try:
            return int(raw_index.strip())
        except ValueError:
            return 0

    @staticmethod
    def _to_float(value: Any) -> float | None:
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _rate(units: int, elapsed_seconds: float) -> float | None:
        if units <= 0 or elapsed_seconds <= 0:
            return None
        return round(float(units) / float(elapsed_seconds), 3)

    @staticmethod
    def _round(value: Any) -> float | None:
        if value is None:
            return None
        try:
            return round(float(value), 3)
        except (TypeError, ValueError):
            return None
