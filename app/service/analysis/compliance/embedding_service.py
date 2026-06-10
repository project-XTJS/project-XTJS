"""Offline Chinese embedding service used by template alignment."""

from __future__ import annotations

import threading
from pathlib import Path
from typing import Any

import numpy as np

from app.config.settings import settings


class ChineseEmbeddingService:
    """Process-level, lazily loaded BGE encoder.

    The service never downloads a model at runtime. A missing or broken model is
    reported to callers so deterministic checks can continue.
    """

    _instance: "ChineseEmbeddingService | None" = None
    _instance_lock = threading.Lock()

    def __new__(cls) -> "ChineseEmbeddingService":
        with cls._instance_lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._initialized = False
        return cls._instance

    def __init__(self) -> None:
        if self._initialized:
            return
        self._initialized = True
        self._load_lock = threading.Lock()
        self._tokenizer: Any = None
        self._model: Any = None
        self._torch: Any = None
        self._load_attempted = False
        self._error: str | None = None
        self._cache: dict[str, np.ndarray] = {}

    @property
    def model_path(self) -> Path:
        return Path(settings.CONSISTENCY_EMBEDDING_MODEL_PATH)

    def status(self, *, load: bool = False) -> dict[str, Any]:
        if load:
            self._ensure_loaded()
        configured = self.model_path.exists()
        if self._model is not None:
            state = "loaded"
        elif self._load_attempted:
            state = "unavailable"
        elif configured:
            state = "not_loaded"
        else:
            state = "missing"
        return {
            "status": state,
            "model": settings.CONSISTENCY_EMBEDDING_MODEL_NAME,
            "revision": settings.CONSISTENCY_EMBEDDING_MODEL_REVISION,
            "path": str(self.model_path),
            "device": settings.CONSISTENCY_EMBEDDING_DEVICE,
            "local_files_only": True,
            "error": self._error,
        }

    def encode(self, texts: list[str]) -> np.ndarray | None:
        normalized = [str(text or "").strip() for text in texts]
        if not normalized:
            return np.empty((0, 0), dtype=np.float32)
        if not self._ensure_loaded():
            return None

        missing = [text for text in dict.fromkeys(normalized) if text not in self._cache]
        if missing:
            try:
                self._encode_uncached(missing)
            except Exception as exc:  # pragma: no cover - depends on local model runtime
                self._error = f"{exc.__class__.__name__}: {exc}"
                return None
        return np.stack([self._cache[text] for text in normalized])

    def similarities(self, query: str, candidates: list[str]) -> list[float] | None:
        if not candidates:
            return []
        vectors = self.encode([query, *candidates])
        if vectors is None or len(vectors) != len(candidates) + 1:
            return None
        return [float(score) for score in vectors[1:] @ vectors[0]]

    def _ensure_loaded(self) -> bool:
        if self._model is not None:
            return True
        if self._load_attempted:
            return False
        with self._load_lock:
            if self._model is not None:
                return True
            if self._load_attempted:
                return False
            self._load_attempted = True
            if not self.model_path.exists():
                self._error = f"model path does not exist: {self.model_path}"
                return False
            try:
                import torch
                from transformers import AutoModel, AutoTokenizer

                device = settings.CONSISTENCY_EMBEDDING_DEVICE
                if str(device).startswith("cuda") and not torch.cuda.is_available():
                    device = "cpu"
                self._torch = torch
                self._tokenizer = AutoTokenizer.from_pretrained(
                    str(self.model_path),
                    local_files_only=True,
                )
                self._model = AutoModel.from_pretrained(
                    str(self.model_path),
                    local_files_only=True,
                )
                self._model.to(device)
                self._model.eval()
                self._device = device
                self._error = None
                return True
            except Exception as exc:  # pragma: no cover - depends on optional runtime
                self._tokenizer = None
                self._model = None
                self._error = f"{exc.__class__.__name__}: {exc}"
                return False

    def _encode_uncached(self, texts: list[str]) -> None:
        torch = self._torch
        batch_size = max(1, int(settings.CONSISTENCY_EMBEDDING_BATCH_SIZE))
        max_length = max(16, int(settings.CONSISTENCY_EMBEDDING_MAX_LENGTH))
        for start in range(0, len(texts), batch_size):
            batch = texts[start : start + batch_size]
            inputs = self._tokenizer(
                batch,
                padding=True,
                truncation=True,
                max_length=max_length,
                return_tensors="pt",
            )
            inputs = {key: value.to(self._device) for key, value in inputs.items()}
            with torch.no_grad():
                output = self._model(**inputs)
                vectors = output.last_hidden_state[:, 0]
                vectors = torch.nn.functional.normalize(vectors, p=2, dim=1)
            for text, vector in zip(batch, vectors.cpu().numpy(), strict=True):
                self._cache[text] = np.asarray(vector, dtype=np.float32)


def get_embedding_service() -> ChineseEmbeddingService:
    return ChineseEmbeddingService()


def get_embedding_model_status(*, load: bool = False) -> dict[str, Any]:
    return get_embedding_service().status(load=load)
