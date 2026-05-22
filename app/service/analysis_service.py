# -*- coding: utf-8 -*-
"""
文本分析与 OCR 提取服务模块。

提供 AnalysisService（单一设备）与 AnalysisServiceDispatcher（多设备负载均衡），
并包含设备发现与解析逻辑。
"""

from functools import lru_cache
import os
import subprocess
import threading
from typing import Callable

from app.config.settings import settings
from app.service.ocr_service import OCRService
from app.utils.text_utils import preprocess_text

from app.service.analysis.compliance.integrity import IntegrityChecker
from app.service.analysis.compliance.consistency import ConsistencyChecker
from app.service.analysis.reasonableness import ReasonablenessChecker
from app.service.analysis.itemized import ItemizedPricingChecker
from app.service.analysis.deviation import DeviationChecker
from app.service.analysis.verification import VerificationChecker


class AnalysisService:
    """单个 OCR 设备的文本提取与分析服务，封装所有分析检查器。"""

    SUPPORTED_EXTENSIONS = ["pdf", "jpg", "jpeg", "png"]

    def __init__(self, ocr_service: OCRService) -> None:
        """初始化服务并创建所有分析检查器实例。"""
        self.ocr_service = ocr_service
        self.integrity = IntegrityChecker()
        self.consistency = ConsistencyChecker()
        self.reasonableness = ReasonablenessChecker()
        self.itemized = ItemizedPricingChecker()
        self.deviation = DeviationChecker()
        self.verification = VerificationChecker(ocr_service)

    def get_supported_extensions(self) -> list[str]:
        """返回支持的文件扩展名列表副本。"""
        return self.SUPPORTED_EXTENSIONS.copy()

    def extract_text_result(
        self,
        file_path: str,
        file_extension: str,
        *,
        cancel_check: Callable[[], None] | None = None,
    ) -> dict:
        """
        对指定文件执行 OCR 提取，返回标准化的结果字典。

        包含文本、页面、版面段落、表格、印章/签名信息及识别元数据。
        """
        normalized_extension = file_extension.lower().lstrip(".")
        if normalized_extension not in self.SUPPORTED_EXTENSIONS:
            raise ValueError(
                f"Unsupported file type: {file_extension}. "
                f"Supported types: {', '.join(self.SUPPORTED_EXTENSIONS)}."
            )

        if not bool(getattr(self.ocr_service, "available", False)):
            raise RuntimeError("PaddleOCR-VL-1.5 is unavailable.")

        if cancel_check is not None:
            cancel_check()
        ocr_result = self.ocr_service.extract_all(
            file_path,
            normalized_extension,
            cancel_check=cancel_check,
        )
        if cancel_check is not None:
            cancel_check()
        raw_text = str(ocr_result.get("text") or "")
        pages = ocr_result.get("pages") or []
        page_count = len(pages) if isinstance(pages, list) else 0
        layout_sections = ocr_result.get("layout_sections") or []
        native_tables = ocr_result.get("native_tables") or []
        logical_tables = ocr_result.get("logical_tables") or []
        seal_data = ocr_result.get("seals") or {"count": 0, "texts": []}
        signature_data = ocr_result.get("signatures") or {"count": 0, "texts": []}

        try:
            seal_count = int(seal_data.get("count", 0))
        except (TypeError, ValueError):
            seal_count = 0
        try:
            signature_count = int(signature_data.get("count", 0))
        except (TypeError, ValueError):
            signature_count = 0

        # 从版面区段中筛选出表格类型
        table_sections = [
            section
            for section in layout_sections
            if isinstance(section, dict) and str(section.get("type") or "").strip().lower() == "table"
        ]

        return {
            "content": raw_text,
            "text_length": len(raw_text),
            "pages": pages,
            "page_count": page_count,
            "parser_engine": "PaddleOCR-VL-1.5",
            "source_mode": "local",
            "active_device": getattr(self.ocr_service, "active_device", "cpu"),
            "ocr_engine": "PaddleOCR-VL-1.5",
            "ocr_used": True,
            "layout_used": bool(layout_sections),
            "layout_sections": layout_sections,
            "layout_section_count": len(layout_sections),
            "table_sections": table_sections,
            "table_section_count": len(table_sections),
            "native_tables": native_tables,
            "native_table_count": len(native_tables),
            "logical_tables": logical_tables,
            "logical_table_count": len(logical_tables),
            "seal_detected": seal_count > 0,
            "seal_count": seal_count,
            "seal_texts": seal_data.get("texts", []),
            "seal_locations": seal_data.get("locations", []),
            "signature_detected": signature_count > 0,
            "signature_count": signature_count,
            "signature_texts": signature_data.get("texts", []),
            "signature_locations": signature_data.get("locations", []),
            "bbox_coordinate_space": ocr_result.get("bbox_coordinate_space", "ocr_image"),
            "bbox_source_coordinate_space": ocr_result.get("bbox_source_coordinate_space", "ocr_image"),
            "recognition_route": "paddleocr_vl",
            "recognition_reason": "vl_only_pipeline",
            "pdf_mode": "vl_only",
            "pdf_text_stats": {},
            "ppstructure_v3_requested": False,
            "ppstructure_v3_enabled": False,
            "seal_recognition_enabled": bool(settings.PADDLE_VL_USE_SEAL_RECOGNITION),
        }

    def run_full_analysis(self, text: str, extraction_meta: dict) -> dict:
        """执行完整文本分析（完整性、价格合理性、清单逻辑、偏差、印章验证）。"""
        clean_text = preprocess_text(text)
        return {
            "integrity_result": self.integrity.check_integrity(clean_text),
            "pricing_reasonableness": self.reasonableness.check_price_reasonableness(clean_text),
            "itemized_check": self.itemized.check_itemized_logic(text),
            "deviation_result": self.deviation.check_technical_deviation(clean_text),
            "verification_result": self.verification.check_seal_and_date(extraction_meta),
        }


class AnalysisServiceDispatcher:
    """在多设备 AnalysisService 实例间进行负载均衡的分发器。"""

    def __init__(
        self,
        services: list[AnalysisService],
        devices: list[str],
        *,
        max_inflight_per_device: int = 1,
    ) -> None:
        if not services:
            raise ValueError("services cannot be empty")
        if len(services) != len(devices):
            raise ValueError("services/devices length mismatch")

        self._services = services
        self._devices = devices
        self._capacity = max(1, int(max_inflight_per_device))
        # 每个设备一个信号量，控制并发数
        self._permits = [
            threading.BoundedSemaphore(value=self._capacity)
            for _ in services
        ]
        self._inflight = [0 for _ in services]
        self._state_lock = threading.Lock()
        self._rr_cursor = 0

        # 将第一台设备的分析器引用暴露，供纯文本分析使用
        primary = services[0]
        self.integrity = primary.integrity
        self.consistency = primary.consistency
        self.reasonableness = primary.reasonableness
        self.itemized = primary.itemized
        self.deviation = primary.deviation
        self.verification = primary.verification

    def get_supported_extensions(self) -> list[str]:
        return self._services[0].get_supported_extensions()

    def run_full_analysis(self, text: str, extraction_meta: dict) -> dict:
        # 文本分析无需特殊路由，直接使用第一台设备
        return self._services[0].run_full_analysis(text, extraction_meta)

    def _acquire_slot(self) -> int:
        """
        获取一个可用设备槽位，优先负载最低的设备。
        若所有设备已满，则阻塞等待。
        """
        total = len(self._services)
        while True:
            with self._state_lock:
                start = self._rr_cursor
                # 按当前 inflight 数排序，同时尽量轮询
                ordered = sorted(
                    range(total),
                    key=lambda idx: (self._inflight[idx], (idx - start) % total),
                )
                self._rr_cursor = (self._rr_cursor + 1) % total

            for idx in ordered:
                if self._permits[idx].acquire(blocking=False):
                    with self._state_lock:
                        self._inflight[idx] += 1
                    return idx

            # 无可用槽时，阻塞等待第一个设备
            fallback_idx = ordered[0]
            self._permits[fallback_idx].acquire()
            with self._state_lock:
                self._inflight[fallback_idx] += 1
            return fallback_idx

    def _release_slot(self, idx: int) -> None:
        """释放指定设备的槽位，减少 inflight 计数并归还信号量。"""
        with self._state_lock:
            self._inflight[idx] = max(0, self._inflight[idx] - 1)
        self._permits[idx].release()

    def extract_text_result(
        self,
        file_path: str,
        file_extension: str,
        *,
        cancel_check: Callable[[], None] | None = None,
    ) -> dict:
        """获取槽位后，在对应设备上执行 OCR 文本提取。"""
        if cancel_check is not None:
            cancel_check()
        idx = self._acquire_slot()
        try:
            service = self._services[idx]
            device = self._devices[idx]
            if bool(getattr(settings, "PADDLE_OCR_MULTI_GPU_LOG_SCHEDULING", False)):
                print(
                    f"AnalysisServiceDispatcher: route request to worker={idx}, "
                    f"configured_device={device}, active_device={service.ocr_service.active_device}"
                )
            return service.extract_text_result(
                file_path,
                file_extension,
                cancel_check=cancel_check,
            )
        finally:
            self._release_slot(idx)


# 设备标识规范化：数字转为 "gpu:数字" 格式
def _normalize_device_token(raw_value: str) -> str:
    token = str(raw_value or "").strip()
    if not token:
        return ""
    if token.isdigit():
        return f"gpu:{token}"
    return token


# 自动发现系统中可用的 GPU 设备
def _discover_visible_gpu_devices() -> list[str]:
    """按优先级通过环境变量、nvidia-smi、paddle 库探测可用 GPU 列表。"""
    # 1) 检查 CUDA_VISIBLE_DEVICES / NVIDIA_VISIBLE_DEVICES
    for env_name in ("CUDA_VISIBLE_DEVICES", "NVIDIA_VISIBLE_DEVICES"):
        raw_value = str(os.environ.get(env_name, "") or "").strip()
        if not raw_value:
            continue

        lowered = raw_value.lower()
        if lowered in {"none", "void", "no", "false"}:
            return []
        if lowered not in {"all", "auto"}:
            entries = [
                entry.strip()
                for entry in raw_value.replace(";", ",").replace("|", ",").split(",")
                if entry.strip()
            ]
            if entries:
                return [f"gpu:{idx}" for idx in range(len(entries))]

    # 2) 尝试 nvidia-smi 命令
    try:
        result = subprocess.run(
            ["nvidia-smi", "--query-gpu=index", "--format=csv,noheader"],
            check=False,
            capture_output=True,
            text=True,
            timeout=3,
        )
        if result.returncode == 0:
            lines = [line.strip() for line in result.stdout.splitlines() if line.strip()]
            if lines:
                return [f"gpu:{idx}" for idx in range(len(lines))]
    except Exception:
        pass

    # 3) 通过 paddle 库探测
    try:
        import paddle

        cuda_module = getattr(getattr(paddle, "device", None), "cuda", None)
        if cuda_module is not None:
            device_count = getattr(cuda_module, "device_count", None)
            if callable(device_count):
                count = int(device_count() or 0)
                if count > 0:
                    return [f"gpu:{idx}" for idx in range(count)]
    except Exception:
        pass

    return []


# 解析配置中的设备池字符串，返回最终的设备列表
def _resolve_ocr_device_pool() -> list[str]:
    """
    根据配置 PADDLE_OCR_DEVICE_POOL / PADDLE_OCR_DEVICE 确定待使用的设备列表。
    支持 "auto"、"all" 等自动发现关键字。
    """
    raw_pool = str(getattr(settings, "PADDLE_OCR_DEVICE_POOL", "") or "").strip()
    if not raw_pool:
        return [_normalize_device_token(settings.PADDLE_OCR_DEVICE)]

    if raw_pool.lower() in {"auto", "all", "visible", "visible_gpus"}:
        devices = _discover_visible_gpu_devices()
        if devices:
            return devices
        fallback_device = _normalize_device_token(settings.PADDLE_OCR_DEVICE)
        return [fallback_device] if fallback_device else ["cpu"]

    normalized = raw_pool.replace(";", ",").replace("|", ",")
    devices: list[str] = []
    for item in normalized.split(","):
        token = _normalize_device_token(item)
        if not token:
            continue
        if token not in devices:
            devices.append(token)
    if devices:
        return devices

    fallback_device = _normalize_device_token(settings.PADDLE_OCR_DEVICE)
    return [fallback_device] if fallback_device else ["cpu"]


# 模块级单例工厂，缓存分析服务实例
@lru_cache(maxsize=1)
def get_analysis_service() -> AnalysisService | AnalysisServiceDispatcher:
    """
    创建并缓存分析服务实例。
    单设备时返回 AnalysisService，多设备时返回 AnalysisServiceDispatcher。
    """
    devices = _resolve_ocr_device_pool()
    if len(devices) <= 1:
        preferred = devices[0] if devices else None
        return AnalysisService(ocr_service=OCRService(preferred_device=preferred))

    max_inflight = max(1, int(getattr(settings, "PADDLE_OCR_MAX_INFLIGHT_PER_DEVICE", 1)))
    services: list[AnalysisService] = []
    for device in devices:
        services.append(AnalysisService(ocr_service=OCRService(preferred_device=device)))

    print(
        "AnalysisService: multi-device OCR pool initialized "
        f"(devices={devices}, max_inflight_per_device={max_inflight})"
    )
    return AnalysisServiceDispatcher(
        services=services,
        devices=devices,
        max_inflight_per_device=max_inflight,
    )
