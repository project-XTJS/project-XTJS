# -*- coding: utf-8 -*-
import os
import copy
import re
import shutil
import threading
import uuid
from pathlib import Path
from typing import Any, Callable
from app.config.settings import settings
from app.service.ocr_progress import OCRProgressMonitor

class OCREngineMixin:
    """
    OCR 引擎调度与生命周期混入类。
    负责 PaddleOCRVL 实例的初始化、线程绑定限制、以及文档的分块预测调度。
    """

    def _bind_engine_thread(self) -> None:
        """
        Paddle 动态图模型对线程上下文敏感。
        我们在初始化时绑定专属的引擎线程，后续推理强制切回该线程。
        """
        current_ident = threading.get_ident()
        if getattr(self, "_engine_thread_id", None) is None:
            self._engine_thread_id = current_ident

    def _is_engine_thread(self) -> bool:
        return getattr(self, "_engine_thread_id", None) is not None and threading.get_ident() == self._engine_thread_id

    def _run_on_engine_thread(self, func, /, *args, **kwargs):
        """将引擎相关的核心调用代理到初始化的专属线程中执行，防止出现参数内存位置错误。"""
        if self._is_engine_thread():
            return func(*args, **kwargs)
        def _runner():
            self._bind_engine_thread()
            return func(*args, **kwargs)
        future = self._engine_executor.submit(_runner)
        return future.result()

    def _describe_document(self, file_path: str, file_type: str, total_pages: int) -> str:
        file_name = Path(file_path).name
        normalized_type = str(file_type or "").strip().lower().lstrip(".") or "unknown"
        page_label = total_pages if total_pages > 0 else "unknown"
        return f"file={file_name}, type={normalized_type}, estimated_pages={page_label}"

    def _runtime_cache_dirs(self) -> tuple[str, ...]:
        runtime_root = settings.OCR_STORAGE_ROOT
        return (str(runtime_root / ".cache"), str(runtime_root / "hf-home"), str(runtime_root / "hf-cache"), str(runtime_root / "modelscope-cache"), str(runtime_root / "aistudio-cache"))

    def _prepare_runtime_dirs(self) -> None:
        """创建模型运行时依赖的各类缓存目录。"""
        for path in (settings.OCR_STORAGE_ROOT, settings.PADDLE_PDX_CACHE_HOME, settings.OCR_RUNTIME_TEMP_DIR, *self._runtime_cache_dirs()):
            os.makedirs(path, exist_ok=True)

    def _prepare_runtime_env(self) -> None:
        """设置 HuggingFace、ModelScope 等下载缓存所需的环境变量。"""
        runtime_tmp = str(settings.OCR_RUNTIME_TEMP_DIR)
        runtime_root = settings.OCR_STORAGE_ROOT
        os.environ["PADDLE_PDX_CACHE_HOME"] = str(settings.PADDLE_PDX_CACHE_HOME)
        os.environ["PADDLE_PDX_DISABLE_MODEL_SOURCE_CHECK"] = "1" if settings.PADDLE_PDX_DISABLE_MODEL_SOURCE_CHECK else "0"
        os.environ["TMPDIR"] = os.environ["TMP"] = os.environ["TEMP"] = runtime_tmp
        os.environ["XDG_CACHE_HOME"] = str(runtime_root / ".cache")
        os.environ["HF_HOME"] = str(runtime_root / "hf-home")
        os.environ["HUGGINGFACE_HUB_CACHE"] = str(runtime_root / "hf-cache")
        os.environ["MODELSCOPE_CACHE"] = str(runtime_root / "modelscope-cache")
        os.environ["AISTUDIO_CACHE_HOME"] = str(runtime_root / "aistudio-cache")

    def _patch_paddle_tensor_int(self) -> None:
        """
        Hotfix：修复 paddle 张量在静态图下转 int 时可能引发的 shape 报错问题。
        为 tensor 的 __int__ 方法增加 fallback。
        """
        try:
            import numpy as np
            import paddle
        except Exception:
            return
        tensor_type = type(paddle.to_tensor([0]))
        current_int = getattr(tensor_type, "__int__", None)
        if current_int is None or getattr(current_int, "_xtjs_len1_tensor_patch", False):
            return
        def _patched_tensor_int(value: Any) -> int:
            try: return current_int(value)
            except TypeError:
                array = np.asarray(value)
                if getattr(array, "size", 0) == 1: return int(array.reshape(-1)[0].item())
                raise
        _patched_tensor_int._xtjs_len1_tensor_patch = True
        tensor_type.__int__ = _patched_tensor_int

    def _patch_paddlex_paddleocr_vl_processor(self) -> None:
        """
        Hotfix：修补 PaddleX doc-vlm processor，避免在静态图中出现 int(Tensor) 错误。
        通过额外提取一份 numpy 格式的网格尺寸来绕过 Tensor 计算。
        """
        import sys
        common_module = sys.modules.get("paddlex.inference.models.doc_vlm.processors.common")
        processor_module = sys.modules.get("paddlex.inference.models.doc_vlm.processors.paddleocr_vl._paddleocr_vl")
        if common_module is None or processor_module is None: return
        BatchFeature = common_module.BatchFeature
        PaddleOCRVLProcessor = processor_module.PaddleOCRVLProcessor
        fetch_image = processor_module.fetch_image
        current_preprocess = getattr(PaddleOCRVLProcessor, "preprocess", None)
        if current_preprocess is None or getattr(current_preprocess, "_xtjs_numpy_grid_patch", False): return
        
        def _patched_preprocess(processor_self, input_dicts, min_pixels=None, max_pixels=None):
            images = [fetch_image(input_dict["image"]) for input_dict in input_dicts]
            text = []
            for input_dict in input_dicts:
                messages = [{"role": "user", "content": [{"type": "image", "image": "placeholder"}, {"type": "text", "text": input_dict["query"]}]}]
                text.append(processor_self.tokenizer.apply_chat_template(messages, tokenize=False))
            output_kwargs = {"tokenizer_init_kwargs": processor_self.tokenizer.init_kwargs, "text_kwargs": copy.deepcopy(processor_self._DEFAULT_TEXT_KWARGS), "video_kwargs": copy.deepcopy(processor_self._DEFAULT_VIDEO_KWARGS)}
            size = {"min_pixels": min_pixels or processor_self.image_processor.min_pixels, "max_pixels": max_pixels or processor_self.image_processor.max_pixels} if min_pixels is not None or max_pixels is not None else None
            
            image_inputs, image_grid_thw, image_grid_thw_for_placeholder = {}, None, None
            if images is not None:
                image_inputs = processor_self.image_processor(images=images, size=size, return_tensors="pd")
                image_grid_thw = image_inputs.get("image_grid_thw")
                if image_grid_thw is not None:
                    placeholder_inputs = processor_self.image_processor(images=images, size=size, return_tensors="np")
                    image_grid_thw_for_placeholder = placeholder_inputs.get("image_grid_thw")
                    
            videos_inputs, video_grid_thw = {}, None
            if not isinstance(text, list): text = [text]
            merge_length = int(processor_self.image_processor.merge_size) ** 2
            
            if image_grid_thw is not None:
                index = 0
                for i in range(len(text)):
                    while processor_self.image_token in text[i]:
                        grid_source = image_grid_thw_for_placeholder if image_grid_thw_for_placeholder is not None else image_grid_thw
                        placeholder_count = max(int(grid_source[index].prod()) // merge_length, 0)
                        text[i] = text[i].replace(processor_self.image_token, "<|placeholder|>" * placeholder_count, 1)
                        index += 1
                    text[i] = text[i].replace("<|placeholder|>", processor_self.image_token)
                    
            if video_grid_thw is not None:
                index = 0
                for i in range(len(text)):
                    while processor_self.video_token in text[i]:
                        placeholder_count = max(int(video_grid_thw[index].prod()) // merge_length, 0)
                        text[i] = text[i].replace(processor_self.video_token, "<|placeholder|>" * placeholder_count, 1)
                        index += 1
                    text[i] = text[i].replace("<|placeholder|>", processor_self.video_token)
                    
            text_inputs = processor_self.tokenizer(text, **output_kwargs["text_kwargs"])
            return BatchFeature(data={**text_inputs, **image_inputs, **videos_inputs}, tensor_type="pd")
            
        _patched_preprocess._xtjs_numpy_grid_patch = True
        PaddleOCRVLProcessor.preprocess = _patched_preprocess

    def _candidate_devices(self) -> list[str]:
        """决定模型加载时尝试设备的先后顺序。"""
        primary_device = self.preferred_device or settings.PADDLE_OCR_DEVICE
        candidates = [primary_device]
        if self.preferred_device is None and settings.PADDLE_OCR_DEVICE.startswith("gpu:"): candidates.append("gpu")
        if settings.PADDLE_OCR_FALLBACK_TO_CPU: candidates.append("cpu")
        unique_candidates = []
        for device in candidates:
            token = str(device or "").strip()
            if token and token not in unique_candidates: unique_candidates.append(token)
        return unique_candidates

    def _build_pipeline_kwargs(self, device: str) -> dict[str, Any]:
        """组装 PaddleOCRVL 所需的实例化配置字典。"""
        return {
            "device": device, "pipeline_version": settings.PADDLE_VL_PIPELINE_VERSION,
            "use_doc_orientation_classify": settings.PADDLE_OCR_USE_DOC_ORIENTATION,
            "use_doc_unwarping": settings.PADDLE_OCR_USE_DOC_UNWARPING,
            "use_layout_detection": settings.PADDLE_VL_USE_LAYOUT_DETECTION,
            "use_chart_recognition": settings.PADDLE_VL_USE_CHART_RECOGNITION,
            "use_seal_recognition": settings.PADDLE_VL_USE_SEAL_RECOGNITION,
            "use_signature_recognition": settings.PADDLE_VL_USE_SIGNATURE_RECOGNITION,
            "use_ocr_for_image_block": settings.PADDLE_VL_USE_OCR_FOR_IMAGE_BLOCK,
            "format_block_content": settings.PADDLE_VL_FORMAT_BLOCK_CONTENT,
            "merge_layout_blocks": settings.PADDLE_VL_MERGE_LAYOUT_BLOCKS,
            "use_queues": settings.PADDLE_VL_USE_QUEUES,
        }

    def _extract_unknown_argument(self, exc: Exception) -> str | None:
        match = re.search(r"Unknown argument:\s*([A-Za-z0-9_]+)", str(exc or ""))
        return match.group(1) if match else None

    def _instantiate_pipeline(self, pipeline_cls: Any, device: str) -> tuple[Any, list[str]]:
        """带有参数降级兼容机制的 Pipeline 实例化逻辑。"""
        kwargs = dict(self._build_pipeline_kwargs(device))
        disabled_args = []
        while True:
            try: return pipeline_cls(**kwargs), disabled_args
            except Exception as exc:
                unknown_arg = self._extract_unknown_argument(exc)
                if not unknown_arg or unknown_arg not in kwargs: raise
                kwargs.pop(unknown_arg, None)
                disabled_args.append(unknown_arg)
                print(f"OCRService: retrying model load without unsupported argument {unknown_arg!r} (device={device})", flush=True)

    def _init_engine(self) -> None:
        """核心初始化函数，按优先级轮询设备并加载模型。"""
        if os.name == "nt":
            try: import torch  # noqa: F401
            except Exception as exc: print(f"OCRService: optional torch preload skipped on Windows (reason: {exc})", flush=True)
        try:
            self._patch_paddle_tensor_int()
            from paddleocr import PaddleOCRVL
        except Exception as exc:
            print(f"OCRService bootstrap failed: {exc}", flush=True)
            return
            
        last_error = None
        for device in self._candidate_devices():
            try:
                print(f"OCRService: model loading started (device={device}, pipeline_version={settings.PADDLE_VL_PIPELINE_VERSION})", flush=True)
                self.pipeline, disabled_args = self._instantiate_pipeline(PaddleOCRVL, device)
                # 模型加载完毕后注入 numpy 补丁
                self._patch_paddlex_paddleocr_vl_processor()
                self.available, self.active_device = True, device
                if disabled_args: print(f"OCRService: model loading completed with compatibility fallback (device={self.active_device}, disabled_args={disabled_args})", flush=True)
                print(f"OCRService: model loading completed (device={self.active_device}, pipeline_version={settings.PADDLE_VL_PIPELINE_VERSION})", flush=True)
                return
            except Exception as exc:
                last_error = exc
                print(f"OCRService: model loading failed (device={device}): {exc}", flush=True)
                
        self.pipeline, self.available = None, False
        print(f"OCRService bootstrap failed: {last_error}", flush=True)

    def _estimate_total_pages(self, file_path: str, file_type: str) -> int:
        """在推理前快速预估文档总页数。"""
        normalized_type = str(file_type or "").strip().lower().lstrip(".")
        if normalized_type in {"jpg", "jpeg", "png", "bmp", "tif", "tiff"}: return 1
        if normalized_type != "pdf": return 0
        try:
            import pypdfium2 as pdfium
            document = pdfium.PdfDocument(file_path)
            try: return len(document)
            finally:
                if callable(getattr(document, "close", None)): document.close()
        except Exception: return 0

    def _build_progress_monitor(self, *, file_path: str, file_type: str, total_pages: int) -> OCRProgressMonitor:
        """构建进度与硬件资源监控器实例。"""
        return OCRProgressMonitor(
            file_path=file_path, file_type=file_type, device=self.active_device, total_pages=total_pages,
            enabled=bool(getattr(settings, "OCR_PROGRESS_ENABLED", True)), bar_width=int(getattr(settings, "OCR_PROGRESS_BAR_WIDTH", 24)),
            keep_recent_updates=int(getattr(settings, "OCR_PROGRESS_KEEP_RECENT_UPDATES", 12)), heartbeat_seconds=float(getattr(settings, "OCR_PROGRESS_HEARTBEAT_SECONDS", 2.0)),
        )

    def _stage_input_for_pipeline(self, file_path: str) -> tuple[str, Path | None]:
        """避免非 ASCII 路径导致的底层 C++ 报错，必要时拷贝至暂存区。"""
        source = Path(file_path)
        try:
            str(source).encode("ascii")
            return str(source), None
        except UnicodeEncodeError: pass
        staging_dir = Path(settings.OCR_RUNTIME_TEMP_DIR) / "input-staging"
        staging_dir.mkdir(parents=True, exist_ok=True)
        staged_path = staging_dir / f"{uuid.uuid4().hex}{source.suffix or '.bin'}"
        shutil.copy2(source, staged_path)
        return str(staged_path), staged_path

    def _resolve_predict_chunk_pages(self, file_type: str, total_pages: int) -> int:
        """决定是否启用长 PDF 的分块策略（避免内存爆炸）。"""
        if str(file_type or "").strip().lower().lstrip(".") != "pdf" or total_pages <= 1: return 0
        configured = int(getattr(settings, "OCR_PREDICT_CHUNK_PAGES", 0) or 0)
        return configured if 0 < configured < total_pages else 0

    def _build_pdf_chunk_inputs(self, file_path: str, *, total_pages: int, chunk_pages: int) -> tuple[list[dict[str, Any]], Path]:
        """使用 PyMuPDF 对超长 PDF 进行物理切割，生成一系列小的 PDF 文件提供给流水线。"""
        import fitz
        staging_dir = Path(settings.OCR_RUNTIME_TEMP_DIR) / "input-staging" / f"ocr-chunks-{uuid.uuid4().hex}"
        staging_dir.mkdir(parents=True, exist_ok=True)
        document = fitz.open(file_path)
        inputs = []
        try:
            actual_pages = len(document)
            if actual_pages > 0: total_pages = actual_pages
            for start_page in range(0, max(total_pages, 0), chunk_pages):
                end_page = min(start_page + chunk_pages, total_pages)
                chunk_path = staging_dir / f"chunk_{start_page + 1:05d}_{end_page:05d}.pdf"
                chunk_doc = fitz.open()
                try:
                    chunk_doc.insert_pdf(document, from_page=start_page, to_page=end_page - 1)
                    chunk_doc.save(chunk_path)
                finally: chunk_doc.close()
                inputs.append({"input_path": str(chunk_path), "page_offset": start_page, "page_count": end_page - start_page, "cleanup_path": None, "page_range": (start_page + 1, end_page)})
        finally: document.close()
        if not inputs: raise RuntimeError("Failed to build OCR PDF chunks.")
        return inputs, staging_dir

    def _build_pipeline_inputs(self, file_path: str, file_type: str, total_pages: int) -> tuple[list[dict[str, Any]], Path | None]:
        """总控分发：决定是整卷读取还是切块读取。"""
        chunk_pages = self._resolve_predict_chunk_pages(file_type, total_pages)
        if chunk_pages <= 0:
            input_path, staged_path = self._stage_input_for_pipeline(file_path)
            return [{"input_path": input_path, "page_offset": 0, "page_count": total_pages, "cleanup_path": staged_path, "page_range": (1, total_pages if total_pages > 0 else 1)}], staged_path
        return self._build_pdf_chunk_inputs(file_path, total_pages=total_pages, chunk_pages=chunk_pages)

    def _run_pipeline(
        self,
        input_path: str,
        *,
        progress_monitor: OCRProgressMonitor | None = None,
        total_pages: int = 0,
        progress_page_offset: int = 0,
        progress_total_pages: int | None = None,
        cancel_check: Callable[[], None] | None = None,
    ) -> list[Any]:
        """实际触发 Paddle OCR 模型推理的方法。"""
        if not self.available or self.pipeline is None: raise RuntimeError("PaddleOCR-VL-1.5 is unavailable.")
        if cancel_check is not None:
            cancel_check()
        with self._predictor_lock:
            if progress_monitor is not None: progress_monitor.update(stage="predict", current=max(progress_page_offset, 0), total=max(int(progress_total_pages or total_pages or 1), 1), detail="starting pipeline.predict_iter", emit=False)
            results = []
            for index, item in enumerate(self.pipeline.predict_iter(input_path), start=1):
                if cancel_check is not None:
                    cancel_check()
                results.append(item)
                if progress_monitor is not None:
                    current_page = max(progress_page_offset + index, 0)
                    progress_monitor.update(stage="predict", current=current_page, total=max(int(progress_total_pages or total_pages or 1), current_page, 1), detail=f"page/batch {current_page} predicted", emit=True)
            if cancel_check is not None:
                cancel_check()
            return results

    def _restructure_pipeline_results(self, results: list[Any], *, progress_monitor: OCRProgressMonitor | None = None) -> list[Any]:
        """模型自带的页面级表格、版面重组阶段。"""
        if not settings.PADDLE_VL_RESTRUCTURE_PAGES or len(results) <= 1: return results
        if progress_monitor is not None: progress_monitor.update(stage="restructure", current=0, total=1, detail="restructure pages", emit=False)
        results = list(self.pipeline.restructure_pages(results, merge_tables=True, relevel_titles=True, concatenate_pages=False))
        if progress_monitor is not None: progress_monitor.update(stage="restructure", current=1, total=1, detail="restructure completed", emit=False)
        return results
