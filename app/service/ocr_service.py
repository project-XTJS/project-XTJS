# -*- coding: utf-8 -*-
import threading
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable

from app.config.settings import settings
from app.service.ocr_progress import OCRProgressMonitor
# 注意这里引入的包路径改为了 app.service.ocr
from app.service.ocr.ocr_utils_mixin import OCRUtilsMixin
from app.service.ocr.ocr_engine_mixin import OCREngineMixin
from app.service.ocr.ocr_signature_mixin import OCRSignatureMixin
from app.service.ocr.ocr_layout_mixin import OCRLayoutMixin

class OCRService(OCREngineMixin, OCRSignatureMixin, OCRLayoutMixin, OCRUtilsMixin):
    """
    OCR 服务主调度入口。
    通过多重继承 Mixin，整合底层引擎的 PaddleOCR-VL 调用、多线程预测、以及页面数据后处理。
    """
    def __init__(self, preferred_device: str | None = None):
        self.available = False
        self.pipeline = None
        self.preferred_device = str(preferred_device or "").strip() or None
        self.active_device = "cpu"
        self._predictor_lock = threading.Lock()
        
        # 引擎相关的静态初始化工作交给主线程池执行，确保后期的 run_in_threadpool 能回到同一上下文。
        self._engine_thread_id: int | None = None
        self._engine_executor = ThreadPoolExecutor(
            max_workers=1,
            thread_name_prefix="ocr-engine-" + (self.preferred_device or "cpu").replace(":", "-").replace("/", "-"),
        )
        self._prepare_runtime_dirs()
        self._prepare_runtime_env()
        self._run_on_engine_thread(self._init_engine)

    def _postprocess_page_payload(
        self,
        page_payload: dict[str, Any],
        fallback_page_no: int,
        pdf_page_sizes: dict[int, tuple[float, float]] | None,
        *,
        cancel_check: Callable[[], None] | None = None,
    ) -> dict[str, Any]:
        """后处理核心单页流水线"""
        if cancel_check is not None:
            cancel_check()
        page_no = self._page_number_from_payload(page_payload, fallback_page_no)
        coordinate_context = self._page_coordinate_context(page_payload, page_no, pdf_page_sizes)
        page_blocks = self._extract_layout_blocks(page_payload, page_no)
        page_sections = self._simplify_layout_sections(page_blocks)
        page_sections = self._enrich_page_signature_sections(page_sections, page_blocks, coordinate_context.get("ocr_image_size"))
        
        return {
            "fallback_page_no": fallback_page_no,
            "page_no": page_no,
            "page_text": self._extract_page_text(page_sections, page_payload),
            "page_sections": [self._project_section_for_output(s, coordinate_context) for s in page_sections],
            "page_seals": self._project_detection_info_for_output(self._extract_page_seals(page_payload, page_no), coordinate_context),
            "page_signatures": self._project_detection_info_for_output(self._extract_page_signatures(page_blocks, page_sections, page_no), coordinate_context),
            "coordinate_context": coordinate_context,
        }

    def extract_all(
        self,
        file_path: str,
        file_type: str = "pdf",
        *,
        cancel_check: Callable[[], None] | None = None,
    ) -> dict[str, Any]:
        """
        全量提取接口。包含文档分发拆块 -> GPU/CPU推理 -> 后处理提取表格/印章/签名/文本
        """
        if not self._is_engine_thread():
            return self._run_on_engine_thread(
                self.extract_all,
                file_path,
                file_type,
                cancel_check=cancel_check,
            )

        if cancel_check is not None:
            cancel_check()
        total_pages = self._estimate_total_pages(file_path, file_type)
        print(f"OCRService: OCR inference started ({self._describe_document(file_path, file_type, total_pages)}, device={self.active_device})", flush=True)
        
        progress_monitor = self._build_progress_monitor(file_path=file_path, file_type=file_type, total_pages=total_pages)
        progress_monitor.start()
        
        pipeline_inputs, staged_path = self._build_pipeline_inputs(file_path, file_type, total_pages)
        pdf_page_sizes = self._load_pdf_page_sizes(file_path, file_type)
        
        if len(pipeline_inputs) > 1:
            print(f"OCRService: predict chunking enabled (chunks={len(pipeline_inputs)}, chunk_pages={max((int(i.get('page_count', 0) or 0) for i in pipeline_inputs), default=0)}, total_pages={total_pages})", flush=True)

        try:
            # 第一阶段：推理（预测）
            raw_results, chunked_predict = [], len(pipeline_inputs) > 1
            for idx, p_input in enumerate(pipeline_inputs, start=1):
                if cancel_check is not None:
                    cancel_check()
                page_offset, page_count = int(p_input.get("page_offset", 0) or 0), int(p_input.get("page_count", 0) or 0)
                if progress_monitor and chunked_predict:
                    progress_monitor.update(stage="predict", current=max(page_offset, 0), total=max(total_pages, 1), detail=f"predict chunk {idx}/{len(pipeline_inputs)}", emit=True)
                raw_results.extend(
                    self._run_pipeline(
                        str(p_input["input_path"]),
                        progress_monitor=progress_monitor,
                        total_pages=page_count,
                        progress_page_offset=page_offset,
                        progress_total_pages=total_pages,
                        cancel_check=cancel_check,
                    )
                )
            
            # 第二阶段：版面重组
            if cancel_check is not None:
                cancel_check()
            results = self._restructure_pipeline_results(raw_results, progress_monitor=progress_monitor)
            if not results: raise RuntimeError("PaddleOCR-VL-1.5 returned no results.")
            total_res_pages = max(total_pages, len(results))
            
            # 第三阶段：并发执行页面级清洗和后处理（文本、印章提取）
            progress_monitor.update(stage="postprocess", current=0, total=max(total_res_pages, 1), detail="normalizing OCR results", emit=True)
            page_payloads = []
            for fpn, result in enumerate(results, start=1):
                payload = self._to_builtin(result)
                if isinstance(payload, dict):
                    if chunked_predict: payload["_xtjs_prefer_fallback_page_no"] = True
                    page_payloads.append((fpn, payload))

            processed_pages = []
            if (worker_count := self._resolve_postprocess_workers(len(page_payloads))) > 1:
                with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="ocr-postprocess") as executor:
                    futures = {
                        executor.submit(
                            self._postprocess_page_payload,
                            p,
                            fpn,
                            pdf_page_sizes,
                            cancel_check=cancel_check,
                        ): fpn
                        for fpn, p in page_payloads
                    }
                    for count, future in enumerate(as_completed(futures), start=1):
                        if cancel_check is not None:
                            cancel_check()
                        res = future.result()
                        processed_pages.append(res)
                        progress_monitor.update(stage="postprocess", current=count, total=max(total_res_pages, count, 1), detail=f"parsed page {res['page_no']}", emit=True)
            else:
                for count, (fpn, p) in enumerate(page_payloads, start=1):
                    if cancel_check is not None:
                        cancel_check()
                    res = self._postprocess_page_payload(
                        p,
                        fpn,
                        pdf_page_sizes,
                        cancel_check=cancel_check,
                    )
                    processed_pages.append(res)
                    progress_monitor.update(stage="postprocess", current=count, total=max(total_res_pages, count, 1), detail=f"parsed page {res['page_no']}", emit=True)

            processed_pages.sort(key=lambda i: (int(i.get("page_no", 0) or 0), int(i.get("fallback_page_no", 0) or 0)))
            
            pages, layout_sections, seals, sigs = [], [], {"count": 0, "texts": [], "locations": []}, {"count": 0, "texts": [], "locations": []}
            for item in processed_pages:
                pages.append({"page": item["page_no"], "text": item["page_text"]})
                layout_sections.extend(item["page_sections"])
                seals["count"] += int(item["page_seals"].get("count", 0) or 0)
                seals["texts"].extend(item["page_seals"]["texts"])
                seals["locations"].extend(item["page_seals"]["locations"])
                sigs["count"] += int(item["page_signatures"].get("count", 0) or 0)
                sigs["texts"].extend(item["page_signatures"]["texts"])
                sigs["locations"].extend(item["page_signatures"]["locations"])

            # 执行跨页关联的页眉拦截过滤
            layout_sections = self._strip_running_headers(layout_sections)
            pages = self._rebuild_pages_from_sections(layout_sections, [int(p.get("page", 0) or 0) for p in pages if int(p.get("page", 0) or 0) > 0])
            
            payload = {
                "text": self._merge_text_parts([str(p.get("text") or "") for p in pages], join_char="\n"),
                "pages": pages,
                "seals": {"count": seals["count"], "texts": self._dedupe_text_parts(seals["texts"]), "locations": seals["locations"]},
                "signatures": {"count": sigs["count"], "texts": self._dedupe_text_parts(sigs["texts"]), "locations": sigs["locations"]},
                "layout_sections": layout_sections,
                "bbox_coordinate_space": "pdf" if pdf_page_sizes else "ocr_image",
                "bbox_source_coordinate_space": "ocr_image",
                "ocr_applied": True,
                "structure_used": bool(layout_sections),
                "structure_enabled": bool(settings.PADDLE_VL_USE_LAYOUT_DETECTION),
                "seal_recognition_enabled": bool(settings.PADDLE_VL_USE_SEAL_RECOGNITION),
                "engine": "PaddleOCR-VL-1.5",
            }

            # 最终阶段：表格解析挂载
            if cancel_check is not None:
                cancel_check()
            progress_monitor.update(stage="tables", current=0, total=1, detail="building logical tables", emit=False)
            payload = self._attach_table_outputs(payload)
            progress_monitor.update(stage="tables", current=1, total=1, detail="logical tables ready", emit=False)
            progress_monitor.finish(success=True)
            return payload
            
        except Exception as exc:
            progress_summary = progress_monitor.finish(success=False, error_message=str(exc))
            if isinstance(exc, RuntimeError) and str(exc): raise
            raise RuntimeError(f"OCR failed after {progress_summary.get('total_elapsed_seconds', 0)}s: {exc}") from exc
        finally:
            if staged_path is not None:
                try: shutil.rmtree(staged_path, ignore_errors=True) if staged_path.is_dir() else staged_path.unlink(missing_ok=True)
                except Exception: pass
