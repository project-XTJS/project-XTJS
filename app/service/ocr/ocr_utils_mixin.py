# -*- coding: utf-8 -*-
import html
import re
from typing import Any

class OCRUtilsMixin:
    """
    OCR 基础工具混入类。
    提供数据类型转换、文本清洗去重、边界框（BBox）计算与坐标系映射等纯函数。
    """

    def _to_builtin(self, value: Any) -> Any:
        """递归地将 Paddle 模型返回的特殊对象（如 Tensor、自定义包装类）转换为 Python 内置的基础类型。"""
        if value is None or isinstance(value, (str, int, float, bool)):
            return value
        
        # 处理带有 json 属性的自定义对象
        json_value = getattr(value, "json", None)
        if json_value is not None:
            try:
                payload = value.json
            except Exception:
                payload = None
            if isinstance(payload, dict) and "res" in payload:
                return self._to_builtin(payload["res"])
        
        # 递归处理字典、列表等容器
        if isinstance(value, dict):
            return {key: self._to_builtin(item) for key, item in value.items()}
        if isinstance(value, (list, tuple, set)):
            return [self._to_builtin(item) for item in value]
        
        # 处理 numpy 数组或 paddle tensor
        if hasattr(value, "tolist"):
            try:
                return value.tolist()
            except Exception:
                pass
        if hasattr(value, "item"):
            try:
                return value.item()
            except Exception:
                pass
        return value

    def _dedupe_text_parts(self, parts: list[str]) -> list[str]:
        """对文本片段列表进行去重，忽略多余的空白字符差异。"""
        deduped: list[str] = []
        seen = set()
        for item in parts:
            normalized = str(item or "").strip()
            if not normalized:
                continue
            # 将连续空白替换为单个空格作为去重指纹
            key = re.sub(r"\s+", " ", normalized)
            if key not in seen:
                seen.add(key)
                deduped.append(normalized)
        return deduped

    def _merge_text_parts(self, parts: list[str], *, join_char: str = "\n") -> str:
        """合并文本片段列表，并在合并前进行去重。"""
        filtered = self._dedupe_text_parts(parts)
        return join_char.join(filtered).strip()

    def _normalize_section_text(self, text: Any, *, preserve_lines: bool = False) -> str:
        """
        清洗 OCR 提取的文本：反转义 HTML 实体，归一化空白字符。
        :param preserve_lines: 若为 True，则尽量保留文本原本的换行和表格结构的制表符。
        """
        normalized = html.unescape(str(text or ""))
        if preserve_lines:
            # 统一换行符
            normalized = re.sub(r"\r\n?", "\n", normalized)
            # 将 HTML 换行和块级元素转换为换行
            normalized = re.sub(r"<br\s*/?>", "\n", normalized, flags=re.IGNORECASE)
            normalized = re.sub(
                r"</?(table|thead|tbody|tfoot|tr|p|div|section|article)[^>]*>",
                "\n",
                normalized,
                flags=re.IGNORECASE,
            )
            # 将表格单元格转换为制表符
            normalized = re.sub(r"</?(td|th)[^>]*>", "\t", normalized, flags=re.IGNORECASE)
            # 移除残余的 HTML 标签
            normalized = re.sub(r"<[^>]+>", " ", normalized)
            
            # 清理行内多余空白，保留换行和制表符
            normalized = re.sub(r"[^\S\n\t]+", " ", normalized)
            normalized = re.sub(r" *\t *", "\t", normalized)
            normalized = re.sub(r"[ \t]*\n[ \t]*", "\n", normalized)
            normalized = re.sub(r"\n{3,}", "\n\n", normalized)
            
            # 逐行二次清理
            lines: list[str] = []
            for line in normalized.splitlines():
                cells = [re.sub(r" {2,}", " ", cell).strip() for cell in line.split("\t")]
                cleaned = "\t".join(cells).strip()
                if cleaned:
                    lines.append(cleaned)
            return "\n".join(lines)
            
        # 若不需要保留换行，直接暴力清除标签并压缩所有空白
        normalized = re.sub(r"<[^>]+>", " ", normalized)
        normalized = re.sub(r"\s+", " ", normalized).strip()
        return normalized

    def _normalize_bbox(self, value: Any) -> Any:
        """将不规则的边界框对象转为基础类型，空值统一返回 None。"""
        builtin_value = self._to_builtin(value)
        if builtin_value in (None, ""):
            return None
        return builtin_value

    def _xywh_to_bbox(self, bbox: list[int] | None) -> list[int] | None:
        """将 (x, y, w, h) 格式的坐标转换为 (x1, y1, x2, y2) 格式。"""
        if bbox is None or len(bbox) < 4:
            return None
        left, top, width, height = [int(round(float(item))) for item in bbox[:4]]
        return [left, top, left + max(1, width), top + max(1, height)]

    def _clip_xywh_to_page(self, bbox: list[int] | None, page_size: tuple[int, int] | None) -> list[int] | None:
        """将估算的 xywh 坐标严格裁剪到页面的物理尺寸范围内，防止越界。"""
        if bbox is None or len(bbox) < 4:
            return bbox
        if page_size is None:
            return [int(round(float(item))) for item in bbox[:4]]
            
        page_width, page_height = page_size
        if page_width <= 0 or page_height <= 0:
            return [int(round(float(item))) for item in bbox[:4]]
            
        left, top, width, height = [int(round(float(item))) for item in bbox[:4]]
        left = max(0, min(left, page_width - 1))
        top = max(0, min(top, page_height - 1))
        width = max(1, min(width, page_width - left))
        height = max(1, min(height, page_height - top))
        return [left, top, width, height]

    def _page_payload_size(self, page_payload: dict[str, Any]) -> tuple[int, int] | None:
        """从 OCR 模型返回的单页解析结果中提取图像的实际宽高。"""
        try:
            width = int(round(float(page_payload.get("width") or 0)))
            height = int(round(float(page_payload.get("height") or 0)))
        except (TypeError, ValueError):
            return None
        if width <= 0 or height <= 0:
            return None
        return (width, height)

    def _load_pdf_page_sizes(self, file_path: str, file_type: str) -> dict[int, tuple[float, float]]:
        """利用 pypdfium2 读取 PDF 源文件的每页真实物理尺寸（点数），用于后续的坐标映射。"""
        normalized_type = str(file_type or "").strip().lower().lstrip(".")
        if normalized_type != "pdf":
            return {}
        try:
            import pypdfium2 as pdfium
        except Exception:
            return {}
            
        document = pdfium.PdfDocument(file_path)
        page_sizes: dict[int, tuple[float, float]] = {}
        try:
            for page_index in range(len(document)):
                page = document[page_index]
                try:
                    width = float(page.get_width())
                    height = float(page.get_height())
                except Exception:
                    try:
                        width, height = page.get_size()
                        width = float(width)
                        height = float(height)
                    except Exception:
                        continue
                if width > 0 and height > 0:
                    page_sizes[page_index + 1] = (width, height)
                close_page = getattr(page, "close", None)
                if callable(close_page):
                    close_page()
        finally:
            close_document = getattr(document, "close", None)
            if callable(close_document):
                close_document()
        return page_sizes

    def _scale_bbox_to_pdf(self, bbox: Any, ocr_image_size: tuple[int, int] | None, pdf_page_size: tuple[float, float] | None) -> Any:
        """将基于 OCR 渲染图象素坐标的 BBox，等比例缩放回原始 PDF 的坐标空间。"""
        builtin_bbox = self._to_builtin(bbox)
        if builtin_bbox is None or ocr_image_size is None or pdf_page_size is None:
            return self._normalize_bbox(builtin_bbox)
            
        image_width, image_height = ocr_image_size
        pdf_width, pdf_height = pdf_page_size
        if image_width <= 0 or image_height <= 0 or pdf_width <= 0 or pdf_height <= 0:
            return self._normalize_bbox(builtin_bbox)
            
        scale_x = float(pdf_width) / float(image_width)
        scale_y = float(pdf_height) / float(image_height)
        
        # 处理 [x1, y1, x2, y2] 格式
        if isinstance(builtin_bbox, (list, tuple)) and len(builtin_bbox) >= 4 and all(isinstance(item, (int, float)) for item in builtin_bbox[:4]):
            x1, y1, x2, y2 = [float(item) for item in builtin_bbox[:4]]
            return [int(round(x1 * scale_x)), int(round(y1 * scale_y)), int(round(x2 * scale_x)), int(round(y2 * scale_y))]
            
        # 处理 [[x1,y1], [x2,y2], ...] 多边形格式
        if isinstance(builtin_bbox, (list, tuple)) and builtin_bbox and all(isinstance(item, (list, tuple)) and len(item) >= 2 and isinstance(item[0], (int, float)) and isinstance(item[1], (int, float)) for item in builtin_bbox):
            return [[int(round(float(item[0]) * scale_x)), int(round(float(item[1]) * scale_y))] for item in builtin_bbox]
            
        return self._normalize_bbox(builtin_bbox)

    def _scale_xywh_to_pdf(self, bbox: list[int] | None, ocr_image_size: tuple[int, int] | None, pdf_page_size: tuple[float, float] | None) -> list[int] | None:
        """针对 [x, y, width, height] 格式的 PDF 坐标缩放。"""
        if bbox is None or len(bbox) < 4 or ocr_image_size is None or pdf_page_size is None:
            return bbox
        image_width, image_height = ocr_image_size
        pdf_width, pdf_height = pdf_page_size
        if image_width <= 0 or image_height <= 0 or pdf_width <= 0 or pdf_height <= 0:
            return bbox
        scale_x = float(pdf_width) / float(image_width)
        scale_y = float(pdf_height) / float(image_height)
        left, top, width, height = [float(item) for item in bbox[:4]]
        return [int(round(left * scale_x)), int(round(top * scale_y)), max(1, int(round(width * scale_x))), max(1, int(round(height * scale_y)))]

    def _page_coordinate_context(self, page_payload: dict[str, Any], page_no: int, pdf_page_sizes: dict[int, tuple[float, float]] | None) -> dict[str, Any]:
        """打包当前页的坐标上下文，便于后续将 OCR 坐标统一投影回 PDF 源坐标。"""
        ocr_image_size = self._page_payload_size(page_payload)
        pdf_page_size = (pdf_page_sizes or {}).get(page_no)
        bbox_coordinate_space = "pdf" if ocr_image_size and pdf_page_size else "ocr_image"
        return {"bbox_coordinate_space": bbox_coordinate_space, "ocr_image_size": ocr_image_size, "pdf_page_size": pdf_page_size}

    def _project_section_for_output(self, section: dict[str, Any], coordinate_context: dict[str, Any]) -> dict[str, Any]:
        """将版面区段的坐标投影到最终输出的坐标系中（保留原始 ocr 坐标作备用）。"""
        projected = dict(section)
        bbox = self._normalize_bbox(section.get("bbox"))
        if bbox is None:
            return projected
        projected["bbox_ocr"] = bbox
        projected["bbox"] = self._scale_bbox_to_pdf(bbox, coordinate_context.get("ocr_image_size"), coordinate_context.get("pdf_page_size"))
        return projected

    def _project_detection_info_for_output(self, info: dict[str, Any], coordinate_context: dict[str, Any]) -> dict[str, Any]:
        """将印章/签名检测结果的坐标统一投影。"""
        projected = {"count": int(info.get("count", 0) or 0), "texts": list(info.get("texts") or []), "locations": []}
        for item in info.get("locations") or []:
            if not isinstance(item, dict):
                continue
            projected_item = dict(item)
            box = self._to_builtin(item.get("box"))
            if isinstance(box, list) and len(box) >= 4:
                projected_item["box_ocr"] = [int(round(float(value))) for value in box[:4]]
                projected_item["box"] = self._scale_xywh_to_pdf(projected_item["box_ocr"], coordinate_context.get("ocr_image_size"), coordinate_context.get("pdf_page_size"))
            projected["locations"].append(projected_item)
        projected["texts"] = self._dedupe_text_parts(projected["texts"])
        return projected

    def _bbox_anchor(self, bbox: Any) -> tuple[float, float]:
        """提取包围盒的左上角坐标 (x, y)，常用于对版面块进行从上到下、从左到右的排序。"""
        builtin_bbox = self._to_builtin(bbox)
        if builtin_bbox is None:
            return (1e9, 1e9)
        if isinstance(builtin_bbox, (list, tuple)):
            if len(builtin_bbox) >= 2 and all(isinstance(item, (int, float)) for item in builtin_bbox[:2]):
                return (float(builtin_bbox[0]), float(builtin_bbox[1]))
            if builtin_bbox and all(isinstance(item, (list, tuple)) and len(item) >= 2 and isinstance(item[0], (int, float)) and isinstance(item[1], (int, float)) for item in builtin_bbox):
                x_values = [float(item[0]) for item in builtin_bbox]
                y_values = [float(item[1]) for item in builtin_bbox]
                return (min(x_values), min(y_values))
        return (1e9, 1e9)

    def _bbox_to_xywh(self, bbox: Any) -> list[int] | None:
        """将不规则的 bbox (如点集、两点坐标) 统一转换为标准的 [x, y, width, height] 格式。"""
        builtin_bbox = self._to_builtin(bbox)
        if builtin_bbox is None:
            return None
        if isinstance(builtin_bbox, (list, tuple)) and len(builtin_bbox) >= 4 and all(isinstance(item, (int, float)) for item in builtin_bbox[:4]):
            x1, y1, x2, y2 = [int(round(float(item))) for item in builtin_bbox[:4]]
            return [x1, y1, max(1, x2 - x1), max(1, y2 - y1)]
        if isinstance(builtin_bbox, (list, tuple)) and builtin_bbox and all(isinstance(item, (list, tuple)) and len(item) >= 2 and isinstance(item[0], (int, float)) and isinstance(item[1], (int, float)) for item in builtin_bbox):
            xs = [float(item[0]) for item in builtin_bbox]
            ys = [float(item[1]) for item in builtin_bbox]
            return [int(round(min(xs))), int(round(min(ys))), max(1, int(round(max(xs))) - int(round(min(xs)))), max(1, int(round(max(ys))) - int(round(min(ys))))]
        return None

    def _bbox_signature_key(self, bbox: Any) -> tuple[int, int, int, int] | None:
        """基于 bbox 生成一个可哈希的键值，用于去重计算（例如同位置的重复签名）。"""
        xywh = self._bbox_to_xywh(bbox)
        return tuple(xywh) if xywh is not None else None

    def _normalize_layout_type(self, value: str) -> str:
        """将 PaddleOCR 返回的多种版面标签映射为内部的标准化类型。"""
        normalized = str(value or "").strip().lower()
        if not normalized: return "text"
        if "signature" in normalized: return "signature"
        if "seal" in normalized: return "seal"
        if "table" in normalized: return "table"
        if any(token in normalized for token in ("title", "header", "heading")): return "heading"
        if any(token in normalized for token in ("figure", "image", "chart", "photo")): return "figure"
        return "text"

    def _extract_text_value(self, value: Any) -> str:
        """从复杂的嵌套结果对象中提取并清洗出核心文本内容。"""
        builtin_value = self._to_builtin(value)
        if isinstance(builtin_value, str):
            return self._normalize_section_text(builtin_value, preserve_lines=True)
        if isinstance(builtin_value, list):
            return self._merge_text_parts([self._extract_text_value(item) for item in builtin_value], join_char="\n")
        if isinstance(builtin_value, dict):
            parts: list[str] = []
            for key in ("block_content", "markdown", "text", "html", "content", "caption"):
                if candidate := builtin_value.get(key):
                    parts.append(self._extract_text_value(candidate))
            return self._merge_text_parts(parts, join_char="\n")
        return ""

    def _page_number_from_payload(self, payload: dict[str, Any], fallback_page_no: int) -> int:
        """安全地从解析结果中提取绝对页码。"""
        if bool(payload.get("_xtjs_prefer_fallback_page_no", False)):
            return fallback_page_no
        page_offset = int(payload.get("_xtjs_page_offset", 0) or 0)
        raw_page_index = payload.get("page_index")
        if isinstance(raw_page_index, int):
            return (raw_page_index + 1 + page_offset) if raw_page_index >= 0 else fallback_page_no
        return fallback_page_no