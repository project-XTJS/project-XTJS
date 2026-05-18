# -*- coding: utf-8 -*-
from collections import defaultdict
from typing import Any
import os
from app.config.settings import settings
from app.service.table_parser import build_logical_tables, build_table_structure

class OCRLayoutMixin:
    """
    文档版面与表格混入类。
    处理复杂版面的降维解析、HTML 表格结构提取，以及跨页的页眉页脚去重干扰逻辑。
    """

    def _build_table_section(self, page_no: int, block: dict[str, Any]) -> dict[str, Any] | None:
        """为具备 HTML 表格形式的版面块建立逻辑结构包裹。"""
        raw_text = str(block.get("text") or "")
        normalized_raw_text = self._normalize_section_text(raw_text, preserve_lines=True)
        if len(normalized_raw_text) < 2: return None
        html_parts = [raw_text] if "<table" in raw_text.lower() else []
        section = {"page": page_no, "type": "table", "text": normalized_raw_text}
        bbox = self._normalize_bbox(block.get("bbox"))
        if bbox is not None: section["bbox"] = bbox
        if normalized_raw_text: section["raw_text"] = normalized_raw_text
        if html_parts: section["html"] = "\n\n".join(html_parts)
        
        native_table = self._extract_native_table_payload(block, page_no)
        if native_table: section["native_table"] = native_table
        
        # 委托给外部表格解析工具重建多维结构
        table_structure = build_table_structure(html_parts=html_parts, raw_text=normalized_raw_text)
        if table_structure is not None: section["table_structure"] = table_structure
        return section

    def _extract_native_table_payload(self, block: dict[str, Any], page_no: int) -> dict[str, Any] | None:
        raw_payload = self._to_builtin(block.get("_raw"))
        native_payload = dict(raw_payload) if isinstance(raw_payload, dict) else {}
        if page_no > 0: native_payload["page"] = page_no
        bbox = self._normalize_bbox(native_payload.get("block_bbox") or native_payload.get("bbox") or native_payload.get("box") or block.get("bbox"))
        if bbox is not None and not any(key in native_payload for key in ("block_bbox", "bbox", "box")): native_payload["bbox"] = bbox
        return native_payload if native_payload else None

    def _collect_native_tables(self, layout_sections: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [dict(s.get("native_table")) for s in layout_sections if isinstance(s, dict) and str(s.get("type") or "").strip().lower() == "table" and isinstance(s.get("native_table"), dict)]

    def _extract_layout_blocks(self, page_payload: dict[str, Any], page_no: int) -> list[dict[str, Any]]:
        """初筛当页的解析结果，获取基础布局块。"""
        blocks = []
        for i, item in enumerate(page_payload.get("parsing_res_list") or []):
            built_item = self._to_builtin(item)
            if not isinstance(built_item, dict): continue
            label = str(built_item.get("block_label") or built_item.get("label") or built_item.get("type") or "text").strip()
            order = int(built_item.get("block_order")) if built_item.get("block_order") is not None else i + 1
            blocks.append({"page": page_no, "label": label, "type": self._normalize_layout_type(label), "text": self._extract_text_value(built_item), "bbox": self._normalize_bbox(built_item.get("block_bbox") or built_item.get("bbox") or built_item.get("box")), "_order": order, "_raw": built_item})
        return blocks

    def _simplify_layout_sections(self, layout_blocks: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """将版面块进一步聚合，剔除尺寸不足和高度重叠的部分。"""
        if not layout_blocks: return []
        sorted_blocks = sorted(layout_blocks, key=lambda i: (int(i.get("page", 0) or 0), self._bbox_anchor(i.get("bbox"))[1], self._bbox_anchor(i.get("bbox"))[0], int(i.get("_order", 0) or 0)))
        sections, seen = [], set()
        for block in sorted_blocks:
            section_type, page_no = str(block.get("type") or "text"), int(block.get("page", 0) or 0)
            if section_type not in {"heading", "text", "table", "seal", "signature"} or page_no <= 0: continue
            if section_type == "table":
                section = self._build_table_section(page_no, block)
                if section is None: continue
            else:
                section_text = self._normalize_section_text(block.get("text") or "")
                if section_type == "signature":
                    if not section_text and self._normalize_bbox(block.get("bbox")) is None: continue
                elif len(section_text) < 2: continue
                section = {"page": page_no, "type": section_type, "text": section_text}
                bbox = self._normalize_bbox(block.get("bbox"))
                if bbox is not None: section["bbox"] = bbox
                
            sig = (page_no, section["type"], section["text"], str(section.get("bbox")))
            if sig not in seen:
                seen.add(sig)
                sections.append(section)
        return sections

    def _include_section_in_page_text(self, section: dict[str, Any]) -> bool:
        """控制哪些类型的区段可以最终输出为全量正文。"""
        return not section.get("_merged") and str(section.get("type") or "") in {"heading", "text", "table", "seal", "signature"}

    def _normalize_running_header_signature(self, text: Any) -> str:
        """过滤所有无关符号，只保留汉字与字母数字作为页眉页脚对比指纹。"""
        return __import__('re').sub(r"[^\u4e00-\u9fffA-Za-z0-9]", "", __import__('re').sub(r"\s+", "", self._normalize_section_text(text)))

    def _is_running_header_candidate(self, section: dict[str, Any], page_extents: dict[int, int]) -> bool:
        """判断候选段落是否身居页眉区（页面高度的顶部 18% 以内），并排除常规标题样式。"""
        if str(section.get("type") or "") != "heading": return False
        text = self._normalize_section_text(section.get("text") or "")
        if not text or self.RUNNING_HEADER_HEADING_RE.match(text): return False
        signature = self._normalize_running_header_signature(text)
        if not signature or len(signature) > 30: return False
        bbox = self._bbox_to_xywh(section.get("bbox"))
        if bbox is not None and (extent := page_extents.get(int(section.get("page", 0) or 0))):
            if bbox[1] / max(extent, 1) <= 0.18: return True
        return "招标文件" in text or "投标文件" in text

    def _strip_running_headers(self, sections: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """全文统计出现次数 >= 3 的疑似页眉，将其屏蔽。"""
        if not sections: return []
        page_extents = {}
        for section in sections:
            page_no, bbox = int(section.get("page", 0) or 0), self._bbox_to_xywh(section.get("bbox"))
            if page_no > 0 and bbox is not None: page_extents[page_no] = max(page_extents.get(page_no, 0), bbox[1] + bbox[3])
        candidate_pages = defaultdict(set)
        for section in sections:
            if self._is_running_header_candidate(section, page_extents):
                signature = self._normalize_running_header_signature(section.get("text") or "")
                if signature and int(section.get("page", 0) or 0) > 0: candidate_pages[signature].add(int(section.get("page", 0) or 0))
        repeated_signatures = {sig for sig, pgs in candidate_pages.items() if len(pgs) >= 3}
        return [s for s in sections if not (self._normalize_running_header_signature(s.get("text") or "") in repeated_signatures and self._is_running_header_candidate(s, page_extents))] if repeated_signatures else sections

    def _rebuild_pages_from_sections(self, sections: list[dict[str, Any]], page_numbers: list[int]) -> list[dict[str, Any]]:
        by_page = defaultdict(list)
        for s in sections:
            if int(s.get("page", 0) or 0) > 0: by_page[int(s.get("page", 0) or 0)].append(s)
        return [{"page": pn, "text": self._merge_text_parts([str(s.get("text") or "") for s in by_page.get(pn, []) if self._include_section_in_page_text(s)], join_char="\n")} for pn in page_numbers]

    def _extract_page_seals(self, page_payload: dict[str, Any], page_no: int) -> dict[str, Any]:
        seal_info = {"count": 0, "texts": [], "locations": []}
        for item in page_payload.get("parsing_res_list") or []:
            built_item = self._to_builtin(item)
            if not isinstance(built_item, dict) or "seal" not in str(built_item.get("block_label") or built_item.get("label") or built_item.get("type") or "").strip().lower(): continue
            text = self._normalize_section_text(built_item.get("block_content") or built_item.get("content") or "")
            bbox = self._bbox_to_xywh(built_item.get("block_bbox") or built_item.get("bbox") or built_item.get("box"))
            seal_info["count"] += 1
            if text: seal_info["texts"].append(text)
            if bbox is not None: seal_info["locations"].append({"page": page_no, "box": bbox})
        seal_info["texts"] = self._dedupe_text_parts(seal_info["texts"])
        return seal_info

    def _extract_page_signatures(self, page_blocks: list[dict[str, Any]], page_sections: list[dict[str, Any]], page_no: int) -> dict[str, Any]:
        info, seen = {"count": 0, "texts": [], "locations": []}, set()
        for items in (page_sections, page_blocks):
            for i in items:
                if str(i.get("type") or "").strip().lower() != "signature": continue
                bbox_key, text = self._bbox_signature_key(i.get("bbox")), self._normalize_section_text(i.get("text") or "")
                if (bbox_key, text or None) not in seen:
                    seen.add((bbox_key, text or None))
                    info["count"] += 1
                    bbox = self._bbox_to_xywh(i.get("bbox"))
                    if text: info["texts"].append(text)
                    if bbox is not None: info["locations"].append({"page": page_no, "box": bbox})
        info["texts"] = self._dedupe_text_parts(info["texts"])
        return info

    def _extract_page_text(self, page_sections: list[dict[str, Any]], page_payload: dict[str, Any]) -> str:
        """优先使用排版区段合并出当页文本；如果区段为空，则直接使用最底层的识别块兜底。"""
        if section_text := self._merge_text_parts([str(s.get("text") or "") for s in page_sections if self._include_section_in_page_text(s)], join_char="\n"): return section_text
        fallback_parts = []
        for item in page_payload.get("parsing_res_list") or []:
            built_item = self._to_builtin(item)
            if not isinstance(built_item, dict) or str(built_item.get("block_label") or built_item.get("label") or built_item.get("type") or "").strip().lower() in {"image", "chart"}: continue
            if candidate := self._normalize_section_text(built_item.get("block_content") or built_item.get("content") or "", preserve_lines=True): fallback_parts.append(candidate)
        return self._merge_text_parts(fallback_parts, join_char="\n")

    def _attach_table_outputs(self, result: dict[str, Any] | None) -> dict[str, Any]:
        payload = dict(result or {})
        sections = payload.get("layout_sections") or []
        if not isinstance(sections, list):
            payload.update({"logical_tables": [], "native_tables": []})
            return payload
        payload.update({"native_tables": self._collect_native_tables(sections), "logical_tables": build_logical_tables(sections)})
        return payload

    def _resolve_postprocess_workers(self, total_pages: int) -> int:
        if total_pages <= 1: return 1
        return max(1, min(int(getattr(settings, "OCR_POSTPROCESS_MAX_WORKERS", 0) or min(4, os.cpu_count() or 1)), total_pages))