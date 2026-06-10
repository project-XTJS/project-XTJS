# -*- coding: utf-8 -*-
import re
from typing import Any
from app.config.settings import settings

class OCRSignatureMixin:
    """
    签名印章识别混入类。
    处理招标文件中的授权签字、法人代表签字等占位符匹配、周边印章识别，
    以及基于曼哈顿距离的启发式锚点挂载逻辑。
    """
    
    # 标题段落判断，避免误伤页眉
    RUNNING_HEADER_HEADING_RE = re.compile(r"^第[一二三四五六七八九十0-9]+章|^[一二三四五六七八九十]+、|^[（(][一二三四五六七八九十A-Za-z0-9]+[)）]|^\d+\s*[)）.．、]|^(附件|附表|附录)\s*\d+(?:-\d+)?")
    SIGNATURE_ANCHOR_TOKENS = ("签字", "签章", "签名", "手签")
    SIGNATURE_PLACEHOLDER_TOKENS = ("手写签字", "签字", "签名", "手写", "字或盖章", "签字或盖章", "盖章")
    
    # 防止人名识别时误将公司名或日期卷入
    SIGNATURE_CANDIDATE_BLOCKED_WORDS = ("签字", "签章", "签名", "盖章", "授权", "代表", "法定", "日期", "公司", "有限公司", "项目", "投标")
    SIGNATURE_CANDIDATE_BLOCKED_CHARS = set("签章盖日期公司项目设备授权代表投标")
    SIGNATURE_ATTRIBUTE_BLOCKED_TOKENS = ("性别", "年龄", "身份证", "号码", "公司注册号码", "注册号码", "单位类型", "经营范围", "住址", "地址", "电话", "邮编", "职务", "签发日期", "签发机关")
    DEFAULT_SIGNATURE_PLACEHOLDER_TEXT = "已签字"

    def _signature_placeholder_text(self) -> str:
        """获取设置中配置的默认占位符（例如："已签字"）"""
        configured = self._normalize_section_text(getattr(settings, "OCR_SIGNATURE_PLACEHOLDER_TEXT", self.DEFAULT_SIGNATURE_PLACEHOLDER_TEXT))
        return configured or self.DEFAULT_SIGNATURE_PLACEHOLDER_TEXT

    def _normalize_signature_candidate_text(self, text: Any) -> str:
        """评估某段文本是否具备真实人名的特征（长度、无数字、非公司后缀等）。"""
        normalized = self._normalize_section_text(text)
        if not normalized: return ""
        compact = re.sub(r"\s+", "", normalized)
        compact = re.sub(r"[_＿:：()\[\]（）【】\-—.·•]", "", compact)
        if not compact or len(compact) < 2 or len(compact) > 12: return ""
        if any(token in compact for token in self.SIGNATURE_CANDIDATE_BLOCKED_WORDS) or re.search(r"\d", compact): return ""
        cleaned = re.sub(r"[^A-Za-z\u4e00-\u9fff]", "", compact)
        if not cleaned or len(cleaned) < 2 or len(cleaned) > 8 or any(char in self.SIGNATURE_CANDIDATE_BLOCKED_CHARS for char in cleaned): return ""
        if re.fullmatch(r"[\u4e00-\u9fff]{2,4}|[A-Za-z]{2,20}|[\u4e00-\u9fffA-Za-z]{2,6}", cleaned): return cleaned
        return ""

    def _boxes_are_close(self, left_bbox: list[int] | None, right_bbox: list[int] | None, *, max_dx: int = 260, max_dy: int = 120) -> bool:
        """粗略判定两个边界框在视觉上是否毗邻（同宽容度内的同一行，或正下方）。"""
        if left_bbox is None or right_bbox is None: return False
        left_x, left_y, left_w, left_h = left_bbox
        right_x, right_y, right_w, right_h = right_bbox
        left_right, right_right, left_bottom = left_x + left_w, right_x + right_w, left_y + left_h
        same_band = abs(left_y - right_y) <= max(max(left_h, right_h), 28)
        horizontal_overlap = right_right >= left_x - 80 and right_x <= left_right + max_dx
        below = 0 <= right_y - left_bottom <= max_dy and right_right >= left_x - 80 and right_x <= left_right + max_dx
        return (same_band and horizontal_overlap) or below

    def _bbox_distance(self, source_bbox: list[int] | None, target_bbox: list[int] | None) -> int:
        """计算两个包围盒中心点的曼哈顿距离。"""
        if source_bbox is None or target_bbox is None: return 10**9
        source_x, source_y, source_w, source_h = source_bbox
        target_x, target_y, target_w, target_h = target_bbox
        return int(abs((source_x + source_w / 2) - (target_x + target_w / 2)) + abs((source_y + source_h / 2) - (target_y + target_h / 2)))

    def _resolve_signature_section_text(self, signature_section: dict[str, Any], page_blocks: list[dict[str, Any]]) -> str:
        """若检测到签字区块本身为空白，则在周围相邻的 block 中搜寻可能性最高的人名文本。"""
        raw_text = self._normalize_section_text(signature_section.get("text") or "")
        if raw_text and not self._is_signature_placeholder_text(raw_text): return raw_text
        signature_bbox = self._bbox_to_xywh(signature_section.get("bbox"))
        best_text, best_score = "", None
        for block in page_blocks:
            block_type = str(block.get("type") or "").strip().lower()
            if block_type not in {"text", "figure"}: continue
            candidate_text = self._normalize_signature_candidate_text(block.get("text") or "")
            if not candidate_text: continue
            candidate_bbox = self._bbox_to_xywh(block.get("bbox"))
            if signature_bbox is not None and candidate_bbox is not None and not self._boxes_are_close(signature_bbox, candidate_bbox, max_dx=320, max_dy=160): continue
            score = self._bbox_distance(signature_bbox, candidate_bbox) - (16 if block_type == "figure" else 0)
            if best_score is None or score < best_score: best_score, best_text = score, candidate_text
        return best_text or raw_text

    def _merge_signature_into_anchor(self, signature_section: dict[str, Any], page_sections: list[dict[str, Any]]) -> None:
        """将独立游离的签名文本段落合并到附近的“法定代表人”等锚点段落尾部。"""
        signature_text = self._normalize_section_text(signature_section.get("text") or "")
        if not signature_text: return
        signature_bbox = self._bbox_to_xywh(signature_section.get("bbox"))
        signature_page = int(signature_section.get("page", 0) or 0)
        best_anchor, best_score = None, None
        for section in page_sections:
            if section is signature_section or int(section.get("page", 0) or 0) != signature_page or str(section.get("type") or "").strip().lower() not in {"heading", "text"}: continue
            anchor_text = self._normalize_section_text(section.get("text") or "")
            if not self._is_signature_anchor_text(anchor_text): continue
            anchor_bbox = self._bbox_to_xywh(section.get("bbox"))
            if signature_bbox is not None and anchor_bbox is not None and not self._boxes_are_close(anchor_bbox, signature_bbox, max_dx=420, max_dy=180): continue
            score = self._bbox_distance(anchor_bbox, signature_bbox)
            if best_score is None or score < best_score: best_score, best_anchor = score, section
        if best_anchor is None: return
        
        anchor_text = self._normalize_section_text(best_anchor.get("text") or "")
        if re.sub(r"\s+", "", signature_text) in re.sub(r"\s+", "", anchor_text):
            signature_section["_merged"] = True
            return
            
        merged_text = re.sub(r"[_＿]{2,}$", "", anchor_text).rstrip()
        if re.search(r"[：:]\s*$", merged_text): merged_text = f"{merged_text}{signature_text}"
        elif any(token in merged_text for token in self.SIGNATURE_ANCHOR_TOKENS): merged_text = f"{merged_text.rstrip('：: ')}：{signature_text}"
        else: return
        best_anchor["text"], signature_section["_merged"] = merged_text, True

    def _is_signature_placeholder_text(self, text: Any) -> bool:
        normalized = self._normalize_section_text(text)
        if not normalized: return False
        compact = re.sub(r"[：:_＿\-\u2014.·•()\[\]（）【】\s]", "", normalized)
        if compact == re.sub(r"\s+", "", self._signature_placeholder_text()) or compact in self.SIGNATURE_PLACEHOLDER_TOKENS: return True
        return compact.endswith("签字") and len(compact) <= 6

    def _is_signature_attribute_text(self, text: Any) -> bool:
        """身份证明、基础信息行需要排除。"""
        compact = re.sub(r"\s+", "", self._normalize_section_text(text))
        return bool(compact and any(token in compact for token in self.SIGNATURE_ATTRIBUTE_BLOCKED_TOKENS))

    def _is_strong_signature_anchor_text(self, text: Any) -> bool:
        compact = re.sub(r"\s+", "", self._normalize_section_text(text))
        if not compact or self._is_signature_attribute_text(compact): return False
        return any(re.search(pattern, compact) for pattern in (r"(?:法定代表人(?:或(?:其)?(?:委托代理人|授权代理人|授权代表|授权委托人))?|委托代理人|授权代理人|授权代表|授权委托人|被授权人)(?:[（(]?(?:签字或盖章|签章或盖章|签名或盖章|签字|签章|签名|手签)[)）]?)?(?:[:：]|$)", r"持证人签名(?:[:：]|$)"))

    def _is_signature_anchor_text(self, text: Any) -> bool:
        compact = re.sub(r"\s+", "", self._normalize_section_text(text))
        if not compact or self._is_signature_attribute_text(compact): return False
        if self._is_strong_signature_anchor_text(compact): return True
        return bool(re.search(r"(签字或盖章|签章或盖章|签名或盖章)[\)）】】]*([：:]|$)", compact))

    def _is_table_signature_anchor_text(self, text: Any) -> bool:
        return self._is_strong_signature_anchor_text(text)

    def _find_signature_anchor_match(self, text: Any) -> tuple[str, int, int, str, str] | None:
        """正则匹配签字锚点的头尾，抽取前缀与后续内容。"""
        normalized = self._normalize_section_text(text)
        if not normalized: return None
        patterns = (re.compile(r"(?P<prefix>(?:(?:法定代表人(?:或(?:其)?(?:委托代理人|授权代理人|授权代表|授权委托人))?|委托代理人|授权代理人|授权代表|授权委托人|被授权人)(?:[（(]?(?:签字或盖章|签章或盖章|签名或盖章|签字|签章|签名|手签)[)）]?)?|持证人签名)[：:])\s*(?P<value>[^)）\]】>\n]{0,24})"), re.compile(r"(?P<prefix>(?:签字或盖章|签章或盖章|签名或盖章)[\)）】】]*[：:])\s*(?P<value>[^)）\]】>\n]{0,24})"))
        best_match = None
        for pattern in patterns:
            for match in pattern.finditer(normalized): best_match = (normalized, match.start(), match.end(), str(match.group("prefix") or ""), self._normalize_section_text(match.group("value") or ""))
        return best_match

    def _extract_signature_anchor_value(self, text: Any) -> str:
        match = self._find_signature_anchor_match(text)
        return re.sub(r"^[_＿\-\u2014~.·•\s]+|[_＿\-\u2014~.·•\s]+$", "", match[4]) if match else ""

    def _strip_signature_anchor_value(self, text: Any) -> str:
        match = self._find_signature_anchor_match(text)
        normalized = self._normalize_section_text(text)
        return f"{normalized[:match[1]]}{match[3]}" if match and normalized else normalized

    def _build_signature_anchor_text(self, text: Any, signature_value: Any = None) -> str:
        """替换或补齐带占位符的签名锚点字符串。"""
        match = self._find_signature_anchor_match(text)
        placeholder = self._signature_placeholder_text()
        actual_value = self._normalize_section_text(signature_value or "")
        replacement = actual_value if actual_value and not self._is_signature_placeholder_text(actual_value) else placeholder
        if match is None:
            prefix = self._strip_signature_anchor_value(text)
            if not prefix: return replacement
            return self._clean_signature_anchor_text(f"{prefix}{replacement}" if prefix.endswith(("：", ":")) else f"{prefix.rstrip('：: ')}：{replacement}")
        normalized, start, end, prefix, _ = match
        return self._clean_signature_anchor_text(f"{normalized[:start]}{prefix}{replacement}{normalized[end:]}") if prefix else replacement

    def _clean_signature_anchor_text(self, text: Any) -> str:
        normalized = self._normalize_section_text(text)
        if not normalized: return ""
        placeholder = re.escape(self._signature_placeholder_text())
        normalized = normalized.replace("$", "")
        # 处理占位符与“签字/盖章”描述文字的相互融合去重
        normalized = re.sub(rf"({placeholder})\s*([（(]\s*(?:签字或盖章|签章或盖章|签名或盖章|签字|签章|签名)\s*[)）])", r"\1\2", normalized)
        normalized = re.sub(r"([（(]\s*(?:签字或盖章|签章或盖章|签名或盖章|签字|签章|签名)\s*[)）])\s*[A-Za-z\u4e00-\u9fff]{1,4}(?=(?:\s|$|日期|20\d{2}年|\d{4}年))", r"\1", normalized)
        normalized = re.sub(rf"({placeholder})\s*[)）]?\s*[A-Za-z\u4e00-\u9fff]{{1,4}}(?=(?:\s|$|日期|20\d{{2}}年|\d{{4}}年))", r"\1", normalized)
        normalized = re.sub(rf"({placeholder})\s*[)）](?=(?:\s|$|日期|20\d{{2}}年|\d{{4}}年))", r"\1", normalized)
        return re.sub(r"\s{2,}", " ", normalized).strip()

    def _estimate_signature_bbox_from_anchor(self, anchor_bbox: Any, page_image_size: tuple[int, int] | None) -> list[int] | None:
        """从文字锚点盲猜签名的右侧偏下位置。"""
        anchor_xywh = self._bbox_to_xywh(anchor_bbox)
        if anchor_xywh is None: return None
        left, top, width, height = anchor_xywh
        return self._clip_xywh_to_page([left + int(width * 0.56), top - int(height * 0.35), max(132, int(width * 0.26)), max(72, int(height * 1.9))], page_image_size)

    def _estimate_signature_bbox_from_text_anchor(self, anchor_section: dict[str, Any], page_sections: list[dict[str, Any]], page_blocks: list[dict[str, Any]], page_image_size: tuple[int, int] | None) -> list[int] | None:
        """结合文字锚点和附近的印章位置，精准预估签名可能出现的包围盒位置。"""
        anchor_xywh = self._bbox_to_xywh(anchor_section.get("bbox"))
        if anchor_xywh is None: return self._estimate_signature_bbox_from_anchor(anchor_section.get("bbox"), page_image_size)
        nearest_seal_bbox = self._find_nearest_seal_bbox(anchor_xywh, page_sections, page_blocks)
        if nearest_seal_bbox is not None and self._bbox_distance(anchor_xywh, nearest_seal_bbox) <= max(280, anchor_xywh[2] + nearest_seal_bbox[2]):
            estimated_bbox = [max(0, min(anchor_xywh[0] + int(anchor_xywh[2] * 0.34), nearest_seal_bbox[0] - int(nearest_seal_bbox[2] * 0.72))), max(0, min(anchor_xywh[1], nearest_seal_bbox[1]) - int(max(anchor_xywh[3], nearest_seal_bbox[3]) * 0.28)), max(108, int(max(anchor_xywh[2] * 0.20, nearest_seal_bbox[2] * 2.1))), max(72, int(max(anchor_xywh[3], nearest_seal_bbox[3]) * 1.45))]
            # 若印章面积离谱，切回保守估算
            if estimated_bbox[2] > max(int(anchor_xywh[2] * 1.2), 220) or estimated_bbox[3] > max(int(anchor_xywh[3] * 3), 120): return self._estimate_signature_bbox_from_anchor(anchor_section.get("bbox"), page_image_size)
            return self._clip_xywh_to_page(estimated_bbox, page_image_size)
        return self._estimate_signature_bbox_from_anchor(anchor_section.get("bbox"), page_image_size)

    def _find_nearest_seal_bbox(self, reference_bbox: list[int] | None, page_sections: list[dict[str, Any]], page_blocks: list[dict[str, Any]]) -> list[int] | None:
        if reference_bbox is None: return None
        best_bbox, best_score = None, None
        for item in [*page_sections, *page_blocks]:
            if str(item.get("type") or "").strip().lower() != "seal": continue
            bbox = self._bbox_to_xywh(item.get("bbox"))
            if bbox is not None:
                score = self._bbox_distance(reference_bbox, bbox)
                if best_score is None or score < best_score: best_score, best_bbox = score, bbox
        return best_bbox

    def _estimate_signature_bbox_from_table_anchor(self, anchor_section: dict[str, Any], page_sections: list[dict[str, Any]], page_blocks: list[dict[str, Any]], page_image_size: tuple[int, int] | None) -> list[int] | None:
        table_bbox = self._bbox_to_xywh(anchor_section.get("bbox"))
        if table_bbox is None: return None
        left, top, width, height = table_bbox
        nearest_seal_bbox = self._find_nearest_seal_bbox(table_bbox, page_sections, page_blocks)
        if nearest_seal_bbox is not None:
            return self._clip_xywh_to_page([max(left + int(width * 0.46), nearest_seal_bbox[0] + int(nearest_seal_bbox[2] * 0.85)), max(top + int(height * 0.72), nearest_seal_bbox[1] - int(nearest_seal_bbox[3] * 0.18)), max(144, int(width * 0.22)), max(72, int(nearest_seal_bbox[3] * 0.9))], page_image_size)
        return self._clip_xywh_to_page([left + int(width * 0.62), top + int(height * 0.78), max(144, int(width * 0.22)), max(72, int(height * 0.12))], page_image_size)

    def _signature_anchor_reference_bbox(self, anchor_section: dict[str, Any], page_sections: list[dict[str, Any]], page_blocks: list[dict[str, Any]], page_image_size: tuple[int, int] | None) -> list[int] | None:
        if str(anchor_section.get("type") or "").strip().lower() == "table": return self._estimate_signature_bbox_from_table_anchor(anchor_section, page_sections, page_blocks, page_image_size)
        return self._estimate_signature_bbox_from_text_anchor(anchor_section, page_sections, page_blocks, page_image_size) or self._bbox_to_xywh(anchor_section.get("bbox"))

    def _anchor_has_signature_evidence(self, anchor_section: dict[str, Any], page_sections: list[dict[str, Any]], page_blocks: list[dict[str, Any]], page_image_size: tuple[int, int] | None) -> bool:
        """判定目标锚点附近是否存在印章、手写签名的实质性线索。"""
        compact_value = re.sub(r"\s+", "", self._extract_signature_anchor_value(anchor_section.get("text") or ""))
        strong_anchor = self._is_strong_signature_anchor_text(anchor_section.get("text") or "")
        if compact_value and (self._is_signature_placeholder_text(compact_value) and strong_anchor or self._normalize_signature_candidate_text(compact_value)): return True
        anchor_bbox = self._signature_anchor_reference_bbox(anchor_section, page_sections, page_blocks, page_image_size)
        for items in (page_sections, page_blocks):
            for item in items:
                if item is anchor_section: continue
                item_type, item_bbox = str(item.get("type") or "").strip().lower(), self._bbox_to_xywh(item.get("bbox"))
                if item_type == "signature" and anchor_bbox and item_bbox and self._boxes_are_close(anchor_bbox, item_bbox, max_dx=420, max_dy=180): return True
                if strong_anchor and item_type == "seal" and anchor_bbox and item_bbox and self._boxes_are_close(anchor_bbox, item_bbox, max_dx=180, max_dy=180): return True
        return False

    def _dedupe_signature_sections(self, sections: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """同页同坐标的伪合成签字区只保留一份。"""
        result, seen = [], set()
        for section in sections:
            if str(section.get("type") or "").strip().lower() != "signature":
                result.append(section)
                continue
            section_key = (int(section.get("page", 0) or 0), self._bbox_signature_key(section.get("bbox")), bool(section.get("_synthetic")))
            if section_key not in seen:
                seen.add(section_key)
                result.append(section)
        return result

    def _match_signatures_to_anchors(self, signatures: list[dict[str, Any]], anchors: list[dict[str, Any]]) -> list[tuple[int, int]]:
        """计算签字区与文本锚点（如“授权代表”）的距离二分图，获得最佳匹配。"""
        if not signatures or not anchors: return []
        candidate_pairs = []
        for s_idx, signature in enumerate(signatures):
            s_bbox = self._bbox_to_xywh(signature.get("bbox"))
            for a_idx, anchor in enumerate(anchors):
                a_bbox = self._bbox_to_xywh(anchor.get("_signature_anchor_bbox") or anchor.get("bbox"))
                if a_bbox and s_bbox and not self._boxes_are_close(a_bbox, s_bbox, max_dx=420, max_dy=180): continue
                candidate_pairs.append((self._bbox_distance(a_bbox, s_bbox), abs(s_idx - a_idx), a_idx, s_idx))
        matches, matched_s, matched_a = [], set(), set()
        for _, _, a_idx, s_idx in sorted(candidate_pairs):
            if s_idx not in matched_s and a_idx not in matched_a:
                matched_s.add(s_idx)
                matched_a.add(a_idx)
                matches.append((s_idx, a_idx))
        rem_s = [i for i in range(len(signatures)) if i not in matched_s]
        rem_a = [i for i in range(len(anchors)) if i not in matched_a]
        for s_idx, a_idx in zip(rem_s, rem_a): matches.append((s_idx, a_idx))
        return sorted(matches, key=lambda item: item[1])

    def _enrich_page_signature_sections(self, page_sections: list[dict[str, Any]], page_blocks: list[dict[str, Any]], page_image_size: tuple[int, int] | None) -> list[dict[str, Any]]:
        """补全漏掉的签名占位，并在锚点和签名之间建立联系与坐标替换。"""
        if not page_sections and not page_blocks: return page_sections
        sections, placeholder = page_sections, self._signature_placeholder_text()
        existing_keys = {self._bbox_signature_key(s.get("bbox")) for s in sections if str(s.get("type") or "").strip().lower() == "signature"}
        inferred_page_no = next((int(s.get("page", 0) or 0) for s in sections if int(s.get("page", 0) or 0) > 0), 0)
        
        # 1. 扫描原始区块，看有没有漏掉的 signature 类标签
        for block in page_blocks:
            if str(block.get("type") or "").strip().lower() != "signature": continue
            bbox, key = self._normalize_bbox(block.get("bbox")), self._bbox_signature_key(self._normalize_bbox(block.get("bbox")))
            if key not in existing_keys:
                signature_text = self._resolve_signature_section_text(block, page_blocks)
                if not signature_text or self._is_signature_placeholder_text(signature_text): signature_text = placeholder
                synth = {"page": int(block.get("page", 0) or inferred_page_no or 0), "type": "signature", "text": signature_text}
                if bbox is not None: synth["bbox"] = bbox
                sections.append(synth)
                existing_keys.add(key)
                
        sig_sections, anc_sections = [], []
        for s in sections:
            s_type = str(s.get("type") or "").strip().lower()
            if s_type == "signature":
                signature_text = self._resolve_signature_section_text(s, page_blocks)
                s["text"] = placeholder if not signature_text or self._is_signature_placeholder_text(signature_text) else signature_text
                s.pop("_merged", None)
                sig_sections.append(s)
            elif s_type in {"heading", "text", "table"}:
                s["text"] = self._normalize_section_text(s.get("text") or "")
                if self._is_signature_anchor_text(s.get("text") or "") or self._is_table_signature_anchor_text(s.get("text") or ""):
                    s["_signature_anchor_bbox"] = self._xywh_to_bbox(self._signature_anchor_reference_bbox(s, sections, page_blocks, page_image_size))
                    anc_sections.append(s)
                    
        # 2. 对匹配空缺的锚点生成“伪装”合成签名区
        matched_a_idx = {a_idx for _, a_idx in self._match_signatures_to_anchors(sig_sections, anc_sections)}
        for a_idx, a_sec in enumerate(anc_sections):
            if a_idx not in matched_a_idx and self._anchor_has_signature_evidence(a_sec, sections, page_blocks, page_image_size):
                inferred_bbox = self._xywh_to_bbox(self._signature_anchor_reference_bbox(a_sec, sections, page_blocks, page_image_size))
                synth = {"page": int(a_sec.get("page", 0) or inferred_page_no or 0), "type": "signature", "text": placeholder, "_synthetic": True}
                if inferred_bbox is not None: synth["bbox"] = inferred_bbox
                sections.append(synth)
                sig_sections.append(synth)
                
        # 3. 最终应用替换和属性合并
        for s_idx, a_idx in self._match_signatures_to_anchors(sig_sections, anc_sections):
            s_sec, a_sec = sig_sections[s_idx], anc_sections[a_idx]
            signature_value = "" if self._is_signature_placeholder_text(s_sec.get("text")) else s_sec.get("text")
            a_sec["text"] = self._build_signature_anchor_text(a_sec.get("text") or "", signature_value)
            target_bbox, cur_bbox = self._bbox_to_xywh(a_sec.get("_signature_anchor_bbox")), self._bbox_to_xywh(s_sec.get("bbox"))
            if target_bbox is not None and (cur_bbox is None or cur_bbox[2] > max(target_bbox[2] * 2, 220) or cur_bbox[3] > max(target_bbox[3] * 2, 160) or (((page_image_size or (0,0))[0] or 0) > 0 and cur_bbox[2] >= int(((page_image_size or (0,0))[0] or 0) * 0.72))): 
                s_sec["bbox"] = self._xywh_to_bbox(target_bbox)
            s_sec["_merged"] = True
            
            # 若锚点位于表格内，一并更新 html 和内部内容缓存
            if str(a_sec.get("type") or "").strip().lower() == "table":
                for k in ("raw_text", "html"):
                    if k in a_sec: a_sec[k] = self._build_signature_anchor_text(a_sec.get(k) or "", signature_value)
                if isinstance(a_sec.get("native_table"), dict):
                    for nk in ("block_content", "content", "text", "html"):
                        if nk in a_sec["native_table"]: a_sec["native_table"][nk] = self._build_signature_anchor_text(a_sec["native_table"].get(nk) or "", signature_value)
                        
        # 返回前按页面和物理坐标从上到下重新排序
        sections.sort(key=lambda item: (int(item.get("page", 0) or 0), self._bbox_anchor(item.get("bbox"))[1], self._bbox_anchor(item.get("bbox"))[0], int(item.get("_order", 0) or 0)))
        return self._dedupe_signature_sections(sections)
