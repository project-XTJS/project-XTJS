import hashlib
import re
import html
import json
import os
import random
import string
from collections import Counter

from app.core.document_types import (
    DOCUMENT_TYPE_BUSINESS_BID,
    DOCUMENT_TYPE_TECHNICAL_BID,
)

class ReportVisualizer:
    """可视化工具：生成基于投标人(投标文件)正文内容的合规报告"""

    def __init__(self):
        self.CONTENT_PATTERN = re.compile(r'[\u4e00-\u9fa5a-zA-Z0-9]+')
        self._document_preview_config = None

        self.CSS_STYLE = """
        <style>
            html { scroll-behavior: smooth; } 
            :root { 
                --bg-color: #f5f7fa;
                --card-bg: #ffffff;
                --text-main: #303133; 
                --text-regular: #606266;
                --text-light: #909399; 
                --border-color: #ebeef5; 
                --primary-color: #409eff; 
                --success-color: #67c23a;
                --danger-color: #f56c6c;
                --warning-color: #e6a23c;
                --warning-light: #fdf6ec;
                --warning-border: #faecd8;
                --danger-light: #fef0f0;
                --danger-border: #fde2e2;
                --missing-bg: #f4f4f5;
                --missing-border: #dcdfe6;
            }
            
            body { font-family: "Helvetica Neue", Arial, sans-serif; margin: 0; padding: 30px 20px; color: var(--text-main); font-size: 14px; background: var(--bg-color); line-height: 1.6; }
            .container { max-width: 1200px; margin: 0 auto; }
            
            .file-switcher {
                position: fixed;
                top: 40%;
                left: -230px;               /* 默认隐藏在左侧外 */
                transform: translateY(-50%);
                z-index: 10000;
                display: flex;
                flex-direction: column;
                width: 260px;
                background: rgba(255, 255, 255, 0.95);
                backdrop-filter: blur(8px);
                border: 1px solid var(--border-color);
                border-left: none;
                border-radius: 0 12px 12px 0;
                box-shadow: 4px 0 20px rgba(0,0,0,0.1);
                transition: left 0.3s cubic-bezier(0.2, 0.9, 0.4, 1);
                padding: 16px 0;
            }
            .file-switcher:hover {
                left: 0;                     /* 鼠标悬停时滑出 */
            }
            .switcher-header {
                padding: 8px 20px 12px;
                font-weight: 600;
                color: var(--text-main);
                border-bottom: 1px solid var(--border-color);
                margin-bottom: 8px;
                display: flex;
                align-items: center;
                gap: 6px;
            }
            .switcher-header .icon {
                font-size: 16px;
            }
            .switcher-list {
                list-style: none;
                margin: 0;
                padding: 0;
            }
            .switcher-item {
                padding: 10px 20px;
                display: flex;
                align-items: center;
                gap: 10px;
                color: var(--text-regular);
                text-decoration: none;
                transition: background 0.2s;
                border-left: 3px solid transparent;
            }
            .switcher-item:hover {
                background: #ecf5ff;
                color: var(--primary-color);
                border-left-color: var(--primary-color);
            }
            .switcher-item.active {
                background: #e6f1fc;
                color: var(--primary-color);
                font-weight: 500;
                border-left-color: var(--primary-color);
            }
            .switcher-item .file-badge {
                margin-left: auto;
                font-size: 11px;
                background: #e0e0e0;
                padding: 2px 6px;
                border-radius: 10px;
                color: #555;
            }
            .switcher-trigger {
                position: fixed;
                top: 40%;
                left: 0;
                transform: translateY(-50%);
                z-index: 9999;
                width: 8px;
                height: 80px;
                background: transparent;
                pointer-events: none;        /* 让鼠标穿透，触发后方菜单的悬停 */
            }
            /* 适配移动端/小屏 */
            @media (max-width: 768px) {
                .file-switcher { width: 220px; left: -220px; }
            }

            .card { background: var(--card-bg); border-radius: 12px; box-shadow: 0 4px 12px rgba(0,0,0,0.05); padding: 30px; margin-bottom: 25px; transition: all 0.3s ease; }
            .card:hover { box-shadow: 0 6px 16px rgba(0,0,0,0.08); }
            
            .header-card { border-top: 4px solid var(--primary-color); display: flex; justify-content: space-between; align-items: center; }
            h1 { font-size: 24px; margin: 0; font-weight: 600; color: #1f2d3d; letter-spacing: 1px; }
            .score-value { font-size: 36px; color: var(--success-color); font-weight: bold; line-height: 1; font-family: 'Arial', sans-serif; }
            
            h2 { font-size: 18px; margin: 0 0 20px 0; padding-bottom: 12px; border-bottom: 2px solid var(--border-color); font-weight: 600; color: #1f2d3d; }
            
            .it-table { width: 100%; border-collapse: collapse; table-layout: fixed; }
            .it-table th { background: #f8f9fb; padding: 12px 15px; text-align: left; color: #909399; font-weight: 500; border-bottom: 2px solid var(--border-color); }
            .it-table td { padding: 12px 15px; border-bottom: 1px solid var(--border-color); vertical-align: top; }
            
            .main-item-row { font-weight: 600; color: #303133; background-color: #fafafa; }
            .main-item-row:hover { background-color: #f5f7fa; }
            
            .sub-item-row td:first-child { padding-left: 35px; color: #606266; font-size: 13px; position: relative; font-weight: normal; }
            .sub-item-row td:first-child::before { content: '└'; position: absolute; left: 18px; color: #c0c4cc; }
            .sub-item-row:hover { background-color: #fdfdfd; }

            .status-tag { padding: 2px 8px; border-radius: 10px; font-size: 12px; font-weight: bold; display: inline-block; text-decoration: none; }
            .tag-ok { background: #f0f9eb; color: var(--success-color); }
            .tag-err { background: var(--danger-light); color: var(--danger-color); border: 1px solid var(--danger-border); }
            .tag-warning { background: var(--warning-light); color: var(--warning-color); border: 1px solid var(--warning-border); }
            .tag-missing { background: var(--missing-bg); color: #606266; border: 1px solid var(--missing-border); opacity: 0.7; }
            
            a.status-tag:hover { opacity: 0.85; text-decoration: none; }
            .consistency-toggle { cursor: pointer; user-select: none; }

            .cell-truncate { display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical; overflow: hidden; text-overflow: ellipsis; word-break: break-all; }
            .name-truncate { color: inherit; font-size: inherit; }
            .preview-truncate { color: var(--text-light); font-size: 13px; }

            .legend-card { background: #fffaf0; border-left: 4px solid #e6a23c; padding: 16px 24px; border-radius: 6px; margin-bottom: 24px; font-size: 13px; color: var(--text-regular); display: flex; gap: 24px; align-items: center; }
            
            .table-wrapper { margin: 16px 0; border: 1px solid var(--border-color); border-radius: 6px; overflow: hidden; }
            .report-table { width: 100%; min-width: 80px;border-collapse: collapse; font-size: 13px; background: #fff; }
            .report-table td { border-bottom: 1px solid var(--border-color); min-width: 80px;border-right: 1px solid var(--border-color); padding: 10px 14px; vertical-align: middle; color: var(--text-regular); }
            .report-table td:last-child { border-right: none; }
            .report-table tr:last-child td { border-bottom: none; }
            .report-table tr:nth-child(even) { background: #fafbfc; }
            .report-table tr:hover { background: #ecf5ff; transition: background 0.2s; }
            
            .content-box { 
                background: #fdfdfd; 
                border: 1px solid var(--border-color); 
                border-radius: 6px; 
                padding: 20px 24px; 
                white-space: normal; 
                word-wrap: break-word; 
                color: var(--text-main); 
                font-size: 14px; 
                line-height: 1.8; 
                transition: max-height 0.3s ease;
            }
            .content-collapsed {
                max-height: 36em; 
                overflow: hidden;
                position: relative;
            }
            .content-collapsed::after {
                content: "";
                position: absolute;
                bottom: 0;
                left: 0;
                width: 100%;
                height: 40px;
                background: linear-gradient(transparent, #fdfdfd);
            }
            .content-expanded {
                max-height: none;
            }
            .expand-btn {
                display: inline-flex;
                align-items: center;
                gap: 6px;
                margin-top: 12px;
                padding: 6px 16px;
                background: #f0f2f5;
                border-radius: 20px;
                color: #606266;
                font-size: 13px;
                cursor: pointer;
                transition: all 0.2s;
                user-select: none;
            }
            .expand-btn:hover {
                background: #e4e7ed;
                color: #303133;
            }
            .expand-icon {
                font-size: 10px;
                transition: transform 0.2s;
            }
            
            .template-match { color: #0d5cb6; background-color: #e6f1fc; padding: 2px 6px; border-radius: 4px; margin: 0 2px; font-weight: 500; } 
            .bidder-data { color: var(--text-main); } 
            
            .missing-badge { display: inline-block; background: var(--danger-light); border: 1px solid var(--danger-border); color: var(--danger-color); padding: 2px 8px; border-radius: 12px; font-size: 12px; font-weight: 500; margin: 0 4px; vertical-align: middle; }
            
            .empty-text { color: var(--text-light); font-style: italic; background: #fafafa; padding: 24px; border-radius: 6px; text-align: center; border: 1px dashed #dcdfe6; letter-spacing: 1px; }
            .truncate-alert { margin-top: 16px; padding: 10px; text-align: center; background: #f4f4f5; color: #909399; border-radius: 4px; font-size: 13px; }
            
            .nav-link:hover { text-decoration: underline !important; }
            
            .back-link { 
                position: fixed;
                top: 40%;
                left: 0;
                transform: translateY(-50%);
                z-index: 9999;
                display: inline-flex; 
                align-items: center; 
                padding: 12px 10px; 
                background: rgba(255, 255, 255, 0.9);
                backdrop-filter: blur(4px);
                border: 1px solid var(--border-color);
                border-left: none; /* 贴边不需要左边框 */
                border-radius: 0 8px 8px 0; /* 仅右侧圆角 */
                box-shadow: 2px 0 10px rgba(0,0,0,0.08);
                color: var(--text-regular) !important; 
                text-decoration: none !important; 
                font-weight: bold; 
                font-size: 13px;
                writing-mode: vertical-lr;
                transition: all 0.3s ease;
            }
            .back-link:hover { 
                background: #fff;
                padding-left: 15px; /* 悬浮时向右微伸，增加交互感 */
                color: var(--primary-color) !important;
                border-color: var(--primary-color);
            }
            
            .child-row { display: none; }
            .child-row.visible { display: table-row; }
            .toggle-icon { display: inline-block; margin-left: 6px; font-size: 10px; }
        </style>
        """

    def _normalize(self, text: str) -> str:
        if not text:
            return ""
        text = text.replace('(', '（').replace(')', '）')
        return "".join(self.CONTENT_PATTERN.findall(text))

    def _highlight_bidder_text(self, bidder_text, all_anchors, missing_anchors):
        """高亮投标文件正文，返回 HTML 字符串"""
        if not bidder_text:
            return "<span class='empty-text'>[ 无内容 ]</span>"

        found_anchors = [a for a in all_anchors if a not in missing_anchors]
        anchor_counts = Counter(found_anchors)
        used_counts = {a: 0 for a in anchor_counts}

        missing_blocks = {}
        current_key = ("__START__", 0)
        missing_blocks[current_key] = []

        build_counts = {}
        seen_missing = set()

        for anchor in all_anchors:
            if anchor in missing_anchors:
                if anchor not in seen_missing:
                    missing_blocks[current_key].append(anchor)
                    seen_missing.add(anchor)
            else:
                count = build_counts.get(anchor, 0)
                current_key = (anchor, count)
                if current_key not in missing_blocks:
                    missing_blocks[current_key] = []
                build_counts[anchor] = count + 1

        active_patterns = {}
        for anchor in anchor_counts:
            gap = r'[^\u4e00-\u9fa5a-zA-Z0-9]*'
            regex_str = gap.join([re.escape(c) for c in anchor])
            active_patterns[anchor] = re.compile(regex_str, re.S)

        def highlight_string(s):
            if not s.strip():
                return s

            intervals = []
            for anchor, pattern in active_patterns.items():
                if used_counts[anchor] >= anchor_counts[anchor]:
                    continue
                for match in pattern.finditer(s):
                    intervals.append((match.start(), match.end(), anchor))

            if not intervals:
                return f"<span class='bidder-data'>{html.escape(s)}</span>"

            intervals.sort(key=lambda x: (x[0], -(x[1] - x[0])))
            selected_intervals = []
            local_used = {a: 0 for a in anchor_counts}
            last_end = 0

            for start, end, anchor in intervals:
                if used_counts[anchor] + local_used[anchor] >= anchor_counts[anchor]:
                    continue
                if start >= last_end:
                    current_instance_idx = used_counts[anchor] + local_used[anchor]
                    selected_intervals.append((start, end, anchor, current_instance_idx))
                    local_used[anchor] += 1
                    last_end = end

            for a, count in local_used.items():
                used_counts[a] += count

            parts = []
            curr = 0
            for start, end, anchor, anchor_idx in selected_intervals:
                if start > curr:
                    parts.append(f"<span class='bidder-data'>{html.escape(s[curr:start])}</span>")

                parts.append(f"<span class='template-match'>{html.escape(s[start:end])}</span>")

                key = (anchor, anchor_idx)
                if key in missing_blocks and missing_blocks[key]:
                    missing_texts = ", ".join(missing_blocks[key])
                    parts.append(f"<span class='missing-badge' title='此处缺失模版规定的内容'>缺: {html.escape(missing_texts)}</span>")
                    missing_blocks[key] = []

                curr = end

            if curr < len(s):
                parts.append(f"<span class='bidder-data'>{html.escape(s[curr:])}</span>")

            return "".join(parts)

        lines = bidder_text.split('\n')
        if len(lines) > 80:
            lines = lines[:80]
            lines.append('<div class="truncate-alert">... [ 篇幅过长，已触发全局折叠 ]</div>')

        parsed_lines = []

        start_key = ("__START__", 0)
        if start_key in missing_blocks and missing_blocks[start_key]:
            start_texts = ", ".join(missing_blocks[start_key])
            parsed_lines.append(f"<div style='margin-bottom:8px;'><span class='missing-badge'>缺: {html.escape(start_texts)}</span></div>")
            missing_blocks[start_key] = []

        in_table = False
        is_table_first_row = False

        for i, line in enumerate(lines):
            is_doc_first_line = (i == 0)

            if ' | ' in line:
                if not in_table:
                    parsed_lines.append('<div class="table-wrapper"><table class="report-table"><tbody>')
                    in_table = True
                    is_table_first_row = True

                cells = line.split(' | ')
                tr_cells = []
                for c in cells:
                    cell_text = c.strip()
                    if is_doc_first_line or (in_table and not is_table_first_row):
                        highlighted_cell = f"<span class='bidder-data'>{html.escape(cell_text)}</span>"
                    else:
                        highlighted_cell = highlight_string(cell_text)

                    clean_text = html.escape(re.sub(r'<[^>]+>', '', highlighted_cell)).replace('"', '&quot;')
                    tr_cells.append(f'<td><div class="cell-truncate" title="{clean_text}">{highlighted_cell}</div></td>')

                parsed_lines.append('<tr>' + ''.join(tr_cells) + '</tr>')
                is_table_first_row = False
            else:
                if in_table:
                    parsed_lines.append('</tbody></table></div>')
                    in_table = False

                if line.startswith('<div class="truncate-alert"'):
                    parsed_lines.append(line)
                else:
                    if is_doc_first_line:
                        highlighted_line = f"<span class='bidder-data'>{html.escape(line)}</span>"
                    else:
                        highlighted_line = highlight_string(line)
                    parsed_lines.append(highlighted_line + '<br>')

        if in_table:
            parsed_lines.append('</tbody></table></div>')

        leftover_missing = []
        for m_list in missing_blocks.values():
            leftover_missing.extend(m_list)
        if leftover_missing:
            final_leftovers = ", ".join(list(dict.fromkeys(leftover_missing)))
            parsed_lines.append(f"<br><div style='margin-top: 10px;'><span class='missing-badge' title='文末缺失内容'>缺: {html.escape(final_leftovers)}</span></div>")

        return "".join(parsed_lines)

    def _build_attachment_info(self, consistency_report, test_dict):
        info_map = {}
        for idx, rec in enumerate(consistency_report):
            title = rec['name']
            match = re.search(r'附件\s*(\d+(?:-\d+)?)', title)
            if not match:
                continue
            attachment_num = match.group(1)
            info_map[attachment_num] = {
                'title': title,
                'missing': rec.get('missing_anchors', []),
                'bidder_text': test_dict.get(title, ""),
                'idx': idx,
                'pages': list(rec.get('pages') or []),
                'locations': list(rec.get('locations') or []),
            }
        return info_map

    def _get_status_for_single_item(self, infos):
        if not infos:
            return ("无模版文件", "tag-missing")
        total_missing = sum(len(info['missing']) for info in infos)
        has_missing_content = any(not info['bidder_text'] for info in infos)
        if has_missing_content:
            return ("⚠️ 内容缺失", "tag-err")
        if total_missing > 0:
            return (f"⚠️ 缺失 {total_missing} 项", "tag-err")
        return ("✨ 格式匹配", "tag-ok")

    def _normalize_bbox(self, bbox):
        if not isinstance(bbox, (list, tuple)) or len(bbox) < 4:
            return None
        if not all(isinstance(item, (int, float)) for item in bbox[:4]):
            return None
        x0, y0, x1, y1 = [int(round(float(item))) for item in bbox[:4]]
        if x1 < x0 or y1 < y0:
            if x1 > 0 and y1 > 0:
                x1 = x0 + x1
                y1 = y0 + y1
        normalized = [min(x0, x1), min(y0, y1), max(x0, x1), max(y0, y1)]
        return normalized

    def _format_bbox_text(self, bbox):
        normalized = self._normalize_bbox(bbox)
        if not normalized:
            return ""
        return "[" + ", ".join(str(item) for item in normalized) + "]"

    def _coalesce_page_ranges(self, pages):
        normalized = sorted({page for page in pages or [] if isinstance(page, int)})
        if not normalized:
            return []
        ranges = []
        start = normalized[0]
        end = start
        for page in normalized[1:]:
            if page == end + 1:
                end = page
                continue
            ranges.append((start, end))
            start = page
            end = page
        ranges.append((start, end))
        return ranges

    def _render_location_summary(self, locations=None, pages=None, limit=4, default_document="bidder"):
        entries = []
        seen = set()
        seen_pages = set()
        for location in locations or []:
            if not isinstance(location, dict):
                continue
            page = location.get("page") if isinstance(location.get("page"), int) else None
            bbox = self._normalize_bbox(location.get("bbox"))
            bbox_text = self._format_bbox_text(bbox)
            note = str(
                location.get("label")
                or location.get("note")
                or location.get("section_anchor")
                or ""
            ).strip()
            document = str(location.get("document") or default_document or "bidder").strip() or "bidder"
            label = f"P{page}" if page is not None else "P?"
            if bbox_text:
                label = f"{label} {bbox_text}"
            if note:
                label = f"{note} {label}".strip()
            bbox_key = (
                tuple(int(round(value / 3)) for value in bbox if isinstance(value, (int, float)))
                if bbox
                else None
            )
            key = (document, note, page, bbox_key)
            if key in seen:
                continue
            seen.add(key)
            if page is not None:
                seen_pages.add(page)
            entries.append(
                {
                    "label": label,
                    "page": page,
                    "page_end": page,
                    "bbox": bbox if isinstance(bbox, (list, tuple)) else None,
                    "document": document,
                }
            )
        remaining_pages = [
            page for page in (pages or [])
            if isinstance(page, int) and page not in seen_pages
        ]
        for start_page, end_page in self._coalesce_page_ranges(remaining_pages):
            key = (default_document or "bidder", "", start_page, end_page)
            if key in seen:
                continue
            seen.add(key)
            label = f"P{start_page}" if start_page == end_page else f"P{start_page}-P{end_page}"
            entries.append(
                {
                    "label": label,
                    "page": start_page,
                    "page_end": end_page,
                    "bbox": None,
                    "document": default_document or "bidder",
                }
            )
        if not entries:
            return "<span class='status-tag tag-missing'>-</span>"
        chips = []
        for entry in entries[:limit]:
            label = html.escape(str(entry.get("label") or "-"))
            page = entry.get("page")
            page_end = entry.get("page_end")
            bbox = entry.get("bbox")
            document = html.escape(str(entry.get("document") or default_document or "bidder"))
            if page is not None:
                bbox_attr = ""
                if isinstance(bbox, (list, tuple)) and len(bbox) >= 4:
                    bbox_attr = ",".join(str(int(round(float(item)))) for item in bbox[:4] if isinstance(item, (int, float)))
                chips.append(
                    f"<button type='button' class='locator-chip locator-open' data-document='{document}' data-page='{page}' data-page-end='{page_end if isinstance(page_end, int) else page}' data-bbox='{html.escape(bbox_attr)}' data-label='{label}'>{label}</button>"
                )
            else:
                chips.append(f"<div class='locator-chip'>{label}</div>")
        if len(entries) > limit:
            chips.append("<div style='font-size:12px; color:#909399;'>...</div>")
        return "".join(chips)

    def _entry_locations(self, entry, default_label=None):
        if not isinstance(entry, dict):
            return []
        label_parts = []
        base_label = str(default_label or "").strip()
        if base_label:
            label_parts.append(base_label)
        anchor = str(entry.get("section_anchor") or "").strip()
        if anchor and anchor not in {"logical_table", "layout"}:
            label_parts.append(anchor)
        label = " / ".join(label_parts)
        pages = [page for page in (entry.get("section_pages") or []) if isinstance(page, int)]
        if not pages and isinstance(entry.get("page"), int):
            pages = [entry.get("page")]
        return [{"page": page, "label": label} for page in pages]

    def _build_locator_support_style(self):
        return """
        <style>
            .locator-chip {
                margin: 2px 0;
                padding: 2px 6px;
                border-radius: 10px;
                background: #eef1ed;
                color: #4f5a52;
                font-size: 12px;
                white-space: nowrap;
                border: 1px solid #d9ded8;
                cursor: pointer;
                transition: all 0.2s ease;
            }
            button.locator-chip {
                font: inherit;
            }
            .locator-chip:hover {
                background: #e3ebff;
                border-color: #b8cdf5;
                color: #2457b2;
            }
            .locator-overlay {
                position: fixed;
                inset: 0;
                background: rgba(17, 24, 39, 0.45);
                display: none;
                align-items: center;
                justify-content: center;
                z-index: 30000;
                padding: 20px;
            }
            .locator-overlay.visible {
                display: flex;
            }
            .locator-dialog {
                width: min(1180px, calc(100vw - 40px));
                height: min(92vh, 920px);
                background: #fff;
                border-radius: 14px;
                box-shadow: 0 20px 60px rgba(0, 0, 0, 0.18);
                display: flex;
                flex-direction: column;
                overflow: hidden;
            }
            .locator-toolbar {
                display: flex;
                align-items: center;
                justify-content: space-between;
                gap: 12px;
                padding: 14px 18px;
                border-bottom: 1px solid var(--border-color);
                background: #f8fafc;
            }
            .locator-title {
                font-size: 16px;
                font-weight: 600;
                color: var(--text-main);
            }
            .locator-subtitle {
                margin-top: 4px;
                font-size: 12px;
                color: var(--text-light);
            }
            .locator-close {
                border: 1px solid var(--border-color);
                background: #fff;
                color: var(--text-regular);
                border-radius: 8px;
                padding: 6px 12px;
                cursor: pointer;
                font-size: 13px;
            }
            .locator-close:hover {
                color: var(--primary-color);
                border-color: #bfd6ff;
            }
            .locator-actions {
                display: flex;
                align-items: center;
                gap: 8px;
            }
            .locator-source {
                display: inline-flex;
                align-items: center;
                justify-content: center;
                border: 1px solid #bfd6ff;
                background: #f4f8ff;
                color: #2457b2;
                border-radius: 8px;
                padding: 6px 12px;
                cursor: pointer;
                font-size: 13px;
                text-decoration: none;
            }
            .locator-source:hover {
                background: #eaf1ff;
            }
            .locator-stage {
                flex: 1;
                overflow: auto;
                padding: 18px;
                background: #f5f7fa;
            }
            .locator-empty {
                height: 100%;
                display: flex;
                align-items: center;
                justify-content: center;
                color: var(--text-light);
                font-size: 14px;
                text-align: center;
            }
            .locator-page-wrap {
                position: relative;
                margin: 0 auto;
                display: inline-block;
                background: #fff;
                box-shadow: 0 8px 24px rgba(15, 23, 42, 0.12);
            }
            .locator-page-image {
                display: block;
                max-width: 100%;
                height: auto;
            }
            .locator-box {
                position: absolute;
                border: 3px solid #ef4444;
                background: rgba(239, 68, 68, 0.14);
                box-shadow: 0 0 0 1px rgba(255,255,255,0.6) inset;
                pointer-events: none;
                display: none;
            }
            .locator-box.visible {
                display: block;
            }
            .locator-note {
                margin-top: 12px;
                color: var(--text-regular);
                font-size: 13px;
            }
        </style>
        """

    def _build_locator_support_markup(self):
        return """
        <div id="locator-overlay" class="locator-overlay">
            <div class="locator-dialog" role="dialog" aria-modal="true" aria-label="定位预览">
                <div class="locator-toolbar">
                    <div>
                        <div id="locator-title" class="locator-title">定位预览</div>
                        <div id="locator-subtitle" class="locator-subtitle">请选择页码定位</div>
                    </div>
                    <div class="locator-actions">
                        <a id="locator-source" class="locator-source" href="#" target="_blank" rel="noopener noreferrer" style="display:none;">打开原文件</a>
                        <button type="button" id="locator-close" class="locator-close">关闭</button>
                    </div>
                </div>
                <div class="locator-stage">
                    <div id="locator-empty" class="locator-empty">当前没有可用的页面预览资源。</div>
                    <div id="locator-page-wrap" class="locator-page-wrap" style="display:none;">
                        <img id="locator-page-image" class="locator-page-image" alt="定位预览页">
                        <div id="locator-box" class="locator-box"></div>
                    </div>
                    <div id="locator-note" class="locator-note" style="display:none;"></div>
                </div>
            </div>
        </div>
        """

    def _build_locator_support_script(self):
        config_json = json.dumps(self._document_preview_config or {"documents": {}}, ensure_ascii=False)
        return f"""
        <script>
            (function() {{
                const locatorPreviewConfig = {config_json};
                const overlay = document.getElementById('locator-overlay');
                const closeBtn = document.getElementById('locator-close');
                const sourceLink = document.getElementById('locator-source');
                const titleNode = document.getElementById('locator-title');
                const subtitleNode = document.getElementById('locator-subtitle');
                const emptyNode = document.getElementById('locator-empty');
                const pageWrap = document.getElementById('locator-page-wrap');
                const imageNode = document.getElementById('locator-page-image');
                const boxNode = document.getElementById('locator-box');
                const noteNode = document.getElementById('locator-note');

                function hideLocator() {{
                    if (!overlay) return;
                    overlay.classList.remove('visible');
                    imageNode.removeAttribute('src');
                    imageNode.onload = null;
                    boxNode.classList.remove('visible');
                    pageWrap.style.display = 'none';
                    emptyNode.style.display = 'flex';
                    noteNode.style.display = 'none';
                    noteNode.textContent = '';
                    if (sourceLink) {{
                        sourceLink.style.display = 'none';
                        sourceLink.setAttribute('href', '#');
                    }}
                }}

                function parseBbox(raw) {{
                    if (!raw) return null;
                    const values = String(raw).split(',').map(Number).filter(Number.isFinite);
                    if (values.length < 4) return null;
                    return values.slice(0, 4);
                }}

                function resolvePagePreview(documentKey, page) {{
                    const docs = (locatorPreviewConfig && locatorPreviewConfig.documents) || {{}};
                    const docConfig = docs[documentKey];
                    if (!docConfig) return null;
                    const pageConfig = (docConfig.pages || {{}})[String(page)];
                    if (!pageConfig) return null;
                    return {{
                        docConfig,
                        pageConfig,
                    }};
                }}

                function sourceKindLabel(kind) {{
                    if (kind === 'pdf') return 'PDF预览';
                    if (kind === 'image') return '图片预览';
                    if (kind === 'synthetic') return '合成预览';
                    return '页面预览';
                }}

                function sourceHint(docConfig) {{
                    if (!docConfig) return '';
                    if (docConfig.source_kind === 'synthetic') {{
                        return '未找到原始PDF/图片，当前使用OCR结果生成的定位预览。';
                    }}
                    if (docConfig.source_kind === 'pdf') {{
                        return '当前预览页由真实PDF渲染生成，可用于定位原文位置。';
                    }}
                    if (docConfig.source_kind === 'image') {{
                        return '当前预览页来自原始图片文件。';
                    }}
                    return '';
                }}

                function renderBox(bbox, pageConfig) {{
                    if (!bbox || !pageConfig) {{
                        boxNode.classList.remove('visible');
                        return;
                    }}
                    const naturalWidth = Number(pageConfig.width) || imageNode.naturalWidth || imageNode.width;
                    const naturalHeight = Number(pageConfig.height) || imageNode.naturalHeight || imageNode.height;
                    if (!naturalWidth || !naturalHeight) {{
                        boxNode.classList.remove('visible');
                        return;
                    }}
                    const scaleX = imageNode.clientWidth / naturalWidth;
                    const scaleY = imageNode.clientHeight / naturalHeight;
                    const left = bbox[0] * scaleX;
                    const top = bbox[1] * scaleY;
                    const width = Math.max((bbox[2] - bbox[0]) * scaleX, 6);
                    const height = Math.max((bbox[3] - bbox[1]) * scaleY, 6);
                    boxNode.style.left = left + 'px';
                    boxNode.style.top = top + 'px';
                    boxNode.style.width = width + 'px';
                    boxNode.style.height = height + 'px';
                    boxNode.classList.add('visible');
                }}

                function openLocator(button) {{
                    if (!overlay) return;
                    const documentKey = button.dataset.document || 'bidder';
                    const page = Number(button.dataset.page || 0);
                    const pageEnd = Number(button.dataset.pageEnd || button.dataset.page || 0);
                    const label = button.dataset.label || button.textContent || '定位预览';
                    const bbox = parseBbox(button.dataset.bbox);
                    const resolved = resolvePagePreview(documentKey, page);
                    const docs = (locatorPreviewConfig && locatorPreviewConfig.documents) || {{}};
                    const docConfig = docs[documentKey] || {{}};
                    const pageRangeText = pageEnd && pageEnd > page ? `${{page}}-${{pageEnd}}` : `${{page || '?'}}`;
                    titleNode.textContent = label;
                    subtitleNode.textContent = `${{docConfig.title || documentKey}} - 第 ${{pageRangeText}} 页 · ${{sourceKindLabel(docConfig.source_kind)}}`;
                    const extraHint = sourceHint(docConfig);
                    const rangeHint = pageEnd && pageEnd > page ? `连续页范围：${{page}}-${{pageEnd}}。` : '';
                    if (bbox && extraHint) {{
                        noteNode.textContent = `坐标：[${{bbox.join(', ')}}]  ·  ${{rangeHint}}${{extraHint}}`;
                    }} else if (bbox) {{
                        noteNode.textContent = rangeHint ? `坐标：[${{bbox.join(', ')}}]  ·  ${{rangeHint}}` : `坐标：[${{bbox.join(', ')}}]`;
                    }} else if (rangeHint && extraHint) {{
                        noteNode.textContent = `${{rangeHint}}${{extraHint}}`;
                    }} else if (rangeHint) {{
                        noteNode.textContent = rangeHint;
                    }} else {{
                        noteNode.textContent = extraHint || '';
                    }}
                    noteNode.style.display = noteNode.textContent ? 'block' : 'none';
                    if (sourceLink) {{
                        const rawSourceUrl = docConfig.source_url || '';
                        sourceLink.textContent = pageEnd && pageEnd > page ? `打开原文件（第 ${{pageRangeText}} 页）` : '打开原文件';
                        if (rawSourceUrl) {{
                            let finalUrl = rawSourceUrl;
                            if (docConfig.source_kind === 'pdf' && page) {{
                                finalUrl = `${{rawSourceUrl}}#page=${{page}}`;
                            }}
                            sourceLink.setAttribute('href', finalUrl);
                            sourceLink.style.display = 'inline-flex';
                        }} else {{
                            sourceLink.style.display = 'none';
                            sourceLink.setAttribute('href', '#');
                        }}
                    }}

                    if (!resolved) {{
                        pageWrap.style.display = 'none';
                        boxNode.classList.remove('visible');
                        emptyNode.style.display = 'flex';
                        emptyNode.textContent = '当前缺少该页的预览资源。';
                        overlay.classList.add('visible');
                        return;
                    }}

                    const pageConfig = resolved.pageConfig;
                    emptyNode.style.display = 'none';
                    pageWrap.style.display = 'inline-block';
                    overlay.classList.add('visible');
                    imageNode.onload = function() {{
                        renderBox(bbox, pageConfig);
                    }};
                    imageNode.src = pageConfig.image_url;
                    if (imageNode.complete) {{
                        renderBox(bbox, pageConfig);
                    }}
                }}

                function bindLocatorChips() {{
                    document.querySelectorAll('.locator-open').forEach((button) => {{
                        if (button.dataset.locatorBound === '1') return;
                        button.dataset.locatorBound = '1';
                        button.addEventListener('click', function() {{
                            openLocator(this);
                        }});
                    }});
                }}

                if (closeBtn) {{
                    closeBtn.addEventListener('click', hideLocator);
                }}
                if (overlay) {{
                    overlay.addEventListener('click', function(event) {{
                        if (event.target === overlay) {{
                            hideLocator();
                        }}
                    }});
                }}
                document.addEventListener('keydown', function(event) {{
                    if (event.key === 'Escape') {{
                        hideLocator();
                    }}
                }});

                bindLocatorChips();
                window.__bindLocatorChips = bindLocatorChips;
            }})();
        </script>
        """

    def _generate_detail_card(self, title, missing, bidder_text, model_text, checker, pages=None, locations=None):
        location_html = self._render_location_summary(locations=locations, pages=pages)
        if not bidder_text:
            return f"""
                <div class="card">
                    <h2 style="color: var(--primary-color); border: none; margin-bottom: 15px;">■ {title}</h2>
                    <div style='margin-bottom: 12px;'><strong style='font-size:13px; color: var(--text-regular);'>定位：</strong>{location_html}</div>
                    <div class='empty-text'>[ ⚠️ 投标文件中未检测到该部分内容，请重点核实 ]</div>
                </div>
            """
            
        lines = bidder_text.split('\n')
        line_count = len(lines)
        is_long_content = line_count > 20
        
        collapse_class = "content-collapsed" if is_long_content else "content-expanded"
        expand_btn_style = "display: inline-flex;" if is_long_content else "display: none;"

        if '\n' in model_text:
            model_text = model_text.split('\n', 1)[1]
        all_anchors = checker._get_anchors(model_text)
        highlighted = self._highlight_bidder_text(bidder_text, all_anchors, missing)
        
        if highlighted is None:
            highlighted = "<span class='empty-text'>[ 内容解析异常 ]</span>"
        
        all_matched = (len(missing) == 0)
        uid = ''.join(random.choices(string.ascii_lowercase, k=8))
        status_badge = "<span class='status-tag tag-ok'>✨ 格式与模版内容完全匹配</span>" if all_matched else f"<span class='status-tag tag-err'>⚠️ 缺失 {len(missing)} 项模版内容</span>"

        return f"""
        <div class="card">
            <h2 style="color: var(--primary-color); border: none; margin-bottom: 15px;">■ {title}</h2>
            <div style='margin-bottom: 12px; display: flex; flex-wrap: wrap; gap: 10px; align-items: center;'>
                {status_badge}
            </div>
            <div style='margin-bottom: 12px;'><strong style='font-size:13px; color: var(--text-regular);'>定位：</strong>{location_html}</div>
            <div class="content-box {collapse_class}" id="content-{uid}">
                {highlighted}
            </div>
            <div class="expand-btn" id="toggle-{uid}" data-uid="{uid}" style="{expand_btn_style}">
                <span class="expand-text">显示全部</span>
                <span class="expand-icon">▼</span>
            </div>
        </div>
        """

    def _generate_combined_detail_page(self, attachments_info, combined_filename, model_dict, checker):
        detail_cards = []
        for info in attachments_info:
            title = info['title']
            missing = info['missing']
            bidder_text = info['bidder_text']
            model_text = model_dict.get(title, "")
            card = self._generate_detail_card(
                title,
                missing,
                bidder_text,
                model_text,
                checker,
                pages=info.get("pages"),
                locations=info.get("locations"),
            )
            detail_cards.append(card)

        combined_html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <title>合并附件详情 - 审查报告</title>
            {self.CSS_STYLE}
            {self._build_locator_support_style()}
        </head>
        <body>
            {self._build_locator_support_markup()}
            <a href="javascript:void(0);" onclick="window.history.back();" class="back-link">返回总览报告</a>

            <div class="container">
                <div style="height: 40px;"></div>

                <div class="legend-card">
                    <strong>💡 阅读指引：</strong>
                    <span><span class="template-match">浅蓝底纹</span> = 模版内容匹配成功</span>
                    <span><span class="bidder-data" style="font-weight:600;">深灰文本</span> = 投标人填写的业务数据</span>
                    <span><span class="missing-badge" style="margin:0;">缺: 缺失内容</span> = 文本中缺失的模版要求</span>
                </div>

                {"".join(detail_cards)}
            </div>

            <script>
                (function() {{
                    function bindExpandButtons() {{
                        document.querySelectorAll('.expand-btn').forEach(btn => {{
                            btn.removeEventListener('click', btn._expandHandler);
                            const handler = function() {{
                                const uid = this.dataset.uid;
                                const content = document.getElementById('content-' + uid);
                                const textSpan = this.querySelector('.expand-text');
                                const iconSpan = this.querySelector('.expand-icon');
                                if (content.classList.contains('content-collapsed')) {{
                                    content.classList.remove('content-collapsed');
                                    content.classList.add('content-expanded');
                                    textSpan.textContent = '收起';
                                    iconSpan.style.transform = 'rotate(180deg)';
                                }} else {{
                                    content.classList.add('content-collapsed');
                                    content.classList.remove('content-expanded');
                                    textSpan.textContent = '显示全部';
                                    iconSpan.style.transform = 'rotate(0deg)';
                                }}
                            }};
                            btn._expandHandler = handler;
                            btn.addEventListener('click', handler);
                        }});
                    }}
                    bindExpandButtons();
                }})();
            </script>
            {self._build_locator_support_script()}
        </body>
        </html>
        """
        with open(combined_filename, "w", encoding="utf-8") as f:
            f.write(combined_html)

    def _generate_deviation_section(self, deviation_report):
        if not deviation_report:
            return ""
        
        stats = deviation_report.get('stats', {})
        findings = deviation_report.get('key_findings', [])
        match_results = deviation_report.get('match_results', [])
        
        type_map = {
            'no_deviation': ('tag-ok', '无偏离'),
            'positive_deviation': ('tag-ok', '正偏离'),
            'negative_deviation': ('tag-err', '负偏离'),
            'listed_response': ('tag-warning', '已响应'),
            'missing': ('tag-err', '缺失响应'),
            'unclear_deviation': ('tag-warning', '偏离不明确')
        }

        findings_html = "".join([f"<li>{html.escape(f)}</li>" for f in findings])
        
        rows_html = ""
        for item in match_results:
            d_type = item.get('deviation_type', 'unclear')
            tag_cls, tag_text = type_map.get(d_type, ('tag-missing', '未知'))
            
            evidence = item.get('response_evidence', '-')
            page = item.get("response_page")
            line_number = item.get("response_line_number")
            if page or line_number:
                location_html = self._render_location_summary(
                    locations=[
                        {
                            "page": page if isinstance(page, int) else None,
                            "label": f"L{line_number}" if line_number else "响应位置",
                            "document": "bidder",
                        }
                    ],
                    pages=[page] if isinstance(page, int) else None,
                    limit=2,
                )
            else:
                location_html = "-"
            
            rows_html += f"""
                <tr>
                    <td><div class="cell-truncate" title="{html.escape(item['requirement'])}">{html.escape(item['requirement'])}</div></td>
                    <td><span class="status-tag {tag_cls}">{tag_text}</span></td>
                    <td>{location_html}</td>
                    <td><div class="cell-truncate preview-truncate" title="{html.escape(evidence)}">{html.escape(evidence)}</div></td>
                </tr>
            """

        return f"""
        <div class="card">
            <h2 style="display:flex; justify-content:space-between;">
                <span>⚖️ 偏离条款审查 (★ 强制性项)</span>
                <span style="font-size:13px; font-weight:normal;">
                    发现 {deviation_report.get('core_requirements_count', 0)} 项，
                    负偏离/缺失 <span style="color:var(--danger-color); font-weight:bold;">{stats.get('negative_deviation_count',0) + stats.get('missing_count',0)}</span> 项
                </span>
            </h2>
            <div style="background:#fefefe; border:1px solid #eee; padding:15px; border-radius:8px; margin-bottom:20px; font-size:13px;">
                <ul style="margin:0; padding-left:20px; color:var(--text-regular);">
                    {findings_html}
                </ul>
            </div>
            <table class="it-table">
                <thead>
                    <tr>
                        <th width="45%">招标要求 (★)</th>
                        <th width="15%">偏离状态</th>
                        <th width="12%">定位</th>
                        <th>响应证据</th>
                    </tr>
                </thead>
                <tbody>
                    {rows_html if rows_html else '<tr><td colspan="4" class="empty-text">未检测到带 ★ 的强制性要求</td></tr>'}
                </tbody>
            </table>
        </div>
        """
    
    def _generate_pricing_section(self, pricing_report):
        """生成分项报价审查的可视化 HTML 片段"""
        if not pricing_report: return ""
        status = pricing_report.get('status', 'unknown')
        summary = pricing_report.get('summary', '未提取到报价信息')
        details = pricing_report.get('details', [])
        checks = pricing_report.get('checks', {}) or {}
        evidence = pricing_report.get('evidence', {}) or {}
        sum_check = checks.get('sum_consistency', {})
        row_arithmetic = checks.get("row_arithmetic", {}) or {}
        extracted_items = evidence.get("extracted_items") or []
        total_candidates = evidence.get("total_candidates") or []
        
        status_map = {'pass': ('tag-ok', '逻辑一致'), 'fail': ('tag-err', '存在疑点'), 'unknown': ('tag-warning', '待人工核实')}
        tag_cls, tag_text = status_map.get(status, ('tag-missing', '未知'))
        details_html = "".join([f"<li>{html.escape(d)}</li>" for d in details])

        overview_locations = []
        for entry in total_candidates[:6]:
            overview_locations.extend(self._entry_locations(entry, default_label=entry.get("label") or "总价"))
        if not overview_locations:
            for entry in extracted_items[:6]:
                overview_locations.extend(self._entry_locations(entry, default_label=entry.get("label") or "分项金额"))
        location_html = self._render_location_summary(locations=overview_locations, limit=6)
        
        sum_panel = ""
        if sum_check.get('calculated_total') is not None:
            sum_panel = f"""
            <div style="margin-top:15px; background:#f9fafc; border-radius:6px; padding:12px; border:1px solid #eee;">
                <div style="display:grid; grid-template-columns: 1fr 1fr 1fr; gap:10px; font-size:13px; text-align:center;">
                    <div>计算汇总: <strong>{sum_check.get('calculated_total')}</strong></div>
                    <div>声明总价: <strong>{sum_check.get('declared_total') or '-'}</strong></div>
                    <div>偏差: <span style="color:{'var(--danger-color)' if float(sum_check.get('difference', 0) or 0) != 0 else 'var(--success-color)'}; font-weight:bold;">{sum_check.get('difference') or '0.00'}</span></div>
                </div>
            </div>
            """

        issue_panel = ""
        row_issues = row_arithmetic.get("issues") or []
        unresolved_rows = row_arithmetic.get("unresolved_rows") or []
        if row_issues:
            issue_rows_html = ""
            for issue in row_issues[:10]:
                issue_location_html = self._render_location_summary(
                    locations=self._entry_locations(
                        issue,
                        default_label=issue.get("serial") or issue.get("label") or "算术疑点",
                    ),
                    limit=2,
                )
                issue_rows_html += f"""
                <tr>
                    <td>{html.escape(str(issue.get('label') or '-'))}</td>
                    <td>{html.escape(str(issue.get('line_total') or issue.get('declared_group_total') or '-'))}</td>
                    <td>{html.escape(str(issue.get('expected_total') or '-'))}</td>
                    <td>{html.escape(str(issue.get('difference') or '-'))}</td>
                    <td>{issue_location_html}</td>
                </tr>
                """
            issue_panel += f"""
            <div style="margin-top:15px;">
                <div style="font-size:13px; font-weight:bold; color:var(--text-main); margin-bottom:8px;">算术疑点定位</div>
                <table class="it-table">
                    <thead>
                        <tr><th>条目</th><th>声明值</th><th>计算值</th><th>偏差</th><th>定位</th></tr>
                    </thead>
                    <tbody>{issue_rows_html}</tbody>
                </table>
            </div>
            """
        if unresolved_rows:
            unresolved_rows_html = ""
            for item in unresolved_rows[:10]:
                unresolved_location_html = self._render_location_summary(
                    locations=self._entry_locations(
                        item,
                        default_label=item.get("serial") or item.get("label") or "未完整识别",
                    ),
                    limit=2,
                )
                unresolved_rows_html += f"""
                <tr>
                    <td>{html.escape(str(item.get('label') or '-'))}</td>
                    <td>{html.escape(str(item.get('reason_text') or item.get('reason') or '-'))}</td>
                    <td>{unresolved_location_html}</td>
                </tr>
                """
            issue_panel += f"""
            <div style="margin-top:15px;">
                <div style="font-size:13px; font-weight:bold; color:var(--text-main); margin-bottom:8px;">未完整识别条目</div>
                <table class="it-table">
                    <thead>
                        <tr><th>条目</th><th>原因</th><th>定位</th></tr>
                    </thead>
                    <tbody>{unresolved_rows_html}</tbody>
                </table>
            </div>
            """

        return f"""
        <div class="card">
            <h2 style="display:flex; justify-content:space-between;">
                <span>💰 分项报价逻辑审查</span>
                <span class="status-tag {tag_cls}">{tag_text}</span>
            </h2>
            <div style="background:#fefefe; border:1px solid #eee; padding:15px; border-radius:8px; margin-bottom:15px; font-size:13px;">
                <div style="font-weight:bold; margin-bottom:8px; color:var(--text-main);">{html.escape(summary)}</div>
                <ul style="margin:0; padding-left:20px; color:var(--text-regular);">{details_html}</ul>
                <div style="margin-top:12px;"><strong style='font-size:13px; color: var(--text-regular);'>定位：</strong>{location_html}</div>
            </div>
            {sum_panel}
            {issue_panel}
        </div>
        """

    def _generate_reasonableness_section(self, reasonableness_report):
        """生成报价合理性审查的可视化 HTML 片段"""
        if not reasonableness_report: return ""
        
        # 结果可能是单个字典或列表
        reports = reasonableness_report if isinstance(reasonableness_report, list) else [reasonableness_report]
        
        sections_html = ""
        for report in reports:
            status = report.get('result', '失败')
            type_text = report.get('type', '报价检查')
            summary_list = report.get('summary', [])
            location_html = self._render_location_summary(
                locations=report.get("locations"),
                pages=None if report.get("locations") else report.get("pages"),
                limit=6,
            )
            
            tag_cls = 'tag-ok' if status == '合格' else 'tag-err'
            findings_html = "".join([f"<li>{html.escape(s)}</li>" for s in summary_list])
            
            sections_html += f"""
            <div style="margin-bottom: 20px;">
                <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom: 10px;">
                    <h3 style="margin:0; font-size:15px; color:var(--text-main); border-left: 4px solid var(--primary-color); padding-left: 10px;">{html.escape(type_text)}</h3>
                    <span class="status-tag {tag_cls}">{html.escape(status)}</span>
                </div>
                <div style="background:#f9fafc; border:1px solid #eee; padding:12px; border-radius:6px;">
                    <ul style="margin:0; padding-left:20px; font-size:13px; color:var(--text-regular);">
                        {findings_html}
                    </ul>
                    <div style="margin-top:12px;"><strong style='font-size:13px; color: var(--text-regular);'>定位：</strong>{location_html}</div>
                </div>
            </div>
            """

        return f"""
        <div class="card">
            <h2>⚖️ 报价合理性与合规性审查</h2>
            {sections_html}
        </div>
        """
    
    def _generate_verification_section(self, verification_report):
        """Render signature, seal, and signing-date verification."""
        if not verification_report:
            return ""

        overall_status = str(verification_report.get("compliance_status") or "pending")
        status_map = {
            "pass": ("tag-ok", "通过"),
            "fail": ("tag-err", "未通过"),
            "pending": ("tag-warning", "待复核"),
        }
        tag_cls, tag_text = status_map.get(overall_status, ("tag-missing", "未知"))

        position_check = verification_report.get("position_check") or {}
        date_check = verification_report.get("date_check") or {}
        seal_company_check = verification_report.get("seal_company_check") or {}
        attachment_results = verification_report.get("attachment_results") or []

        summary_items = []
        summary_text = str(verification_report.get("summary") or "").strip()
        if summary_text:
            summary_items.append(summary_text)

        required_count = verification_report.get("required_attachment_count")
        checked_count = verification_report.get("checked_attachment_count")
        if required_count is not None:
            if checked_count is not None:
                summary_items.append(f"已核验附件 {checked_count} / {required_count} 个")
            else:
                summary_items.append(f"需核验附件 {required_count} 个")

        deadline_date = date_check.get("deadline_date")
        deadline_text = date_check.get("matched_deadline_text")
        if deadline_date:
            deadline_summary = f"招标截止日期：{deadline_date}"
            if deadline_text:
                deadline_summary += f"（依据：{deadline_text}）"
            summary_items.append(deadline_summary)

        bidder_name = str(verification_report.get("bidder_name") or "").strip()
        seal_best_match = seal_company_check.get("best_match") or {}
        if bidder_name:
            seal_summary = f"投标人名称：{bidder_name}"
            if seal_best_match.get("seal_text"):
                seal_summary += f"，公章文本：{seal_best_match['seal_text']}"
            if seal_best_match.get("score") is not None:
                seal_summary += f"，匹配分值：{seal_best_match['score']}"
            summary_items.append(seal_summary)

        issue_groups = [
            ("缺少签字", position_check.get("missing_signature_attachments") or []),
            ("签字待复核", position_check.get("pending_signature_attachments") or []),
            ("缺少盖章", position_check.get("missing_seal_attachments") or []),
            ("缺少落款日期", date_check.get("missing_date_attachments") or []),
            ("落款晚于截止日期", date_check.get("late_date_attachments") or []),
        ]
        issue_panels = []
        for label, items in issue_groups:
            if not items:
                continue
            items_html = "".join(f"<li>{html.escape(str(item))}</li>" for item in items)
            issue_panels.append(f"""
            <div style="margin-top:12px; background:#f9fafc; border:1px solid #eee; padding:12px; border-radius:6px;">
                <div style="font-weight:bold; margin-bottom:6px; color:var(--text-main);">{html.escape(label)}（{len(items)}）</div>
                <ul style="margin:0; padding-left:20px; color:var(--text-regular); font-size:13px;">
                    {items_html}
                </ul>
            </div>
            """)

        sub_status_map = {
            "pass": ("tag-ok", "通过"),
            "fail": ("tag-err", "缺失"),
            "pending": ("tag-warning", "待复核"),
            "late": ("tag-err", "晚于截止"),
            "missing_date": ("tag-err", "缺少日期"),
            "missing_deadline": ("tag-warning", "缺截止日期"),
            "not_required": ("tag-missing", "不要求"),
        }

        def render_sub_status(status):
            sub_tag_cls, sub_tag_text = sub_status_map.get(str(status or ""), ("tag-missing", str(status or "未知")))
            return f'<span class="status-tag {sub_tag_cls}">{html.escape(sub_tag_text)}</span>'

        def build_attachment_note(item):
            notes = []
            signature_check = item.get("signature_check") or {}
            seal_check = item.get("seal_check") or {}
            sign_date_check = item.get("date_check") or {}

            filled_values = signature_check.get("filled_values") or []
            if filled_values:
                values = [str(x.get("value") or x.get("line") or "").strip() for x in filled_values]
                values = [value for value in values if value]
                if values:
                    notes.append("签字值：" + "；".join(values))
                location_values = []
                for value in filled_values:
                    signature_page = value.get("signature_page") or value.get("seal_page") or value.get("evidence_page") or value.get("page")
                    bbox = value.get("signature_box") or value.get("seal_box")
                    bbox_text = self._format_bbox_text(bbox)
                    if signature_page is not None or bbox_text:
                        label = f"P{signature_page}" if signature_page is not None else "P?"
                        if bbox_text:
                            label += f" {bbox_text}"
                        location_values.append(label)
                if location_values:
                    notes.append("签字定位：" + "；".join(location_values))

            pending_fields = signature_check.get("pending_fields") or []
            if pending_fields:
                values = [str(x.get("line") or "").strip() for x in pending_fields]
                values = [value for value in values if value]
                if values:
                    notes.append("待复核签字位：" + "；".join(values))

            empty_fields = signature_check.get("empty_fields") or []
            if empty_fields:
                values = [str(x.get("line") or "").strip() for x in empty_fields]
                values = [value for value in values if value]
                if values:
                    notes.append("空签字位：" + "；".join(values))

            best_match = seal_check.get("best_match") or {}
            if best_match.get("seal_text"):
                notes.append(f"公章：{best_match['seal_text']}")
            seal_locations = seal_check.get("seal_locations") or []
            if seal_locations:
                location_values = []
                for value in seal_locations[:3]:
                    page = value.get("page")
                    bbox_text = self._format_bbox_text(value.get("box") or value.get("bbox"))
                    label = f"P{page}" if page is not None else "P?"
                    if bbox_text:
                        label += f" {bbox_text}"
                    location_values.append(label)
                if location_values:
                    notes.append("盖章定位：" + "；".join(location_values))

            matched_sign_text = sign_date_check.get("matched_sign_text")
            if matched_sign_text:
                notes.append(f"落款日期：{matched_sign_text}")
            if sign_date_check.get("matched_sign_page") is not None:
                notes.append(f"日期定位：P{sign_date_check['matched_sign_page']}")

            if sign_date_check.get("status") == "late" and sign_date_check.get("deadline_date"):
                notes.append(f"晚于截止日期 {sign_date_check['deadline_date']}")

            return "；".join(notes) or "-"

        rows_html = ""
        for item in attachment_results:
            signature_check = item.get("signature_check") or {}
            seal_check = item.get("seal_check") or {}
            sign_date_check = item.get("date_check") or {}
            note_text = build_attachment_note(item)
            pages = item.get("pages") or []
            location_entries = []
            for value in signature_check.get("signature_locations") or []:
                if not isinstance(value, dict):
                    continue
                location_entries.append(
                    {
                        "page": value.get("page"),
                        "bbox": value.get("box") or value.get("bbox"),
                        "label": "签字",
                        "document": "bidder",
                    }
                )
            for value in seal_check.get("seal_locations") or []:
                if not isinstance(value, dict):
                    continue
                location_entries.append(
                    {
                        "page": value.get("page"),
                        "bbox": value.get("box") or value.get("bbox"),
                        "label": "盖章",
                        "document": "bidder",
                    }
                )
            if sign_date_check.get("matched_sign_page") is not None:
                location_entries.append(
                    {
                        "page": sign_date_check.get("matched_sign_page"),
                        "label": "落款日期",
                        "document": "bidder",
                    }
                )
            page_html = self._render_location_summary(
                locations=location_entries,
                pages=pages,
                limit=5,
            )
            rows_html += f"""
                <tr>
                    <td><div class="cell-truncate" title="{html.escape(str(item.get('title') or '-'))}">{html.escape(str(item.get('title') or '-'))}</div></td>
                    <td>{page_html}</td>
                    <td>{render_sub_status(signature_check.get('status'))}</td>
                    <td>{render_sub_status(seal_check.get('status'))}</td>
                    <td>{render_sub_status(sign_date_check.get('status'))}</td>
                    <td><div class="cell-truncate preview-truncate" title="{html.escape(note_text)}">{html.escape(note_text)}</div></td>
                </tr>
            """

        summary_html = "".join(f"<li>{html.escape(str(item))}</li>" for item in summary_items) or "<li>未返回摘要信息</li>"

        seal_company_status_html = ""
        if seal_company_check:
            company_tag_cls, company_tag_text = sub_status_map.get(
                str(seal_company_check.get("status") or ""),
                ("tag-missing", str(seal_company_check.get("status") or "未知")),
            )
            seal_company_status_html = f"""
            <div style="margin-top:15px; display:flex; justify-content:space-between; align-items:center; background:#f9fafc; border:1px solid #eee; padding:12px; border-radius:6px;">
                <div style="font-size:13px; color:var(--text-regular);">
                    公章与投标人匹配：{html.escape(str(seal_company_check.get('reason') or ''))}
                </div>
                <span class="status-tag {company_tag_cls}">{html.escape(company_tag_text)}</span>
            </div>
            """

        return f"""
        <div class="card">
            <h2 style="display:flex; justify-content:space-between;">
                <span>✍️ 签字盖章日期审查</span>
                <span class="status-tag {tag_cls}">{html.escape(tag_text)}</span>
            </h2>
            <div style="background:#fefefe; border:1px solid #eee; padding:15px; border-radius:8px; margin-bottom:15px; font-size:13px;">
                <ul style="margin:0; padding-left:20px; color:var(--text-regular);">
                    {summary_html}
                </ul>
            </div>
            {''.join(issue_panels)}
            {seal_company_status_html}
            <table class="it-table" style="margin-top:15px;">
                <thead>
                    <tr>
                        <th width="28%">附件</th>
                        <th width="18%">定位</th>
                        <th width="12%">签字</th>
                        <th width="12%">盖章</th>
                        <th width="12%">日期</th>
                        <th>识别说明</th>
                    </tr>
                </thead>
                <tbody>
                    {rows_html if rows_html else '<tr><td colspan="6" class="empty-text">未识别到需要核验的签字盖章附件</td></tr>'}
                </tbody>
            </table>
        </div>
        """

    def generate_html(self, integrity_report, consistency_report, test_segments, model_segments,
                      deviation_report=None, pricing_report=None, reasonableness_report=None,
                      verification_report=None, file_switcher_info=None,
                      detail_dir="details", detail_href_prefix=None,
                      document_preview_config=None):
        """
        生成全维度 HTML 报告，可选左侧悬浮文件切换菜单。

        Args:
            integrity_report (dict): 完整性检查报告。
            consistency_report (list): 一致性比对报告。
            test_segments (list): 投标文件分段列表，每项含 title, text。
            model_segments (list): 招标文件分段列表，每项含 title, text。
            deviation_report (dict, optional): 偏离条款审查报告。
            pricing_report (dict, optional): 分项报价逻辑审查报告。
            reasonableness_report (list, optional): 报价合理性审查报告列表。
            file_switcher_info (dict, optional): 左侧文件切换菜单配置，格式：
                {
                    'current_file': '当前文件名（仅用于展示）',
                    'files': [
                        {'name': '显示名称', 'url': '报告链接', 'active': True/False}
                    ]
                }

        Returns:
            str: 完整的 HTML 报告字符串。
        """
        # 预处理基础数据
        model_dict = {item['title']: item['text'] for item in model_segments}
        test_dict = {item['title']: item['text'] for item in test_segments}
        self._document_preview_config = document_preview_config or {"documents": {}}

        from .consistency import ConsistencyChecker
        checker = ConsistencyChecker()

        attachment_info_map = self._build_attachment_info(consistency_report, test_dict)
        attachment_mapping = integrity_report.get('attachment_mapping', {})

        # 生成各附加章节
        deviation_html = self._generate_deviation_section(deviation_report)
        pricing_html = self._generate_pricing_section(pricing_report)
        reasonableness_html = self._generate_reasonableness_section(reasonableness_report)
        verification_html = self._generate_verification_section(verification_report)

        # ---------- 左侧悬浮菜单 HTML 构建 ----------
        switcher_html = ""
        if file_switcher_info:
            files = file_switcher_info.get('files', [])
            items_html = []
            for f in files:
                active_class = 'active' if f.get('active') else ''
                items_html.append(f'''
                    <a class="switcher-item {active_class}" href="{f['url']}">
                        <span>📄 {f['name']}</span>
                        <span class="file-badge">{'当前' if f.get('active') else '切换'}</span>
                    </a>
                ''')
            switcher_html = f'''
                <div class="file-switcher">
                    <div class="switcher-header">
                        <span class="icon">📂</span> 投标文件切换
                    </div>
                    <ul class="switcher-list">
                        {''.join(items_html)}
                    </ul>
                </div>
                <div class="switcher-trigger"></div>
            '''

        # ---------- 主表 HTML 构建 ----------
        main_html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <title>标书全维度智能审查报告</title>
            {self.CSS_STYLE}
            {self._build_locator_support_style()}
            <!-- 追加悬浮菜单专用样式 -->
            <style>
                /* 左侧悬浮文件切换菜单 */
                .file-switcher {{
                    position: fixed;
                    top: 40%;
                    left: -260px;
                    transform: translateY(-50%);
                    z-index: 10000;
                    display: flex;
                    flex-direction: column;
                    width: 260px;
                    background: rgba(255, 255, 255, 0.95);
                    backdrop-filter: blur(8px);
                    border: 1px solid var(--border-color);
                    border-left: none;
                    border-radius: 0 12px 12px 0;
                    box-shadow: 4px 0 20px rgba(0,0,0,0.1);
                    transition: left 0.3s cubic-bezier(0.2, 0.9, 0.4, 1);
                    padding: 16px 0;
                }}
                .file-switcher:hover {{
                    left: 0;
                }}
                .switcher-header {{
                    padding: 8px 20px 12px;
                    font-weight: 600;
                    color: var(--text-main);
                    border-bottom: 1px solid var(--border-color);
                    margin-bottom: 8px;
                    display: flex;
                    align-items: center;
                    gap: 6px;
                }}
                .switcher-header .icon {{
                    font-size: 16px;
                }}
                .switcher-list {{
                    list-style: none;
                    margin: 0;
                    padding: 0;
                }}
                .switcher-item {{
                    padding: 10px 20px;
                    display: flex;
                    align-items: center;
                    gap: 10px;
                    color: var(--text-regular);
                    text-decoration: none;
                    transition: background 0.2s;
                    border-left: 3px solid transparent;
                }}
                .switcher-item:hover {{
                    background: #ecf5ff;
                    color: var(--primary-color);
                    border-left-color: var(--primary-color);
                }}
                .switcher-item.active {{
                    background: #e6f1fc;
                    color: var(--primary-color);
                    font-weight: 500;
                    border-left-color: var(--primary-color);
                }}
                .switcher-item .file-badge {{
                    margin-left: auto;
                    font-size: 11px;
                    background: #e0e0e0;
                    padding: 2px 6px;
                    border-radius: 10px;
                    color: #555;
                }}
                .switcher-trigger {{
                    position: fixed;
                    top: 40%;
                    left: 0;
                    transform: translateY(-50%);
                    z-index: 9999;
                    width: 8px;
                    height: 80px;
                    background: transparent;
                    pointer-events: none;
                }}
                @media (max-width: 768px) {{
                    .file-switcher {{ width: 220px; left: -220px; }}
                }}
            </style>
        </head>
        <body>
            {self._build_locator_support_markup()}
            {switcher_html}
            <div class="container">
                <div class="card header-card">
                    <h1>标书全维度智能审查报告</h1>
                </div>

                <div class="card">
                    <h2>🔍 完整性与格式一致性审查</h2>
                    <table class="it-table" id="report-table">
                        <thead>
                            <tr>
                                <th width="5%"></th>
                                <th width="26%">审查项目名称 (悬浮查看完整)</th>
                                <th width="12%">完整性状态</th>
                                <th width="15%">一致性详情</th>
                                <th width="18%">定位</th>
                                <th>内容定位预览</th>
                            </tr>
                        </thead>
                        <tbody>
        """

        # 创建详情目录
        details_dir = detail_dir or "details"
        detail_href_prefix = detail_href_prefix or str(details_dir).replace("\\", "/")
        os.makedirs(details_dir, exist_ok=True)

        # 生成主表各行
        combined_page_counter = 0
        items = list(integrity_report.get('details', {}).items())
        group_id = 0
        i = 0

        while i < len(items):
            name, info = items[i]
            # 跳过字母/数字子条目（它们将在父条目循环中作为子项处理）
            if re.match(r'^[A-Z][．\.]|^[\(（]\d+[\)）]', name):
                i += 1
                continue

            child_items = []
            j = i + 1
            while j < len(items):
                child_name, child_info = items[j]
                if re.match(r'^[A-Z][．\.]|^[\(（]\d+[\)）]', child_name):
                    child_items.append((child_name, child_info))
                    j += 1
                else:
                    break

            is_passed = info.get('is_passed', False)
            tag_cls = "tag-ok" if is_passed else "tag-err"
            display_status = f"✓ {info['status']}" if is_passed else f"⚠️ {info['status']}"
            seq_match = re.match(r'^([\d]+|[A-Z])[\.、]', name.strip())
            seq = seq_match.group(1) if seq_match else None

            # 构建一致性状态列
            consistency_html = ""
            infos = []
            if len(child_items) > 0:
                consistency_html = f'<span class="status-tag tag-warning consistency-toggle" data-group="{group_id}">⚠️ 查看子附件情况<span class="toggle-icon">▼</span></span>'
            else:
                attach_nums = attachment_mapping.get(seq, [])
                infos = [attachment_info_map[num] for num in attach_nums if num in attachment_info_map]
                if infos:
                    s_label, s_tag = self._get_status_for_single_item(infos)
                    if len(infos) == 1:
                        consistency_html = f'<a class="status-tag {s_tag}" href="{detail_href_prefix}/detail_{infos[0]["idx"]}.html">{s_label}</a>'
                    else:
                        path = f"{detail_href_prefix}/detail_combined_{combined_page_counter}.html"
                        self._generate_combined_detail_page(
                            infos,
                            os.path.join(details_dir, f"detail_combined_{combined_page_counter}.html"),
                            model_dict,
                            checker
                        )
                        consistency_html = f'<a class="status-tag {s_tag}" href="{path}">{s_label}</a>'
                        combined_page_counter += 1
                else:
                    consistency_html = '<span class="status-tag tag-missing">无附件模版</span>'

            main_pages = []
            main_locations = list(info.get("locations") or [])
            for attachment_info in infos:
                main_pages.extend(attachment_info.get("pages") or [])
                main_locations.extend(attachment_info.get("locations") or [])
            location_html = self._render_location_summary(locations=main_locations, pages=main_pages)

            # 主项行
            main_html += f"""
                                <tr class="main-item-row group-row" data-group-id="{group_id}">
                                    <td class="toggle-cell"></td>
                                    <td><div class="cell-truncate name-truncate" title="{html.escape(name)}">{html.escape(name)}</div></td>
                                    <td><span class="status-tag {tag_cls}">{display_status}</span></td>
                                    <td>{consistency_html}</td>
                                    <td>{location_html}</td>
                                    <td><div class="cell-truncate preview-truncate">{info.get('preview', '-')}</div></td>
                                </tr>
            """

            # 子项行
            for c_name, c_info in child_items:
                c_seq_match = re.match(r'^([A-Z])[\.、]', c_name.strip())
                c_seq = c_seq_match.group(1) if c_seq_match else None
                c_attach_nums = attachment_mapping.get(c_seq, [])
                c_infos = [attachment_info_map[num] for num in c_attach_nums if num in attachment_info_map]
                c_consistency = '<span class="status-tag tag-missing">无附件模版</span>'
                if c_infos:
                    sl, st = self._get_status_for_single_item(c_infos)
                    if len(c_infos) == 1:
                        c_consistency = f'<a class="status-tag {st}" href="{detail_href_prefix}/detail_{c_infos[0]["idx"]}.html">{sl}</a>'
                    else:
                        c_path = f"{detail_href_prefix}/detail_combined_{combined_page_counter}.html"
                        self._generate_combined_detail_page(
                            c_infos,
                            os.path.join(details_dir, f"detail_combined_{combined_page_counter}.html"),
                            model_dict,
                            checker
                        )
                        c_consistency = f'<a class="status-tag {st}" href="{c_path}">{sl}</a>'
                        combined_page_counter += 1

                c_pages = []
                c_locations = list(c_info.get("locations") or [])
                for attachment_info in c_infos:
                    c_pages.extend(attachment_info.get("pages") or [])
                    c_locations.extend(attachment_info.get("locations") or [])
                c_location_html = self._render_location_summary(locations=c_locations, pages=c_pages)

                main_html += f"""
                                <tr class="sub-item-row child-row" data-parent="{group_id}">
                                    <td></td>
                                    <td><div class="cell-truncate name-truncate" title="{html.escape(c_name)}">{html.escape(c_name)}</div></td>
                                    <td><span class="status-tag {"tag-ok" if c_info.get("is_passed") else "tag-err"}">{c_info['status']}</span></td>
                                    <td>{c_consistency}</td>
                                    <td>{c_location_html}</td>
                                    <td><div class="cell-truncate preview-truncate">{c_info.get('preview', '-')}</div></td>
                                </tr>
                """
            group_id += 1
            i = j

        # 生成单个附件详情页
        for idx, rec in enumerate(consistency_report):
            title, missing = rec['name'], rec.get('missing_anchors', [])
            b_text, m_text = test_dict.get(title, ""), model_dict.get(title, "")
            card_html = self._generate_detail_card(
                title,
                missing,
                b_text,
                m_text,
                checker,
                pages=rec.get("pages"),
                locations=rec.get("locations"),
            )
            detail_html = f"""
            <!DOCTYPE html>
            <html><head><meta charset="UTF-8"><title>审查详情 - {html.escape(title)}</title>{self.CSS_STYLE}{self._build_locator_support_style()}</head>
            <body>{self._build_locator_support_markup()}<a href="javascript:void(0);" onclick="window.history.back();" class="back-link">← 返回总览</a>
            <div class="container"><div style="height: 40px;"></div><div class="legend-card"><strong>💡 阅读指引：</strong><span>浅蓝底纹=匹配成功</span><span>缺=缺失模版内容</span></div>{card_html}</div>
            <script>document.querySelectorAll('.expand-btn').forEach(btn => {{ btn.onclick = function() {{ const content = document.getElementById('content-' + this.dataset.uid); const isCol = content.classList.contains('content-collapsed'); content.classList.toggle('content-collapsed', !isCol); content.classList.toggle('content-expanded', isCol); this.querySelector('.expand-text').textContent = isCol ? '收起' : '显示全部'; this.querySelector('.expand-icon').textContent = isCol ? '▲' : '▼'; }}; }});if(window.__bindLocatorChips){{window.__bindLocatorChips();}}</script>
            {self._build_locator_support_script()}
            </body></html>
            """
            with open(os.path.join(details_dir, f"detail_{idx}.html"), "w", encoding="utf-8") as f:
                f.write(detail_html)

        # 收尾 HTML
        main_html += f"""
                            </tbody>
                        </table>
                    </div>

                    {deviation_html}
                    {pricing_html}
                    {reasonableness_html}
                    {verification_html}

                </div>

                <script>
                    document.querySelectorAll('.consistency-toggle').forEach(t => {{
                        t.addEventListener('click', () => {{
                            const gid = t.dataset.group;
                            const icon = t.querySelector('.toggle-icon');
                            const isExpanded = icon.textContent === '▲';
                            icon.textContent = isExpanded ? '▼' : '▲';
                            document.querySelectorAll(`tr[data-parent="${{gid}}"]`).forEach(r => {{ r.classList.toggle('visible'); }});
                        }});
                    }});
                </script>
                {self._build_locator_support_script()}
            </body>
            </html>
        """
        return main_html

    def _project_role_label(self, role):
        mapping = {
            DOCUMENT_TYPE_BUSINESS_BID: "商务标",
            DOCUMENT_TYPE_TECHNICAL_BID: "技术标",
        }
        return mapping.get(role, str(role or "-"))

    def _project_normalize_pages(self, *values):
        pages = set()

        def visit(value):
            if value is None:
                return
            if isinstance(value, int):
                if value > 0:
                    pages.add(value)
                return
            if isinstance(value, (list, tuple, set)):
                for item in value:
                    visit(item)
                return
            if isinstance(value, str):
                for token in re.findall(r"\d+", value):
                    page = int(token)
                    if page > 0:
                        pages.add(page)

        for current in values:
            visit(current)
        return sorted(pages)

    def _project_make_stable_token(self, *parts):
        joined = "|".join(str(part) for part in parts)
        return hashlib.sha1(joined.encode("utf-8")).hexdigest()[:12]

    def _project_append_page_fragment(self, url, page=None):
        if not url:
            return ""
        base = str(url).split("#", 1)[0]
        if isinstance(page, int) and page > 0:
            return f"{base}#page={page}"
        return base

    def _project_severity_css_class(self, value):
        normalized = str(value or "").strip().lower()
        if normalized in {"high", "critical", "error", "严重"}:
            return "issue-row issue-severity-high"
        if normalized in {"medium", "warning", "warn", "中", "中等"}:
            return "issue-row issue-severity-medium"
        return "issue-row issue-severity-low"

    def _project_build_issue_detail_link(self, href, label):
        if not href:
            return "<span class='issue-muted'>-</span>"
        return (
            f"<a class='issue-link issue-detail-link' href='{html.escape(str(href))}' "
            f"target='_blank' rel='noreferrer'>{html.escape(str(label))}</a>"
        )

    def _project_build_source_file_link_html(self, source_lookup, file_name, *, label=None, page=None):
        entry = source_lookup.get(file_name) or {}
        display_name = str(label or entry.get("display_name") or file_name or "-")
        source_url = str(entry.get("source_url") or "")
        if not source_url:
            return f"<span>{html.escape(display_name)}</span>"
        href = self._project_append_page_fragment(source_url, page)
        return (
            f"<a class='issue-link issue-file-link' href='{html.escape(href)}' "
            f"target='_blank' rel='noreferrer'>{html.escape(display_name)}</a>"
        )

    def _project_build_source_page_links_html(self, source_lookup, file_name, pages):
        normalized_pages = self._project_normalize_pages(pages)
        if not normalized_pages:
            return "<span class='issue-muted'>页码待补充</span>"

        entry = source_lookup.get(file_name) or {}
        source_url = str(entry.get("source_url") or "")
        fragments = []
        for start_page, end_page in self._coalesce_page_ranges(normalized_pages):
            label = f"P{start_page}" if start_page == end_page else f"P{start_page}-P{end_page}"
            if source_url:
                href = self._project_append_page_fragment(source_url, start_page)
                fragments.append(
                    f"<a class='issue-link issue-page-link' href='{html.escape(href)}' "
                    f"target='_blank' rel='noreferrer'>{html.escape(label)}</a>"
                )
            else:
                fragments.append(f"<span>{html.escape(label)}</span>")
        return "<span class='issue-page-links'>" + " ".join(fragments) + "</span>"

    def _project_build_source_doc_cell_html(self, source_lookup, file_name, pages):
        entry = source_lookup.get(file_name) or {}
        display_name = str(entry.get("display_name") or file_name or "-")
        json_name = str(entry.get("json_name") or file_name or "")
        normalized_pages = self._project_normalize_pages(pages)
        first_page = normalized_pages[0] if normalized_pages else None
        parts = [
            "<div class='issue-doc-cell'>",
            "<div>",
            self._project_build_source_file_link_html(
                source_lookup,
                file_name,
                label=display_name,
                page=first_page,
            ),
            "</div>",
        ]
        if json_name and json_name != display_name:
            parts.append(f"<div class='issue-subtext'>{html.escape(json_name)}</div>")
        parts.append("<div>")
        parts.append(self._project_build_source_page_links_html(source_lookup, file_name, normalized_pages))
        parts.append("</div></div>")
        return "".join(parts)

    def _project_trim_text(self, value, limit=140):
        text = re.sub(r"\s+", " ", str(value or "")).strip()
        if len(text) <= limit:
            return text
        return text[: limit - 1].rstrip() + "…"

    def _project_highlight_text_html(self, text, needle):
        raw_text = str(text or "-")
        keyword = str(needle or "").strip()
        if not keyword:
            return html.escape(raw_text)
        index = raw_text.find(keyword)
        if index < 0:
            return html.escape(raw_text)
        before = raw_text[:index]
        matched = raw_text[index:index + len(keyword)]
        after = raw_text[index + len(keyword):]
        return f"{html.escape(before)}<mark>{html.escape(matched)}</mark>{html.escape(after)}"

    def _project_duplicate_pair_anchor(self, doc_type, item):
        return (
            f"duplicate-{doc_type}-"
            f"{self._project_make_stable_token(item.get('left_file_name'), item.get('right_file_name'))}"
        )

    def _project_typo_issue_anchor(self, role, file_name, item):
        return (
            "typo-"
            + self._project_make_stable_token(
                role,
                file_name,
                item.get("page"),
                item.get("matched_text"),
                item.get("suggestion"),
            )
        )

    def _project_personnel_issue_anchor(self, role, item):
        occurrence_keys = [
            f"{entry.get('file_name')}:{entry.get('page')}:{entry.get('name')}"
            for entry in (item.get("items") or [])
        ]
        return "personnel-" + self._project_make_stable_token(
            role,
            item.get("name"),
            *sorted(occurrence_keys),
        )

    def _project_issue_page_href(self, page, anchor=None):
        if not page:
            return ""
        return f"{page}#{anchor}" if anchor else str(page)

    def _project_collect_duplicate_pages(self, item, side):
        collected = []
        page_key = f"{side}_page"
        pages_key = f"{side}_pages"
        for section in item.get("duplicate_sections") or []:
            collected.extend(self._project_normalize_pages(section.get(pages_key)))
        for table in item.get("duplicate_tables") or []:
            collected.extend(self._project_normalize_pages(table.get(pages_key)))
        for block in item.get("duplicate_blocks") or []:
            collected.extend(self._project_normalize_pages(block.get(page_key), block.get("page")))
        return sorted(set(collected))

    def _project_is_duplicate_issue(self, item):
        if bool(item.get("suspicious")) or bool(item.get("exact_duplicate")):
            return True
        risk_level = str(item.get("risk_level") or "").strip().lower()
        if risk_level and risk_level not in {"none", "low", "-"}:
            return True
        try:
            score = float(item.get("exact_match_score") or 0)
        except (TypeError, ValueError):
            score = 0.0
        return score >= 0.8

    def _project_iter_duplicate_items(self, result, doc_type, *, current_files=None):
        group = ((result.get("groups") or {}).get(doc_type) or {})
        items = [
            item
            for item in (group.get("items") or [])
            if self._project_is_duplicate_issue(item)
        ]
        if not current_files:
            return items
        return [
            item
            for item in items
            if str(item.get("left_file_name") or "") in current_files
            or str(item.get("right_file_name") or "") in current_files
        ]

    def _project_iter_typo_items(self, result, *, current_files=None):
        rows = []
        for role, group in (result.get("groups") or {}).items():
            typo = group.get("typo_check") or {}
            for document in typo.get("documents") or []:
                file_name = str(document.get("file_name") or "")
                if current_files and file_name not in current_files:
                    continue
                for item in document.get("items") or []:
                    rows.append((str(role), file_name, item))
        return rows

    def _project_iter_personnel_reuse_items(self, result, *, current_files=None):
        rows = []
        for role, group in (result.get("groups") or {}).items():
            reuse = group.get("personnel_reuse_check") or {}
            for item in (reuse.get("items") or reuse.get("reused_names") or []):
                related_files = {
                    str(entry.get("file_name") or "")
                    for entry in (item.get("items") or [])
                }
                if current_files and not (related_files & current_files):
                    continue
                rows.append((str(role), item))
        return rows

    def _project_duplicate_score(self, item):
        value = item.get("match_score")
        if value is None:
            value = item.get("exact_match_score")
        try:
            return f"{float(value or 0):.4f}".rstrip("0").rstrip(".")
        except (TypeError, ValueError):
            return str(value or "0")

    def _project_metric_display(self, metrics, exact_key, similar_key):
        exact_value = int(metrics.get(exact_key) or 0)
        similar_value = int(metrics.get(similar_key) or 0)
        if similar_value > 0:
            return f"{exact_value} / 相似{similar_value}"
        return str(exact_value)

    def _project_render_duplicate_rows(
        self,
        result,
        doc_type,
        *,
        source_lookup,
        issue_pages,
        current_files=None,
    ):
        items = list(self._project_iter_duplicate_items(result, doc_type, current_files=current_files))
        if not items:
            return "<tr><td colspan='8'>未发现相关可疑对</td></tr>"

        page_key = "business_duplicates" if doc_type == DOCUMENT_TYPE_BUSINESS_BID else "technical_duplicates"
        detail_page = issue_pages.get(page_key, "")
        rows = []
        for item in items:
            metrics = item.get("metrics") or {}
            risk_level = str(item.get("risk_level") or "-")
            left_file = str(item.get("left_file_name") or "")
            right_file = str(item.get("right_file_name") or "")
            left_pages = self._project_collect_duplicate_pages(item, "left")
            right_pages = self._project_collect_duplicate_pages(item, "right")
            detail_anchor = self._project_duplicate_pair_anchor(doc_type, item)
            detail_href = self._project_issue_page_href(detail_page, detail_anchor)
            rows.append(
                f"<tr class='{self._project_severity_css_class(risk_level)}'>"
                f"<td>{html.escape(risk_level)}</td>"
                f"<td>{html.escape(self._project_duplicate_score(item))}</td>"
                f"<td>{self._project_build_source_doc_cell_html(source_lookup, left_file, left_pages)}</td>"
                f"<td>{self._project_build_source_doc_cell_html(source_lookup, right_file, right_pages)}</td>"
                f"<td>{html.escape(self._project_metric_display(metrics, 'exact_section_count', 'similar_section_count'))}</td>"
                f"<td>{html.escape(self._project_metric_display(metrics, 'exact_block_count', 'similar_block_count'))}</td>"
                f"<td>{html.escape(self._project_metric_display(metrics, 'exact_table_count', 'similar_table_count'))}</td>"
                f"<td>{self._project_build_issue_detail_link(detail_href, '查看全部证据')}</td>"
                "</tr>"
            )
        return "\n".join(rows)

    def _project_render_typo_rows(
        self,
        result,
        *,
        source_lookup,
        issue_pages,
        current_files=None,
    ):
        items = list(self._project_iter_typo_items(result, current_files=current_files))
        if not items:
            return "<tr><td colspan='7'>未发现错别字候选</td></tr>"

        detail_page = issue_pages.get("typos", "")
        rows = []
        for role, file_name, item in items:
            pages = self._project_normalize_pages(item.get("page"))
            detail_anchor = self._project_typo_issue_anchor(role, file_name, item)
            detail_href = self._project_issue_page_href(detail_page, detail_anchor)
            rows.append(
                f"<tr class='{self._project_severity_css_class('warning')}'>"
                f"<td>{html.escape(self._project_role_label(role))}</td>"
                f"<td>{self._project_build_source_doc_cell_html(source_lookup, file_name, pages)}</td>"
                f"<td>{self._project_build_source_page_links_html(source_lookup, file_name, pages)}</td>"
                f"<td><mark>{html.escape(str(item.get('matched_text') or '-'))}</mark></td>"
                f"<td>{html.escape(str(item.get('suggestion') or '-'))}</td>"
                f"<td>{self._project_highlight_text_html(str(item.get('text') or '-'), str(item.get('matched_text') or ''))}</td>"
                f"<td>{self._project_build_issue_detail_link(detail_href, '查看问题页')}</td>"
                "</tr>"
            )
        return "\n".join(rows)

    def _project_render_personnel_rows(
        self,
        result,
        *,
        source_lookup,
        issue_pages,
        current_files=None,
    ):
        items = list(self._project_iter_personnel_reuse_items(result, current_files=current_files))
        if not items:
            return "<tr><td colspan='6'>未发现一人多用</td></tr>"

        detail_page = issue_pages.get("personnel", "")
        rows = []
        for role, item in items:
            detail_anchor = self._project_personnel_issue_anchor(role, item)
            detail_href = self._project_issue_page_href(detail_page, detail_anchor)
            occurrences = list(item.get("items") or [])
            occurrence_html = "<br>".join(
                self._project_build_source_doc_cell_html(
                    source_lookup,
                    str(entry.get("file_name") or ""),
                    self._project_normalize_pages(entry.get("page")),
                )
                for entry in occurrences
            ) or "-"
            rows.append(
                f"<tr class='{self._project_severity_css_class(str(item.get('risk_level') or 'warning'))}'>"
                f"<td>{html.escape(self._project_role_label(role))}</td>"
                f"<td><mark>{html.escape(str(item.get('name') or '-'))}</mark></td>"
                f"<td>{html.escape(str(item.get('risk_level') or '-'))}</td>"
                f"<td>{html.escape(str(item.get('document_count') or 0))}</td>"
                f"<td>{occurrence_html}</td>"
                f"<td>{self._project_build_issue_detail_link(detail_href, '查看问题页')}</td>"
                "</tr>"
            )
        return "\n".join(rows)

    def _project_render_duplicate_evidence_sections(self, item, *, source_lookup):
        left_file = str(item.get("left_file_name") or "")
        right_file = str(item.get("right_file_name") or "")
        blocks = item.get("duplicate_blocks") or []
        sections = item.get("duplicate_sections") or []
        tables = item.get("duplicate_tables") or []
        similar_blocks = item.get("similar_blocks") or []
        similar_sections = item.get("similar_sections") or []
        similar_tables = item.get("similar_tables") or []
        parts = []

        if sections:
            entries = []
            for section in sections:
                left_pages = self._project_normalize_pages(section.get("left_pages"))
                right_pages = self._project_normalize_pages(section.get("right_pages"))
                entries.append(
                    "<li>"
                    f"<div><strong>文件A：</strong>{self._project_build_source_page_links_html(source_lookup, left_file, left_pages)}</div>"
                    f"<div class='issue-preview'>{html.escape(self._project_trim_text(str(section.get('left_preview') or section.get('left_title') or '-')))}</div>"
                    f"<div><strong>文件B：</strong>{self._project_build_source_page_links_html(source_lookup, right_file, right_pages)}</div>"
                    f"<div class='issue-preview'>{html.escape(self._project_trim_text(str(section.get('right_preview') or section.get('right_title') or '-')))}</div>"
                    "</li>"
                )
            parts.append(
                f"<details open><summary>重复段落证据（{len(sections)}）</summary>"
                f"<ul class='issue-evidence-list'>{''.join(entries)}</ul></details>"
            )

        if blocks:
            entries = []
            for block in blocks:
                left_pages = self._project_normalize_pages(block.get("left_page"), block.get("page"))
                right_pages = self._project_normalize_pages(block.get("right_page"), block.get("page"))
                entries.append(
                    "<li>"
                    f"<div><strong>文件A：</strong>{self._project_build_source_page_links_html(source_lookup, left_file, left_pages)}</div>"
                    f"<div><strong>文件B：</strong>{self._project_build_source_page_links_html(source_lookup, right_file, right_pages)}</div>"
                    f"<div class='issue-preview'><mark>{html.escape(self._project_trim_text(str(block.get('text') or '-')))}</mark></div>"
                    "</li>"
                )
            parts.append(
                f"<details open><summary>重复句证据（{len(blocks)}）</summary>"
                f"<ul class='issue-evidence-list'>{''.join(entries)}</ul></details>"
            )

        if tables:
            entries = []
            for table in tables:
                left_pages = self._project_normalize_pages(table.get("left_pages"))
                right_pages = self._project_normalize_pages(table.get("right_pages"))
                sample_rows = table.get("sample_rows") or []
                sample_text = self._project_trim_text(
                    json.dumps(sample_rows, ensure_ascii=False),
                    220,
                ) if sample_rows else "-"
                entries.append(
                    "<li>"
                    f"<div><strong>文件A：</strong>{self._project_build_source_page_links_html(source_lookup, left_file, left_pages)}</div>"
                    f"<div><strong>文件B：</strong>{self._project_build_source_page_links_html(source_lookup, right_file, right_pages)}</div>"
                    f"<div class='issue-preview'>{html.escape(sample_text)}</div>"
                    "</li>"
                )
            parts.append(
                f"<details open><summary>重复表格证据（{len(tables)}）</summary>"
                f"<ul class='issue-evidence-list'>{''.join(entries)}</ul></details>"
            )

        if similar_sections:
            entries = []
            for section in similar_sections:
                left_pages = self._project_normalize_pages(section.get("left_pages"))
                right_pages = self._project_normalize_pages(section.get("right_pages"))
                similarity = html.escape(str(section.get("similarity") or 0))
                entries.append(
                    "<li>"
                    f"<div><strong>文件A：</strong>{self._project_build_source_page_links_html(source_lookup, left_file, left_pages)}</div>"
                    f"<div class='issue-preview'>{html.escape(self._project_trim_text(str(section.get('left_preview') or section.get('left_title') or '-')))}</div>"
                    f"<div><strong>文件B：</strong>{self._project_build_source_page_links_html(source_lookup, right_file, right_pages)}</div>"
                    f"<div class='issue-preview'>{html.escape(self._project_trim_text(str(section.get('right_preview') or section.get('right_title') or '-')))}</div>"
                    f"<div class='issue-preview issue-muted'>相似度：{similarity}</div>"
                    "</li>"
                )
            parts.append(
                f"<details open><summary>相似段落证据（{len(similar_sections)}）</summary>"
                f"<ul class='issue-evidence-list'>{''.join(entries)}</ul></details>"
            )

        if similar_blocks:
            entries = []
            for block in similar_blocks:
                left_pages = self._project_normalize_pages(block.get("left_page"), block.get("page"))
                right_pages = self._project_normalize_pages(block.get("right_page"), block.get("page"))
                similarity = html.escape(str(block.get("similarity") or 0))
                entries.append(
                    "<li>"
                    f"<div><strong>文件A：</strong>{self._project_build_source_page_links_html(source_lookup, left_file, left_pages)}</div>"
                    f"<div class='issue-preview'>{html.escape(self._project_trim_text(str(block.get('left_text') or '-')))}</div>"
                    f"<div><strong>文件B：</strong>{self._project_build_source_page_links_html(source_lookup, right_file, right_pages)}</div>"
                    f"<div class='issue-preview'>{html.escape(self._project_trim_text(str(block.get('right_text') or '-')))}</div>"
                    f"<div class='issue-preview issue-muted'>相似度：{similarity}</div>"
                    "</li>"
                )
            parts.append(
                f"<details open><summary>相似句证据（{len(similar_blocks)}）</summary>"
                f"<ul class='issue-evidence-list'>{''.join(entries)}</ul></details>"
            )

        if similar_tables:
            entries = []
            for table in similar_tables:
                left_pages = self._project_normalize_pages(table.get("left_pages"))
                right_pages = self._project_normalize_pages(table.get("right_pages"))
                similarity = html.escape(str(table.get("similarity") or 0))
                left_rows = self._project_trim_text(json.dumps(table.get("left_sample_rows") or [], ensure_ascii=False), 220)
                right_rows = self._project_trim_text(json.dumps(table.get("right_sample_rows") or [], ensure_ascii=False), 220)
                entries.append(
                    "<li>"
                    f"<div><strong>文件A：</strong>{self._project_build_source_page_links_html(source_lookup, left_file, left_pages)}</div>"
                    f"<div class='issue-preview'>{html.escape(left_rows)}</div>"
                    f"<div><strong>文件B：</strong>{self._project_build_source_page_links_html(source_lookup, right_file, right_pages)}</div>"
                    f"<div class='issue-preview'>{html.escape(right_rows)}</div>"
                    f"<div class='issue-preview issue-muted'>相似度：{similarity}</div>"
                    "</li>"
                )
            parts.append(
                f"<details open><summary>相似表格证据（{len(similar_tables)}）</summary>"
                f"<ul class='issue-evidence-list'>{''.join(entries)}</ul></details>"
            )

        return "".join(parts) or "<p class='issue-muted'>当前未返回更细的重复证据。</p>"

    def _project_build_duplicate_issue_detail_html(
        self,
        *,
        project_identifier,
        title,
        result,
        doc_type,
        source_lookup,
    ):
        items = list(self._project_iter_duplicate_items(result, doc_type))
        if not items:
            body = "<p class='issue-empty'>未发现相关可疑对。</p>"
        else:
            cards = []
            for item in items:
                metrics = item.get("metrics") or {}
                risk_level = str(item.get("risk_level") or "-")
                left_file = str(item.get("left_file_name") or "")
                right_file = str(item.get("right_file_name") or "")
                left_pages = self._project_collect_duplicate_pages(item, "left")
                right_pages = self._project_collect_duplicate_pages(item, "right")
                cards.append(
                    f"""
                    <article id="{html.escape(self._project_duplicate_pair_anchor(doc_type, item))}" class="issue-card {self._project_severity_css_class(risk_level)}">
                      <div class="issue-card-header">
                        <div>
                          <h2>{html.escape(left_file)} &lt;&gt; {html.escape(right_file)}</h2>
                          <p class="issue-meta">风险：{html.escape(risk_level)} ｜ 分数：{html.escape(self._project_duplicate_score(item))}</p>
                        </div>
                        <div class="issue-metrics">
                          <span>重复段 {html.escape(self._project_metric_display(metrics, 'exact_section_count', 'similar_section_count'))}</span>
                          <span>重复句 {html.escape(self._project_metric_display(metrics, 'exact_block_count', 'similar_block_count'))}</span>
                          <span>重复表 {html.escape(self._project_metric_display(metrics, 'exact_table_count', 'similar_table_count'))}</span>
                        </div>
                      </div>
                      <div class="issue-doc-grid">
                        <div>{self._project_build_source_doc_cell_html(source_lookup, left_file, left_pages)}</div>
                        <div>{self._project_build_source_doc_cell_html(source_lookup, right_file, right_pages)}</div>
                      </div>
                      {self._project_render_duplicate_evidence_sections(item, source_lookup=source_lookup)}
                    </article>
                    """
                )
            body = "".join(cards)
        return self._project_build_issue_page_shell(
            project_identifier=project_identifier,
            title=title,
            body=body,
        )

    def _project_build_typo_issue_detail_html(self, *, project_identifier, result, source_lookup):
        items = list(self._project_iter_typo_items(result))
        if not items:
            body = "<p class='issue-empty'>未发现错别字候选。</p>"
        else:
            cards = []
            for role, file_name, item in items:
                pages = self._project_normalize_pages(item.get("page"))
                cards.append(
                    f"""
                    <article id="{html.escape(self._project_typo_issue_anchor(role, file_name, item))}" class="issue-card issue-row issue-severity-medium">
                      <div class="issue-card-header">
                        <div>
                          <h2><mark>{html.escape(str(item.get("matched_text") or "-"))}</mark> -&gt; {html.escape(str(item.get("suggestion") or "-"))}</h2>
                          <p class="issue-meta">{html.escape(self._project_role_label(role))}</p>
                        </div>
                      </div>
                      <div>{self._project_build_source_doc_cell_html(source_lookup, file_name, pages)}</div>
                      <div class="issue-preview issue-block">{self._project_highlight_text_html(str(item.get("text") or "-"), str(item.get("matched_text") or ""))}</div>
                    </article>
                    """
                )
            body = "".join(cards)
        return self._project_build_issue_page_shell(
            project_identifier=project_identifier,
            title="错别字问题总览",
            body=body,
        )

    def _project_build_personnel_issue_detail_html(self, *, project_identifier, result, source_lookup):
        items = list(self._project_iter_personnel_reuse_items(result))
        if not items:
            body = "<p class='issue-empty'>未发现一人多用。</p>"
        else:
            cards = []
            for role, item in items:
                occurrences = []
                for entry in item.get("items") or []:
                    file_name = str(entry.get("file_name") or "")
                    pages = self._project_normalize_pages(entry.get("page"))
                    occurrences.append(
                        "<li>"
                        f"{self._project_build_source_doc_cell_html(source_lookup, file_name, pages)}"
                        f"<div class='issue-preview'>{html.escape(self._project_trim_text(str(entry.get('text') or '-')))}</div>"
                        "</li>"
                    )
                cards.append(
                    f"""
                    <article id="{html.escape(self._project_personnel_issue_anchor(role, item))}" class="issue-card {self._project_severity_css_class(str(item.get('risk_level') or 'warning'))}">
                      <div class="issue-card-header">
                        <div>
                          <h2><mark>{html.escape(str(item.get("name") or "-"))}</mark></h2>
                          <p class="issue-meta">{html.escape(self._project_role_label(role))} ｜ 风险：{html.escape(str(item.get("risk_level") or "-"))} ｜ 涉及文件：{html.escape(str(item.get("document_count") or 0))}</p>
                        </div>
                      </div>
                      <ul class="issue-evidence-list">{''.join(occurrences)}</ul>
                    </article>
                    """
                )
            body = "".join(cards)
        return self._project_build_issue_page_shell(
            project_identifier=project_identifier,
            title="一人多用问题总览",
            body=body,
        )

    def _project_build_issue_page_shell(self, *, project_identifier, title, body):
        return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{html.escape(str(title))}</title>
  <style>
    body {{ margin: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; background: #f6f7f4; color: #1c1c1c; }}
    main {{ max-width: 1320px; margin: 0 auto; padding: 28px 32px 40px; }}
    h1 {{ margin: 0 0 8px; font-size: 28px; }}
    .meta {{ color: #5f6862; margin-bottom: 18px; }}
    .toolbar {{ display: flex; gap: 12px; flex-wrap: wrap; margin-bottom: 18px; }}
    .toolbar a {{ color: #0b57d0; text-decoration: none; font-weight: 600; }}
    .issue-card {{ background: #fff; border: 1px solid #d9ded8; border-radius: 10px; padding: 16px 18px; margin-bottom: 16px; }}
    .issue-card-header {{ display: flex; justify-content: space-between; gap: 16px; align-items: flex-start; }}
    .issue-card h2 {{ margin: 0 0 6px; font-size: 20px; }}
    .issue-meta {{ margin: 0; color: #5f6862; }}
    .issue-doc-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 12px; margin: 14px 0; }}
    .issue-doc-cell {{ background: #f8faf7; border: 1px solid #d9ded8; border-radius: 8px; padding: 10px 12px; }}
    .issue-subtext {{ margin-top: 4px; color: #5f6862; font-size: 12px; }}
    .issue-link {{ color: #0b57d0; text-decoration: none; }}
    .issue-link:hover {{ text-decoration: underline; }}
    .issue-page-links {{ display: inline-flex; gap: 8px; flex-wrap: wrap; margin-top: 8px; }}
    .issue-evidence-list {{ margin: 12px 0 0; padding-left: 18px; }}
    .issue-evidence-list li + li {{ margin-top: 12px; }}
    .issue-preview {{ margin-top: 8px; line-height: 1.6; }}
    .issue-block {{ background: #fff7dd; border-radius: 8px; padding: 10px 12px; }}
    .issue-metrics {{ display: flex; flex-wrap: wrap; gap: 10px; color: #5f6862; font-size: 13px; }}
    .issue-empty, .issue-muted {{ color: #5f6862; }}
    .issue-row.issue-severity-high {{ border-left: 6px solid #c62828; background: #fff5f5; }}
    .issue-row.issue-severity-medium {{ border-left: 6px solid #f9a825; background: #fff8e1; }}
    .issue-row.issue-severity-low {{ border-left: 6px solid #2e7d32; background: #f6fbf6; }}
    mark {{ background: #ffe082; padding: 0 2px; }}
    details {{ margin-top: 12px; }}
    summary {{ cursor: pointer; font-weight: 700; }}
  </style>
</head>
<body>
  <main>
    <h1>{html.escape(str(title))}</h1>
    <div class="meta">{html.escape(str(project_identifier))}</div>
    <div class="toolbar">
      <a href="project_review_summary.html">返回项目总览</a>
    </div>
    {body}
  </main>
</body>
</html>"""

    def write_project_issue_pages(
        self,
        *,
        output_dir,
        project_identifier,
        project_results,
        source_lookup,
    ):
        page_map = {
            "business_duplicates": "project_issue_business_duplicates.html",
            "technical_duplicates": "project_issue_technical_duplicates.html",
            "typos": "project_issue_typos.html",
            "personnel": "project_issue_personnel.html",
        }
        contents = {
            "business_duplicates": self._project_build_duplicate_issue_detail_html(
                project_identifier=project_identifier,
                title="商务标查重问题总览",
                result=project_results.get("business_duplicate_check") or {},
                doc_type=DOCUMENT_TYPE_BUSINESS_BID,
                source_lookup=source_lookup,
            ),
            "technical_duplicates": self._project_build_duplicate_issue_detail_html(
                project_identifier=project_identifier,
                title="技术标查重问题总览",
                result=project_results.get("technical_duplicate_check") or {},
                doc_type=DOCUMENT_TYPE_TECHNICAL_BID,
                source_lookup=source_lookup,
            ),
            "typos": self._project_build_typo_issue_detail_html(
                project_identifier=project_identifier,
                result=project_results.get("bid_document_review") or {},
                source_lookup=source_lookup,
            ),
            "personnel": self._project_build_personnel_issue_detail_html(
                project_identifier=project_identifier,
                result=project_results.get("bid_document_review") or {},
                source_lookup=source_lookup,
            ),
        }
        for key, file_name in page_map.items():
            path = output_dir / file_name
            path.write_text(contents[key], encoding="utf-8")
        return page_map

    def build_project_review_section(
        self,
        project_results,
        *,
        source_lookup,
        issue_pages,
        current_business_file=None,
    ):
        business_duplicate = project_results.get("business_duplicate_check") or {}
        technical_duplicate = project_results.get("technical_duplicate_check") or {}
        bid_review = project_results.get("bid_document_review") or {}
        bid_summary = bid_review.get("summary") or {}
        business_summary = business_duplicate.get("summary") or {}
        technical_summary = technical_duplicate.get("summary") or {}
        current_files = {current_business_file} if current_business_file else None
        scope_text = current_business_file or "全项目"

        return f"""
        <style>
          .project-review-addon {{
            margin: 32px auto 0;
            padding: 24px;
            border: 1px solid #d9ded8;
            border-radius: 12px;
            background: #fbfcfa;
          }}
          .project-review-addon h2 {{
            margin: 0 0 10px;
          }}
          .project-review-addon h3 {{
            margin: 26px 0 10px;
          }}
          .project-review-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
            gap: 12px;
          }}
          .project-review-card {{
            background: #fff;
            border: 1px solid #d9ded8;
            border-radius: 8px;
            padding: 12px 14px;
          }}
          .project-review-card .label {{
            color: #5f6862;
            font-size: 12px;
          }}
          .project-review-card .value {{
            margin-top: 6px;
            font-size: 22px;
            font-weight: 700;
          }}
          .project-review-scope {{
            margin: 10px 0 0;
            color: #5f6862;
            font-size: 13px;
          }}
          .project-review-addon table {{
            width: 100%;
            border-collapse: collapse;
            background: #fff;
            border: 1px solid #d9ded8;
          }}
          .project-review-addon th,
          .project-review-addon td {{
            padding: 9px 10px;
            border-bottom: 1px solid #e6e8e4;
            text-align: left;
            vertical-align: top;
            font-size: 13px;
          }}
          .project-review-addon th {{
            background: #eef1ed;
            font-weight: 700;
          }}
          .project-review-addon .project-review-links {{
            display: flex;
            gap: 12px;
            flex-wrap: wrap;
            margin: 16px 0 4px;
          }}
          .project-review-addon .project-review-links a,
          .project-review-addon .issue-link {{
            color: #0b57d0;
            text-decoration: none;
          }}
          .project-review-addon .project-review-links a:hover,
          .project-review-addon .issue-link:hover {{
            text-decoration: underline;
          }}
          .project-review-addon .issue-doc-cell {{
            min-width: 220px;
          }}
          .project-review-addon .issue-page-links {{
            display: inline-flex;
            gap: 8px;
            flex-wrap: wrap;
            margin-top: 8px;
          }}
          .project-review-addon .issue-subtext {{
            margin-top: 4px;
            color: #5f6862;
            font-size: 12px;
          }}
          .project-review-addon .issue-muted {{
            color: #5f6862;
          }}
          .project-review-addon .issue-severity-high {{
            background: #fff2f2;
          }}
          .project-review-addon .issue-severity-medium {{
            background: #fff9e6;
          }}
          .project-review-addon .issue-severity-low {{
            background: #f8fbf7;
          }}
          .project-review-addon mark {{
            background: #ffe082;
            padding: 0 2px;
          }}
        </style>
        <section class="project-review-addon">
          <h2>项目级补充审查</h2>
          <div class="project-review-grid">
            <div class="project-review-card"><div class="label">商务标可疑对</div><div class="value">{html.escape(str(business_summary.get("suspicious_pair_count") or 0))}</div></div>
            <div class="project-review-card"><div class="label">技术标可疑对</div><div class="value">{html.escape(str(technical_summary.get("suspicious_pair_count") or 0))}</div></div>
            <div class="project-review-card"><div class="label">错别字候选</div><div class="value">{html.escape(str(bid_summary.get("typo_issue_count") or 0))}</div></div>
            <div class="project-review-card"><div class="label">一人多用姓名</div><div class="value">{html.escape(str(bid_summary.get("reused_name_count") or 0))}</div></div>
          </div>
          <p class="project-review-scope">当前视角：{html.escape(scope_text)}</p>
          <div class="project-review-links">
            <a href="{html.escape(str(issue_pages.get('business_duplicates') or ''))}" target="_blank" rel="noreferrer">全部商务标查重问题</a>
            <a href="{html.escape(str(issue_pages.get('technical_duplicates') or ''))}" target="_blank" rel="noreferrer">全部技术标查重问题</a>
            <a href="{html.escape(str(issue_pages.get('typos') or ''))}" target="_blank" rel="noreferrer">全部错别字问题</a>
            <a href="{html.escape(str(issue_pages.get('personnel') or ''))}" target="_blank" rel="noreferrer">全部一人多用问题</a>
          </div>

          <h3>商务标查重（当前文件相关）</h3>
          <table>
            <thead>
              <tr><th>风险</th><th>分数</th><th>文件 A</th><th>文件 B</th><th>段落</th><th>句子</th><th>表格</th><th>证据</th></tr>
            </thead>
            <tbody>{self._project_render_duplicate_rows(business_duplicate, DOCUMENT_TYPE_BUSINESS_BID, source_lookup=source_lookup, issue_pages=issue_pages, current_files=current_files)}</tbody>
          </table>

          <h3>技术标查重（全项目）</h3>
          <table>
            <thead>
              <tr><th>风险</th><th>分数</th><th>文件 A</th><th>文件 B</th><th>段落</th><th>句子</th><th>表格</th><th>证据</th></tr>
            </thead>
            <tbody>{self._project_render_duplicate_rows(technical_duplicate, DOCUMENT_TYPE_TECHNICAL_BID, source_lookup=source_lookup, issue_pages=issue_pages)}</tbody>
          </table>

          <h3>一人多用（当前文件相关）</h3>
          <table>
            <thead>
              <tr><th>文档类型</th><th>姓名</th><th>风险</th><th>涉及文件数</th><th>文件与页码</th><th>详情</th></tr>
            </thead>
            <tbody>{self._project_render_personnel_rows(bid_review, source_lookup=source_lookup, issue_pages=issue_pages, current_files=current_files)}</tbody>
          </table>

          <h3>错别字识别（当前文件相关）</h3>
          <table>
            <thead>
              <tr><th>文档类型</th><th>文件</th><th>页码</th><th>原字</th><th>建议</th><th>原文</th><th>详情</th></tr>
            </thead>
            <tbody>{self._project_render_typo_rows(bid_review, source_lookup=source_lookup, issue_pages=issue_pages, current_files=current_files)}</tbody>
          </table>
        </section>
        """

    def build_project_summary_html(
        self,
        *,
        project_identifier,
        business_infos,
        project_results,
        source_lookup,
        issue_pages,
    ):
        link_items = "\n".join(
            f"<li><a href='{html.escape(str(item['url']))}'>{html.escape(str(item['name']))}</a></li>"
            for item in business_infos
        )
        return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>项目级审查总览</title>
  <style>
    body {{ margin: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; background: #f7f7f4; color: #1c1c1c; }}
    main {{ padding: 28px 36px 40px; max-width: 1320px; margin: 0 auto; }}
    h1 {{ margin: 0 0 8px; font-size: 28px; }}
    .meta {{ color: #5f6862; margin-bottom: 18px; }}
    ul {{ background: #fff; border: 1px solid #d9ded8; border-radius: 8px; padding: 14px 28px; }}
    li + li {{ margin-top: 8px; }}
  </style>
</head>
<body>
  <main>
    <h1>项目级审查总览</h1>
    <div class="meta">{html.escape(str(project_identifier))}</div>
    <h2>单文件报告</h2>
    <ul>{link_items}</ul>
    {self.build_project_review_section(project_results, source_lookup=source_lookup, issue_pages=issue_pages)}
  </main>
</body>
</html>"""

    def inject_project_review_section(self, html_report, extra_section):
        marker = "</body>"
        if marker in html_report:
            return html_report.replace(marker, f"{extra_section}\n{marker}", 1)
        return f"{html_report}\n{extra_section}"
