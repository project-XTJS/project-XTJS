import re
import html
import os
import random
import string
from collections import Counter

class ReportVisualizer:
    """可视化工具：生成基于投标人(投标文件)正文内容的合规报告"""

    def __init__(self):
        self.CONTENT_PATTERN = re.compile(r'[\u4e00-\u9fa5a-zA-Z0-9]+')

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
                'idx': idx
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

    def _generate_detail_card(self, title, missing, bidder_text, model_text, checker):
        if not bidder_text:
            return f"""
                <div class="card">
                    <h2 style="color: var(--primary-color); border: none; margin-bottom: 15px;">■ {title}</h2>
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
            card = self._generate_detail_card(title, missing, bidder_text, model_text, checker)
            detail_cards.append(card)

        combined_html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <title>合并附件详情 - 审查报告</title>
            {self.CSS_STYLE}
        </head>
        <body>
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
            page_info = f" (P{item['response_page']})" if item.get('response_page') else ""
            
            rows_html += f"""
                <tr>
                    <td><div class="cell-truncate" title="{html.escape(item['requirement'])}">{html.escape(item['requirement'])}</div></td>
                    <td><span class="status-tag {tag_cls}">{tag_text}</span></td>
                    <td><div class="cell-truncate preview-truncate" title="{html.escape(evidence)}">{html.escape(evidence)}{page_info}</div></td>
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
                        <th>响应证据 / 所在位置</th>
                    </tr>
                </thead>
                <tbody>
                    {rows_html if rows_html else '<tr><td colspan="3" class="empty-text">未检测到带 ★ 的强制性要求</td></tr>'}
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
        sum_check = pricing_report.get('checks', {}).get('sum_consistency', {})
        
        status_map = {'pass': ('tag-ok', '逻辑一致'), 'fail': ('tag-err', '存在疑点'), 'unknown': ('tag-warning', '待人工核实')}
        tag_cls, tag_text = status_map.get(status, ('tag-missing', '未知'))
        details_html = "".join([f"<li>{html.escape(d)}</li>" for d in details])
        
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

        return f"""
        <div class="card">
            <h2 style="display:flex; justify-content:space-between;">
                <span>💰 分项报价逻辑审查</span>
                <span class="status-tag {tag_cls}">{tag_text}</span>
            </h2>
            <div style="background:#fefefe; border:1px solid #eee; padding:15px; border-radius:8px; margin-bottom:15px; font-size:13px;">
                <div style="font-weight:bold; margin-bottom:8px; color:var(--text-main);">{html.escape(summary)}</div>
                <ul style="margin:0; padding-left:20px; color:var(--text-regular);">{details_html}</ul>
            </div>
            {sum_panel}
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

            matched_sign_text = sign_date_check.get("matched_sign_text")
            if matched_sign_text:
                notes.append(f"落款日期：{matched_sign_text}")

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
            page_text = "、".join(f"P{page}" for page in pages if page is not None) or "-"
            rows_html += f"""
                <tr>
                    <td><div class="cell-truncate" title="{html.escape(str(item.get('title') or '-'))}">{html.escape(str(item.get('title') or '-'))}</div></td>
                    <td>{html.escape(page_text)}</td>
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
                        <th width="10%">页码</th>
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
                      verification_report=None, file_switcher_info=None):
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
                                <th width="30%">审查项目名称 (悬浮查看完整)</th>
                                <th width="12%">完整性状态</th>
                                <th width="15%">一致性详情</th>
                                <th>内容定位预览</th>
                            </tr>
                        </thead>
                        <tbody>
        """

        # 创建详情目录
        details_dir = "details"
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
            if len(child_items) > 0:
                consistency_html = f'<span class="status-tag tag-warning consistency-toggle" data-group="{group_id}">⚠️ 查看子附件情况<span class="toggle-icon">▼</span></span>'
            else:
                attach_nums = attachment_mapping.get(seq, [])
                infos = [attachment_info_map[num] for num in attach_nums if num in attachment_info_map]
                if infos:
                    s_label, s_tag = self._get_status_for_single_item(infos)
                    if len(infos) == 1:
                        consistency_html = f'<a class="status-tag {s_tag}" href="details/detail_{infos[0]["idx"]}.html">{s_label}</a>'
                    else:
                        path = f"details/detail_combined_{combined_page_counter}.html"
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

            # 主项行
            main_html += f"""
                                <tr class="main-item-row group-row" data-group-id="{group_id}">
                                    <td class="toggle-cell"></td>
                                    <td><div class="cell-truncate name-truncate" title="{html.escape(name)}">{html.escape(name)}</div></td>
                                    <td><span class="status-tag {tag_cls}">{display_status}</span></td>
                                    <td>{consistency_html}</td>
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
                        c_consistency = f'<a class="status-tag {st}" href="details/detail_{c_infos[0]["idx"]}.html">{sl}</a>'
                    else:
                        c_path = f"details/detail_combined_{combined_page_counter}.html"
                        self._generate_combined_detail_page(
                            c_infos,
                            os.path.join(details_dir, f"detail_combined_{combined_page_counter}.html"),
                            model_dict,
                            checker
                        )
                        c_consistency = f'<a class="status-tag {st}" href="{c_path}">{sl}</a>'
                        combined_page_counter += 1

                main_html += f"""
                                <tr class="sub-item-row child-row" data-parent="{group_id}">
                                    <td></td>
                                    <td><div class="cell-truncate name-truncate" title="{html.escape(c_name)}">{html.escape(c_name)}</div></td>
                                    <td><span class="status-tag {"tag-ok" if c_info.get("is_passed") else "tag-err"}">{c_info['status']}</span></td>
                                    <td>{c_consistency}</td>
                                    <td><div class="cell-truncate preview-truncate">{c_info.get('preview', '-')}</div></td>
                                </tr>
                """
            group_id += 1
            i = j

        # 生成单个附件详情页
        for idx, rec in enumerate(consistency_report):
            title, missing = rec['name'], rec.get('missing_anchors', [])
            b_text, m_text = test_dict.get(title, ""), model_dict.get(title, "")
            card_html = self._generate_detail_card(title, missing, b_text, m_text, checker)
            detail_html = f"""
            <!DOCTYPE html>
            <html><head><meta charset="UTF-8"><title>审查详情 - {html.escape(title)}</title>{self.CSS_STYLE}</head>
            <body><a href="javascript:void(0);" onclick="window.history.back();" class="back-link">← 返回总览</a>
            <div class="container"><div style="height: 40px;"></div><div class="legend-card"><strong>💡 阅读指引：</strong><span>浅蓝底纹=匹配成功</span><span>缺=缺失模版内容</span></div>{card_html}</div>
            <script>document.querySelectorAll('.expand-btn').forEach(btn => {{ btn.onclick = function() {{ const content = document.getElementById('content-' + this.dataset.uid); const isCol = content.classList.contains('content-collapsed'); content.classList.toggle('content-collapsed', !isCol); content.classList.toggle('content-expanded', isCol); this.querySelector('.expand-text').textContent = isCol ? '收起' : '显示全部'; this.querySelector('.expand-icon').textContent = isCol ? '▲' : '▼'; }}; }});</script>
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
            </body>
            </html>
        """
        return main_html
