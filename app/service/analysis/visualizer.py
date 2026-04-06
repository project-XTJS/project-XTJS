import re
import html

class ReportVisualizer:
    """可视化工具：生成基于投标人(投标文件)正文内容的合规报告"""
    
    def __init__(self):
        self.CONTENT_PATTERN = re.compile(r'[\u4e00-\u9fa5a-zA-Z0-9]+')

    def _normalize(self, text: str) -> str:
        if not text: return ""
        text = text.replace('(', '（').replace(')', '）')
        return "".join(self.CONTENT_PATTERN.findall(text))

    def _highlight_bidder_text(self, bidder_text, all_anchors, missing_anchors):
        from collections import Counter
        
        # 1. 区分已找到和缺失的锚点，并初始化额度
        found_anchors = [a for a in all_anchors if a not in missing_anchors]
        anchor_counts = Counter(found_anchors)
        used_counts = {a: 0 for a in anchor_counts}
        
        # ==================== 核心新增：缺失块跟随映射 ====================
        # 结构：{(锚点名称, 第几次出现): [缺失项1, 缺失项2, ...]}
        missing_blocks = {}
        current_key = ("__START__", 0)  # 用于记录在第一个有效锚点之前的缺失项
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
        # =================================================================
                
        # 2. 预编译正则
        active_patterns = {}
        for anchor in anchor_counts:
            gap = r'[^\u4e00-\u9fa5a-zA-Z0-9]*'
            regex_str = gap.join([re.escape(c) for c in anchor])
            active_patterns[anchor] = re.compile(regex_str, re.S)

        # 3. 核心工具方法：对单个字符串进行高亮，并在匹配项后注入缺失块
        def highlight_string(s):
            if not s.strip(): return s
            
            intervals = []
            for anchor, pattern in active_patterns.items():
                if used_counts[anchor] >= anchor_counts[anchor]:
                    continue
                for match in pattern.finditer(s):
                    intervals.append((match.start(), match.end(), anchor))
            
            if not intervals:
                return f"<span class='bidder-data'>{html.escape(s)}</span>"
                
            # 排序及消歧义
            intervals.sort(key=lambda x: (x[0], -(x[1]-x[0])))
            selected_intervals = []
            local_used = {a: 0 for a in anchor_counts}
            last_end = 0
            
            for start, end, anchor in intervals:
                if used_counts[anchor] + local_used[anchor] >= anchor_counts[anchor]:
                    continue
                if start >= last_end:
                    # 记录当前锚点是第几次被成功匹配
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
                
                # 渲染命中高亮
                parts.append(f"<span class='template-match'>{html.escape(s[start:end])}</span>")
                
                # ================= 注入连续缺失块 =================
                key = (anchor, anchor_idx)
                if key in missing_blocks and missing_blocks[key]:
                    missing_texts = ", ".join(missing_blocks[key])
                    parts.append(f"<span class='missing-badge' title='此处缺失模版规定的内容'>缺: {html.escape(missing_texts)}</span>")
                    missing_blocks[key] = []  # 渲染后清空，防止重复
                # =================================================
                
                curr = end
                
            if curr < len(s):
                parts.append(f"<span class='bidder-data'>{html.escape(s[curr:])}</span>")
                
            return "".join(parts)

        # 4. 行切分
        lines = bidder_text.split('\n')
        
        if len(lines) > 80:
            lines = lines[:80]
            lines.append('<div class="truncate-alert">... [ 篇幅过长，已触发全局折叠 ]</div>')

        parsed_lines = []
        
        # 处理在全文最开头就缺失的锚点
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

        # ================= 扫尾：处理未成功依附的缺失块 =================
        leftover_missing = []
        for m_list in missing_blocks.values():
            leftover_missing.extend(m_list)
        if leftover_missing:
            # 去重并合并
            final_leftovers = ", ".join(list(dict.fromkeys(leftover_missing)))
            parsed_lines.append(f"<br><div style='margin-top: 10px;'><span class='missing-badge' title='文末缺失内容'>缺: {html.escape(final_leftovers)}</span></div>")
        # ==============================================================

        return "".join(parsed_lines)

    def generate_html(self, integrity_report, consistency_report, test_segments, model_segments):
        model_dict = {item['title']: item['text'] for item in model_segments}
        test_dict = {item['title']: item['text'] for item in test_segments}
        
        from .consistency import ConsistencyChecker
        checker = ConsistencyChecker()

        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <title>标书合规性智能审查报告</title>
            <style>
                :root {{ 
                    --bg-color: #f5f7fa;
                    --card-bg: #ffffff;
                    --text-main: #303133; 
                    --text-regular: #606266;
                    --text-light: #909399; 
                    --border-color: #ebeef5; 
                    --primary-color: #409eff; 
                    --success-color: #67c23a;
                    --danger-color: #f56c6c;
                    --danger-light: #fef0f0;
                    --danger-border: #fde2e2;
                }}
                
                body {{ font-family: "Helvetica Neue", Arial, sans-serif; margin: 0; padding: 30px 20px; color: var(--text-main); font-size: 14px; background: var(--bg-color); line-height: 1.6; }}
                .container {{ max-width: 1100px; margin: 0 auto; }}
                
                .card {{ background: var(--card-bg); border-radius: 12px; box-shadow: 0 4px 12px rgba(0,0,0,0.05); padding: 30px; margin-bottom: 25px; transition: all 0.3s ease; }}
                .card:hover {{ box-shadow: 0 6px 16px rgba(0,0,0,0.08); }}
                
                .header-card {{ border-top: 4px solid var(--primary-color); display: flex; justify-content: space-between; align-items: center; }}
                h1 {{ font-size: 24px; margin: 0; font-weight: 600; color: #1f2d3d; letter-spacing: 1px; }}
                .score-value {{ font-size: 36px; color: var(--success-color); font-weight: bold; line-height: 1; font-family: 'Arial', sans-serif; }}
                
                h2 {{ font-size: 18px; margin: 0 0 20px 0; padding-bottom: 12px; border-bottom: 2px solid var(--border-color); font-weight: 600; color: #1f2d3d; }}
                
                .it-table {{ width: 100%; border-collapse: collapse; table-layout: fixed; }}
                .it-table th {{ background: #f8f9fb; padding: 12px 15px; text-align: left; color: #909399; font-weight: 500; border-bottom: 2px solid var(--border-color); }}
                .it-table td {{ padding: 12px 15px; border-bottom: 1px solid var(--border-color); vertical-align: top; }}
                
                .main-item-row {{ font-weight: 600; color: #303133; background-color: #fafafa; }}
                .main-item-row:hover {{ background-color: #f5f7fa; }}
                
                .sub-item-row td:first-child {{ padding-left: 35px; color: #606266; font-size: 13px; position: relative; font-weight: normal; }}
                .sub-item-row td:first-child::before {{ content: '└'; position: absolute; left: 18px; color: #c0c4cc; }}
                .sub-item-row:hover {{ background-color: #fdfdfd; }}

                .status-tag {{ padding: 2px 8px; border-radius: 10px; font-size: 12px; font-weight: bold; }}
                .tag-ok {{ background: #f0f9eb; color: var(--success-color); }}
                .tag-err {{ background: var(--danger-light); color: var(--danger-color); border: 1px solid var(--danger-border); }}

                /* 截断样式复用：不论名称还是内容都可以限制行数 */
                .cell-truncate {{
                    display: -webkit-box;
                    -webkit-line-clamp: 2; /* 限制2行 */
                    -webkit-box-orient: vertical;
                    overflow: hidden;
                    text-overflow: ellipsis;
                    word-break: break-all;
                }}
                /* 专门针对要求名称列的字体颜色，继承父级避免受子类覆盖 */
                .name-truncate {{ color: inherit; font-size: inherit; }}
                /* 专门针对定位预览的字体颜色 */
                .preview-truncate {{ color: var(--text-light); font-size: 13px; }}

                .legend-card {{ background: #fffaf0; border-left: 4px solid #e6a23c; padding: 16px 24px; border-radius: 6px; margin-bottom: 24px; font-size: 13px; color: var(--text-regular); display: flex; gap: 24px; align-items: center; }}
                
                .table-wrapper {{ margin: 16px 0; border: 1px solid var(--border-color); border-radius: 6px; overflow: hidden; }}
                .report-table {{ width: 100%; min-width: 80px;border-collapse: collapse; font-size: 13px; background: #fff; }}
                .report-table td {{ border-bottom: 1px solid var(--border-color); min-width: 80px;border-right: 1px solid var(--border-color); padding: 10px 14px; vertical-align: middle; color: var(--text-regular); }}
                .report-table td:last-child {{ border-right: none; }}
                .report-table tr:last-child td {{ border-bottom: none; }}
                .report-table tr:nth-child(even) {{ background: #fafbfc; }}
                .report-table tr:hover {{ background: #ecf5ff; transition: background 0.2s; }}
                
                .content-box {{ background: #fdfdfd; border: 1px solid var(--border-color); border-radius: 6px; padding: 20px 24px; white-space: normal; word-wrap: break-word; color: var(--text-main); font-size: 14px; line-height: 1.8; }}
                
                .template-match {{ color: #0d5cb6; background-color: #e6f1fc; padding: 2px 6px; border-radius: 4px; margin: 0 2px; font-weight: 500; }} 
                .bidder-data {{ color: var(--text-main); }} 
                
                .missing-badge {{ 
                    display: inline-block; 
                    background: var(--danger-light); 
                    border: 1px solid var(--danger-border); 
                    color: var(--danger-color); 
                    padding: 2px 8px; 
                    border-radius: 12px; 
                    font-size: 12px; 
                    font-weight: 500; 
                    margin: 0 4px; 
                    vertical-align: middle;
                }}
                
                .missing-summary {{ margin-top: 16px; font-size: 13px; display: flex; background: var(--danger-light); padding: 12px 20px; border-radius: 6px; border-left: 4px solid var(--danger-color); align-items: baseline; }}
                .missing-summary-label {{ color: var(--danger-color); font-weight: 600; white-space: nowrap; margin-right: 12px; }}
                .missing-list {{ display: flex; flex-wrap: wrap; gap: 8px; }}
                .missing-list-item {{ color: var(--danger-color); font-size: 13px; }}
                .missing-list-item:not(:last-child)::after {{ content: '、'; color: rgba(245,108,108,0.5); }}
                
                .empty-text {{ color: var(--text-light); font-style: italic; background: #fafafa; padding: 24px; border-radius: 6px; text-align: center; border: 1px dashed #dcdfe6; letter-spacing: 1px; }}
                .truncate-alert {{ margin-top: 16px; padding: 10px; text-align: center; background: #f4f4f5; color: #909399; border-radius: 4px; font-size: 13px; }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="card header-card">
                    <h1>标书合规性智能审查报告</h1>
                    <div style="text-align: right;">
                        <div style="font-size: 13px; color: var(--text-light); margin-bottom: 4px;"></div>
                    </div>
                </div>

                <div class="card">
                    <h2>🔍 一、完整性核验结果</h2>
                    <table class="it-table">
                        <thead>
                            <tr>
                                <th width="35%">审查项目名称 (悬浮查看完整)</th>
                                <th width="15%">核验状态</th>
                                <th>内容定位预览 (悬浮查看完整)</th>
                            </tr>
                        </thead>
                        <tbody>
        """
        
        for name, info in integrity_report.get('details', {}).items():
            is_sub = re.match(r'^[A-Z][．\.]|^[\(（]\d+[\)）]', name)
            row_cls = "sub-item-row" if is_sub else "main-item-row"
            
            is_ok = "已找到" in info['status']
            tag_cls = "tag-ok" if is_ok else "tag-err"
            
            preview_text = info.get('preview', '-')
            safe_preview = html.escape(preview_text).replace('"', '&quot;')
            safe_name = html.escape(name).replace('"', '&quot;')
            
            # 核心修改：左侧的项目名（name）和右侧的内容预览（preview_text）都加上了截断类
            html_content += f"""
                            <tr class="{row_cls}">
                                <td><div class="cell-truncate name-truncate" title="{safe_name}">{html.escape(name)}</div></td>
                                <td><span class="status-tag {tag_cls}">{"✓ 已找到" if is_ok else "⚠️ 未找到"}</span></td>
                                <td><div class="cell-truncate preview-truncate" title="{safe_preview}">{preview_text}</div></td>
                            </tr>
            """
            
        html_content += """
                        </tbody>
                    </table>
                </div>

                <div class="legend-card">
                    <strong>💡 阅读指引：</strong>
                    <span><span class="template-match">浅蓝底纹</span> = 模版内容匹配成功</span>
                    <span><span class="bidder-data" style="font-weight:600;">深灰文本</span> = 投标人填写的业务数据</span>
                    <span><span class="missing-badge" style="margin:0;">缺: 缺失内容</span> = 文本中缺失的模版要求</span>
                </div>

                <div class="consistency-details">
        """

        # ==================================================================
        # ==================== 新增代码 START ================================
        # ==================================================================
        html_content += """
                <style>
                    html { scroll-behavior: smooth; }
                    .overview-link { color: var(--primary-color); text-decoration: none; font-weight: 600; display: block; transition: color 0.2s; }
                    .overview-link:hover { color: #0056b3; text-decoration: underline; }
                </style>
                <div class="card" style="border-top: 4px solid var(--primary-color);">
                    <h2>🔍 二、一致性审查结果总览</h2>
                    <table class="it-table">
                        <thead>
                            <tr>
                                <th width="75%">审查项目名称 (点击名称可跳转至下方对应详情)</th>
                                <th width="25%">一致性状态</th>
                            </tr>
                        </thead>
                        <tbody>
        """
        for _idx, _rec in enumerate(consistency_report):
            _title = _rec['name']
            _missing = _rec.get('missing_anchors', [])
            _jump_id = f"detail-section-{_idx}"
            
            # 提取正文内容判断是否存在，若不存在则为未检测到内容
            _bidder_text = test_dict.get(_title, "")
            
            if not _bidder_text:
                _status_html = "<span class='status-tag' style='background: #f4f4f5; color: #909399; border: 1px solid #dcdfe6;'>⚠️ 未检测到内容</span>"
            elif not _missing:
                _status_html = "<span class='status-tag tag-ok'>✨ 完全匹配</span>"
            else:
                _status_html = f"<span class='status-tag tag-err'>⚠️ 缺失 {len(_missing)} 项</span>"
                
            html_content += f"""
                            <tr class="main-item-row">
                                <td>
                                    <a href="#{_jump_id}" class="overview-link">
                                        ■ {html.escape(_title)}
                                    </a>
                                </td>
                                <td>{_status_html}</td>
                            </tr>
            """
        html_content += """
                        </tbody>
                    </table>
                </div>
        """
        
        # 增加用于标记各个卡片的全局索引变量
        _anchor_idx = 0
        # ==================================================================
        # ==================== 新增代码 END ==================================
        # ==================================================================

        for rec in consistency_report:
            title = rec['name']
            missing = rec.get('missing_anchors', [])
            
            bidder_text = test_dict.get(title, "")
            model_text = model_dict.get(title, "")

            if '\n' in model_text:
                model_text = model_text.split('\n', 1)[1]
            all_anchors = checker._get_anchors(model_text)

            # ==================== 新增代码 START：注入隐形锚点用于准确跳转定位 =========
            html_content += f'<div id="detail-section-{_anchor_idx}" style="position: relative; top: -20px;"></div>'
            _anchor_idx += 1
            # ==================== 新增代码 END ========================================
            
            html_content += f"""
                <div class="card">
                    <h2 style="color: var(--primary-color); border: none; margin-bottom: 15px;">■ {title}</h2>
            """
            
            if not bidder_text:
                html_content += "<div class='empty-text'>[ ⚠️ 投标文件中未检测到该部分内容，请重点核实 ]</div>"
            else:
                highlighted = self._highlight_bidder_text(bidder_text, all_anchors, missing)
                html_content += "<div style='margin-bottom: 12px; display: flex; flex-wrap: wrap; gap: 10px; align-items: center;'>"
                
                if not missing:
                    # 完全匹配样式
                    html_content += "<span class='status-tag tag-ok'>✨ 格式与模版内容完全匹配</span>"
                else:
                    # 存在缺失时的样式
                    html_content += f"<span class='status-tag tag-err'>⚠️ 缺失 {len(missing)} 项模版内容</span>"
                    for m in missing:
                        html_content += f"<span class='missing-badge'>{html.escape(m)}</span>"
                
                html_content += "</div>"
                html_content += f"<div class='content-box'>{highlighted}</div>"
                    
            html_content += "</div>"

        html_content += """
                </div>
            </div>
        </body>
        </html>
        """
        return html_content