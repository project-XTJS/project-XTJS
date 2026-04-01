import re

class ReportVisualizer:
    """可视化工具：生成基于投标人(投标文件)正文内容的合规报告"""
    
    def __init__(self):
        self.CONTENT_PATTERN = re.compile(r'[\u4e00-\u9fa5a-zA-Z0-9]+')

    def _normalize(self, text: str) -> str:
        if not text: return ""
        text = text.replace('(', '（').replace(')', '）')
        return "".join(self.CONTENT_PATTERN.findall(text))

    def _highlight_bidder_text(self, bidder_text, all_anchors, missing_anchors):
        """
        在投标人的文本中高亮匹配到的模版内容。
        """
        # 找出哪些锚点是匹配成功的
        found_anchors = [a for a in all_anchors if a not in missing_anchors]
        
        rendered_parts = []
        current_pos = 0
        
        # 遍历所有成功找到的锚点，在投标人原文中寻找它们的物理位置
        for anchor in found_anchors:
            # 构建一个模糊匹配正则：锚点字之间允许有任意换行、空格或特殊符号
            # 比如 "比选保证书" -> "比.*?选.*?保.*?证.*?书"
            regex_str = ".*?".join([re.escape(c) for c in anchor])
            pattern = re.compile(regex_str, re.S) # re.S 允许匹配换行符
            
            match = pattern.search(bidder_text, current_pos)
            if match:
                # 1. 匹配点之前的内容：属于投标人填写的业务数据 -> 黑色
                rendered_parts.append(f"<span class='bidder-data'>{bidder_text[current_pos:match.start()]}</span>")
                # 2. 匹配到的内容：属于模版自带的固定锚点 -> 绿色
                rendered_parts.append(f"<span class='template-match'>{bidder_text[match.start():match.end()]}</span>")
                # 更新指针
                current_pos = match.end()
        
        # 3. 剩余的内容 -> 黑色
        rendered_parts.append(f"<span class='bidder-data'>{bidder_text[current_pos:]}</span>")
        return "".join(rendered_parts)

    def generate_html(self, integrity_report, consistency_report, test_segments, model_segments):
        """生成 HTML 报告"""
        # 数据索引化
        model_dict = {item['title']: item['text'] for item in model_segments}
        test_dict = {item['title']: item['text'] for item in test_segments}
        
        # 借用 ConsistencyChecker 的逻辑来解析锚点
        from .consistency import ConsistencyChecker
        checker = ConsistencyChecker()

        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <title>标书合规性智能审查报告</title>
            <style>
                body {{ font-family: 'PingFang SC', 'Microsoft YaHei', sans-serif; background-color: #f5f7fa; padding: 40px; color: #333; }}
                .report-wrapper {{ max-width: 1100px; margin: 0 auto; background: #fff; padding: 40px; border-radius: 8px; box-shadow: 0 2px 12px rgba(0,0,0,0.1); }}
                h1 {{ text-align: center; color: #2c3e50; border-bottom: 2px solid #eee; padding-bottom: 20px; }}
                .summary-box {{ background: #f0f9eb; border: 1px solid #e1f3d8; padding: 20px; border-radius: 4px; margin-bottom: 30px; text-align: center; }}
                .score {{ font-size: 32px; color: #67c23a; font-weight: bold; }}
                
                h2 {{ color: #409eff; border-left: 4px solid #409eff; padding-left: 10px; margin-top: 40px; }}
                
                /* 完整性表格 */
                .it-table {{ width: 100%; border-collapse: collapse; margin-top: 15px; }}
                .it-table th, .it-table td {{ border: 1px solid #ebeef5; padding: 12px; text-align: left; }}
                .it-table th {{ background: #fafafa; }}
                .tag-green {{ color: #67c23a; font-weight: bold; }}
                .tag-red {{ color: #f56c6c; font-weight: bold; }}

                /* 一致性展示区 */
                .consistency-section {{ margin-top: 30px; border: 1px solid #ebeef5; border-radius: 4px; padding: 20px; }}
                .section-head {{ font-size: 18px; font-weight: bold; color: #303133; margin-bottom: 15px; background: #f5f7fa; padding: 10px; border-radius: 4px; }}
                .bidder-content {{ 
                    background: #ffffff; border: 1px solid #dcdfe6; padding: 20px; border-radius: 4px; 
                    white-space: pre-wrap; font-size: 15px; line-height: 1.8; color: #000; 
                }}
                
                .template-match {{ color: #27ae60; font-weight: bold; }} /* 绿色：模版匹配上的内容 */
                .bidder-data {{ color: #000; }} /* 黑色：投标人填写的具体数据 */
                
                .missing-box {{ 
                    margin-top: 15px; background: #fef0f0; border: 1px solid #fde2e2; 
                    padding: 15px; border-radius: 4px; color: #f56c6c; 
                }}
                .missing-title {{ font-weight: bold; margin-bottom: 5px; }}
                .missing-item {{ display: inline-block; background: #fff; padding: 2px 8px; border: 1px solid #fde2e2; margin: 3px; border-radius: 3px; font-size: 13px; text-decoration: line-through; }}
            </style>
        </head>
        <body>
            <div class="report-wrapper">
                <h1>标书合规性全方位审查报告</h1>
                
                <div class="summary-box">
                    <div style="font-size: 16px;">完整性核验总分</div>
                    <div class="score">{integrity_report['integrity_score']}</div>
                </div>

                <h2>一、完整性核验结果 (是否存在该章节)</h2>
                <table class="it-table">
                    <thead>
                        <tr><th>审查项目</th><th>核验状态</th><th>预览内容</th></tr>
                    </thead>
                    <tbody>
        """
        
        for name, info in integrity_report.get('details', {}).items():
            status = info['status']
            cls = "tag-green" if "已找到" in status else "tag-red"
            html_content += f"<tr><td>{name}</td><td class='{cls}'>{status}</td><td style='font-size:12px;color:#666'>{info.get('preview','-')}</td></tr>"
            
        html_content += """
                    </tbody>
                </table>

                <h2>二、一致性比对 (投标文件正文细节核对)</h2>
                <div style="margin-bottom: 20px; font-size: 14px; color: #666;">
                    <strong>图例：</strong>
                    <span class="template-match">绿色</span> = 匹配成功的模版文字 | 
                    <span class="bidder-data">黑色</span> = 投标人填写的业务内容 | 
                    <span class="tag-red">红色删除线</span> = 缺失的模版要求
                </div>
        """

        for rec in consistency_report:
            title = rec['name']
            missing = rec.get('missing_anchors', [])
            
            bidder_text = test_dict.get(title, "")
            model_text = model_dict.get(title, "")
            
            # 获取模版章节的所有锚点
            all_anchors = checker._get_anchors(model_text) if model_text else []
            
            html_content += f"""
                <div class="consistency-section">
                    <div class="section-head">{title}</div>
            """
            
            if not bidder_text:
                html_content += "<div class='bidder-content' style='color:#999;text-align:center'>[ 投标文件中未检测到该内容 ]</div>"
            elif "【纯图" in str(missing):
                html_content += f"<div class='bidder-content'>{bidder_text}</div>"
                html_content += "<div class='missing-box' style='color:#67c23a;background:#f0f9eb;border-color:#e1f3d8'>检测为证件/图片类材料，已通过完整性核验，无需文本细节比对。</div>"
            else:
                # 核心逻辑：高亮渲染投标人正文
                highlighted = self._highlight_bidder_text(bidder_text, all_anchors, missing)
                html_content += f"<div class='bidder-content'>{highlighted}</div>"
                
                # 单独列出缺失项
                if missing:
                    html_content += """
                        <div class="missing-box">
                            <div class="missing-title">⚠️ 缺失的模版锚点内容（请核实）：</div>
                    """
                    for m in missing:
                        html_content += f"<span class='missing-item'>{m}</span>"
                    html_content += "</div>"
                    
            html_content += "</div>"

        html_content += """
            </div>
        </body>
        </html>
        """
        return html_content