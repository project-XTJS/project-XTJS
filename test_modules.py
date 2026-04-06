import sys
import os
import json

# 确保项目根目录在系统路径中，解决跨文件夹导入问题
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

# 引入各业务模块
from app.service.analysis.template_extractor import TemplateExtractor
from app.service.analysis.integrity import IntegrityChecker
from app.service.analysis.consistency import ConsistencyChecker, DocumentProcessor
from app.service.analysis.visualizer import ReportVisualizer

def main():
    m_path, t_path = "./ocr_results/369/369-model.json", "./ocr_results/369/369-sangwu.json"
    if not os.path.exists(m_path) or not os.path.exists(t_path):
        print(f"错误: 找不到文件 {m_path} 或 {t_path}")
        return

    print("正在加载 JSON 数据...")
    with open(m_path, 'r', encoding='utf-8') as f: m_json = json.load(f)
    with open(t_path, 'r', encoding='utf-8') as f: t_json = json.load(f)
    integrity_report = IntegrityChecker().check_integrity(m_json, t_json)
    checker = ConsistencyChecker()
    consistency_report = checker.compare_raw_data(m_json, t_json)
    temps = TemplateExtractor.extract_consistency_templates(m_json)
    m_segs = [{"title": t['title'], "text": "\n".join(t['content'])} for t in temps]
    b_segs = DocumentProcessor.segment_document(t_json, temps, is_test_file=True)
    print("正在生成可视化报告...")
    html_report = ReportVisualizer().generate_html(integrity_report, consistency_report, b_segs, m_segs)
    with open("final_report.html", "w", encoding="utf-8") as f:
        f.write(html_report)
    print(f"运行完成！")

if __name__ == "__main__":
    main()