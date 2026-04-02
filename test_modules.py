import json, os
from app.service.analysis.template_extractor import TemplateExtractor
from app.service.analysis.integrity import IntegrityChecker
from app.service.analysis.consistency import ConsistencyChecker, DocumentProcessor
from app.service.analysis.visualizer import ReportVisualizer

def main():
    # 1. 加载文件
    m_path, t_path = "./ocr_results/427/427-model.json", "./ocr_results/427/427-rongyuan.json"
    
    if not os.path.exists(m_path) or not os.path.exists(t_path):
        print(f"错误: 找不到文件 {m_path} 或 {t_path}")
        return

    with open(m_path, 'r', encoding='utf-8') as f: m_json = json.load(f)
    with open(t_path, 'r', encoding='utf-8') as f: t_json = json.load(f)

    print("正在执行合规性审查...")

    # 2. 完整性检查：核对清单是否缺失
    integrity_report = IntegrityChecker().check_integrity(m_json, t_json)
    
    # 3. 一致性检查：比对模板锚点
    consistency_report = ConsistencyChecker().compare_raw_data(m_json, t_json)
    
    # 4. 获取用于可视化的分段数据
    # 先从招标文件提取标准模板信息
    templates = TemplateExtractor.extract_consistency_templates(m_json)
    
    # 分别切分“招标文件模板”和“投标文件正文”
    m_segs = DocumentProcessor.segment_document(m_json, templates)  # 模板段落（用于计算锚点）
    b_segs = DocumentProcessor.segment_document(t_json, templates)  # 投标人段落（用于 HTML 展示主体）

    # 5. 生成可视化报告
    print("正在生成基于投标文件内容的可视化报告...")
    # 🌟 关键修改：同时传入 b_segs 和 m_segs
    # 报告将以 b_segs (投标文件) 为底色进行渲染
    html = ReportVisualizer().generate_html(
        integrity_report, 
        consistency_report, 
        b_segs, 
        m_segs
    )
    
    with open("final_report.html", "w", encoding="utf-8") as f: 
        f.write(html)
        
    print("✨ 报告已成功生成: final_report.html")
    print("提示: 现在报告主体展示的是投标人的实际内容，绿色代表匹配成功的固定格式，黑色代表填写的业务数据。")

if __name__ == "__main__": 
    main()