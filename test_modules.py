import json, os
from app.service.analysis.template_extractor import TemplateExtractor
from app.service.analysis.integrity import IntegrityChecker
from app.service.analysis.consistency import ConsistencyChecker, DocumentProcessor
from app.service.analysis.visualizer import ReportVisualizer

def main():
    # 1. 加载文件
    # 🌟 恢复为您之前测试 427 标书稳定的文件路径
    m_path, t_path = "./ocr_results/427/427-model.json", "./ocr_results/427/427-hongyin.json"
    
    if not os.path.exists(m_path) or not os.path.exists(t_path):
        print(f"错误: 找不到文件 {m_path} 或 {t_path}")
        return

    print("正在加载 JSON 数据...")
    with open(m_path, 'r', encoding='utf-8') as f: m_json = json.load(f)
    with open(t_path, 'r', encoding='utf-8') as f: t_json = json.load(f)

    print("正在执行合规性审查...")

    # 2. 完整性检查：核对清单是否缺失
    integrity_report = IntegrityChecker().check_integrity(m_json, t_json)
    
    # 3. 一致性检查：比对模板锚点
    consistency_report = ConsistencyChecker().compare_raw_data(m_json, t_json)
    
    # 4. 获取用于可视化的分段数据
    print("正在切分文档段落...")
    templates = TemplateExtractor.extract_consistency_templates(m_json)
    m_segs = DocumentProcessor.segment_document(m_json, templates) 
    b_segs = DocumentProcessor.segment_document(t_json, templates) 

    # ========================================================
    # 依然保留保存中间提取结果到本地的逻辑，方便人工审查
    # ========================================================
    print("正在保存中间提取结果到本地...")
    
    with open("extracted_model_segments.json", "w", encoding="utf-8") as f:
        json.dump(m_segs, f, ensure_ascii=False, indent=4)
    
    with open("extracted_test_segments.json", "w", encoding="utf-8") as f:
        json.dump(b_segs, f, ensure_ascii=False, indent=4)
        
    print(f"💾 已保存招标文件提取段落: extracted_model_segments.json")
    print(f"💾 已保存投标文件提取段落: extracted_test_segments.json")
    # ========================================================

    # 5. 生成可视化报告
    print("正在生成基于投标文件内容的可视化报告...")
    html = ReportVisualizer().generate_html(
        integrity_report, 
        consistency_report, 
        b_segs, 
        m_segs
    )
    
    with open("final_report.html", "w", encoding="utf-8") as f: 
        f.write(html)
        
    print("✨ 流程执行完毕！报告已成功生成: final_report.html")

if __name__ == "__main__": 
    main()