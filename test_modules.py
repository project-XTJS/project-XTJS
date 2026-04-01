import json
import os

# 导入改造后的三大模块
from app.service.analysis.template_extractor import TemplateExtractor
from app.service.analysis.integrity import IntegrityChecker
from app.service.analysis.consistency import ConsistencyChecker, DocumentProcessor

def main():
    # 替换为你实际的文件路径（请根据你的本地环境调整）
    model_path = "./ocr_results/427/427-model.json"
    test_path = "./ocr_results/427/427-hongyin.json"

    if not os.path.exists(model_path) or not os.path.exists(test_path):
        print(f"找不到指定的 JSON 文件，请检查路径。")
        return

    print("正在加载 JSON 数据...")
    with open(model_path, 'r', encoding='utf-8') as f:
        model_raw_json = json.load(f)
    with open(test_path, 'r', encoding='utf-8') as f:
        test_raw_json = json.load(f)

    # ========================================================
    # 测试 1：动态白名单提取 (雷达 A：完整性清单)
    # ========================================================
    print("\n" + "="*50)
    print(" 1. 测试动态白名单提取 (用于完整性检查)")
    print("="*50)
    reqs = TemplateExtractor.extract_requirements(model_raw_json)
    print("✅ 提取的主项目录 (Main):")
    for item in reqs['main']: print(f"  - {item}")
    print("\n✅ 提取的子项材料 (Sub):")
    for item in reqs['sub']: print(f"  - {item}")

    # ========================================================
    # 测试 2：完整性检查服务
    # ========================================================
    print("\n" + "="*50)
    print(" 2. 测试完整性检查服务 (Integrity Checker)")
    print("="*50)
    
    integrity_checker = IntegrityChecker() 
    integrity_report = integrity_checker.check_integrity(model_raw_json, test_raw_json)
    
    print(f"📊 完整性合规得分: {integrity_report['integrity_score']} 分")
    print(f"🔍 找到的项目数: {integrity_report['found_count']} 项")
    print(f"❌ 缺失的项目数: {integrity_report['missing_count']} 项")
    
    with open("result_integrity.json", "w", encoding="utf-8") as f:
        json.dump(integrity_report, f, ensure_ascii=False, indent=4)
    print("💾 -> 完整性详细报告已保存至: result_integrity.json")

    # ========================================================
    # 测试 3：一致性模板提取 & 切分导出 (雷达 B：标准模板)
    # ========================================================
    print("\n" + "="*50)
    print(" 3. 测试模板提取与文档切分 (用于一致性检查)")
    print("="*50)
    
    # 独立提取 Model 的真实模板（带内容的附件）
    templates = TemplateExtractor.extract_consistency_templates(model_raw_json)
    print(f"✅ 共提取到 {len(templates)} 个标准附件模板")
    for temp in templates:
        print(f"  - {temp['title']} (模板长度: {len(''.join(temp['content']))} 字符)")
    
    print("\n正在根据真实模板切分文档...")
    m_segs = DocumentProcessor.segment_document(model_raw_json, templates)
    b_segs = DocumentProcessor.segment_document(test_raw_json, templates)

    with open("extracted_model_segments.json", "w", encoding="utf-8") as f:
        json.dump(m_segs, f, ensure_ascii=False, indent=4)
    with open("extracted_test_segments.json", "w", encoding="utf-8") as f:
        json.dump(b_segs, f, ensure_ascii=False, indent=4)
        
    print("💾 -> Model 用于比对的切分段落已保存至: extracted_model_segments.json")
    print("💾 -> Test(投标人) 用于比对的切分段落已保存至: extracted_test_segments.json")

    # ========================================================
    # 测试 4：一致性检查服务
    # ========================================================
    print("\n" + "="*50)
    print(" 4. 测试一致性比对服务 (Consistency Checker)")
    print("="*50)

    consistency_checker = ConsistencyChecker()
    consistency_report = consistency_checker.compare_raw_data(
        model_raw_json, 
        test_raw_json, 
        integrity_report.get('found_sections', []) # 灵魂联动：传入完整性结果
    )
    
    passed_count = sum(1 for r in consistency_report if r['is_passed'])
    print(f"📊 一致性检查完成: 共对比了 {len(consistency_report)} 个模块")
    print(f"✅ 完全一致/免审通过: {passed_count} 个")
    print(f"❌ 存在缺失/篡改: {len(consistency_report) - passed_count} 个")

    with open("result_consistency.json", "w", encoding="utf-8") as f:
        json.dump(consistency_report, f, ensure_ascii=False, indent=4)
    print("💾 -> 一致性检查详细报告已保存至: result_consistency.json")
    print("\n测试执行完毕！✨")

if __name__ == "__main__":
    main()