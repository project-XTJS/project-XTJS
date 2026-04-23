import json
import os
from app.service.analysis.template_extractor import TemplateExtractor
from app.service.analysis.integrity import IntegrityChecker
from app.service.analysis.consistency import ConsistencyChecker, DocumentProcessor
from app.service.analysis.visualizer import ReportVisualizer
from app.service.analysis.deviation import DeviationChecker
from app.service.analysis.itemized_pricing import ItemizedPricingChecker
from app.service.analysis.pricing_reasonableness import ReasonablenessChecker
from app.service.analysis.verification import VerificationChecker

def generate_report_for_bidder(tender_json, bidder_json_path, output_html_path, all_bidder_infos):
    """
    针对单个投标文件生成审查报告，并传入所有投标文件信息用于左侧切换菜单。
    all_bidder_infos: 列表，每个元素为 {'name': 显示名, 'filename': 文件名, 'url': 报告链接}
    """
    print(f"正在处理投标文件: {bidder_json_path}")
    with open(bidder_json_path, 'r', encoding='utf-8') as f:
        bidder_json = json.load(f)

    # 完整性
    integrity_checker = IntegrityChecker()
    integrity_report = integrity_checker.check_integrity(tender_json, bidder_json)

    # 一致性
    cons_checker = ConsistencyChecker()
    consistency_report = cons_checker.compare_raw_data(tender_json, bidder_json)

    # 偏离条款
    dev_checker = DeviationChecker()
    deviation_report = dev_checker.check_technical_deviation(tender_json, bidder_json)

    # 分项报价
    price_checker = ItemizedPricingChecker()
    pricing_report = price_checker.check_itemized_logic(bidder_json, tender_text=tender_json)

    # 报价合理性
    reason_checker = ReasonablenessChecker()
    limit_report = reason_checker.check_bid_price_against_tender_limit(tender_json, bidder_json)
    compliance_report = reason_checker.check_price_compliance(bidder_json)
    reasonableness_final = [limit_report, compliance_report]

    # 签字、盖章、落款日期
    verification_checker = VerificationChecker(None)
    verification_report = verification_checker.check_seal_and_date(tender_json, bidder_json)

    # 提取分段文本
    temps = TemplateExtractor.extract_consistency_templates(tender_json)
    m_segs = [{"title": t['title'], "text": "\n".join(t['content'])} for t in temps]
    b_segs = DocumentProcessor.segment_document(bidder_json, temps, is_test_file=True)

    # 构建左侧切换菜单所需数据
    switcher_info = {
        'current_file': os.path.basename(bidder_json_path),
        'files': all_bidder_infos
    }

    # 生成 HTML
    visualizer = ReportVisualizer()
    html_report = visualizer.generate_html(
        integrity_report=integrity_report,
        consistency_report=consistency_report,
        test_segments=b_segs,
        model_segments=m_segs,
        deviation_report=deviation_report,
        pricing_report=pricing_report,
        reasonableness_report=reasonableness_final,
        verification_report=verification_report,
        file_switcher_info=switcher_info
    )

    with open(output_html_path, "w", encoding="utf-8") as f:
        f.write(html_report)
    print(f"  已生成报告: {output_html_path}")

def main():
    # 配置路径
    # tender_path = "./ocr_results/369/369-model.json"
    # bidder_files = [
    #     "./ocr_results/369/369-sangwu.json",
        # "./ocr_results/369/369-huolaiwo.json"
    # ]
    # # 配置路径
    # tender_path = "./药品JSON识别结果/招标.JSON"
    # bidder_files = [
    #     "./药品JSON识别结果/宏银商务标.JSON",
        # "./药品JSON识别结果/戎元商务标.JSON",
        # "./药品JSON识别结果/舒源商务标.JSON",
    # ]
    # 配置路径
    tender_path = "./出口退税/招标文件.json"
    bidder_files = [
        "./出口退税/征盛商务标.json",
        # "./出口退税/智税商务标.json",
        # "./出口退税/链坤商务标.json"
    ]
    output_dir = "."
    os.makedirs(output_dir, exist_ok=True)

    if not os.path.exists(tender_path):
        print(f"错误: 招标文件不存在: {tender_path}")
        return

    with open(tender_path, 'r', encoding='utf-8') as f:
        tender_json = json.load(f)

    # 预先构建所有投标文件的切换信息列表
    all_infos = []
    for bf in bidder_files:
        base = os.path.splitext(os.path.basename(bf))[0]   # 如 "369-sangwu"
        # 尝试从 JSON 中提取原始 PDF 文件名（若存在）
        try:
            with open(bf, 'r', encoding='utf-8') as f:
                data = json.load(f)
            # 假设 OCR 结果中有 'filename' 字段（根据实际结构调整）
            display_name = data.get('filename', base)
        except:
            display_name = base
        report_name = f"report_{base}.html"
        all_infos.append({
            'name': display_name,
            'filename': base,
            'url': report_name
        })

    # 逐个生成报告，并标记当前激活项
    for idx, bf in enumerate(bidder_files):
        base = os.path.splitext(os.path.basename(bf))[0]
        output_html = os.path.join(output_dir, f"report_{base}.html")
        # 复制一份 infos，并设置当前文件的 active 为 True
        infos_for_this = []
        for info in all_infos:
            info_copy = info.copy()
            info_copy['active'] = (info['filename'] == base)
            infos_for_this.append(info_copy)

        generate_report_for_bidder(tender_json, bf, output_html, infos_for_this)

    print("\n全部报告生成完成。")
    print(f"报告目录: {output_dir}")
    print("打开任意报告，左侧悬浮菜单可快速切换不同投标文件。")

if __name__ == "__main__":
    main()
