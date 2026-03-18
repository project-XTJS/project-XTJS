# test_modules.py
import sys
import os
import time
import json

# 1. 确保项目根目录在系统路径中，解决跨文件夹导入问题
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

# 引入各业务模块
from app.service.analysis.integrity import IntegrityChecker
from app.service.analysis.pricing_reasonableness import ReasonablenessChecker
from app.service.analysis.itemized_pricing import ItemizedPricingChecker
from app.service.analysis.deviation import DeviationChecker
from app.service.analysis.verification import VerificationChecker

def run_business_tests_with_ocr(json_path: str):
    """
    使用 OCR 识别 JSON 结果进行业务模块全量测试
    """
    if not os.path.exists(json_path):
        print(f"错误：找不到文件 '{json_path}'，请确认文件路径。")
        return
    
    print(f"正在加载 OCR 解析结果: {json_path}")
    start_time = time.time()

    try:
        # 读取 JSON
        with open(json_path, 'r', encoding='utf-8') as f:
            res_data = json.load(f)
        
        # 1. 提取核心文本
        raw_text = res_data.get("data", {}).get("content", "")
        
        # 2. 提取印章与元数据 
        real_meta = {
            "seal_count": res_data.get("data", {}).get("seal_count", 0),
            "seal_texts": res_data.get("data", {}).get("seal_texts", []),
            "ocr_used": res_data.get("data", {}).get("ocr_used", True)
        }
        duration = time.time() - start_time
    except Exception as e:
        print(f"解析 JSON 失败: {str(e)}")
        return

    if not raw_text or len(raw_text.strip()) < 5:
        print("警告：提取到的文本几乎为空，检查 JSON 结构。")
        return
    else:
        #print(f"提取到印章数据: {real_meta['seal_texts']}\n")
        return

    # 1. 虞光勇、陶明宇 - 完整性
    integrity_res = IntegrityChecker().check_integrity(raw_text)
    print(json.dumps(integrity_res, indent=4, ensure_ascii=False))

    # 2. 曾俊、滑鹏鹏 - 报价合理性
    #print(ReasonablenessChecker().check_price_reasonableness(raw_text))

    # 3. 江宇 - 分项报价
    #print(ItemizedPricingChecker().check_itemized_logic(raw_text))

    # 4. 高海斌 - 偏离项
    #print(DeviationChecker().check_technical_deviation(raw_text))

    # 5. 镇昊天、张化飞 - 印章日期
    #print(VerificationChecker(ocr_service=None).check_seal_and_date(real_meta))

if __name__ == "__main__":
    # 指向 JSON 文件
    SAMPLE_JSON = "test.json" 
    run_business_tests_with_ocr(SAMPLE_JSON)