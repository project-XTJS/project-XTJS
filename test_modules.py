# test_modules.py
import sys
import os
import time

# 1. 确保项目根目录在系统路径中，解决跨文件夹导入问题
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

# 引入项目工具与各业务模块
from app.utils.text_utils import extract_text
from app.service.analysis.integrity import IntegrityChecker
from app.service.analysis.pricing_reasonableness import ReasonablenessChecker
from app.service.analysis.itemized_pricing import ItemizedPricingChecker
from app.service.analysis.deviation import DeviationChecker
from app.service.analysis.verification import VerificationChecker

def run_business_tests(file_path: str):
    """
    业务模块自测函数
    """
    if not os.path.exists(file_path):
        print(f"错误：找不到文件 '{file_path}'，请确认文件已放置在正确路径。")
        return
    
    file_extension = os.path.splitext(file_path)[1].lower().replace('.', '')

    try:
        start_time = time.time()
        raw_text = extract_text(file_path, file_extension)
        duration = time.time() - start_time
    except Exception as e:
        print(f"提取失败！底层解析引擎报错: {str(e)}")
        return

    if not raw_text or len(raw_text.strip()) < 5:
        print("警告：提取到的文本几乎为空。")
    else:
        print(f"文本提取成功共 ({len(raw_text)} 字符)，耗时: {duration:.2f}s")

    # 模拟 OCR 识别出的元数据（用于测试盖章校验模块）
    mock_meta = {
        "seal_count": 1,
        "seal_texts": ["测试单位专用章"],
        "ocr_used": True if file_extension in ['jpg', 'png'] else False
    }

    # 1. 虞光勇、陶明宇 - 完整性
    print(f"\n[测试] 完整性审查")
    print(f"结果: {IntegrityChecker().check_integrity(raw_text)}")

    # 2. 曾俊、滑鹏鹏 - 报价合理性
    print(f"\n[测试] 报价合理性")
    print(f"结果: {ReasonablenessChecker().check_price_reasonableness(raw_text)}")

    # 3. 江宇 - 分项报价
    print(f"\n[测试] 分项报价表")
    print(f"结果: {ItemizedPricingChecker().check_itemized_logic(raw_text)}")

    # 4. 高海斌 - 偏离项
    print(f"\n[测试] 偏离条款检查")
    print(f"结果: {DeviationChecker().check_technical_deviation(raw_text)}")

    # 5. 镇昊天、张化飞 - 印章日期
    print(f"\n[测试] 签字盖章与日期")
    # Verification 模块需要 meta 数据支持
    print(f"结果: {VerificationChecker(ocr_service=None).check_seal_and_date(mock_meta)}")

if __name__ == "__main__":
    # 修改路径为实际测试文件位置，确保文件存在
    SAMPLE_FILE = "test.pdf" 
    run_business_tests(SAMPLE_FILE)