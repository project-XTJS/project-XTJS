# test_modules.py
import sys
import os
import json

# 确保项目根目录在系统路径中，解决跨文件夹导入问题
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

# 引入各业务模块
from app.service.analysis.integrity import IntegrityChecker
from app.service.analysis.pricing_reasonableness import ReasonablenessChecker
from app.service.analysis.itemized_pricing import ItemizedPricingChecker
from app.service.analysis.deviation import DeviationChecker
from app.service.analysis.verification import VerificationChecker


def extract_text_from_json(res_data: dict) -> str:
    """
    从当前 OCR/解析 JSON 中提取文本

    这里专门保持 layout_sections 的原始顺序，
    不再手动重排 layout_sections / table_sections，
    以免破坏原始阅读顺序，影响开标一览表截取逻辑。
    """
    data = res_data.get("data", {})
    layout_sections = data.get("layout_sections", [])

    text_parts = []

    if isinstance(layout_sections, list):
        for sec in layout_sections:
            if not isinstance(sec, dict):
                continue

            txt = sec.get("text", "")
            if txt and str(txt).strip():
                text_parts.append(str(txt).strip())

    return "\n".join(text_parts).strip()


def run_business_tests_with_ocr(json_path: str):
    """
    使用真实 OCR 识别 JSON 结果进行业务模块全量测试
    这里只输出最终业务结果
    """
    if not os.path.exists(json_path):
        print(f"错误：找不到文件 '{json_path}'，请确认文件是否在项目根目录下。")
        return

    try:
        # 读取 JSON
        with open(json_path, 'r', encoding='utf-8') as f:
            res_data = json.load(f)

        data = res_data.get("data", {})

        # 1. 提取核心文本
        raw_text = extract_text_from_json(res_data)

        # 2. 提取真实的印章与元数据
        real_meta = {
            "seal_count": data.get("seal_count", 0),
            "seal_texts": data.get("seal_texts", []),
            "ocr_used": data.get("ocr_used", True)
        }

    except Exception as e:
        print(f"解析 JSON 失败: {str(e)}")
        return

    if not raw_text or len(raw_text.strip()) < 5:
        print("警告：提取到的文本几乎为空，检查 JSON 结构。")
        return

    # 1. 虞光勇、陶明宇 - 完整性与格式审查
    # integrity_res = IntegrityChecker().check_integrity(raw_text)
    # print(json.dumps(integrity_res, indent=4, ensure_ascii=False))

    # 2. 曾俊、滑鹏鹏 -
    result = ReasonablenessChecker().check_price_reasonableness(raw_text)
    print(result)

    # 3. 江宇 - 分项报价
    # print(ItemizedPricingChecker().check_itemized_logic(raw_text))

    # 4. 高海斌 - 偏离项
    # print(DeviationChecker().check_technical_deviation(raw_text))

    # 5. 镇昊天、张化飞 - 印章日期
    # print(VerificationChecker(ocr_service=None).check_seal_and_date(real_meta))


if __name__ == "__main__":
    # 按你的实际文件名修改
    SAMPLE_JSON = "3.json"
    run_business_tests_with_ocr(SAMPLE_JSON)

