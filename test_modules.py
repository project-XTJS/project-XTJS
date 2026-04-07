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
    从当前 OCR/解析 JSON 中提取文本。
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


def load_json_file(json_path: str):
    """
    通用 JSON 读取函数
    """
    if not os.path.exists(json_path):
        print(f"错误：找不到文件 '{json_path}'，请确认路径是否正确。")
        return None

    try:
        with open(json_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"解析 JSON 失败: {str(e)}")
        return None


def run_business_tests_with_ocr(json_path: str):
    """
    使用真实 OCR 识别 JSON 结果进行业务模块全量测试。
    """
    res_data = load_json_file(json_path)
    if res_data is None:
        return

    data = res_data.get("data", {})

    # 1. 提取核心文本
    raw_text = extract_text_from_json(res_data)

    # 2. 提取真实的印章与元数据
    real_meta = {
        "seal_count": data.get("seal_count", 0),
        "seal_texts": data.get("seal_texts", []),
        "ocr_used": data.get("ocr_used", True),
    }

    if not raw_text or len(raw_text.strip()) < 5:
        print("警告：提取到的文本几乎为空，请检查 JSON 结构。")
        return

    # 1. 虞光勇、陶明宇 - 完整性与格式审查
    # integrity_res = IntegrityChecker().check_integrity(raw_text)
    # print("【完整性审查结果】")
    # print(json.dumps(integrity_res, indent=4, ensure_ascii=False))

    # 2. 曾俊、滑鹏鹏 - 报价合理性（原逻辑保留）
    print("【报价合理性检查结果】")
    result = ReasonablenessChecker().check_price_reasonableness(res_data)
    print(json.dumps(result, indent=4, ensure_ascii=False))

    # 3. 江宇 - 分项报价
    # print("【分项报价检查结果】")
    # itemized_res = ItemizedPricingChecker().check_itemized_logic(raw_text)
    # print(json.dumps(itemized_res, indent=4, ensure_ascii=False))

    # 4. 高海斌 - 偏离项
    # print("【偏离项检查结果】")
    # deviation_res = DeviationChecker().check_technical_deviation(raw_text)
    # print(json.dumps(deviation_res, indent=4, ensure_ascii=False))

    # 5. 镇昊天、张化飞 - 印章日期
    # print("【印章与日期检查结果】")
    # verification_res = VerificationChecker(ocr_service=None).check_seal_and_date(real_meta)
    # print(json.dumps(verification_res, indent=4, ensure_ascii=False))


def run_tender_bid_price_compare(tender_json_path: str, bid_json_path: str):
    """
    同时读取招标文件 JSON 和投标文件 JSON，
    判断投标总金额是否超过招标文件中的最高限价/预算/控制价。
    """
    tender_data = load_json_file(tender_json_path)
    if tender_data is None:
        print(f"错误：招标文件读取失败 -> {tender_json_path}")
        return

    bid_data = load_json_file(bid_json_path)
    if bid_data is None:
        print(f"错误：投标文件读取失败 -> {bid_json_path}")
        return

    print("【招标限价与投标总金额对比结果】")
    result = ReasonablenessChecker().check_bid_price_against_tender_limit(
        tender_data,
        bid_data
    )
    print(json.dumps(result, indent=4, ensure_ascii=False))


def run_all_tests(
    bid_json_path: str,
    tender_json_path: str = None,
    enable_single_bid_check: bool = True,
    enable_tender_bid_compare: bool = False,
):
    """
    统一入口：
    1. 可执行原来的单文件报价合理性检查
    2. 可执行新增的招标 vs 投标金额对比检查
    """

    if enable_single_bid_check:
        print("=" * 80)
        print("单文件测试：投标文件报价合理性检查")
        print("=" * 80)
        run_business_tests_with_ocr(bid_json_path)
        print()

    if enable_tender_bid_compare:
        if not tender_json_path:
            print("错误：已开启招标/投标对比模式，但未提供 tender_json_path。")
            return

        print("=" * 80)
        print("双文件测试：招标限价 vs 投标总金额")
        print("=" * 80)
        run_tender_bid_price_compare(tender_json_path, bid_json_path)
        print()


if __name__ == "__main__":
    # =========================
    # 你原来的单文件测试路径
    # =========================
    SAMPLE_JSON = "ocr_results/price/10.json"

    # =========================
    # 新增：招标文件 JSON 路径
    # =========================
    TENDER_JSON = "ocr_results/tender/tender.json"

    # =========================
    # 使用方式 1：只跑你原来的单文件逻辑
    # =========================
    # run_all_tests(
    #     bid_json_path=SAMPLE_JSON,
    #     tender_json_path=TENDER_JSON,
    #     enable_single_bid_check=True,
    #     enable_tender_bid_compare=False,
    # )

    # =========================
    # 使用方式 2：只跑招标 vs 投标金额对比
    # =========================
    # run_all_tests(
    #     bid_json_path=SAMPLE_JSON,
    #     tender_json_path=TENDER_JSON,
    #     enable_single_bid_check=False,
    #     enable_tender_bid_compare=True,
    # )

    # =========================
    # 使用方式 3：两个都跑
    # =========================
    run_all_tests(
        bid_json_path=SAMPLE_JSON,
        tender_json_path=TENDER_JSON,
        enable_single_bid_check=True,
        enable_tender_bid_compare=True,
    )