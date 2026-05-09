# -*- coding: utf-8 -*-
import json
from pathlib import Path
from pprint import pprint

# 确保导入路径指向你重构后的包
from app.service.analysis.pricing_reasonableness import ReasonablenessChecker

# ================= 配置：卫星项目路径 =================
TENDER_PATH = Path("./ocr_results/卫星/招标.json")
BUSINESS_BID_FILES = [
    Path("./ocr_results/369/369-huolaiwo.json")
]
# =====================================================

def load_json(path: Path):
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)

def run_reasonableness_test():
    print(f"🔍 开始价格合理性专项测试: {TENDER_PATH.parent.name}")
    
    if not TENDER_PATH.exists():
        print(f"❌ 错误: 招标文件不存在: {TENDER_PATH}")
        return

    # 1. 加载招标文件 OCR 结果
    tender_json = load_json(TENDER_PATH)
    
    # 2. 初始化检查器
    # 设置兜底下浮率阈值为 1.5%
    checker = ReasonablenessChecker(min_float_rate=1.5) 

    for bid_path in BUSINESS_BID_FILES:
        if not bid_path.exists():
            print(f"⚠️ 警告: 投标文件未找到: {bid_path.name}")
            continue

        print(f"\n--- 处理投标文件: {bid_path.name} ---")
        bidder_json = load_json(bid_path)

        # A. 招标限价校验 (Tender vs Bidder)
        # 提取招标文件中的最高限价并与投标总额比对
        print("1. 执行最高限价校验...")
        limit_res = checker.check_bid_price_against_tender_limit(tender_json, bidder_json)
        pprint(limit_res)

        # B. 报价合规性校验 (Bidder Only)
        # 自动识别直接报价或下浮率模式，检查大小写一致性或下浮率规则
        print("\n2. 执行报价合规性校验...")
        compliance_res = checker.check_price_compliance(bidder_json)
        pprint(compliance_res)

if __name__ == "__main__":
    run_reasonableness_test()