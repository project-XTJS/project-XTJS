# test_modules.py
import sys
import os
import json
import re

# 确保项目根目录在系统路径中
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

# 引入各业务模块
from app.service.analysis.integrity import IntegrityChecker, TemplateConsistencyChecker

def run_business_tests_with_ocr(bidder_json_path: str, template_json_path: str):
    if not os.path.exists(bidder_json_path) or not os.path.exists(template_json_path):
        print(f"错误：找不到 JSON 文件")
        return

    try:
        # 1. 加载乙方全文
        with open(bidder_json_path, 'r', encoding='utf-8') as f:
            bidder_text = json.load(f).get("data", {}).get("content", "")

        # 2. 加载甲方全文
        with open(template_json_path, 'r', encoding='utf-8') as f:
            template_text = json.load(f).get("data", {}).get("content", "")

    except Exception as e:
        print(f"解析失败: {str(e)}")
        return

    # 1. 全文完整性检查 (虞光勇) - 这个需要看全文
    integrity_res = IntegrityChecker().check_integrity(bidder_text)
    print(json.dumps(integrity_res, indent=4, ensure_ascii=False))

    # 2. 智能切片与防篡改检查
    # 从招标模版中找出《投标保证书》的原版模板
    template_match = re.search(r'附件1\s*投标保证书.*?致（招标人）：(.*?)\(4\)\s*投标有效期', template_text, re.DOTALL)
    template_chunk = template_match.group(1) if template_match else ""

    # 从投标文件中找出《投标保证书》
    bidder_match = re.search(r'一、投标保证书.*?致上海信投建设有限公司：(.*?)\（4\）投标有效期', bidder_text, re.DOTALL)
    bidder_chunk = bidder_match.group(1) if bidder_match else ""

    if not template_chunk or not bidder_chunk:
        print("警告：无法在文本中精确定位到《投标保证书》章节，请检查正则匹配条件。")
    else:
        consistency_res = TemplateConsistencyChecker().check_consistency(template_chunk, bidder_chunk)
        print(json.dumps(consistency_res, indent=4, ensure_ascii=False))


if __name__ == "__main__":
    BIDDER_JSON = os.path.join(CURRENT_DIR, "test.json")
    TEMPLATE_JSON = os.path.join(CURRENT_DIR, "model.json")
    run_business_tests_with_ocr(BIDDER_JSON, TEMPLATE_JSON)