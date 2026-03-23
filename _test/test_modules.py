import os
import sys
import json
import re

# =====================================================================
# 1. 环境与路径配置
# =====================================================================
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR) 
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

# 导入独立的核查引擎
try:
    from app.service.analysis.integrity import IntegrityChecker
    from app.service.analysis.consistency import TemplateAnalysisService
except ImportError as e:
    print(f"\033[31m❌ 导入错误：{e}\033[0m")
    sys.exit(1)

# ANSI 颜色与 UI 工具
class Color:
    CYAN, GREEN, RED, YELLOW, BOLD, END = '\033[96m', '\033[92m', '\033[91m', '\033[93m', '\033[1m', '\033[0m'

# =====================================================================
# 2. 文本对齐与截断工具
# =====================================================================
def get_width(s):
    """计算中英文混合字符串的视觉宽度"""
    return sum(2 if ord(c) > 0x7f else 1 for c in s)

def truncate_text(text, max_width):
    """
    智能截断函数：如果文本显示宽度超过 max_width，则截断并加省略号
    """
    if get_width(text) <= max_width:
        return text
    
    current_width = 0
    truncated = ""
    # 预留 3 个位置给 "..."
    limit = max_width - 3
    for char in text:
        char_w = 2 if ord(char) > 0x7f else 1
        if current_width + char_w > limit:
            break
        truncated += char
        current_width += char_w
    return truncated + "..."

def align(text, width):
    """视觉对齐填充"""
    return text + ' ' * (width - get_width(text))

# =====================================================================
# 3. 主执行函数
# =====================================================================
def run_separated_tests():
    # 文件路径 (指向原始 OCR 生肉 JSON)
    RAW_MODEL = os.path.join(PROJECT_ROOT, "model.json")
    RAW_BIDDER = os.path.join(PROJECT_ROOT, "test.json")

    if not all(os.path.exists(p) for p in [RAW_MODEL, RAW_BIDDER]):
        print(f"{Color.RED}❌ 错误：在根目录未找到原始 JSON 文件。{Color.END}")
        return

    # 加载数据
    with open(RAW_MODEL, 'r', encoding='utf-8') as f: m_raw = json.load(f)
    with open(RAW_BIDDER, 'r', encoding='utf-8') as f: b_raw = json.load(f)
    bidder_text = b_raw.get("data", {}).get("content", "")

    # 实例化引擎
    i_checker = IntegrityChecker()
    c_service = TemplateAnalysisService()

    # -----------------------------------------------------------------
    # 阶段一：完整性检查 (Integrity)
    # -----------------------------------------------------------------
    print(f"\n{Color.BOLD}{Color.CYAN}╔" + "═"*76 + "╗")
    print("║" + " "*23 + "第一阶段：商务标材料完整性核查" + " "*23 + "║")
    print("╚" + "═"*76 + "╝" + Color.END)
    
    i_res = i_checker.check_integrity(bidder_text) 
    
    # 定义完整性表格列宽
    W_ID, W_CAT, W_NAME, W_STAT = 4, 16, 32, 10
    print(f"  {Color.BOLD}{align('序号', W_ID)} | {align('类别', W_CAT)} | {align('核查项', W_NAME)} | 状态{Color.END}")
    print("  " + "─" * 74)

    idx = 1
    for section, detail in i_res["details"].items():
        c = Color.GREEN if detail["status"] == "已找到" else Color.RED if detail["status"] == "缺失" else Color.YELLOW
        # 截断过长的核查项名称
        display_name = truncate_text(section, W_NAME)
        print(f"  {idx:02d}   | {align(detail['category'], W_CAT)} | {align(display_name, W_NAME)} | {c}{detail['status']}{Color.END}")
        idx += 1

    print("  " + "─" * 74)
    print(f"  {Color.BOLD}阶段汇总：{Color.END}已找到 {i_res['found_count']} 项，缺失 {i_res['missing_count']} 项。")

    # -----------------------------------------------------------------
    # 阶段二：格式一致性检查 (Consistency)
    # -----------------------------------------------------------------
    print(f"\n{Color.BOLD}{Color.YELLOW}╔" + "═"*76 + "╗")
    print("║" + " "*23 + "第二阶段：模板格式一致性核查" + " "*25 + "║")
    print("╚" + "═"*76 + "╝" + Color.END)

    c_reports = c_service.compare_raw_data(m_raw, b_raw)
    
    # 定义一致性表格列宽
    W_CON_NAME, W_CON_STAT, W_RATE = 36, 12, 10
    print(f"  {Color.BOLD}{align('核查模块', W_CON_NAME)} | {align('核查状态', W_CON_STAT)} | 匹配率{Color.END}")
    print("  " + "─" * 74)

    for r in c_reports:
        c = Color.GREEN if r['is_passed'] else Color.RED
        status_txt = "✔ 合规" if r['is_passed'] else "✘ 异常"
        # 截断模块名称
        display_module = truncate_text(r['name'], W_CON_NAME)
        
        print(f"  {align(display_module, W_CON_NAME)} | {c}{align(status_txt, W_CON_STAT)}{Color.END} | {c}{r['match_rate']:>6.1%}{Color.END}")
        
        # 处理差异点说明文字（若有）
        if not r['is_passed']:
            issues_text = "，".join(r['missing_segments'])
            # 差异文字限制在 60 个字符宽
            display_issues = truncate_text(issues_text, 60)
            print(f"       {Color.RED}╰── 差异文字: {display_issues}{Color.END}")

    print("  " + "─" * 74)
    print(f"  {Color.BOLD}阶段汇总：{Color.END}一致性核查已完成，请针对异常项核对原文。\n")

if __name__ == "__main__":
    run_separated_tests()