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

try:
    from app.service.analysis.integrity import IntegrityChecker
    from app.service.analysis.consistency import TemplateAnalysisService, DocumentProcessor
except ImportError as e:
    print(f"\033[31m❌ 导入错误：{e}\033[0m")
    sys.exit(1)

# ANSI 颜色与 UI 工具
class Color:
    CYAN, GREEN, RED, YELLOW, GRAY, BOLD, END = '\033[96m', '\033[92m', '\033[91m', '\033[93m', '\033[90m', '\033[1m', '\033[0m'

def get_width(s):
    return sum(2 if ord(c) > 0x7f else 1 for c in s)

def truncate_text(text, max_width):
    if get_width(text) <= max_width: return text
    current_width, truncated = 0, ""
    for char in text:
        char_w = 2 if ord(char) > 0x7f else 1
        if current_width + char_w > max_width - 3: break
        truncated += char
        current_width += char_w
    return truncated + "..."

def align(text, width):
    return text + ' ' * (width - get_width(text))

# =====================================================================
# 3. 主执行函数
# =====================================================================
def run_separated_tests():
    RAW_MODEL = os.path.join(PROJECT_ROOT, "model.json")
    RAW_BIDDER = os.path.join(PROJECT_ROOT, "test.json")
    if not os.path.exists(RAW_BIDDER): RAW_BIDDER = os.path.join(PROJECT_ROOT, "response.json")

    if not all(os.path.exists(p) for p in [RAW_MODEL, RAW_BIDDER]):
        print(f"{Color.RED}❌ 错误：在根目录未找到原始 JSON 文件。{Color.END}")
        return

    with open(RAW_MODEL, 'r', encoding='utf-8') as f: m_raw = json.load(f)
    with open(RAW_BIDDER, 'r', encoding='utf-8') as f: b_raw = json.load(f)

    # 实例化引擎 (需要调用它的文本清洗方法)
    i_checker = IntegrityChecker()
    c_service = TemplateAnalysisService()

    # =================================================================
    # 🟢 新增：保存实际用于比对的“纯净骨架”中间文本
    # =================================================================
    print(f"\n{Color.BOLD}{Color.GRAY}正在提取版面文本并生成比对专用中间文件 (已剥离括号与标点)...{Color.END}")
    m_segs = DocumentProcessor.extract_segments(m_raw)
    b_segs = DocumentProcessor.extract_segments(b_raw)
    
    # 使用一致性引擎的逻辑，处理出真正用于比对的文本
    for m in m_segs:
        # clean_text: 去除所有括号后的可读文本
        m['clean_text'] = c_service._remove_brackets(m['text'])
        # compare_anchors: 模板最终被切割成的固定文字锚点列表
        m['compare_anchors'] = c_service._get_anchors(m['text'])
        
    for b in b_segs:
        b['clean_text'] = c_service._remove_brackets(b['text'])
        # compare_normalized_text: 标书被剥离所有符号后的连体纯文字骨架
        b['compare_normalized_text'] = c_service._normalize(b['text'])

    OUT_M = os.path.join(PROJECT_ROOT, "model_extracted.json")
    OUT_B = os.path.join(PROJECT_ROOT, "response_extracted.json")
    
    with open(OUT_M, 'w', encoding='utf-8') as f:
        json.dump(m_segs, f, ensure_ascii=False, indent=2)
    with open(OUT_B, 'w', encoding='utf-8') as f:
        json.dump(b_segs, f, ensure_ascii=False, indent=2)
        
    print(f"{Color.GREEN}💾 模板骨架锚点已保存至: {OUT_M}{Color.END}")
    print(f"{Color.GREEN}💾 标书纯文字骨架已保存至: {OUT_B}{Color.END}")
    # =================================================================

    # --- 阶段一：完整性检查 ---
    print(f"\n{Color.BOLD}{Color.CYAN}╔" + "═"*76 + "╗")
    print("║" + " "*23 + "第一阶段：商务标材料完整性核查" + " "*23 + "║")
    print("╚" + "═"*76 + "╝" + Color.END)
    
    i_res = i_checker.check_integrity(b_raw) 
    W_ID, W_CAT, W_NAME, W_STAT = 4, 16, 32, 10
    print(f"  {Color.BOLD}{align('序号', W_ID)} | {align('类别', W_CAT)} | {align('核查项', W_NAME)} | 状态{Color.END}")
    print("  " + "─" * 74)

    idx = 1
    for section, detail in i_res["details"].items():
        c = Color.GREEN if detail["status"] == "已找到" else Color.RED if detail["status"] == "缺失" else Color.YELLOW
        print(f"  {idx:02d}   | {align(detail['category'], W_CAT)} | {align(truncate_text(section, W_NAME), W_NAME)} | {c}{detail['status']}{Color.END}")
        idx += 1

    # --- 阶段二：一致性检查 ---
    print(f"\n{Color.BOLD}{Color.YELLOW}╔" + "═"*76 + "╗")
    print("║" + " "*23 + "第二阶段：模板格式一致性核查" + " "*25 + "║")
    print("╚" + "═"*76 + "╝" + Color.END)

    c_reports = c_service.compare_raw_data(m_raw, b_raw)
    W_CON_NAME, W_CON_STAT = 36, 12
    print(f"  {Color.BOLD}{align('核查模块', W_CON_NAME)} | {align('核查状态', W_CON_STAT)} | 匹配率{Color.END}")
    print("  " + "─" * 74)

    for r in c_reports:
        c = Color.GREEN if r['is_passed'] else Color.RED
        status_txt = "✔ 合规" if r['is_passed'] else "✘ 异常"
        print(f"  {align(truncate_text(r['name'], W_CON_NAME), W_CON_NAME)} | {c}{align(status_txt, W_CON_STAT)}{Color.END} | {c}{r['match_rate']:>6.1%}{Color.END}")
        
        # 输出高亮对比文本
        if not r['is_passed'] and r['diff_snippets']:
            for snip in r['diff_snippets'][:3]: 
                print(f"       {Color.GRAY}╰── 对比: {Color.END}{snip}")

    print("  " + "─" * 74)
    print(f"  {Color.BOLD}说明：红色 [- -] 为原文被删改处，绿色 [+ +] 为投标方私自添加处。{Color.END}\n")

if __name__ == "__main__":
    run_separated_tests()