import json
import os
from app.service.analysis.consistency import TemplateAnalysisService

def run_test():
    model_path = 'model.json'    
    response_path = 'response.json'   

    if not os.path.exists(model_path) or not os.path.exists(response_path):
        print(f"错误：请确保根目录下存在 {model_path} 和 {response_path}")
        return

    with open(model_path, 'r', encoding='utf-8') as f1, \
         open(response_path, 'r', encoding='utf-8') as f2:
        model_json = json.load(f1)
        response_json = json.load(f2)

    service = TemplateAnalysisService()
    results = service.compare_raw_data(model_json, response_json)

    print("\n" + "="*70)
    print(" 商务标合规性审查 (严苛模式：零容忍) ".center(65, " "))
    print("="*70)

    for r in results:
        # 统一使用 missing_segments_count
        status = "✅ 通过" if r["is_passed"] else "❌ 异常"
        # 修改这里：
        print(f"[{r['index']:02d}] {r['name']:<20} {status} (未匹配: {r['missing_segments_count']} 个)")
        
        # 修改这里：
        if not r["is_passed"] and r["missing_segments_count"] > 0:
            # 直接把有问题的内容回显出来
            missing_text = "、".join([f"「{a}」" for a in r["missing_anchors"]])
            print(f"     └─ ⚠️ 缺失内容: {missing_text}")
            
    print("="*70 + "\n")

if __name__ == "__main__":
    run_test()