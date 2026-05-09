# -*- coding: utf-8 -*-
"""
全模块功能测试脚本（适配实际文件结构）
自动加载 ocr_results/369/ 下的招标文件及所有商务标文件，
依次测试价格合理性、偏离、一致性、完整性、查重及合并。
"""
import json
import traceback
from pathlib import Path
from pprint import pprint
from typing import List, Dict, Any

# ---------- 尝试导入所有拆分后的服务 ----------
try:
    from app.service.analysis.reasonableness import ReasonablenessChecker
except ImportError:
    print("❌ 无法导入 ReasonablenessChecker，请检查包路径")
    ReasonablenessChecker = None

try:
    from app.service.analysis.deviation import DeviationChecker
except ImportError:
    print("❌ 无法导入 DeviationChecker")
    DeviationChecker = None

try:
    from app.service.analysis.compliance import ConsistencyChecker, IntegrityChecker
except ImportError:
    print("❌ 无法导入 ConsistencyChecker / IntegrityChecker")
    ConsistencyChecker = None
    IntegrityChecker = None

try:
    from app.service.analysis.duplicate_check import DuplicateCheckService
except ImportError:
    print("❌ 无法导入 DuplicateCheckService")
    DuplicateCheckService = None

try:
    from app.service.analysis.duplicate_merge import build_duplicate_merge_results
except ImportError:
    print("❌ 无法导入 build_duplicate_merge_results")
    build_duplicate_merge_results = None

# ---------- 配置 ----------
BASE_DIR = Path("./ocr_results/369")
TENDER_FILE = BASE_DIR / "369-model.json"
# 所有非 model 的 json 文件视为商务标
BID_FILES = sorted(
    [f for f in BASE_DIR.glob("*.json") if f.name != "369-model.json"],
    key=lambda x: x.name
)

def load_json(path: Path) -> dict | None:
    """安全加载 JSON 文件"""
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"❌ 无法加载 {path.name}: {e}")
        return None

def run_reasonableness(tender_json: dict, bid_json: dict, bid_name: str):
    """价格合理性检查"""
    if ReasonablenessChecker is None:
        return "跳过（模块未导入）"
    checker = ReasonablenessChecker(min_float_rate=1.5)
    try:
        # 限价校验
        limit_res = checker.check_bid_price_against_tender_limit(tender_json, bid_json)
        # 合规性校验
        comp_res = checker.check_price_compliance(bid_json)
        return {
            "limit_result": limit_res.get("result"),
            "compliance_result": comp_res.get("result"),
            "compliance_type": comp_res.get("type"),
        }
    except Exception as e:
        return f"异常: {e}"

def run_deviation(tender_json: dict, bid_json: dict, bid_name: str):
    """偏离条款检查"""
    if DeviationChecker is None:
        return "跳过（模块未导入）"
    checker = DeviationChecker()
    try:
        result = checker.check_technical_deviation(tender_json, bid_json)
        return {
            "compliance_status": result.get("compliance_status"),
            "deviation_status": result.get("deviation_status"),
            "star_requirements_count": result.get("core_requirements_count"),
            "stats": {
                "missing": result.get("stats", {}).get("missing_count"),
                "negative": result.get("stats", {}).get("negative_deviation_count"),
                "positive": result.get("stats", {}).get("positive_deviation_count"),
            }
        }
    except Exception as e:
        return f"异常: {e}"

def run_consistency(tender_json: dict, bid_json: dict, bid_name: str):
    """一致性检查"""
    if ConsistencyChecker is None:
        return "跳过（模块未导入）"
    checker = ConsistencyChecker()
    try:
        result = checker.compare_raw_data(tender_json, bid_json)
        passed = sum(1 for item in result if item.get("is_passed"))
        total = len(result)
        return {"passed": passed, "total": total, "missing_anchors_sample": [item.get("missing_anchors") for item in result if not item.get("is_passed")][:2]}
    except Exception as e:
        return f"异常: {e}"

def run_integrity(tender_json: dict, bid_json: dict, bid_name: str):
    """完整性检查"""
    if IntegrityChecker is None:
        return "跳过（模块未导入）"
    checker = IntegrityChecker()
    try:
        result = checker.check_integrity(tender_json, bid_json)
        passed = sum(1 for v in result.get("details", {}).values() if v.get("is_passed"))
        total = len(result.get("details", {}))
        return {"integrity_score": result.get("integrity_score"), "passed": passed, "total": total}
    except Exception as e:
        return f"异常: {e}"

def run_duplicate_check_batch(bid_files: List[Path], tender_json: dict) -> dict:
    """对多个商务标文件执行两两查重"""
    if DuplicateCheckService is None:
        return {"error": "模块未导入"}

    service = DuplicateCheckService()
    records = []
    for idx, path in enumerate(bid_files):
        bid_json = load_json(path)
        if bid_json is None:
            continue
        records.append({
            "identifier_id": f"bid_{idx}",
            "file_name": path.name,
            "content": bid_json,
            "relation_role": "business",       # 假设都是商务标
            "tender_identifier_id": "tender_369",
            "tender_content": tender_json,
        })

    if len(records) < 2:
        return {"error": "至少需要两个商务标文件才能执行查重"}

    try:
        result = service.check_project_documents(
            project_identifier="test_project_369",
            project=None,
            document_records=records,
            document_types=["business_bid"],
            max_evidence_sections=3,
            max_pairs_per_type=10,
        )
        summary = result.get("summary", {})
        groups = result.get("groups", {})
        # 提取关键统计
        return {
            "document_count": summary.get("document_count"),
            "pair_count": summary.get("pair_count"),
            "suspicious_pair_count": summary.get("suspicious_pair_count"),
            "high_risk_pair_count": summary.get("high_risk_pair_count"),
            "medium_risk_pair_count": summary.get("medium_risk_pair_count"),
        }
    except Exception as e:
        return {"error": f"查重异常: {e}"}

def run_duplicate_merge(raw_result: dict):
    """对查重结果进行合并"""
    if build_duplicate_merge_results is None:
        return {"error": "模块未导入"}
    try:
        merged = build_duplicate_merge_results(
            raw_result=raw_result,
            source_result_key="business_bid_duplicate_check",
        )
        # 汇总聚类数量
        summary = {}
        for key, payload in merged.items():
            summary[key] = {
                "cluster_count": payload["summary"].get("cluster_count"),
                "suspicious_cluster_count": payload["summary"].get("suspicious_cluster_count"),
            }
        return summary
    except Exception as e:
        return {"error": f"合并异常: {e}"}

# ---------- 主测试流程 ----------
def main():
    print(f"========================================")
    print(f"全模块功能测试")
    print(f"招标文件: {TENDER_FILE.name}")
    print(f"商务标文件 ({len(BID_FILES)} 个): {[f.name for f in BID_FILES]}")
    print(f"========================================")

    tender_json = load_json(TENDER_FILE)
    if tender_json is None:
        print("❌ 无法加载招标文件，测试终止")
        return

    # 逐个商务标文件测试（单文件模块）
    for bid_path in BID_FILES:
        bid_json = load_json(bid_path)
        if bid_json is None:
            print(f"\n⚠️ 跳过 {bid_path.name} (加载失败)")
            continue

        print(f"\n--- 测试商务标: {bid_path.name} ---")
        # 价格合理性
        res = run_reasonableness(tender_json, bid_json, bid_path.name)
        print(f"  价格合理性: {res}")
        # 偏离
        res = run_deviation(tender_json, bid_json, bid_path.name)
        print(f"  偏离条款: {res}")
        # 一致性
        res = run_consistency(tender_json, bid_json, bid_path.name)
        print(f"  一致性: {res}")
        # 完整性
        res = run_integrity(tender_json, bid_json, bid_path.name)
        print(f"  完整性: {res}")

    # ---------- 查重（多文件） ----------
    print(f"\n========== 查重测试（所有商务标两两比较）==========")
    dup_res = run_duplicate_check_batch(BID_FILES, tender_json)
    print(f"查重结果: {dup_res}")

    # 如果查重成功且 DuplicateCheckService 可用，尝试合并
    if isinstance(dup_res, dict) and "error" not in dup_res:
        # 需要重新调用一次查重获取完整的 raw_result 用于合并
        service = DuplicateCheckService()
        records = []
        for idx, path in enumerate(BID_FILES):
            bid_json = load_json(path)
            if bid_json is None:
                continue
            records.append({
                "identifier_id": f"bid_{idx}",
                "file_name": path.name,
                "content": bid_json,
                "relation_role": "business",
                "tender_identifier_id": "tender_369",
                "tender_content": tender_json,
            })
        if len(records) >= 2:
            raw_result = service.check_project_documents(
                project_identifier="test_project_369_merge",
                project=None,
                document_records=records,
                document_types=["business_bid"],
            )
            merge_res = run_duplicate_merge(raw_result)
            print(f"查重合并结果: {merge_res}")
        else:
            print("文件不足，跳过合并")
    else:
        print("查重失败或文件不足，跳过合并")

    print("\n✅ 所有测试执行完毕")

if __name__ == "__main__":
    main()