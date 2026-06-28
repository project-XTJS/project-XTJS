# -*- coding: utf-8 -*-
"""MinIO 对象生命周期（冷热分层 + 自动过期）配置脚本。

结合本项目对象布局，对“派生可重建”的临时对象设置短 TTL 自动过期，对“原件/外置主数据”
保持长期保留，并为合规留存/冷层转储提供可选规则与说明。

重要前提（与 DB 瘦身 Feature 4/5 的关系）：
  - `JSON识别/content/`、`<项目>/JSON识别/result.json.gz` 在 DB 瘦身后是 OCR 内容/分析结果的
    **主存储**（数据库内联列已置 NULL），**默认不过期**，否则会造成数据丢失、需重新 OCR。
  - 真正可安全过期的是“纯派生缓存”：预览图 `cache/previews/`、其它 `cache/` 临时对象，
    以及未完成的分段上传（AbortIncompleteMultipartUpload）。
  - 原件 `<项目>/招标文件/`、`<项目>/投标文件/` 属法定留存，默认长期保留；如需冷层转储，
    用 `--enable-cold --cold-tier <名称>` 启用 Transition（需先用 `mc ilm tier add` 配好远端冷层）。

用法：
    python tools/setup_minio_lifecycle.py                 # 应用默认规则（仅过期派生缓存）
    python tools/setup_minio_lifecycle.py --preview-ttl 7 # 自定义预览缓存过期天数
    python tools/setup_minio_lifecycle.py --expire-json-recognition --json-ttl 30
                                                          # 显式同意把 JSON识别 也设为可过期（谨慎）
    python tools/setup_minio_lifecycle.py --enable-cold --cold-tier COLD --originals-cold-days 90
    python tools/setup_minio_lifecycle.py --show          # 仅打印当前已生效规则
    python tools/setup_minio_lifecycle.py --dry-run       # 打印将要写入的规则但不应用
"""

import argparse
import sys
from pathlib import Path

# 确保项目根目录在搜索路径中
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from minio.commonconfig import ENABLED, Filter  # noqa: E402
from minio.lifecycleconfig import (  # noqa: E402
    Expiration,
    LifecycleConfig,
    Rule,
    Transition,
)

from app.config.settings import settings  # noqa: E402
from app.service.minio_service import MinioService  # noqa: E402

# 预览缓存对象前缀（与运行时配置保持一致）
PREVIEW_PREFIX = (str(getattr(settings, "XTJS_CACHE_PREVIEW_OBJECT_PREFIX", "cache/previews")).strip() or "cache/previews").rstrip("/") + "/"


def build_lifecycle_config(args) -> LifecycleConfig:
    """根据参数构建生命周期规则集合。"""
    rules: list[Rule] = []

    # 1) 预览图缓存：短 TTL 过期（可按需重算）
    rules.append(Rule(
        ENABLED,
        rule_filter=Filter(prefix=PREVIEW_PREFIX),
        rule_id="expire-preview-cache",
        expiration=Expiration(days=int(args.preview_ttl)),
    ))

    # 2) 其它 cache/ 临时对象：短 TTL 过期
    rules.append(Rule(
        ENABLED,
        rule_filter=Filter(prefix="cache/"),
        rule_id="expire-generic-cache",
        expiration=Expiration(days=int(args.preview_ttl)),
    ))

    # 注：未完成分段上传的清理（AbortIncompleteMultipartUpload）在当前 MinIO 版本上
    # 序列化会被服务端按 XML schema 拒绝，故不在此处下发；如需可用 `mc ilm` 单独配置。

    # 4)（可选，谨慎）把 JSON识别 主存储也设为过期——仅在显式同意时启用
    if args.expire_json_recognition:
        rules.append(Rule(
            ENABLED,
            rule_filter=Filter(prefix="JSON识别/"),
            rule_id="expire-json-recognition",
            expiration=Expiration(days=int(args.json_ttl)),
        ))

    # 5)（可选）原件转冷层：需先配置好远端冷层（mc ilm tier add）
    if args.enable_cold:
        if not args.cold_tier:
            raise SystemExit("--enable-cold 需同时提供 --cold-tier <冷层名称>")
        for prefix, rid in (("招标文件/", "cold-tender"), ("投标文件/", "cold-bid")):
            rules.append(Rule(
                ENABLED,
                rule_filter=Filter(prefix=prefix),
                rule_id=rid,
                transition=Transition(days=int(args.originals_cold_days), storage_class=args.cold_tier),
            ))

    return LifecycleConfig(rules)


def _print_rules(config: LifecycleConfig) -> None:
    for rule in config.rules:
        flt = rule.rule_filter
        prefix = getattr(flt, "prefix", "") if flt else ""
        parts = [f"id={rule.rule_id}", f"status={rule.status}", f"prefix='{prefix}'"]
        if rule.expiration is not None:
            parts.append(f"expire_days={rule.expiration.days}")
        if rule.transition is not None:
            parts.append(f"transition_days={rule.transition.days}->{rule.transition.storage_class}")
        if rule.abort_incomplete_multipart_upload is not None:
            parts.append(f"abort_mpu_days={rule.abort_incomplete_multipart_upload.days_after_initiation}")
        print("  - " + ", ".join(parts))


def main() -> int:
    parser = argparse.ArgumentParser(description="配置 MinIO 对象生命周期（冷热分层 + 过期）")
    parser.add_argument("--preview-ttl", type=int, default=14, help="预览/缓存对象过期天数，默认 14")
    parser.add_argument("--expire-json-recognition", action="store_true",
                        help="谨慎：把 JSON识别/ 主存储也设为可过期（会丢失外置的 OCR/结果，需重算）")
    parser.add_argument("--json-ttl", type=int, default=30, help="JSON识别 过期天数（仅在上面开启时生效）")
    parser.add_argument("--enable-cold", action="store_true", help="为原件启用转冷层 Transition")
    parser.add_argument("--cold-tier", default="", help="冷层名称（mc ilm tier add 配置的名称）")
    parser.add_argument("--originals-cold-days", type=int, default=90, help="原件多少天后转冷层，默认 90")
    parser.add_argument("--show", action="store_true", help="仅打印当前桶已生效的生命周期规则")
    parser.add_argument("--dry-run", action="store_true", help="打印将写入的规则但不应用")
    args = parser.parse_args()

    oss = MinioService()
    bucket = oss.bucket_name
    oss.ensure_bucket()

    if args.show:
        try:
            current = oss.client.get_bucket_lifecycle(bucket)
        except Exception as exc:  # noqa: BLE001
            print(f"读取生命周期失败：{exc}")
            return 1
        if not current or not getattr(current, "rules", None):
            print(f"桶 {bucket} 当前没有生命周期规则。")
            return 0
        print(f"桶 {bucket} 当前生命周期规则：")
        _print_rules(current)
        return 0

    config = build_lifecycle_config(args)
    print(f"将为桶 {bucket} 写入以下生命周期规则：")
    _print_rules(config)
    print(f"  预览前缀={PREVIEW_PREFIX}")
    if not args.expire_json_recognition:
        print("  注意：JSON识别/（OCR 内容/结果主存储）未设过期，避免数据丢失。")

    if args.dry_run:
        print("[dry-run] 未实际写入。")
        return 0

    oss.client.set_bucket_lifecycle(bucket, config)
    print("生命周期规则已应用。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
