# 重复内容错别字 v4：离线训练与验收

v4 默认关闭。现网未设置 `TYPO_PIPELINE_VERSION` 时仍执行 v3；不得把训练输出直接挂到现网。v4 不读取 `word_rules.json`，不会展示或导出未确认候选。

## 1. CPU 隔离训练

入口：`tools/train_typo_verifier_v4.py`。只读取公开 `train` 和 `dev`，不会读取 `test`；最多 2 epoch。先用 `--max-train-records 16 --max-dev-records 16 --epochs 1` 做烟测；正式训练不传数量限制。建议在独立容器中使用 `--network none --cpus 2 --memory 8g --memory-swap 8g`，不传 `--gpus`，把仓库及公开数据只读挂载，输出挂载到独立目录。成功时生成 `model.safetensors`、tokenizer 和 `model-manifest.json`。测试集不得用于训练或选检查点。

判别器将原句上下文和“只替换这一字”的候选上下文组成配对输入；`[unused1]` / `[unused2]` 是位置标记，必须作为完整 token 编码。模型加载时会校验这一点。

## 2. 隔离 v4 服务与开发集校准

在与生产服务不同的容器、端口和 SQLite 缓存中设置：

```text
TYPO_PIPELINE_VERSION=duplicate-typo-macbert-cec3-v4
TYPO_VERIFIER_MODEL_PATH=/models/verifier
TYPO_V4_EVAL_TRACE=true
```

挂载原 MacBERT、CEC3 和训练出的 verifier 模型目录。仅低峰运行 CEC3 离线评测；在线 `/health` 异常或 GPU 显存余量不足时停止。评测 trace 包含隐藏候选和分数，**只能保存在离线受控目录**，不得接入线上 API/前端或导出。线上发布时 `TYPO_V4_EVAL_TRACE` 必须为 `false`。

用 `tools/evaluate_duplicate_typo_candidates.py PUBLIC.jsonl --split dev --service-url http://127.0.0.1:PORT --save-results DEV-TRACE.jsonl --online-container xtjs-typo --min-gpu-free-mib N --throttle-seconds 0.2` 保存一次开发集回放；每块推理前检查现网健康及显存余量。中断后对同一文件加 `--resume-results` 只续跑未检查记录。`python3 -m tools.calibrate_typo_v4 DEV-TRACE.jsonl --output DEV-CALIBRATION.json` 在固定网格搜索 CEC3 支持/不支持两档阈值；取得同证据耗时报告后可再传 `--latency-report LATENCY.json` 核验。校准器拒绝非 `dev` 数据。不能把同一模型的两侧重复文本当成两份独立证据。

耗时回放分两阶段：先用 `tools/compare_typo_v4_latency.py EVIDENCE.jsonl --phase v3-baseline --service-url V3_URL --online-health-url ONLINE_URL --min-gpu-free-mib N --output V3-LATENCY.json` 测隔离 v3 的冷、热缓存；**停止该隔离 v3 服务并释放显存**，再用同一证据运行 `--phase v4-candidate --service-url V4_URL --baseline-report V3-LATENCY.json --output V4-LATENCY.json`（同时带健康与显存参数）。工具验证 v3 服务已停止、证据哈希和顺序一致、冷/热缓存状态正确；不满足 `v4 ≤ 1.5 × v3` 即失败。用于校准的耗时报告必须含 `same_evidence=true` 与 `cold_warm_matched=true`。

## 3. 冻结与发布门槛

先冻结模型哈希、两档分数门槛、提示词和词典版本，再仅一次运行独立 `test`。用 `--save-results` 保存测试回放，之后只针对保存文件评估；传入 `--v3-baseline-report`、`--by-source` 和 `--enforce-gate`。最低要求：真实错字 ≥200、正确文本 ≥10 万字、自动确认 ≥100；位置精确率和修改精确率均 ≥98%，正确文本误报 ≤1/万字，召回率高于同集 v3。开发集达标并不等于测试集达标。

另需按项目去重、逐条核对原始 PDF/OCR 的脱敏标书重复片段（约 50 个真实错字和 2 万正确字）做外部风险报告。`tools/validate_typo_v4_external.py` 核对样本数量、独立正确字数与来源声明；人工 PDF/OCR 核对不能由脚本代替。该样本不得加入训练或规则，也不能单独证明 98% 精确率。还须回归两侧对齐、单侧错、跨页、非 BMP、三文件聚合、超时、旧结果及导出。缺少任何一项验收证据，维持 v3；不自动重算历史项目。
