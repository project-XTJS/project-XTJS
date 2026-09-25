# 重复内容错别字 v5（离线实验）

本目录的 v5 只在 `docker-compose.typo-v5-eval.yml` 中按 profile 启动。线上 `xtjs-typo` 仍运行 v3；不要把该 compose 文件叠加到生产 `up`，也不要重算历史项目。`/check` 协议与 Unicode 码点位置不变。v5 只确认原词本身写错的词内单字替换：原文检测器高分、原词不在 Jieba 默认词典、MacBERT 提议、目标词有效、同音或固定字形相似、CEC3 独立同改且回程稳定、两侧重复证据完整词语对齐，缺一项就隐藏。

## 资源和隔离

- 检测器从现有 MacBERT 编码器初始化，`BertForTokenClassification` 仅接收原句。训练容器限 2 CPU、8GB、`CUDA_VISIBLE_DEVICES` 为空，最多 2 epoch；模型输出到 `/data/xtjs-models/typo-training/v5/detector-public-v1`，不覆盖线上权重。
- 字体固定为 `/data/xtjs-models/typo-training/v5/assets/NotoSansSC-wght.ttf`，SHA-256 为 `a3041811a78c361b1de50f953c805e0244951c21c5bd412f7232ef0d899af0da`；许可证文件 `OFL-LICENSE.txt` 为 SIL OFL 1.1，来源及校验在同目录 `asset-manifest.json`。实验镜像固定 `pypinyin==0.55.0`，运行时从包元数据取得拼音和 Pillow 渲染版本并纳入缓存键。
- 新环境可运行 `python tools/download_typo_v5_assets.py /data/xtjs-models/typo-training/v5/assets` 下载并逐文件校验，已有目录不会被覆盖。
- 只用公开训练集监督；开发集只用于检查点和阈值，测试集需冻结后使用一次。人工规则文件 `word_rules.json` 不参与 v5。

## 执行顺序

1. 构建实验镜像：`docker build -f Dockerfile.typo-v5 -t xtjs-typo:v5-offline-experiment .`
2. 训练：`docker compose -f docker-compose.typo-v5-eval.yml --profile v5-train run --rm typo-v5-train --dataset /data/public/public_typo_dataset.jsonl --base-model /models/current --output /output/detector-public-v1 --epochs 2`。输出目录必须为空。权重、tokenizer 和清洗计数写入 `model-manifest.json`。
3. 生成开发集分类草稿：`python tools/prepare_typo_v5_scope.py /data/xtjs-models/typo-training/public-v1/data/public_typo_dataset.jsonl --split dev --output /data/xtjs-models/typo-training/v5/eval/dev-scope-draft.jsonl`。运行 `python tools/export_typo_v5_review_queue.py <草稿> --output <待复核队列.jsonl>`。人工逐项核对后在 JSONL 中填写 `project_id,start,category`，以 `--reviews <文件> --review-source human` 重建；AI 复核必须用 `--review-source agent`，产物只能用于诊断，不得冻结或声称人工确认。类别为 `spelling`、`legal_word`、`uncertain`。若 Jieba 将明显错词误收录，允许从 `legal_word` 改判 `spelling`，但每项必须注明 `rationale=dictionary_false_positive` 并保留原文、目标词。原本对齐不可靠的 `uncertain` 不得升级。草稿不能作为冻结真值。
4. 在低峰检查线上健康和显存后，单独启动 v5 实验容器。收集开发集 trace 时设置 `TYPO_V5_EVAL_TRACE=true TYPO_DETECTOR_THRESHOLD=0.5 TYPO_GLYPH_THRESHOLD=0.0`；`tools/evaluate_duplicate_typo_candidates.py` 的 `--save-results` 可逐条保存，完整开发集而非旧的 2119 条片段才可校准。trace 只用于开发集，不导出或展示为正式候选。
5. 使用 `tools/calibrate_typo_v5.py` 搜索检测分数和字形阈值；必须提供完整开发集条数、人工复核结果和同证据冷热缓存的 v3/v5 p95 报告。`tools/compare_typo_v5_latency.py` 分阶段运行独立 v3 基线和 v5 容器，线上健康或显存不足时停止。冻结文件包含两模型身份、检测器哈希、阈值、字体及拼音版本。
6. 冻结后用 `tools/prepare_typo_v5_scope.py --split test --frozen-manifest ...` 制作一次性测试输入，再用 `tools/evaluate_duplicate_typo_candidates.py --split test --frozen-manifest ... --v3-baseline-report ... --latency-report ... --enforce-gate` 评测，并分数据来源报告。该工具会对未完成的人工范围复核、样本量不足或精确率、误报、召回、耗时门槛不达标明确判失败。脱敏标书片段的 PDF/OCR 外部检查另做，不能代替公开测试。

## 重要限制

- 当前字形门槛默认 0.75 只是占位值，不能据此认定通过验收。例如固定字体下“圳→训”的余弦相似度约 0.477，且不同音；因此“培圳→培训”在默认 v5 门槛下会隐藏。不得为了单个例子直接放宽，而应使用完整、人工复核的开发集确定门槛。
- 当前审阅产物为 `/data/xtjs-models/typo-training/v5/eval/dev-scope-agent-reviews-v2.jsonl`、`dev-scope-agent-reviewed-v2.jsonl`。全部 2551 处改动保留来源与复核方法，分类为 407 处词内拼写、1011 处合法词替换、1133 处不确定；其中 49 处“词典合法”草稿改判为明确拼写错误。`dev-scope-structural-ceiling-v2.json` 显示在 0.55 字形门槛下，407 处中最多 232 处可通过当前词典与形音硬规则（约 57.0%），还未计模型和两侧对齐漏检。以上都不是人工金标准或模型实际召回。`tools/materialize_typo_v5_agent_review.py` 可根据草稿 SHA 和 `agent-review-notes.json` 复现，`dev-scope-human-second-review-queue-v2.jsonl` 供人工二次核验。
- 公开数据的自动范围分类只是审核草稿；词内拼写、合法词替换和不确定类别必须完成复核，尤其不能把合法词替换算成漏检。只统计原本正确的独立文本作为“正确字数”。
- 若未满足 ≥200 处范围内真实错字、≥10 万正确字、≥100 项确认、两个精确率 ≥98%、每万字误报 ≤1、召回高于同口径 v3 及 p95 ≤1.5 倍，继续运行线上 v3。
