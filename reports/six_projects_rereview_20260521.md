# 6个项目重审结果（2026-05-21）

本次使用当前代码重新执行并写回 `business_bid_format_review`，项目如下：

- 6G紧缩场系统：`bbac1276-c5eb-4e13-86da-73175d8e74e2`
- 出口退税：`09993522-245b-43ad-b185-5b667882e0be`
- 卫星科技：`b42d3013-7292-4374-a179-7cdf697934e4`
- 仪仗：`cb313aa0-e623-4763-9a0c-0085ec4b94f0`
- 工程建设：`ae9e4fe3-7c22-4770-9fa3-22d7d12409f7`
- 测试船体广告3：`136367fa-c054-4ae7-ba84-e11cd8d4520e`

## 总体结论

- 6 个项目均已完成重审并写回数据库，结果更新时间集中在 `2026-05-21 16:18:31` 到 `2026-05-21 16:19:10`。
- 只有 `工程建设` 的结果结构发生明显改善：`fail 23 -> 2`，`pass 12 -> 26`，`unclear 1 -> 8`。
- 其余 5 个项目的汇总计数与重审前一致，说明当前代码下这些项目的剩余问题仍未修掉。

## 各项目结果

### 1. 6G紧缩场系统

- 重审后汇总：`pass 19 / fail 5 / missing 0 / unclear 0`
- 与重审前相比：无变化
- 主要未通过项：
- 中电科思仪：`verification_check` 失败，`附件 8-1 法定代表人资格证明书（格式）`
- 苏州益谱：`consistency_check` 失败，`附件 4 商务条款偏离表（格式）`；`itemized_pricing_check` 失败，`分项汇总一致性校验`
- 上海霍莱沃：`consistency_check` 失败，`附件 13 财务状况及税收、社会保障资金缴纳情况声明函`、`附件 14 制造商声明函或制造商授权书（格式自拟）`
- 北京中测国宇：`consistency_check` 失败，`附件 8-2 法定代表人授权委托书（格式）`

### 2. 出口退税

- 重审后汇总：`pass 12 / fail 6 / missing 0 / unclear 0`
- 与重审前相比：无变化
- 主要未通过项：
- 上海智税：`consistency_check` 失败，`附件 7 类似项目业绩清单（格式）`；`verification_check` 失败，`附件 6 投标人基本情况表（格式）`
- 上海征盛：`consistency_check` 失败，`附件 3 分项报价表（格式自拟）`、`附件 7 类似项目业绩清单（格式）`、`附件 8-2 法定代表人授权委托书（格式）`；`verification_check` 失败，`附件 6 投标人基本情况表（格式）`
- 上海链坤：`integrity_check` 失败，`10. 投标人认为需加以说明的其他内容（如综合实力证明等）`；`verification_check` 失败，`附件 6 投标人基本情况表（格式）`、`附件 8-2 法定代表人授权委托书（格式）`

### 3. 卫星科技

- 重审后汇总：`pass 11 / fail 4 / missing 3 / unclear 0`
- 与重审前相比：无变化
- 主要未通过项：
- 首汽：`consistency_check` 失败，`附件 1`、`附件 2`、`附件 4`、`法定代表人资格证明书`；`itemized_pricing_check` 为 `missing`
- 衡山：`integrity_check` 失败，`附件 8`、`附件 9`；`consistency_check` 失败，`附件 1`、`附件 4`、`法定代表人资格证明书`；`itemized_pricing_check` 为 `missing`
- 锦江：`consistency_check` 失败，`附件 1`、`附件 2`、`附件 6`、`法定代表人资格证明书`；`itemized_pricing_check` 为 `missing`
- 说明：这一组当前结果仍保留我们之前已确认的多条一致性误报；`pricing_check` 虽显示 `pass`，但比对逻辑仍不可信。

### 4. 仪仗

- 重审后汇总：`pass 6 / fail 12 / missing 0 / unclear 0`
- 与重审前相比：无变化
- 主要未通过项：
- 上海清境：`integrity_check`、`consistency_check`、`pricing_check`、`verification_check` 均失败
- 上海鸿盛源：`integrity_check`、`consistency_check`、`pricing_check`、`verification_check` 均失败
- 钢铁之翼（结果中仍显示为 `信息科技有限公司`）：`integrity_check`、`consistency_check`、`pricing_check`、`verification_check` 均失败
- 说明：这组结果仍然保留明显的模板套用过严问题，未因本次重审消失。

### 5. 工程建设

- 重审后汇总：`pass 26 / fail 2 / missing 6 / unclear 8`
- 与重审前相比：`fail 23 -> 2`，`pass 12 -> 26`，`unclear 1 -> 8`
- 当前状态：
- 5 家投标人整体状态已从 `fail` 变为 `unclear`
- `deviation_check` 不再直接判失败，改为 `unclear`
- 仍有 6 家 `itemized_pricing_check` 为 `missing`
- 剩余明确 `fail`：
- 上海静旺建设工程有限公司：`consistency_check` 失败，`投标函`
- 上海电信科技发展有限公司：`consistency_check` 失败，`投标承诺书`

### 6. 测试船体广告3

- 重审后汇总：`pass 8 / fail 10 / missing 0 / unclear 0`
- 与重审前相比：无变化
- 主要未通过项：
- 上海阳生：`consistency_check`、`pricing_check`、`verification_check` 失败
- 亚元营销策划：`consistency_check`、`pricing_check`、`verification_check` 失败
- 上海善元：`integrity_check`、`consistency_check`、`itemized_pricing_check`、`verification_check` 失败

## 本轮最重要的结论

- `工程建设` 的结果已明显改善，当前主要问题转为 `unclear` 和少量 `consistency_check` 残留项。
- `卫星科技`、`仪仗`、`测试船体广告3` 这三组仍保留较多我们之前已经确认过的误报或可疑结果。
- `出口退税` 的早前一批模板误报没有重新出现在当前失败项里，但仍有新的签章/一致性残留问题。
- `6G紧缩场系统` 仍然是“少量真实问题 + 少量残留模板误报”混合状态。
