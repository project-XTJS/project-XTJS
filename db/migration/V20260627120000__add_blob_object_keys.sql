-- DB 瘦身：把大 JSON（文档识别内容 / 项目分析结果）外置到 MinIO，
-- 数据库只保留对象键引用。content/result 列暂保留以兼容回退，存量迁移后清空。

ALTER TABLE xtjs_documents
    ADD COLUMN IF NOT EXISTS content_object_key TEXT;

COMMENT ON COLUMN xtjs_documents.content_object_key IS
    '识别内容 JSON 在 MinIO 的对象键；非空时 content 列置 NULL，读取走对象存储';

-- 人工复核工作副本 review_content 同样外置到 MinIO（JSON识别/review/）。
ALTER TABLE xtjs_documents
    ADD COLUMN IF NOT EXISTS review_content_object_key TEXT;

COMMENT ON COLUMN xtjs_documents.review_content_object_key IS
    '人工复核工作副本 JSON 在 MinIO 的对象键；非空时 review_content 列置 NULL，读取走对象存储';

-- review_content 外置后内联列置 NULL，需放开原 NOT NULL 约束。
ALTER TABLE xtjs_documents
    ALTER COLUMN review_content DROP NOT NULL;

ALTER TABLE xtjs_result
    ADD COLUMN IF NOT EXISTS result_object_key TEXT;

COMMENT ON COLUMN xtjs_result.result_object_key IS
    '分析结果 JSON 在 MinIO 的对象键；非空时 result 列置 NULL，读取走对象存储';

-- result 外置后内联列置 NULL，需放开原 NOT NULL 约束。
ALTER TABLE xtjs_result
    ALTER COLUMN result DROP NOT NULL;

-- 结果外置后 result 列为 NULL，但项目列表需要展示“已完成的分析项”徽标。
-- 这里保留一份轻量的顶层结果键列表（仅键名，不含重数据），供列表查询统计使用。
ALTER TABLE xtjs_result
    ADD COLUMN IF NOT EXISTS result_keys JSONB NOT NULL DEFAULT '[]'::jsonb;

COMMENT ON COLUMN xtjs_result.result_keys IS
    '结果顶层键名列表（轻量摘要），用于项目列表统计分析项，避免读取 MinIO 大对象';
