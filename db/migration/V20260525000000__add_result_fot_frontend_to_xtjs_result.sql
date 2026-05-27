ALTER TABLE xtjs_result
    ADD COLUMN IF NOT EXISTS result_fot_frontend JSONB NOT NULL DEFAULT '{}'::jsonb;

COMMENT
ON COLUMN xtjs_result.result_fot_frontend IS '前端删减后的项目审查结果 JSON，未提交时为空对象';
