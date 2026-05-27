ALTER TABLE xtjs_projects
    ADD COLUMN IF NOT EXISTS report_url TEXT NOT NULL DEFAULT '';

COMMENT
ON COLUMN xtjs_projects.report_url IS '前端删减结果导出的 Markdown 报告 MinIO 地址，未生成时为空字符串';
