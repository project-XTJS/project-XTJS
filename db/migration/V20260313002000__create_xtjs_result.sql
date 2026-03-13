CREATE TABLE xtjs_result
(
    id                    BIGSERIAL PRIMARY KEY,
    project_identifier_id VARCHAR(64) NOT NULL,
    result                JSONB       NOT NULL DEFAULT '{}'::jsonb,
    create_time           TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_time           TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_xtjs_result_project_identifier
        FOREIGN KEY (project_identifier_id)
            REFERENCES xtjs_projects (identifier_id)
            ON DELETE CASCADE
);

COMMENT
ON TABLE xtjs_result IS '项目业务审查结果表';

COMMENT
ON COLUMN xtjs_result.id IS '自增主键';
COMMENT
ON COLUMN xtjs_result.project_identifier_id IS '项目业务唯一标识ID（外键关联 xtjs_projects.identifier_id）';
COMMENT
ON COLUMN xtjs_result.result IS 'JSON 格式审查结果，可动态扩展业务检查项';
COMMENT
ON COLUMN xtjs_result.create_time IS '创建时间';
COMMENT
ON COLUMN xtjs_result.update_time IS '更新时间';

CREATE UNIQUE INDEX uk_xtjs_result_project_identifier
    ON xtjs_result (project_identifier_id);

CREATE INDEX idx_xtjs_result_update_time
    ON xtjs_result (update_time);
