CREATE TABLE xtjs_projects
(
    id            BIGSERIAL PRIMARY KEY,
    identifier_id VARCHAR(64) NOT NULL,
    deleted       BOOLEAN     NOT NULL DEFAULT FALSE,
    create_time   TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_time   TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP
);

COMMENT
ON TABLE xtjs_projects IS 'XTJS项目表';

COMMENT
ON COLUMN xtjs_projects.id IS '自增主键';
COMMENT
ON COLUMN xtjs_projects.identifier_id IS '项目业务唯一标识符';
COMMENT
ON COLUMN xtjs_projects.deleted IS '逻辑删除：false-未删除，true-已删除';
COMMENT
ON COLUMN xtjs_projects.create_time IS '创建时间';
COMMENT
ON COLUMN xtjs_projects.update_time IS '更新时间';

CREATE UNIQUE INDEX uk_xtjs_projects_identifier
    ON xtjs_projects (identifier_id);

CREATE INDEX idx_xtjs_projects_deleted_create_time
    ON xtjs_projects (deleted, create_time);