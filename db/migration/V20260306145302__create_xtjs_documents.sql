CREATE TABLE xtjs_documents
(
    id            BIGSERIAL PRIMARY KEY,
    identifier_id VARCHAR(64) NOT NULL,
    file_name     TEXT        NOT NULL,
    file_url      TEXT        NOT NULL,
    extracted     BOOLEAN     NOT NULL DEFAULT FALSE,
    content       JSONB,
    deleted       BOOLEAN     NOT NULL DEFAULT FALSE,
    create_time   TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_time   TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP
);

COMMENT
ON TABLE xtjs_documents IS 'XTJS文档表';

COMMENT
ON COLUMN xtjs_documents.id IS '自增主键';
COMMENT
ON COLUMN xtjs_documents.identifier_id IS '文档业务唯一标识符';
COMMENT
ON COLUMN xtjs_documents.file_name IS '文件名称';
COMMENT
ON COLUMN xtjs_documents.file_url IS '文件URL（MinIO存储地址）';
COMMENT
ON COLUMN xtjs_documents.extracted IS '是否已识别：false-未识别，true-已识别';
COMMENT
ON COLUMN xtjs_documents.content IS 'JSON格式的文件识别结果';
COMMENT
ON COLUMN xtjs_documents.deleted IS '逻辑删除：false-未删除，true-已删除';
COMMENT
ON COLUMN xtjs_documents.create_time IS '创建时间';
COMMENT
ON COLUMN xtjs_documents.update_time IS '更新时间';

CREATE INDEX idx_xtjs_documents_identifier
    ON xtjs_documents (identifier_id);

CREATE INDEX idx_xtjs_documents_deleted_create_time
    ON xtjs_documents (deleted, create_time);
