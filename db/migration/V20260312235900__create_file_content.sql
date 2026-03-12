CREATE TABLE file_content
(
    id          BIGSERIAL PRIMARY KEY,
    document_id BIGINT    NOT NULL,
    create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    content     JSONB     NOT NULL,

    CONSTRAINT fk_file_content_document
        FOREIGN KEY (document_id)
            REFERENCES xtjs_documents (id)
            ON DELETE CASCADE
);

COMMENT
ON TABLE file_content IS '文件识别内容表';

COMMENT
ON COLUMN file_content.id IS '自增主键';
COMMENT
ON COLUMN file_content.document_id IS '文件唯一标识ID（外键关联 xtjs_documents.id）';
COMMENT
ON COLUMN file_content.create_time IS '创建时间';
COMMENT
ON COLUMN file_content.content IS 'JSON 格式识别内容';

CREATE UNIQUE INDEX uk_file_content_document
    ON file_content (document_id);

CREATE INDEX idx_file_content_create_time
    ON file_content (create_time);
