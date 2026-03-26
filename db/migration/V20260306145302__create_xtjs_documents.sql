CREATE TABLE xtjs_documents
(
    id            BIGSERIAL PRIMARY KEY,
    identifier_id VARCHAR(64) NOT NULL,
    document_type VARCHAR(16) NOT NULL,
    file_name     TEXT        NOT NULL,
    file_url      TEXT        NOT NULL,
    extracted     BOOLEAN     NOT NULL DEFAULT FALSE,
    content       JSONB,
    deleted       BOOLEAN     NOT NULL DEFAULT FALSE,
    create_time   TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_time   TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT chk_xtjs_documents_type
        CHECK (document_type IN ('tender', 'business_bid', 'technical_bid', 'bid'))
);

COMMENT ON TABLE xtjs_documents IS 'XTJS文档表';
COMMENT ON COLUMN xtjs_documents.id IS '自增主键';
COMMENT ON COLUMN xtjs_documents.identifier_id IS '文档业务唯一标识';
COMMENT ON COLUMN xtjs_documents.document_type IS
    '文档类型：tender-招标文件，business_bid-商务标文件，technical_bid-技术标文件，bid-历史投标文件';
COMMENT ON COLUMN xtjs_documents.file_name IS '文件名';
COMMENT ON COLUMN xtjs_documents.file_url IS '文件持久化存储地址';
COMMENT ON COLUMN xtjs_documents.extracted IS '是否已提取识别内容';
COMMENT ON COLUMN xtjs_documents.content IS '识别结果JSON';
COMMENT ON COLUMN xtjs_documents.deleted IS '逻辑删除标记';
COMMENT ON COLUMN xtjs_documents.create_time IS '创建时间';
COMMENT ON COLUMN xtjs_documents.update_time IS '更新时间';

CREATE INDEX idx_xtjs_documents_identifier
    ON xtjs_documents (identifier_id);

CREATE INDEX idx_xtjs_documents_type
    ON xtjs_documents (document_type);

CREATE INDEX idx_xtjs_documents_deleted_create_time
    ON xtjs_documents (deleted, create_time);

CREATE INDEX idx_xtjs_documents_type_deleted_create_time
    ON xtjs_documents (document_type, deleted, create_time);
