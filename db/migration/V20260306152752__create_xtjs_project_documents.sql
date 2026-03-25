CREATE TABLE xtjs_project_documents
(
    id                       BIGSERIAL PRIMARY KEY,
    project_id               BIGINT    NOT NULL,
    tender_document_id       BIGINT    NOT NULL,
    business_bid_document_id BIGINT    NOT NULL,
    technical_bid_document_id BIGINT,
    create_time              TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_project
        FOREIGN KEY (project_id)
            REFERENCES xtjs_projects (id)
            ON DELETE CASCADE,

    CONSTRAINT fk_tender_document
        FOREIGN KEY (tender_document_id)
            REFERENCES xtjs_documents (id)
            ON DELETE CASCADE,

    CONSTRAINT fk_business_bid_document
        FOREIGN KEY (business_bid_document_id)
            REFERENCES xtjs_documents (id)
            ON DELETE CASCADE,

    CONSTRAINT fk_technical_bid_document
        FOREIGN KEY (technical_bid_document_id)
            REFERENCES xtjs_documents (id)
            ON DELETE CASCADE
);

COMMENT ON TABLE xtjs_project_documents IS 'XTJS项目文档关联表';
COMMENT ON COLUMN xtjs_project_documents.id IS '自增主键';
COMMENT ON COLUMN xtjs_project_documents.project_id IS '项目ID';
COMMENT ON COLUMN xtjs_project_documents.tender_document_id IS '招标文件ID';
COMMENT ON COLUMN xtjs_project_documents.business_bid_document_id IS '商务标文件ID';
COMMENT ON COLUMN xtjs_project_documents.technical_bid_document_id IS '技术标文件ID';
COMMENT ON COLUMN xtjs_project_documents.create_time IS '创建时间';

CREATE INDEX idx_project_documents_project
    ON xtjs_project_documents (project_id);

CREATE INDEX idx_project_documents_tender
    ON xtjs_project_documents (tender_document_id);

CREATE INDEX idx_project_documents_business_bid
    ON xtjs_project_documents (business_bid_document_id);

CREATE INDEX idx_project_documents_technical_bid
    ON xtjs_project_documents (technical_bid_document_id);
