CREATE TABLE xtjs_project_documents
(
    id                 BIGSERIAL PRIMARY KEY,
    project_id         BIGINT    NOT NULL,
    tender_document_id BIGINT    NOT NULL,
    bid_document_id    BIGINT    NOT NULL,
    create_time        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_project
        FOREIGN KEY (project_id)
            REFERENCES xtjs_projects (id)
            ON DELETE CASCADE,

    CONSTRAINT fk_tender_document
        FOREIGN KEY (tender_document_id)
            REFERENCES xtjs_documents (id)
            ON DELETE CASCADE,

    CONSTRAINT fk_bid_document
        FOREIGN KEY (bid_document_id)
            REFERENCES xtjs_documents (id)
            ON DELETE CASCADE
);

COMMENT
ON TABLE xtjs_project_documents IS 'XTJS项目招标文件与投标文件关联表';

COMMENT
ON COLUMN xtjs_project_documents.id IS '自增主键';
COMMENT
ON COLUMN xtjs_project_documents.project_id IS '项目ID';
COMMENT
ON COLUMN xtjs_project_documents.tender_document_id IS '招标文件ID';
COMMENT
ON COLUMN xtjs_project_documents.bid_document_id IS '投标文件ID';
COMMENT
ON COLUMN xtjs_project_documents.create_time IS '创建时间';

CREATE INDEX idx_project_documents_project
    ON xtjs_project_documents (project_id);

CREATE INDEX idx_project_documents_tender
    ON xtjs_project_documents (tender_document_id);

CREATE INDEX idx_project_documents_bid
    ON xtjs_project_documents (bid_document_id);