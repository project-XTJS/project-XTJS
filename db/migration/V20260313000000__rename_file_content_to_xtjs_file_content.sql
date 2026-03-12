ALTER TABLE file_content
    RENAME TO xtjs_file_content;

DO
$$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'fk_file_content_document'
          AND conrelid = 'xtjs_file_content'::regclass
    ) THEN
        ALTER TABLE xtjs_file_content
            RENAME CONSTRAINT fk_file_content_document TO fk_xtjs_file_content_document;
    END IF;
END
$$;

ALTER INDEX IF EXISTS uk_file_content_document
    RENAME TO uk_xtjs_file_content_document;

ALTER INDEX IF EXISTS idx_file_content_create_time
    RENAME TO idx_xtjs_file_content_create_time;

COMMENT
ON TABLE xtjs_file_content IS '文件识别内容表';

COMMENT
ON COLUMN xtjs_file_content.id IS '自增主键';
COMMENT
ON COLUMN xtjs_file_content.document_id IS '文件唯一标识ID（外键关联 xtjs_documents.id）';
COMMENT
ON COLUMN xtjs_file_content.create_time IS '创建时间';
COMMENT
ON COLUMN xtjs_file_content.content IS 'JSON 格式识别内容';
