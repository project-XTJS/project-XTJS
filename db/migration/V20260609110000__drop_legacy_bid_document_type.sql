DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM xtjs_documents
        WHERE document_type = 'bid'
        LIMIT 1
    ) THEN
        RAISE EXCEPTION 'Cannot remove legacy document_type=bid while xtjs_documents still contains bid rows';
    END IF;
END
$$;

ALTER TABLE xtjs_documents
    DROP CONSTRAINT IF EXISTS chk_xtjs_documents_type;

ALTER TABLE xtjs_documents
    ADD CONSTRAINT chk_xtjs_documents_type
        CHECK (document_type IN ('tender', 'business_bid', 'technical_bid'));

COMMENT ON COLUMN xtjs_documents.document_type IS
    '文档类型：tender-招标文件，business_bid-商务标文件，technical_bid-技术标文件';
