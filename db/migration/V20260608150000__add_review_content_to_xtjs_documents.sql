ALTER TABLE xtjs_documents
    ADD COLUMN IF NOT EXISTS review_content JSONB NOT NULL DEFAULT '{}'::jsonb;

COMMENT ON COLUMN xtjs_documents.review_content IS
    'Manual OCR working copy: base_content, effective_content, domain inputs and updated_at';
