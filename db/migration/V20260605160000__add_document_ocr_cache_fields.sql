ALTER TABLE xtjs_documents
    ADD COLUMN IF NOT EXISTS source_file_hash TEXT,
    ADD COLUMN IF NOT EXISTS source_file_size BIGINT,
    ADD COLUMN IF NOT EXISTS ocr_cache_key TEXT,
    ADD COLUMN IF NOT EXISTS ocr_engine_version TEXT,
    ADD COLUMN IF NOT EXISTS ocr_config_hash TEXT,
    ADD COLUMN IF NOT EXISTS ocr_cache_source_document_id UUID;

COMMENT ON COLUMN xtjs_documents.source_file_hash IS 'SHA256 hash of the original uploaded source file';
COMMENT ON COLUMN xtjs_documents.source_file_size IS 'Original uploaded source file size in bytes';
COMMENT ON COLUMN xtjs_documents.ocr_cache_key IS 'Stable OCR reuse cache key derived from file hash, document type and OCR configuration';
COMMENT ON COLUMN xtjs_documents.ocr_engine_version IS 'OCR engine/model version used for the stored recognition content';
COMMENT ON COLUMN xtjs_documents.ocr_config_hash IS 'Hash of OCR configuration that affects recognition results';
COMMENT ON COLUMN xtjs_documents.ocr_cache_source_document_id IS 'Source document copied from when OCR content was reused';

CREATE INDEX IF NOT EXISTS idx_xtjs_documents_ocr_cache_lookup
    ON xtjs_documents (source_file_hash, document_type, ocr_engine_version, ocr_config_hash)
    WHERE deleted = FALSE AND extracted = TRUE AND source_file_hash IS NOT NULL;
