-- OCR failure is operational metadata; it must not invalidate project inputs/results.
ALTER TABLE xtjs_documents ADD COLUMN IF NOT EXISTS ocr_last_error JSONB;
