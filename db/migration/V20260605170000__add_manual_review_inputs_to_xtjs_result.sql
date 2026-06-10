ALTER TABLE xtjs_result
    ADD COLUMN IF NOT EXISTS manual_review_inputs JSONB NOT NULL DEFAULT '{}'::jsonb;

COMMENT
ON COLUMN xtjs_result.manual_review_inputs IS 'Manual correction inputs captured from result review pages, grouped by result key';
