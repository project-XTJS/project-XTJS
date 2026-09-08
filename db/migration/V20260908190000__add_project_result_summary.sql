-- Derived metadata only; existing report objects remain unchanged.
SET lock_timeout = '5s';
ALTER TABLE xtjs_result ADD COLUMN IF NOT EXISTS result_summary JSONB;
COMMENT ON COLUMN xtjs_result.result_summary IS
    'Versioned visible result count and risk flag, refreshed atomically with result writes';
