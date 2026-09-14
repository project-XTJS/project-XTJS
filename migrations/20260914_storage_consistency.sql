BEGIN;
SET LOCAL lock_timeout = '5s';
ALTER TABLE xtjs_projects ADD COLUMN IF NOT EXISTS upload_manifest jsonb;
ALTER TABLE xtjs_result ADD COLUMN IF NOT EXISTS workflow_scope jsonb;
COMMIT;
