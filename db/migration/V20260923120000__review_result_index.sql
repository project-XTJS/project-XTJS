ALTER TABLE xtjs_result
    ADD COLUMN IF NOT EXISTS result_version TEXT,
    ADD COLUMN IF NOT EXISTS review_summary JSONB,
    ADD COLUMN IF NOT EXISTS review_index_status TEXT NOT NULL DEFAULT 'missing';

ALTER TABLE xtjs_result_history
    ADD COLUMN IF NOT EXISTS result_version TEXT,
    ADD COLUMN IF NOT EXISTS review_summary JSONB,
    ADD COLUMN IF NOT EXISTS review_index_status TEXT;

CREATE TABLE IF NOT EXISTS xtjs_result_components (
    project_identifier_id UUID NOT NULL,
    result_version TEXT NOT NULL,
    result_key TEXT NOT NULL,
    object_key TEXT NOT NULL,
    summary JSONB NOT NULL DEFAULT '{}'::jsonb,
    issue_count INTEGER NOT NULL DEFAULT 0,
    create_time TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (project_identifier_id, result_version, result_key)
);

CREATE INDEX IF NOT EXISTS ix_xtjs_result_components_project_version
    ON xtjs_result_components(project_identifier_id, result_version);

CREATE TABLE IF NOT EXISTS xtjs_review_issues (
    project_identifier_id UUID NOT NULL,
    result_version TEXT NOT NULL,
    result_key TEXT NOT NULL,
    issue_id TEXT NOT NULL,
    issue_order INTEGER NOT NULL,
    risk_level TEXT NOT NULL DEFAULT 'none',
    status TEXT NOT NULL DEFAULT 'passed',
    check_code TEXT NOT NULL DEFAULT '',
    title TEXT NOT NULL DEFAULT '',
    description TEXT NOT NULL DEFAULT '',
    file_names JSONB NOT NULL DEFAULT '[]'::jsonb,
    list_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    detail_object_key TEXT NOT NULL,
    evidence_object_key TEXT NOT NULL,
    evidence_count INTEGER NOT NULL DEFAULT 0,
    create_time TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (project_identifier_id, result_version, issue_id)
);

CREATE INDEX IF NOT EXISTS ix_xtjs_review_issues_page
    ON xtjs_review_issues(project_identifier_id, result_version, result_key, issue_order, issue_id);
CREATE INDEX IF NOT EXISTS ix_xtjs_review_issues_filter
    ON xtjs_review_issues(project_identifier_id, result_version, result_key, risk_level, status, check_code);

CREATE TABLE IF NOT EXISTS xtjs_result_conversion_log (
    id BIGSERIAL PRIMARY KEY,
    project_identifier_id UUID NOT NULL,
    input_revision BIGINT NOT NULL,
    old_result_object_key TEXT NOT NULL,
    new_result_object_key TEXT NOT NULL,
    old_content_sha256 TEXT NOT NULL,
    new_content_sha256 TEXT NOT NULL,
    old_update_time TIMESTAMPTZ NOT NULL,
    result_version TEXT NOT NULL,
    converted_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (project_identifier_id, old_result_object_key, new_result_object_key)
);

CREATE INDEX IF NOT EXISTS ix_xtjs_result_conversion_log_project
    ON xtjs_result_conversion_log(project_identifier_id, converted_at DESC);

ALTER TABLE xtjs_result_components ENABLE ROW LEVEL SECURITY;
ALTER TABLE xtjs_review_issues ENABLE ROW LEVEL SECURITY;
ALTER TABLE xtjs_result_conversion_log ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS xtjs_result_components_scope ON xtjs_result_components;
CREATE POLICY xtjs_result_components_scope ON xtjs_result_components
    USING (xtjs_can_access_project(project_identifier_id, nullif(current_setting('xtjs.actor_id', true), '')::uuid));

DROP POLICY IF EXISTS xtjs_review_issues_scope ON xtjs_review_issues;
CREATE POLICY xtjs_review_issues_scope ON xtjs_review_issues
    USING (xtjs_can_access_project(project_identifier_id, nullif(current_setting('xtjs.actor_id', true), '')::uuid));

DROP POLICY IF EXISTS xtjs_result_conversion_log_scope ON xtjs_result_conversion_log;
CREATE POLICY xtjs_result_conversion_log_scope ON xtjs_result_conversion_log
    USING (xtjs_can_access_project(project_identifier_id, nullif(current_setting('xtjs.actor_id', true), '')::uuid));

CREATE OR REPLACE FUNCTION xtjs_invalidate_materials(pid UUID) RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
    PERFORM 1 FROM xtjs_projects WHERE identifier_id=pid FOR UPDATE;
    INSERT INTO xtjs_result_history(
        project_identifier_id,input_revision,result_object_key,result,result_keys,
        result_summary,workflow_scope,result_version,review_summary,review_index_status
    )
      SELECT project_identifier_id,input_revision,result_object_key,result,result_keys,
             result_summary,workflow_scope,result_version,review_summary,review_index_status
      FROM xtjs_result WHERE project_identifier_id=pid
      ON CONFLICT(project_identifier_id,input_revision) DO NOTHING;
    UPDATE xtjs_projects SET input_revision=input_revision+1,report_url='',update_time=CURRENT_TIMESTAMP WHERE identifier_id=pid;
    PERFORM xtjs_sync_materials(pid);
END $$;
