CREATE TABLE IF NOT EXISTS xtjs_analysis_tasks (
    identifier_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_identifier_id UUID NOT NULL REFERENCES xtjs_projects(identifier_id),
    operation TEXT NOT NULL,
    request_id UUID NOT NULL,
    input_revision BIGINT NOT NULL,
    start_result_version TEXT,
    requested_by UUID NOT NULL REFERENCES xtjs_users(identifier_id),
    status TEXT NOT NULL DEFAULT 'queued',
    stage TEXT NOT NULL DEFAULT 'queued',
    progress JSONB NOT NULL DEFAULT '{}'::jsonb,
    request_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    execution_token UUID,
    published_at TIMESTAMPTZ,
    publish_attempts INTEGER NOT NULL DEFAULT 0,
    last_publish_error TEXT,
    heartbeat_at TIMESTAMPTZ,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    result_version TEXT,
    error_message TEXT,
    create_time TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_time TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT ck_xtjs_analysis_tasks_status CHECK (
        status IN ('queued', 'running', 'succeeded', 'failed', 'interrupted', 'stale')
    ),
    CONSTRAINT uq_xtjs_analysis_tasks_request UNIQUE (
        project_identifier_id, operation, request_id
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_xtjs_analysis_tasks_active_project_operation
    ON xtjs_analysis_tasks(project_identifier_id, operation)
    WHERE status IN ('queued', 'running');

CREATE INDEX IF NOT EXISTS ix_xtjs_analysis_tasks_project_latest
    ON xtjs_analysis_tasks(project_identifier_id, operation, create_time DESC);

CREATE INDEX IF NOT EXISTS ix_xtjs_analysis_tasks_dispatch
    ON xtjs_analysis_tasks(status, published_at, create_time)
    WHERE status = 'queued';

ALTER TABLE xtjs_analysis_tasks ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS xtjs_analysis_tasks_scope ON xtjs_analysis_tasks;
CREATE POLICY xtjs_analysis_tasks_scope ON xtjs_analysis_tasks
    USING (
        xtjs_can_access_project(
            project_identifier_id,
            nullif(current_setting('xtjs.actor_id', true), '')::uuid
        )
    );
