CREATE OR REPLACE FUNCTION xtjs_jsonb_path_from_review_path(review_path TEXT)
RETURNS TEXT[]
LANGUAGE SQL
IMMUTABLE
AS $$
    SELECT CASE
        WHEN COALESCE(BTRIM(review_path), '') = '' THEN ARRAY[]::TEXT[]
        ELSE string_to_array(
            regexp_replace(
                regexp_replace(BTRIM(review_path), '\[([0-9]+)\]', '.\1', 'g'),
                '^\.',
                ''
            ),
            '.'
        )
    END;
$$;

CREATE OR REPLACE FUNCTION xtjs_apply_manual_review_items(base_payload JSONB, input_payload JSONB)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    result_payload JSONB := COALESCE(base_payload, '{}'::JSONB);
    item JSONB;
    review_path TEXT;
    path_parts TEXT[];
BEGIN
    IF jsonb_typeof(input_payload) <> 'object'
       OR jsonb_typeof(input_payload -> 'items') <> 'array' THEN
        RETURN result_payload;
    END IF;

    FOR item IN SELECT value FROM jsonb_array_elements(input_payload -> 'items')
    LOOP
        review_path := item ->> 'result_path';
        IF COALESCE(BTRIM(review_path), '') = '' OR NOT (item ? 'manual_value') THEN
            CONTINUE;
        END IF;

        path_parts := xtjs_jsonb_path_from_review_path(review_path);
        IF array_length(path_parts, 1) IS NULL THEN
            CONTINUE;
        END IF;

        result_payload := jsonb_set(
            result_payload,
            path_parts,
            COALESCE(item -> 'manual_value', 'null'::JSONB),
            FALSE
        );
    END LOOP;

    RETURN result_payload;
END;
$$;

CREATE OR REPLACE FUNCTION xtjs_legacy_manual_latest(result_payload JSONB, legacy_inputs JSONB)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    latest_payload JSONB := '{}'::JSONB;
    pair RECORD;
    base_payload JSONB;
BEGIN
    IF jsonb_typeof(legacy_inputs) <> 'object' THEN
        RETURN latest_payload;
    END IF;

    FOR pair IN SELECT key, value FROM jsonb_each(legacy_inputs)
    LOOP
        IF pair.key = 'workflow' THEN
            CONTINUE;
        END IF;

        base_payload := COALESCE(result_payload, '{}'::JSONB) -> pair.key;
        IF jsonb_typeof(base_payload) = 'object' THEN
            latest_payload := jsonb_set(
                latest_payload,
                ARRAY[pair.key],
                xtjs_apply_manual_review_items(base_payload, pair.value),
                TRUE
            );
        END IF;
    END LOOP;

    RETURN latest_payload;
END;
$$;

WITH legacy_business_items AS (
    SELECT
        item ->> 'document_identifier_id' AS document_identifier_id,
        item
    FROM xtjs_result r
    CROSS JOIN LATERAL jsonb_array_elements(
        COALESCE(r.result -> 'manual_review_inputs' -> 'business_bid_format_review' -> 'items', '[]'::JSONB)
        || COALESCE(r.manual_review_inputs -> 'business_bid_format_review' -> 'items', '[]'::JSONB)
    ) AS item
    WHERE item ->> 'document_identifier_id' ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
),
legacy_business_payloads AS (
    SELECT
        document_identifier_id::UUID AS document_identifier_id,
        jsonb_build_object(
            'schema_version', '1.0',
            'result_key', 'business_bid_format_review',
            'updated_at', to_char(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
            'items', jsonb_agg(item)
        ) AS input_payload
    FROM legacy_business_items
    GROUP BY document_identifier_id
)
UPDATE xtjs_documents d
SET
    review_content = jsonb_build_object(
        'schema_version', COALESCE(NULLIF(d.review_content ->> 'schema_version', ''), '1.0'),
        'base_content',
            CASE
                WHEN jsonb_typeof(d.review_content -> 'base_content') = 'object' THEN d.review_content -> 'base_content'
                WHEN jsonb_typeof(d.content) = 'object' THEN d.content
                ELSE '{}'::JSONB
            END,
        'effective_content',
            jsonb_set(
                CASE
                    WHEN jsonb_typeof(d.review_content -> 'effective_content') = 'object' THEN d.review_content -> 'effective_content'
                    WHEN jsonb_typeof(d.content) = 'object' THEN d.content
                    ELSE '{}'::JSONB
                END,
                '{manual_extractions,business_bid_format_review}',
                p.input_payload,
                TRUE
            ),
        'inputs',
            COALESCE(d.review_content -> 'inputs', '{}'::JSONB)
            || jsonb_build_object('business_bid_format_review', p.input_payload),
        'updated_at', to_char(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
    ),
    update_time = CURRENT_TIMESTAMP
FROM legacy_business_payloads p
WHERE d.identifier_id = p.document_identifier_id;

UPDATE xtjs_documents d
SET review_content = jsonb_build_object(
        'schema_version', COALESCE(NULLIF(d.review_content ->> 'schema_version', ''), '1.0'),
        'base_content',
            CASE
                WHEN jsonb_typeof(d.review_content -> 'base_content') = 'object' THEN d.review_content -> 'base_content'
                WHEN jsonb_typeof(d.content) = 'object' THEN d.content
                ELSE '{}'::JSONB
            END,
        'effective_content',
            CASE
                WHEN jsonb_typeof(d.review_content -> 'effective_content') = 'object' THEN d.review_content -> 'effective_content'
                WHEN jsonb_typeof(d.content) = 'object' THEN d.content
                ELSE '{}'::JSONB
            END,
        'inputs', COALESCE(d.review_content -> 'inputs', '{}'::JSONB),
        'updated_at', COALESCE(NULLIF(d.review_content ->> 'updated_at', ''), '')
    )
WHERE jsonb_typeof(d.review_content) <> 'object'
   OR NOT (d.review_content ? 'schema_version')
   OR NOT (d.review_content ? 'base_content')
   OR NOT (d.review_content ? 'effective_content')
   OR NOT (d.review_content ? 'inputs')
   OR NOT (d.review_content ? 'updated_at');

WITH result_sources AS (
    SELECT
        r.project_identifier_id,
        COALESCE(r.result, '{}'::JSONB) AS result_payload,
        COALESCE(r.result -> 'manual_review_inputs', '{}'::JSONB)
            || COALESCE(r.manual_review_inputs, '{}'::JSONB) AS legacy_inputs,
        CASE
            WHEN jsonb_typeof(r.result_fot_frontend) = 'object' THEN r.result_fot_frontend
            ELSE '{}'::JSONB
        END AS frontend_latest,
        CASE
            WHEN jsonb_typeof(r.result -> 'manual_review_results' -> 'latest') = 'object'
                THEN r.result -> 'manual_review_results' -> 'latest'
            ELSE '{}'::JSONB
        END AS existing_latest,
        CASE
            WHEN jsonb_typeof(r.result -> 'manual_review_results' -> 'workflow_scope') = 'object'
                 AND r.result -> 'manual_review_results' -> 'workflow_scope' <> '{}'::JSONB
                THEN r.result -> 'manual_review_results' -> 'workflow_scope'
            WHEN jsonb_typeof(
                (COALESCE(r.result -> 'manual_review_inputs', '{}'::JSONB)
                 || COALESCE(r.manual_review_inputs, '{}'::JSONB)) -> 'workflow'
            ) = 'object'
                THEN (COALESCE(r.result -> 'manual_review_inputs', '{}'::JSONB)
                      || COALESCE(r.manual_review_inputs, '{}'::JSONB)) -> 'workflow'
            ELSE '{}'::JSONB
        END AS workflow_scope,
        COALESCE(NULLIF(r.result -> 'manual_review_results' ->> 'updated_at', ''), to_char(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')) AS updated_at
    FROM xtjs_result r
),
result_payloads AS (
    SELECT
        project_identifier_id,
        jsonb_build_object(
            'latest', xtjs_legacy_manual_latest(result_payload, legacy_inputs) || frontend_latest || existing_latest,
            'workflow_scope', workflow_scope,
            'updated_at', updated_at
        ) AS manual_review_results
    FROM result_sources
)
UPDATE xtjs_result r
SET
    result = jsonb_set(
        COALESCE(r.result, '{}'::JSONB) - 'manual_review_inputs',
        '{manual_review_results}',
        p.manual_review_results,
        TRUE
    ),
    update_time = CURRENT_TIMESTAMP
FROM result_payloads p
WHERE r.project_identifier_id = p.project_identifier_id;

DROP FUNCTION xtjs_legacy_manual_latest(JSONB, JSONB);
DROP FUNCTION xtjs_apply_manual_review_items(JSONB, JSONB);
DROP FUNCTION xtjs_jsonb_path_from_review_path(TEXT);

ALTER TABLE xtjs_result
    DROP COLUMN IF EXISTS manual_review_inputs,
    DROP COLUMN IF EXISTS result_fot_frontend;
