-- Additive migration: historical inputs and tokens start at version zero.
ALTER TABLE xtjs_users ADD COLUMN IF NOT EXISTS token_version BIGINT NOT NULL DEFAULT 0;
ALTER TABLE xtjs_projects ADD COLUMN IF NOT EXISTS input_revision BIGINT NOT NULL DEFAULT 0;
ALTER TABLE xtjs_result ADD COLUMN IF NOT EXISTS input_revision BIGINT NOT NULL DEFAULT 0;
ALTER TABLE xtjs_projects ADD COLUMN IF NOT EXISTS upload_manifest JSONB;
ALTER TABLE xtjs_result ADD COLUMN IF NOT EXISTS workflow_scope JSONB;
ALTER TABLE xtjs_project_documents ADD COLUMN IF NOT EXISTS upload_group_slot TEXT;
CREATE UNIQUE INDEX IF NOT EXISTS ux_xtjs_upload_group ON xtjs_project_documents(project_id,upload_group_slot) WHERE upload_group_slot IS NOT NULL;
CREATE TABLE IF NOT EXISTS xtjs_result_history (
    id BIGSERIAL PRIMARY KEY, project_identifier_id UUID NOT NULL, input_revision BIGINT NOT NULL,
    result_object_key TEXT, result JSONB, result_keys JSONB, result_summary JSONB, workflow_scope JSONB,
    archived_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (project_identifier_id,input_revision)
);

-- One transactional source of truth for every relation/document mutation entrypoint.
CREATE OR REPLACE FUNCTION xtjs_sync_materials(pid UUID) RETURNS VOID LANGUAGE plpgsql AS $$
DECLARE m JSONB; f JSONB; g JSONB; fs JSONB := '[]'; gs JSONB := '[]';
        rel RECORD; doc RECORD; slot TEXT; candidates INTEGER; ids TEXT[]; complete BOOLEAN := TRUE;
        tender_ok BOOLEAN; business_ok BOOLEAN; technical_ok BOOLEAN; n INTEGER;
BEGIN
    SELECT upload_manifest INTO m FROM xtjs_projects WHERE identifier_id=pid FOR UPDATE;
    IF m IS NOT NULL THEN
        -- A shared tender slot changes only when every expected group agrees on its replacement.
        SELECT ARRAY_AGG(DISTINCT r.tender_document_id::TEXT),count(*) INTO ids,n
          FROM xtjs_project_documents r WHERE r.project_id=pid;
        IF cardinality(ids)=1 AND n=jsonb_array_length(COALESCE(m->'groups','[]')) THEN
            SELECT identifier_id,file_name INTO doc FROM xtjs_documents WHERE identifier_id::TEXT=ids[1] AND NOT deleted;
            IF FOUND THEN
                fs := '[]';
                FOR f IN SELECT value FROM jsonb_array_elements(m->'files') LOOP
                    IF f->>'slot'='tender' AND f->>'status' IS DISTINCT FROM 'uploading' AND f->>'document_id' IS DISTINCT FROM doc.identifier_id::TEXT THEN
                        f := f || jsonb_build_object('document_id',doc.identifier_id,'name',doc.file_name,'status','uploaded');
                    END IF;
                    fs := fs || jsonb_build_array(f);
                END LOOP;
                m := jsonb_set(m,'{files}',fs);
            END IF;
        END IF;
        FOR g IN SELECT value FROM jsonb_array_elements(COALESCE(m->'groups','[]')) LOOP
            slot := COALESCE(g->>'slot',g->>'business_bid');
            SELECT ARRAY_AGG(x->>'document_id' ORDER BY CASE x->>'role' WHEN 'tender' THEN 1 WHEN 'business_bid' THEN 2 ELSE 3 END)
              INTO ids FROM jsonb_array_elements(m->'files') x
              WHERE x->>'slot' IN ('tender',g->>'business_bid',g->>'technical_bid');
            SELECT count(*) INTO candidates FROM xtjs_project_documents r WHERE r.project_id=pid AND
                (r.upload_group_slot=slot OR (r.upload_group_slot IS NULL AND
                 COALESCE(r.tender_document_id::TEXT,'')=COALESCE(ids[1],'') AND
                 COALESCE(r.business_bid_document_id::TEXT,'')=COALESCE(ids[2],'') AND
                 COALESCE(r.technical_bid_document_id::TEXT,'')=COALESCE(ids[3],'')));
            g := g || jsonb_build_object('slot',slot,'bound',FALSE);
            IF candidates=1 THEN
                SELECT * INTO rel FROM xtjs_project_documents r WHERE r.project_id=pid AND
                    (r.upload_group_slot=slot OR (r.upload_group_slot IS NULL AND
                     COALESCE(r.tender_document_id::TEXT,'')=COALESCE(ids[1],'') AND
                     COALESCE(r.business_bid_document_id::TEXT,'')=COALESCE(ids[2],'') AND
                     COALESCE(r.technical_bid_document_id::TEXT,'')=COALESCE(ids[3],'')));
                UPDATE xtjs_project_documents SET upload_group_slot=slot WHERE id=rel.id AND upload_group_slot IS NULL;
                fs := '[]';
                FOR f IN SELECT value FROM jsonb_array_elements(m->'files') LOOP
                    IF f->>'slot' IN (g->>'business_bid',g->>'technical_bid') THEN
                        SELECT identifier_id,file_name,deleted INTO doc FROM xtjs_documents WHERE identifier_id=
                            CASE WHEN f->>'slot'=g->>'business_bid' THEN rel.business_bid_document_id ELSE rel.technical_bid_document_id END;
                        -- Only material replacement changes the slot, never overwrite a live upload lease.
                        IF f->>'status' IS DISTINCT FROM 'uploading' AND f->>'document_id' IS DISTINCT FROM doc.identifier_id::TEXT THEN
                            f := f || jsonb_build_object('document_id',doc.identifier_id,'status',CASE WHEN doc.identifier_id IS NOT NULL AND NOT doc.deleted THEN 'uploaded' ELSE 'failed' END);
                            IF doc.identifier_id IS NOT NULL THEN f := f || jsonb_build_object('name',doc.file_name); END IF;
                        END IF;
                    END IF;
                    fs := fs || jsonb_build_array(f);
                END LOOP;
                m := jsonb_set(m,'{files}',fs);
                g := g || jsonb_build_object('bound',EXISTS(
                    SELECT 1 FROM xtjs_documents t,xtjs_documents b,xtjs_documents tech
                    WHERE t.identifier_id=rel.tender_document_id AND b.identifier_id=rel.business_bid_document_id
                      AND tech.identifier_id=rel.technical_bid_document_id AND NOT t.deleted AND NOT b.deleted AND NOT tech.deleted
                      AND t.identifier_id::TEXT=(SELECT x->>'document_id' FROM jsonb_array_elements(m->'files') x WHERE x->>'slot'='tender')
                      AND b.identifier_id::TEXT=(SELECT x->>'document_id' FROM jsonb_array_elements(m->'files') x WHERE x->>'slot'=g->>'business_bid')
                      AND tech.identifier_id::TEXT=(SELECT x->>'document_id' FROM jsonb_array_elements(m->'files') x WHERE x->>'slot'=g->>'technical_bid')
                ));
            END IF;
            gs := gs || jsonb_build_array(g);
        END LOOP;
        fs := '[]';
        FOR f IN SELECT value FROM jsonb_array_elements(m->'files') LOOP
            IF f->>'status'='uploaded' AND NOT EXISTS (SELECT 1 FROM xtjs_documents d WHERE d.identifier_id::TEXT=f->>'document_id' AND NOT d.deleted) THEN
                f := f || jsonb_build_object('status','failed','error','原文档不存在或已删除');
            END IF;
            IF f->>'status' IS DISTINCT FROM 'uploaded' OR COALESCE(f->>'document_id','')='' THEN complete:=FALSE; END IF;
            fs := fs || jsonb_build_array(f);
        END LOOP;
        IF jsonb_array_length(gs)=0 OR EXISTS(SELECT 1 FROM jsonb_array_elements(gs) x WHERE NOT COALESCE((x->>'bound')::BOOLEAN,FALSE)) THEN complete:=FALSE; END IF;
        m := m || jsonb_build_object('files',fs,'groups',gs);
        UPDATE xtjs_projects SET upload_manifest=m WHERE identifier_id=pid;
    END IF;
    SELECT count(*),bool_and(COALESCE(t.extracted AND NOT t.deleted,FALSE)),
           bool_and(COALESCE(b.extracted AND NOT b.deleted,FALSE)),bool_and(COALESCE(v.extracted AND NOT v.deleted,FALSE))
      INTO n,tender_ok,business_ok,technical_ok FROM xtjs_project_documents r
      LEFT JOIN xtjs_documents t ON t.identifier_id=r.tender_document_id
      LEFT JOIN xtjs_documents b ON b.identifier_id=r.business_bid_document_id
      LEFT JOIN xtjs_documents v ON v.identifier_id=r.technical_bid_document_id WHERE r.project_id=pid;
    UPDATE xtjs_projects SET parsing_status=CASE WHEN NOT complete OR n=0 OR NOT tender_ok THEN 0
        WHEN NOT business_ok THEN 1 WHEN NOT technical_ok THEN 2 ELSE 3 END WHERE identifier_id=pid;
END $$;

CREATE OR REPLACE FUNCTION xtjs_invalidate_materials(pid UUID) RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
    PERFORM 1 FROM xtjs_projects WHERE identifier_id=pid FOR UPDATE;
    INSERT INTO xtjs_result_history(project_identifier_id,input_revision,result_object_key,result,result_keys,result_summary,workflow_scope)
      SELECT project_identifier_id,input_revision,result_object_key,result,result_keys,result_summary,workflow_scope FROM xtjs_result WHERE project_identifier_id=pid
      ON CONFLICT(project_identifier_id,input_revision) DO NOTHING;
    UPDATE xtjs_projects SET input_revision=input_revision+1,report_url='',update_time=CURRENT_TIMESTAMP WHERE identifier_id=pid;
    PERFORM xtjs_sync_materials(pid);
END $$;

CREATE OR REPLACE FUNCTION xtjs_relation_changed() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    PERFORM xtjs_invalidate_materials(COALESCE(NEW.project_id,OLD.project_id));
    RETURN NULL;
END $$;
DROP TRIGGER IF EXISTS xtjs_relation_insert_delete ON xtjs_project_documents;
CREATE TRIGGER xtjs_relation_insert_delete AFTER INSERT OR DELETE ON xtjs_project_documents FOR EACH ROW EXECUTE FUNCTION xtjs_relation_changed();
DROP TRIGGER IF EXISTS xtjs_relation_update ON xtjs_project_documents;
CREATE TRIGGER xtjs_relation_update AFTER UPDATE ON xtjs_project_documents FOR EACH ROW
    WHEN ((OLD.tender_document_id,OLD.business_bid_document_id,OLD.technical_bid_document_id) IS DISTINCT FROM
          (NEW.tender_document_id,NEW.business_bid_document_id,NEW.technical_bid_document_id)) EXECUTE FUNCTION xtjs_relation_changed();

CREATE OR REPLACE FUNCTION xtjs_document_deleted() RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE pid UUID;
BEGIN
    FOR pid IN SELECT DISTINCT project_id FROM xtjs_project_documents WHERE OLD.identifier_id IN
        (tender_document_id,business_bid_document_id,technical_bid_document_id) ORDER BY project_id LOOP
        PERFORM xtjs_invalidate_materials(pid);
    END LOOP;
    RETURN NULL;
END $$;
DROP TRIGGER IF EXISTS xtjs_document_deleted ON xtjs_documents;
CREATE TRIGGER xtjs_document_deleted AFTER UPDATE OF deleted ON xtjs_documents FOR EACH ROW WHEN(OLD.deleted IS DISTINCT FROM NEW.deleted) EXECUTE FUNCTION xtjs_document_deleted();

-- Unambiguous old groups can acquire stable slots; ambiguous groups remain for manual review.
DO $$ DECLARE pid UUID; BEGIN
    FOR pid IN SELECT identifier_id FROM xtjs_projects WHERE upload_manifest IS NOT NULL ORDER BY identifier_id LOOP
        PERFORM xtjs_sync_materials(pid);
    END LOOP;
END $$;
