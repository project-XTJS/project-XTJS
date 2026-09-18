ALTER TABLE xtjs_projects ADD COLUMN owner_user_id UUID REFERENCES xtjs_users(identifier_id);
ALTER TABLE xtjs_documents ADD COLUMN owner_user_id UUID REFERENCES xtjs_users(identifier_id);
CREATE TABLE xtjs_project_access (
 project_id UUID NOT NULL REFERENCES xtjs_projects(identifier_id),
 user_id UUID NOT NULL REFERENCES xtjs_users(identifier_id),
 PRIMARY KEY(project_id,user_id)
);
CREATE TABLE xtjs_document_access (
 document_id UUID NOT NULL REFERENCES xtjs_documents(identifier_id),
 user_id UUID NOT NULL REFERENCES xtjs_users(identifier_id),
 PRIMARY KEY(document_id,user_id)
);
-- Freeze historical access once. New accounts never inherit it implicitly.
INSERT INTO xtjs_project_access SELECT p.identifier_id,u.identifier_id FROM xtjs_projects p CROSS JOIN xtjs_users u WHERE NOT p.deleted AND NOT u.deleted;
INSERT INTO xtjs_document_access SELECT DISTINCT r.document_identifier_id,u.identifier_id FROM xtjs_tender_reviews r JOIN xtjs_documents d ON d.identifier_id=r.document_identifier_id CROSS JOIN xtjs_users u WHERE NOT r.deleted AND NOT d.deleted AND NOT u.deleted;
ALTER TABLE xtjs_projects ALTER COLUMN owner_user_id SET DEFAULT nullif(current_setting('xtjs.actor_id',true),'')::uuid;
ALTER TABLE xtjs_documents ALTER COLUMN owner_user_id SET DEFAULT nullif(current_setting('xtjs.actor_id',true),'')::uuid;
CREATE INDEX ON xtjs_projects(owner_user_id) WHERE NOT deleted;
CREATE INDEX ON xtjs_documents(owner_user_id) WHERE NOT deleted;
CREATE OR REPLACE FUNCTION xtjs_can_access_project(pid UUID, uid UUID) RETURNS BOOLEAN LANGUAGE SQL STABLE AS $$
 SELECT EXISTS(SELECT 1 FROM xtjs_projects p WHERE p.identifier_id=pid AND NOT p.deleted AND
 (p.owner_user_id=uid OR EXISTS(SELECT 1 FROM xtjs_project_access a WHERE a.project_id=pid AND a.user_id=uid)))
$$;
CREATE OR REPLACE FUNCTION xtjs_can_access_document(did UUID, uid UUID) RETURNS BOOLEAN LANGUAGE SQL STABLE AS $$
 SELECT EXISTS(SELECT 1 FROM xtjs_documents d WHERE d.identifier_id=did AND NOT d.deleted AND
 (d.owner_user_id=uid OR EXISTS(SELECT 1 FROM xtjs_document_access a WHERE a.document_id=did AND a.user_id=uid)
 OR EXISTS(SELECT 1 FROM xtjs_project_documents pd WHERE did IN(pd.tender_document_id,pd.business_bid_document_id,pd.technical_bid_document_id) AND xtjs_can_access_project(pd.project_id,uid))))
$$;

-- Bounded homepage identity metadata avoids downloading OCR during project listing.
ALTER TABLE xtjs_documents ADD COLUMN IF NOT EXISTS bidder_identity JSONB;
