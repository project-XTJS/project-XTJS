ALTER TABLE xtjs_projects
    ADD COLUMN IF NOT EXISTS parsing_status INTEGER NOT NULL DEFAULT 0;

UPDATE xtjs_projects p
SET parsing_status = CASE
    WHEN EXISTS (
        SELECT 1
        FROM xtjs_project_documents pd
        WHERE pd.project_id = p.id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM xtjs_project_documents pd
        JOIN xtjs_documents td
          ON td.id = pd.tender_document_id
        JOIN xtjs_documents bbd
          ON bbd.id = pd.business_bid_document_id
        WHERE pd.project_id = p.id
          AND (
              COALESCE(td.extracted, FALSE) = FALSE
              OR COALESCE(bbd.extracted, FALSE) = FALSE
          )
    )
    AND NOT EXISTS (
        SELECT 1
        FROM xtjs_project_documents pd
        JOIN xtjs_documents tbd
          ON tbd.id = pd.technical_bid_document_id
        WHERE pd.project_id = p.id
          AND pd.technical_bid_document_id IS NOT NULL
          AND COALESCE(tbd.extracted, FALSE) = FALSE
    )
    THEN 1
    ELSE 0
END;

COMMENT ON COLUMN xtjs_projects.parsing_status IS
    '项目解析状态：0-未全部完成OCR，1-项目关联的招标文件、商务标、技术标均已完成OCR';

CREATE INDEX IF NOT EXISTS idx_xtjs_projects_parsing_status
    ON xtjs_projects (parsing_status);
