DO
$$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'file_content_pkey'
          AND conrelid = 'xtjs_file_content'::regclass
    ) THEN
        ALTER TABLE xtjs_file_content
            RENAME CONSTRAINT file_content_pkey TO xtjs_file_content_pkey;
    END IF;
END
$$;

ALTER SEQUENCE IF EXISTS file_content_id_seq
    RENAME TO xtjs_file_content_id_seq;

DO
$$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_class
        WHERE relname = 'xtjs_file_content_id_seq'
          AND relkind = 'S'
    ) THEN
        ALTER TABLE xtjs_file_content
            ALTER COLUMN id SET DEFAULT nextval('xtjs_file_content_id_seq'::regclass);
    END IF;
END
$$;
