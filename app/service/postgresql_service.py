import psycopg2
import uuid
from psycopg2 import pool, extras
from contextlib import contextmanager
# ========================psycopg2 func========================
connection_pool = psycopg2.pool.ThreadedConnectionPool(minconn=1, maxconn=20, **POSTGRESQL_CONFIG)

@contextmanager
def get_db_connection():
    conn = connection_pool.getconn()
    try:
        yield conn
    finally:
        connection_pool.putconn(conn)


@contextmanager
def get_db_cursor(commit=False):
    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=extras.RealDictCursor) as cur:
            try:
                yield cur
                if commit:
                    conn.commit()
            except Exception as e:
                conn.rollback()
                raise e

# ------ xtjs_documents ------
def create_document(document_data: dict) -> dict:
    document_data["identifier_id"] = str(uuid.uuid4())
    query = """
            INSERT INTO xtjs_documents (identifier_id, file_name, file_url)
            VALUES (%(identifier_id)s, %(file_name)s,
                    %(file_url)s)
                RETURNING identifier_id, file_name, file_url, update_time; \
            """
    with get_db_cursor(commit=True) as cur:
        cur.execute(query, document_data)
        result = cur.fetchone()
        return dict(result)