"""Run explicitly: isolated PostgreSQL schema; object writes use an in-memory store."""
import copy
import json
import threading
import asyncio
import io
from pathlib import Path
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4
from unittest.mock import patch, Mock, AsyncMock

import psycopg2
from psycopg2 import sql
from app.config.settings import settings
from app.service import document_blob_store as blobs
from app.service.postgresql_service import PostgreSQLService


schema='xtjs_storage_test_'+uuid4().hex
pid=str(uuid4())
store={}
store_lock=threading.Lock()
bootstrap=psycopg2.connect(settings.DATABASE_URL)
bootstrap.autocommit=True


@contextmanager
def connection():
    conn=psycopg2.connect(settings.DATABASE_URL, options=f'-c search_path={schema} -c statement_timeout=15000 -c lock_timeout=10000')
    try:
        with conn: yield conn
    finally: conn.close()


class TestService(PostgreSQLService):
    _get_connection=staticmethod(connection)

    def _prepare_project_result_for_persistence(self,pid,result,existing=None):
        return result


class MemoryBlobClient:
    def put_json_gz(self,key,value):
        with store_lock: store[key]=copy.deepcopy(value)

    def get_json_gz(self,key):
        with store_lock: return copy.deepcopy(store.get(key))


try:
    with bootstrap.cursor() as cur:
        cur.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(schema)))
    with connection() as conn, conn.cursor() as cur:
        for table in ['xtjs_projects','xtjs_result','xtjs_documents','xtjs_project_documents','xtjs_users','xtjs_tender_reviews']:
            cur.execute(sql.SQL('CREATE TABLE {} (LIKE public.{} INCLUDING ALL)').format(sql.Identifier(table),sql.Identifier(table)))
            cur.execute("SELECT column_name FROM information_schema.columns WHERE table_schema=%s AND table_name=%s AND column_default LIKE 'nextval%%'",(schema,table))
            for (col,) in cur.fetchall():
                seq=table+'_'+col+'_fixture_seq';cur.execute(sql.SQL('CREATE SEQUENCE {}').format(sql.Identifier(seq)))
                cur.execute(sql.SQL('ALTER TABLE {} ALTER COLUMN {} SET DEFAULT nextval({})').format(sql.Identifier(table),sql.Identifier(col),sql.Literal(schema+'.'+seq)))
        cur.execute(Path('db/migration/V20260914193000__audit_consistency.sql').read_text())
        cur.execute('''ALTER TABLE xtjs_projects ADD COLUMN IF NOT EXISTS upload_manifest jsonb;
            ALTER TABLE xtjs_result ADD COLUMN IF NOT EXISTS workflow_scope jsonb;''')
        cur.execute('INSERT INTO xtjs_projects(identifier_id,project_name) VALUES(%s,%s)',(pid,'test'))
    service=TestService()
    client=MemoryBlobClient()
    with patch.object(blobs,'_client',return_value=client):
        service.upsert_project_result_item(pid,'original',{'status':'pass'})
        barrier=threading.Barrier(2)
        def update(key):
            barrier.wait(timeout=5)
            return TestService().upsert_project_result_item(pid,key,{'status':'pass'})
        with ThreadPoolExecutor(max_workers=2) as pool:
            list(pool.map(update,['concurrent_a','concurrent_b']))
        record=service.get_project_result(pid)
        assert set(record['result'])=={'original','concurrent_a','concurrent_b'}
        original_key=record['result_object_key']
        with patch.object(client,'get_json_gz',side_effect=RuntimeError('transient')):
            try: service.upsert_project_result_item(pid,'must_not_save',{})
            except blobs.BlobReadError: pass
            else: raise AssertionError('read failure was swallowed')
        assert service.get_project_result(pid)['result_object_key']==original_key
        with connection() as conn,conn.cursor() as cur:
            cur.execute("ALTER TABLE xtjs_result ADD CONSTRAINT reject_fixture CHECK (NOT result_keys ? 'sql_failure')")
        try: service.upsert_project_result_item(pid,'sql_failure',{})
        except psycopg2.IntegrityError: pass
        else: raise AssertionError('database fault was not injected')
        after=service.get_project_result(pid)
        assert after['result_object_key']==original_key
        assert set(after['result'])=={'original','concurrent_a','concurrent_b'}
        service.update_project_manual_review_workflow_scope(pid,{'excluded_bidders':[]})
        service.update_project_manual_review_result(pid,'manual_check',{'status':'pass'})
        service.clear_project_manual_review_latest_result(pid,'manual_check')
        final=service.get_project_result(pid)
        assert {'original','concurrent_a','concurrent_b'}.issubset(final['result'])
    # Exercise initial partial upload and manual repair against the actual DB methods.
    from fastapi import UploadFile, HTTPException
    from app.router import postgresql_batch as batch
    fail_technical=True
    async def fake_upload(**kwargs):
        if fail_technical and kwargs.get('document_name')=='B技术标.pdf':
            return {'ok':False,'status_code':500,'error':'simulated upload failure'}
        name=kwargs.get('document_name') or kwargs['file'].filename
        doc=service.create_document(name,'minio://fixture/'+uuid4().hex,kwargs['document_type'])
        return {'ok':True,'document':doc,'document_summary':doc,'upload':{}}
    paths=['folder/招标文件.pdf','folder/A/商务标.pdf','folder/A/技术标.pdf','folder/B/商务标.pdf','folder/B/技术标.pdf']
    files=[UploadFile(filename=p.split('/')[-1],file=io.BytesIO(b'fixture')) for p in paths]
    async def upload_test():
        global fail_technical
        with patch.object(batch,'upload_and_create_document_without_ocr',side_effect=fake_upload), \
             patch.object(batch,'_invalidate_project_cache_or_error'), \
             patch.object(batch,'_invalidate_project_cache_for_task'):
            result=await batch.upload_project_folder(files=files,paths=json.dumps(paths),db_service=service,oss_service=Mock())
            assert result['status']=='partial_success'
            project_id=str(result['project']['identifier_id'])
            project=service.get_project_by_identifier(project_id)
            assert not project['upload_complete']
            assert len(project['upload_issues'])==1
            assert len(service.get_project_detail(project_id)['relations'])==1
            fail_technical=False
            repaired=await batch.upload_missing_project_file(project_id,slot='technical_bid:2',attempt_id=None,
                file=UploadFile(filename='技术标.pdf',file=io.BytesIO(b'fixture')),db_service=service,oss_service=Mock())
            assert repaired['project']['upload_complete']
            assert len(repaired['relations'])==2
            # Metadata path must work even when every object read is unavailable.
            with patch.object(blobs,'_client',side_effect=AssertionError('unexpected OSS access')):
                meta=service.get_project_ocr_metadata(project_id)
                assert len(meta['documents'])==4
                assert service.refresh_project_parsing_status(project_id)['parsing_status']==0
    asyncio.run(upload_test())
    print(json.dumps({'concurrent_updates':'passed','read_failure_preserves_old_pointer':'passed',
        'database_failure_preserves_old_object':'passed','manual_result_writers':'passed',
        'partial_upload_and_manual_repair':'passed','metadata_query_without_oss':'passed'}))
finally:
    with bootstrap.cursor() as cur:
        cur.execute(sql.SQL('DROP SCHEMA IF EXISTS {} CASCADE').format(sql.Identifier(schema)))
    bootstrap.close()
