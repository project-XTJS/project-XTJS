"""Explicit integration suite: isolated schema and memory OSS; never changes project data."""
import asyncio, copy, json, threading, io
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path
from uuid import uuid4
from unittest.mock import patch, Mock
import psycopg2
from psycopg2 import sql
from app.config.settings import settings
from app.service import document_blob_store as blobs
from app.service.postgresql_service import PostgreSQLService
from app.core.consistency import ConsistencyConflict

schema='xtjs_fix_test_'+uuid4().hex
store={}; results=[]
bootstrap=psycopg2.connect(settings.DATABASE_URL);bootstrap.autocommit=True
@contextmanager
def connection():
    conn=psycopg2.connect(settings.DATABASE_URL,options=f'-c search_path={schema} -c statement_timeout=15000 -c lock_timeout=10000')
    try:
        with conn:yield conn
    finally:conn.close()
class Service(PostgreSQLService):
    _get_connection=staticmethod(connection)
    def _prepare_project_result_for_persistence(self,pid,result,existing=None):return result
class Memory:
    def put_json_gz(self,key,value):store[key]=copy.deepcopy(value)
    def get_json_gz(self,key):return copy.deepcopy(store.get(key))
def fixture():
    s=Service();pid=str(s.create_project('isolated-'+uuid4().hex)['identifier_id']);docs=[];files=[]
    for slot,role in [('tender','tender'),('business_bid:1','business_bid'),('technical_bid:1','technical_bid'),('business_bid:2','business_bid'),('technical_bid:2','technical_bid')]:
        did=str(s.create_document(slot+'.pdf','minio://fixture/'+uuid4().hex,role)['identifier_id'])
        s.update_document_content(did,{'marker':'OLD'},source_file_hash='old-hash')
        docs.append(did);files.append({'slot':slot,'name':slot+'.pdf','company':slot,'role':role,'status':'uploaded','document_id':did})
    groups=[{'company':str(i),'business_bid':f'business_bid:{i}','technical_bid':f'technical_bid:{i}','bound':False} for i in [1,2]]
    s.initialize_upload_manifest(pid,{'version':1,'files':files,'groups':groups})
    rels=[s.bind_project_documents(pid,docs[0],docs[1],docs[2]),s.bind_project_documents(pid,docs[0],docs[3],docs[4])]
    s.upsert_project_result_item(pid,'old_a',{'status':'pass'})
    s.upsert_project_result_item(pid,'old_b',{'status':'pass'})
    return s,pid,docs,rels
try:
    with bootstrap.cursor() as cur:cur.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(schema)))
    with connection() as conn,conn.cursor() as cur:
        for table in ['xtjs_projects','xtjs_result','xtjs_documents','xtjs_project_documents','xtjs_users','xtjs_tender_reviews']:
            cur.execute(sql.SQL('CREATE TABLE {} (LIKE public.{} INCLUDING ALL)').format(sql.Identifier(table),sql.Identifier(table)))
            cur.execute("SELECT column_name FROM information_schema.columns WHERE table_schema=%s AND table_name=%s AND column_default LIKE 'nextval%%'",(schema,table))
            for (col,) in cur.fetchall():
                seq=table+'_'+col+'_fixture_seq';cur.execute(sql.SQL('CREATE SEQUENCE {}').format(sql.Identifier(seq)))
                cur.execute(sql.SQL('ALTER TABLE {} ALTER COLUMN {} SET DEFAULT nextval({})').format(sql.Identifier(table),sql.Identifier(col),sql.Literal(schema+'.'+seq)))
        cur.execute(Path('db/migration/V20260914193000__audit_consistency.sql').read_text())
    with patch.object(blobs,'_client',return_value=Memory()):
        s,pid,docs,rels=fixture()
        try:s.update_document(docs[1],file_url='minio://fixture/new')
        except ConsistencyConflict:pass
        else:raise AssertionError('source replacement accepted')
        s.update_document(docs[1],file_name='renamed.pdf')
        assert s.get_document_by_identifier(docs[1])['content']['marker']=='OLD'
        results.append('source_replacement_rejected_and_rename_preserved')
        old_worker=Service();old_worker.get_project_documents_for_duplicate_check(pid)
        s.delete_relation(rels[1]['id'])
        project=s.get_project_by_identifier(pid)
        assert project['upload_complete'] is False
        assert len(s.get_project_detail(pid)['relations'])==1
        assert Service().get_project_result(pid)['results_stale'] is True
        assert Service().get_project_result(pid)['result']=={}
        try:old_worker.upsert_project_result_item(pid,'late',{})
        except ConsistencyConflict:pass
        else:raise AssertionError('late worker accepted')
        results.append('relation_change_invalidates_and_rejects_late_worker')
        fresh=Service();fresh.upsert_project_result_item(pid,'new_only',{})
        assert set(Service().get_project_result(pid)['result'])=={'new_only'}
        with connection() as conn,conn.cursor() as cur:
            cur.execute('SELECT count(*) FROM xtjs_result_history WHERE project_identifier_id=%s',(pid,));assert cur.fetchone()[0]>=1
        results.append('old_results_archived_not_merged')
        # Only one concurrent claimant can upload a missing logical slot.
        s.record_upload_file(pid,'technical_bid:2',error='fixture missing')
        barrier=threading.Barrier(2)
        def claim(token):
            barrier.wait(timeout=5)
            try:return Service().claim_upload(pid,'technical_bid:2',token)['lease_id']
            except ConsistencyConflict:return None
        with ThreadPoolExecutor(max_workers=2) as pool:winners=list(pool.map(claim,['one','two']))
        winner=next(t for t in winners if t);assert sum(t is not None for t in winners)==1
        newdoc=str(s.create_document('technical_bid:2.pdf','minio://fixture/'+uuid4().hex,'technical_bid')['identifier_id'])
        s.finish_upload(pid,'technical_bid:2',winner,document_id=newdoc)
        s.finish_upload(pid,'technical_bid:2',winner,document_id=newdoc)
        assert len(s.get_project_detail(pid)['relations'])==2
        assert s.get_project_by_identifier(pid)['upload_complete'] is True
        try:s.finish_upload(pid,'technical_bid:2','stale-attempt',document_id=docs[4])
        except ConsistencyConflict:pass
        else:raise AssertionError('late upload overwrote slot')
        results.append('concurrent_retry_idempotent_and_stale_completion_rejected')
        s.record_upload_file(pid,'technical_bid:2',error='retry fixture')
        first=s.claim_upload(pid,'technical_bid:2','same-client-request')
        with connection() as conn,conn.cursor() as cur:
            cur.execute("UPDATE xtjs_projects SET upload_manifest=jsonb_set(upload_manifest,'{files,4,lease_expires_at}',to_jsonb('2000-01-01T00:00:00+00:00'::text)) WHERE identifier_id=%s",(pid,))
        second=s.claim_upload(pid,'technical_bid:2','same-client-request')
        assert first['lease_id']!=second['lease_id']
        try:s.finish_upload(pid,'technical_bid:2',first['lease_id'],document_id=docs[4])
        except ConsistencyConflict:pass
        else:raise AssertionError('expired lease with same request ID accepted')
        s.finish_upload(pid,'technical_bid:2',second['lease_id'],document_id=newdoc)
        results.append('lease_generation_fences_same_request_retries')
        # Removing the bidder explicitly updates expected materials instead of inventing a missing group.
        s.delete_relation(rels[0]['id'],remove_expected_group=True)
        assert s.get_project_by_identifier(pid)['upload_complete'] is True
        assert len(s.get_project_by_identifier(pid)['upload_manifest']['groups'])==1
        results.append('explicit_bidder_removal_updates_expectations')
        s.soft_delete_document(newdoc)
        assert s.get_project_by_identifier(pid)['upload_complete'] is False
        results.append('document_soft_delete_invalidates_references')
        # A failed SQL update may leave a new unreferenced object, but never mutates the old one.
        s.update_document_review_content(docs[1],effective_content={'marker':'OLD_MANUAL'})
        old=s.get_document_by_identifier(docs[1]);key=old['review_content_object_key'];saved=copy.deepcopy(store[key])
        with connection() as conn,conn.cursor() as cur:
            cur.execute('ALTER TABLE xtjs_documents ADD CONSTRAINT reject_manual CHECK(review_content_object_key IS NULL) NOT VALID')
        try:s.update_document_review_content(docs[1],effective_content={'marker':'NEW_MANUAL'})
        except psycopg2.IntegrityError:pass
        else:raise AssertionError('fault injection did not trigger')
        assert s.get_document_by_identifier(docs[1])['review_content_object_key']==key
        assert store[key]==saved
        results.append('manual_sql_failure_preserves_committed_content')
        with connection() as conn,conn.cursor() as cur:cur.execute('ALTER TABLE xtjs_documents DROP CONSTRAINT reject_manual')
        # Faults after writing any of the three blob families retain the last committed pointer.
        tender_review=s.create_tender_review(docs[0]);rid=str(tender_review['identifier_id'])
        first=blobs.save_tender_review_result({'marker':'OLD'},review_identifier_id=rid)
        s.complete_tender_review(rid,result_object_key=first,summary={})
        @contextmanager
        def commit_failure():
            conn=psycopg2.connect(settings.DATABASE_URL,options=f'-c search_path={schema}')
            try:
                with conn:
                    yield conn
                    # A deferred trigger fails specifically during COMMIT.
            finally:conn.close()
        class CommitService(Service):_get_connection=staticmethod(commit_failure)
        with connection() as conn,conn.cursor() as cur:
            cur.execute("CREATE FUNCTION fail_commit() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected commit failure'; END $$")
        for family,table,write,read in [
            ('ocr','xtjs_documents',lambda svc:svc.update_document_content(docs[1],{'marker':'NEW'}),lambda:s.get_document_by_identifier(docs[1])['content_object_key']),
            ('manual','xtjs_documents',lambda svc:svc.update_document_review_content(docs[1],effective_content={'marker':'NEW'}),lambda:s.get_document_by_identifier(docs[1])['review_content_object_key']),
            ('tender','xtjs_tender_reviews',lambda svc:svc.complete_tender_review(rid,result_object_key=blobs.save_tender_review_result({'marker':'NEW'},review_identifier_id=rid),summary={}),lambda:s.get_tender_review(rid)['result_object_key'])]:
            previous=read();before=copy.deepcopy(store[previous])
            with patch.object(Memory,'put_json_gz',side_effect=RuntimeError('injected OSS failure')):
                try:write(s)
                except RuntimeError:pass
                else:raise AssertionError('OSS fault missing')
            assert read()==previous and store[previous]==before
            with connection() as conn,conn.cursor() as cur:
                cur.execute(sql.SQL('CREATE CONSTRAINT TRIGGER inject_commit AFTER UPDATE ON {} DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION fail_commit()').format(sql.Identifier(table)))
            try:write(CommitService())
            except psycopg2.Error:pass
            else:raise AssertionError('COMMIT fault missing')
            assert read()==previous and store[previous]==before
            with connection() as conn,conn.cursor() as cur:cur.execute(sql.SQL('DROP TRIGGER inject_commit ON {}').format(sql.Identifier(table)))
            results.append(f'{family}_oss_and_commit_failure_preserve_content')
        # The same source may be referenced elsewhere; replacement never mutates it.
        other,otherpid,otherdocs,otherrels=fixture()
        shared=str(other.create_project('shared-reference')['identifier_id'])
        other.bind_project_documents(shared,otherdocs[0],otherdocs[1],otherdocs[2])
        replacement=str(other.create_document('new business.pdf','minio://fixture/'+uuid4().hex,'business_bid')['identifier_id'])
        other.update_relation(otherrels[0]['id'],otherdocs[0],replacement,otherdocs[2])
        assert other.get_document_by_identifier(replacement)['extracted'] is False
        assert str(other.get_project_detail(shared)['relations'][0]['business_bid_identifier_id'])==otherdocs[1]
        assert Service().get_project_result(otherpid)['results_stale'] is True
        # All groups must agree before the shared tender position can switch.
        tender2=str(other.create_document('replacement tender.pdf','minio://fixture/'+uuid4().hex,'tender')['identifier_id'])
        other.update_relation(otherrels[0]['id'],tender2,replacement,otherdocs[2])
        assert other.get_project_by_identifier(otherpid)['upload_complete'] is False
        other.update_relation(otherrels[1]['id'],tender2,otherdocs[3],otherdocs[4])
        assert other.get_project_by_identifier(otherpid)['upload_complete'] is True
        other.soft_delete_document(otherdocs[2])
        assert other.get_project_by_identifier(otherpid)['upload_complete'] is False
        assert other.get_project_by_identifier(shared)['upload_complete'] is False
        results.append('shared_replacement_tender_sync_and_soft_delete')
        # A failed initial upload retains the expected slot and can be manually resumed.
        partial=Service();partialpid=str(partial.create_project('partial-upload')['identifier_id'])
        manifest={'version':1,'files':[{'slot':slot,'name':slot+'.pdf','role':role,'status':'pending'} for slot,role in [('tender','tender'),('business_bid:1','business_bid'),('technical_bid:1','technical_bid')]],'groups':[{'company':'fixture','business_bid':'business_bid:1','technical_bid':'technical_bid:1','bound':False}]}
        partial.initialize_upload_manifest(partialpid,manifest)
        for slot,did in [('tender',otherdocs[0]),('business_bid:1',otherdocs[1])]:
            lease=partial.claim_upload(partialpid,slot,uuid4().hex)
            partial.finish_upload(partialpid,slot,lease['lease_id'],document_id=did)
        lease=partial.claim_upload(partialpid,'technical_bid:1','fail')
        partial.finish_upload(partialpid,'technical_bid:1',lease['lease_id'],error='injected upload failure')
        assert partial.get_project_by_identifier(partialpid)['upload_complete'] is False
        lease=partial.claim_upload(partialpid,'technical_bid:1','resume')
        partial.finish_upload(partialpid,'technical_bid:1',lease['lease_id'],document_id=otherdocs[4])
        assert partial.get_project_by_identifier(partialpid)['upload_complete'] is True
        assert len(partial.get_project_detail(partialpid)['relations'])==1
        assert partial.claim_upload(partialpid,'technical_bid:1','resume')['document_id']==otherdocs[4]
        results.append('initial_partial_upload_and_manual_resume')
        # Schema-backed auth tests use a temporary signing key only.
        from app.service.user_service import UserService
        from app.core.security import create_access_token
        from app.router.auth_dependencies import get_current_user
        from fastapi.security import HTTPAuthorizationCredentials
        from fastapi import HTTPException
        class Users(UserService):_get_connection=staticmethod(connection)
        users=Users();u=users.create_user('fixture-user','OldFixture42')
        with patch.object(settings,'JWT_SECRET_KEY','fixture-only-key-not-valid-on-platform'):
            token=create_access_token(u['identifier_id'],1)
            credentials=HTTPAuthorizationCredentials(scheme='Bearer',credentials=token)
            assert get_current_user(credentials,users)
            users.reset_password(u['identifier_id'],'NewFixture43')
            try:get_current_user(credentials,users)
            except HTTPException as exc:assert exc.status_code==401
            else:raise AssertionError('old token accepted')
            new=create_access_token(u['identifier_id'],1,1)
            assert get_current_user(HTTPAuthorizationCredentials(scheme='Bearer',credentials=new),users)
            users.update_user(u['identifier_id'],display_name='new fixture display')
            assert get_current_user(HTTPAuthorizationCredentials(scheme='Bearer',credentials=new),users)
            before=users.get_auth_record_by_username('fixture-user')
            with connection() as conn,conn.cursor() as cur:
                cur.execute('CREATE CONSTRAINT TRIGGER inject_password_commit AFTER UPDATE ON xtjs_users DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION fail_commit()')
            try:users.reset_password(u['identifier_id'],'FailedFixture44')
            except psycopg2.Error:pass
            else:raise AssertionError('password commit fault missing')
            after=users.get_auth_record_by_username('fixture-user')
            assert before['hashed_password']==after['hashed_password'] and before['token_version']==after['token_version']
            from app.core.security import verify_password
            assert verify_password('NewFixture43',after['hashed_password'])
            assert get_current_user(HTTPAuthorizationCredentials(scheme='Bearer',credentials=new),users)
        results.append('password_reset_invalidates_old_token')
    print(json.dumps({'passed':results},ensure_ascii=False,indent=2))
finally:
    with bootstrap.cursor() as cur:cur.execute(sql.SQL('DROP SCHEMA IF EXISTS {} CASCADE').format(sql.Identifier(schema)))
    bootstrap.close()
