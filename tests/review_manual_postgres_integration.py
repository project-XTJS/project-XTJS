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
        # Failure metadata survives new connections, never changes revisions/results,
        # and a successful OCR transaction clears it (including late failure races).
        from app.service.ocr_failure import describe_ocr_failure
        s, fail_pid, fail_docs, _ = fixture()
        with connection() as c, c.cursor() as q:
            q.execute("UPDATE xtjs_documents SET extracted=FALSE WHERE identifier_id=%s", (fail_docs[2],))
            q.execute("SELECT input_revision FROM xtjs_projects WHERE identifier_id=%s", (fail_pid,))
            before_failure_revision = q.fetchone()[0]
            q.execute("SELECT result_object_key FROM xtjs_result WHERE project_identifier_id=%s", (fail_pid,))
            before_failure_result = q.fetchone()[0]
        failure = describe_ocr_failure('PDFium: Data format error')
        assert s.record_document_ocr_failure(fail_docs[2], failure)
        payload = Service().get_project_ocr_metadata(fail_pid)
        assert next(d for d in payload['documents'] if str(d['identifier_id']) == fail_docs[2])['ocr_last_error'] == failure
        assert payload['project']['input_revision'] == before_failure_revision
        with connection() as c, c.cursor() as q:
            q.execute("SELECT result_object_key FROM xtjs_result WHERE project_identifier_id=%s", (fail_pid,))
            assert q.fetchone()[0] == before_failure_result
        # OSS failure must preserve the previous failure for a retry.
        with patch.object(blobs, 'save_document_content', side_effect=RuntimeError('storage unavailable')):
            try: s.update_document_content(fail_docs[2], {'marker': 'NEW'})
            except RuntimeError: pass
            else: raise AssertionError('expected storage failure')
        assert next(d for d in s.get_project_ocr_metadata(fail_pid)['documents'] if str(d['identifier_id']) == fail_docs[2])['ocr_last_error'] == failure
        s.update_document_content(fail_docs[2], {'marker': 'NEW'})
        assert not s.record_document_ocr_failure(fail_docs[2], failure)
        assert next(d for d in Service().get_project_ocr_metadata(fail_pid)['documents'] if str(d['identifier_id']) == fail_docs[2])['ocr_last_error'] is None
        with connection() as c, c.cursor() as q:
            q.execute("UPDATE xtjs_documents SET extracted=FALSE,deleted=TRUE WHERE identifier_id=%s", (fail_docs[2],))
        assert not s.record_document_ocr_failure(fail_docs[2], failure)
        results.append('OCR failure persistence, unchanged revisions/results, failed retry, successful retry, late failure and deletion guards')
        s, replace_pid, replace_docs, replace_rels = fixture()
        other_pid = str(s.create_project('shared-original-'+uuid4().hex)['identifier_id'])
        other_rel = s.bind_project_documents(other_pid, *replace_docs[:3])
        def revision():
            with connection() as c, c.cursor() as q:
                q.execute('SELECT input_revision FROM xtjs_projects WHERE identifier_id=%s', (replace_pid,))
                return q.fetchone()[0]
        def refs(pid):
            with connection() as c, c.cursor() as q:
                q.execute('SELECT tender_document_id,business_bid_document_id,technical_bid_document_id FROM xtjs_project_documents WHERE project_id=%s ORDER BY id', (pid,))
                return q.fetchall()
        original_refs, shared_refs = refs(replace_pid), refs(other_pid)
        new_tech = str(s.create_document('new-tech.pdf','minio://fixture/'+uuid4().hex,'technical_bid')['identifier_id'])
        old_revision = revision()
        try:
            s.replace_project_document(replace_pid, replace_docs[2], new_tech, 'technical_bid', old_revision)
            raise AssertionError('unrecognized document accepted')
        except ValueError: pass
        assert refs(replace_pid) == original_refs
        s.update_document_content(new_tech, {'marker':'NEW'})
        s.replace_project_document(replace_pid, replace_docs[2], new_tech, 'technical_bid', old_revision)
        assert str(refs(replace_pid)[0][2]) == new_tech
        assert refs(other_pid) == shared_refs
        assert revision() > old_revision
        assert s.get_project_result(replace_pid)['results_stale']
        try:
            s.replace_project_document(replace_pid, replace_docs[2], new_tech, 'technical_bid', old_revision)
            raise AssertionError('stale replacement accepted')
        except ConsistencyConflict: pass
        new_tender = str(s.create_document('new-tender.pdf','minio://fixture/'+uuid4().hex,'tender')['identifier_id'])
        s.update_document_content(new_tender, {'marker':'NEW-TENDER'})
        before_refs, before_revision = refs(replace_pid), revision()
        @contextmanager
        def commit_failure():
            with connection() as c:
                yield c
                raise RuntimeError('simulated commit failure')
        with patch.object(s, '_get_connection', commit_failure):
            try:
                s.replace_project_document(replace_pid, replace_docs[0], new_tender, 'tender', before_revision)
                raise AssertionError('expected transaction failure')
            except RuntimeError: pass
        assert refs(replace_pid) == before_refs and revision() == before_revision
        result = s.replace_project_document(replace_pid, replace_docs[0], new_tender, 'tender', before_revision)
        assert result['replaced_relation_count'] == 2
        assert all(str(row[0]) == new_tender for row in refs(replace_pid))
        assert refs(other_pid) == shared_refs
        assert s.get_document_by_identifier(replace_docs[0])
        print(json.dumps({'single_file_replacement':'passed','revision_conflict':'passed','commit_rollback':'passed','shared_project':'unchanged'}))
        from app.service.analysis.manual_review.business_bid_format import (
            _build_business_format_editable_items, _save_business_manual_inputs,
            _business_manual_payload_for_project, _apply_manual_business_review_inputs)
        s,pid,docs,rels=fixture()
        requirements={'requires_signature':True,'signature_field_count':1,'requires_seal':False,'requires_date':False,
                      'is_optional':True,'optionality_locations':[{'page':32,'document_role':'tender','text':'附件13（如有）'}]}
        attachment={'title':'附件13','attachment_number':'13','found':True,'pages':[40],'check_pages':[40],
                    'requirements':requirements,'template_locations':[{'page':49,'document_role':'tender'}],
                    'signature_check':{'status':'pass','filled_values':[]},'seal_check':{'status':'not_required'},'date_check':{'status':'not_required'}}
        attachment.update(location_status='matched', location_candidates=[], locations=[{'page':40,'document_role':'business_bid'}])
        value={'signature_status':'pass','seal_status':'not_required','date_status':'not_required'}
        review={'bidders':[{'bidder_key':'fixture','bidder_name':'Fixture','documents':{'business':{'identifier_id':docs[1]}},
                           'checks':{'verification_check':{'raw_result':{'attachment_results':[attachment]},'issues':{},'metrics':{},'review':{'status':'pass'}}}}],
                'extraction_tables':{'bidder_tables':[{'rows':[{'check_code':'verification_check','field_group':'attachment_result','field_name':'附件13','value':value,'page_refs':[40]}]}]}}
        group={'operator':'any_of','source_text':'《许可证》或《备案证明》',
               'branches':[{'title':'许可证','matched':False,'locations':[]},
                           {'title':'备案证明','matched':True,'locations':[{'page':40,'document_role':'business_bid'}]}]}
        review['bidders'][0]['checks']['integrity_check']={'raw_result':{'details':{'证照':{
            'requirement_group':group,'resolution_status':'matched','is_passed':True,
            'template_locations':[{'page':32,'document_role':'tender'},{'page':33,'document_role':'tender'}]}}}}
        s.upsert_project_result_item(pid,'business_bid_format_review',review)
        original_result=s.get_project_result(pid)
        raw_doc=s.get_document_by_identifier(docs[1])
        editable=_build_business_format_editable_items(review,{})[0]
        assert editable['original_value']['requirements']==requirements
        first={**editable,'manual_value':{**editable['original_value'],'signature_evidence':['人工核对签字'], 'review_note':'保留人工意见'}}
        _save_business_manual_inputs(identifier_id=pid,db_service=s,raw_items=[first])
        stored=_business_manual_payload_for_project(identifier_id=pid,db_service=s)
        assert stored['items'][0]['manual_value']['review_note']=='保留人工意见'
        corrected=_apply_manual_business_review_inputs(review,stored)
        effective=corrected['extraction_tables']['bidder_tables'][0]['rows'][0]['value']
        assert effective['location_status']=='matched'
        assert effective['locations']==attachment['locations']
        assert corrected['bidders'][0]['checks']['integrity_check']['raw_result']['details']['证照']['requirement_group']==group
        assert s.get_project_result(pid)['result']['business_bid_format_review']['bidders'][0]['checks']['integrity_check']['raw_result']['details']['证照']['requirement_group']==group
        assert effective['requirements']==requirements
        assert effective['template_locations']==attachment['template_locations']
        assert effective['review_note']=='保留人工意见'
        assert s.get_project_result(pid)['result_object_key']==original_result['result_object_key']
        assert s.get_document_by_identifier(docs[1])['content_object_key']==raw_doc['content_object_key']
        # A second field edit must merge the existing manual opinion; tender requirements stay authoritative.
        second={**editable,'manual_value':{**first['manual_value'],'requirements':{'is_optional':False},'signature_evidence':['再次核对签字']}}
        _save_business_manual_inputs(identifier_id=pid,db_service=s,raw_items=[second])
        reread=_business_manual_payload_for_project(identifier_id=pid,db_service=s)
        corrected=_apply_manual_business_review_inputs(review,reread)
        effective=corrected['extraction_tables']['bidder_tables'][0]['rows'][0]['value']
        assert effective['location_status']=='matched'
        assert effective['locations']==attachment['locations']
        assert corrected['bidders'][0]['checks']['integrity_check']['raw_result']['details']['证照']['requirement_group']==group
        assert s.get_project_result(pid)['result']['business_bid_format_review']['bidders'][0]['checks']['integrity_check']['raw_result']['details']['证照']['requirement_group']==group
        assert effective['requirements']==requirements
        assert effective['review_note']=='保留人工意见'
        assert review['extraction_tables']['bidder_tables'][0]['rows'][0]['value']==value
        print(json.dumps({'manual_save_reread':'passed','requirements_and_roles':'preserved','historical_result_and_ocr':'unchanged'}))
finally:
    with bootstrap.cursor() as cur:cur.execute(sql.SQL('DROP SCHEMA IF EXISTS {} CASCADE').format(sql.Identifier(schema)))
    bootstrap.close()
