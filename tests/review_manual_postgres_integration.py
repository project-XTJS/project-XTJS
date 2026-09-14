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
        from app.service.analysis.manual_review.business_bid_format import (
            _build_business_format_editable_items, _save_business_manual_inputs,
            _business_manual_payload_for_project, _apply_manual_business_review_inputs)
        s,pid,docs,rels=fixture()
        requirements={'requires_signature':True,'signature_field_count':1,'requires_seal':False,'requires_date':False,
                      'is_optional':True,'optionality_locations':[{'page':32,'document_role':'tender','text':'附件13（如有）'}]}
        attachment={'title':'附件13','attachment_number':'13','found':True,'pages':[40],'check_pages':[40],
                    'requirements':requirements,'template_locations':[{'page':49,'document_role':'tender'}],
                    'signature_check':{'status':'pass','filled_values':[]},'seal_check':{'status':'not_required'},'date_check':{'status':'not_required'}}
        value={'signature_status':'pass','seal_status':'not_required','date_status':'not_required'}
        review={'bidders':[{'bidder_key':'fixture','bidder_name':'Fixture','documents':{'business':{'identifier_id':docs[1]}},
                           'checks':{'verification_check':{'raw_result':{'attachment_results':[attachment]},'issues':{},'metrics':{},'review':{'status':'pass'}}}}],
                'extraction_tables':{'bidder_tables':[{'rows':[{'check_code':'verification_check','field_group':'attachment_result','field_name':'附件13','value':value,'page_refs':[40]}]}]}}
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
        assert effective['requirements']==requirements
        assert effective['review_note']=='保留人工意见'
        assert review['extraction_tables']['bidder_tables'][0]['rows'][0]['value']==value
        print(json.dumps({'manual_save_reread':'passed','requirements_and_roles':'preserved','historical_result_and_ocr':'unchanged'}))
finally:
    with bootstrap.cursor() as cur:cur.execute(sql.SQL('DROP SCHEMA IF EXISTS {} CASCADE').format(sql.Identifier(schema)))
    bootstrap.close()
