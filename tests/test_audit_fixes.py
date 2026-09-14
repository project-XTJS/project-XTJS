import asyncio
import copy
import time
import unittest
from unittest.mock import Mock, patch
from fastapi import HTTPException, Response
from fastapi.security import HTTPAuthorizationCredentials
from app.service import document_blob_store as blobs
from app.service.analysis.verification import VerificationChecker
from app.service.analysis import bidder_identity
from app.core.security import create_access_token
from app.config.settings import settings

class IdentityTests(unittest.TestCase):
    def setUp(self):self.checker=VerificationChecker(None)
    def payload(self,*lines):return {'layout_sections':[{'page':1,'text':line,'type':'text'} for line in lines]}
    def test_parentheses_are_kept_and_ocr_is_untouched(self):
        for name in ['捷飨（上海）餐饮管理有限公司','至和益科技（深圳）有限公司','上海天焱餐饮管理有限公司','新闻报社']:
            payload=self.payload('投标单位：'+name);before=copy.deepcopy(payload)
            self.assertEqual(self.checker._bidder_name(payload,[]),name)
            self.assertEqual(payload,before)
    def test_directory_and_contract_company_are_not_subject(self):
        payload={'layout_sections':[{'page':1,'text':'投标单位：上海百联商贸有限公司'}, {'page':2,'text':'目录'},{'page':2,'text':'投标人：上海国有资本投资有限公司……68'}]}
        self.assertEqual(self.checker._bidder_name(payload,[]),'上海百联商贸有限公司')
        self.assertIsNone(self.checker._bidder_name(self.payload('历史合同甲方：上海其他有限公司'),['上海其他有限公司']))
    def test_explicit_fields_on_historical_contract_pages_are_excluded(self):
        payload={'layout_sections':[{'page':1,'text':'投标人：上海星淼文化传媒有限公司'}, {'page':9,'text':'甲方：上海客户有限公司\n乙方：上海航界文化传媒有限公司\n供应商名称：上海航界文化传媒有限公司'}]}
        self.assertEqual(self.checker._bidder_name(payload,[]),'上海星淼文化传媒有限公司')
    def test_table_field_stays_in_its_cell(self):
        payload=self.payload('<tr><td>供应商名称</td><td>至和益科技（深圳）有限公司</td><td>联系人</td><td>张三</td></tr>')
        self.assertEqual(self.checker._bidder_name(payload,[]),'至和益科技（深圳）有限公司')
    def test_conflicting_explicit_names_need_confirmation(self):
        identity=self.checker._bidder_identity(self.payload('投标人：上海甲方科技有限公司','投标单位：上海乙方科技有限公司'))
        self.assertIsNone(identity['name']);self.assertEqual(identity['reason'],'conflicting_bidder_fields')
    def test_branch_and_instruction_boundaries(self):
        for name in ['中国电信股份有限公司上海分公司','中国联合网络通信有限公司上海市分公司','新闻报社']:
            self.assertEqual(self.checker._bidder_name(self.payload('投标人：（名称加盖公章）'+name),[]),name)
    def test_conflicting_cover_ocr_needs_confirmation_without_correction(self):
        payload={'layout_sections':[{'page':1,'text':'投标单位：深圳市骑士动音商贸有限公司'},{'page':8,'text':'投标人：（加盖公章）深圳市骑士勋章商贸有限公司'}]}
        self.assertIsNone(self.checker._bidder_name(payload,[]))
        self.assertEqual(self.checker._bidder_identity(payload)['reason'],'conflicting_bidder_fields')
    def test_generic_suffix_and_near_matches_cannot_pass(self):
        for name,seal in [('餐饮管理有限公司','上海天焮餐饮管理有限公司'),('上海天焮餐饮管理有限公司','上海天焱餐饮管理有限公司'),('餐饮管理有限公司','餐饮管理有限公司')]:
            self.assertEqual(self.checker._seal_company_check(name,[seal])['status'],'pending')
        self.assertEqual(self.checker._seal_company_check('捷飨（上海）餐饮管理有限公司',['捷飨(上海)餐饮管理有限公司'])['status'],'pass')
    def test_detected_seal_without_identity_does_not_pass(self):
        result=self.checker._seal_check({'requirements':{'requires_seal':True}}, {'sections':[],'seal_texts':['上海其他有限公司'],'seal_locations':[]},None)
        self.assertTrue(result['detected']);self.assertEqual(result['status'],'pending')

class BlobTests(unittest.TestCase):
    def test_every_blob_family_is_immutable(self):
        store={};client=Mock();client.put_json_gz.side_effect=lambda key,value:store.update({key:copy.deepcopy(value)})
        with patch.object(blobs,'_client',return_value=client):
            for func,kwargs in [(blobs.save_document_content,{'identifier_id':'same'}),(blobs.save_document_review_content,{'identifier_id':'same'}),(blobs.save_tender_review_result,{'review_identifier_id':'same'})]:
                first=func({'old':True},**kwargs);second=func({'new':True},**kwargs)
                self.assertNotEqual(first,second);self.assertEqual(store[first],{'old':True})
    def test_blob_write_error_propagates(self):
        client=Mock();client.put_json_gz.side_effect=RuntimeError('write failed')
        with patch.object(blobs,'_client',return_value=client):
            with self.assertRaises(RuntimeError):blobs.save_document_review_content({},identifier_id='x')

class AsyncSafetyTests(unittest.IsolatedAsyncioTestCase):
    async def test_delete_endpoint_never_calls_storage(self):
        from app.router.file import delete_file
        oss=Mock()
        with self.assertRaises(HTTPException) as e:await delete_file('referenced-object',oss)
        self.assertEqual(e.exception.status_code,410);oss.delete_file.assert_not_called()
    async def test_result_read_does_not_block_event_loop(self):
        from app.router import postgresql as routes
        db=Mock();db.get_project_by_identifier.return_value={'identifier_id':'p'}
        def slow(_):time.sleep(.25);return {'result':{}}
        db.get_project_result.side_effect=slow
        async def probe():await asyncio.sleep(.02);return time.monotonic()
        start=time.monotonic();heartbeat=asyncio.create_task(probe())
        with patch.object(routes,'_invalidate_project_cache_or_error'):
            await routes.get_project_results('p',Response(),view='raw',include_raw_results=False,include_result_record=False,force_refresh=True,db_service=db,cache_service=Mock())
        self.assertLess((await heartbeat)-start,.15)
    async def test_shared_io_budget_bounds_concurrency(self):
        from app.core.io_dispatch import run_io
        import threading
        active=0;peak=0;lock=threading.Lock()
        def work():
            nonlocal active,peak
            with lock:active+=1;peak=max(peak,active)
            time.sleep(.025)
            with lock:active-=1
        await asyncio.gather(*(run_io(work) for _ in range(24)))
        self.assertLessEqual(peak,settings.XTJS_IO_CONCURRENCY)

class OpenApiTests(unittest.TestCase):
    def test_openapi_uses_examples_without_database_queries(self):
        from app import main
        from app.service.postgresql_service import PostgreSQLService
        with patch.object(main.app,'openapi_schema',None), patch.object(PostgreSQLService,'_get_connection',side_effect=AssertionError('must not query data')):
            schema=main.custom_openapi()
        self.assertTrue(schema['paths'])
        import json
        self.assertNotIn('位育',json.dumps(schema,ensure_ascii=False))
        fields=[]
        def walk(value):
            if isinstance(value,dict):
                if value.get('example')=='示例采购项目':fields.append(value)
                for v in value.values():walk(v)
            elif isinstance(value,list):
                for v in value:walk(v)
        walk(schema);self.assertTrue(fields)
        self.assertTrue(all('enum' not in field for field in fields))

class TokenTests(unittest.TestCase):
    def test_legacy_token_requires_version_zero_and_version_types_are_strict(self):
        from app.router.auth_dependencies import get_current_user
        from jose import jwt
        user={'identifier_id':'fixture','username':'u','role_level':1,'is_active':True,'token_version':0}
        service=Mock();service.get_session_record.return_value=user
        with patch.object(settings,'JWT_SECRET_KEY','fixture-key'):
            payload={'sub':'fixture','exp':int(time.time())+60}
            def request(p):return get_current_user(HTTPAuthorizationCredentials(scheme='Bearer',credentials=jwt.encode(p,settings.JWT_SECRET_KEY,algorithm=settings.JWT_ALGORITHM)),service)
            self.assertNotIn('token_version',request(payload))
            for version in ['0',False,-1,0.0]:
                with self.assertRaises(HTTPException):request({**payload,'token_version':version})
            user['token_version']=1
            with self.assertRaises(HTTPException):request(payload)
            self.assertTrue(request({**payload,'token_version':1}))
