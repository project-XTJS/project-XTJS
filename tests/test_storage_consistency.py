import asyncio
import copy
import io
import json
import time
import unittest
import urllib.error
import http.client
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock, MagicMock, patch

from app.service import document_blob_store as blobs
from app.service.minio_service import AlibabaEcsInstanceRoleProvider, MinioService
from app.service.postgresql_service import PostgreSQLService
from app.service.upload_manifest import make_upload_manifest, upload_summary


def credential_payload(**changes):
    return json.dumps({"Code": "Success", "AccessKeyId": "test-key", "AccessKeySecret": "test-secret",
        "SecurityToken": "test-token", "Expiration": (datetime.now(timezone.utc)+timedelta(hours=1)).isoformat(), **changes})


class CredentialTests(unittest.TestCase):
    def test_upload_cleanup_failure_keeps_original_error_without_secret_message(self):
        from minio.error import S3Error
        original = TimeoutError('private request headers')
        cleanup = S3Error(response=None, code='AccessDenied', message='denied',
                          resource='resource', request_id='request', host_id='host')
        cleanup.__context__ = original
        detail = MinioService._upload_error_detail(cleanup)
        self.assertTrue(detail.startswith('TimeoutError -> S3Error'))
        self.assertIn('AccessDenied', detail)
        self.assertNotIn('private request headers', detail)

    def test_concurrent_requests_fetch_only_one_credential_set(self):
        provider = AlibabaEcsInstanceRoleProvider()
        with patch.object(provider, '_metadata', side_effect=['role', credential_payload()]) as fetch:
            with ThreadPoolExecutor(max_workers=16) as pool:
                values = list(pool.map(lambda _: provider.retrieve(), range(64)))
        self.assertEqual(fetch.call_count, 2)
        self.assertTrue(all(value is values[0] for value in values))

    def test_service_instances_share_provider_but_static_credentials_still_work(self):
        from app.config.settings import settings
        with patch.object(settings, 'MINIO_ACCESS_KEY', ''), patch.object(settings, 'MINIO_SECRET_KEY', ''):
            self.assertIs(MinioService()._credential_provider, MinioService()._credential_provider)
        with patch.object(settings, 'MINIO_ACCESS_KEY', 'static'), patch.object(settings, 'MINIO_SECRET_KEY', 'secret'):
            self.assertEqual(MinioService()._credential_provider.retrieve().access_key, 'static')

    def test_failed_early_refresh_reuses_valid_credentials_and_cools_down(self):
        provider = AlibabaEcsInstanceRoleProvider()
        with patch.object(provider, '_metadata', side_effect=['role', credential_payload()]):
            first = provider.retrieve()
        provider._expires_at = time.time()+120
        provider._retry_after = 0
        with patch.object(provider, '_refresh', side_effect=TimeoutError) as refresh:
            self.assertIs(provider.retrieve(), first)
            self.assertIs(provider.retrieve(), first)
            self.assertEqual(refresh.call_count, 1)
        provider._expires_at = time.time()-1
        with self.assertRaises(RuntimeError): provider.retrieve()

    def test_incomplete_or_expired_credentials_are_rejected_without_leaking_secrets(self):
        for changes in ({'SecurityToken': ''}, {'Expiration': 'bad'}, {'Code': 'Failed'},
                        {'Expiration': (datetime.now(timezone.utc)-timedelta(minutes=1)).isoformat()}):
            provider = AlibabaEcsInstanceRoleProvider()
            with patch.object(provider, '_metadata', side_effect=['role', credential_payload(**changes)]):
                with self.assertRaises(RuntimeError) as error: provider.retrieve()
                self.assertNotIn('test-secret', str(error.exception))

    def test_transient_metadata_disconnect_is_retried_but_forbidden_is_not(self):
        provider = AlibabaEcsInstanceRoleProvider()
        response = MagicMock()
        response.__enter__.return_value.read.return_value = b'role'
        with patch.object(provider._opener, 'open', side_effect=[http.client.RemoteDisconnected(), response]) as opening, \
             patch('app.service.minio_service.time.sleep'):
            self.assertEqual(provider._fetch('http://metadata'), 'role')
            self.assertEqual(opening.call_count, 2)
        with patch.object(provider._opener, 'open', side_effect=urllib.error.HTTPError('url',403,'denied',{},None)) as opening:
            with self.assertRaises(urllib.error.HTTPError): provider._fetch('http://metadata')
            self.assertEqual(opening.call_count, 1)

    def test_imdsv2_token_is_cached(self):
        provider = AlibabaEcsInstanceRoleProvider()
        with patch.object(provider, '_fetch', side_effect=['token', 'role', credential_payload()]) as fetch:
            provider.retrieve()
            requests = [call.args[0] for call in fetch.call_args_list]
            self.assertEqual(requests[0].get_method(), 'PUT')
            self.assertEqual(requests[1].get_header('X-aliyun-ecs-metadata-token'), 'token')
            self.assertEqual(requests[2].get_header('X-aliyun-ecs-metadata-token'), 'token')

    def test_precreated_bucket_skips_probe(self):
        from app.config.settings import settings
        service = MinioService()
        with patch.object(settings, 'MINIO_BUCKET_PRECREATED', True), patch.object(service.client, 'bucket_exists') as probe:
            service.ensure_bucket()
            probe.assert_not_called()


class ResultSafetyTests(unittest.TestCase):
    def test_referenced_result_read_failure_does_not_become_empty(self):
        client = Mock()
        for failure in (RuntimeError('temporary'), None, [], 'broken'):
            client.get_json_gz.reset_mock(side_effect=True)
            if isinstance(failure, Exception): client.get_json_gz.side_effect=failure
            else: client.get_json_gz.return_value=failure
            with patch.object(blobs, '_client', return_value=client):
                with self.assertRaises(blobs.BlobReadError):
                    PostgreSQLService._sanitize_project_result_record({'result':None, 'result_object_key':'old'})

    def test_legacy_inline_result_and_missing_record_still_work(self):
        with patch.object(blobs, '_client') as client:
            self.assertEqual(blobs.get_result_payload({'result':{'old':{}}}), {'old':{}})
            self.assertIsNone(blobs.read_blob(None))
            client.assert_not_called()

    def test_ocr_read_error_propagates_instead_of_becoming_missing_text(self):
        client=Mock();client.get_json_gz.side_effect=RuntimeError('temporary')
        with patch.object(blobs,'_client',return_value=client):
            with self.assertRaises(blobs.BlobReadError):
                blobs.hydrate_document_content({'content':None,'content_object_key':'ocr'})

    def test_database_failure_cannot_replace_old_result_object(self):
        store={'old.json.gz':{'old':{'status':'pass'}}}
        client=Mock()
        client.put_json_gz.side_effect=lambda key,value:store.update({key:copy.deepcopy(value)})
        cursor=Mock();cursor.execute.side_effect=RuntimeError('database unavailable')
        with patch.object(blobs,'_client',return_value=client):
            with self.assertRaises(RuntimeError):
                PostgreSQLService()._persist_project_result(cursor,{'identifier_id':'pid','project_name':'project'}, {'new':{}})
        self.assertEqual(store['old.json.gz'],{'old':{'status':'pass'}})
        self.assertEqual(len(store),2)

    def test_every_result_write_gets_an_independent_object(self):
        with patch.object(blobs,'_client'):
            a=blobs.save_project_result({'a':1},project_name='same',project_identifier_id='p')
            b=blobs.save_project_result({'b':2},project_name='same',project_identifier_id='p')
        self.assertNotEqual(a,b)

    def test_failure_to_read_locked_result_prevents_writes(self):
        service=PostgreSQLService()
        connection=MagicMock();cursor=connection.cursor.return_value.__enter__.return_value
        cursor.fetchone.side_effect=[{'identifier_id':'p','project_name':'P'}, {'result':None,'result_object_key':'old'}]
        context=MagicMock();context.__enter__.return_value=connection
        with patch.object(service,'_get_connection',return_value=context), \
             patch.object(service,'_resolve_project_identifier',return_value='p'), \
             patch.object(blobs,'_read_required_json',side_effect=blobs.BlobReadError('read failed')), \
             patch.object(service,'_persist_project_result') as save:
            with self.assertRaises(blobs.BlobReadError): service.upsert_project_result_item('p','new',{})
            save.assert_not_called()
        self.assertIn('FOR UPDATE',cursor.execute.call_args_list[0].args[0])

    def test_ocr_save_failure_cannot_mark_extracted(self):
        service=PostgreSQLService();connection=MagicMock();context=MagicMock()
        context.__enter__.return_value=connection
        cursor=connection.cursor.return_value.__enter__.return_value
        with patch.object(service,'_get_connection',return_value=context), \
             patch.object(service,'_resolve_document_identifier',return_value='d'), \
             patch.object(blobs,'save_document_content',side_effect=RuntimeError('save failed')):
            with self.assertRaises(RuntimeError): service.update_document_content('d',{'text':'ocr'})
            self.assertFalse(any(str(call.args[0]).lstrip().upper().startswith("UPDATE") for call in cursor.execute.call_args_list))


class ProgressAndManifestTests(unittest.TestCase):
    def test_metadata_loading_does_not_fetch_ocr_or_result_objects(self):
        service=PostgreSQLService();connection=MagicMock();context=MagicMock()
        context.__enter__.return_value=connection
        cursor=connection.cursor.return_value.__enter__.return_value
        cursor.fetchall.return_value=[]
        cursor.fetchone.return_value={'workflow_scope':{},'result_keys':['check'],'result_summary':{}}
        with patch.object(service,'_get_connection',return_value=context), \
             patch.object(service,'get_project_by_identifier',return_value={'identifier_id':'p'}), \
             patch.object(blobs,'_client') as client:
            self.assertEqual(service.get_project_ocr_metadata('p')['result_keys'],['check'])
            client.assert_not_called()

    def test_incomplete_manifest_blocks_analysis_even_if_bound_ocr_is_complete(self):
        from app.router.postgresql import _ensure_project_analysis_status
        from fastapi import HTTPException
        manifest=make_upload_manifest(Mock(filename='tender.pdf'),[('A',Mock(filename='biz.pdf'),Mock(filename='tech.pdf'))])
        manifest['files'][0].update(status='uploaded',document_id='t')
        manifest['files'][1].update(status='uploaded',document_id='b')
        summary=upload_summary(manifest)
        self.assertFalse(summary['upload_complete'])
        self.assertEqual(len(summary['upload_issues']),1)
        with self.assertRaises(HTTPException) as err:
            _ensure_project_analysis_status({'parsing_status':3,**summary},required_status=2,analysis_name='test')
        self.assertEqual(err.exception.status_code,409)
        manifest['files'][2].update(status='uploaded',document_id='c')
        self.assertFalse(upload_summary(manifest)['upload_complete'])
        manifest['groups'][0]['bound']=True
        self.assertTrue(upload_summary(manifest)['upload_complete'])

    def test_legacy_project_without_manifest_remains_available(self):
        self.assertTrue(upload_summary(None)['upload_complete'])

    def test_ocr_status_does_not_block_the_event_loop(self):
        from app.router import postgresql_batch as batch
        from fastapi import Response
        project={'identifier_id':'p','parsing_status':0}
        payload={'project':project,'documents':[]}
        def slow_metadata(*args):
            time.sleep(0.2)
            return payload
        db=Mock();db.get_project_ocr_metadata.side_effect=slow_metadata
        db.refresh_project_parsing_status.return_value={**project, 'parsing_status': 1}
        async def run():
            with patch.object(batch,'_cache_get_or_set_payload',side_effect=lambda **kwargs:kwargs['factory']()), \
                 patch.object(batch.ocr_progress_publisher,'read_live',return_value=[]):
                task=asyncio.create_task(batch.get_project_ocr_status('p',Response(),db,Mock()))
                await asyncio.sleep(0.04)
                self.assertFalse(task.done())
                result = await task
                self.assertEqual(result['parsing_status'], 1)
        asyncio.run(run())


if __name__=='__main__':
    unittest.main()
