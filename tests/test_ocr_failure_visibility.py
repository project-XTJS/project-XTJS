import unittest
from unittest.mock import Mock, patch

from app.service.ocr_failure import describe_ocr_failure
from app.service.document_ingest_service import recognize_existing_document
from app.service.project_runtime import ProjectTaskCancelledError
from app.router.postgresql_batch import _ocr_stage_progress


class OcrFailureVisibilityTests(unittest.IsolatedAsyncioTestCase):
    def test_public_errors_are_classified_without_exposing_private_details(self):
        for text, code in [
            ('Failed to load document (PDFium: Data format error).', 'invalid_pdf'),
            ('AccessDenied https://private/key?token=secret', 'storage_permission'),
            ('read timed out https://private', 'timeout'),
            ('PDF password required secret=example', 'encrypted_pdf'),
            ('RuntimeError secret@example.invalid', 'recognition_failed'),
        ]:
            failure = describe_ocr_failure(text)
            self.assertEqual(failure['code'], code)
            self.assertNotIn('secret', failure['message'])
            self.assertNotIn('https:', failure['message'])
            self.assertTrue(failure['failed_at'])

    async def test_download_failure_is_persisted_and_returned(self):
        db, oss = Mock(), Mock()
        db.get_document_by_identifier.return_value = {'file_name': 'bad.pdf', 'file_url': 'minio://bucket/object', 'document_type': 'technical_bid'}
        oss.get_object_bytes.side_effect = RuntimeError('AccessDenied private credential')
        result = await recognize_existing_document(document_identifier='d', db_service=db,
            oss_service=oss, analysis_service=Mock(), raise_http_exception=False)
        self.assertFalse(result['ok'])
        self.assertEqual(db.record_document_ocr_failure.call_args.args[0], 'd')
        self.assertEqual(db.record_document_ocr_failure.call_args.args[1]['code'], 'storage_permission')
        db.update_document_content.assert_not_called()

    async def test_persistence_failure_does_not_replace_original_exception(self):
        db, oss = Mock(), Mock()
        db.get_document_by_identifier.return_value = {'file_name': 'bad.pdf', 'file_url': 'minio://bucket/object'}
        db.record_document_ocr_failure.side_effect = RuntimeError('database unavailable')
        oss.get_object_bytes.side_effect = RuntimeError('original OCR error')
        result = await recognize_existing_document(document_identifier='d', db_service=db,
            oss_service=oss, analysis_service=Mock(), raise_http_exception=False)
        self.assertFalse(result['ok'])
        self.assertIn('original OCR error', result['error'])

    async def test_deleted_project_cancellation_is_not_a_file_failure(self):
        db = Mock()
        with self.assertRaises(ProjectTaskCancelledError):
            await recognize_existing_document(document_identifier='d', db_service=db,
                oss_service=Mock(), analysis_service=Mock(), cancel_check=Mock(side_effect=ProjectTaskCancelledError('deleted')))
        db.record_document_ocr_failure.assert_not_called()

    def test_failures_stay_pending_for_retry_and_completed_files_hide_old_errors(self):
        failure = describe_ocr_failure('PDFium: Data format error')
        records = [dict(identifier_id='b', relation_role='business_bid', extracted=False,
            ocr_last_error=failure, tender_identifier_id='t', tender_extracted=False, tender_ocr_last_error=failure),
            dict(identifier_id='c', relation_role='technical_bid', extracted=True, ocr_last_error=failure)]
        stages = _ocr_stage_progress({'documents': records})
        self.assertEqual([stage['failed_count'] for stage in stages], [1, 1, 0])
        self.assertEqual(stages[1]['pending_documents'][0]['ocr_last_error'], failure)
        self.assertIsNone(stages[2]['completed_documents'][0].get('ocr_last_error'))
