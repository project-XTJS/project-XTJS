import io
import unittest
from unittest.mock import Mock, AsyncMock, patch
from fastapi import HTTPException, UploadFile
from app.router import postgresql as router


class ReplacementRouteTests(unittest.IsolatedAsyncioTestCase):
    async def invoke(self, upload, role='technical_bid', live=None):
        db = Mock()
        db.get_project_ocr_metadata.return_value = {
            'project': {'identifier_id': 'p', 'input_revision': 4},
            'documents': [{'identifier_id': 'old', 'relation_role': 'technical_bid', 'tender_identifier_id': 't'}]}
        cache = Mock()
        self.db = db
        with patch.object(router, 'upload_extract_and_create_document', upload), \
             patch('app.service.ocr_progress_publisher.read_live', return_value=live or []), \
             patch.object(router, '_invalidate_project_cache_or_error'):
            return await router.replace_project_document('p', 'old', role,
                UploadFile(filename='new.pdf', file=io.BytesIO(b'pdf')), db, cache, Mock(), Mock())

    async def test_identification_failure_keeps_original_relation(self):
        upload = AsyncMock(side_effect=HTTPException(400, 'PDF格式错误'))
        with self.assertRaises(HTTPException):
            await self.invoke(upload)
        self.db.replace_project_document.assert_not_called()

    async def test_success_passes_captured_revision_and_new_document(self):
        await self.invoke(AsyncMock(return_value={'document': {'identifier_id': 'new'}}))
        self.db.replace_project_document.assert_called_once_with('p', 'old', 'new', 'technical_bid', 4)

    async def test_wrong_role_or_active_ocr_never_uploads(self):
        for role, live in [('business_bid', None), ('technical_bid', [{'active': True}])]:
            upload = AsyncMock()
            with self.assertRaises(HTTPException) as caught:
                await self.invoke(upload, role, live)
            self.assertEqual(caught.exception.status_code, 409)
            upload.assert_not_called()

    async def test_conflict_does_not_retry_or_replace_original(self):
        from app.core.consistency import ConsistencyConflict
        upload = AsyncMock(return_value={'document': {'identifier_id': 'new'}})
        with patch.object(router.PostgreSQLService, 'replace_project_document', side_effect=ConsistencyConflict()):
            # Transaction conflict is propagated without an unconditional fallback.
            with patch.object(router, '_invalidate_project_cache_or_error'):
                db = Mock()
                db.get_project_ocr_metadata.return_value = {'project': {'identifier_id': 'p', 'input_revision': 1},
                    'documents': [{'identifier_id': 'old', 'relation_role': 'technical_bid'}]}
                db.replace_project_document.side_effect = ConsistencyConflict()
                with patch.object(router, 'upload_extract_and_create_document', upload), patch('app.service.ocr_progress_publisher.read_live', return_value=[]):
                    with self.assertRaises(ConsistencyConflict):
                        await router.replace_project_document('p', 'old', 'technical_bid', UploadFile(filename='a.pdf', file=io.BytesIO(b'')), db, Mock(), Mock(), Mock())
                self.assertEqual(db.replace_project_document.call_count, 1)
