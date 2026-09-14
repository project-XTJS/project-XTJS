"""Idempotent upload leases and atomic completion of expected bidder groups."""
from datetime import datetime, timedelta, timezone
from uuid import uuid4
from psycopg2.extras import Json, RealDictCursor
from app.core.consistency import ConsistencyConflict

LEASE_SECONDS = 900

def utcnow():
    return datetime.now(timezone.utc)

def lease_live(entry):
    try:
        return datetime.fromisoformat(entry.get('lease_expires_at') or '') > utcnow()
    except (ValueError, TypeError):
        return False

class UploadRecoveryMixin:
    def _locked_manifest(self, cursor, pid):
        cursor.execute('SELECT upload_manifest FROM xtjs_projects WHERE identifier_id=%s AND deleted=FALSE FOR UPDATE',(pid,))
        row=cursor.fetchone()
        if not row or not row['upload_manifest']:
            raise ConsistencyConflict('项目不存在或没有上传清单')
        return row['upload_manifest']

    @staticmethod
    def _upload_entry(manifest,slot):
        entry=next((f for f in manifest.get('files',[]) if f.get('slot')==slot),None)
        if entry is None: raise ConsistencyConflict('文件不在项目上传清单内')
        return entry

    @staticmethod
    def _save_manifest(cursor,pid,manifest):
        cursor.execute('UPDATE xtjs_projects SET upload_manifest=%s,update_time=CURRENT_TIMESTAMP WHERE identifier_id=%s',(Json(manifest),pid))

    def claim_upload(self,pid,slot,attempt_id):
        with self._get_connection() as conn,conn.cursor(cursor_factory=RealDictCursor) as cursor:
            manifest=self._locked_manifest(cursor,pid);entry=self._upload_entry(manifest,slot)
            if entry.get('status')=='uploaded':
                if entry.get('attempt_id')==attempt_id:return dict(entry)
                raise ConsistencyConflict('该文件已上传，请刷新项目')
            if entry.get('status')=='uploading' and lease_live(entry):
                raise ConsistencyConflict('该文件正在补传，请稍后刷新')
            entry.update(attempt_id=attempt_id,lease_id=uuid4().hex,status='uploading',lease_expires_at=(utcnow()+timedelta(seconds=LEASE_SECONDS)).isoformat(),error=None)
            self._save_manifest(cursor,pid,manifest)
            return dict(entry)

    def renew_upload(self,pid,slot,lease_id):
        with self._get_connection() as conn,conn.cursor(cursor_factory=RealDictCursor) as cursor:
            manifest=self._locked_manifest(cursor,pid);entry=self._upload_entry(manifest,slot)
            if entry.get('lease_id')!=lease_id or entry.get('status')!='uploading' or not lease_live(entry):
                raise ConsistencyConflict('上传租约已失效，请刷新项目后重试')
            entry['lease_expires_at']=(utcnow()+timedelta(seconds=LEASE_SECONDS)).isoformat()
            self._save_manifest(cursor,pid,manifest)

    def _bind_manifest_groups(self,cursor,pid,manifest):
        files={f['slot']:f for f in manifest['files']}
        for group in manifest['groups']:
            entries=[files.get(key,{}) for key in ['tender',group['business_bid'],group['technical_bid']]]
            if any(f.get('status')!='uploaded' or not f.get('document_id') for f in entries):continue
            ids=[f['document_id'] for f in entries]
            cursor.execute('SELECT identifier_id,document_type FROM xtjs_documents WHERE identifier_id=ANY(%s::uuid[]) AND deleted=FALSE',(ids,))
            valid={str(d['identifier_id']):d['document_type'] for d in cursor.fetchall()}
            if any(valid.get(str(did))!=role for did,role in zip(ids,['tender','business_bid','technical_bid'])):
                raise ConsistencyConflict('待关联文件不存在或类型不符，请刷新项目')
            slot=group.get('slot') or group['business_bid']
            cursor.execute('SELECT id,upload_group_slot FROM xtjs_project_documents WHERE project_id=%s AND tender_document_id=%s AND business_bid_document_id=%s AND technical_bid_document_id=%s',(pid,*ids))
            matching=cursor.fetchall()
            if len(matching)>1:raise ConsistencyConflict('已有重复关联，请人工核查，系统未自动删除')
            if matching and matching[0]['upload_group_slot'] is None:
                cursor.execute('UPDATE xtjs_project_documents SET upload_group_slot=%s WHERE id=%s',(slot,matching[0]['id']))
            cursor.execute('''INSERT INTO xtjs_project_documents(project_id,tender_document_id,business_bid_document_id,technical_bid_document_id,upload_group_slot)
                VALUES(%s,%s,%s,%s,%s) ON CONFLICT(project_id,upload_group_slot) WHERE upload_group_slot IS NOT NULL
                DO UPDATE SET tender_document_id=EXCLUDED.tender_document_id,business_bid_document_id=EXCLUDED.business_bid_document_id,technical_bid_document_id=EXCLUDED.technical_bid_document_id''',(pid,*ids,slot))
        cursor.execute('SELECT xtjs_sync_materials(%s::uuid)',(pid,))

    def finish_upload(self,pid,slot,lease_id,*,document_id=None,error=None):
        with self._get_connection() as conn,conn.cursor(cursor_factory=RealDictCursor) as cursor:
            manifest=self._locked_manifest(cursor,pid);entry=self._upload_entry(manifest,slot)
            if entry.get('lease_id')==lease_id and entry.get('status')=='uploaded':return dict(entry)
            if entry.get('lease_id')!=lease_id or entry.get('status')!='uploading' or not lease_live(entry):
                raise ConsistencyConflict('上传租约已失效，旧请求不能覆盖新文件')
            entry.update(status='uploaded' if document_id else 'failed',document_id=str(document_id) if document_id else None,error=error,lease_expires_at=None)
            self._save_manifest(cursor,pid,manifest)
            self._bind_manifest_groups(cursor,pid,manifest)
            return dict(entry)

    def bind_uploaded_groups(self,pid):
        with self._get_connection() as conn,conn.cursor(cursor_factory=RealDictCursor) as cursor:
            manifest=self._locked_manifest(cursor,pid)
            self._bind_manifest_groups(cursor,pid,manifest)
