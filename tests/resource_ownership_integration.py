"""Run ONLY against a disposable database: applies migrations to empty public schema."""
import copy,json
from pathlib import Path
from contextlib import contextmanager
from unittest.mock import patch
import psycopg2
from fastapi import HTTPException
from app.config.settings import settings
from app.service.postgresql_service import PostgreSQLService
from app.service.resource_access import actor_context
from app.service import document_blob_store as blobs
assert '127.0.0.1' in settings.DATABASE_URL, 'Disposable local database required'
c=psycopg2.connect(settings.DATABASE_URL);c.autocommit=True
with c.cursor() as cur:
 cur.execute('SELECT count(*) FROM information_schema.tables WHERE table_schema=\'public\'')
 assert cur.fetchone()[0]==0
 for path in sorted(Path('/app/db/migration').glob('V*.sql')):
  if 'resource_ownership' not in path.name:cur.execute(path.read_text())
 cur.execute("INSERT INTO xtjs_users(username,hashed_password,role_level) VALUES('old','test',2) RETURNING identifier_id");old= str(cur.fetchone()[0])
 cur.execute("INSERT INTO xtjs_projects(project_name) VALUES('historical') RETURNING identifier_id");hist=str(cur.fetchone()[0])
 cur.execute(Path('/app/db/migration/V20260917010000__resource_ownership.sql').read_text())
 users=[]
 for name,role in [('alice',2),('bob',2),('senior',3),('admin',4)]:
  cur.execute("INSERT INTO xtjs_users(username,hashed_password,role_level) VALUES(%s,'test',%s) RETURNING identifier_id",(name,role));users.append({'identifier_id':str(cur.fetchone()[0]),'role_level':role})
@contextmanager
def actor(user):
 token=actor_context.set(user)
 try:yield
 finally:actor_context.reset(token)
def denied(fn):
 try:fn()
 except HTTPException as e:assert e.status_code==403
 else:raise AssertionError('unauthorized operation allowed')
class Memory:
 values={}
 def put_json_gz(self,key,value):self.values[key]=copy.deepcopy(value)
 def get_json_gz(self,key):return copy.deepcopy(self.values.get(key))
s=PostgreSQLService();out=[]
with patch.object(blobs,'_client',return_value=Memory()), patch('app.service.minio_service.MinioService.get_presigned_url', return_value='https://fixture.invalid/file'):
 projects=[];docs=[]
 for i,u in enumerate(users[:2]):
  with actor(u):
   project=s.create_project('owner-'+str(i));pid=str(project['identifier_id']);projects.append(pid)
   assert str(project['owner_user_id'])==u['identifier_id']
   t=s.create_document('t.pdf','minio://fixture/t'+str(i),'tender');b=s.create_document('b.pdf','minio://fixture/b'+str(i),'business_bid')
   docs.append([str(t['identifier_id']),str(b['identifier_id'])]);s.bind_project_documents(pid,*docs[-1])
   s.update_document_content(str(b['identifier_id']), {'layout_sections':[{'page':1,'type':'text','text':'投标人：上海测试科技有限公司（公章）'}]})
   assert s.get_project_detail(pid)['relations'][0]['bidder_identity']['name']=='上海测试科技有限公司'
   assert s.get_document_by_identifier(str(b['identifier_id']))['bidder_identity']['name']=='上海测试科技有限公司'
   s.update_project(pid,project_name='renamed-'+str(i))
   s.upsert_project_result_item(pid,'test',{'ok':True})
 with actor(users[0]):
  assert s.list_projects()['total']==1
  assert s.list_documents()['total']==2
  assert s.list_relations()['total']==1
  assert s.list_project_results()['total']==1
  assert s.get_project_documents_for_duplicate_check(projects[0])
  assert s.get_project_by_name('renamed-1') is None
  for fn in [lambda:s.get_project_detail(projects[1]),lambda:s.get_project_result(projects[1]),lambda:s.get_document_by_identifier(docs[1][1]),lambda:s.bind_project_documents(projects[0],docs[0][0],docs[1][1]),lambda:s.soft_delete_projects(projects)]:denied(fn)
  assert s.get_project_detail(projects[0])
  denied(lambda:s.get_project_by_identifier(hist))
 out.append('intermediate_lists_details_results_documents_relations_and_atomic_batches_isolated')
 for u in users[2:]:
  with actor(u):assert s.list_projects()['total']==3
 out.append('admin_senior_all_projects')
 with actor({'identifier_id':old,'role_level':2}):assert s.get_project_by_identifier(hist)
 out.append('historical_grant_frozen_new_users_excluded')
 with actor(users[3]):s.bind_project_documents(projects[1],docs[0][0],docs[0][1])
 with actor(users[0]):
  assert s.get_document_by_identifier(docs[0][1])
  denied(lambda:s.soft_delete_document(docs[0][1]))
  denied(lambda:s.update_document_review_content(docs[0][1],effective_content={'changed':True}))
 out.append('shared_document_write_requires_all_projects')
print(json.dumps({'passed':out},ensure_ascii=False))
