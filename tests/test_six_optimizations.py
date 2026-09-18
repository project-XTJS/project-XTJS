import copy,json,tempfile,unittest
from pathlib import Path
from unittest.mock import Mock,patch
from app.service.analysis import bidder_identity
from app.service.analysis.verification import VerificationChecker
from app.service.analysis.verification_evidence import aggregate_status,component_message
from app.service.analysis.manual_review.business_bid_format import _verification_value_status
from app.service.analysis.compliance.underline_projection import project_text
from app.service.analysis.compliance.template_pdf_evidence import build_pdf_underline_evidence
from app.service.typo_runtime.contract import validate_edits,TypoUnavailable
from app.service.typo_runtime.manager import ModelManager
from app.service.analysis.typo_client import common_edits

class HomepageTests(unittest.TestCase):
 def test_only_four_colon_fields_and_page_one(self):
  c=VerificationChecker(None)
  for label in bidder_identity.ANCHORS:
   payload={'layout_sections':[{'page':1,'type':'text','text':label+'：捷飨（上海）餐饮管理有限公司（公章）联系人：李四'}]}
   before=copy.deepcopy(payload)
   self.assertEqual(c._bidder_identity(payload)['name'],'捷飨（上海）餐饮管理有限公司');self.assertEqual(payload,before)
  for label in ['供应商名称：','投标人名称：','投标人 ']:
   self.assertIsNone(c._bidder_identity({'layout_sections':[{'page':1,'text':label+'上海测试有限公司'}]})['name'])
  self.assertIsNone(c._bidder_identity({'bidder_name':'上海其他有限公司','layout_sections':[{'page':2,'text':'投标人：上海测试有限公司'}]})['name'])
 def test_role_conflicts_are_explicit(self):
  a=bidder_identity.identify({},[{'page':1,'text':'投标人：上海甲方有限公司'}]);b=bidder_identity.identify({},[{'page':1,'text':'投标人：上海乙方有限公司'}])
  self.assertEqual(bidder_identity.combine(('business',a),('technical',b))['status'],'pending')

class VerificationTests(unittest.TestCase):
 def test_pending_signature_cannot_be_overruled_by_passed_seal(self):
  v={'signature_status':'pending','seal_status':'pass','date_status':'pass'}
  self.assertEqual(_verification_value_status(v,'attachment_result'),'unclear')
  self.assertEqual(aggregate_status(['pending','pass','not_required']),'unclear')
 def test_description_does_not_change_component(self):
  msg=component_message('signature',{'status':'pending','presence':'detected','text_status':'unparsed'})
  self.assertIn('签字区域',msg);self.assertNotIn('盖章',msg)
 def test_no_signature_text_is_unknown_not_missing(self):
  c=VerificationChecker(None);req={'requirements':{'signature_field_count':2}}
  result=c._signature_check(req,{'text':'授权书','sections':[],'pages':[1]}, {'status':'pass'},{'status':'pass'})
  self.assertEqual(result['status'],'pending');self.assertEqual(result['required_count'],2);self.assertEqual(result['filled_count'],0)
 def test_unreadable_seal_separate_from_signature(self):
  c=VerificationChecker(None);r=c._seal_check({'requirements':{'requires_seal':True}}, {'text':'声明','sections':[],'pages':[1],'seal_locations':[{'page':1,'box':[1,2,3,4]}]},None)
  self.assertEqual(r['presence'],'detected');self.assertEqual(r['text_status'],'unparsed');self.assertEqual(r['status'],'pending')

 def test_pending_seal_summary_cannot_report_position_pass(self):
  from app.service.analysis.verification_evidence import refresh_verification_summary
  raw={'attachment_results':[{'title':'声明','requirements':{'requires_signature':True,'requires_seal':True,'requires_date':False},'signature_check':{'status':'pass'},'seal_check':{'status':'pending'},'date_check':{'status':'not_required'}}]}
  refresh_verification_summary(raw)
  self.assertEqual(raw['position_check']['status'],'pending')
  self.assertEqual(raw['position_check']['pending_seal_attachments'],['声明'])
  self.assertEqual(raw['evidence_counts']['position_pass_count'],0)

 def test_signature_detector_cannot_pair_distant_boxes_by_order(self):
  from app.service.ocr.ocr_signature_mixin import OCRSignatureMixin
  m=OCRSignatureMixin()
  m._bbox_to_xywh=lambda x:x
  m._boxes_are_close=lambda *a,**k:False
  m._bbox_distance=lambda *a:1000
  self.assertEqual(m._match_signatures_to_anchors([{'bbox':[1,1,10,10]}],[{'bbox':[600,700,10,10]}]),[])
  self.assertEqual(m._match_signatures_to_anchors([{}],[{}]),[])

class UnderlineTests(unittest.TestCase):
 def test_nested_markup_and_emphasis_removed_numbers_preserved(self):
  text=r'我方\underline{\text{愿承担全部责任}}，合同金额200000元，期限2026年9月18日（上海）。'
  r=project_text(text)
  self.assertEqual(r['status'],'ready');self.assertNotIn('承担',r['text']);self.assertIn('200000元',r['text']);self.assertIn('（上海）',r['text'])
 def test_ambiguous_physical_span_never_guess(self):
  e={'pages':{'1':{'status':'ready','spans':[{'text':'公司'}]}}}
  self.assertEqual(project_text('公司保证公司承诺',evidence=e,pages=[1],require_physical=True)['status'],'unclear')
  self.assertEqual(project_text('没有原件',pages=[1],require_physical=True)['status'],'unclear')
 def test_native_underline_but_not_table_border(self):
  import fitz
  pdf=fitz.open();p=pdf.new_page();p.insert_text((50,50),'Fixed Value');p.draw_line((78,52),(110,52))
  p.insert_text((50,105),'Table');p.draw_rect(fitz.Rect(45,85,130,108))
  e=build_pdf_underline_evidence(pdf.tobytes(),[1]);pdf.close()
  texts=''.join(v['text'] for v in e['pages']['1']['spans'])
  self.assertIn('Value',texts);self.assertNotIn('Table',texts)

 def test_scanned_line_requires_fine_ocr_and_keeps_auditable_box(self):
  import cv2, fitz, numpy as np
  canvas=np.full((260,500),255,np.uint8)
  cv2.putText(canvas,'VALUE',(120,145),cv2.FONT_HERSHEY_SIMPLEX,1,0,2)
  cv2.line(canvas,(110,155),(290,155),0,2)
  ok,encoded=cv2.imencode('.png',canvas);self.assertTrue(ok)
  pdf=fitz.open();page=pdf.new_page(width=250,height=130)
  page.insert_image(page.rect,stream=encoded.tobytes())
  calls=[]
  def fine(path):
   calls.append(path)
   return {'items':[{'text':'VALUE','bbox':[10,18,170,48]}]}
  evidence=build_pdf_underline_evidence(pdf.tobytes(),[1],local_ocr=fine);pdf.close()
  self.assertTrue(calls)
  self.assertEqual(evidence['pages']['1']['status'],'ready')
  self.assertTrue(all(span.get('coordinate_system')=='pdf_points' for span in evidence['pages']['1']['spans']))

 def test_same_page_other_attachment_underline_does_not_remove_text(self):
  e={'pages':{'1':{'status':'ready','spans':[{'text':'测试','bbox':[10,400,80,420]}]}}}
  r=project_text('测试公司保证',evidence=e,pages=[1],locations=[{'page':1,'bbox':[0,20,200,50]}],require_scope=True)
  self.assertEqual(r['text'],'测试公司保证');self.assertEqual(r['status'],'ready')
  self.assertEqual(project_text('测试公司保证',evidence=e,pages=[1],require_scope=True)['status'],'unclear')

class TypoTests(unittest.TestCase):
 def test_edit_source_mapping_and_protected_fields(self):
  t='定期开展安全培圳。投标人：上海测试有限公司'
  r=validate_edits(t,{'edits':[{'original':'安全培圳','replacement':'安全培训'},{'original':'测试','replacement':'测式'}]})
  self.assertEqual(len(r),1);self.assertEqual(t[r[0]['start']:r[0]['end']],'圳');self.assertEqual(r[0]['replacement'],'训')
 def test_whole_sentence_suggestion_projects_to_minimal_original_span(self):
  t='我们合理安排菜试搭配，确保营养均衡。';new=t.replace('菜试','菜式')
  r=validate_edits(t,{'edits':[{'original':t,'replacement':new}]});self.assertEqual(r[0]['original'],'试')
 def test_rewrite_and_unlocatable_output_are_not_empty_success(self):
  for old,new in [('不存在','内容'),('甲乙丙丁戊己庚辛','一二三四五六七八')]:
   with self.assertRaises(TypoUnavailable):validate_edits('甲乙丙丁戊己庚辛',{'edits':[{'original':old,'replacement':new}]})
 def test_common_errors_are_intersection_not_equal_lists(self):
  left='安全培圳。其他错。';right='前言。安全培圳。'
  a=validate_edits(left,{'edits':[{'original':'培圳','replacement':'培训'},{'original':'其他错','replacement':'其他措'}]});b=validate_edits(right,{'edits':[{'original':'培圳','replacement':'培训'}]})
  self.assertEqual(len(common_edits(left,right,a,b)),2)
 def test_idle_cache_and_busy_lifecycle(self):
  with tempfile.TemporaryDirectory() as d:
   now=[0];m=ModelManager([],worker_url='',model_id='fixture',cache_path=Path(d)/'c.db',clock=lambda:now[0])
   worker=Mock();worker.poll.return_value=None;worker.pid=999999;m.worker=worker;m.state='ready'
   with patch.object(m,'_start') as start,patch.object(m,'_request',return_value={'corrected_text':'正常文字'}),patch('os.killpg') as kill:
    m.check('正常文字');self.assertEqual(m.last_used,0)
    now[0]=1799;self.assertFalse(m.reap_idle());self.assertTrue(m.check('正常文字')['cache_hit']);self.assertEqual(m.last_used,0)
    now[0]=1800;m.pending=1;self.assertFalse(m.reap_idle());m.pending=0;self.assertTrue(m.reap_idle());self.assertEqual(kill.call_count,2);self.assertEqual(m.status()['state'],'unloaded')

 def test_unaccepted_model_is_explicitly_disabled_without_legacy_fallback(self):
  from app.service.analysis.duplicate_check.service import DuplicateCheckService
  from app.config.settings import settings
  service=DuplicateCheckService()
  with patch.object(settings,'TYPO_CHECK_ENABLED',False), patch.object(service,'_short_duplicate_typo_issues') as legacy:
   decision=service._short_duplicate_report_decision({'left_text':'菜试搭配','right_text':'菜试搭配'})
  legacy.assert_not_called()
  self.assertFalse(decision['report'])
  self.assertEqual(decision['reason'],'short_duplicate_without_typo')


class AccessRequestTests(unittest.TestCase):
 def test_intermediate_cannot_request_arbitrary_server_write_path(self):
  from fastapi import HTTPException
  from app.router.analysis import _parse_source_paths_json
  from app.service.resource_access import actor_context
  token=actor_context.set({'identifier_id':'middle','role_level':2})
  try:
   with self.assertRaises(HTTPException) as denied:
    _parse_source_paths_json('"/tmp/private.pdf"',1)
   self.assertEqual(denied.exception.status_code,403)
   self.assertEqual(_parse_source_paths_json(None,1),[None])
  finally:
   actor_context.reset(token)

 def test_actor_flows_to_bounded_worker_and_is_reset(self):
  from fastapi import FastAPI,Depends,Header
  import asyncio,httpx
  from app.router.auth_dependencies import bind_resource_actor,get_current_user
  from app.service.resource_access import actor_context,actor_id,restricted
  from app.core.io_dispatch import bounded_sync
  api=FastAPI(dependencies=[Depends(bind_resource_actor)])
  def user(x_user: str=Header()):return {'identifier_id':x_user,'role_level':2}
  api.dependency_overrides[get_current_user]=user
  @api.get('/probe')
  @bounded_sync
  def probe():return {'actor':actor_id(),'restricted':restricted()}
  async def exercise():
   async with httpx.AsyncClient(transport=httpx.ASGITransport(app=api),base_url='http://test') as client:
    self.assertEqual((await client.get('/probe',headers={'x-user':'alice'})).json(),{'actor':'alice','restricted':True})
    self.assertEqual((await client.get('/probe',headers={'x-user':'bob'})).json()['actor'],'bob')
    self.assertEqual((await client.get('/probe')).status_code,422)
   self.assertIsNone(actor_context.get())
  asyncio.run(exercise())
