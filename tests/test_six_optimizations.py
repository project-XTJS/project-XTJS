import copy,json,tempfile,unittest
from pathlib import Path
from unittest.mock import Mock,patch
from app.service.analysis import bidder_identity
from app.service.analysis.verification import VerificationChecker
from app.service.analysis.verification_evidence import aggregate_status,component_message
from app.service.analysis.manual_review.business_bid_format import _verification_value_status
from app.service.analysis.compliance.underline_projection import project_text
from app.service.analysis.compliance.template_pdf_evidence import build_pdf_underline_evidence
from app.service.typo_runtime.contract import classify_candidates,classify_dual_candidates,validate_candidates,validate_edits,TypoUnavailable
from app.service.typo_runtime.manager import ModelManager
from app.service.analysis.typo_client import DuplicateTypoService,common_edits,common_word_edits
from app.service.analysis.duplicate_merge.merger import DuplicateResultMerger

class HomepageTests(unittest.TestCase):
 def test_explicit_homepage_company_fields(self):
  c=VerificationChecker(None)
  for label in bidder_identity.ANCHORS:
   payload={'layout_sections':[{'page':1,'type':'text','text':label+'：捷飨（上海）餐饮管理有限公司（公章）联系人：李四'}]}
   before=copy.deepcopy(payload)
   self.assertEqual(c._bidder_identity(payload)['name'],'捷飨（上海）餐饮管理有限公司');self.assertEqual(payload,before)
  payload={'layout_sections':[{'page':1,'type':'text','text':'供应商（加盖公章）上海测试有限公司 二〇二六年九月十四日','bbox':[1,2,3,4]}]}
  self.assertEqual(c._bidder_identity(payload)['name'],'上海测试有限公司')
  for label in ['供应商 ','投标人名称：','投标人 ']:
   self.assertIsNone(c._bidder_identity({'layout_sections':[{'page':1,'text':label+'上海测试有限公司'}]})['name'])
  self.assertIsNone(c._bidder_identity({'bidder_name':'上海其他有限公司','layout_sections':[{'page':2,'text':'投标人：上海测试有限公司'}]})['name'])
 def test_role_conflicts_are_explicit(self):
  a=bidder_identity.identify({},[{'page':1,'text':'投标人：上海甲方有限公司'}]);b=bidder_identity.identify({},[{'page':1,'text':'投标人：上海乙方有限公司'}])
  self.assertEqual(bidder_identity.combine(('business',a),('technical',b))['status'],'pending')

 def test_repeated_explicit_document_fields_recover_missing_cover_identity(self):
  payload={'layout_sections':[
   {'page':1,'type':'heading','text':'目录','bbox':[1,1,20,10]},
   {'page':13,'type':'text','text':'供应商（加盖公章）：上海闽香贸易有限公司','bbox':[50,326,312,342]},
   {'page':40,'type':'text','text':'供应商（加盖公章）：上海闽香贸易有限公司','bbox':[70,505,315,523]},
   {'page':47,'type':'text','text':'磋商响应单位（加盖公章）：上海国香贸易有限公司','bbox':[38,292,340,310]},
   {'page':47,'type':'seal','text':'上海闽香贸易有限公司','bbox':[200,300,100,100]},
  ]}
  identity=VerificationChecker(None)._bidder_identity(payload)
  self.assertEqual(identity['name'],'上海闽香贸易有限公司')
  self.assertEqual(identity['reason'],'repeated_explicit_document_fields')
  self.assertEqual({item['page'] for item in identity['candidates']},{13,40})
  self.assertEqual([item['page'] for item in identity['ignored_candidates']],[47])

 def test_document_identity_fallback_requires_two_pages_and_no_conflict(self):
  one={'layout_sections':[
   {'page':40,'type':'text','text':'供应商（加盖公章）：上海闽香贸易有限公司','bbox':[1,2,3,4]},
   {'page':41,'type':'seal','text':'上海闽香贸易有限公司','bbox':[5,6,7,8]},
  ]}
  self.assertIsNone(VerificationChecker(None)._bidder_identity(one)['name'])
  conflict={'layout_sections':[
   {'page':10,'type':'text','text':'供应商（公章）：上海甲方贸易有限公司','bbox':[1,2,3,4]},
   {'page':11,'type':'text','text':'供应商（公章）：上海甲方贸易有限公司','bbox':[1,2,3,4]},
   {'page':20,'type':'text','text':'供应商（加盖公章）：上海乙方贸易有限公司','bbox':[1,2,3,4]},
   {'page':21,'type':'text','text':'供应商（加盖公章）：上海乙方贸易有限公司','bbox':[1,2,3,4]},
  ]}
  identity=VerificationChecker(None)._bidder_identity(conflict)
  self.assertIsNone(identity['name']);self.assertEqual(identity['reason'],'conflicting_bidder_fields')

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
 def test_partial_handwritten_mark_in_located_signature_field_counts_as_signed(self):
  c=VerificationChecker(None);req={'requirements':{'signature_field_count':1}}
  section={'page':40,'type':'text','text':'法定代表人或授权委托人（签字或盖章）：张','bbox':[70,529,431,552]}
  result=c._signature_check(req,{'text':section['text'],'sections':[section],'pages':[40]}, {'status':'pass'},{'status':'pass'})
  self.assertEqual(result['status'],'pass');self.assertEqual(result['presence'],'detected')
  self.assertEqual(result['filled_values'][0]['value'],'张')
  self.assertEqual(result['filled_values'][0]['mode'],'ocr_inline_text')
  self.assertEqual(result['filled_values'][0]['recognition_status'],'partial')
  self.assertEqual(result['filled_values'][0]['signature_box'],[70,529,361,23])
  blank={**section,'text':'法定代表人或授权委托人（签字或盖章）：___'}
  self.assertEqual(c._signature_check(req,{'text':blank['text'],'sections':[blank],'pages':[40]}, {'status':'pass'},{'status':'pass'})['status'],'pending')
  no_box={**section};no_box.pop('bbox')
  self.assertEqual(c._signature_check(req,{'text':no_box['text'],'sections':[no_box],'pages':[40]}, {'status':'pass'},{'status':'pass'})['status'],'pending')
 def test_unreadable_seal_separate_from_signature(self):
  c=VerificationChecker(None);r=c._seal_check({'requirements':{'requires_seal':True}}, {'text':'声明','sections':[],'pages':[1],'seal_locations':[{'page':1,'box':[1,2,3,4]}]},None)
  self.assertEqual(r['presence'],'detected');self.assertEqual(r['text_status'],'unparsed');self.assertEqual(r['status'],'pending')

 def test_pending_seal_summary_cannot_report_position_pass(self):
  from app.service.analysis.verification_evidence import refresh_verification_summary
  raw={'attachment_results':[{'title':'声明','requirements':{'requires_signature':True,'requires_seal':True,'requires_date':False},'signature_check':{'status':'pass'},'seal_check':{'status':'pending'},'date_check':{'status':'not_required'}}]}
  refresh_verification_summary(raw)
  self.assertEqual(raw['position_check']['status'],'unclear')
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
 def test_underlined_fixed_emphasis_is_retained(self):
  text=r'我方\underline{\text{愿承担全部责任}}，合同金额200000元，期限2026年9月18日（上海）。'
  r=project_text(text)
  self.assertEqual(r['status'],'ready');self.assertIn('承担',r['text']);self.assertIn('200000元',r['text']);self.assertIn('（上海）',r['text'])
 def test_ordinary_underlined_text_is_fixed_and_missing_evidence_is_unclear(self):
  e={'pages':{'1':{'status':'ready','spans':[{'text':'公司'}]}}}
  projected=project_text('公司保证公司承诺',evidence=e,pages=[1],require_physical=True)
  self.assertEqual(projected['status'],'ready');self.assertEqual(projected['text'],'公司保证公司承诺')
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
 def _v5_manager(self,d,**kwargs):
  with patch('app.service.typo_runtime.manager.ShapeSoundGate') as gate:
   gate.return_value.evidence.return_value={'similarity_type':'pinyin','glyph_similarity':0.5}
   return ModelManager([],worker_url='',model_id='fixture',cec3_model_id='cec3',detector_model_id='detector',
                       font_path='/unused-font',font_sha256='font',pinyin_version='0.55.0',
                       cache_path=Path(d)/'c.db',**kwargs)
 def _candidate(self,text,original,replacement,probability=.99,ratio=100):
  start=text.index(original);return {'candidates':[{'start':start,'end':start+1,'original':original,'replacement':replacement,'candidate_probability':probability,'source_probability':probability/ratio,'probability_ratio':ratio}]}
 def test_word_rules_confirm_only_approved_high_confidence_edits(self):
  text='定期开展安全培圳，确保知识图谱兼容性。'
  confirmed,review=classify_candidates(text,validate_candidates(text,self._candidate(text,'圳','训')))
  self.assertEqual((confirmed[0]['original_word'],confirmed[0]['replacement_word']),('培圳','培训'));self.assertEqual(review,[])
  protected,protected_review=classify_candidates(text,validate_candidates(text,self._candidate(text,'性','新')))
  self.assertEqual((protected,protected_review),([],[]))
 def test_unknown_or_low_confidence_word_is_review_only(self):
  text='我们合理安排菜试搭配。'
  confirmed,review=classify_candidates(text,validate_candidates(text,self._candidate(text,'试','式')))
  self.assertEqual(confirmed,[]);self.assertEqual(review[0]['review_reason'],'unverified_word')
  text='定期开展安全培圳。'
  confirmed,review=classify_candidates(text,validate_candidates(text,self._candidate(text,'圳','训',.8,15)))
  self.assertEqual(confirmed,[]);self.assertEqual(review[0]['review_reason'],'below_auto_accept_threshold')
 def test_shared_word_edit_is_one_issue_with_two_precise_occurrences(self):
  left='安全培圳。';right='说明：安全培圳。'
  def issues(text,side):
   confirmed,_=classify_candidates(text,validate_candidates(text,self._candidate(text,'圳','训')))
   item=confirmed[0];item.update(side=side,source_evidence_id='evidence',page=1)
   return [item]
  shared=common_word_edits(left,right,issues(left,'left'),issues(right,'right'))
  self.assertEqual(len(shared),1);self.assertEqual(len(shared[0]['occurrences']),2);self.assertEqual(shared[0]['verification_status'],'confirmed')
 def test_cross_page_segment_maps_candidate_to_its_actual_page(self):
  text='第一句。 安全培圳。';start=text.index('圳');word_start=text.index('培圳')
  item=DuplicateTypoService._project_issue(
   {'start':start-word_start,'end':start-word_start+1,'word_start':0,'word_end':2,'original':'圳','original_word':'培圳'},
   offset=word_start,source_text=text,
   snippet={'page':1,'source_location_reliable':True,'segments':[{'start':0,'end':4,'page':1},{'start':5,'end':10,'page':2}]},
   payload={},
  )
  self.assertEqual(item['page'],2);self.assertTrue(item['source_location_reliable'])
  self.assertEqual((item['start'],item['end']),(start,start+1))
 def test_cluster_uses_occurrence_candidates_and_merges_three_document_pairs(self):
  def occurrence(file_name,side,evidence_id):
   return {'side':side,'file_name':file_name,'page':1,'source_kind':'block','source_evidence_id':evidence_id,'source_text_length':5,'start':3,'end':4,'word_start':2,'word_end':4}
  a1=occurrence('A.pdf','left','ab');b=occurrence('B.pdf','right','ab');a2=occurrence('A.pdf','left','ac');c=occurrence('C.pdf','right','ac')
  def issue(shared,values):
   return {'shared_id':shared,'original_word':'培圳','replacement_word':'培训','original':'圳','replacement':'训','matched_text':'培圳','suggestion':'培训','verification_status':'confirmed','occurrences':values}
  cluster={'occurrences':[
   {'evidence':{'short_duplicate_typo_issues':[issue('ab',[a1,b])]}},
   {'evidence':{'short_duplicate_typo_issues':[issue('ac',[a2,c])]}},
   {'evidence':{'typo_evidence_id':'unrelated'}},
  ]}
  merged=DuplicateResultMerger._cluster_typo_issues(cluster)
  self.assertEqual(len(merged),1);self.assertEqual({item['file_name'] for item in merged[0]['occurrences']},{'A.pdf','B.pdf','C.pdf'})
  second_a={**a1,'start':1,'end':2,'word_start':0,'word_end':2}
  second_b={**b,'start':1,'end':2,'word_start':0,'word_end':2}
  distinct={'occurrences':[
   {'evidence':{'short_duplicate_typo_issues':[issue('first',[a1,b])]}},
   {'evidence':{'short_duplicate_typo_issues':[issue('second',[second_a,second_b])]}},
  ]}
  self.assertEqual(len(DuplicateResultMerger._cluster_typo_issues(distinct)),2)
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
   now=[0];m=self._v5_manager(d,clock=lambda:now[0])
   worker=Mock();worker.poll.return_value=None;worker.pid=999999;m.worker=worker;m.state='ready'
   with patch.object(m,'_start'),patch.object(m,'_request',return_value={'scores':[0.1]*4}),patch('os.killpg') as kill:
    m.check('正常文字');self.assertEqual(m.last_used,0)
    now[0]=1799;self.assertFalse(m.reap_idle());self.assertTrue(m.check('正常文字')['cache_hit']);self.assertEqual(m.last_used,0)
    now[0]=1800;m.pending=1;self.assertFalse(m.reap_idle());m.pending=0;self.assertTrue(m.reap_idle());self.assertEqual(kill.call_count,2);self.assertEqual(m.status()['state'],'unloaded')
 def test_manager_returns_separate_buckets_and_runtime_metrics(self):
  with tempfile.TemporaryDirectory() as d:
   m=self._v5_manager(d)
   payload={'corrected_text':'安全培训','candidates':[{'start':3,'end':4,'original':'圳','replacement':'训','candidate_probability':.99,'source_probability':.001,'probability_ratio':990}]}
   def reference(path,*args,**kwargs):
    return {'scores':[0.1,0.1,0.1,0.99]} if path=='/detect' else payload if path=='/correct' else {'corrected_text':'安全培训'}
   with patch.object(m,'_start'),patch.object(m,'_request',side_effect=reference):
    result=m.check('安全培圳')
   self.assertEqual(result['confirmed_count'],1);self.assertEqual(result['review_candidate_count'],0)
   metrics=m.status()['metrics'];self.assertEqual(metrics['checks'],1);self.assertEqual(metrics['completed'],1);self.assertEqual(metrics['confirmed_results'],1)

 def test_unaccepted_model_is_explicitly_disabled_without_legacy_fallback(self):
  from app.service.analysis.duplicate_check.service import DuplicateCheckService
  from app.config.settings import settings
  service=DuplicateCheckService()
  with patch.object(settings,'TYPO_CHECK_ENABLED',False), patch.object(service,'_short_duplicate_typo_issues') as legacy:
   decision=service._short_duplicate_report_decision({'left_text':'菜试搭配','right_text':'菜试搭配'})
  legacy.assert_not_called()
  self.assertFalse(decision['report'])
  self.assertEqual(decision['reason'],'short_duplicate_without_typo')
 def test_review_candidate_keeps_short_duplicate_unclear_without_confirming_it(self):
  from app.service.analysis.duplicate_check.service import DuplicateCheckService
  from app.config.settings import settings
  service=DuplicateCheckService();candidate={'shared_id':'review-1','verification_status':'review'}
  with patch.object(settings,'TYPO_CHECK_ENABLED',True),patch.object(service,'_short_duplicate_typo_results',return_value=([],[candidate],{'hidden_count':1})):
   decision=service._short_duplicate_report_decision({'left_text':'菜试搭配','right_text':'菜试搭配'},evidence_key='duplicate_blocks')
  self.assertFalse(decision['report']);self.assertNotIn('review_candidates',decision)
 def test_typo_failure_does_not_suppress_long_duplicate_rule(self):
  from app.service.analysis.duplicate_check.service import DuplicateCheckService
  from app.config.settings import settings
  service=DuplicateCheckService()
  text='这是一段超过三十个汉字的重复正文，用于确认错别字模型失败不会覆盖原有的长文本查重结论。'
  with patch.object(settings,'TYPO_CHECK_ENABLED',True),patch.object(service,'_short_duplicate_typo_results',side_effect=TypoUnavailable('offline')):
   decision=service._short_duplicate_report_decision({'text':text},source_issue={})
  self.assertTrue(decision['report']);self.assertFalse(decision.get('review_only',False))
  self.assertEqual(decision['reason'],'duplicate_text_at_least_30_chars')
  self.assertEqual(decision['typo_check']['status'],'incomplete')
 def test_confirmed_short_typo_is_a_report_basis_but_review_candidate_is_not(self):
  from app.service.analysis.duplicate_check.service import DuplicateCheckService
  from app.config.settings import settings
  service=DuplicateCheckService();confirmed={'shared_id':'confirmed','verification_status':'confirmed'}
  base={'risk_level':'none','duplicate_blocks':[{'left_text':'安全培圳','right_text':'安全培圳'}]}
  with patch.object(settings,'TYPO_CHECK_ENABLED',True),patch.object(service,'_short_duplicate_typo_results',return_value=([confirmed],[],{})):
   issue=service._filter_short_duplicate_evidence(copy.deepcopy(base))
  self.assertEqual(issue['risk_level'],'low');self.assertTrue(issue['suspicious']);self.assertFalse(issue['review_only'])
  candidate={'shared_id':'review','verification_status':'review'}
  with patch.object(settings,'TYPO_CHECK_ENABLED',True),patch.object(service,'_short_duplicate_typo_results',return_value=([],[candidate],{'hidden_count':1})):
   issue=service._filter_short_duplicate_evidence(copy.deepcopy(base))
  self.assertEqual(issue['risk_level'],'none');self.assertFalse(issue['suspicious']);self.assertFalse(issue.get('review_only',False))

 def test_cec3_agreement_rejects_invalid_words_semantic_choices_and_extra_edits(self):
  for text,original,replacement,target in [
   ('链路畅通','路','络','链络畅通'),
   ('权利明确','利','力','权力明确'),
   ('数据链路畅通','路','络','数据链络畅通'),
  ]:
   candidates=validate_candidates(text,self._candidate(text,original,replacement))
   accepted,counts=classify_dual_candidates(text,candidates,target,target)
   self.assertEqual(accepted,[],text);self.assertEqual(counts['hidden_count'],1)
  text='安全培圳。'
  candidates=validate_candidates(text,self._candidate(text,'圳','训'))
  accepted,counts=classify_dual_candidates(text,candidates,'安全培训。','安全培训。')
  self.assertEqual([(x['original_word'],x['replacement_word']) for x in accepted],[('培圳','培训')])
  self.assertEqual(counts['hidden_count'],0)
  for corrected,roundtrip in [('安全培训！','安全培训！'),('安全培训。解释','安全培训。解释'),('安全培训。','安全培圳。'),('安全培训。','安全培训！')]:
   accepted,_=classify_dual_candidates(text,candidates,corrected,roundtrip)
   self.assertEqual(accepted,[])
 def test_cec3_rejects_prompt_echo_as_incomplete(self):
  with tempfile.TemporaryDirectory() as d:
   m=self._v5_manager(d)
   payload={'candidates':[{'start':3,'end':4,'original':'圳','replacement':'训','candidate_probability':.99,'source_probability':.001,'probability_ratio':990}]}
   def response(path,*args,**kwargs):
    return {'scores':[0.1,0.1,0.1,0.99]} if path=='/detect' else payload if path=='/correct' else {'corrected_text':'纠正后的文本：安全培训'}
   with patch.object(m,'_start'),patch.object(m,'_request',side_effect=response):
    with self.assertRaises(TypoUnavailable):m.check('安全培圳')
   self.assertEqual(m.status()['metrics']['incomplete'],1)
 def test_repeated_word_and_non_bmp_codepoint_offsets_remain_distinct(self):
  left='😀安全培圳，安全培圳。';right='说明：😀安全培圳，安全培圳。'
  def collect(text,side):
   output=[]
   for index in (text.index('圳'),text.rindex('圳')):
    candidate={'candidates':[{'start':index,'end':index+1,'original':'圳','replacement':'训','candidate_probability':.99,'source_probability':.001,'probability_ratio':990}]}
    accepted,_=classify_dual_candidates(text,validate_candidates(text,candidate),text[:index]+'训'+text[index+1:],text[:index]+'训'+text[index+1:])
    self.assertEqual(len(accepted),1)
    accepted[0].update(side=side,source_evidence_id='e',page=2,source_location_reliable=True)
    output.extend(accepted)
   return output
  shared=common_word_edits(left,right,collect(left,'left'),collect(right,'right'))
  self.assertEqual(len(shared),2)
  self.assertEqual(len({item['shared_id'] for item in shared}),2)
  self.assertEqual(sorted(item['occurrences'][0]['start'] for item in shared),[left.index('圳'),left.rindex('圳')])


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
