import json
import unittest
from copy import deepcopy
from app.service.analysis.reasonableness import ReasonablenessChecker
from app.service.analysis.reasonableness.evidence import money_fact, manual_money, compare_money
from app.service.analysis.reasonableness.business_rules import rate_status
from app.service.analysis.verification import VerificationChecker
from app.service.analysis.verification_evidence import attachment_counts, attachment_summary, compare_dates
from app.service.analysis.unified import UnifiedBusinessReviewService
from app.service.analysis.manual_review.business_bid_format import (
    _compact_price_constraint_value, _compact_opening_amount_value, _compact_rate_quote_value,
    _recompute_manual_pricing, _recompute_manual_verification)


def doc(*texts):
    return {'layout_sections':[{'page':i+1,'type':'text','text':text,'bbox':[1,2,3,4]} for i,text in enumerate(texts)]}


class PricingEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.p = ReasonablenessChecker()

    def limit(self, tender, bid):
        return self.p.check_bid_price_against_tender_limit(doc(tender),doc('报价一览表\n'+bid))

    def test_units_and_decimal(self):
        for raw, expected in [('20万元',200000),('0.002亿元',200000),('20万',200000),('0.002亿',200000),('200000元',200000),('0.1万元',1000)]:
            with self.subTest(raw=raw):
                self.assertEqual(money_fact(raw)['amount_yuan'],expected)
        result=self.limit('最高限价：100000元','投标总价：20万元')
        self.assertEqual(result['status'],'fail')
        self.assertEqual(result['bid_total']['amount_yuan'],200000)

    def test_capital_pair_and_unknown_units(self):
        raw=self.p.check_price_compliance(doc('报价一览表\n投标总价：小写：20万元 大写：贰拾万元'))
        self.assertEqual(raw['status'],'pass')
        self.assertEqual(raw['amount_yuan'],200000)
        raw=self.p.check_price_compliance(doc('报价一览表\n投标总价：小写：20 大写：贰拾万元'))
        self.assertEqual(raw['status'],'unclear')

    def test_unit_inheritance_is_bounded(self):
        self.assertEqual(money_fact('20',context='投标总价（万元）：20',label='投标总价')['amount_yuan'],200000)
        self.assertEqual(money_fact('20',context='最高限价（万元/年）：20',label='最高限价')['amount_yuan'],200000)
        self.assertEqual(money_fact('20元',context='单位：万元')['resolution'],'unresolved')
        result=self.p.check_bid_price_against_tender_limit(doc('最高限价：10万元'),doc('单位：万元','报价一览表\n投标总价：20'))
        self.assertEqual(result['status'],'unclear')

    def test_annual_not_total_and_no_implicit_conversion(self):
        self.assertEqual(self.limit('年度最高限价：10万元/年，服务期三年','投标总价：24万元，三年服务总价')['status'],'unclear')
        self.assertEqual(self.limit('合同总限价：30万元','合同总价：24万元')['status'],'pass')

    def test_annual_rows_locate_excess(self):
        result=self.limit('年度最高限价：10万元/年','第一年年度报价：9万元/年\n第二年年度报价：11万元/年\n第三年年度报价：4万元/年')
        self.assertEqual(result['status'],'fail')
        failed=[x for x in result['comparisons'] if x['status']=='fail']
        self.assertEqual(len(failed),1)
        self.assertEqual(failed[0]['bid_amount']['period'],'二')
        self.assertIn('第二年',failed[0]['bid_amount']['locations'][0]['text'])

    def test_conflicting_bids_do_not_choose_one(self):
        self.assertEqual(self.limit('最高限价：10万元','投标总价：9万元\n投标总价：11万元')['status'],'unclear')

    def test_explicit_none_and_unresolved(self):
        for text in ['最高限价详见需求部分','最高限价：同预算','最高限价：','本项目采购服务']:
            with self.subTest(text=text):
                self.assertEqual(self.limit(text,'投标总价：20万元')['status'],'unclear')
        for text in ['本项目不设最高限价','最高限价：无']:
            with self.subTest(text=text):
                self.assertEqual(self.limit(text,'投标总价：20万元')['status'],'not_applicable')

    def test_same_budget_reference_and_conflict(self):
        result=self.limit('采购预算：10万元\n最高限价：同预算','投标总价：9万元')
        self.assertEqual(result['status'],'pass')
        self.assertTrue(any('同预算' in x['text'] for x in result['locations']))
        self.assertTrue(any('10万元' in x['text'] for x in result['locations']))
        self.assertEqual(self.limit('最高限价：10万元\n最高限价：20万元','投标总价：15万元')['status'],'unclear')
        self.assertEqual(self.limit('最高限价：10万元\n最高限价：100000.00元','投标总价：9万元')['status'],'pass')

    def test_no_cross_package(self):
        tender='包件1 最高限价：同预算\n包件2 采购预算：10万元'
        self.assertEqual(self.limit(tender,'包件1 投标总价：9万元')['status'],'unclear')
        self.assertEqual(self.limit('包件1 最高限价：10万元\n包件2 最高限价：20万元','包件2 投标总价：15万元')['status'],'pass')

    def test_no_percentage_exemption(self):
        self.assertEqual(self.limit('最高限价：10万元','投标下浮率：10%')['status'],'unclear')

    def test_rates_use_tender_and_boundaries(self):
        for rule,rate,status in [('≥5%',2,'fail'),('≥5%',5,'pass'),('>5%',5,'fail'),('≥0%',0,'pass'),('≤100%',100,'pass')]:
            with self.subTest(rule=rule,rate=rate):
                label='折扣率' if '100' in rule else '下浮率'
                r=self.p.check_price_compliance(doc(f'报价一览表\n投标{label}：{rate}%'),tender_source=doc(f'{label}{rule}'))
                self.assertEqual(r['status'],status)
                self.assertTrue(any(x.get('document')=='tender' for x in r['locations']))
        r=self.p.check_price_compliance(doc('报价一览表\n投标下浮率：2%\n下浮率应大于1.5%'),tender_source=doc('下浮率不低于5%'))
        self.assertEqual(r['status'],'fail')

    def test_missing_or_conflicting_rule_is_unclear(self):
        for tender in [None, doc('下浮率≥5%\n下浮率≥8%'),doc('包件2 下浮率≥5%')]:
            self.assertEqual(self.p.check_price_compliance(doc('报价一览表\n下浮率：10%'),tender_source=tender)['status'],'unclear')
        self.assertEqual(self.p.check_price_compliance(doc('报价一览表\n折扣率：101%'))['status'],'unclear')

    def test_formula_requires_explicit_tender_statement(self):
        r=self.limit('年度限价：10万元/年\n合同总限价=年度限价×3','合同总价：24万元')
        self.assertEqual(r['status'],'pass')
        self.assertEqual(r['limit_resolution']['amount_yuan'],300000)
        self.assertTrue(r['limit_resolution']['conversion']['formula'])

    def test_history_and_serials_not_current_prices(self):
        source=doc('报价一览表\n投标总价：9万元','历史合同\n合同总价：100万元\n序号 数量 单价 总价 1 项目 2 100 200')
        before=deepcopy(source)
        r=self.p.check_bid_price_against_tender_limit(doc('最高限价：10万元'),source)
        self.assertEqual(r['status'],'pass')
        self.assertEqual(source,before)

    def test_rate_rejection_clause_and_business_priority(self):
        r=self.p.check_price_compliance(doc('报价一览表\n下浮率：5%'),tender_source=doc('下浮率低于或等于5%的投标将被否决'))
        self.assertEqual(r['status'],'fail')
        from unittest.mock import patch
        rows=[{'biz_name':'设计咨询','float_rate':4,'rate_label':'下浮率','pages':[1]}]
        with patch.object(self.p,'_extract_float_rate_rows',return_value=rows):
            r=self.p.check_price_compliance(doc('报价一览表\n下浮率：4%'),tender_source=doc('本项目下浮率≥1%\n设计咨询下浮率≥5%'))
        self.assertEqual(r['status'],'fail')
        self.assertEqual(r['rate_rows'][0]['required_min_float_rate'],5)

    def test_table_cell_reference_and_units(self):
        r=self.limit('采购预算\n10万元\n最高限价\n同预算','投标总价/元 小写：90000')
        self.assertEqual(r['status'],'pass')
        r=self.limit('最高限价：10万元','投标总价（元） 小写）：90000')
        self.assertEqual(r['status'],'pass')

    def test_manual_roundtrip_preserves_units_basis(self):
        bid=money_fact('24万元',context='合同总价：24万元')
        bid.update(capital_amount_yuan=240000)
        limit=money_fact('10万元',context='年度最高限价：10万元/年')
        compact=_compact_opening_amount_value(bid,tender_limit_value=limit)
        compact=json.loads(json.dumps(compact))
        limit=json.loads(json.dumps(_compact_price_constraint_value(limit)))
        self.assertEqual(manual_money(compact)['amount_yuan'],240000)
        self.assertEqual(compare_money(manual_money(compact),manual_money(limit))[0],'unclear')
        check={}
        _recompute_manual_pricing(check,[{'field_group':'opening_amount','effective_value':compact},{'field_group':'price_constraint','effective_value':limit}])
        self.assertEqual(check['review']['status'],'unclear')
        compact.update(small_amount_yuan='20万元',capital_amount_yuan=200000,basis='contract')
        limit=money_fact('10万元',context='合同总限价：10万元')
        _recompute_manual_pricing(check,[{'field_group':'opening_amount','effective_value':compact},{'field_group':'price_constraint','effective_value':limit}])
        self.assertEqual(check['review']['status'],'fail')

    def test_legacy_amount_yuan_does_not_invent_price_basis(self):
        bid=manual_money({'amount_yuan':240000})
        limit=manual_money({'amount_yuan':100000})
        self.assertEqual(bid['amount_yuan'],240000)
        self.assertEqual(bid['basis'],'unknown')
        self.assertEqual(compare_money(bid,limit)[0],'unclear')

    def test_manual_rate_retains_rule_and_zero(self):
        raw=self.p.check_price_compliance(doc('报价一览表\n下浮率：0%'),tender_source=doc('下浮率≥0%'))
        value=json.loads(json.dumps(_compact_rate_quote_value(raw['rate_rows'][0])))
        self.assertEqual(rate_status(value)[0],'pass')
        self.assertEqual(rate_status({'current_float_rate':2,'required_min_float_rate':1.5,'rule_source':'fallback_rule'})[0],'unclear')

    def test_normalizer_prioritizes_structured_status_and_roles(self):
        u=UnifiedBusinessReviewService.__new__(UnifiedBusinessReviewService)
        raw=self.p.check_price_compliance(doc('报价一览表\n下浮率：5%'),tender_source=doc('下浮率≥5%'))
        out=u._normalize_pricing({'self_check':raw,'tender_limit_check':{'status':'unclear','result':'合格','summary':['旧摘要']}})
        self.assertEqual(out['review']['status'],'unclear')
        evidence=out['issues']['unclear'][0]['evidence']
        self.assertTrue(evidence['tender_price_locations'])

    def test_manual_save_read_and_extraction_integration(self):
        from unittest.mock import Mock
        from app.service.analysis.manual_review.business_bid_format import _build_business_format_editable_items, _apply_manual_business_review_inputs
        u=UnifiedBusinessReviewService.__new__(UnifiedBusinessReviewService)
        u.reasonableness_checker=self.p
        u.verification_checker=VerificationChecker(None)
        u.consistency_checker=Mock(build_template_skeleton=Mock(return_value=[]))
        tender=doc('年度最高限价：10万元/年')
        bid=doc('报价一览表\n合同总价：小写：24万元 大写：贰拾肆万元')
        raw={'self_check':self.p.check_price_compliance(bid,tender_source=tender),'tender_limit_check':self.p.check_bid_price_against_tender_limit(tender,bid)}
        bidder={'bidder_key':'b','bidder_name':'测试投标人','checks':{'pricing_check':{**u._normalize_pricing(raw),'raw_result':raw}},'documents':{'business':{'identifier_id':'bid','file_name':'投标.pdf'}}}
        review={'bidders':[bidder]}
        review['extraction_tables']=u._build_review_extraction_tables(tender_payload=tender,tender_meta={'identifier_id':'tender','file_name':'招标.pdf'},bidder_sources=[{'bidder_key':'b','business':{'content':bid}}],bidder_reviews=[bidder])
        rows=review['extraction_tables']['tender_table']['rows']
        constraint=next(x for x in rows if x['field_group']=='price_constraint')
        self.assertEqual(constraint['value']['basis'],'annual')
        editables=_build_business_format_editable_items(review)
        opening=next(x for x in editables if x['field_group']=='opening_amount')
        payload={'items':[{'editable_id':opening['editable_id'],'manual_value':{'small_amount_yuan':'20万元','capital_amount_yuan':200000}}]}
        corrected=_apply_manual_business_review_inputs(json.loads(json.dumps(review)),json.loads(json.dumps(payload)))
        check=corrected['bidders'][0]['checks']['pricing_check']
        self.assertEqual(check['review']['status'],'unclear')
        self.assertEqual(check['raw_result']['self_check']['amount_yuan'],200000)
        reopened=_build_business_format_editable_items(json.loads(json.dumps(corrected)))
        self.assertEqual(next(x for x in reopened if x['field_group']=='price_constraint')['original_value']['basis'],'annual')
        self.assertEqual(next(x for x in reopened if x['field_group']=='opening_amount')['original_value']['basis'],'contract')

    def test_amount_and_rate_response_both_checked(self):
        r=self.p.check_price_compliance(doc('报价一览表\n投标总价：小写：9万元 大写：玖万元\n下浮率：2%'),tender_source=doc('最高限价：10万元\n下浮率≥5%'))
        self.assertEqual(r['case_consistency_status'],'pass')
        self.assertEqual(r['quote_mode'],'mixed')
        self.assertEqual(r['status'],'fail')

    def test_manual_rate_cannot_replace_tender_authority(self):
        from app.service.analysis.manual_review.business_bid_format import _build_business_format_editable_items, _apply_manual_business_review_inputs
        raw=self.p.check_price_compliance(doc('报价一览表\n下浮率：5%'),tender_source=doc('下浮率≥5%'))
        review={'bidders':[{'bidder_key':'b','checks':{'pricing_check':{'raw_result':{'self_check':raw,'tender_limit_check':{'status':'not_applicable'}}}}}]}
        edit=next(x for x in _build_business_format_editable_items(review) if x['field_group']=='rate_quote')
        payload={'items':[{'editable_id':edit['editable_id'],'manual_value':{'current_float_rate':2,'required_min_float_rate':1,'applicable_rule':{'op':'>=','threshold':1}}}]}
        corrected=_apply_manual_business_review_inputs(review,payload)
        self.assertEqual(corrected['bidders'][0]['checks']['pricing_check']['review']['status'],'fail')



class DeadlineEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.v=VerificationChecker(None)

    def test_amendment_chain_and_conflicts(self):
        for text in ['投标截止时间原为2026年9月1日，现延期至2026年9月10日',
                     '投标截止时间原为2026年9月1日，调整为2026年9月5日，现延期至2026年9月10日',
                     '投标截止时间原为2026年9月1日\n现延期至2026年9月10日']:
            with self.subTest(text=text):
                self.assertEqual(self.v.resolve_deadline(doc(text))['date'],'2026-09-10')
        self.assertEqual(self.v.resolve_deadline(doc('投标截止时间2026年9月1日','投标截止时间2026年9月10日'))['resolution'],'unresolved')
        self.assertEqual(self.v.resolve_deadline(doc('投标截止时间2026年9月1日或2026年9月10日'))['resolution'],'unresolved')
        self.assertEqual(self.v.resolve_deadline(doc('投标截止时间2026年9月1日','投标截止时间2026年9月1日'))['date'],'2026-09-01')

    def test_sign_date_retained_without_deadline(self):
        result=self.v._date_check({'requirements':{'requires_date':True}}, {'text':'期：2026年09月08日','sections':[]}, None)
        self.assertEqual(result['status'],'missing_deadline')
        self.assertEqual(result['sign_date'],'2026-09-08')
        self.assertEqual(result['match_method'],'single_date_fallback')

    def test_manual_compares_dates_by_day(self):
        self.assertEqual(compare_dates('2026年9月10日','2026-09-10'),'pass')
        self.assertEqual(compare_dates('2026年9月11日','2026-09-10'),'late')
        self.assertEqual(compare_dates('2026年9月8日',None),'missing_deadline')
        check={'raw_result':{'attachment_results':[{'title':'附件1','requirements':{'requires_date':True},'date_check':{'status':'pass'}}]}}
        value={'date_text':'2026年9月11日','deadline_date':'2026-09-10','date_status':'pass','signature_status':'pass'}
        _recompute_manual_verification(check,[{'field_group':'attachment_result','field_name':'附件1','effective_value':value}])
        self.assertEqual(check['review']['status'],'fail')
        self.assertEqual(check['metrics']['date_late_count'],1)

    def test_legacy_deadline_requires_evidence_or_explicit_correction(self):
        value={'date_text':'2026-09-08','deadline_date':'2026-09-01','date_status':'pass'}
        original={**value,'date_text':'2026-08-31'}
        check={'raw_result':{'attachment_results':[{'title':'附件1','requirements':{'requires_date':True},'date_check':{'status':'pass'}}]}}
        item={'field_group':'attachment_result','field_name':'附件1','effective_value':value,'original_value':original,'has_manual_value':True}
        _recompute_manual_verification(check,[item])
        self.assertEqual(value['date_status'],'missing_deadline')
        value['deadline_date']='2026-09-10'
        _recompute_manual_verification(check,[item])
        self.assertEqual(value['date_status'],'pass')

    def test_counts_use_unique_effective_details(self):
        a={'title':'附件1','attachment_number':'1','requirements':{'requires_signature':True,'requires_seal':True,'requires_date':True},'signature_check':{'status':'missing'},'seal_check':{'status':'missing'},'date_check':{'status':'missing_deadline'}}
        raw={'attachment_results':[a,deepcopy(a)],'skipped_optional_attachments':['附件2']}
        counts=attachment_counts(raw)
        self.assertEqual(counts['position_required_count'],1)
        self.assertEqual(counts['position_missing_count'],1)
        self.assertEqual(counts['date_pass_count'],0)
        self.assertEqual(counts['date_unclear_count'],1)
        self.assertEqual(counts['skipped_attachment_count'],1)
        a['requirements']['requires_date']=False
        self.assertIn('无需日期核验',attachment_summary(attachment_counts({'attachment_results':[a]})))

if __name__=='__main__':
    unittest.main()
