from copy import deepcopy
import unittest
from unittest.mock import Mock, patch

from app.service.analysis.compliance.template_extractor import TemplateExtractor
from app.service.analysis.compliance.integrity import IntegrityChecker
from app.service.analysis.compliance.structured_consistency import StructuredConsistencyEngine
from app.service.analysis.verification import VerificationChecker
from app.service.analysis.deviation import DeviationChecker
from app.service.analysis.unified import UnifiedBusinessReviewService
from app.service.analysis.manual_review.business_bid_format import _enrich_business_attachment_value


def block(text, page=1, kind='text'):
    return {'text': text, 'type': kind, 'page': page, 'bbox': [10, 20, 500, 700],
            'lines': [{'text': line, 'bbox': [10, 20 + i * 15, 500, 32 + i * 15]} for i, line in enumerate(text.splitlines())]}


def tender(entries, *attachments):
    return {'layout_sections': [block('一、投标文件的组成', kind='heading'),
        block('投标文件由商务标文件、技术标文件两部分组成。\n（一）商务标文件\n' + entries + '\n（二）技术标文件\n1. 技术方案'),
        block('第五章 投标文件格式', 2, 'heading'), block('一、商务标', 2, 'heading'),
        *[block(text, 3 + i, 'heading') for i, text in enumerate(attachments)]]}


def bid(text):
    return {'layout_sections': [block(text, 5, 'heading')]}


class CompositionAndAttachmentTests(unittest.TestCase):
    def test_composition_stays_in_list_and_keeps_letter_items(self):
        t = tender('1. 资格证明文件：\nA. 营业执照\nB. 食品安全许可证\nC. 具备履行合同所必需的设备和专业技术\n能力的证明材料\n2. 授权委托书（格式参见本章附件1）',
                   '附件1 授权委托书（格式）\n本授权书声明：\n人无转委托权\n特此声明')
        original = deepcopy(t)
        scope = TemplateExtractor.extract_business_attachment_scope(t)
        reqs, _ = TemplateExtractor.extract_requirements(t)
        self.assertEqual(scope['scope_status'], 'resolved')
        self.assertTrue(any('营业执照' in x for x in reqs))
        self.assertTrue(any('许可证' in x for x in reqs))
        self.assertTrue(any('技术能力的证明材料' in x for x in reqs))
        self.assertNotIn('人无转委托权', reqs)
        self.assertNotIn('特此声明', reqs)
        self.assertEqual(t, original)

    def test_unclosed_composition_is_reviewed_not_silently_complete(self):
        t = {'layout_sections': [block('投标文件的组成', kind='heading'), block('（一）商务标文件\n1. 营业执照')]}
        result = IntegrityChecker().check_integrity(t, bid('营业执照'))
        normalized = UnifiedBusinessReviewService()._normalize_integrity(result)
        self.assertEqual(result['scope_status'], 'unclear')
        self.assertTrue(normalized['issues']['unclear'])

    def test_same_page_text_heading_starts_next_attachment(self):
        t = tender('1. 分项报价表（格式参见本章附件8）\n2. 商务条款偏离表（格式参见本章附件9）',
            '附件8 分项报价表（格式）\n内容详见附件9的填写要求。\n法定代表人签字：\n附件9 商务条款偏离表（格式）\n法定代表人签字：')
        attachments = VerificationChecker(None)._required_attachments(t)
        by_no = {a['attachment_number']: a for a in attachments}
        self.assertEqual(by_no['8']['requirements']['signature_field_count'], 1)
        self.assertNotIn('附件9 商务条款偏离表', by_no['8']['text'])
        self.assertEqual(by_no['9']['requirements']['signature_field_count'], 1)
        self.assertIn('详见附件9', by_no['8']['text'])
        self.assertNotEqual(by_no['8']['template_locations'][0]['bbox'], by_no['9']['template_locations'][0]['bbox'])

    def test_actual_two_signature_fields_remain_required(self):
        t = tender('1. 授权委托书（格式参见本章附件1）',
                   '附件1 授权委托书（格式）\n法定代表人签字：\n被授权人签字：')
        a = VerificationChecker(None)._required_attachments(t)[0]
        self.assertEqual(a['requirements']['signature_field_count'], 2)

    def test_continued_deviation_table_does_not_drop_signing_page(self):
        v = VerificationChecker(None)
        sections = [block('附件9 商务条款偏离表', 2, 'heading'),
                    block('在30天内向中标人付清发票所开费用 无偏离', 3, 'table'),
                    block('日期：2025年4月16日', 3),
                    block('附件10 投标人基本情况表', 4, 'heading')]
        checked = v._effective_attachment_check_chunk(sections)
        self.assertTrue(any('2025年4月16日' in s['text'] for s in checked))
        self.assertFalse(any('附件10' in s['text'] for s in checked))

    def test_unified_response_list_keeps_legacy_behavior(self):
        t = {'layout_sections': [block('投标文件的组成', kind='heading'), block('1. 营业执照\n2. 授权委托书')]}
        self.assertNotEqual(TemplateExtractor.extract_business_attachment_scope(t).get('scope_status'), 'unclear')

    def test_separate_composition_headings_keep_legacy_parser(self):
        t = {'layout_sections': [block('投标文件的组成', kind='heading'),
            block('（一）商务标文件', kind='heading'), block('A. 营业执照或事业单位法人证书'),
            block('如为分支机构则须提供总公司授权函'), block('（二）技术标文件', kind='heading')]}
        self.assertEqual(TemplateExtractor.extract_business_attachment_scope(t),
                         TemplateExtractor._legacy_business_attachment_scope(t))

    def test_legacy_consistency_keeps_previous_template_scope(self):
        t = {'layout_sections': [block('投标文件的组成', kind='heading'),
            block('（一）商务标文件', kind='heading'), block('1. 营业执照（附件1）'),
            block('（二）技术标文件', kind='heading'), block('2. 技术条款偏离表（附件2）'),
            block('第五章 投标文件格式', 2, 'heading'),
            block('附件1 营业执照\n营业执照内容', 3, 'heading'),
            block('附件2 技术条款偏离表\n技术要求 响应 偏离说明', 4, 'heading')]}
        # Existing requirement extraction may retain a referenced form even
        # when the business-title filter cannot classify that form.
        with patch.object(TemplateExtractor, 'extract_requirements', return_value=(
            ['附件2 技术条款偏离表'], {'附件2 技术条款偏离表': ['2']})):
            templates = TemplateExtractor.extract_consistency_templates(t)
        self.assertTrue(any('技术条款偏离表' in item['title'] for item in templates))

    def test_separate_text_blocks_on_same_page_are_split(self):
        sections = [block('附件8 分项报价表', 3, 'heading'), block('法定代表人签字：', 3),
                    block('附件9 商务条款偏离表', 3), block('法定代表人签字：', 3)]
        repaired = TemplateExtractor._template_boundary_sections(sections)
        self.assertEqual([s['text'] for s in repaired if s['type'] == 'heading'],
                         ['附件8 分项报价表', '附件9 商务条款偏离表'])

    def test_standalone_conditional_subform_keeps_existing_boundary(self):
        sections = [block('附件8 业绩清单', 3, 'heading'), block('附件9-1 法定代表人资格证明书', 4)]
        self.assertEqual(TemplateExtractor._template_boundary_sections(sections), sections)

    def test_optional_absent_is_not_counted_as_pass(self):
        t = tender('1. 营业执照\n2. 残疾人福利性单位声明函（格式参见本章附件13）（如有）',
                   '附件13 残疾人福利性单位声明函（格式）\n单位名称（加盖公章）：')
        b = bid('营业执照')
        raw = IntegrityChecker().check_integrity(t, b)
        detail = next(v for k, v in raw['details'].items() if '声明函' in k)
        self.assertFalse(detail['scored'])
        self.assertFalse(detail['is_passed'])
        self.assertTrue(detail['optionality_locations'])
        normalized = UnifiedBusinessReviewService()._normalize_integrity(raw)
        self.assertEqual(normalized['metrics']['passed_item_count'], 1)
        self.assertEqual(normalized['issues']['missing'], [])
        verified = VerificationChecker(None).check_seal_and_date(t, b)
        self.assertTrue(verified['skipped_optional_attachments'])
        templates = TemplateExtractor.extract_consistency_templates(t)
        self.assertTrue(templates[0]['is_optional'])
        self.assertTrue(templates[0]['optionality_locations'])

    def test_optional_extra_does_not_exempt_main_form(self):
        t = tender('1. 投标人基本情况表（格式参见本章附件10）可另外再附公司简介（如有）',
                   '附件10 投标人基本情况表（格式）\n单位名称（加盖公章）：')
        a = VerificationChecker(None)._required_attachments(t)[0]
        self.assertFalse(a['requirements']['is_optional'])
        raw = IntegrityChecker().check_integrity(t, bid('投标人自行填写：不适用'))
        self.assertTrue(UnifiedBusinessReviewService()._normalize_integrity(raw)['issues']['missing'])

    def test_conflicting_optionality_needs_review(self):
        t = tender('1. 供应商声明函（格式参见本章附件1）（如有）\n2. 必须提供供应商声明函（格式参见本章附件1）',
                   '附件1 供应商声明函（格式）\n单位名称（加盖公章）：')
        raw = IntegrityChecker().check_integrity(t, bid('其他内容'))
        normalized = UnifiedBusinessReviewService()._normalize_integrity(raw)
        self.assertTrue(normalized['issues']['unclear'])
        self.assertFalse(normalized['issues']['passed'])
        v = VerificationChecker(None).check_seal_and_date(t, bid('其他内容'))
        self.assertFalse(v['skipped_optional_attachments'])
        self.assertEqual(v['attachment_results'][0]['status'], 'pending')
        normalized = UnifiedBusinessReviewService()._normalize_verification(v)
        self.assertTrue(normalized['issues']['unclear'])
        self.assertFalse(normalized['issues']['missing'])

    def test_unqualified_listing_does_not_override_optional_title(self):
        t = tender('1. 保证金缴纳凭证（格式参见本章附件1）',
                   '附件1 保证金缴纳凭证（格式）（如有）\n单位名称（加盖公章）：')
        v = VerificationChecker(None).check_seal_and_date(t, bid('其他内容'))
        self.assertTrue(v['skipped_optional_attachments'])
        self.assertFalse(v['missing_attachment_results'])

    def test_consistency_conflict_stays_reviewed_even_when_form_matches(self):
        checker = Mock()
        checker._build_attachment_lookup.return_value = ({}, [])
        checker._integrity_skip_reason_for_title.return_value = {'type': 'integrity_attachment_missing'}
        engine = StructuredConsistencyEngine(checker)
        skeleton = {'title': '附件1 声明函', 'reference_text': '声明函', 'is_optional': False,
                    'optionality_conflict': True, 'optionality_locations': [{'page': 2}]}
        with patch.object(engine, 'build_template_skeleton', return_value=[skeleton]), \
             patch.object(engine, '_match_attachment_with_integrity_fallback', return_value={'section': {}}), \
             patch.object(engine, '_evaluate_attachment', return_value={'name': skeleton['title'], 'status': 'pass', 'is_passed': True}):
            result = engine.compare({}, {}, {})
        self.assertEqual(result[0]['status'], 'unclear')
        normalized = UnifiedBusinessReviewService()._normalize_consistency(result)
        self.assertIn('必交与可选', normalized['issues']['unclear'][0]['message'])
        self.assertEqual(normalized['issues']['unclear'][0]['evidence']['optionality_locations'][0]['document_role'], 'tender')


class DeviationFooterTests(unittest.TestCase):
    def check_text(self, text):
        return DeviationChecker().check_technical_deviation({'pages': [{'page': 1, 'text': '项目需求书'}]},
            {'pages': [{'page': 5, 'text': '技术条款偏离表\n序号 需求 响应 偏离\n' + text}]})

    def test_scanned_text_only_footer_is_not_negative(self):
        result = self.check_text('1. 全部内容 全部响应 无偏离\n2.\n3.\n注：对不满足竞争性磋商文件要求的部分，必须明确如实填写并说明原因。\n投标人名称（盖章）：测试有限公司')
        self.assertEqual(result['negative_deviation_items'], [])
        self.assertEqual(result['unclear_response_items'], [])

    def test_real_negative_not_masked_by_other_row(self):
        result = self.check_text('1. 技术要求 全部响应 无偏离\n2. 驻场服务 备注：我方不满足每日驻场要求，改为每周到场 负偏离\n注：对不满足招标文件要求的部分，必须明确如实填写并说明原因。')
        self.assertTrue(result['negative_deviation_items'])
        self.assertTrue(any('驻场' in i['response_evidence'] for i in result['negative_deviation_items']))

    def test_partial_response_is_preserved_without_star(self):
        result = self.check_text('1. 关键帧误差要求 部分满足，部分帧误差超过要求 部分满足')
        self.assertTrue(result['negative_deviation_items'])

    def test_incomplete_instruction_is_unclear(self):
        result = self.check_text('1.\n注：对不满足条款要求的情况填写说明')
        self.assertFalse(result['negative_deviation_items'])
        self.assertTrue(result['unclear_response_items'])

    def test_incomplete_instruction_in_table_cell_is_unclear(self):
        from tests.test_deviation_self_declared import _bid_with_deviation_row, _tender_without_star
        b = _bid_with_deviation_row(requirement='填写说明', response='对不满足条款要求的情况填写说明', deviation='')
        result = DeviationChecker().check_technical_deviation(_tender_without_star(), b)
        self.assertFalse(result['negative_deviation_items'])
        self.assertTrue(result['unclear_response_items'])

    def test_same_response_concession_does_not_mask_negative(self):
        from tests.test_deviation_self_declared import _bid_with_deviation_row, _tender_without_star
        b = _bid_with_deviation_row(requirement='付款安排', response='其余无偏离，但付款条件不满足', deviation='')
        result = DeviationChecker().check_technical_deviation(_tender_without_star(), b)
        self.assertTrue(result['negative_deviation_items'])

    def test_evidence_filling_instructions_are_not_responses(self):
        result = self.check_text('1. 全部要求 无偏离\n注：对不满足招标文件要求的部分，必须明确如实填写并说明原因。\n未点对点应答或未按要求提供证明材料的视为未响应。\n证明材料，建议采用箭头、标红或下划线等进行明显标注，并在偏离表中标明页码，否则评标委员会有权做负偏离处理。')
        self.assertFalse(result['negative_deviation_items'])

    def test_next_declaration_does_not_become_deviation_row(self):
        rows = DeviationChecker()._extract_rows_from_section({'title': '技术条款偏离表', 'page': 5,
            'text': '1. 全部要求 无偏离\n投标人名称（盖章）：测试公司\n11.残疾人福利性单位声明函\n不符合条件的单位不适用，不能认为部分满足。'}, 'technical')
        self.assertFalse(any('声明函' in r['joined_text'] or '部分满足' in r['joined_text'] for r in rows))


class PreviewEvidenceTests(unittest.TestCase):
    def test_missing_bid_does_not_inherit_tender_page(self):
        service = UnifiedBusinessReviewService()
        evidence = service._verification_attachment_evidence('附件13', source='position_check', lookup={'附件13': {
            'found': False, 'pages': [], 'locations': [], 'template_locations': [{'page': 49, 'bbox': [1, 2, 3, 4]}]}})
        issue = service._issue(status='missing', title='附件13', message='未找到', evidence=evidence)
        self.assertNotIn('page', issue)
        self.assertNotIn('source_page', issue)
        self.assertEqual(issue['locations'][0]['document_role'], 'tender')
        self.assertEqual(issue['locations'][0]['page'], 49)

    def test_manual_attachment_keeps_requirements_and_evidence(self):
        attachment = {'found': True, 'requirements': {'is_optional': True, 'optionality_locations': [{'page': 32}]},
                      'template_locations': [{'page': 49, 'document_role': 'tender'}]}
        value = _enrich_business_attachment_value({'date_text': '2025-04-11', 'manual_note': '人工意见'}, attachment)
        self.assertEqual(value['requirements'], attachment['requirements'])
        self.assertEqual(value['manual_note'], '人工意见')
        self.assertEqual(value['template_locations'], attachment['template_locations'])
        self.assertTrue(value['bid_content_found'])


if __name__ == '__main__':
    unittest.main()
