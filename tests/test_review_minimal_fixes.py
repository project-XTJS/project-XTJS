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

    def test_unified_response_list_without_business_boundary_is_unclear(self):
        t = {'layout_sections': [block('投标文件的组成', kind='heading'), block('1. 营业执照\n2. 授权委托书')]}
        self.assertEqual(TemplateExtractor.extract_business_attachment_scope(t).get('scope_status'), 'unclear')

    def test_separate_composition_headings_use_unified_line_parser(self):
        t = {'layout_sections': [block('投标文件的组成', kind='heading'),
            block('（一）商务标文件', kind='heading'), block('A. 营业执照或事业单位法人证书'),
            block('如为分支机构则须提供总公司授权函'), block('（二）技术标文件', kind='heading')]}
        scope = TemplateExtractor.extract_business_attachment_scope(t)
        self.assertEqual(scope['scope_status'], 'resolved')
        self.assertEqual(len(scope['item_entries']), 1)
        self.assertIn('营业执照', scope['item_entries'][0]['content'])

    def test_response_wording_and_text_blocks_extract_scope_and_templates(self):
        sections = [
            block('医院项目技术支持服务\n第五章 响应文件格式', 22),
            block('一、响应文件的组成', 24),
            block('响应文件由商务文件、技术文件两部分组成。', 24),
            block('（一）商务文件', 24),
            block('1.\n比选保证书（格式参见本章附件1）；', 24),
            block('2.\n报价一览表（格式参见本章附件2）；', 24),
            block('3. 分项报价表（格式参见本章附件3）；', 24),
            block('（二）技术文件（包含但不限于以下内容）', 24),
            block('医院项目技术支持服务\n二、响应文件部分格式附件', 25),
        ]
        attachment_titles = [
            '附件1 比选保证书（格式）',
            '附件2 报价一览表（格式）',
            '附件3 分项报价表（格式自拟）',
            '附件4 响应文件偏离表（格式）',
            '附件5 参选人基本情况表（格式）',
            '附件6 近三年完成的类似项目业绩清单（格式）',
            '附件7-1 法定代表人资格证明书（格式）',
            '附件7-2 法定代表人授权委托书（格式）',
            '附件8 参选人承诺声明函（须加盖公章）',
            '附件9 不参与围标串标承诺书（须加盖公章）',
            '附件10 项目人员配置表（格式）',
            '附件11 财务状况及税收、社会保障资金缴纳情况声明函',
        ]
        for index, title in enumerate(attachment_titles):
            text = title + '\n模板固定正文内容不少于二十个字，用于执行一致性比较。'
            if title.startswith('附件8 '):
                text += '\n合计总价须与附件2保持一致。'
            sections.append(block(text, 25 + index))
        t = {'layout_sections': sections}

        scope = TemplateExtractor.extract_business_attachment_scope(t)
        attachments = TemplateExtractor.extract_response_format_attachments(t)
        requirements, _ = TemplateExtractor.extract_requirements(t)

        self.assertEqual(scope['scope_status'], 'resolved')
        self.assertEqual(len(scope['item_entries']), 3)
        self.assertEqual(
            [item['attachment_number'] for item in attachments],
            ['1', '2', '3', '4', '5', '6', '7-1', '7-2', '8', '9', '10', '11'],
        )
        self.assertTrue(requirements)
        self.assertEqual(
            sum(item['attachment_number'] == '2' for item in attachments),
            1,
        )

    def test_hospital_layout_extracts_thirteen_forms_and_twelve_business_forms(self):
        scope_lines = [
            '1. 比选保证书（格式参见本章附件1）；',
            '2. 报价一览表（格式参见本章附件2）；',
            '3. 分项报价表（格式参见本章附件3）；',
            '4. 响应文件偏离表（格式参见本章附件4）；',
            '5. 参选人基本情况表（格式参见本章附件5）；',
            '6. 业绩清单（格式参见本章附件6）；',
            '7. 法定代表人直接参加或委托授权人参加（格式参见本章附件7-1、7-2）；',
            '8. 承诺声明函（格式参见本章附件8）；',
            '9. 围标串标承诺书（格式参见本章附件9）；',
            '10. 财务声明函（格式参见本章附件11）；',
            '11. 保证金缴纳凭证（如有，格式参见本章附件12）；',
        ]
        sections = [
            block('一、响应文件的组成', 24),
            block('（一）商务文件', 24),
            *[block(line, 24) for line in scope_lines],
            block('（二）技术文件', 24),
            block('1. 项目人员配置表（格式参见本章附件10）；', 24),
            block('第五章 响应文件格式', 22),
            block('二、响应文件部分格式附件', 25),
        ]
        numbers = ['1', '2', '3', '4', '5', '6', '7-1', '7-2', '8', '9', '10', '11', '12']
        for page, number in enumerate(numbers, 25):
            sections.extend([
                block(f'附件{number} 示例表单（格式）', page),
                block('模板固定正文及待填写字段：__________。', page),
            ])
        payload = {'layout_sections': sections}
        bundle = TemplateExtractor.extract_response_format_bundle(payload)
        business, scoped = TemplateExtractor.filter_business_response_attachments(
            payload,
            bundle['attachments'],
        )
        self.assertEqual(len(bundle['attachments']), 13)
        self.assertTrue(scoped)
        self.assertEqual(len(business), 12)
        self.assertNotIn('10', {item['attachment_number'] for item in business})

    def test_blank_form_after_structure_heading_is_valid_template_evidence(self):
        payload = {'layout_sections': [
            block('第五章 响应文件格式', 20),
            block('附件1 空白声明表（格式）', 21),
        ]}
        bundle = TemplateExtractor.extract_response_format_bundle(payload)
        self.assertEqual(bundle['extraction_status'], 'resolved')
        self.assertEqual(len(bundle['attachments']), 1)

    def test_sentence_starting_with_attachment_number_is_not_a_template_title(self):
        self.assertFalse(TemplateExtractor._explicit_template_heading(
            '附件1 中列明的总价应与报价一览表保持一致'
        ))

    def test_regular_bid_security_section_does_not_validate_format_region(self):
        payload = {'layout_sections': [
            block('投标文件格式', 10),
            block('15. 投标保证金', 12, 'heading'),
            block('15.1 投标保证金递交方式见前附表。', 12),
        ]}
        bundle = TemplateExtractor.extract_response_format_bundle(payload)
        self.assertEqual(bundle['extraction_status'], 'failed')
        self.assertFalse(bundle['attachments'])

    def test_zero_integrity_items_are_unclear_not_pass(self):
        raw = IntegrityChecker().check_integrity(
            {'layout_sections': [block('普通招标正文，没有文件组成或附件模板。')]},
            bid('营业执照'),
        )
        normalized = UnifiedBusinessReviewService()._normalize_integrity(raw)
        self.assertEqual(raw['scored_item_count'], 0)
        self.assertEqual(normalized['review']['status'], 'unclear')
        self.assertIn('未建立完整性检查项', normalized['review']['summary'])
        self.assertNotIn('0/0项通过', normalized['review']['summary'])
        self.assertTrue(normalized['issues']['unclear'])

    def test_zero_consistency_segments_keep_extraction_reason(self):
        normalized = UnifiedBusinessReviewService()._normalize_consistency({
            'evaluated_segments': [],
            'skipped_segments': [],
            'original_segment_count': 0,
            'extraction_status': 'failed',
            'extraction_reason': '未定位到明确的响应文件格式区域。',
            'structure_locations': [],
        })
        self.assertEqual(normalized['review']['status'], 'unclear')
        self.assertEqual(
            normalized['review']['summary'],
            '未定位到明确的响应文件格式区域。',
        )
        self.assertTrue(normalized['issues']['unclear'])

    def test_old_consistency_result_never_displays_none_reason(self):
        normalized = UnifiedBusinessReviewService()._normalize_consistency({
            'evaluated_segments': [],
            'skipped_segments': [],
            'extraction_reason': None,
        })
        self.assertNotEqual(normalized['review']['summary'], 'None')
        self.assertIn('未提取到可比较的模板段', normalized['review']['summary'])

    def test_all_consistency_templates_explicitly_skipped_are_not_counted_passed(self):
        normalized = UnifiedBusinessReviewService()._normalize_consistency({
            'evaluated_segments': [],
            'skipped_segments': [
                {'name': '附件12', 'skip_reason': {'type': 'optional_attachment_not_provided'}},
            ],
            'original_segment_count': 1,
            'extraction_status': 'resolved',
        })
        self.assertEqual(normalized['review']['status'], 'not_applicable')
        self.assertIn('均按明确规则跳过', normalized['review']['summary'])
        self.assertEqual(normalized['metrics']['passed_segment_count'], 0)

    def test_unstable_only_consistency_skip_is_unclear(self):
        normalized = UnifiedBusinessReviewService()._normalize_consistency({
            'evaluated_segments': [],
            'skipped_segments': [
                {'name': '附件1', 'skip_reason': {'type': 'body_too_short'}},
            ],
            'original_segment_count': 1,
            'extraction_status': 'resolved',
        })
        self.assertEqual(normalized['review']['status'], 'unclear')
        self.assertTrue(normalized['issues']['unclear'])

    def test_directory_format_heading_does_not_preempt_real_template_region(self):
        t = {'layout_sections': [
            block('目录', 1, 'heading'),
            block('第五章 响应文件格式', 1),
            block('25', 1),
            block('第四章 评审办法', 10, 'heading'),
            block('第五章 响应文件格式', 20),
            block('二、响应文件部分格式附件', 21),
            block('附件1 比选保证书（格式）', 22),
            block('致采购人：我方已经认真阅读并响应全部采购要求。', 22),
        ]}
        bundle = TemplateExtractor.extract_response_format_bundle(t)
        self.assertEqual(bundle['extraction_status'], 'resolved')
        self.assertEqual([item['attachment_number'] for item in bundle['attachments']], ['1'])
        self.assertEqual(bundle['structure_locations'][0]['page'], 21)

    def test_structure_line_view_does_not_restore_table_data_rows(self):
        table = block('模板表头', 3, 'table')
        table['lines'] = [
            {'text': '项目名称 数量 单价', 'bbox': [10, 20, 500, 32]},
            {'text': '服务器 2 10000', 'bbox': [10, 35, 500, 47]},
        ]
        repaired = TemplateExtractor._template_boundary_sections([table])
        self.assertEqual(len(repaired), 1)
        self.assertEqual(repaired[0]['text'], '模板表头')

    def test_independent_writing_instruction_is_not_appended_to_last_requirement(self):
        t = {'layout_sections': [
            block('响应文件的组成', kind='heading'),
            block('（一）商务文件', kind='heading'),
            block('1. 比选保证书（格式参见本章附件1）；'),
            block('注意：响应文件应编制目录并连续编码。'),
            block('（二）技术文件', kind='heading'),
        ]}
        scope = TemplateExtractor.extract_business_attachment_scope(t)
        self.assertEqual(len(scope['item_entries']), 1)
        self.assertNotIn('注意', scope['item_entries'][0]['source_text'])
        self.assertEqual(scope['scope_status'], 'resolved')

    def test_consistency_does_not_restore_technical_template_outside_business_scope(self):
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
        self.assertFalse(any('技术条款偏离表' in item['title'] for item in templates))

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

    def test_all_optional_absent_is_not_extraction_failure_or_pass(self):
        t = tender(
            '1. 残疾人福利性单位声明函（格式参见本章附件13）（如有）',
            '附件13 残疾人福利性单位声明函（格式）\n单位名称（加盖公章）：',
        )
        raw = IntegrityChecker().check_integrity(t, bid('其他内容'))
        normalized = UnifiedBusinessReviewService()._normalize_integrity(raw)
        self.assertEqual(raw['extracted_item_count'], 1)
        self.assertEqual(raw['actual_check_count'], 0)
        self.assertEqual(normalized['review']['status'], 'not_applicable')
        self.assertIn('本次无必检项', normalized['review']['summary'])
        self.assertFalse(normalized['issues']['unclear'])

    def test_catch_all_other_content_is_optional_when_absent(self):
        t = tender('1. 投标人认为需加以说明的其他内容（如综合实力证明等）')
        raw = IntegrityChecker().check_integrity(t, bid('已提交其他必备材料'))
        detail = next(iter(raw['details'].values()))
        self.assertTrue(detail['is_optional'])
        self.assertFalse(detail['scored'])
        self.assertEqual(detail['status'], '可选项未提供')
        normalized = UnifiedBusinessReviewService()._normalize_integrity(raw)
        self.assertEqual(normalized['review']['status'], 'not_applicable')

    def test_conditional_absent_stays_pending_in_all_three_checks(self):
        t = tender(
            '1. 如为委托参加，提供法定代表人授权委托书（格式参见本章附件7-2）',
            '附件7-2 法定代表人授权委托书（格式）\n法定代表人签字：\n被授权人签字：',
        )
        b = bid('其他内容')
        raw = IntegrityChecker().check_integrity(t, b)
        detail = next(iter(raw['details'].values()))
        self.assertEqual(detail['applicability_status'], 'unclear')
        self.assertFalse(detail['scored'])
        templates = TemplateExtractor.extract_consistency_templates(t)
        self.assertEqual(templates[0]['applicability_status'], 'unclear')
        verified = VerificationChecker(None).check_seal_and_date(t, b)
        result = verified['attachment_results'][0]
        self.assertEqual(result['status'], 'pending')
        self.assertIsNone(result['found'])

    def test_conditional_material_found_is_checked_instead_of_forced_pending(self):
        t = tender(
            '1. 如为委托参加，提供法定代表人授权委托书（格式参见本章附件7-2）',
            '附件7-2 法定代表人授权委托书（格式）\n法定代表人签字：\n被授权人签字：',
        )
        b = bid('附件7-2 法定代表人授权委托书（格式）\n法定代表人签字：张三\n被授权人签字：李四')
        raw = IntegrityChecker().check_integrity(t, b)
        detail = next(iter(raw['details'].values()))
        self.assertEqual(detail['status'], '已找到')
        self.assertEqual(detail['applicability_status'], 'conditional_satisfied')
        self.assertTrue(detail['scored'])
        self.assertTrue(detail['is_passed'])
        self.assertEqual(raw['applicability_unclear_count'], 0)
        normalized = UnifiedBusinessReviewService()._normalize_integrity(raw)
        self.assertEqual(normalized['review']['status'], 'pass')
        self.assertFalse(normalized['issues']['unclear'])

    def test_direct_or_delegated_choice_accepts_one_actual_branch(self):
        t = tender(
            '1. 法定代表人直接参加的应提供法定代表人资格证明书及身份证；委托授权人参加的应提供法定代表人授权委托书及被授权人身份证',
        )
        b = bid('法定代表人授权委托书\n委托代理人：李四')
        raw = IntegrityChecker().check_integrity(t, b)
        detail = next(iter(raw['details'].values()))
        self.assertEqual(detail['status'], '已找到')
        self.assertEqual(detail['applicability_status'], 'conditional_satisfied')
        self.assertEqual(detail['applicability_resolution'], 'delegated')
        self.assertTrue(detail['is_passed'])

    def test_business_license_or_legal_person_certificate_accepts_either_branch(self):
        t = tender('1. 提供企业营业执照或事业单位法人证书，或其他性质单位组织的合法证明材料')
        b = bid('1、事业单位法人证书\n统一社会信用代码：1234567890')
        raw = IntegrityChecker().check_integrity(t, b)
        detail = next(iter(raw['details'].values()))
        self.assertTrue(detail['is_passed'])
        self.assertEqual(detail['resolution_status'], 'matched')
        self.assertEqual(detail['requirement_group']['operator'], 'any_of')
        self.assertEqual(detail['material_resolution'], 'entity_proof_any_of')
        self.assertTrue(any(
            branch['title'] == '事业单位法人证书' and branch['matched']
            for branch in detail['requirement_group']['branches']
        ))

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
