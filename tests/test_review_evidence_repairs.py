"""Regression cases from the saved 2026-09-15 business review audit."""
import unittest
from copy import deepcopy
from datetime import date
from decimal import Decimal
from unittest.mock import Mock

from app.service.analysis.verification import VerificationChecker
from app.service.analysis.compliance.consistency import ConsistencyChecker
from app.service.analysis.compliance.structured_consistency import StructuredConsistencyEngine
from app.service.analysis.itemized import ItemizedPricingChecker
from app.service.analysis.manual_review.business_bid_format import _recompute_manual_verification
from app.service.analysis.unified import UnifiedBusinessReviewService


def block(text, page=1, kind='text', bbox=None):
    return {'text': text, 'page': page, 'type': kind, 'bbox': bbox}


class ReviewEvidenceRepairs(unittest.TestCase):
    def setUp(self):
        self.v = VerificationChecker(None)

    def _signature_seal_fixture(self, *, include_attachment_title=False):
        tender = {'layout_sections': [
            block('响应截止时间：2026年09月09日', 1),
            block('一、投标文件的组成', 1, 'heading'),
            block('（一）商务标文件\n1. 授权委托书（格式参见本章附件1）\n（二）技术标文件', 1),
            block('第五章 投标文件格式', 2, 'heading'),
            block('一、商务标', 2, 'heading'),
            block('附件1 授权委托书（格式）\n法定代表人签字：\n投标人名称（盖章）：\n日期：', 3, 'heading'),
        ]}
        bid_sections = [block('投标人：上海测试有限公司', 1, bbox=[10, 20, 300, 40])]
        if include_attachment_title:
            bid_sections.append(block('附件1 授权委托书', 5, 'heading', [10, 50, 300, 70]))
        bid_sections.extend([
            block('法定代表人签字：张三', 5, bbox=[70, 500, 350, 520]),
            block('张三', 5, 'signature', [360, 500, 390, 520]),
            block('上海测试有限公司', 5, 'seal', [300, 550, 450, 680]),
            block('日期：2026年09月08日', 5, bbox=[380, 700, 530, 720]),
        ])
        return self.v.check_seal_and_date(tender, {'layout_sections': bid_sections})

    def test_unlocated_attachment_signature_and_seal_are_pending(self):
        raw = self._signature_seal_fixture()
        self.assertTrue(raw['signature_detected'])
        self.assertTrue(raw['seal_detected'])
        self.assertFalse(raw['missing_attachment_results'])
        self.assertFalse(raw['skipped_missing_attachments'])
        attachment = raw['attachment_results'][0]
        self.assertIsNone(attachment['found'])
        self.assertEqual(attachment['location_status'], 'not_found')
        self.assertEqual(attachment['status'], 'unclear')
        for key in ('signature_check', 'seal_check', 'date_check'):
            self.assertEqual(attachment[key]['status'], 'pending')

        normalized = UnifiedBusinessReviewService()._normalize_verification(raw)
        self.assertFalse(normalized['issues']['missing'])
        self.assertEqual(len(normalized['issues']['unclear']), 1)
        self.assertEqual(
            normalized['issues']['unclear'][0]['message'],
            '未定位到对应附件，签字、盖章及日期待人工复核。',
        )
        rows = UnifiedBusinessReviewService()._build_bid_extraction_rows(
            bidder={
                'bidder_key': 'test-bidder',
                'bidder_name': '上海测试有限公司',
                'checks': {'verification_check': {'raw_result': raw}},
            },
            business_payload=None,
            technical_payload=None,
        )
        review_row = next(row for row in rows if row.get('field_group') == 'attachment_result')
        self.assertEqual(review_row['status'], 'unclear')
        self.assertEqual(review_row['value']['signature_status'], 'pending')
        self.assertEqual(review_row['value']['seal_status'], 'pending')

    def test_located_attachment_signature_seal_and_date_still_pass(self):
        raw = self._signature_seal_fixture(include_attachment_title=True)
        attachment = raw['attachment_results'][0]
        self.assertTrue(attachment['found'])
        self.assertEqual(attachment['status'], 'pass')
        for key in ('signature_check', 'seal_check', 'date_check'):
            self.assertEqual(attachment[key]['status'], 'pass')

    def test_verification_uses_saved_ocr_without_calling_ocr_service(self):
        ocr_service = Mock()
        self.v.ocr_service = ocr_service
        self._signature_seal_fixture(include_attachment_title=True)
        ocr_service.assert_not_called()

    def test_image_stage_only_selects_located_required_signature_fields(self):
        tender = {'layout_sections': [
            block('响应截止时间：2026年09月09日', 1),
            block('一、投标文件的组成', 1, 'heading'),
            block('（一）商务标文件\n1. 授权委托书（格式参见本章附件1）\n（二）技术标文件', 1),
            block('第五章 投标文件格式', 2, 'heading'),
            block('一、商务标', 2, 'heading'),
            block('附件1 授权委托书（格式）\n法定代表人签字：', 3, 'heading'),
        ]}
        business = {'layout_sections': [
            block('附件1 授权委托书', 5, 'heading', [10, 50, 300, 70]),
            block('法定代表人签字：___', 5, bbox=[70, 500, 350, 520]),
            block('附件2 说明', 6, 'heading', [10, 50, 300, 70]),
            block('经办人签字：___', 6, bbox=[70, 500, 350, 520]),
        ]}
        fields = self.v.required_signature_image_fields(tender, business)
        self.assertEqual(len(fields), 1)
        self.assertEqual(fields[0]['page'], 5)
        self.assertIn('法定代表人', fields[0]['field_text'])

    def test_manual_confirmation_of_blank_signature_is_fail(self):
        raw = self._signature_seal_fixture(include_attachment_title=True)
        attachment = raw['attachment_results'][0]
        original = {
            'requirements': attachment['requirements'],
            'signature_status': 'pass',
            'signature_evidence': ['张三'],
            'seal_status': 'pass',
            'seal_evidence': attachment['seal_check'].get('seal_texts') or ['上海测试有限公司'],
            'date_status': 'pass',
            'date_text': attachment['date_check']['matched_sign_text'],
            'deadline_date': attachment['date_check']['deadline_date'],
            'deadline_resolution': attachment['date_check']['deadline_resolution'],
        }
        value = deepcopy(original)
        value['signature_evidence'] = []
        check = {'raw_result': deepcopy(raw)}
        _recompute_manual_verification(check, [{
            'field_name': attachment['title'],
            'original_value': original,
            'effective_value': value,
            'has_manual_value': True,
            'editable_id': 'verification-blank-signature',
        }])
        self.assertEqual(value['signature_status'], 'fail')
        self.assertEqual(check['review']['status'], 'fail')

    def test_integrity_missing_suppresses_unlocated_verification(self):
        service = UnifiedBusinessReviewService()
        raw = self._signature_seal_fixture()
        title = raw['attachment_results'][0]['title']
        filtered = service._filter_verification_raw_result(raw, {
            'details': {title: {'is_passed': False, 'scored': True, 'status': 'missing'}},
        })
        self.assertFalse(filtered['attachment_results'])
        self.assertEqual(filtered['suppressed_by_integrity'][0]['attachment'], title)
        normalized = service._normalize_verification(filtered)
        self.assertFalse(any(normalized['issues'].values()))
        self.assertNotIn('签章核验通过', normalized['review']['summary'])

    def test_legacy_manual_confirmation_does_not_override_first_ocr_evidence(self):
        raw = self._signature_seal_fixture()
        attachment = raw['attachment_results'][0]
        original = {
            'signature_status': 'pending',
            'seal_status': 'pending',
            'date_status': 'pending',
            'deadline_date': attachment['date_check']['deadline_date'],
            'deadline_resolution': attachment['date_check']['deadline_resolution'],
        }
        manual = {
            **original,
            'signature_manually_confirmed': True,
            'seal_manually_confirmed': True,
            'date_text': '2026年09月08日',
        }
        check = {'raw_result': deepcopy(raw)}
        _recompute_manual_verification(check, [{
            'field_name': attachment['title'],
            'original_value': original,
            'effective_value': manual,
            'has_manual_value': True,
            'editable_id': 'verification-attachment-1',
        }])
        self.assertEqual(check['review']['status'], 'unclear')
        corrected = check['raw_result']['attachment_results'][0]
        self.assertEqual(corrected['signature_check']['status'], 'pending')
        self.assertEqual(corrected['seal_check']['status'], 'pending')
        self.assertEqual(corrected['date_check']['status'], 'pass')

    def test_submission_deadline_and_explicit_amendment(self):
        payload = {'layout_sections': [block('提交响应文件截止时间：2026年8月18日下午13时30分')]}
        self.assertEqual(self.v.resolve_deadline(payload)['date'], '2026-08-18')
        payload['layout_sections'].append(block('提交响应文件截止时间原为2026年8月18日，现延期至2026年8月20日'))
        self.assertEqual(self.v.resolve_deadline(payload)['date'], '2026-08-20')

    def test_conflicting_deadlines_remain_unresolved(self):
        result = self.v.resolve_deadline({'layout_sections': [
            block('提交响应文件截止时间：2026年8月18日'),
            block('提交响应文件截止时间：2026年8月20日')]})
        self.assertEqual(result['resolution'], 'unresolved')

    def test_cross_page_reference_keeps_date_but_next_form_stops(self):
        sections = [block('附件3 分项报价表', 7, 'heading'),
                    block('此表合计总价须与', 7),
                    block('附件2报价一览表的报价一致。', 8),
                    block('日期：2026年8月10日', 8, bbox=[380, 700, 530, 720]),
                    block('附件4 商务条款偏离表', 8)]
        checked = self.v._effective_attachment_check_chunk(sections)
        self.assertEqual(checked, sections[:4])
        result = self.v._date_check({'requirements': {'requires_date': True}},
            {'text': '\n'.join(s['text'] for s in checked), 'sections': checked, 'pages': [7, 8]},
            {'date': date(2026, 8, 11), 'text': '截止时间', 'page': 1})
        self.assertEqual(result['status'], 'pass')
        self.assertEqual(result['matched_sign_page'], 8)

    def test_underlined_ocr_date_maps_back_to_source_coordinates(self):
        raw_date = (
            r'日期： $ \underline{2026} $年 $ \underline{9} $月 '
            r'$ \underline{14} $日'
        )
        section = block(raw_date, 17, bbox=[59, 607, 256, 624])
        result = self.v._date_check(
            {'requirements': {'requires_date': True}},
            {'text': raw_date, 'sections': [section], 'pages': [17]},
            {'date': date(2026, 9, 14), 'text': '截止时间', 'page': 4},
        )

        self.assertEqual(result['status'], 'pass')
        self.assertEqual(result['reason_code'], 'date_within_deadline')
        self.assertEqual(result['matched_sign_page'], 17)
        self.assertEqual(result['matched_sign_bbox'], [59, 607, 197, 17])

    def test_multiple_forms_in_text_block_keep_independent_content(self):
        payload = {'layout_sections': [block('附件8 分项报价表\n法定代表人签字：张三\n附件9 商务条款偏离表\n法定代表人签字：李四', 3)]}
        original = deepcopy(payload)
        sections = self.v._attachment_sections(payload)
        self.assertEqual(len(sections), 2)
        self.assertNotIn('李四', sections[0]['text'])
        self.assertNotIn('张三', sections[1]['text'])
        self.assertEqual(payload, original)

    def test_same_number_cannot_override_correct_title(self):
        required = {'attachment_number': '2', 'title': '附件2 报价一览表'}
        wrong = {'attachment_number': '2', 'title': '附件2 历史合同', 'text': '历史合同', 'pages': [37]}
        right = {'attachment_number': '3', 'title': '附件3 报价一览表', 'text': '报价一览表', 'pages': [16]}
        self.assertIs(self.v._match_attachment(required, {'2': [wrong]}, [wrong, right]), right)
        self.assertIsNone(self.v._match_attachment(required, {'2': [wrong]}, [wrong]))
        engine = StructuredConsistencyEngine(ConsistencyChecker())
        self.assertIs(engine._match_attachment(required, [wrong, right])['section'], right)

    def test_spaced_heading_fragment_does_not_match_quote_title(self):
        self.assertFalse(self.v._attachment_titles_compatible('附件2 报价一览表', '一 24 诗白文化'))

    def test_response_document_deviation_title_is_a_boundary(self):
        self.assertTrue(self.v._is_attachment_heading(block('附件 5 响应文件偏离表', 19, 'text')))
        self.assertFalse(self.v._is_attachment_scope_stop_section(
            block('3. 此表合计总价须与附件 2 报价一览表参选总价一致。', 8), '分项报价表'))

    def test_signature_geometry_does_not_accept_distant_prose(self):
        field = block('法定代表人或授权代表签字：', 3, bbox=[65, 594, 369, 610])
        section = {'sections': [block('比选保证书', 3, bbox=[71, 80, 180, 98]),
                                block('权代表宣布如下', 3, bbox=[71, 184, 159, 198]), field],
                   'text': '权代表宣布如下\n'+field['text'], 'pages': [3]}
        attachment = {'requirements': {'signature_field_count': 1}}
        slot = self.v._collect_signature_slots(attachment, section)[0]
        self.assertEqual(slot['bbox'], [65, 594, 304, 16])
        self.assertIsNone(self.v._signature_nearby_text_evidence(slot, section))
        result = self.v._signature_check(attachment, section, {'status': 'missing'}, {'status': 'pass'})
        self.assertEqual(result['filled_count'], 0)
        self.assertNotEqual(result['status'], 'pass')

    def test_nearby_name_and_detection_coordinates_still_work(self):
        field = block('法定代表人签字：', 3, bbox=[65, 594, 269, 610])
        section = {'sections': [field, block('张三', 3, bbox=[280, 591, 315, 612])],
                   'pages': [3], 'text': field['text']+'\n张三'}
        slot = self.v._collect_signature_slots({'requirements': {}}, section)[0]
        self.assertEqual(self.v._signature_nearby_text_evidence(slot, section)['value'], '张三')
        section['signature_locations'] = [{'page': 3, 'box': [280, 591, 35, 21]}]
        self.assertEqual(self.v._signature_nearby_detected_signature(slot, section)['box'], [280, 591, 35, 21])

    def test_distant_signature_detection_cannot_reenter_by_fallback(self):
        slot = {'line': '法定代表人签字：', 'page': 3, 'bbox': [65, 594, 304, 16]}
        section = {'pages': [3, 4], 'signature_locations': [
            {'page': 3, 'box': [70, 180, 40, 20]}, {'page': 4, 'box': [70, 595, 40, 20]}]}
        self.assertIsNone(self.v._signature_nearby_detected_signature(slot, section))

    def test_one_signature_cannot_fill_two_required_slots(self):
        section = {'pages': [3], 'sections': [
            block('法定代表人签字：', 3, bbox=[65, 550, 269, 570]),
            block('被授权人签字：', 3, bbox=[65, 590, 269, 610]),
            block('张三', 3, 'signature', [280, 585, 315, 612])], 'text': ''}
        result = self.v._signature_check({'requirements': {'signature_field_count': 2}},
                                        section, {'status': 'missing'}, {'status': 'pass'})
        self.assertEqual(result['filled_count'], 1)
        self.assertEqual(result['filled_values'][0]['line'], '被授权人签字：')
        self.assertNotEqual(result['status'], 'pass')

    def test_printed_name_on_adjacent_row_cannot_fill_signature_slot(self):
        field = block('法定代表人签字：', 3, bbox=[65, 550, 269, 570])
        section = {
            'pages': [3],
            'sections': [field, block('张三', 3, bbox=[280, 605, 315, 626])],
            'text': field['text'] + '\n张三',
        }
        result = self.v._signature_check(
            {'requirements': {'signature_field_count': 1}}, section,
            {'status': 'missing'}, {'status': 'pass'},
        )
        self.assertEqual(result['filled_count'], 0)
        self.assertEqual(result['status'], 'pending')

    def test_image_signature_presence_fills_only_matching_slot(self):
        legal = block('法定代表人签字：___', 3, bbox=[65, 550, 269, 570])
        delegate = block('被授权人签字：___', 3, bbox=[65, 590, 269, 610])
        section = {
            'pages': [3], 'sections': [legal, delegate], 'text': '',
            'signature_locations': [{
                'page': 3, 'box': [280, 548, 55, 24],
                'source': 'signature_image_detector', 'confidence': 0.91,
                'model': 'signature-model:test', 'field_box': [65, 550, 204, 20],
            }],
        }
        self.assertIsNone(self.v._signature_nearby_detected_signature(
            {'line': delegate['text'], 'page': 3, 'bbox': [65, 590, 204, 20]},
            section,
        ))
        result = self.v._signature_check(
            {'requirements': {'signature_field_count': 2}}, section,
            {'status': 'missing'}, {'status': 'pass'},
        )
        self.assertEqual(result['filled_count'], 1)
        self.assertEqual(result['filled_values'][0]['mode'], 'image_signature_presence')
        self.assertEqual(result['filled_values'][0]['recognition_status'], 'presence_only')
        self.assertEqual(result['pending_count'], 1)

    def test_company_seal_cannot_fill_a_personal_signature_slot(self):
        field = block('法定代表人签字或盖章：___', 3, bbox=[65, 594, 369, 610])
        section = {
            'sections': [
                field,
                block('上海测试有限公司', 3, 'seal', [280, 585, 430, 715]),
            ],
            'pages': [3],
            'text': field['text'],
            'seal_locations': [{'page': 3, 'box': [280, 585, 150, 130]}],
            'seal_texts': ['上海测试有限公司'],
        }
        result = self.v._signature_check(
            {'requirements': {'signature_field_count': 1}},
            section,
            {'status': 'pass'},
            {'status': 'pass'},
        )
        self.assertEqual(result['status'], 'pending')
        self.assertFalse(result['filled_values'])

    def test_signature_field_without_coordinates_stays_pending(self):
        section = {
            'sections': [block('法定代表人签字：张三', 3)],
            'pages': [3],
            'text': '法定代表人签字：张三',
        }
        result = self.v._signature_check(
            {'requirements': {'signature_field_count': 1}},
            section,
            {'status': 'pass'},
            {'status': 'pass'},
        )
        self.assertEqual(result['status'], 'pending')
        self.assertEqual(result['pending_fields'][0]['reason'], 'signature_field_coordinates_missing')

    def test_legacy_missing_lists_merge_into_one_attachment_issue(self):
        item = {
            'title': '附件7 首次报价一览表（格式）',
            'attachment_number': '7',
            'found': False,
            'status': 'missing',
            'requirements': {'requires_signature': True, 'requires_seal': True, 'requires_date': False},
            'signature_check': {'status': 'missing'},
            'seal_check': {'status': 'missing'},
            'date_check': {'status': 'not_required'},
        }
        raw = {
            'compliance_status': 'missing',
            'missing_attachment_results': [item],
            'attachment_results': [],
            'position_check': {
                'missing_attachments': [item['title']],
                'missing_signature_attachments': [item['title']],
                'missing_seal_attachments': [item['title']],
            },
            'date_check': {},
        }
        normalized = UnifiedBusinessReviewService()._normalize_verification(raw)
        issues = normalized['issues']['missing']
        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0]['title'], item['title'])

    def test_prefixed_numbered_form_title_still_matches(self):
        self.assertTrue(self.v._is_attachment_heading(block('1.附件1 比选保证书')))


class ItemizedEvidenceRepairs(unittest.TestCase):
    def setUp(self):
        self.c = ItemizedPricingChecker()

    def table(self, headers, rows, total):
        table = {'id': 't1', 'title': '分项报价表', 'pages': [2], 'headers': headers, 'header_row_count': 1,
                 'rows': [headers, *rows, ['合计', str(total)]]}
        return {'layout_sections': [block('分项报价表', 2, 'heading'),
                    {**block('分项报价表', 2, 'table'), 'logical_table_id': 't1'},
                    block('合计 '+str(total), 2)], 'logical_tables': [table]}

    def test_currency_units_and_model_digits(self):
        headers = ['序号', '货物名称', '品牌', '规格型号', '预估数量', '单价', '总价']
        payload = self.table(headers, [['1', '智能电压力锅', '品牌', 'HY-402D', '2062台', '455元', '938210元']], 938210)
        original = deepcopy(payload)
        result = self.c.check_itemized_logic(payload)
        self.assertEqual(result['status'], 'pass')
        self.assertEqual(result['checks']['sum_consistency']['calculated_total'], '938210.00')
        self.assertEqual(payload, original)
        self.assertIsNone(self.c._money_cell_decimal('HY-402D'))
        self.assertIsNone(self.c._money_cell_decimal('10%'))
        self.assertEqual(self.c._money_cell_decimal('20万元'), Decimal('200000'))

    def test_merged_leading_cells_preserve_quoted_total(self):
        headers = ['使用需求', '序号', '设备名称', '单位', '数量', '报价型号', '含税单价', '含税合价', '备注']
        rows = [['校区甲', '1', '空调', '台', '2', 'MDV-504', '22500', '45000', '品牌'],
                ['2', '空调', '台', '2', 'MDV-680', '27690', '55380', '品牌', '']]
        result = self.c.check_itemized_logic(self.table(headers, rows, 100380))
        self.assertEqual(result['checks']['sum_consistency']['calculated_total'], '100380.00')
        self.assertEqual(result['status'], 'pass')

    def test_missing_amount_row_is_not_a_definite_total_failure(self):
        payload = self.table(['序号', '服务名称', '金额'],
                            [['1', '实施', '10000'], ['2', '培训', ''], ['3', '运维', '20000']], 40000)
        result = self.c.check_itemized_logic(payload)
        self.assertEqual(result['status'], 'unknown')
        self.assertEqual(result['checks']['sum_consistency']['status'], 'unknown')
        self.assertGreater(result['checks']['row_arithmetic']['unresolved_count'], 0)

    def test_missing_total_does_not_borrow_unit_price(self):
        payload = self.table(['序号', '名称', '数量', '单价', '总价'],
                            [['1', '设备', '2', '100', ''], ['2', '服务', '1', '500', '500']], 700)
        result = self.c.check_itemized_logic(payload)
        self.assertEqual(result['status'], 'unknown')

    def test_explicit_additional_fee_is_counted_once(self):
        payload = self.table(['序号', '服务名称', '数量', '单价', '总价', '备注'],
                            [['1', '设计', '1', '436000', '436000', ''],
                             ['税费及管理费（10%）：', '', '', '43600', '', '']], 479600)
        result = self.c.check_itemized_logic(payload)
        self.assertEqual(result['status'], 'pass')
        self.assertEqual(result['checks']['sum_consistency']['calculated_total'], '479600.00')

    def test_included_fee_is_not_added_again(self):
        payload = self.table(['序号', '服务名称', '数量', '单价', '总价', '备注'],
                            [['1', '设计', '1', '436000', '436000', '已含税费及管理费10%']], 436000)
        self.assertEqual(self.c.check_itemized_logic(payload)['status'], 'pass')

    def test_actual_arithmetic_error_remains_failure(self):
        payload = self.table(['序号', '服务名称', '数量', '单价', '总价'],
                            [['1', '数码印刷', '1', '21500', '215000']], 215000)
        self.assertEqual(self.c.check_itemized_logic(payload)['checks']['row_arithmetic']['status'], 'fail')


if __name__ == '__main__':
    unittest.main()
