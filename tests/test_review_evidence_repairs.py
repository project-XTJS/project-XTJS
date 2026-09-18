"""Regression cases from the saved 2026-09-15 business review audit."""
import unittest
from copy import deepcopy
from datetime import date
from decimal import Decimal

from app.service.analysis.verification import VerificationChecker
from app.service.analysis.compliance.consistency import ConsistencyChecker
from app.service.analysis.compliance.structured_consistency import StructuredConsistencyEngine
from app.service.analysis.itemized import ItemizedPricingChecker


def block(text, page=1, kind='text', bbox=None):
    return {'text': text, 'page': page, 'type': kind, 'bbox': bbox}


class ReviewEvidenceRepairs(unittest.TestCase):
    def setUp(self):
        self.v = VerificationChecker(None)

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
                    block('日期：2026年8月10日', 8),
                    block('附件4 商务条款偏离表', 8)]
        checked = self.v._effective_attachment_check_chunk(sections)
        self.assertEqual(checked, sections[:4])
        result = self.v._date_check({'requirements': {'requires_date': True}},
            {'text': '\n'.join(s['text'] for s in checked), 'sections': checked, 'pages': [7, 8]},
            {'date': date(2026, 8, 11), 'text': '截止时间', 'page': 1})
        self.assertEqual(result['status'], 'pass')
        self.assertEqual(result['matched_sign_page'], 8)

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
        self.assertNotEqual(result['status'], 'pass')

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
