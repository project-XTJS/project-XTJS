from copy import deepcopy
import unittest
from app.service.analysis.verification import VerificationChecker
from app.service.analysis.compliance.consistency import ConsistencyChecker
from app.service.analysis.compliance.integrity import IntegrityChecker
from app.service.analysis.compliance.template_extractor import TemplateExtractor
from app.service.analysis.requirement_groups import parse_group
from app.service.analysis.unified import UnifiedBusinessReviewService


def block(text, page=1, kind='text'):
    return {'text': text, 'page': page, 'type': kind, 'bbox': [10, 20, 500, 50]}


def composition(*items):
    return {'layout_sections': [block('一、投标文件的组成', kind='heading'),
        block('（一）商务标文件', kind='heading'), *items,
        block('（二）技术标文件', page=3, kind='heading')]}


class AttachmentResolutionTests(unittest.TestCase):
    def setUp(self):
        self.v = VerificationChecker(None)
        self.c = ConsistencyChecker()

    def sections(self, payload, names):
        return self.c._build_attachment_lookup(payload, [{'title': n} for n in names])[1]

    def test_text_supplier_title_is_not_instruction(self):
        name = '供应商承诺声明函'
        d = {'layout_sections': [block('（七）' + name), block('我公司承诺遵守要求。')]}
        before = deepcopy(d)
        sections = self.sections(d, [name])
        self.assertEqual(self.v._resolve_attachment({'title': name}, sections)['location_status'], 'matched')
        self.assertEqual(d, before)
        self.assertFalse(self.v._looks_like_attachment_text_title('供应商应当提供声明函'))

    def test_prefix_with_body_keeps_original_text(self):
        name = '财务状况及税收、社会保障资金缴纳情况声明函'
        text = '（九）' + name + ' 我方某物业有限公司符合下述条件：'
        d = {'layout_sections': [block(text, 58), block('具有健全的财务会计制度。', 58)]}
        sections = self.sections(d, [name])
        match = self.v._resolve_attachment({'title': name}, sections)['section']
        self.assertEqual(match['pages'], [58])
        self.assertIn('我方某物业有限公司符合下述条件', match['text'])
        self.assertEqual(d['layout_sections'][0]['text'], text)
        self.assertEqual(match['sections'][0]['location_precision'], 'block')

    def test_body_reference_does_not_create_form(self):
        d = {'layout_sections': [block('请供应商提交供应商承诺声明函，详见附件9。')]}
        self.assertFalse(self.sections(d, ['供应商承诺声明函']))

    def test_container_does_not_replace_child(self):
        names = ['法定代表人资格证明书', '法定代表人授权委托书']
        d = {'layout_sections': [block('三、法定代表人/单位负责人授权委托书', 89, 'heading'),
            block('（一）法定代表人资格证明书', 89, 'heading'), block('兹证明某人为本公司法定代表人。', 89),
            block('（二）法定代表人授权委托书', 90, 'heading'), block('本授权书声明：授权某人为合法代理人。', 90)]}
        sections = self.sections(d, names)
        for name, page in zip(names, [89, 90]):
            self.assertEqual(self.v._resolve_attachment({'title': name}, sections)['section']['pages'], [page])

    def test_identity_document_cannot_substitute_proof(self):
        for actual in ['身份证', '被授权人身份证', '身份证正反面']:
            self.assertFalse(self.v._attachment_titles_compatible('法定代表人资格证明书', actual))
            self.assertFalse(self.v._attachment_titles_compatible('法定代表人授权委托书', actual))
        self.assertTrue(self.v._attachment_titles_compatible('法定代表人资格证明书', '法定代表人身份证明'))

    def test_duplicate_form_is_ambiguous_even_through_fallback(self):
        title = '供应商承诺声明函'
        d = {'layout_sections': [block(title, 5, 'heading'), block('声明正文甲。', 5),
                                  block(title, 8, 'heading'), block('声明正文乙。', 8)]}
        sections = self.sections(d, [title])
        skeleton = {'title': title, 'attachment_number': None, 'reference_text': '声明正文'}
        for candidates in [sections, list(reversed(sections))]:
            result = self.c._structured_engine._match_attachment_with_integrity_fallback(skeleton, {}, candidates)
            self.assertIsNone(result['section'])
            self.assertEqual(result['location_status'], 'ambiguous')
            self.assertEqual(len(result['candidates']), 2)

    def test_ambiguous_signature_does_not_become_missing(self):
        t = {'layout_sections': [block('第五章 投标文件格式', 1, 'heading'),
             block('附件1 供应商承诺声明函', 2, 'heading'), block('投标人名称（盖章）：', 2)]}
        d = {'layout_sections': [block('供应商承诺声明函', 5, 'heading'), block('声明正文。', 5),
                                 block('供应商承诺声明函', 8, 'heading'), block('声明正文。', 8)]}
        raw = self.v.check_seal_and_date(t, d)
        self.assertFalse(raw['missing_attachment_results'])
        self.assertTrue(any(a.get('location_status') == 'ambiguous' for a in raw['attachment_results']))


class RequirementGroupTests(unittest.TestCase):
    A = '保安服务许可证'
    B = '自行招用保安员单位备案证明'

    def check(self, available, op='或', split=True, cross=False):
        first = 'B. 投标人应提供《' + self.A + '》' + op
        last = '《' + self.B + '》；'
        t = composition(block(first, 1), block(last, 2 if cross else 1)) if split else composition(block(first + last))
        before = deepcopy(t)
        d = {'layout_sections': [block(x, i + 5, 'heading') for i, x in enumerate(available)]}
        raw = IntegrityChecker().check_integrity(t, d)
        self.assertEqual(t, before)
        self.assertEqual(raw['scored_item_count'], 1)
        return raw

    def test_any_of_four_combinations(self):
        for available, passed in [([], False), ([self.A], True), ([self.B], True), ([self.A, self.B], True)]:
            with self.subTest(available=available):
                detail = next(iter(self.check(available)['details'].values()))
                self.assertEqual(detail['is_passed'], passed)
                self.assertEqual(detail['requirement_group']['operator'], 'any_of')

    def test_all_of_requires_both(self):
        self.assertFalse(next(iter(self.check([self.B], op='及')['details'].values()))['is_passed'])
        self.assertTrue(next(iter(self.check([self.A, self.B], op='及')['details'].values()))['is_passed'])

    def test_same_block_and_cross_page(self):
        for split, cross in [(False, False), (True, True)]:
            raw = self.check([self.B], split=split, cross=cross)
            detail = next(iter(raw['details'].values()))
            self.assertTrue(detail['is_passed'])
            if cross:
                self.assertEqual({x['page'] for x in detail['template_locations']}, {1, 2})

    def test_role_or_is_not_material_or(self):
        self.assertEqual(parse_group('法定代表人或单位负责人资格证明书')['operator'], 'single')

    def test_catalog_only_does_not_satisfy_group(self):
        raw = self.check(['目录\n1. 自行招用保安员单位备案证明.....8'])
        self.assertFalse(next(iter(raw['details'].values()))['is_passed'])

    def test_next_number_is_not_continuation(self):
        t = composition(block('B. 《保安服务许可证》或'), block('C. 营业执照'))
        entries = TemplateExtractor.extract_business_attachment_scope(t)['item_entries']
        self.assertEqual(len(entries), 2)
        self.assertNotIn('营业执照', entries[0]['content'])

    def test_mixed_connectors_are_unclear(self):
        t = composition(block('B. 《保安服务许可证》或《备案证明》及《营业执照》；'))
        raw = IntegrityChecker().check_integrity(t, {'layout_sections': []})
        self.assertEqual(next(iter(raw['details'].values()))['resolution_status'], 'unclear')
        normal = UnifiedBusinessReviewService()._normalize_integrity(raw)
        self.assertEqual(len(normal['issues']['unclear']), 1)
        self.assertFalse(normal['issues']['missing'])


class GroupBoundaryRegressionTests(unittest.TestCase):
    A = RequirementGroupTests.A
    B = RequirementGroupTests.B
    check = RequirementGroupTests.check

    def test_employee_certificate_does_not_satisfy_enterprise_license(self):
        detail = next(iter(self.check(['保安证'])['details'].values()))
        self.assertFalse(detail['is_passed'])
        self.assertFalse(any(b['matched'] for b in detail['requirement_group']['branches']))

    def test_unfinished_choice_is_unclear(self):
        t = composition(block('B. 《保安服务许可证》或'), block('C. 营业执照'))
        raw = IntegrityChecker().check_integrity(t, {'layout_sections': []})
        detail = next(v for k,v in raw['details'].items() if '保安' in k)
        self.assertEqual(detail['resolution_status'], 'unclear')

    def test_branch_templates_do_not_create_independent_mandatory_items(self):
        from unittest.mock import patch
        t = composition(block('B. 《供应商承诺声明函》或《财务状况声明函》；'))
        forms = [{'title':'附件1 供应商承诺声明函', 'attachment_number':'1'},
                 {'title':'附件2 财务状况声明函', 'attachment_number':'2'}]
        with patch.object(TemplateExtractor, 'filter_business_response_attachments', return_value=(forms, True)):
            requirements, _ = TemplateExtractor.extract_requirements(t)
        self.assertEqual(len(requirements), 1)

    def test_explicit_independent_requirement_is_preserved(self):
        from unittest.mock import patch
        t = composition(block('B. 《供应商承诺声明函》或《财务状况声明函》；'),
                        block('C. 财务状况声明函'))
        forms = [{'title':'附件2 财务状况声明函', 'attachment_number':'2'}]
        with patch.object(TemplateExtractor, 'filter_business_response_attachments', return_value=(forms, True)):
            requirements, _ = TemplateExtractor.extract_requirements(t)
        self.assertEqual(len(requirements), 2)

class HeadingLayoutRegressionTests(unittest.TestCase):
    setUp = AttachmentResolutionTests.setUp
    sections = AttachmentResolutionTests.sections

    def test_expected_heading_with_field_tail_matches_form(self):
        for title in ('开标一览表（格式） 项目名称：____ 项目编号：____',
                      '投标人基本情况表（格式） （一）基本情况'):
            actual = title.split('（格式）')[0]
            d = {'layout_sections': [block(actual, kind='heading'), block('我公司填写内容。')]}
            self.assertEqual(self.v._resolve_attachment({'title': title}, self.sections(d, [title]))['location_status'], 'matched')

    def test_conditional_requirement_does_not_hide_proof_title_hint(self):
        names = ['直接投标应提供法定代表人证明书及身份证；委托投标应提供法定代表人授权委托书',
                 '法定代表人资格证明书']
        hints = self.v._attachment_title_hints([{'title': n} for n in names])
        self.assertTrue(self.v._is_attachment_heading(block(names[1], kind='heading'), hints))

    def test_located_modified_body_is_not_reclassified_as_missing(self):
        title = '供应商承诺声明函'
        d = {'layout_sections': [block(title, 51, 'heading'), block('正文已经作出实质修改。', 51)]}
        section = self.c._structured_engine._match_attachment_with_integrity_fallback(
            {'title': title, 'reference_text': '招标规定的另一段固定承诺。'}, {}, self.sections(d, [title]))
        self.assertEqual(section['location_status'], 'matched')
        self.assertEqual(section['section']['pages'], [51])

    def test_numbered_same_form_subheading_keeps_continued_signoff(self):
        title = '近三年完成的类似项目业绩清单'
        d = {'layout_sections': [block('7. '+title, 12, 'heading'),
            block('7.1 项目业绩清单', 12, 'heading'), block('项目名称：本项目', 12),
            block('日期：2026年8月28日', 13)]}
        r = self.v._resolve_attachment({'title': title}, self.sections(d, [title, '项目业绩清单']))
        self.assertEqual(r['location_status'], 'matched')
        self.assertIn('日期：2026年8月28日', r['section']['check_text'])

    def test_identity_paste_label_does_not_split_proof_signoff(self):
        title = '法定代表人资格证明书'
        d = {'layout_sections': [block(title, 4, 'heading'), block('兹证明其为法定代表人。', 4),
            block('粘贴法定代表人', 4), block('日期：2026年8月20日', 4)]}
        r = self.v._resolve_attachment({'title': title}, self.sections(d, [title]))
        self.assertIn('日期：2026年8月20日', r['section']['check_text'])

    def test_continued_other_information_field_does_not_end_form(self):
        title = '投标人基本情况表'
        d = {'layout_sections': [block(title, 13, 'heading'), block('1. 单位名称：本公司', 13),
            block('4. 其他需要说明的情况：', 15), block('无。', 15), block('日期：2026年8月28日', 15)]}
        r = self.v._resolve_attachment({'title': title}, self.sections(d, [title]))
        self.assertIn('日期：2026年8月28日', r['section']['check_text'])

    def test_signoff_fields_do_not_cut_off_dates(self):
        for title, signoff in [
            ('磋商响应承诺书', '磋商响应单位（加盖公章）'),
            ('近三年类似项目业绩清单', '投标人法定代表人或授权代表签字或盖'),
        ]:
            for kind in ('heading', 'text'):
                with self.subTest(title=title, kind=kind):
                    d = {'layout_sections': [block(title, 25, 'heading'),
                        block('我公司承诺以上内容属实。', 25), block(signoff, 25, kind),
                        block('日期：2026年8月12日', 25)]}
                    r = self.v._resolve_attachment({'title': title}, self.sections(d, [title, '法定代表人资格证明书']))
                    self.assertEqual(r['location_status'], 'matched')
                    self.assertIn('日期：2026年8月12日', r['section']['check_text'])
        title = '法定代表人授权委托书（须加盖公章）'
        self.assertTrue(self.v._is_attachment_heading(block(title, kind='heading'), [{'title': title}]))

    def test_full_heading_split_across_blocks(self):
        title = '财务状况及税收、社会保障资金缴纳情况声明函'
        d={'layout_sections':[block('11、财务状况及税收、社会保障资金缴纳情况 况声明函',62,'heading'),
            block('财务状况及税收、社会保障资金',62),block('缴纳情况声明函',62,'heading'),
            block('我方符合上述条件。',62),block('日期：2026年9月15日',62)]}
        sections=self.sections(d,[title]);r=self.v._resolve_attachment({'title':title},sections)
        self.assertEqual(r['location_status'],'matched')
        self.assertIn('日期：2026年9月15日',r['section']['check_text'])

    def test_unicode_number_dash_does_not_change_form_identity(self):
        title='财务状况及税收、社会保障资金缴纳情况声明函'
        d={'layout_sections':[block('附件 1–6 '+title,14,'heading'),block('我方声明符合要求。',14)]}
        sections=self.sections(d,['附件11 '+title])
        r=self.v._resolve_attachment({'title':'附件11 '+title},sections)
        self.assertEqual(r['location_status'],'matched')
        self.assertEqual(r['section']['pages'],[14])

    def test_subforms_of_generic_form_remain_ambiguous(self):
        title='响应文件偏离表'
        d={'layout_sections':[block(title+'-商务部分',6,'heading'),block('商务条款无偏离',6),
            block(title+'-技术部分',7,'heading'),block('技术条款无偏离',7)]}
        r=self.v._resolve_attachment({'title':title},self.sections(d,[title]))
        self.assertEqual(r['location_status'],'ambiguous')
        self.assertEqual(len(r['candidates']),2)

    def test_specific_form_under_same_purpose_chapter(self):
        d={'layout_sections':[block('六、投标人基本情况介绍可另外再附公司简介',205,'heading'),
            block('6.1 投标人基本情况介绍',205,'heading'),block('单位名称：某公司',205)]}
        r=self.v._resolve_attachment({'title':'投标人基本情况表'},self.sections(d,['投标人基本情况表']))
        self.assertEqual(r['location_status'],'matched')
        self.assertEqual(r['section']['pages'],[205])


if __name__ == "__main__":
    unittest.main()
