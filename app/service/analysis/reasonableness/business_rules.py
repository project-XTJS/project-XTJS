"""Evidence-based pricing policy. Extraction never supplies a missing business rule."""
import re
from html import unescape
from .evidence import MONEY_VALUE, money_fact, decimal_value, compare_money, package_key, REASONS

LIMIT_LABEL = r'(?:单价限价|合同总限价|年度限价|年限价|最高(?:投标|响应|报价|采购|总价)?限价|招标控制价|最高控制价|控制价|最高总价)'
BUDGET_LABEL = r'(?:采购预算|预算金额|项目预算|总预算|预算)'
BID_LABEL = r'(?:投标价格|最终报价|合同总价|服务期总价|年度总价|年度报价|年报价|年总价|投标报价总价|投标总价|参选总价|响应总报价|总报价|报价总价|总金额|总价|合计|小写)'
RATE_LABEL = r'(?:投标|报价)?(?:下浮率|折扣率|优惠率|折让率)'
OPS = {'低于或等于':'<=', '高于或等于':'>=', '不得超过':'<=', '不得高于':'<=', '不得大于':'<=',
       '不超过':'<=', '不高于':'<=', '不大于':'<=', '不得低于':'>=', '不得小于':'>=',
       '不低于':'>=', '不少于':'>=', '大于':'>', '高于':'>', '低于':'<', '小于':'<', '等于':'==',
       '≥':'>=', '≤':'<=', '>=':'>=', '<=':'<=', '>':'>', '<':'<', '=':'=='}


def rate_status(value):
    """Also used for manual values; legacy fallback rules are not authoritative."""
    rate = decimal_value(value.get('current_float_rate') if value.get('current_float_rate') is not None else value.get('float_rate'))
    rule = value.get('applicable_rule') or {}
    if value.get('rule_resolution') != 'resolved' or value.get('rule_source') != 'tender' or not rule:
        return 'unclear', '未能确定适用招标费率规则'
    threshold = decimal_value(rule.get('threshold'))
    op = rule.get('op')
    if rate is None or threshold is None or op not in {'>', '>=', '<', '<=', '=='}:
        return 'unclear', '费率响应或规则不完整'
    passed = {'>':rate > threshold, '>=':rate >= threshold, '<':rate < threshold,
              '<=':rate <= threshold, '==':rate == threshold}[op]
    return ('pass' if passed else 'fail'), f"{rate}% {op} {threshold}%：{'符合' if passed else '不符合'}招标要求"


class BusinessRulesMixin:
    def _pricing_records(self, source):
        parsed = self._parse_input(source)
        seen = set()
        for section in (parsed.get('sections') or []) + (parsed.get('table_sections') or []) + (parsed.get('logical_tables') or []):
            text = str(section.get('text') or '')
            if section.get('type') == 'seal':
                continue
            # HTML is a serialization of table cells, not an OCR correction.
            text = unescape(re.sub(r'</?[A-Za-z][^>]*>', ' ', re.sub(r'</tr\s*>', '\n', text, flags=re.I)))
            lines = [x.strip() for x in text.splitlines() if x.strip()]
            for index, line in enumerate(lines):
                if self._is_catalog_line(line):
                    continue
                if re.fullmatch(r'(?:\d+\.)?\s*(?:'+LIMIT_LABEL+'|'+BUDGET_LABEL+r')\s*', line) and index+1<len(lines):
                    line += '：' + lines[index+1]
                key = section.get('page'), line
                if key in seen:
                    continue
                seen.add(key)
                context = line
                # A dedicated unit header in this very block may govern its rows.
                headers = [x for x in lines[:index] if re.fullmatch(r'(?:金额\s*)?单位\s*[:：]\s*(?:人民币\s*)?(?:元|万元|亿元|万|亿)', x)]
                if headers:
                    context = '\n'.join(headers) + '\n' + line
                yield {**section, 'text': line, 'context': context}

    def _field_money(self, record, label_pattern, role):
        text = self._strip_price_markup(record['text'])
        labels = list(re.finditer(label_pattern, text))
        facts = []
        for index, label in enumerate(labels):
            end = labels[index + 1].start() if index + 1 < len(labels) else len(text)
            tail = text[label.end():end]
            # Only the associated field may supply a number. Do not search arbitrary prose.
            match = re.match(r'\s*[）)]?\s*(?:[（(][^）)\d]{0,16}[）)]\s*)?[：:|]?\s*(?:(?:包件|标项|标段|包)\s*[A-Za-z\d一二三四五六七八九十]+\s*[-—:：]\s*)?(?:人民币\s*)?(' + MONEY_VALUE + r')(?![\d.%％])', tail)
            if not match:
                continue
            boundary = max(text.rfind('，',0,label.start()), text.rfind('。',0,label.start()), text.rfind('；',0,label.start()))
            context = text[max(boundary+1, label.start()-24):end]
            # A different following price field does not govern this amount's unit/basis.
            context = re.split(r'[，,](?=.{0,6}(?:单价|限价|预算|总价))', context, maxsplit=1)[0]
            if label.group() in {'总价','合计'} and re.search(r'序号|数量|货物名称|规格型号', text[:label.start()]):
                continue
            if record['context'] != record['text'] and '\n' in record['context']:
                context = record['context'][:record['context'].rfind('\n')+1] + context
            location = {'page':record.get('page'), 'text':text, 'document':role, 'document_role':role,
                        'label':label.group(), **{k:record[k] for k in ('bbox','coordinate_system') if k in record}}
            fact = money_fact(match.group(1), context=context, label=label.group(), page=record.get('page'), locations=[location])
            if fact['basis']=='unknown' and label.group()=='小写' and re.search(r'(?:投标|参选|响应|合同)总(?:价|报价)', text) and not re.search(r'年度|每年|单价', text):
                fact['basis']='contract'
            fact['keyword'] = label.group()
            facts.append(fact)
        return facts

    @staticmethod
    def _distinct_facts(facts):
        result = {}
        for fact in facts:
            amount = decimal_value(fact.get('amount_decimal',fact.get('amount_yuan')))
            key = (amount, None if amount is not None else fact.get('raw_amount')) + tuple(str(fact.get(k)) for k in ('currency','basis','period','measure','package','resolution'))
            if key in result:
                result[key]['locations'].extend(x for x in fact.get('locations', []) if x not in result[key]['locations'])
            else:
                result[key] = dict(fact, locations=list(fact.get('locations') or []))
        values = list(result.values())
        return [x for x in values if x.get('basis')!='unknown' or not any(y.get('basis')!='unknown' and all(x.get(k)==y.get(k) for k in ('amount_yuan','currency','package','page','period')) for y in values)]

    def _extract_bid_amounts(self, source):
        parsed = self._parse_input(source)
        page, opening = self._locate_bid_opening_page_and_text(parsed)
        facts = []
        if opening:
            for record in self._pricing_records({'layout_sections':[{'page':page,'type':'table','text':opening}]}):
                facts.extend(self._field_money(record, BID_LABEL, 'bidder'))
        if facts and any(x.get('keyword') not in {'合计','总价'} for x in facts):
            return self._distinct_facts(facts)
        opening_facts = facts
        facts = []
        # Only explicit current-response quote labels can be used outside the opening table.
        # Generic contract amounts and itemized column headings include historical contracts.
        for record in self._pricing_records(source):
            facts.extend(self._field_money(record, r'(?:投标报价总价|投标总价|参选总价|响应总报价|总报价|报价总价)', 'bidder'))
        return self._distinct_facts(facts or opening_facts)

    def _extract_bid_total_amount(self, source):
        facts = self._extract_bid_amounts(source)
        if len(facts) == 1:
            return facts[0]
        if facts:
            return {'amount_yuan':None, 'resolution':'unresolved', 'reason_code':'bid_amount_conflict',
                    'candidates':facts, 'locations':[l for f in facts for l in f['locations']]}
        return None

    def resolve_tender_limit(self, source, *, package=None, basis=None):
        records = list(self._pricing_records(source))
        direct, budgets, declarations = [], [], []
        for record in records:
            text = record['text']
            direct.extend(self._field_money(record, LIMIT_LABEL, 'tender'))
            budgets.extend(self._field_money(record, BUDGET_LABEL, 'tender'))
            if re.search(LIMIT_LABEL, text):
                kind = 'explicit_none' if re.search(r'(?:不设(?:置)?|无|没有)\s*'+LIMIT_LABEL+'|'+LIMIT_LABEL+r'\s*[：:]?\s*(?:不设(?:置)?|无)(?:[。；;\s]|$)', text) else (
                    'budget' if self._looks_like_same_budget_limit_context(text) else ('reference' if re.search(LIMIT_LABEL+r'\s*(?:[（(][^）)]{0,16}[）)])?\s*[：:|]?\s*(?:详见|见第|以.{0,30}为准)', text) else 'amount'))
                declarations.append({'kind':kind, 'package':package_key(text), 'text':text, 'page':record.get('page'),
                                     'locations':[{'document':'tender','document_role':'tender','page':record.get('page'),'text':text}]})
        packages = {x.get('package') for x in direct + declarations + budgets if x.get('package')}
        selected = [x for x in declarations if x['package'] == package]
        candidates = [x for x in direct if x.get('package') == package]
        unresolved_refs = []
        for declaration in selected:
            if declaration['kind'] == 'budget':
                matches = [x for x in budgets if x.get('package') == package]
                if not matches:
                    unresolved_refs.append(declaration)
                for fact in matches:
                    candidates.append({**fact, 'locations': declaration['locations'] + fact['locations'], 'reference':declaration})
            elif declaration['kind'] == 'reference':
                # Follow an explicit chapter/table title only. Unresolved references remain reviewable.
                target = re.search(r'(?:详见|见)\s*(?:招标文件)?\s*(第[一二三四五六七八九十\d]+章|附表[一二三四五六七八九十\d]+)', declaration['text'])
                matches = []
                if target:
                    active = False
                    for record in records:
                        if re.match(r'^\s*(?:第.+章|附表\s*\d+)', record['text']):
                            active = target.group(1) in record['text']
                        if active:
                            matches.extend(x for x in self._field_money(record, LIMIT_LABEL, 'tender') if x.get('package') == package)
                if not matches:
                    unresolved_refs.append(declaration)
                for fact in matches:
                    candidates.append({**fact, 'locations':declaration['locations'] + fact['locations'], 'reference':declaration})
        candidates = self._distinct_facts(candidates)
        if basis == 'contract':
            annual = [x for x in candidates if x.get('basis')=='annual' and x.get('resolution')=='resolved']
            formula_records = []
            for record in records:
                match = re.search(r'合同总限价\s*(?:=|＝|为)\s*年度(?:最高)?限价\s*(?:[×*]|乘以)\s*(\d+(?:\.\d+)?)', record['text'])
                if match and package_key(record['text']) == package:
                    formula_records.append((record, decimal_value(match.group(1))))
            if len(annual)==1 and len({m for _,m in formula_records})==1:
                record, multiplier = formula_records[0]
                if multiplier is not None and multiplier>0:
                    original = annual[0]
                    candidates.append({**original, 'amount_yuan':float(decimal_value(original.get('amount_decimal',original['amount_yuan']))*multiplier),
                        'amount_decimal':str(decimal_value(original.get('amount_decimal',original['amount_yuan']))*multiplier),
                        'basis':'contract','period':None, 'conversion':{'formula':record['text'],'multiplier':str(multiplier),'source_amount':original},
                        'locations':original['locations']+[{'document':'tender','document_role':'tender','page':record.get('page'),'text':record['text']}]})
        compatible = [x for x in candidates if x.get('basis') == basis]
        if compatible:
            candidates = compatible
        no_limit = [x for x in selected if x['kind'] == 'explicit_none']
        evidence = [l for x in selected for l in x['locations']] + [l for x in candidates for l in x['locations']]
        if no_limit and not candidates and not unresolved_refs and all(x['kind']=='explicit_none' for x in selected):
            return {'resolution':'explicit_none','reason_code':'explicit_no_limit','locations':evidence, 'package':package}
        reason = 'limit_unresolved'
        if no_limit and candidates or len(candidates)>1:
            reason = 'limit_conflict'
        elif len(candidates)==1 and not unresolved_refs:
            fact = candidates[0]
            if fact['resolution']=='resolved' and fact['basis']!='unknown':
                return {**fact, 'candidates':candidates, 'locations':evidence or fact['locations']}
            reason = fact.get('reason_code') or 'basis_unknown'
        if package is None and packages and not candidates and not selected:
            reason = 'package_mismatch'
        return {'resolution':'unresolved', 'reason_code':reason, 'candidates':candidates,
                'declarations':selected, 'locations':evidence, 'package':package}

    def _extract_tender_max_limit(self, source):
        fact = self.resolve_tender_limit(source)
        return fact if fact.get('resolution') == 'resolved' else None

    def check_bid_price_against_tender_limit(self, tender_source, bid_source):
        bids = self._extract_bid_amounts(bid_source)
        comparisons = []
        for bid in bids or [None]:
            limit = self.resolve_tender_limit(tender_source, package=(bid or {}).get('package'), basis=(bid or {}).get('basis'))
            if limit['resolution']=='explicit_none':
                status, reason = 'not_applicable', 'explicit_no_limit'
            else:
                status, reason = compare_money(bid, limit)
            comparisons.append({'status':status, 'reason_code':reason, 'bid_amount':bid, 'tender_limit':limit})
        # Distinct quotes require identified periods/packages; an arbitrary row is never selected as the winner.
        if len(bids)>1:
            scopes = [(x.get('package'),x.get('basis'),x.get('period')) for x in bids]
            if len(set(scopes)) != len(scopes):
                for comparison in comparisons:
                    comparison.update(status='unclear', reason_code='bid_amount_conflict')
        status = 'fail' if any(x['status']=='fail' for x in comparisons) else ('unclear' if any(x['status']=='unclear' for x in comparisons) else ('pass' if any(x['status']=='pass' for x in comparisons) else 'not_applicable'))
        locations = [l for c in comparisons for f in (c['tender_limit'],c['bid_amount'] or {}) for l in f.get('locations', [])]
        first = comparisons[0]
        return self._build_result({'pass':'合格','fail':'失败','unclear':'待复核','not_applicable':'不适用'}[status], '最高限价核验',
            [REASONS.get(c['reason_code'], {'explicit_no_limit':'招标明确不设最高限价','bid_amount_conflict':'存在多个未明确归属的报价，需复核'}.get(c['reason_code'],'报价依据不足，需复核')) for c in comparisons],
            locations=locations, pages=[l.get('page') for l in locations],
            extra={'status':status,'comparisons':comparisons,'limit_resolution':first['tender_limit'],
                   'tender_limit':first['tender_limit'],'bid_total':first['bid_amount']})

    def _tender_rate_rules(self, source):
        rules = []
        op_pattern = '|'.join(re.escape(x) for x in sorted(OPS,key=len,reverse=True))
        for record in self._pricing_records(source):
            for clause in re.split(r'[；;。\n]', record['text']):
                match = re.search('('+RATE_LABEL+r')\s*(?:应|须|必须|要求|为|[：:])*\s*('+op_pattern+r')\s*(\d+(?:\.\d+)?)\s*[%％]', clause)
                if not match:
                    continue
                prefix = clause[:match.start()].strip(' ：:|，,')
                prefix = re.sub(r'^\d+[.、]\s*', '', prefix)
                prefix = re.sub(r'的$', '', prefix)
                pkg = package_key(clause)
                prefix = re.sub(r'(?:包件|标项|标段)\s*[：:]?\s*[A-Za-z\d一二三四五六七八九十]+', '', prefix).strip()
                generic = not prefix or bool(re.fullmatch(r'(?:本项目|全项目|所有业务|各项业务|投标人|参选人|供应商|报价要求|投标报价要求)(?:的)?', prefix))
                operator = OPS[match.group(2)]
                if re.search(r'无效|否决|不接受|不予接受', clause[match.end():]):
                    operator = {'<':'>=', '<=':'>', '>':'<=', '>=':'<', '==':None}.get(operator)
                if operator is None:
                    continue
                rules.append({'op':operator,'threshold':float(match.group(3)),
                              'rate_label':re.sub(r'^(?:投标|报价)','',match.group(1)),
                              'biz_name':None if generic else prefix, 'generic':generic, 'package':pkg,
                              'text':clause, 'locations':[{'document':'tender','document_role':'tender','page':record.get('page'),'text':clause}]})
        return rules

    def _check_tender_rates(self, source, tender_source, parsed, page, opening):
        rules = self._tender_rate_rules(tender_source)
        rows = self._extract_float_rate_rows(parsed, page, opening, {})
        if not rows:
            value = self._extract_single_float_rate_from_table(parsed, page, opening)
            if value is not None:
                rows = [{'float_rate':value,'rate_label':self._pick_rate_label(opening),'biz_name':'','raw_line':opening,'pages':[page]}]
        output = []
        for row in rows:
            label = re.sub(r'^(?:投标|报价)','',str(row.get('rate_label') or self._pick_rate_label(opening)))
            biz = str(row.get('biz_name_raw') or row.get('biz_name') or '').strip()
            package = package_key(row.get('raw_line') or opening)
            applicable = [x for x in rules if x['package']==package and x['rate_label']==label]
            specific = [x for x in applicable if not x['generic'] and self._normalize_biz_name(x['biz_name'])==self._normalize_biz_name(biz)]
            applicable = specific or [x for x in applicable if x['generic']]
            unique = {(x['op'],x['threshold']):x for x in applicable}
            rule = next(iter(unique.values())) if len(unique)==1 else None
            result = {'biz_name':biz or label,'current_float_rate':row.get('float_rate'), 'rate_label':label,
                      'quote_type':'discount_rate' if '折扣' in label else 'float_rate', 'package':package,
                      'rule_source':'tender','rule_resolution':'resolved' if rule else 'unresolved',
                      'applicable_rule':rule, 'rule_candidates':applicable,
                      'required_min_float_rate':rule['threshold'] if rule else None,'rule_operator':rule['op'] if rule else None,
                      'pages':row.get('pages') or [page], 'raw_line':row.get('raw_line'),
                      'locations':[{'document':'bidder','page':p,'text':row.get('raw_line'),'label':biz or label} for p in row.get('pages') or [page]],
                      'rule_locations':[l for x in applicable for l in x['locations']]}
            result['status'], result['message'] = rate_status(result)
            output.append(result)
        status = 'fail' if any(x['status']=='fail' for x in output) else ('pass' if output and all(x['status']=='pass' for x in output) else 'unclear')
        locations = [l for row in output for l in row['locations']+row['rule_locations']]
        return self._build_result({'pass':'合格','fail':'失败','unclear':'待复核'}[status], '费率报价',
            [x['message'] for x in output] or ['未能识别可靠的费率响应'], pages=[page],locations=locations,
            extra={'status':status,'quote_mode':'rate','rate_rows':output,'tender_rules':rules})
