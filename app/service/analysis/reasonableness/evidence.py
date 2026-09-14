"""Bounded monetary evidence shared by automatic and manual pricing checks."""
import re
from decimal import Decimal, InvalidOperation

MONEY_VALUE = r'[￥¥]?\s*\d[\d,，]*(?:\.\d+)?\s*(?:亿元|万元|元|亿|万)?'
FACT_KEYS = ('raw_amount', 'amount_decimal', 'amount_yuan', 'unit', 'unit_source', 'currency', 'basis',
             'period', 'measure', 'package', 'resolution', 'reason_code', 'context', 'locations', 'page')
MULTIPLIERS = {'元': Decimal(1), '万元': Decimal(10000), '万': Decimal(10000),
               '亿元': Decimal(100000000), '亿': Decimal(100000000)}


def decimal_value(value):
    try:
        number = Decimal(str(value).replace(',', '').replace('，', '').strip())
        return number if number.is_finite() else None
    except (InvalidOperation, ValueError, TypeError):
        return None


def package_key(text):
    match = re.search(r'(?:包件|标项|标段|包)\s*[：:]?\s*([A-Za-z\d一二三四五六七八九十]+)|第\s*([\d一二三四五六七八九十]+)\s*包', str(text or ''))
    value = next((g for g in match.groups() if g), None) if match else None
    return {'一':'1','二':'2','三':'3','四':'4','五':'5','六':'6','七':'7','八':'8','九':'9','十':'10'}.get(value,value)


def basis_of(text):
    text = str(text or '')
    if re.search(r'(?:合同|服务期|全周期|[一二三四五六七八九十\d]+年服务)\s*(?:总价|总金额|总报价)|合同总限价', text):
        return 'contract'
    if re.search(r'[/／]\s*年|每年|年度|年总价|年报价|年限价|每年度', text):
        return 'annual'
    if re.search(r'单价|[/／]\s*(?:人(?!民)|次|份|台|套|月|天|日|小时|件)|每(?:人|次|月|天|日|小时)', text):
        return 'unit'
    if re.search(r'总价|总金额|总报价|最高.{0,4}限价|预算|控制价', text):
        return 'contract'
    return 'unknown'


def measure_of(text):
    match = re.search(r'(?:[/／]|每)\s*(人次|小时|人(?!民)|次|份|台|套|月|天|日|件)', str(text or ''))
    return match.group(1) if match else None


def inherited_unit(context, label=''):
    """Only a supplied field/table scope may supply an implicit multiplier."""
    text = str(context or '')
    field = re.escape(str(label or ''))
    if field:
        found = re.findall(field + r'\s*[（(]\s*(?:单位\s*[：:]?\s*)?(亿元|万元|元|亿|万)(?:\s*[/／]\s*(?:年|月|日|天|小时|人次|人|次|件|台|套|份))?\s*[)）]', text)
        if found:
            return set(found)
    return set(re.findall(r'(?:单位\s*[：:]\s*|(?:金额|总价|报价)\s*(?:[（(]|[/／]))(亿元|万元|元|亿|万)', text))


def money_fact(raw, *, context='', label='', page=None, locations=None, legacy_yuan=False):
    text = str('' if raw is None else raw).strip()
    fact = {'raw_amount': text, 'amount_yuan': None, 'unit': None, 'unit_source': None,
            'currency': 'CNY', 'basis': basis_of(context or label), 'period': None,
            'package': package_key(context), 'measure': measure_of(context), 'resolution': 'unresolved',
            'reason_code': 'amount_not_found', 'context': str(context or ''),
            'page': page, 'locations': locations or []}
    if re.search(r'美元|美金|USD|欧元|EUR|港币|港元|HKD', context + text, re.I):
        fact.update(currency='unsupported', reason_code='currency_not_supported')
        return fact
    match = re.fullmatch(r'(?:人民币|RMB)?\s*([￥¥]?)\s*(\d[\d,，]*(?:\.\d+)?)\s*(亿元|万元|元|亿|万)?', text, re.I)
    if not match:
        return fact
    symbol, number, explicit = match.groups()
    declarations = inherited_unit(context, label)
    if len(declarations) > 1 or (explicit and declarations and any(MULTIPLIERS[x] != MULTIPLIERS[explicit] for x in declarations)):
        fact['reason_code'] = 'unit_conflict'
        return fact
    unit = explicit or next(iter(declarations), None) or ('元' if symbol or legacy_yuan or re.search(r'人民币|RMB', text, re.I) else None)
    if not unit:
        fact['reason_code'] = 'unit_unknown'
        return fact
    amount = decimal_value(number)
    if amount is None:
        return fact
    period = re.search(r'(?:第\s*([一二三四五六七八九十\d]+)\s*年|((?:19|20)\d{2})年度)', context)
    fact.update(amount_yuan=float(amount * MULTIPLIERS[unit]), amount_decimal=str(amount * MULTIPLIERS[unit]), unit=unit,
                unit_source='inline' if explicit else ('declaration' if declarations else 'yuan_field'),
                resolution='resolved', reason_code='resolved',
                period=next((g for g in period.groups() if g), None) if period else None)
    return fact


def manual_money(value, *, label='总价'):
    if not isinstance(value, dict):
        fact = money_fact(value, label=label, context=label, legacy_yuan=isinstance(value, (int, float, Decimal)))
        fact['basis'] = 'unknown'
        return fact
    # Explicit canonical amount_yuan remains backward compatible; never reuse a stale raw amount after editing it.
    canonical = value.get('small_amount_yuan') if value.get('small_amount_yuan') is not None else value.get('amount_yuan')
    if canonical is not None:
        fact = money_fact(str(canonical), context=label, label=label, legacy_yuan=True)
        fact['context'] = str(value.get('context') or label)
    else:
        raw = next((value[k] for k in ('raw_amount', 'amount', 'small_raw_amount') if value.get(k) is not None), '')
        unit = value.get('unit')
        context = str(value.get('context') or label) + (f' 单位：{unit}' if unit else '')
        fact = money_fact(raw, context=context, label=label)
    for key in ('basis', 'period', 'measure', 'package', 'currency', 'page', 'locations'):
        if value.get(key) is not None:
            fact[key] = value[key]
    if not value.get('basis'):
        fact['basis'] = basis_of(value.get('context') or '')
    fact['basis'] = {'年度':'annual','合同全周期':'contract','全周期':'contract','单价':'unit'}.get(fact['basis'],fact['basis'])
    if value.get('resolution') == 'unresolved' and canonical is None:
        fact.update(resolution='unresolved', reason_code=value.get('reason_code', 'unresolved'))
    return fact


def compare_capital_amounts(small, capital):
    small, capital = decimal_value(small), decimal_value(capital)
    if small is None or capital is None:
        return 'unclear'
    return 'pass' if abs(small-capital) < Decimal('0.01') else 'fail'


def compare_money(bid, limit):
    for fact in (bid, limit):
        if not fact or fact.get('resolution') != 'resolved' or decimal_value(fact.get('amount_yuan')) is None:
            return 'unclear', (fact or {}).get('reason_code', 'amount_not_found')
    if bid.get('currency') != limit.get('currency'):
        return 'unclear', 'currency_mismatch'
    if bid.get('package') != limit.get('package') and (bid.get('package') or limit.get('package')):
        return 'unclear', 'package_mismatch'
    if bid.get('basis') not in {'annual', 'contract', 'unit'} or limit.get('basis') not in {'annual', 'contract', 'unit'}:
        return 'unclear', 'basis_unknown'
    if bid.get('basis') != limit.get('basis'):
        return 'unclear', 'basis_mismatch'
    if bid.get('basis') == 'unit' and (not bid.get('measure') or bid.get('measure') != limit.get('measure')):
        return 'unclear', 'unit_basis_requires_confirmation'
    if limit.get('period') and bid.get('period') != limit.get('period'):
        return 'unclear', 'period_mismatch'
    passed = decimal_value(bid.get('amount_decimal', bid['amount_yuan'])) <= decimal_value(limit.get('amount_decimal', limit['amount_yuan'])) + Decimal('0.01')
    return ('pass', 'within_limit') if passed else ('fail', 'exceeds_limit')


REASONS = {
    'amount_not_found': '未能识别完整金额', 'unit_unknown': '金额单位未能确定',
    'unit_conflict': '金额单位存在冲突', 'currency_not_supported': '币种需要人工确认',
    'currency_mismatch': '双方币种不一致', 'package_mismatch': '双方包件归属不一致或未明确',
    'basis_unknown': '计价口径未能确定', 'basis_mismatch': '年度、合同总价或单价口径不一致',
    'unit_basis_requires_confirmation': '单价计量对象需要人工确认', 'period_mismatch': '报价年度或期间不一致',
    'limit_unresolved': '未能可靠确定最高限价', 'limit_conflict': '最高限价候选存在冲突',
    'within_limit': '投标金额未超过适用最高限价', 'exceeds_limit': '投标金额超过适用最高限价',
}
