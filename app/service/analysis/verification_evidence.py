"""Deadline and attachment statistics shared by automatic and manual review."""
from datetime import date
import re


def resolve_deadline(checker, payload):
    candidates, edges, amendments = {}, set(), []
    for section in checker._sections(payload):
        if section['type'] == 'seal' or checker._catalog_like(section['text']):
            continue
        lines = checker._lines(section['text']) or [section['text']]
        for index, line in enumerate(lines):
            text = checker._compact(line)
            anchors = [x for x in checker.DEADLINE_ANCHORS if checker._compact(x) in text]
            if not anchors:
                continue
            anchor = max(anchors, key=len)
            start = text.find(checker._compact(anchor))
            window = text[start:start+220]
            if index+1<len(lines) and re.match(r'^(?:现|延期|延长|调整|变更|更正|修改|以)', checker._compact(lines[index+1])):
                window += checker._compact(lines[index+1])[:100]
                line += '\n' + lines[index+1]
            dates = checker._date_candidates(window)
            if not dates and index+1<len(lines):
                window += checker._compact(lines[index+1])[:80]
                dates = checker._date_candidates(window)
            if not dates:
                continue
            loc = {'page':section.get('page'),'bbox':section.get('bbox'),'text':line,
                   'document':'tender','document_role':'tender'}
            if section.get('coordinate_system'):
                loc['coordinate_system'] = section['coordinate_system']
            primary = any(x in anchor for x in checker.DEADLINE_PRIMARY_ANCHOR_MARKERS)
            first = dates[0]
            def add(candidate):
                key = candidate['date'].isoformat()
                record = candidates.setdefault(key, {'date':key,'text':line,'page':section.get('page'),'primary_anchor':primary,'locations':[]})
                record['primary_anchor'] = record['primary_anchor'] or primary
                if loc not in record['locations']:
                    record['locations'].append(loc)
                return key
            add(first)
            for index_date, candidate in enumerate(dates[1:],1):
                previous = dates[index_date-1]
                between = window[previous['start']+len(previous['text']):candidate['start']]
                if any(checker._compact(x) in between for x in checker.DEADLINE_ANCHORS) or re.fullmatch(r'[，,、\s]*(?:或|或者|分别为)?[，,、\s]*', between):
                    add(candidate)
            for amendment in re.finditer(r'(?:现(?:延期|延长|变更|调整)?至|(?:延期|延长)至|(?:现)?(?:调整|变更|更正|修改)为|以(?=.{0,40}为准))', window):
                following = [x for x in dates if x['start']>=amendment.end() and x['start']-amendment.end()<24]
                if not following:
                    continue
                new = add(following[0])
                preceding = [x for x in dates if x['start']<amendment.start()]
                if preceding:
                    old = add(preceding[-1])
                    if old != new:
                        edges.add((old,new))
                else:
                    amendments.append(new)
    primary = {k:v for k,v in candidates.items() if v['primary_anchor']}
    candidates = primary or candidates
    edges = {(a,b) for a,b in edges if a in candidates and b in candidates}
    # A single explicit replacement may supersede the only other original date.
    if len(set(amendments))==1 and len(candidates)==2 and not edges:
        new = amendments[0]
        edges.add((next(k for k in candidates if k!=new),new))
    outgoing = {}
    for old,new in edges:
        outgoing.setdefault(old,set()).add(new)
    finals = set()
    conflict = False
    for node in candidates:
        seen = set()
        while node in outgoing:
            if node in seen or len(outgoing[node])!=1:
                conflict = True
                break
            seen.add(node)
            node = next(iter(outgoing[node]))
        finals.add(node)
    chosen = next(iter(finals)) if len(finals)==1 and not conflict else None
    return {'resolution':'resolved' if chosen else 'unresolved',
            'reason_code':'resolved' if chosen else ('deadline_conflict' if candidates else 'deadline_not_found'),
            'date':chosen, 'selected':candidates.get(chosen), 'candidates':list(candidates.values()),
            'amendments':[{'original_date':a,'effective_date':b} for a,b in sorted(edges)],
            'locations':[l for v in candidates.values() for l in v['locations']]}


def compare_dates(sign, deadline):
    def parse(value):
        if isinstance(value,date):
            return value
        text = str(value or '')
        matches = re.findall(r'((?:19|20)\d{2})\s*[-/.年]\s*(\d{1,2})\s*[-/.月]\s*(\d{1,2})', text)
        dates = set()
        for y,m,d in matches:
            try:
                dates.add(date(int(y),int(m),int(d)))
            except ValueError:
                pass
        return next(iter(dates)) if len(dates)==1 else None
    signed, due = parse(sign), parse(deadline)
    if signed is None:
        return 'missing_date'
    if due is None:
        return 'missing_deadline'
    return 'pass' if signed<=due else 'late'


def attachment_counts(raw):
    unique = {}
    for item in (raw.get('missing_attachment_results') or []) + (raw.get('attachment_results') or []):
        if not isinstance(item,dict):
            continue
        key = str(item.get('attachment_number') or '') + '|' + str(item.get('title') or '')
        unique[key] = item
    counts = {k:0 for k in ('date_required_count','date_pass_count','date_missing_count','date_late_count',
                            'date_unclear_count','date_not_required_count','position_required_count','position_pass_count',
                            'position_missing_count','position_unclear_count','skipped_attachment_count')}
    skipped = set(raw.get('skipped_optional_attachments') or [])
    skipped.update(x.get('attachment') for x in raw.get('suppressed_by_integrity', []) if isinstance(x,dict) and x.get('attachment'))
    for item in unique.values():
        if item.get('suppressed_by_integrity') or item.get('skipped') or item.get('title') in skipped:
            skipped.add(item.get('title') or str(item.get('attachment_number')))
            continue
        requirements = item.get('requirements') or {}
        dc = str((item.get('date_check') or {}).get('status') or '')
        if requirements.get('requires_date',dc not in {'','not_required'}):
            counts['date_required_count']+=1
            key = 'pass' if dc=='pass' else ('late' if dc=='late' else ('missing' if dc in {'missing','missing_date'} else 'unclear'))
            counts['date_'+key+'_count']+=1
        else:
            counts['date_not_required_count']+=1
        statuses = []
        for name,required in (('signature','requires_signature'),('seal','requires_seal')):
            status = str((item.get(name+'_check') or {}).get('status') or '')
            if requirements.get(required,status not in {'','not_required'}):
                statuses.append(status)
        if statuses:
            counts['position_required_count']+=1
            key = 'pass' if all(x=='pass' for x in statuses) else ('missing' if any(x in {'missing','fail'} for x in statuses) else 'unclear')
            counts['position_'+key+'_count']+=1
    counts['skipped_attachment_count'] = len(skipped)
    return counts


def attachment_summary(counts):
    text = f"签章核验通过 {counts['position_pass_count']}/{counts['position_required_count']} 个附件"
    if counts['date_required_count']:
        text += f"；日期校验通过 {counts['date_pass_count']}/{counts['date_required_count']} 个，缺日期 {counts['date_missing_count']} 个、日期过晚 {counts['date_late_count']} 个、截止日期或日期依据待复核 {counts['date_unclear_count']} 个"
    else:
        text += '；无需日期核验'
    return text + f"；不要求日期 {counts['date_not_required_count']} 个，已跳过 {counts['skipped_attachment_count']} 个附件。"


"""Single projection of independent signature, seal and date conclusions."""
VERSION = 'independent-verification-v1'

def aggregate_status(statuses):
    states = [str(s or 'pending').lower() for s in statuses if s not in ('not_required','not_applicable','skipped')]
    if any(s in {'fail','late'} for s in states): return 'fail'
    if any(s in {'missing','missing_date'} for s in states): return 'missing'
    if any(s not in {'pass','found'} for s in states): return 'unclear'
    return 'pass' if states else 'not_applicable'

def component_message(kind, check):
    label = {'signature':'签字','seal':'盖章','date':'落款日期'}[kind]
    state = check.get('status')
    if state in ('not_required','not_applicable','skipped'): return f'无需核验{label}'
    if state == 'pass': return f'{label}核验通过'
    if kind != 'date' and check.get('presence') == 'detected' and check.get('text_status') == 'unparsed':
        return f'已检测到{label}区域，内容未稳定识别，待复核'
    if state in ('missing','missing_date'): return f'未提供{label}' if check.get('presence') == 'absent' else f'未定位到{label}，待复核'
    if state == 'late': return '落款日期晚于截止日期'
    if state == 'missing_deadline': return '截止日期未确定，待复核'
    return f'{label}依据不足，待复核'

def project_attachment(attachment):
    for kind in ('signature','seal','date'):
        check = attachment.get(kind+'_check') or {}
        check['message'] = component_message(kind, check)
    if (attachment.get('requirements') or {}).get('optionality_conflict'):
        attachment['status'] = 'pending'
    elif attachment.get('found') is False:
        attachment['status'] = 'missing'
    else:
        state = aggregate_status((attachment.get(kind+'_check') or {}).get('status') for kind in ('signature','seal','date'))
        attachment['status'] = 'pending' if state == 'unclear' else state
    attachment['verification_rule_version'] = VERSION
    attachment['summary'] = '；'.join(component_message(k, attachment.get(k+'_check') or {}) for k in ('signature','seal','date'))
    return attachment


def refresh_verification_summary(raw):
    """Auto and manual paths report the same effective attachment evidence."""
    counts = attachment_counts(raw)
    position = raw.setdefault('position_check', {})
    position['status'] = ('missing' if counts['position_missing_count'] else 'pending' if counts['position_unclear_count'] else 'pass' if counts['position_required_count'] else 'not_applicable')
    attachments = (raw.get('attachment_results') or []) + (raw.get('missing_attachment_results') or [])
    for kind in ('signature', 'seal'):
        for category, states in [('missing', {'missing','fail'}), ('pending', {'pending','unclear'})]:
            position[category+'_'+kind+'_attachments'] = list(dict.fromkeys(
                a.get('title') for a in attachments
                if (a.get(kind+'_check') or {}).get('status') in states and a.get('title')))
    raw['summary'] = attachment_summary(counts)
    raw['evidence_counts'] = counts
    return raw
