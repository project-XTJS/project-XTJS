"""Explicit material relationships; not a general procurement rule interpreter."""
import re

MATERIAL_END = re.compile(r'(?:执照|许可证|证书|证明|证明书|声明函|承诺书|授权书|授权委托书|凭证|清单|报告|情况表|身份证)$')


def parse_group(text):
    source = str(text or '').strip()
    quoted = list(re.finditer(r'《([^》]+)》', source))
    branches, connectors = [], []
    if len(quoted) >= 2:
        branches = [m.group(1) for m in quoted]
        connectors = [source[a.end():b.start()].strip(' ，,；;') for a, b in zip(quoted, quoted[1:])]
    else:
        parts = re.split(r'(或者|或|以及|及|和|且)', source)
        if len(parts) >= 3:
            names = [re.sub(r'^(?:同时)?(?:需|须|应当|应)?(?:提供|提交|具备)', '', p).strip(' ：:；;。') for p in parts[::2]]
            if all(MATERIAL_END.search(p) for p in names):
                branches, connectors = names, parts[1::2]
    if not branches and re.search(r'(?:或|或者|及|和|且)$', source):
        return {'operator': 'unclear', 'source_text': source, 'branches': [{'title': m.group(1)} for m in quoted]}
    if not branches:
        return {'operator': 'single', 'source_text': source, 'branches': []}
    kinds = {'any_of' if c in {'或', '或者', '任选其一', '任选一项'} else
             'all_of' if c in {'及', '和', '且', '以及', '、', '同时提供'} else 'unclear' for c in connectors}
    operator = next(iter(kinds)) if len(kinds) == 1 else 'unclear'
    if re.search(r'如为|若为|直接投标|委托.{0,5}投标|否则', source):
        operator = 'unclear'
    return {'operator': operator, 'source_text': source,
            'branches': [{'title': title} for title in branches]}


def continuation_view(sections):
    """Coalesce list continuations only within an explicit business list."""
    result = []
    active = False
    pending = None
    for section in sections:
        text = str(section.get('text') or '').strip()
        compact = re.sub(r'\s+', '', text)
        boundary = re.match(r'^(?:[（(]?[一二三四五六七八九十\d]+[）)、.．]?\s*)?(商务|技术)标(?:文件)?(?:[（(:：]|$)', compact)
        if boundary:
            active = boundary.group(1) == '商务'
            pending = None
        elif active and re.match(r'^(?:附件\s*\d|第[一二三四五六七八九十\d]+[章节])', text):
            active = False
            pending = None
        numbered = bool(re.match(r'^\s*(?:[A-Za-z]|\d+)[.．、)）]\s*', text))
        if active and pending is not None and not numbered and not boundary:
            previous = result[pending]
            prev_text = previous['text'].rstrip()
            unfinished = bool(re.search(r'(?:或|或者|及|和|且)$', prev_text) or prev_text.count('《') > prev_text.count('》'))
            same_page = previous.get('page') == section.get('page')
            if (same_page or unfinished) and not prev_text.endswith(('；', ';', '。', '：', ':')) and text and not text.isdigit():
                merged = dict(previous, text=prev_text + text)
                lines = list(previous.get('source_lines') or [previous]) + list(section.get('source_lines') or [section])
                merged['source_lines'] = lines
                merged['lines'] = [{'text': x['text'], 'bbox': x.get('bbox'), 'page': x.get('page')} for x in lines]
                result[pending] = merged
                continue
        result.append(section)
        if active and numbered:
            pending = len(result) - 1
        elif not active:
            pending = None
    return result


def alternative_state(verifier, title, details):
    """Only the exact branch of a satisfied group can be exempted from absence."""
    from .attachment_resolution import compatible
    if any(not d.get('requirement_group') and compatible(verifier, title, name)
           for name, d in details.items()):
        return None
    states = []
    for detail in details.values():
        group = detail.get('requirement_group') or {}
        if group.get('operator') != 'any_of':
            continue
        for branch in group.get('branches', []):
            if compatible(verifier, title, branch['title']):
                states.append('selected' if branch.get('matched') else
                              'alternative_not_provided' if detail.get('is_passed') else
                              'ambiguous' if detail.get('resolution_status') == 'unclear' else 'missing')
    return states[0] if states and len(set(states)) == 1 else None
