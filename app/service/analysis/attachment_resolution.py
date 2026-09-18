"""Read-only views and deterministic resolution of response form locations.

This module deliberately does not change the general integrity synonym index.
"""
from __future__ import annotations

import re
from functools import lru_cache
from .attachment_synonyms import ATTACHMENT_TITLE_SYNONYMS


def form_title(text: str) -> str:
    """Use only an explicit heading prefix; keep source text untouched."""
    text = str(text or '')
    prefix = re.split(r'\s+(?=项目名称\s*[:：]|项目编号\s*[:：]|代理机构内部编号\s*[:：]|[（(][一二三四五六七八九十]+[）)])', text, maxsplit=1)[0]
    if re.search(r'(?:书|表|函|清单|报告|证明)(?:\s*[（(][^）)]*[）)])?\s*$', prefix):
        return prefix.strip()
    return text


def form_kind(title: str) -> str:
    if re.search(r'授权委托书|授权书', title):
        return 'authorization'
    if re.search(r'(?:代表人|单位负责人).*(?:资格证明|身份证明|证明书)', title):
        return 'representative_proof'
    if '身份证' in title:
        return 'identity_document'
    return ''


@lru_cache(maxsize=32)
def _alias_index(verifier):
    index = {}
    for canonical, aliases in ATTACHMENT_TITLE_SYNONYMS.items():
        group = frozenset(verifier._raw_attachment_title_key(x) for x in [canonical, *aliases])
        for key in group:
            index[key] = group
    return index


@lru_cache(maxsize=8192)
def title_keys(verifier, title: str) -> frozenset[str]:
    """Only complete aliases confer equivalence, never a contained short noun."""
    title = form_title(title)
    raw = verifier._raw_attachment_title_key(title)
    keys = set(_alias_index(verifier).get(raw, {raw} if raw else set()))
    # Repeated complete form variants may share one heading (格式 / 工程).
    parts = [verifier._raw_attachment_title_key(x) for x in re.split(r'\s+', title)]
    parts = [x for x in parts if len(x) >= 4]
    if len(parts) > 1 and len(set(parts)) == 1:
        keys.update(_alias_index(verifier).get(parts[0], {parts[0]}))
    return frozenset(keys)


def compatible(verifier, expected: str, actual: str) -> bool:
    ek, ak = form_kind(expected), form_kind(actual)
    if ek and ak and ek != ak:
        return False
    return bool(title_keys(verifier, expected) & title_keys(verifier, actual))


def candidate_view(verifier, sections: list[dict], expected: list[dict]) -> list[dict]:
    """Recover an explicit form prefix without rewriting its OCR source block."""
    titles = []
    for item in expected:
        title = verifier._strip_attachment_title_prefix(form_title(item.get('title')))
        title = re.sub(r'[（(](?:格式|如有|自拟)[）)]', '', title).strip()
        if title:
            titles.append(title)
        for canonical, aliases in ATTACHMENT_TITLE_SYNONYMS.items():
            if verifier._raw_attachment_title_key(title) in {
                verifier._raw_attachment_title_key(x) for x in [canonical, *aliases]
            }:
                titles.extend([canonical, *aliases])
    titles = sorted(set(titles), key=len, reverse=True)
    # Recover a heading split over adjacent OCR blocks only when their full
    # concatenation is an explicitly expected title, on the same page.
    expected_keys = {verifier._raw_attachment_title_key(t) for t in titles}
    joined = []
    i = 0
    while i < len(sections):
        first = sections[i]
        if i + 1 < len(sections):
            second = sections[i + 1]
            combined = str(first.get('text') or '') + str(second.get('text') or '')
            if (first.get('page') == second.get('page')
                    and first.get('type') in {'text', 'heading'} and second.get('type') in {'text', 'heading'}
                    and verifier._raw_attachment_title_key(combined) in expected_keys
                    and verifier._raw_attachment_title_key(first.get('text', '')) not in expected_keys
                    and len(combined) <= 100):
                boxes = [x.get('bbox') for x in (first, second) if isinstance(x.get('bbox'), (list, tuple)) and len(x['bbox']) == 4]
                box = [min(b[0] for b in boxes), min(b[1] for b in boxes), max(b[2] for b in boxes), max(b[3] for b in boxes)] if boxes else None
                joined.append(dict(first, text=combined, type='heading', bbox=box,
                    lines=[{'text':x.get('text'), 'bbox':x.get('bbox'), 'page':x.get('page')} for x in (first, second)],
                    source_block_indexes=[i, i + 1]))
                i += 2
                continue
        joined.append(first)
        i += 1
    result = []
    for source_index, section in enumerate(joined):
        raw = str(section.get('text') or '').strip()
        if section.get('type') not in {'heading', 'text'} or verifier._catalog_like(raw):
            result.append(section)
            continue
        # OCR line boxes take precedence; a block-level prefix keeps its coarse box.
        if section.get('source_block_indexes'):
            result.append(section)
            continue
        lines = [x for x in section.get('lines') or [] if str(x.get('text') or '').strip()]
        pieces = lines or [{'text': x} for x in raw.splitlines() if x.strip()]
        first = str(pieces[0].get('text') or '') if pieces else raw
        stripped = verifier._strip_attachment_title_prefix(first)
        prefix_len = len(first) - len(stripped)
        matched = None
        for title in titles:
            pattern = r'\s*'.join(re.escape(ch) for ch in title if not ch.isspace())
            m = re.match(pattern, stripped)
            suffix_form = m and section.get('type') == 'heading' and bool(re.fullmatch(
                r'\s*.{2,40}(?:资格证明书|授权委托书|声明函)\s*', stripped[m.end():]))
            if m and (suffix_form or not stripped[m.end():].strip() or re.match(
                r'^(?:[（(](?:格式|如有)[）)])?\s*(?:我方|我公司|本公司|本授权|兹证明|致[:：\s]|项目名称[:：]|\n)', stripped[m.end():]
            )):
                matched = prefix_len + m.end()
                break
        if matched is None:
            result.append(section)
            continue
        header_text = first[:matched]
        remainder = first[matched:].strip()
        if not remainder and len(pieces) == 1:
            result.append(dict(section, type='heading'))
            continue
        header = dict(section, text=header_text, type='heading', lines=[], source_block_index=source_index)
        if lines and not remainder:
            header['bbox'] = lines[0].get('bbox') or section.get('bbox')
        else:
            header['location_precision'] = 'block'
        result.append(header)
        if remainder:
            result.append(dict(section, text=remainder, type='heading' if suffix_form else 'text', lines=[], source_block_index=source_index,
                               location_precision='block'))
        for line in pieces[1:]:
            result.append(dict(section, text=line['text'], type='text', lines=[line],
                               bbox=line.get('bbox') or section.get('bbox'), source_block_index=source_index))
    return result


def resolve(verifier, attachment: dict, sections: list[dict]) -> dict:
    from app.config.settings import settings
    from .compliance.structured_consistency import lexical_similarity
    title = form_title(attachment.get('title'))
    raw_key = verifier._raw_attachment_title_key(title)
    eligible = []
    for section in sections:
        actual = form_title(section.get('title'))
        ek, ak = form_kind(title), form_kind(actual)
        if section.get('is_container') or (ek and ak and ek != ak):
            continue
        # A continuous chapter + form heading is one span; score its explicit
        # opening form titles rather than the longer navigation label alone.
        opening = [actual]
        for item in section.get('sections') or []:
            if item.get('type') != 'heading':
                break
            opening.append(form_title(item.get('text')))
        actual_keys = [verifier._raw_attachment_title_key(x) for x in opening]
        level = 3 if raw_key and raw_key in actual_keys else 2 if any(compatible(verifier, title, x) for x in opening) else 1
        score = max([lexical_similarity(raw_key, key) for key in actual_keys] + [lexical_similarity(title, actual)])
        # An explicitly suffixed sub-form is a candidate for its generic form,
        # but two such candidates remain ambiguous (business / technical etc.).
        if any(re.fullmatch(re.escape(raw_key) + r'(?:商务|技术)(?:部分)?', key) for key in actual_keys if raw_key):
            level = max(level, 2)
        if level > 1 or score >= settings.CONSISTENCY_TITLE_UNMATCHED_THRESHOLD:
            eligible.append((level, score, section))
    if not eligible:
        return {'section': None, 'location_status': 'not_found', 'method': 'title', 'confidence': 'low', 'candidates': []}
    best_level = max(x[0] for x in eligible)
    ranked = sorted((x for x in eligible if x[0] == best_level), key=lambda x: x[1], reverse=True)
    best, second = ranked[0][1], ranked[1][1] if len(ranked) > 1 else 0
    certain = (len(ranked) == 1 if best_level > 1 else
               best >= settings.CONSISTENCY_TITLE_MATCH_THRESHOLD and best - second >= settings.CONSISTENCY_MATCH_MARGIN)
    return {'section': ranked[0][2] if certain else None,
            'location_status': 'matched' if certain else 'ambiguous',
            'method': 'exact_title' if best_level == 3 else 'complete_alias' if best_level == 2 else 'lexical',
            'score': round(best, 4), 'margin': round(best - second, 4),
            'confidence': 'high' if certain else 'unclear',
            'candidates': [{'title': s['title'], 'pages': s.get('pages', []),
                            'locations': verifier._attachment_heading_locations(s)} for _, _, s in ranked[:8]]}
