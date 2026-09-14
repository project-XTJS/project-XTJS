"""Field-bounded bidder identity extraction. Never correct OCR characters."""
import re
from difflib import SequenceMatcher

ANCHORS = ('投标单位名称','投标人名称','参选人名称','供应商名称','响应人名称','参选单位名称','投标单位','投标人','参选人','响应人','供应商','单位名称','公司名称','企业名称')
ORG_SUFFIX = r'(?:有限责任公司|股份有限公司|集团有限公司|有限公司|公司|报社|事务所|研究院|研究所|大学|学校|中学|中心|合作社|协会|委员会)'
ORG = re.compile(r'^[A-Za-z0-9\u4e00-\u9fff（）()·&.\-]{2,100}?' + ORG_SUFFIX)
FIELD = re.compile(r'^(?:'+'|'.join(ANCHORS)+r')(?:[（(](?:名称|盖章|公章|签章)[）)])?\s*[：:]?\s*')
GENERIC = {'餐饮管理','餐饮','科技','智能科技','实业发展','实业','电子商务','商贸','贸易','服务','上海','深圳'}

def comparison_key(text):
    # Formatting only: parentheses' contents and every recognized letter remain.
    return re.sub(r'[^0-9A-Za-z\u4e00-\u9fff]', '', str(text or ''))

def name_value(text):
    text = re.sub(r'\s+', '', str(text or '')).strip('：:|；;,，')
    text = FIELD.sub('', text, count=1)
    text = re.sub(r'^[（(][^（）()]{0,12}(?:盖章|公章|签章)[^（）()]{0,8}[）)][：:]?', '', text)
    text = text.split('|', 1)[0]
    match = ORG.match(text)
    if not match:return None
    value = match.group(0)
    branch = re.match(r'^[A-Za-z0-9\u4e00-\u9fff]{0,24}?(?:分公司|分行|支行|营业部|分院|分所)', text[match.end():])
    if branch: value += branch.group(0)
    if value.count('(') != value.count(')') or value.count('（') != value.count('）'):return None
    if any(word in value for word in ('填写','投标人名称','供应商名称','示例','附件','目录','法定代表人','注册资本','登记机关','统一社会信用代码')):return None
    return value

def sufficient_name(text):
    value = name_value(text)
    if not value:return False
    stem = re.sub(ORG_SUFFIX+r'$', '', comparison_key(value))
    return len(stem)>=2 and stem not in GENERIC

def company_score(name,seal):
    candidate = name_value(seal)
    left,right=comparison_key(name),comparison_key(candidate or seal)
    if not left or not right:return 0.0
    if left==right and sufficient_name(name) and sufficient_name(candidate):return 1.0
    return min(SequenceMatcher(None,left,right).ratio(),0.999)

def _lines(text):
    # HTML table boundaries are structural separators, not OCR text corrections.
    text=re.sub(r'</(?:tr|p|div)>','\n',str(text),flags=re.I)
    text=re.sub(r'</t[dh]>',' | ',text,flags=re.I)
    text=re.sub(r'<[^>]*>','',text)
    return [v.strip() for v in text.splitlines() if v.strip()]

def identify(container,sections):
    candidates=[]
    directory_pages={s.get('page') for s in sections if re.sub(r'\s+','',s.get('text','')) in {'目录','目次'}}
    certificate_pages={s.get('page') for s in sections if any(x in s.get('text','') for x in ('营业执照','事业单位法人证书'))}
    page_texts={}
    for section in sections:
        page_texts.setdefault(section.get('page'),[]).append(section.get('text',''))
    historical_pages={page for page,texts in page_texts.items() if
        (re.search(r'甲方\s*[：:（(]', '\n'.join(texts)) and re.search(r'乙方\s*[：:（(]', '\n'.join(texts))) or
        any(re.fullmatch(r'.{0,25}(?:采购合同|服务合同|购销合同|合同协议书|审计报告|财务报表)', re.sub(r'\s+','',text)) for text in texts)}
    for index,section in enumerate(sections):
        page=section.get('page');text=section.get('text','')
        if section.get('type')=='seal' or page in directory_pages or page in historical_pages:continue
        lines=_lines(text)
        for pos,line in enumerate(lines):
            if re.search(r'\.{3,}|…{2,}|[·•]{3,}',line):continue
            compact=re.sub(r'\s+','',line)
            matched=FIELD.match(compact)
            rank=100 if isinstance(page,int) and page<=3 else 60
            # Generic entity fields in certificates/financial exhibits are not bidder declarations.
            if rank!=100 and re.match(r'^(?:单位名称|公司名称|企业名称)',compact):rank=30
            if not matched:
                if page in certificate_pages and re.match(r'^名称[：:|]',compact):
                    matched=re.match(r'^名称[：:|]+',compact);rank=30
                else:continue
            raw_value=compact[matched.end():].lstrip('|：:')
            value=name_value(raw_value)
            if not value and not raw_value:
                if pos+1<len(lines):value=name_value(lines[pos+1])
                elif index+1<len(sections):
                    nxt=sections[index+1];a=section.get('bbox');b=nxt.get('bbox')
                    if nxt.get('page')==page and a and b and len(a)==len(b)==4:
                        # Bboxes supplied by VerificationChecker are x,y,width,height.
                        aligned=abs(a[0]-b[0])<=max(a[2],b[2]) and 0<=b[1]-a[1]<=max(3*a[3],60)
                        if aligned:value=name_value(nxt.get('text',''))
            if value:candidates.append({'name':value,'page':page,'text':line,'rank':rank})
    for key in ('bidder_name','company_name','supplier_name'):
        value=name_value(container.get(key))
        if value:candidates.append({'name':value,'page':None,'text':str(container[key]),'rank':10})
    if not candidates:return {'status':'pending','name':None,'reason':'bidder_field_not_found','candidates':[]}
    rank=max(c['rank'] for c in candidates);best=[c for c in candidates if c['rank']==rank]
    keys={comparison_key(c['name']) for c in best}
    winner_key = comparison_key(best[0]['name'])
    # A lower-page field with a near-but-different name is conflicting evidence, not a spelling correction.
    conflicts = [c for c in candidates if c['rank'] >= 60 and comparison_key(c['name']) != winner_key and SequenceMatcher(None, winner_key, comparison_key(c['name'])).ratio() >= 0.75]
    if len(keys)!=1 or conflicts:return {'status':'pending','name':None,'reason':'conflicting_bidder_fields','candidates':candidates}
    winner=best[0]['name']
    return {'status':'resolved' if sufficient_name(winner) else 'pending','name':winner,'reason':'explicit_field' if sufficient_name(winner) else 'incomplete_identity','candidates':candidates}
