"""One explicit local typo engine for duplicate evidence only."""
import json
import urllib.request
from difflib import SequenceMatcher
from app.config.settings import settings
from app.service.typo_runtime.contract import TypoUnavailable, VERSION, chunks

class DuplicateTypoService:
    def check_text_snippets_for_typos(self, snippets):
        issues=[]
        for snippet in snippets:
            text=str(snippet.get('text') or '')
            seen=set()
            for offset,part in chunks(text):
                request=urllib.request.Request(settings.TYPO_SERVICE_URL.rstrip('/')+'/check',
                    data=json.dumps({'text':part},ensure_ascii=False).encode(),headers={'Content-Type':'application/json'})
                try:
                    with urllib.request.urlopen(request,timeout=settings.TYPO_CLIENT_TIMEOUT_SECONDS) as response:payload=json.loads(response.read())
                except (OSError,ValueError) as exc:raise TypoUnavailable('错别字检查未完成：模型暂不可用，请重试') from exc
                if payload.get('status')!='completed':raise TypoUnavailable('错别字检查未完成')
                for issue in payload.get('issues',[]):
                    item=dict(issue);start=offset+item['start'];end=offset+item['end']
                    if text[start:end]!=item['original']:raise TypoUnavailable('错别字定位校验失败')
                    key=(start,end,item['replacement'])
                    if key in seen:continue
                    seen.add(key)
                    item.update(start=start,end=end,position=start,text=text,page=snippet.get('page'),bbox=snippet.get('bbox'),side=snippet.get('side'),model=payload.get('model'),rule_version=VERSION)
                    item['locations']=[{'page':snippet.get('page'),'bbox':snippet.get('bbox'),'text':item['highlight_text'],'highlight_phrases':[item['highlight_text']]}]
                    issues.append(item)
        return issues


def common_edits(left_text,right_text,left_issues,right_issues):
    """Only the same edit at corresponding original spans is shared evidence."""
    blocks=SequenceMatcher(None,left_text,right_text,autojunk=False).get_matching_blocks()
    right_index={(r.get('start'),r.get('end'),r.get('replacement')):r for r in right_issues}
    result=[]
    for item in left_issues:
        start,end=item.get('start'),item.get('end')
        if not isinstance(start,int) or not isinstance(end,int):continue
        for block in blocks:
            if block.a<=start and end<=block.a+block.size:
                other=right_index.get((block.b+start-block.a,block.b+end-block.a,item.get('replacement')))
                if other is not None:result.extend([item,other])
                break
    return result
