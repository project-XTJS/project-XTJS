"""Read physical underlines as auxiliary evidence; stored OCR stays immutable."""
from __future__ import annotations
import hashlib
import json
from pathlib import Path
import tempfile
import threading
from .underline_projection import VERSION, EVIDENCE_KEY

_lock = threading.Lock()
_cache = Path('/tmp/xtjs-template-underlines-v1')

def native_spans(page):
    """Only short horizontal strokes directly below characters, excluding table edges."""
    horizontals, verticals = [], []
    for drawing in page.get_drawings():
        for item in drawing['items']:
            if item[0] == 'l':
                a,b=item[1:3]
                if abs(a.y-b.y)<=1 and abs(a.x-b.x)>=3:horizontals.append((min(a.x,b.x), a.y, max(a.x,b.x)))
                elif abs(a.x-b.x)<=1 and abs(a.y-b.y)>=5:verticals.append((a.x,min(a.y,b.y),max(a.y,b.y)))
            elif item[0] == 're':
                rect=item[1]
                if rect.height<=2 and rect.width>=3:horizontals.append((rect.x0,rect.y0,rect.x1))
                else:
                    verticals.extend([(rect.x0,rect.y0,rect.y1),(rect.x1,rect.y0,rect.y1)])
    raw=page.get_text('rawdict');chars=[]
    for block in raw['blocks']:
        for line in block.get('lines',[]):
            for span in line.get('spans',[]):
                for char in span['chars']:chars.append(char)
    spans=[]
    for x0,y,x1 in horizontals:
        if any(abs(x-x0)<2 or abs(x-x1)<2 for x,top,bottom in verticals if top-2<=y<=bottom+2):continue
        selected=[]
        for char in chars:
            a,top,b,bottom=char['bbox'];baseline=char['origin'][1]
            if x0-1 <= (a+b)/2 <= x1+1 and 0<=y-baseline<=max(4,(bottom-top)*.25):selected.append(char)
        if selected:
            selected.sort(key=lambda c:c['bbox'][0]);text=''.join(c['c'] for c in selected)
            spans.append({'text':text,'bbox':[x0,min(c['bbox'][1] for c in selected),x1,y+1],'coordinate_system':'pdf_points','source':'native_pdf_underline'})
    return spans, len(chars)

def scanned_spans(page, local_ocr):
    import cv2
    import numpy as np
    import fitz
    pix=page.get_pixmap(matrix=fitz.Matrix(2,2),colorspace=fitz.csGRAY)
    gray=np.frombuffer(pix.samples,dtype=np.uint8).reshape(pix.height,pix.width)
    binary=cv2.threshold(gray,180,255,cv2.THRESH_BINARY_INV)[1]
    horizontal=cv2.morphologyEx(binary,cv2.MORPH_OPEN,cv2.getStructuringElement(cv2.MORPH_RECT,(45,1)))
    vertical=cv2.morphologyEx(binary,cv2.MORPH_OPEN,cv2.getStructuringElement(cv2.MORPH_RECT,(1,18)))
    contours=cv2.findContours(horizontal,cv2.RETR_EXTERNAL,cv2.CHAIN_APPROX_SIMPLE)[0]
    spans=[];issues=[]
    for contour in contours:
        x,y,w,h=cv2.boundingRect(contour)
        if h>5 or w<45:continue
        if any(np.any(vertical[max(0,y-3):y+5,max(0,edge-3):edge+4]) for edge in (x,x+w-1)):continue
        # A line alone may be a blank. Read the narrow strip above it, then
        # require the fine OCR box itself to sit on the line.
        top=max(0,y-48);crop=gray[top:y+3,x:x+w]
        if local_ocr is None:
            issues.append('扫描横线需要细粒度文字定位');continue
        with tempfile.NamedTemporaryFile(suffix='.png') as tmp:
            cv2.imwrite(tmp.name,crop);result=local_ocr(tmp.name)
        if not result.get('items') and np.count_nonzero(binary[top:max(top,y-3),x:x+w]) > w:
            issues.append('横线上存在笔画但文字未稳定识别')
        for item in result.get('items',[]):
            box=item.get('bbox') or [];text=str(item.get('text') or '').strip()
            if len(box)!=4 or not text:continue
            if 0 <= y-top-box[3] <= 12:
                # Whole text box must be covered; a partially underlined OCR box
                # cannot be split into invented character coordinates.
                spans.append({'text':text,'bbox':[(x+box[0])/2,(top+box[1])/2,(x+box[2])/2,(top+box[3])/2], 'coordinate_system':'pdf_points','source':'scanned_line_fine_ocr'})
    return {'status':'unclear' if issues else 'ready','spans':spans,'issues':list(dict.fromkeys(issues))}

def build_pdf_underline_evidence(pdf_data, pages, local_ocr=None):
    import fitz
    result={'version':VERSION,'pages':{}}
    with fitz.open(stream=pdf_data,filetype='pdf') as pdf:
        for number in sorted(set(pages)):
            if not 1<=number<=len(pdf):
                result['pages'][str(number)]={'status':'unclear','issues':['原件页码越界'],'spans':[]};continue
            page=pdf[number-1];spans,count=native_spans(page)
            scanned = not count or (count < 100 and bool(page.get_images()))
            entry = scanned_spans(page,local_ocr) if scanned else {'status':'ready','spans':spans,'issues':[]}
            result['pages'][str(number)]=entry
    return result

def evidence_for(payload, pages):
    if EVIDENCE_KEY in payload:return payload[EVIDENCE_KEY]
    import multiprocessing
    if multiprocessing.current_process().name != 'MainProcess':
        return {'version':VERSION,'pages':{str(p):{'status':'unclear','spans':[],'issues':['原件横线证据未预加载']} for p in pages}}
    source=payload.get('_template_source') or {}
    url=source.get('file_url')
    if not url:return {'version':VERSION,'pages':{}}
    key=hashlib.sha256((VERSION+str(url)+str(sorted(set(pages)))).encode()).hexdigest()
    _cache.mkdir(parents=True,exist_ok=True);cache=_cache/(key+'.json')
    with _lock:
        if cache.exists():return json.loads(cache.read_text())
        try:
            from app.service.minio_service import MinioService
            store=MinioService()
            if str(url).startswith('minio://'):bucket,obj=store.bucket_and_object_from_file_url(url)
            else:bucket,obj=store.bucket_and_object_from_presigned_url(url)
            data,_=store.get_object_bytes(obj,bucket)
            def fine(path):
                from app.service.analysis_service import get_analysis_service
                return get_analysis_service().extract_text_boxes_without_layout(path)
            result=build_pdf_underline_evidence(data,pages,local_ocr=fine)
            # Never cache an unavailable service / incomplete observation as success.
            if all(v['status']=='ready' for v in result['pages'].values()):
                temp=cache.with_suffix('.tmp');temp.write_text(json.dumps(result,ensure_ascii=False));temp.replace(cache)
            return result
        except Exception:
            return {'version':VERSION,'pages':{str(p):{'status':'unclear','spans':[],'issues':['原件横线证据读取失败，待复核']} for p in pages}}
