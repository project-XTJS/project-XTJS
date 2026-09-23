"""Read physical underlines as auxiliary evidence; stored OCR stays immutable."""
from __future__ import annotations
import hashlib
import json
import logging
import os
from pathlib import Path
import tempfile
import threading
from contextlib import contextmanager
from typing import Any

import fcntl

from app.config.settings import settings
from .underline_projection import VERSION, EVIDENCE_KEY

_CACHE_VERSION = "pdf-underline-evidence-v2"
_source_bytes: dict[str, bytes] = {}
_source_lock = threading.Lock()
logger = logging.getLogger(__name__)


def _cache_root() -> Path:
    return Path(settings.BUSINESS_REVIEW_EVIDENCE_CACHE_ROOT)


@contextmanager
def _page_lock(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


def _data_node(payload: dict[str, Any]) -> dict[str, Any]:
    data = payload.get("data")
    return data if isinstance(data, dict) else payload


def _pdf_ocr_lines(payload: dict[str, Any], page_number: int) -> list[dict[str, Any]]:
    """Return only OCR line boxes already projected to PDF coordinates."""
    data = _data_node(payload)
    top_level_space = str(data.get("bbox_coordinate_space") or "").lower()
    result: list[dict[str, Any]] = []
    for section in data.get("layout_sections") or []:
        if not isinstance(section, dict) or section.get("page") != page_number:
            continue
        section_space = str(section.get("coordinate_system") or top_level_space).lower()
        for line in section.get("lines") or []:
            if not isinstance(line, dict):
                continue
            text = str(line.get("text") or "").strip()
            bbox = line.get("bbox") or line.get("box")
            coordinate_space = str(line.get("coordinate_system") or section_space).lower()
            if (
                text
                and isinstance(bbox, (list, tuple))
                and len(bbox) >= 4
                and all(isinstance(value, (int, float)) for value in bbox[:4])
                and coordinate_space in {"pdf", "pdf_point", "pdf_points"}
            ):
                result.append({"text": text, "bbox": [float(value) for value in bbox[:4]]})
    return result


def _ocr_fingerprint(lines: list[dict[str, Any]]) -> str:
    encoded = json.dumps(lines, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()

def native_text_conflicted(text):
    value=str(text or '')
    controls=sum(
        1 for char in value
        if char in {'\ufffd', '\x00'} or (ord(char) < 32 and char not in '\n\r\t')
    )
    suspicious=sum(char in '<>=&^`' for char in value)
    return bool(value) and (controls > 0 or suspicious >= 2)

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

def scanned_spans(page, ocr_lines=None, local_ocr=None):
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
        items=[]
        line_x0,line_y,line_x1=x/2,y/2,(x+w)/2
        for item in ocr_lines or []:
            box=item.get('bbox') or []
            if len(box)!=4:continue
            center=(box[0]+box[2])/2
            if line_x0-1 <= center <= line_x1+1 and 0 <= line_y-box[3] <= 6:
                items.append(item)
        # ``local_ocr`` is retained only for isolated tests and explicit tools.
        # Project business review never supplies it, so this path cannot start VLM.
        if not items and local_ocr is not None:
            with tempfile.NamedTemporaryFile(suffix='.png') as tmp:
                cv2.imwrite(tmp.name,crop);items=list((local_ocr(tmp.name) or {}).get('items') or [])
        if not items and np.count_nonzero(binary[top:max(top,y-3),x:x+w]) > w:
            issues.append('横线上存在笔画但文字未稳定识别')
        for item in items:
            box=item.get('bbox') or [];text=str(item.get('text') or '').strip()
            if len(box)!=4 or not text:continue
            if item in (ocr_lines or []):
                spans.append({'text':text,'bbox':list(box[:4]),'coordinate_system':'pdf_points','source':'stored_ocr_line'})
            elif 0 <= y-top-box[3] <= 12:
                # Whole text box must be covered; a partially underlined OCR box
                # cannot be split into invented character coordinates.
                spans.append({'text':text,'bbox':[(x+box[0])/2,(top+box[1])/2,(x+box[2])/2,(top+box[3])/2], 'coordinate_system':'pdf_points','source':'scanned_line_fine_ocr'})
    return {'status':'unclear' if issues else 'ready','spans':spans,'issues':list(dict.fromkeys(issues))}

def build_pdf_underline_evidence(pdf_data, pages, local_ocr=None, ocr_lines_by_page=None):
    import fitz
    result={'version':VERSION,'pages':{}}
    with fitz.open(stream=pdf_data,filetype='pdf') as pdf:
        for number in sorted(set(pages)):
            if not 1<=number<=len(pdf):
                result['pages'][str(number)]={'status':'unclear','issues':['原件页码越界'],'spans':[]};continue
            page=pdf[number-1];spans,count=native_spans(page)
            native_text=page.get_text('text') or ''
            native_conflict=native_text_conflicted(native_text)
            scanned = not count or (count < 100 and bool(page.get_images()))
            if native_conflict:
                entry={'status':'unclear','spans':spans,'issues':['原生PDF文字层包含异常字符，与页面图像可能冲突']}
            else:
                entry = scanned_spans(
                    page,
                    (ocr_lines_by_page or {}).get(number) or [],
                    local_ocr=local_ocr,
                ) if scanned else {'status':'ready','spans':spans,'issues':[]}
            result['pages'][str(number)]=entry
    return result

def source_pdf_bytes(payload):
    """Load the immutable source PDF once for all review-time image evidence."""
    import multiprocessing
    if multiprocessing.current_process().name != 'MainProcess':
        raise RuntimeError('原件图像证据只能在主进程生成')
    source=payload.get('_template_source') or {}
    url=source.get('file_url')
    if not url:
        raise ValueError('缺少原件文件地址')
    declared_fingerprint=source.get('content_checksum') or source.get('sha256')
    from app.service.minio_service import MinioService
    store=MinioService()
    if str(url).startswith('minio://'):bucket,obj=store.bucket_and_object_from_file_url(url)
    else:bucket,obj=store.bucket_and_object_from_presigned_url(url)
    with _source_lock:
        data=_source_bytes.get(str(declared_fingerprint)) if declared_fingerprint else None
    if data is None:
        data,_=store.get_object_bytes(obj,bucket)
        cache_key = str(declared_fingerprint or hashlib.sha256(data).hexdigest())
        with _source_lock:
            if len(_source_bytes) >= 4:
                _source_bytes.pop(next(iter(_source_bytes)))
            _source_bytes[cache_key]=data
    return data


def evidence_for(payload, pages):
    requested=sorted(set(int(p) for p in (pages or []) if p))
    existing=payload.get(EVIDENCE_KEY) if isinstance(payload.get(EVIDENCE_KEY),dict) else {'version':VERSION,'pages':{}}
    existing_pages=existing.get('pages') if isinstance(existing.get('pages'),dict) else {}
    missing=[p for p in requested if str(p) not in existing_pages and p not in existing_pages]
    if not missing:return existing
    try:
        data=source_pdf_bytes(payload)
    except Exception:
        merged=dict(existing_pages)
        merged.update({str(p):{'status':'unclear','spans':[],'issues':['原件横线证据读取失败，待复核']} for p in missing})
        return {'version':VERSION,'pages':merged}

    content_fingerprint=hashlib.sha256(data).hexdigest()
    merged=dict(existing_pages)
    cache_root=_cache_root()
    cache_root.mkdir(parents=True,exist_ok=True)
    for page_number in missing:
        lines=_pdf_ocr_lines(payload,page_number)
        key_payload=f"{_CACHE_VERSION}:{VERSION}:{content_fingerprint}:{page_number}:{_ocr_fingerprint(lines)}"
        key=hashlib.sha256(key_payload.encode()).hexdigest()
        cache=cache_root/(key+'.json')
        lock=cache_root/(key+'.lock')
        with _page_lock(lock):
            if cache.exists():
                try:
                    merged[str(page_number)]=json.loads(cache.read_text(encoding='utf-8'))
                    logger.info("business evidence cache hit page=%s cache_key=%s", page_number, key[:12])
                    continue
                except (OSError,ValueError,TypeError):
                    pass
            try:
                result=build_pdf_underline_evidence(
                    data,[page_number],ocr_lines_by_page={page_number:lines}
                )
                entry=result['pages'][str(page_number)]
                temp=cache.with_suffix(f'.{os.getpid()}.tmp')
                temp.write_text(json.dumps(entry,ensure_ascii=False),encoding='utf-8')
                temp.replace(cache)
                merged[str(page_number)]=entry
                logger.info(
                    "business evidence cache miss page=%s cache_key=%s status=%s",
                    page_number, key[:12], entry.get("status"),
                )
            except Exception:
                merged[str(page_number)]={'status':'unclear','spans':[],'issues':['原件横线证据读取失败，待复核']}
    return {'version':VERSION,'pages':merged}
