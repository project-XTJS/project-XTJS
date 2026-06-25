# -*- coding: utf-8 -*-
"""人名抽取 A/B 对比:纯规则(段落) vs 规则+NER(LAC)补漏。

用法(在后端环境,需 paddlenlp + 可联网下载 LAC 模型):
    python tools/ab_personnel_ner.py                 # 自动挑几份投标文件
    python tools/ab_personnel_ner.py 关键词           # 文件名含该关键词的文档

输出每份文档:规则命中人名 / NER 额外补出人名 / 仅NER(疑似噪声),便于判断是否值得开启。
"""
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2

from app.config import settings as settings_module
from app.service.analysis.bid_document_review import BidDocumentReviewService

DB = "postgresql://admin:password@localhost:5432/xtjs_db"


def load_docs(keyword: str | None):
    conn = psycopg2.connect(DB)
    cur = conn.cursor()
    like = f"%{keyword}%" if keyword else "%标%"
    cur.execute(
        "SELECT identifier_id, file_name, document_type, content "
        "FROM xtjs_documents WHERE deleted IS NOT TRUE AND file_name LIKE %s "
        "ORDER BY update_time DESC LIMIT 6",
        (like,),
    )
    rows = cur.fetchall()
    cur.close(); conn.close()
    return rows


def sections_of(content):
    if content is None:
        return []
    c = content if isinstance(content, (dict, list)) else json.loads(content)
    if not isinstance(c, dict):
        return []
    out = []
    for s in c.get("layout_sections") or []:
        if not isinstance(s, dict):
            continue
        out.append({
            "text": s.get("text") or "",
            "page": s.get("page"),
            "bbox": s.get("bbox") or s.get("bbox_ocr"),
            "title": s.get("title") or "",
        })
    return out


def names_of(entries):
    return sorted({str(e.get("name") or "").strip() for e in entries if e.get("name")})


def run(svc, record, sections, ner_on):
    settings_module.settings.PERSONNEL_NER_ENABLED = ner_on
    entries = svc._extract_personnel_entries(record=record, sections=sections, tables=[])
    return names_of(entries)


def main():
    keyword = sys.argv[1] if len(sys.argv) > 1 else None
    docs = load_docs(keyword)
    if not docs:
        print("没找到文档"); return
    svc = BidDocumentReviewService()
    tot_rule = tot_add = tot_only = 0
    for ident, fname, dtype, content in docs:
        sections = sections_of(content)
        if not sections:
            continue
        record = {"identifier_id": ident, "file_name": fname, "document_type": dtype}
        rule = run(svc, record, sections, False)
        both = run(svc, record, sections, True)
        added = sorted(set(both) - set(rule))     # NER 补出的新名字
        print("=" * 70)
        print(f"文档: {fname[:42]} | 段落数 {len(sections)}")
        print(f"  规则命中({len(rule)}): {rule}")
        print(f"  NER 额外补出({len(added)}): {added}")
        tot_rule += len(rule); tot_add += len(added)
    print("=" * 70)
    print(f"合计: 规则命中 {tot_rule} | NER 额外补出 {tot_add}")
    print("判断: 补出的多为真实人员→值得开;多为无关人名→需收紧或仅在特定页开。")


if __name__ == "__main__":
    main()
