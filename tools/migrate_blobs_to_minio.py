# -*- coding: utf-8 -*-
"""存量大 JSON 迁移脚本：把 xtjs_documents.content / xtjs_result.result 外置到 MinIO。

把历史行的内联大 JSON 写入 MinIO（gzip 压缩），回填对象键引用，并将 JSONB 列置 NULL，
实现数据库瘦身。脚本可重入：已迁移（对象键非空 / 内联为空）的行会被跳过。

用法：
    python tools/migrate_blobs_to_minio.py                # 迁移 content 与 result
    python tools/migrate_blobs_to_minio.py --only documents
    python tools/migrate_blobs_to_minio.py --only results
    python tools/migrate_blobs_to_minio.py --dry-run      # 只统计不写入
    python tools/migrate_blobs_to_minio.py --vacuum       # 迁移后执行 VACUUM (FULL) 回收空间

依赖与既有服务一致：通过 app.config.settings 读取 DATABASE_URL / MinIO 配置，
通过 app.service.document_blob_store 写对象（保证键布局与运行时完全一致）。
"""

import argparse
import sys
from pathlib import Path

import psycopg2
from psycopg2.extras import Json, RealDictCursor

# 确保项目根目录在搜索路径中
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.config.settings import settings  # noqa: E402
from app.service import document_blob_store  # noqa: E402


def _connect():
    return psycopg2.connect(settings.DATABASE_URL)


def migrate_documents(conn, *, dry_run: bool) -> int:
    """迁移 xtjs_documents.content → MinIO，回填 content_object_key，content 置 NULL。"""
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(
            """
            SELECT identifier_id, file_name
            FROM xtjs_documents
            WHERE content IS NOT NULL AND content_object_key IS NULL
            ORDER BY create_time
            """
        )
        rows = cursor.fetchall()

    total = len(rows)
    print(f"[documents] 待迁移行数：{total}")
    if dry_run or total == 0:
        return total

    migrated = 0
    for row in rows:
        identifier_id = str(row["identifier_id"])
        file_name = row.get("file_name")
        # 单独取 content，避免一次性把所有大 JSON 读进内存
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                "SELECT content FROM xtjs_documents WHERE identifier_id = %s",
                (identifier_id,),
            )
            fetched = cursor.fetchone()
        if not fetched or fetched.get("content") is None:
            continue
        content = fetched["content"]

        object_key = document_blob_store.save_document_content(
            content,
            identifier_id=identifier_id,
            file_name=file_name,
        )
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE xtjs_documents
                SET content = NULL, content_object_key = %s, update_time = update_time
                WHERE identifier_id = %s
                """,
                (object_key, identifier_id),
            )
        conn.commit()  # 逐行提交，保证可重入与中断安全
        migrated += 1
        if migrated % 20 == 0 or migrated == total:
            print(f"[documents] 进度 {migrated}/{total}")
    print(f"[documents] 完成，迁移 {migrated} 行")
    return migrated


def migrate_review_contents(conn, *, dry_run: bool) -> int:
    """迁移 xtjs_documents.review_content → MinIO，回填 review_content_object_key，置 NULL。

    仅迁移“有内容”的工作副本（非空对象）；默认空 `{}` 不占空间，直接置 NULL + 不建对象。
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(
            """
            SELECT identifier_id, file_name
            FROM xtjs_documents
            WHERE review_content IS NOT NULL
              AND review_content::text <> '{}'
              AND review_content_object_key IS NULL
            ORDER BY create_time
            """
        )
        rows = cursor.fetchall()

    total = len(rows)
    print(f"[review] 待迁移行数：{total}")
    if dry_run or total == 0:
        return total

    migrated = 0
    for row in rows:
        identifier_id = str(row["identifier_id"])
        file_name = row.get("file_name")
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                "SELECT review_content FROM xtjs_documents WHERE identifier_id = %s",
                (identifier_id,),
            )
            fetched = cursor.fetchone()
        if not fetched or fetched.get("review_content") in (None, {}):
            continue
        review_content = fetched["review_content"]

        object_key = document_blob_store.save_document_review_content(
            review_content,
            identifier_id=identifier_id,
            file_name=file_name,
        )
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE xtjs_documents
                SET review_content = NULL, review_content_object_key = %s, update_time = update_time
                WHERE identifier_id = %s
                """,
                (object_key, identifier_id),
            )
        conn.commit()
        migrated += 1
        if migrated % 20 == 0 or migrated == total:
            print(f"[review] 进度 {migrated}/{total}")
    print(f"[review] 完成，迁移 {migrated} 行")
    return migrated


def migrate_results(conn, *, dry_run: bool) -> int:
    """迁移 xtjs_result.result → MinIO，回填 result_object_key + result_keys，result 置 NULL。"""
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(
            """
            SELECT r.project_identifier_id, p.project_name
            FROM xtjs_result r
            LEFT JOIN xtjs_projects p ON p.identifier_id = r.project_identifier_id
            WHERE r.result IS NOT NULL AND r.result_object_key IS NULL
            ORDER BY r.update_time
            """
        )
        rows = cursor.fetchall()

    total = len(rows)
    print(f"[results] 待迁移行数：{total}")
    if dry_run or total == 0:
        return total

    migrated = 0
    for row in rows:
        pid = str(row["project_identifier_id"])
        project_name = row.get("project_name")
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                "SELECT result FROM xtjs_result WHERE project_identifier_id = %s",
                (pid,),
            )
            fetched = cursor.fetchone()
        if not fetched or fetched.get("result") is None:
            continue
        result = fetched["result"]

        object_key = document_blob_store.save_project_result(
            result,
            project_name=project_name,
            project_identifier_id=pid,
        )
        result_keys = sorted(result.keys()) if isinstance(result, dict) else []
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE xtjs_result
                SET result = NULL, result_object_key = %s, result_keys = %s, update_time = update_time
                WHERE project_identifier_id = %s
                """,
                (object_key, Json(result_keys), pid),
            )
        conn.commit()
        migrated += 1
        print(f"[results] 进度 {migrated}/{total}")
    print(f"[results] 完成，迁移 {migrated} 行")
    return migrated


def run_vacuum() -> None:
    """VACUUM (FULL) 回收 JSONB 置空后的物理空间（需独立 autocommit 连接，不能在事务内）。"""
    conn = _connect()
    try:
        conn.autocommit = True
        with conn.cursor() as cursor:
            print("[vacuum] VACUUM (FULL, ANALYZE) xtjs_documents ...")
            cursor.execute("VACUUM (FULL, ANALYZE) xtjs_documents")
            print("[vacuum] VACUUM (FULL, ANALYZE) xtjs_result ...")
            cursor.execute("VACUUM (FULL, ANALYZE) xtjs_result")
        print("[vacuum] 完成")
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="存量大 JSON 迁移到 MinIO")
    parser.add_argument(
        "--only",
        choices=["documents", "reviews", "results"],
        default=None,
        help="仅迁移指定类型；默认三者都迁移",
    )
    parser.add_argument("--dry-run", action="store_true", help="只统计待迁移行数，不写入")
    parser.add_argument("--vacuum", action="store_true", help="迁移后执行 VACUUM (FULL)")
    args = parser.parse_args()

    conn = _connect()
    try:
        if args.only in (None, "documents"):
            migrate_documents(conn, dry_run=args.dry_run)
        if args.only in (None, "reviews"):
            migrate_review_contents(conn, dry_run=args.dry_run)
        if args.only in (None, "results"):
            migrate_results(conn, dry_run=args.dry_run)
    finally:
        conn.close()

    if args.vacuum and not args.dry_run:
        run_vacuum()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
