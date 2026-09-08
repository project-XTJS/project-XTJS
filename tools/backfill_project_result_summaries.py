"""One-time, bounded report reads to populate small project-list metadata.

Run after the migration and deployment of the summary-aware result writer.
No reports or business timestamps are modified. Concurrent result edits are
protected by comparing the object key and update_time before writing metadata.
"""
import argparse
import json
import time

from psycopg2.extras import Json

from app.service.cache_service import get_cache_service
from app.service.postgresql_service import PostgreSQLService
from app.service.project_result_summary import SUMMARY_VERSION, build_project_result_summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--verify-display', action='store_true', help='Compare against the actual display projection, without allowing report writes')
    args = parser.parse_args()
    service = PostgreSQLService()
    with service._get_connection() as conn, conn.cursor() as cursor:
        cursor.execute("""
            SELECT r.project_identifier_id FROM xtjs_result r
            JOIN xtjs_projects p ON p.identifier_id = r.project_identifier_id
            WHERE p.deleted = FALSE
              AND (r.result_summary IS NULL OR r.result_summary->>'version' IS DISTINCT FROM %s)
            ORDER BY r.project_identifier_id
        """, (str(SUMMARY_VERSION),))
        identifiers = [str(row[0]) for row in cursor.fetchall()]
    print(json.dumps({'pending': len(identifiers), 'apply': args.apply}), flush=True)
    if not args.apply:
        return
    for identifier in identifiers:
        for attempt in range(3):
            started = time.monotonic()
            record = service.get_project_result(identifier)
            if not record:
                break
            summary = build_project_result_summary(record.get('result') or {})
            if args.verify_display:
                from app.router.postgresql import _build_project_display_results, _compact_display_results_for_response
                class NoReportWrites:
                    def upsert_project_result_item(self, **kwargs):
                        raise RuntimeError('Report merge needs rebuilding; backfill will not modify reports')
                _, _, display = _build_project_display_results(
                    identifier_id=identifier, result_record=record, db_service=NoReportWrites(),
                )
                compact = _compact_display_results_for_response(display)
                visible = {key: value for key, value in compact.items()
                           if isinstance(value, (dict, list)) or bool(value)}
                has_risk = any(float((value.get('summary') or {}).get('suspicious') or 0) > 0
                               for value in visible.values() if isinstance(value, dict))
                assert summary['result_count'] == len(visible), identifier
                assert summary['has_suspicious'] == has_risk, identifier
                assert summary['result_keys'] == sorted(visible), identifier
                del display, compact, visible
            with service._get_connection() as conn, conn.cursor() as cursor:
                cursor.execute("""
                    UPDATE xtjs_result SET result_summary = %s
                    WHERE project_identifier_id = %s AND update_time = %s
                      AND result_object_key IS NOT DISTINCT FROM %s
                """, (Json(summary), identifier, record['update_time'], record.get('result_object_key')))
                updated = cursor.rowcount == 1
            del record
            if updated:
                cache = get_cache_service()
                if cache.enabled:
                    cache.delete_patterns([cache.key('projects', 'list', '*')])
                print(json.dumps({'project': identifier, 'summary': summary,
                                  'seconds': round(time.monotonic() - started, 3)}), flush=True)
                break
        else:
            raise RuntimeError(f'Concurrent updates prevented backfill for {identifier}; rerun when idle')


if __name__ == '__main__':
    main()
