import unittest
from unittest.mock import patch

from app.service.project_result_summary import build_project_result_summary


class ProjectResultSummaryTests(unittest.TestCase):
    def test_hidden_and_merged_keys_do_not_inflate_completion(self):
        result = {
            'typo_check': {'summary': {'suspicious': 10}},
            'manual_review_results': {},
            'business_itemized_duplicate_check': {},
            'bid_response_duplicate_check': {},
            'duplicate_check': {},
            'business_bid_duplicate_check': {'summary': {'suspicious': 7}},
            'business_bid_duplicate_clusters': {'summary': {'suspicious': 0}},
        }
        self.assertEqual(build_project_result_summary(result), {
            'version': 1, 'result_count': 1, 'has_suspicious': False,
            'result_keys': ['business_bid_duplicate_check'],
        })

    def test_manual_override_wins_over_original_and_merged_risk(self):
        result = {
            'business_bid_duplicate_check': {'summary': {'suspicious': 3}},
            'business_bid_duplicate_clusters': {'summary': {'suspicious': 4}},
            'manual_review_results': {'latest': {
                'business_bid_duplicate_check': {'summary': {'suspicious': 0}},
                'business_bid_format_review': {'summary': {'suspicious': '2'}},
            }},
        }
        summary = build_project_result_summary(result)
        self.assertEqual(summary['result_count'], 2)
        self.assertTrue(summary['has_suspicious'])
        result['manual_review_results']['latest']['business_bid_format_review']['summary']['suspicious'] = 0
        self.assertFalse(build_project_result_summary(result)['has_suspicious'])

    def test_empty_objects_match_browser_presence_without_mutating_report(self):
        report = {'one': {}, 'two': [], 'absent': None, 'disabled': False}
        self.assertEqual(build_project_result_summary(report)['result_count'], 2)
        self.assertEqual(report, {'one': {}, 'two': [], 'absent': None, 'disabled': False})
        self.assertEqual(build_project_result_summary({})['result_count'], 0)

    def test_result_writer_updates_summary_in_same_sql_statement(self):
        from app.service.postgresql_service import PostgreSQLService
        from unittest.mock import Mock
        service = PostgreSQLService()
        cursor = Mock()
        cursor.fetchone.return_value = {}
        with patch('app.service.postgresql_service.document_blob_store.save_project_result', return_value='object'), \
             patch.object(service, '_sanitize_project_result_record', side_effect=lambda record: record):
            service._persist_project_result(cursor, {'identifier_id': 'project'}, {
                'review': {'summary': {'suspicious': 1}},
            })
        sql, values = cursor.execute.call_args.args
        self.assertIn('result_summary = EXCLUDED.result_summary', sql)
        self.assertTrue(values[3].adapted['has_suspicious'])
        self.assertEqual(values[3].adapted['result_count'], 1)


if __name__ == '__main__':
    unittest.main()
