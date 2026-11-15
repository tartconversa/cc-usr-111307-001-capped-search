"""采集器判定与持久化的单元测试。"""
from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from src.collector.client import parse_providers, parse_search, ProtocolError
from src.collector.engine import Outcome, classify
from src.collector.store import Store, build_query

PROVIDERS = ('甲', '乙')


class ClassifyTests(unittest.TestCase):
    def test_total_equals_cap_with_full_response_is_complete(self):
        # total 恰好等于 cap 并非天然截断：实际收齐即完整。
        verdict = classify(10, 12, None, PROVIDERS, total=2, cap=2, received=2)
        self.assertIs(verdict.outcome, Outcome.COMPLETE)

    def test_zero_records_is_complete(self):
        verdict = classify(10, 12, None, PROVIDERS, total=0, cap=2, received=0)
        self.assertIs(verdict.outcome, Outcome.COMPLETE)

    def test_truncated_wide_range_splits_time(self):
        verdict = classify(10, 14, None, PROVIDERS, total=5, cap=2, received=2)
        self.assertIs(verdict.outcome, Outcome.SPLIT_TIME)
        self.assertEqual(verdict.children,
                         ((10, 12, None), (12, 14, None)))

    def test_truncated_single_tick_splits_providers(self):
        verdict = classify(10, 11, None, PROVIDERS, total=5, cap=2, received=2)
        self.assertIs(verdict.outcome, Outcome.SPLIT_PROVIDER)
        self.assertEqual(verdict.children,
                         ((10, 11, '甲'), (10, 11, '乙')))

    def test_truncated_single_tick_single_provider_is_gap(self):
        verdict = classify(10, 11, '甲', PROVIDERS, total=4, cap=2, received=2)
        self.assertIs(verdict.outcome, Outcome.GAP)

    def test_claim_below_total_is_conflict_not_complete(self):
        # total <= cap 却缺条：声明与响应冲突，不能标完整。
        verdict = classify(10, 11, None, PROVIDERS, total=2, cap=5, received=1)
        self.assertIs(verdict.outcome, Outcome.ERROR)

    def test_response_exceeding_total_or_cap_is_error(self):
        self.assertIs(classify(10, 11, None, PROVIDERS, 1, 2, 2).outcome,
                      Outcome.ERROR)
        self.assertIs(classify(10, 11, None, PROVIDERS, 3, 2, 3).outcome,
                      Outcome.ERROR)

    def test_single_tick_no_providers_is_gap(self):
        verdict = classify(10, 11, None, (), total=3, cap=2, received=2)
        self.assertIs(verdict.outcome, Outcome.GAP)


class ProtocolParseTests(unittest.TestCase):
    def test_parse_search_ok(self):
        response = parse_search(
            {'total': 1, 'cap': 2, 'generation': 7,
             'records': [{'id': 'a', 'registered_at': 10}]})
        self.assertEqual((response.total, response.cap, response.generation), (1, 2, 7))

    def test_parse_search_rejects_bad_shapes(self):
        for payload in (
            {'total': 1, 'cap': 2, 'records': []},
            {'total': 1, 'cap': 2, 'generation': 1, 'records': [{'registered_at': 1}]},
            {'total': 1, 'cap': 2, 'generation': 1,
             'records': [{'id': 'a', 'registered_at': 'x'}]},
            {'total': -1, 'cap': 2, 'generation': 1, 'records': []},
            {'total': 0, 'cap': 0, 'generation': 1,
             'records': [{'id': 'a', 'registered_at': 1}]},
        ):
            with self.assertRaises(ProtocolError):
                parse_search(payload)

    def test_parse_providers_rejects_duplicates(self):
        with self.assertRaises(ProtocolError):
            parse_providers({'providers': ['甲', '甲'], 'generation': 1})
        names, generation = parse_providers({'providers': ['甲', '乙'],
                                             'generation': 3})
        self.assertEqual((names, generation), (('甲', '乙'), 3))


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = Store(str(Path(self.tmp.name) / 'work.db'))

    def tearDown(self):
        self.store.close()
        self.tmp.cleanup()

    def _job(self):
        return self.store.get_or_create_job('http://x', 10, 13, 1, ['甲', '乙'])

    def _new_running_slice(self, job):
        import time
        with self.store.conn:
            cur = self.store.conn.execute(
                'INSERT INTO slices(job_id,parent_id,start_tick,end_tick,'
                'provider,depth,state,created_at,updated_at) '
                "VALUES(?,?,?,?,?,?,'pending',?,?)",
                (job.id, None, 10, 11, None, 0, time.time(), time.time()))
            slice_id = cur.lastrowid
        return self.store.claim_slice(job.id, 'w', 3).id

    def test_job_idempotent_and_root_slice(self):
        first = self._job()
        second = self._job()
        self.assertEqual(first.id, second.id)
        slices = self.store.list_slices(first.id)
        self.assertEqual(len(slices), 1)
        self.assertEqual((slices[0].start_tick, slices[0].end_tick,
                          slices[0].state), (10, 13, 'pending'))

    def test_claim_is_exclusive_and_reclaimable(self):
        job = self._job()
        claimed = self.store.claim_slice(job.id, 'w1', max_attempts=3)
        self.assertIsNotNone(claimed)
        self.assertIsNone(self.store.claim_slice(job.id, 'w2', max_attempts=3))
        self.assertEqual(self.store.reclaim_stale(job.id, 'startup'), 1)
        again = self.store.claim_slice(job.id, 'w2', max_attempts=3)
        self.assertIsNotNone(again)
        self.assertEqual(again.id, claimed.id)
        self.assertEqual(again.attempts, 2)

    def test_complete_persists_records_and_provenance(self):
        job = self._job()
        slc = self.store.claim_slice(job.id, 'w1', 3)
        records = [{'id': 'a', 'registered_at': 10, 'title': 'A'},
                   {'id': 'b', 'registered_at': 11, 'title': 'B'}]
        self.store.complete_slice(slc.id, 2, 2, 2, 1, records)
        self.assertEqual(self.store.get_slice(slc.id).state, 'complete')
        self.assertEqual(self.store.count_records(job.id), 2)
        sources = self.store.sources_for_record(job.id, 'a')
        self.assertEqual(len(sources), 1)
        self.assertEqual(sources[0]['request_query'], build_query(10, 13, None))
        self.assertEqual(sources[0]['generation'], 1)

    def test_duplicate_id_from_second_slice_merges_without_overwrite(self):
        job = self._job()
        first = self.store.claim_slice(job.id, 'w1', 3)
        self.store.complete_slice(
            first.id, 1, 1, 2, 1,
            [{'id': 'a', 'registered_at': 10, 'title': '原版'}])
        # 手工再造一个父切片模拟拆分后的第二来源。
        child_ids = self.store.split_slice(
            self._new_running_slice(job), [(10, 11, '甲'), (10, 11, '乙')])
        second = self.store.claim_slice(job.id, 'w2', 3)
        self.assertIn(second.id, child_ids)
        conflicts = self.store.complete_slice(
            second.id, 1, 1, 2, 1,
            [{'id': 'a', 'registered_at': 10, 'title': '被改写'}])
        self.assertEqual(conflicts, ['a'])
        # 原始内容不被覆盖。
        row = self.store.conn.execute(
            'SELECT payload_json FROM records WHERE job_id=? AND rid=?',
            (job.id, 'a')).fetchone()
        self.assertEqual(json.loads(row['payload_json'])['title'], '原版')
        rows = self.store.list_conflicts(job.id)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['rid'], 'a')
        # 两条来源请求都保留。
        self.assertEqual(len(self.store.sources_for_record(job.id, 'a')), 2)

    def test_same_id_same_content_is_no_conflict(self):
        job = self._job()
        first = self.store.claim_slice(job.id, 'w1', 3)
        record = {'id': 'a', 'registered_at': 10, 'title': '同'}
        self.store.complete_slice(first.id, 1, 1, 2, 1, [record])
        child_ids = self.store.split_slice(
            self._new_running_slice(job), [(10, 11, '甲'), (10, 11, '乙')])
        second = self.store.claim_slice(job.id, 'w2', 3)
        self.assertIn(second.id, child_ids)
        conflicts = self.store.complete_slice(second.id, 1, 1, 2, 1,
                                              [dict(record)])
        self.assertEqual(conflicts, [])
        self.assertEqual(self.store.count_records(job.id), 1)

    def test_gap_keeps_query_total_and_received(self):
        job = self._job()
        slc = self.store.claim_slice(job.id, 'w1', 3)
        self.store.mark_gap(slc.id, total=4, received=2, cap=2)
        saved = self.store.get_slice(slc.id)
        self.assertEqual(saved.state, 'gap')
        self.assertEqual((saved.total, saved.received, saved.cap), (4, 2, 2))

    def test_generations_are_separate_jobs(self):
        first = self.store.get_or_create_job('http://x', 10, 13, 1, ['甲'])
        second = self.store.get_or_create_job('http://x', 10, 13, 2, ['甲'])
        self.assertNotEqual(first.id, second.id)
        self.assertEqual(len(self.store.list_slices(first.id)), 1)
        self.assertEqual(len(self.store.list_slices(second.id)), 1)


if __name__ == '__main__':
    unittest.main()
