"""采集器端到端测试：在本机演练 Source 上验证完整证明、缺口、暂停续跑与混代守卫。"""
from __future__ import annotations

import copy
import json
import tempfile
import threading
import unittest
from pathlib import Path

from src import source as source_module
from src.collector.client import HttpClient, InProcessClient
from src.collector.engine import Collector
from src.collector.report import build_report, render_text_report, tick_coverage
from src.collector.store import Store

ROOT = Path(__file__).resolve().parents[1]
GAP_DATA = json.loads((ROOT / 'fixtures' / 'source_gap.json').read_text(encoding='utf-8'))


class CollectorHarness:
    def __init__(self, data=None):
        self.source = source_module.Source(data=copy.deepcopy(data))
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.tmp.name) / 'work.db')

    def store(self):
        return Store(self.db_path)

    def collector(self, start=10, end=13, after_request=None, url='inprocess://drill'):
        client = InProcessClient(self.source, after_request=after_request)
        return Collector(client, self.store(), url, start, end,
                         max_attempts=2, backoff_base=0.0)

    def cleanup(self):
        self.tmp.cleanup()


class DrillFixtureTests(unittest.TestCase):
    """默认 fixtures/source.json：同刻跨供方可拆开，证明应闭合。"""

    def setUp(self):
        self.harness = CollectorHarness()

    def tearDown(self):
        self.harness.cleanup()

    def test_full_proof_with_provider_split_and_dedup(self):
        collector = self.harness.collector(10, 12)
        result = collector.run(workers=3)
        self.assertEqual(result.state, 'complete')
        self.assertEqual(result.gaps, [])
        store = collector.store
        report = build_report(store, result.job_id)
        self.assertTrue(report['proof']['fully_proven'])
        self.assertEqual(report['proof']['proven_ticks'], ['10', '11'])
        # 4 个不同 id；d 同时登记在甲、乙两家目录，去重后仍是 4。
        self.assertEqual(store.count_records(result.job_id), 4)
        d_sources = store.sources_for_record(result.job_id, 'd')
        self.assertEqual(len(d_sources), 1)
        self.assertEqual({row['rid'] for row in d_sources}, {'d'})
        # t=10 同刻三条：全供方截断后按 provider 拆开，a/b 来自甲切片，c 来自乙切片。
        t10 = tick_coverage(store, result.job_id)['10']
        self.assertTrue(t10['proven_complete'])
        scopes = ' '.join(t10['leaves'])
        self.assertIn('provider=甲', scopes)
        self.assertIn('provider=乙', scopes)
        a_provider = store.sources_for_record(result.job_id, 'a')[0]['provider']
        c_provider = store.sources_for_record(result.job_id, 'c')[0]['provider']
        self.assertEqual((a_provider, c_provider), ('甲', '乙'))
        # 文本报告可渲染且包含覆盖树与来源。
        text = render_text_report(report)
        self.assertIn('全量证明: 闭合', text)
        self.assertIn('资源来源请求', text)

    def test_pause_resume_skips_completed_slices(self):
        collector = self.harness.collector(10, 12)
        # 第一个切片完整落盘后再暂停，保证至少有一个终态切片。
        def pause_after_first_complete(kind, _params):
            if kind == 'search':
                jobs = collector.store.list_jobs()
                if jobs:
                    counts = collector.store.slice_state_counts(jobs[0].id)
                    if counts.get('complete', 0) >= 1:
                        collector.stop()
        collector.client._after_request = pause_after_first_complete
        result = collector.run(workers=1)
        self.assertEqual(result.state, 'paused')
        store = collector.store
        before_counts = store.slice_state_counts(result.job_id)
        completed_before = before_counts.get('complete', 0)
        self.assertGreaterEqual(completed_before, 1)
        requests_before = len(store.list_requests(result.job_id))

        # 续跑：新 Collector、新 Store 句柄，模拟重启进程。
        second = self.harness.collector(10, 12)
        resumed = second.run(workers=2)
        self.assertEqual(resumed.state, 'complete')
        self.assertEqual(resumed.job_id, result.job_id)
        # 已完整的切片没有再次产生 search 请求（只统计成功的 search）。
        store2 = second.store
        after_slices = store2.list_slices(resumed.job_id)
        reclaimed = [slc for slc in after_slices if slc.claimed_count > 1]
        self.assertEqual(reclaimed, [], '终态切片不应被重新认领')
        self.assertGreater(len(store2.list_requests(resumed.job_id)), requests_before)

    def test_resume_after_simulated_crash_releases_running(self):
        collector = self.harness.collector(10, 12)
        result = collector.run(workers=1)
        self.assertEqual(result.state, 'complete')
        store = collector.store
        # 人为把一个 complete 切片改回 running，模拟进程被杀时的僵死认领。
        target = store.conn.execute(
            "SELECT id FROM slices WHERE state='complete' LIMIT 1").fetchone()
        store.conn.execute("UPDATE slices SET state='running' WHERE id=?",
                           (target['id'],))
        second = self.harness.collector(10, 12)
        resumed = second.run(workers=1)
        self.assertEqual(resumed.state, 'complete')
        self.assertEqual(
            second.store.get_slice(target['id']).state, 'complete')


class GapFixtureTests(unittest.TestCase):
    """fixtures/source_gap.json：甲家同刻超过 cap，必须留下不可拆分缺口。"""

    def setUp(self):
        self.harness = CollectorHarness(GAP_DATA)

    def tearDown(self):
        self.harness.cleanup()

    def test_gap_reported_but_other_ranges_complete(self):
        collector = self.harness.collector(10, 13)
        result = collector.run(workers=4)
        self.assertEqual(result.state, 'complete_with_gaps')
        self.assertEqual(len(result.gaps), 1)
        gap = result.gaps[0]
        self.assertEqual((gap['start'], gap['end'], gap['provider']),
                         (10, 11, '甲'))
        self.assertEqual((gap['total'], gap['received'], gap['cap']), (4, 2, 2))
        self.assertEqual(gap['missing_at_least'], 2)
        self.assertIn('start=10&end=11', gap['query'])
        self.assertIn('provider=%E7%94%B2', gap['query'])

        store = collector.store
        report = build_report(store, result.job_id)
        self.assertFalse(report['proof']['fully_proven'])
        coverage = report['proof']['tick_coverage']
        self.assertFalse(coverage['10']['proven_complete'])
        self.assertEqual(coverage['10']['blockers'], ['gap'])
        self.assertTrue(coverage['11']['proven_complete'])
        self.assertTrue(coverage['12']['proven_complete'])
        # 全库 9 个不同 id；甲家缺口切片丢失 g3 的甲副本与 g4，
        # 实际取得 7 个去重 id。去重条数绝不能被当作全量。
        self.assertEqual(store.count_records(result.job_id), 7)
        # 联合同刻资源 g3 的甲副本落在甲家缺口切片中丢失，仅保留乙家来源。
        self.assertEqual(
            {row['provider'] for row in store.sources_for_record(result.job_id, 'g3')},
            {'乙'})
        text = render_text_report(report)
        self.assertIn('不可继续拆分的缺口', text)
        self.assertIn('全量证明: 未闭合', text)

    def test_uncapped_ranges_still_split_by_time(self):
        # 窗口 [11,13) 不含缺口刻度，应完全闭合，且 t=11 先按时间、再按 provider 拆开。
        collector = self.harness.collector(11, 13)
        result = collector.run(workers=2)
        self.assertEqual(result.state, 'complete')
        store = collector.store
        providers_used = {
            slc.provider for slc in store.list_slices(result.job_id)
            if slc.state == 'complete' and slc.provider is not None}
        self.assertEqual(providers_used, {'甲', '乙'})


class GenerationDriftTests(unittest.TestCase):
    """换代发生在并发请求之间：最终结果不得混代。"""

    def setUp(self):
        self.harness = CollectorHarness()

    def tearDown(self):
        self.harness.cleanup()

    def test_flip_between_concurrent_requests_rejects_new_generation(self):
        flipped = threading.Event()
        flip_once = threading.Event()

        def after_request(kind, _params):
            # 第一个 search 请求返回后立刻换代，使其余在途/后续请求看到 gen=2。
            if kind == 'search' and not flip_once.is_set():
                flip_once.set()
                self.harness.source.state['generation'] = 2
                flipped.set()

        collector = self.harness.collector(10, 12, after_request=after_request)
        result = collector.run(workers=4)
        self.assertTrue(flipped.is_set())
        self.assertEqual(result.state, 'aborted_generation_drift')
        self.assertEqual(result.generation, 1)
        store = collector.store
        job = store.get_job(result.job_id)
        self.assertEqual(job.generation, 1)

        # 落盘的资源与来源只可能来自 gen=1。
        bad_sources = store.conn.execute(
            'SELECT COUNT(*) AS n FROM record_sources WHERE generation != 1'
        ).fetchone()
        self.assertEqual(bad_sources['n'], 0)
        # 代际不符的切片是 error 而非 complete，证明不闭合。
        states = {slc.state for slc in store.list_slices(result.job_id)}
        self.assertIn('error', states)
        self.assertNotIn('gap', states)
        report = build_report(store, result.job_id)
        self.assertFalse(report['proof']['fully_proven'])
        drift_events = [e for e in report['events']
                        if e['kind'] == 'generation_drift']
        self.assertTrue(drift_events)
        self.assertEqual(drift_events[0]['detail']['observed'], 2)

        # 重新运行：为 gen=2 建立独立作业，旧 gen=1 作业不被覆盖。
        second = self.harness.collector(10, 12)
        result2 = second.run(workers=3)
        self.assertEqual(result2.generation, 2)
        self.assertNotEqual(result2.job_id, result.job_id)
        self.assertEqual(second.store.get_job(result.job_id).generation, 1)
        self.assertEqual(result2.state, 'complete')
        # 两个作业的资源互不串库。
        self.assertEqual(
            second.store.conn.execute(
                'SELECT DISTINCT generation FROM record_sources WHERE job_id=?',
                (result2.job_id,)).fetchone()['generation'], 2)

    def test_flip_during_paused_resume_still_binds_one_generation(self):
        collector = self.harness.collector(10, 12)
        first = collector.run(workers=1)
        self.assertEqual(first.state, 'complete')
        # 完整采集后上游换代；再次运行必须开新作业，旧切片保持 gen=1。
        self.harness.source.state['generation'] = 2
        second = self.harness.collector(10, 12)
        resumed = second.run(workers=1)
        self.assertEqual(resumed.state, 'complete')
        self.assertNotEqual(resumed.job_id, first.job_id)
        old_sources = collector.store.conn.execute(
            'SELECT DISTINCT generation FROM record_sources WHERE job_id=?',
            (first.job_id,)).fetchall()
        self.assertEqual([row['generation'] for row in old_sources], [1])


class ConflictInjectionTests(unittest.TestCase):
    """同 generation 同 id 内容不一致必须暴露冲突。"""

    def setUp(self):
        self.harness = CollectorHarness(GAP_DATA)

    def tearDown(self):
        self.harness.cleanup()

    def test_same_id_different_payload_raises_conflict(self):
        original_route = self.harness.source.route
        g3_original = next(copy.deepcopy(r) for r in GAP_DATA['records']
                           if r['id'] == 'g3')

        def conflicting_route(method, target, headers, body):
            status, resp_headers, payload = original_route(method, target, headers, body)
            if '/search' not in target or 'provider=' not in target:
                return status, resp_headers, payload
            data = json.loads(payload)
            if 'provider=%E7%94%B2' in target:
                # 甲家缺口切片补回 g3 并把 total 改成 2，使其成为完整切片，
                # 从而同一 id 在两个 provider 切片中都出现。
                if data['total'] == 4:
                    data['records'] = [g3_original,
                                       next(r for r in data['records'] if r['id'] == 'g1')]
                    data['total'] = 2
            elif 'provider=%E4%B9%99' in target:
                # 乙家返回的 g3 内容与甲家不一致（上游同代同 id 冲突）。
                for record in data.get('records', []):
                    if record['id'] == 'g3':
                        record['title'] = '被篡改的联合同刻资源'
            return status, resp_headers, json.dumps(data, ensure_ascii=False).encode()

        self.harness.source.route = conflicting_route
        collector = self.harness.collector(10, 11)
        result = collector.run(workers=2)
        # 两个 provider 切片都完整，但同 id 内容冲突必须可见且不自动取舍。
        self.assertEqual(result.state, 'complete')
        conflict_rids = {c['rid'] for c in result.conflicts}
        self.assertIn('g3', conflict_rids)
        report = build_report(collector.store, result.job_id)
        self.assertTrue(any(c['rid'] == 'g3' for c in report['conflicts']))
        row = collector.store.conn.execute(
            'SELECT payload_json FROM records WHERE job_id=? AND rid=?',
            (result.job_id, 'g3')).fetchone()
        # 先落盘的版本保留，冲突版本不覆盖。
        first_title = json.loads(row['payload_json'])['title']
        self.assertIn(first_title,
                      {'联合同刻资源', '被篡改的联合同刻资源'})
        text = render_text_report(report)
        self.assertIn('同 id 内容冲突', text)
        # 两条来源请求都保留，可逐条回溯。
        self.assertEqual(
            len(collector.store.sources_for_record(result.job_id, 'g3')), 2)


class FailureHandlingTests(unittest.TestCase):
    """请求失败、数量冲突与未知 provider 都不能被标记为完整。"""

    def setUp(self):
        self.harness = CollectorHarness()

    def tearDown(self):
        self.harness.cleanup()

    def test_request_failure_leaves_error_not_complete(self):
        from src.collector.client import SourceError

        class FlakyClient(InProcessClient):
            def __init__(self, source):
                super().__init__(source)
                self.calls = 0

            def search(self, start, end, provider):
                self.calls += 1
                if self.calls <= 4:
                    raise SourceError('模拟连接重置')
                return super().search(start, end, provider)

        store = self.harness.store()
        collector = Collector(FlakyClient(self.harness.source), store,
                              'inprocess://flaky', 10, 12,
                              max_attempts=2, backoff_base=0.0)
        result = collector.run(workers=1)
        # 重试用尽后切片留 error，作业不能给出全量证明；后续运行仍可补齐。
        self.assertEqual(result.state, 'completed_with_errors')
        self.assertIn('error', result.counts)
        self.assertFalse(build_report(store, result.job_id)['proof']['fully_proven'])

        # 再次运行（客户端恢复正常）：error 切片被重新认领并完成。
        healthy = self.harness.collector(10, 12)
        resumed = healthy.run(workers=1)
        self.assertEqual(resumed.state, 'complete')
        self.assertTrue(build_report(healthy.store, resumed.job_id)
                        ['proof']['fully_proven'])

    def test_claim_response_conflict_marks_slice_error(self):
        original_route = self.harness.source.route

        def short_route(method, target, headers, body):
            status, h, payload = original_route(method, target, headers, body)
            if '/search' in target and 'provider=' not in target:
                data = json.loads(payload)
                # 声明 total=3 却只返回 1 条且 total<=cap 的场景难以由原数据构造，
                # 这里直接把 total 改小，制造 received > total 的数量冲突。
                if data['records']:
                    data['total'] = 0
                payload = json.dumps(data, ensure_ascii=False).encode()
            return status, h, payload

        self.harness.source.route = short_route
        collector = self.harness.collector(10, 12)
        result = collector.run(workers=1)
        self.assertIn(result.state, ('completed_with_errors', 'complete_with_gaps'))
        errors = [slc for slc in collector.store.list_slices(result.job_id)
                  if slc.state == 'error']
        self.assertTrue(errors)
        self.assertFalse(build_report(collector.store, result.job_id)
                         ['proof']['fully_proven'])


class MultiProcessTests(unittest.TestCase):
    """两个独立进程共享同一 SQLite 进度库，不得重复落库或互相破坏。"""

    def test_two_processes_share_one_job(self):
        import subprocess
        import sys

        harness = CollectorHarness(GAP_DATA)
        server = source_module.make_server(0, data=GAP_DATA)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            url = 'http://127.0.0.1:%d' % server.server_port
            cmd = [sys.executable, '-m', 'src.collector.cli', 'collect',
                   '--db', harness.db_path, '--url', url,
                   '--start', '10', '--end', '13', '--workers', '2',
                   '--max-attempts', '2']
            procs = [subprocess.Popen(cmd, cwd=str(ROOT),
                                      stdout=subprocess.DEVNULL,
                                      stderr=subprocess.DEVNULL)
                     for _ in range(2)]
            codes = [proc.wait(timeout=60) for proc in procs]
            self.assertEqual(codes, [0, 0])
            store = Store(harness.db_path)
            jobs = store.list_jobs()
            self.assertEqual(len(jobs), 1)
            counts = store.slice_state_counts(jobs[0].id)
            self.assertEqual(counts.get('gap'), 1)
            self.assertEqual(store.count_records(jobs[0].id), 7)
            # 每个来源只登记一次，没有因并发而重复插入。
            dupes = store.conn.execute(
                'SELECT rid, slice_id, COUNT(*) n FROM record_sources '
                'GROUP BY rid, slice_id HAVING n > 1').fetchall()
            self.assertEqual(dupes, [])
            store.close()
        finally:
            server.shutdown()
            thread.join()
            harness.cleanup()


class HttpChannelTests(unittest.TestCase):
    """走真实回环 HTTP，验证线程服务与客户端集成。"""

    def test_http_collect_and_report(self):
        harness = CollectorHarness()
        with source_module.make_server(0, data=GAP_DATA) as server:
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                url = 'http://127.0.0.1:%d' % server.server_port
                store = Store(harness.db_path)
                collector = Collector(HttpClient(url, timeout=5), store, url,
                                      10, 13, max_attempts=2, backoff_base=0.0)
                result = collector.run(workers=3)
                self.assertEqual(result.state, 'complete_with_gaps')
                report = build_report(store, result.job_id)
                self.assertEqual(len(report['gaps']), 1)
                store.close()
            finally:
                server.shutdown()
                thread.join()
        harness.cleanup()


if __name__ == '__main__':
    unittest.main()
