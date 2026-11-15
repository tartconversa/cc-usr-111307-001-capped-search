"""采集引擎：查询分割、代际守卫、并发认领与完整/缺口判定。

判定原则（与上游协议严格对应）：
- 截断只看声明：total > cap 即被截断；total == cap 且实际取回 total 条时是完整切片。
- 声明数量与实际响应冲突（n > total、n > cap 或 total > cap 不成立却缺条）一律不标完整。
- 截断切片先按时间二分；窄到单刻度仍截断才按 provider 拆；
  单刻度 + 单 provider 仍 total > cap 时留下不可继续拆分的 gap。
- 任何响应的 generation 与作业绑定代际不符：拒绝写入、记录事件、停止本轮。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
import os
import signal
import threading
import time
from typing import Any

from .client import ProtocolError, SearchResponse, SourceClient, SourceError
from .store import Store, build_query


class Outcome(str, Enum):
    COMPLETE = 'complete'
    SPLIT_TIME = 'split_time'
    SPLIT_PROVIDER = 'split_provider'
    GAP = 'gap'
    ERROR = 'error'


@dataclass(frozen=True)
class Verdict:
    outcome: Outcome
    reason: str
    children: tuple[tuple[int, int, str | None], ...] = field(default_factory=tuple)


def classify(start: int, end: int, provider: str | None,
             providers: tuple[str, ...], total: int, cap: int,
             received: int) -> Verdict:
    """纯函数判定，便于单元测试与等价证明复核。"""
    if received > cap:
        return Verdict(Outcome.ERROR, '实际响应 %d 条超过 cap=%d' % (received, cap))
    if received > total:
        return Verdict(Outcome.ERROR,
                       '实际响应 %d 条超过声明 total=%d' % (received, total))
    if total == received and total <= cap:
        # total == cap 且收齐 cap 条同样落在本分支：等于 cap 并非天然截断。
        return Verdict(Outcome.COMPLETE, '声明 %d 条且全部取得' % total)
    if total > cap:
        if end - start > 1:
            mid = start + (end - start) // 2
            return Verdict(Outcome.SPLIT_TIME,
                           '声明 %d > cap %d，时间二分' % (total, cap),
                           ((start, mid, provider), (mid, end, provider)))
        if provider is None:
            if not providers:
                return Verdict(Outcome.GAP,
                               '单刻度截断且 provider 清单为空，无法继续拆分')
            return Verdict(Outcome.SPLIT_PROVIDER,
                           '单刻度声明 %d > cap %d，按 provider 拆分' % (total, cap),
                           tuple((start, end, name) for name in providers))
        return Verdict(Outcome.GAP,
                       '单刻度单 provider 声明 %d > cap %d，接口无法继续拆分'
                       % (total, cap))
    # 此时 total <= cap 且 total != received，只可能 total > received：声明与响应冲突。
    return Verdict(Outcome.ERROR,
                   '声明 total=%d 但仅取得 %d 条（cap=%d），数量冲突'
                   % (total, received, cap))


class CollectorError(Exception):
    """采集前置条件不满足（如 provider 清单不可得）。"""


def _scope_label(start: int, end: int, provider: str | None) -> str:
    base = '[%d,%d)' % (start, end)
    return base if provider is None else base + ' provider=%s' % provider


@dataclass
class RunResult:
    job_id: int
    generation: int
    state: str
    counts: dict[str, int]
    gaps: list[dict[str, Any]]
    conflicts: list[dict[str, Any]]
    fetched_records: int
    reason: str | None = None


class Collector:
    def __init__(self, client: SourceClient, store: Store, base_url: str,
                 start: int, end: int, max_attempts: int = 3,
                 backoff_base: float = 0.2):
        if not start < end:
            raise ValueError('采集窗口必须满足 start < end')
        if max_attempts < 1:
            raise ValueError('max_attempts 至少为 1')
        self.client = client
        self.store = store
        self.base_url = base_url
        self.start = start
        self.end = end
        self.max_attempts = max_attempts
        self.backoff_base = backoff_base
        self._stop = threading.Event()
        self._drift = threading.Event()

    def stop(self) -> None:
        """请求优雅暂停；已认领切片会被释放，终态切片不受影响。"""
        self._stop.set()

    @property
    def stopped(self) -> bool:
        return self._stop.is_set()

    def _fetch_providers(self) -> tuple[tuple[str, ...], int]:
        last: Exception | None = None
        for attempt in range(self.max_attempts):
            try:
                return self.client.providers()
            except SourceError as error:
                last = error
                if self._stop.wait(min(self.backoff_base * (2 ** attempt), 5.0)):
                    raise CollectorError('暂停期间无法取得 provider 清单') from error
            except ProtocolError as error:
                raise CollectorError('provider 清单协议非法: %s' % error) from error
        raise CollectorError('provider 清单持续不可得: %s' % last)

    def _fetch_search(self, start: int, end: int,
                      provider: str | None) -> SearchResponse:
        last: Exception | None = None
        for attempt in range(self.max_attempts):
            try:
                return self.client.search(start, end, provider)
            except SourceError as error:
                last = error
                if self._stop.wait(min(self.backoff_base * (2 ** attempt), 5.0)):
                    raise
        raise last  # type: ignore[misc]

    def run(self, workers: int = 1) -> RunResult:
        if workers < 1:
            raise ValueError('workers 至少为 1')
        providers, generation = self._fetch_providers()
        job = self.store.get_or_create_job(
            self.base_url, self.start, self.end, generation, providers)
        if job.state not in ('new', 'running', 'paused', 'complete',
                             'complete_with_gaps', 'completed_with_errors',
                             'aborted_generation_drift', 'aborted_providers',
                             'failed'):
            raise CollectorError('作业状态 %s 不可继续' % job.state)
        if set(job.providers) != set(providers):
            # 同一代际内 provider 清单变化属于协议异常，不能据新清单标记完整。
            self.store.add_event(job.id, 'providers_changed',
                                 {'stored': list(job.providers), 'observed': list(providers)})
            self.store.set_job_state(job.id, 'aborted_providers')
            return RunResult(job.id, generation, 'aborted_providers',
                             self.store.slice_state_counts(job.id), [], [],
                             self.store.count_records(job.id),
                             '同代际 provider 清单不一致，已中止')
        older = [j for j in self.store.list_jobs()
                 if j.base_url == self.base_url
                 and j.start_tick == self.start and j.end_tick == self.end
                 and j.generation < generation]
        if older:
            self.store.add_event(
                job.id, 'generation_advanced',
                {'new_generation': generation,
                 'superseded_jobs': [j.id for j in older],
                 'note': '旧代切片不参与当代完成证明'})
        self.store.set_job_state(job.id, 'running')
        reclaimed = self.store.reclaim_stale(job.id, 'startup')
        if reclaimed:
            self.store.add_event(job.id, 'reclaim_stale',
                                 {'running_slices_reclaimed': reclaimed})
        self.store.reset_attempts(job.id)

        previous_handler = signal.getsignal(signal.SIGINT)

        def _handle_sigint(signum, frame):
            self.stop()

        signal.signal(signal.SIGINT, _handle_sigint)
        worker_errors: list[BaseException] = []
        try:
            threads = []
            for index in range(workers):
                name = 'worker-%d' % index
                thread = threading.Thread(
                    target=self._worker_loop,
                    args=(job.id, job.generation, job.providers, name, worker_errors),
                    name=name, daemon=True)
                thread.start()
                threads.append(thread)
            for thread in threads:
                thread.join()
        finally:
            signal.signal(signal.SIGINT, previous_handler)

        # 线程内异常（非预期错误）：保证 running 切片回收，不冒充完成。
        self.store.reclaim_stale(job.id, 'shutdown')
        counts = self.store.slice_state_counts(job.id)
        gaps = self._gap_details(job.id)
        conflicts = [dict(row) for row in self.store.list_conflicts(job.id)]
        if worker_errors:
            self.store.set_job_state(job.id, 'failed')
            raise worker_errors[0]
        if self._drift.is_set():
            state = 'aborted_generation_drift'
            reason = '检测到上游换代，代际不符的响应全部拒绝写入'
        elif self._stop.is_set():
            state = 'paused'
            reason = '收到暂停信号，终态切片已保留，续跑不会重抓'
        elif any(s in counts for s in ('pending', 'running', 'error')):
            state = 'completed_with_errors'
            reason = '存在失败/未完成切片，不能给出全量证明'
        elif gaps:
            state = 'complete_with_gaps'
            reason = '可证范围完整，另有 %d 个不可拆分缺口' % len(gaps)
        else:
            state = 'complete'
            reason = '全部叶子切片完整，覆盖证明闭合'
        self.store.set_job_state(job.id, state)
        return RunResult(job.id, generation, state, counts, gaps, conflicts,
                         self.store.count_records(job.id), reason)

    def _worker_loop(self, job_id: int, generation: int,
                     providers: tuple[str, ...], name: str,
                     errors: list[BaseException]) -> None:
        worker = 'pid%d-%s' % (os.getpid(), name)
        try:
            while not self._stop.is_set() and not self._drift.is_set():
                slc = self.store.claim_slice(job_id, worker, self.max_attempts)
                if slc is None:
                    return
                try:
                    self._process(slc, generation, providers, worker)
                except Exception as error:  # noqa: BLE001 - 单切片失败隔离
                    detail = '%s: %s' % (type(error).__name__, error)
                    try:
                        self.store.fail_slice(slc.id, detail)
                    except Exception:  # pragma: no cover - 落盘失败向上抛
                        raise
                    self.store.log_request(
                        job_id, 'search',
                        build_query(slc.start_tick, slc.end_tick, slc.provider),
                        None, slc.total, slc.received, 'error', detail)
        except BaseException as error:  # noqa: BLE001
            errors.append(error)

    def _process(self, slc, generation: int,
                 providers: tuple[str, ...], worker: str) -> None:
        query = build_query(slc.start_tick, slc.end_tick, slc.provider)
        if slc.provider is not None and slc.provider not in providers:
            # 未知 provider：不能标完整，留待人工/下轮处理。
            self.store.fail_slice(slc.id, '未知 provider %r' % slc.provider)
            self.store.log_request(slc.job_id, 'search', query, None,
                                   None, None, 'error', 'unknown provider')
            return
        try:
            response = self._fetch_search(slc.start_tick, slc.end_tick, slc.provider)
        except SourceError as error:
            self.store.fail_slice(slc.id, '请求失败: %s' % error)
            self.store.log_request(slc.job_id, 'search', query, None,
                                   None, None, 'error', str(error))
            return
        self.store.log_request(slc.job_id, 'search', query, response.generation,
                               response.total, len(response.records), 'ok', None)
        if response.generation != generation:
            detail = ('代际漂移: 作业绑定 gen=%s，响应 gen=%s，拒绝写入'
                      % (generation, response.generation))
            self.store.fail_slice(slc.id, detail, response.total,
                                  len(response.records), response.cap)
            self.store.add_event(slc.job_id, 'generation_drift',
                                 {'slice_id': slc.id, 'query': query,
                                  'expected': generation,
                                  'observed': response.generation, 'worker': worker})
            self._drift.set()
            return
        def _provider_matches(record: dict[str, Any]) -> bool:
            names = record.get('providers')
            return isinstance(names, (list, tuple)) and slc.provider in names

        out_of_scope = [r['id'] for r in response.records
                        if not (slc.start_tick <= r['registered_at'] < slc.end_tick)
                        or (slc.provider is not None and not _provider_matches(r))]
        if out_of_scope:
            reason = '响应包含查询范围外记录: %s' % out_of_scope
            self.store.fail_slice(slc.id, reason, response.total,
                                  len(response.records), response.cap)
            return
        verdict = classify(slc.start_tick, slc.end_tick, slc.provider, providers,
                           response.total, response.cap, len(response.records))
        if verdict.outcome is Outcome.COMPLETE:
            conflict_ids = self.store.complete_slice(
                slc.id, response.total, len(response.records), response.cap,
                response.generation, list(response.records))
            for rid in conflict_ids:
                self.store.add_event(slc.job_id, 'record_conflict',
                                     {'slice_id': slc.id, 'rid': rid,
                                      'query': query})
        elif verdict.outcome in (Outcome.SPLIT_TIME, Outcome.SPLIT_PROVIDER):
            self.store.split_slice(slc.id, list(verdict.children))
        elif verdict.outcome is Outcome.GAP:
            self.store.mark_gap(slc.id, response.total,
                                len(response.records), response.cap)
            self.store.add_event(
                slc.job_id, 'gap',
                {'slice_id': slc.id, 'query': query, 'provider': slc.provider,
                 'start': slc.start_tick, 'end': slc.end_tick,
                 'total': response.total, 'received': len(response.records),
                 'cap': response.cap, 'reason': verdict.reason})
        else:
            self.store.fail_slice(slc.id, verdict.reason, response.total,
                                  len(response.records), response.cap)
            self.store.add_event(
                slc.job_id, 'claim_conflict',
                {'slice_id': slc.id, 'query': query, 'total': response.total,
                 'received': len(response.records), 'cap': response.cap,
                 'reason': verdict.reason})

    def _gap_details(self, job_id: int) -> list[dict[str, Any]]:
        details = []
        for slc in self.store.list_slices(job_id):
            if slc.state == 'gap':
                details.append({
                    'slice_id': slc.id,
                    'query': build_query(slc.start_tick, slc.end_tick, slc.provider),
                    'scope': _scope_label(slc.start_tick, slc.end_tick, slc.provider),
                    'start': slc.start_tick, 'end': slc.end_tick,
                    'provider': slc.provider,
                    'total': slc.total, 'received': slc.received, 'cap': slc.cap,
                    'missing_at_least': (slc.total or 0) - (slc.received or 0),
                })
        return details
