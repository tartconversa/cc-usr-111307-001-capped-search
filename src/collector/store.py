"""SQLite 持久化层。

一个数据库文件可容纳多个作业；作业绑定唯一 (base_url, 时间窗口, generation)。
切片树、资源、来源请求、内容冲突、请求日志与事件全部落盘，
complete/gap 为终态，重启后绝不重抓；崩溃遗留的 running 认领在开启时回收。
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import json
import sqlite3
import threading
import time
from typing import Any, Iterable
from urllib.parse import urlencode

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS jobs (
    id INTEGER PRIMARY KEY,
    base_url TEXT NOT NULL,
    start_tick INTEGER NOT NULL,
    end_tick INTEGER NOT NULL,
    generation INTEGER NOT NULL,
    providers_json TEXT NOT NULL,
    state TEXT NOT NULL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    UNIQUE(base_url, start_tick, end_tick, generation)
);

CREATE TABLE IF NOT EXISTS slices (
    id INTEGER PRIMARY KEY,
    job_id INTEGER NOT NULL REFERENCES jobs(id),
    parent_id INTEGER REFERENCES slices(id),
    start_tick INTEGER NOT NULL,
    end_tick INTEGER NOT NULL,
    provider TEXT,
    depth INTEGER NOT NULL,
    state TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    claimed_by TEXT,
    claimed_at REAL,
    claimed_count INTEGER NOT NULL DEFAULT 0,
    total INTEGER,
    received INTEGER,
    cap INTEGER,
    error TEXT,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_slices_job_state ON slices(job_id, state);
CREATE INDEX IF NOT EXISTS idx_slices_parent ON slices(parent_id);

CREATE TABLE IF NOT EXISTS records (
    id INTEGER PRIMARY KEY,
    job_id INTEGER NOT NULL REFERENCES jobs(id),
    rid TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at REAL NOT NULL,
    UNIQUE(job_id, rid)
);

CREATE TABLE IF NOT EXISTS record_sources (
    id INTEGER PRIMARY KEY,
    job_id INTEGER NOT NULL REFERENCES jobs(id),
    rid TEXT NOT NULL,
    slice_id INTEGER NOT NULL REFERENCES slices(id),
    provider TEXT,
    start_tick INTEGER NOT NULL,
    end_tick INTEGER NOT NULL,
    request_query TEXT NOT NULL,
    generation INTEGER NOT NULL,
    fetched_at REAL NOT NULL,
    UNIQUE(job_id, rid, slice_id)
);
CREATE INDEX IF NOT EXISTS idx_sources_rid ON record_sources(job_id, rid);
CREATE INDEX IF NOT EXISTS idx_sources_slice ON record_sources(slice_id);

CREATE TABLE IF NOT EXISTS conflicts (
    id INTEGER PRIMARY KEY,
    job_id INTEGER NOT NULL REFERENCES jobs(id),
    rid TEXT NOT NULL,
    slice_id INTEGER NOT NULL REFERENCES slices(id),
    existing_payload_json TEXT NOT NULL,
    incoming_payload_json TEXT NOT NULL,
    detected_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_conflicts_job ON conflicts(job_id);

CREATE TABLE IF NOT EXISTS request_log (
    id INTEGER PRIMARY KEY,
    job_id INTEGER REFERENCES jobs(id),
    kind TEXT NOT NULL,
    request_query TEXT NOT NULL,
    generation INTEGER,
    claimed_total INTEGER,
    received INTEGER,
    status TEXT NOT NULL,
    detail TEXT,
    at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY,
    job_id INTEGER REFERENCES jobs(id),
    kind TEXT NOT NULL,
    detail_json TEXT NOT NULL,
    at REAL NOT NULL
);
"""

# 切片状态机：
# pending  -> running -> pending（失败/暂停回收）
# pending  -> running -> complete | gap | error
# complete/gap 为终态；error 在 attempts 允许范围内可重新认领
TERMINAL_STATES = ('complete', 'gap')
WORKABLE_STATES = ('pending', 'error')


@dataclass(frozen=True)
class Slice:
    id: int
    job_id: int
    parent_id: int | None
    start_tick: int
    end_tick: int
    provider: str | None
    depth: int
    state: str
    attempts: int
    claimed_count: int
    total: int | None
    received: int | None
    cap: int | None
    error: str | None


@dataclass(frozen=True)
class Job:
    id: int
    base_url: str
    start_tick: int
    end_tick: int
    generation: int
    providers: tuple[str, ...]
    state: str


def build_query(start: int, end: int, provider: str | None) -> str:
    """与上游 /search 完全一致的查询串，作为证据原样保存。"""
    params: dict[str, str] = {'start': str(start), 'end': str(end)}
    if provider is not None:
        params['provider'] = provider
    return '/search?' + urlencode(params)


class Store:
    def __init__(self, path: str):
        # check_same_thread=False：采集器允许多线程共享一个 Store。
        # isolation_level=None 关闭 Python 隐式事务，全部由本类显式
        # BEGIN IMMEDIATE / commit 控制，避免嵌套事务与写锁升级失败。
        self._conn = sqlite3.connect(path, check_same_thread=False, timeout=30.0,
                                     isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute('PRAGMA busy_timeout=30000')
        # 单连接多线程：写事务用 RLock 串行化；同线程内嵌套事务走 SAVEPOINT。
        self._write_lock = threading.RLock()
        self._txn_depth = 0
        self._init_schema()

    def _init_schema(self) -> None:
        """建表可能与另一个进程的首次打开撞锁，按 busy_timeout 节奏重试。"""
        deadline = time.time() + 30.0
        while True:
            try:
                self._conn.executescript(SCHEMA)
                return
            except sqlite3.OperationalError:
                if time.time() >= deadline:
                    raise
                time.sleep(0.1)

    def close(self) -> None:
        self._conn.close()

    @property
    def conn(self) -> sqlite3.Connection:
        return self._conn

    @contextmanager
    def _txn(self):
        """显式写事务：RLock 串行化跨线程写入，同线程嵌套时退化为 SAVEPOINT。"""
        with self._write_lock:
            if self._txn_depth == 0:
                self._conn.execute('BEGIN IMMEDIATE')
                self._txn_depth = 1
                try:
                    yield
                except Exception:
                    self._conn.execute('ROLLBACK')
                    raise
                else:
                    self._conn.execute('COMMIT')
                finally:
                    self._txn_depth = 0
            else:
                name = 'sp_%d' % self._txn_depth
                self._txn_depth += 1
                self._conn.execute('SAVEPOINT ' + name)
                try:
                    yield
                except Exception:
                    self._conn.execute('ROLLBACK TO ' + name)
                    self._conn.execute('RELEASE ' + name)
                    raise
                else:
                    self._conn.execute('RELEASE ' + name)
                finally:
                    self._txn_depth -= 1

    # ------------------------------------------------------------- 作业
    def get_or_create_job(self, base_url: str, start: int, end: int,
                          generation: int, providers: Iterable[str]) -> Job:
        if not start < end:
            raise ValueError('作业窗口必须满足 start < end')
        now = time.time()
        providers_t = tuple(providers)
        with self._txn():
            row = self._conn.execute(
                'SELECT * FROM jobs WHERE base_url=? AND start_tick=? AND end_tick=? AND generation=?',
                (base_url, start, end, generation)).fetchone()
            if row is None:
                cur = self._conn.execute(
                    'INSERT INTO jobs(base_url,start_tick,end_tick,generation,'
                    'providers_json,state,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)',
                    (base_url, start, end, generation,
                     json.dumps(list(providers_t), ensure_ascii=False),
                     'new', now, now))
                job_id = cur.lastrowid
                self._conn.execute(
                    'INSERT INTO slices(job_id,parent_id,start_tick,end_tick,'
                    'provider,depth,state,created_at,updated_at) '
                    'VALUES(?,?,?,?,?,?,?,?,?)',
                    (job_id, None, start, end, None, 0, 'pending', now, now))
                self.add_event(job_id, 'job_created',
                               {'generation': generation, 'providers': list(providers_t)})
            else:
                job_id = row['id']
        return self.get_job(job_id)

    def find_job(self, base_url: str, start: int, end: int, generation: int) -> Job | None:
        row = self._conn.execute(
            'SELECT * FROM jobs WHERE base_url=? AND start_tick=? AND end_tick=? AND generation=?',
            (base_url, start, end, generation)).fetchone()
        return self._row_to_job(row) if row else None

    def get_job(self, job_id: int) -> Job:
        row = self._conn.execute('SELECT * FROM jobs WHERE id=?', (job_id,)).fetchone()
        if row is None:
            raise KeyError('作业不存在: %s' % job_id)
        return self._row_to_job(row)

    def list_jobs(self) -> list[Job]:
        rows = self._conn.execute('SELECT * FROM jobs ORDER BY id').fetchall()
        return [self._row_to_job(row) for row in rows]

    @staticmethod
    def _row_to_job(row: sqlite3.Row) -> Job:
        return Job(id=row['id'], base_url=row['base_url'],
                   start_tick=row['start_tick'], end_tick=row['end_tick'],
                   generation=row['generation'],
                   providers=tuple(json.loads(row['providers_json'])),
                   state=row['state'])

    def set_job_state(self, job_id: int, state: str) -> None:
        with self._txn():
            self._conn.execute(
                'UPDATE jobs SET state=?, updated_at=? WHERE id=?',
                (state, time.time(), job_id))

    def add_event(self, job_id: int | None, kind: str, detail: dict[str, Any]) -> None:
        with self._txn():
            self._conn.execute(
                'INSERT INTO events(job_id,kind,detail_json,at) VALUES(?,?,?,?)',
                (job_id, kind, json.dumps(detail, ensure_ascii=False, sort_keys=True),
                 time.time()))

    def list_events(self, job_id: int) -> list[sqlite3.Row]:
        return self._conn.execute(
            'SELECT * FROM events WHERE job_id=? ORDER BY id', (job_id,)).fetchall()

    # ----------------------------------------------------------- 请求日志
    def log_request(self, job_id: int | None, kind: str, query: str,
                    generation: int | None, claimed_total: int | None,
                    received: int | None, status: str, detail: str | None) -> None:
        with self._txn():
            self._conn.execute(
                'INSERT INTO request_log(job_id,kind,request_query,generation,'
                'claimed_total,received,status,detail,at) VALUES(?,?,?,?,?,?,?,?,?)',
                (job_id, kind, query, generation, claimed_total, received,
                 status, detail, time.time()))

    # ------------------------------------------------------------- 切片
    def reclaim_stale(self, job_id: int, worker: str) -> int:
        """把本作业所有 running 认领回收为 pending（暂停/崩溃恢复入口）。"""
        now = time.time()
        with self._txn():
            cur = self._conn.execute(
                "UPDATE slices SET state='pending', claimed_by=NULL, claimed_at=NULL, "
                'updated_at=? WHERE job_id=? AND state=?',
                (now, job_id, 'running'))
            return cur.rowcount

    def reset_attempts(self, job_id: int) -> None:
        """新一轮运行开始：可工作切片的 attempts 归零，允许完整重试。"""
        with self._txn():
            self._conn.execute(
                "UPDATE slices SET attempts=0 WHERE job_id=? AND state IN ('pending','error')",
                (job_id,))

    def claim_slice(self, job_id: int, worker: str, max_attempts: int) -> Slice | None:
        """原子认领一个可工作切片。多线程/多进程安全。"""
        now = time.time()
        with self._txn():
            row = self._conn.execute(
                "SELECT * FROM slices WHERE job_id=? AND state IN ('pending','error') "
                'AND attempts < ? ORDER BY id LIMIT 1',
                (job_id, max_attempts)).fetchone()
            if row is None:
                return None
            self._conn.execute(
                "UPDATE slices SET state='running', claimed_by=?, claimed_at=?, "
                'attempts=attempts+1, claimed_count=claimed_count+1, updated_at=? '
                'WHERE id=?',
                (worker, now, now, row['id']))
        return self.get_slice(row['id'])

    def release_slice(self, slice_id: int) -> None:
        """worker 正常退出/暂停时释放未完成的认领。"""
        with self._txn():
            self._conn.execute(
                "UPDATE slices SET state='pending', claimed_by=NULL, claimed_at=NULL, "
                'updated_at=? WHERE id=? AND state=?',
                (time.time(), slice_id, 'running'))

    def fail_slice(self, slice_id: int, error: str, total: int | None = None,
                   received: int | None = None, cap: int | None = None) -> None:
        with self._txn():
            self._conn.execute(
                "UPDATE slices SET state='error', claimed_by=NULL, claimed_at=NULL, "
                'total=COALESCE(?, total), received=COALESCE(?, received), '
                'cap=COALESCE(?, cap), error=?, updated_at=? WHERE id=?',
                (total, received, cap, error, time.time(), slice_id))

    def split_slice(self, slice_id: int, children: list[tuple[int, int, str | None]]) -> list[int]:
        """把切片拆为子切片（时间半开区间或同刻度按 provider），父切片标记 split。"""
        now = time.time()
        ids: list[int] = []
        with self._txn():
            row = self._conn.execute('SELECT * FROM slices WHERE id=?', (slice_id,)).fetchone()
            if row is None or row['state'] != 'running':
                raise RuntimeError('只有 running 切片可以拆分: %s' % slice_id)
            depth = row['depth'] + 1
            for start, end, provider in children:
                cur = self._conn.execute(
                    'INSERT INTO slices(job_id,parent_id,start_tick,end_tick,'
                    'provider,depth,state,created_at,updated_at) '
                    'VALUES(?,?,?,?,?,?,?,?,?)',
                    (row['job_id'], slice_id, start, end, provider, depth,
                     'pending', now, now))
                ids.append(cur.lastrowid)
            self._conn.execute(
                "UPDATE slices SET state='split', claimed_by=NULL, claimed_at=NULL, "
                'updated_at=? WHERE id=?', (now, slice_id))
        return ids

    def mark_gap(self, slice_id: int, total: int, received: int, cap: int) -> None:
        """不可继续拆分的缺口：保留查询参数、声明 total 与实际取得数。"""
        with self._txn():
            self._conn.execute(
                "UPDATE slices SET state='gap', claimed_by=NULL, claimed_at=NULL, "
                'total=?, received=?, cap=?, error=NULL, updated_at=? WHERE id=?',
                (total, received, cap, time.time(), slice_id))

    def complete_slice(self, slice_id: int, total: int, received: int, cap: int,
                       generation: int, records: list[dict[str, Any]]) -> list[str]:
        """完整取得一个切片：落资源、来源与同 id 内容冲突。

        返回本切片检测到的冲突 id 列表（冲突不阻断完成证明，但必须可见）。
        """
        now = time.time()
        conflict_ids: list[str] = []
        with self._txn():
            row = self._conn.execute('SELECT * FROM slices WHERE id=?', (slice_id,)).fetchone()
            if row is None or row['state'] != 'running':
                raise RuntimeError('只有 running 切片可以完成: %s' % slice_id)
            job_id = row['job_id']
            query = build_query(row['start_tick'], row['end_tick'], row['provider'])
            for record in records:
                rid = record['id']
                payload = json.dumps(record, ensure_ascii=False, sort_keys=True)
                existing = self._conn.execute(
                    'SELECT payload_json FROM records WHERE job_id=? AND rid=?',
                    (job_id, rid)).fetchone()
                if existing is None:
                    self._conn.execute(
                        'INSERT INTO records(job_id,rid,payload_json,created_at) '
                        'VALUES(?,?,?,?)', (job_id, rid, payload, now))
                elif existing['payload_json'] != payload:
                    # 同 generation 同 id 内容不一致：暴露冲突，不覆盖、不挑选。
                    self._conn.execute(
                        'INSERT INTO conflicts(job_id,rid,slice_id,'
                        'existing_payload_json,incoming_payload_json,detected_at) '
                        'VALUES(?,?,?,?,?,?)',
                        (job_id, rid, slice_id, existing['payload_json'], payload, now))
                    conflict_ids.append(rid)
                self._conn.execute(
                    'INSERT OR IGNORE INTO record_sources(job_id,rid,slice_id,'
                    'provider,start_tick,end_tick,request_query,generation,fetched_at) '
                    'VALUES(?,?,?,?,?,?,?,?,?)',
                    (job_id, rid, slice_id, row['provider'], row['start_tick'],
                     row['end_tick'], query, generation, now))
            self._conn.execute(
                "UPDATE slices SET state='complete', claimed_by=NULL, claimed_at=NULL, "
                'total=?, received=?, cap=?, error=NULL, updated_at=? WHERE id=?',
                (total, received, cap, now, slice_id))
        return conflict_ids

    def get_slice(self, slice_id: int) -> Slice:
        row = self._conn.execute('SELECT * FROM slices WHERE id=?', (slice_id,)).fetchone()
        if row is None:
            raise KeyError('切片不存在: %s' % slice_id)
        return self._row_to_slice(row)

    def list_slices(self, job_id: int) -> list[Slice]:
        rows = self._conn.execute(
            'SELECT * FROM slices WHERE job_id=? ORDER BY id', (job_id,)).fetchall()
        return [self._row_to_slice(row) for row in rows]

    def slice_state_counts(self, job_id: int) -> dict[str, int]:
        rows = self._conn.execute(
            'SELECT state, COUNT(*) AS n FROM slices WHERE job_id=? GROUP BY state',
            (job_id,)).fetchall()
        return {row['state']: row['n'] for row in rows}

    @staticmethod
    def _row_to_slice(row: sqlite3.Row) -> Slice:
        return Slice(
            id=row['id'], job_id=row['job_id'], parent_id=row['parent_id'],
            start_tick=row['start_tick'], end_tick=row['end_tick'],
            provider=row['provider'], depth=row['depth'], state=row['state'],
            attempts=row['attempts'], claimed_count=row['claimed_count'],
            total=row['total'], received=row['received'], cap=row['cap'],
            error=row['error'])

    # ------------------------------------------------------------- 读取证据
    def count_records(self, job_id: int) -> int:
        return self._conn.execute(
            'SELECT COUNT(*) AS n FROM records WHERE job_id=?', (job_id,)).fetchone()['n']

    def list_records(self, job_id: int) -> list[dict[str, Any]]:
        rows = self._conn.execute(
            'SELECT rid, payload_json FROM records WHERE job_id=? ORDER BY rid',
            (job_id,)).fetchall()
        return [dict(json.loads(row['payload_json']), _id=row['rid']) for row in rows]

    def list_sources(self, job_id: int) -> list[sqlite3.Row]:
        return self._conn.execute(
            'SELECT * FROM record_sources WHERE job_id=? ORDER BY id',
            (job_id,)).fetchall()

    def sources_for_record(self, job_id: int, rid: str) -> list[sqlite3.Row]:
        return self._conn.execute(
            'SELECT * FROM record_sources WHERE job_id=? AND rid=? ORDER BY id',
            (job_id, rid)).fetchall()

    def list_conflicts(self, job_id: int) -> list[sqlite3.Row]:
        return self._conn.execute(
            'SELECT * FROM conflicts WHERE job_id=? ORDER BY id', (job_id,)).fetchall()

    def list_requests(self, job_id: int) -> list[sqlite3.Row]:
        return self._conn.execute(
            'SELECT * FROM request_log WHERE job_id=? ORDER BY id', (job_id,)).fetchall()
