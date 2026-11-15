"""采集器命令行。

用法：
  python -m src.collector.cli collect --db work/catalog.db --url http://127.0.0.1:8765 \
      --start 10 --end 13 [--workers 4]
  python -m src.collector.cli report --db work/catalog.db --job 1 [--json]
  python -m src.collector.cli export --db work/catalog.db --job 1 --out records.jsonl

暂停：collect 运行中按 Ctrl-C，切片回滚为 pending，complete/gap 终态保留；
再次执行同一条 collect 命令即续跑，不会重抓已证完整的切片。
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .client import HttpClient
from .engine import Collector, CollectorError
from .report import build_report, export_records_jsonl, render_text_report
from .store import Store


def _cmd_collect(args: argparse.Namespace) -> int:
    store = Store(args.db)
    try:
        client = HttpClient(args.url, timeout=args.timeout)
        collector = Collector(client, store, args.url, args.start, args.end,
                              max_attempts=args.max_attempts)
        result = collector.run(workers=args.workers)
    except CollectorError as error:
        print('采集中止: %s' % error, file=sys.stderr)
        return 2
    finally:
        store.close()
    print('作业 #%s 代际 gen=%s 结束状态: %s'
          % (result.job_id, result.generation, result.state))
    print('切片计数: %s' % result.counts)
    print('已取得去重资源 id 数: %d' % result.fetched_records)
    if result.reason:
        print(result.reason)
    if result.gaps:
        print('缺口 %d 个:' % len(result.gaps))
        for gap in result.gaps:
            print('  - %s total=%s got=%s cap=%s'
                  % (gap['query'], gap['total'], gap['received'], gap['cap']))
    if result.conflicts:
        print('同 id 内容冲突 %d 处，详见 report' % len(result.conflicts))
    return 0 if result.state in ('complete', 'complete_with_gaps', 'paused') else 1


def _cmd_report(args: argparse.Namespace) -> int:
    store = Store(args.db)
    try:
        job_id = args.job
        if job_id is None:
            jobs = store.list_jobs()
            if not jobs:
                print('数据库中没有作业', file=sys.stderr)
                return 2
            job_id = jobs[-1].id
        report = build_report(store, job_id)
    finally:
        store.close()
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    else:
        print(render_text_report(report))
    return 0


def _cmd_export(args: argparse.Namespace) -> int:
    store = Store(args.db)
    try:
        text = export_records_jsonl(store, args.job)
    finally:
        store.close()
    if args.out:
        Path(args.out).write_text(text + ('\n' if text else ''), encoding='utf-8')
        print('已写出 %s' % args.out)
    else:
        print(text)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='受截断公共目录采集器')
    sub = parser.add_subparsers(dest='command', required=True)

    p_collect = sub.add_parser('collect', help='采集/续跑一个时间窗口')
    p_collect.add_argument('--db', required=True, help='SQLite 进度库路径')
    p_collect.add_argument('--url', required=True, help='上游基址')
    p_collect.add_argument('--start', type=int, required=True, help='窗口起始刻度（含）')
    p_collect.add_argument('--end', type=int, required=True, help='窗口结束刻度（不含）')
    p_collect.add_argument('--workers', type=int, default=1, help='并发 worker 数')
    p_collect.add_argument('--timeout', type=float, default=10.0)
    p_collect.add_argument('--max-attempts', type=int, default=3)
    p_collect.set_defaults(func=_cmd_collect)

    p_report = sub.add_parser('report', help='查看覆盖树、证明、缺口与来源')
    p_report.add_argument('--db', required=True)
    p_report.add_argument('--job', type=int, default=None, help='作业 id，默认最新')
    p_report.add_argument('--json', action='store_true', help='输出机器可读 JSON')
    p_report.set_defaults(func=_cmd_report)

    p_export = sub.add_parser('export', help='导出作业已取得的去重资源 JSONL')
    p_export.add_argument('--db', required=True)
    p_export.add_argument('--job', type=int, required=True)
    p_export.add_argument('--out', default=None)
    p_export.set_defaults(func=_cmd_export)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == '__main__':
    raise SystemExit(main())
