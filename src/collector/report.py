"""覆盖报告：覆盖树（等价证明）、缺口、同 id 冲突与每条资源的来源请求。

完成证明的形式化含义：
对窗口 [start,end) 的每个刻度 t，终端叶子切片的并集恰好覆盖 t；
若该并集中每个叶子均为 complete，则刻度 t 已证全；
含 gap/error/pending/running 叶子的刻度只能证明“已取得部分”，不能计为全量。
"""
from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Any

from .store import Store, build_query

TERMINAL = ('complete', 'gap', 'error', 'pending', 'running')


@dataclass
class TreeNode:
    slice_id: int
    start: int
    end: int
    provider: str | None
    state: str
    depth: int
    total: int | None
    received: int | None
    cap: int | None
    error: str | None
    children: list['TreeNode']

    def to_dict(self) -> dict[str, Any]:
        return {
            'slice_id': self.slice_id,
            'scope': scope_label(self.start, self.end, self.provider),
            'state': self.state,
            'total': self.total,
            'received': self.received,
            'cap': self.cap,
            'error': self.error,
            'children': [child.to_dict() for child in self.children],
        }


def scope_label(start: int, end: int, provider: str | None) -> str:
    base = '[%d,%d)' % (start, end)
    return base if provider is None else base + ' provider=%s' % provider


def build_tree(store: Store, job_id: int) -> TreeNode:
    slices = store.list_slices(job_id)
    nodes = {slc.id: TreeNode(
        slice_id=slc.id, start=slc.start_tick, end=slc.end_tick,
        provider=slc.provider, state=slc.state, depth=slc.depth,
        total=slc.total, received=slc.received, cap=slc.cap,
        error=slc.error, children=[]) for slc in slices}
    roots = []
    for slc in slices:
        if slc.parent_id is None:
            roots.append(nodes[slc.id])
        else:
            nodes[slc.parent_id].children.append(nodes[slc.id])
    if len(roots) != 1:
        raise RuntimeError('作业 %s 切片树异常: 根节点数=%d' % (job_id, len(roots)))
    return roots[0]


def render_tree(node: TreeNode, lines: list[str] | None = None,
                prefix: str = '') -> list[str]:
    if lines is None:
        lines = []
    stats = ''
    if node.state in ('complete', 'gap', 'error') and node.total is not None:
        stats = ' total=%s got=%s cap=%s' % (node.total, node.received, node.cap)
    marker = {'complete': '✓', 'gap': '✗缺口', 'error': '!错误',
              'pending': '·待采', 'running': '~进行中', 'split': '┬拆分'}.get(
        node.state, node.state)
    lines.append('%s%s %s [%s]%s' % (
        prefix, marker, scope_label(node.start, node.end, node.provider),
        node.state, stats))
    if node.error:
        lines.append('%s    └─ %s' % (prefix, node.error))
    for index, child in enumerate(node.children):
        last = index == len(node.children) - 1
        render_tree(child, lines, prefix + ('   ' if last else '│  '))
    return lines


def _terminal_leaves(node: TreeNode) -> list[TreeNode]:
    if not node.children:
        return [node]
    leaves: list[TreeNode] = []
    for child in node.children:
        leaves.extend(_terminal_leaves(child))
    return leaves


def tick_coverage(store: Store, job_id: int) -> dict[str, Any]:
    """逐刻度的等价证明：列出覆盖该刻度的全部终端叶子及其状态。"""
    job = store.get_job(job_id)
    leaves = _terminal_leaves(build_tree(store, job_id))
    result: dict[str, Any] = {}
    for tick in range(job.start_tick, job.end_tick):
        # provider 是同一刻度内的并行拆分，覆盖该刻度的全部叶子都要计入。
        covering = [leaf for leaf in leaves if leaf.start <= tick < leaf.end]
        states = sorted({leaf.state for leaf in covering})
        proven = bool(covering) and all(leaf.state == 'complete' for leaf in covering)
        blockers = sorted({leaf.state for leaf in covering
                           if leaf.state != 'complete'})
        result[str(tick)] = {
            'proven_complete': proven,
            'leaf_states': states,
            'blockers': blockers,
            'leaves': [scope_label(leaf.start, leaf.end, leaf.provider)
                       for leaf in covering],
        }
    return result


def build_report(store: Store, job_id: int) -> dict[str, Any]:
    job = store.get_job(job_id)
    tree = build_tree(store, job_id)
    leaves = _terminal_leaves(tree)
    counts = store.slice_state_counts(job_id)
    gaps = []
    for leaf in leaves:
        if leaf.state == 'gap':
            gaps.append({
                'slice_id': leaf.slice_id,
                'query': build_query(leaf.start, leaf.end, leaf.provider),
                'scope': scope_label(leaf.start, leaf.end, leaf.provider),
                'total': leaf.total,
                'received': leaf.received,
                'cap': leaf.cap,
                'missing_at_least': (leaf.total or 0) - (leaf.received or 0),
            })
    coverage = tick_coverage(store, job_id)
    proven_ticks = [tick for tick, info in coverage.items()
                    if info['proven_complete']]
    conflicts = []
    for row in store.list_conflicts(job_id):
        conflicts.append({
            'rid': row['rid'], 'slice_id': row['slice_id'],
            'existing': json.loads(row['existing_payload_json']),
            'incoming': json.loads(row['incoming_payload_json']),
            'detected_at': row['detected_at'],
        })
    provenance = []
    for row in store.list_sources(job_id):
        provenance.append({
            'rid': row['rid'], 'query': row['request_query'],
            'provider': row['provider'], 'generation': row['generation'],
            'slice_id': row['slice_id'], 'fetched_at': row['fetched_at'],
        })
    requests = [{
        'kind': row['kind'], 'query': row['request_query'],
        'generation': row['generation'], 'claimed_total': row['claimed_total'],
        'received': row['received'], 'status': row['status'],
        'detail': row['detail'], 'at': row['at'],
    } for row in store.list_requests(job_id)]
    events = [{
        'kind': row['kind'], 'detail': json.loads(row['detail_json']),
        'at': row['at'],
    } for row in store.list_events(job_id)]
    all_complete = bool(leaves) and all(leaf.state == 'complete' for leaf in leaves)
    return {
        'job': {
            'id': job.id, 'base_url': job.base_url,
            'window': [job.start_tick, job.end_tick],
            'generation': job.generation,
            'providers': list(job.providers), 'state': job.state,
        },
        'proof': {
            'fully_proven': all_complete,
            'proven_ticks': proven_ticks,
            'tick_coverage': coverage,
            'terminal_leaf_count': len(leaves),
            'slice_state_counts': counts,
        },
        'tree': tree.to_dict(),
        'gaps': gaps,
        'conflicts': conflicts,
        'records': {
            'distinct_id_count': store.count_records(job_id),
            'provenance': provenance,
        },
        'requests': requests,
        'events': events,
    }


def render_text_report(report: dict[str, Any]) -> str:
    job = report['job']
    proof = report['proof']
    lines = []
    lines.append('作业 #%s  %s  窗口[%d,%d)  代际 gen=%s  状态=%s'
                 % (job['id'], job['base_url'], job['window'][0], job['window'][1],
                    job['generation'], job['state']))
    lines.append('供方清单: %s' % ', '.join(job['providers']))
    lines.append('')
    lines.append('全量证明: %s（已证刻度 %s，终端叶子 %d）'
                 % ('闭合' if proof['fully_proven'] else '未闭合',
                    proof['proven_ticks'] or '无',
                    proof['terminal_leaf_count']))
    lines.append('切片状态: %s' % proof['slice_state_counts'])
    lines.append('去重后资源 id 数: %d（仅在全量证明闭合时才等于全量）'
                 % report['records']['distinct_id_count'])
    lines.append('')
    lines.append('覆盖树（等价证明）:')
    lines.extend(render_tree_dict(report['tree']))
    lines.append('')
    if report['gaps']:
        lines.append('不可继续拆分的缺口:')
        for gap in report['gaps']:
            lines.append('  - slice#%s %s 声明 total=%s，实得 %s，cap=%s，至少缺 %s 条'
                         % (gap['slice_id'], gap['scope'], gap['total'],
                            gap['received'], gap['cap'], gap['missing_at_least']))
            lines.append('    查询参数: %s' % gap['query'])
    else:
        lines.append('缺口: 无')
    if report['conflicts']:
        lines.append('')
        lines.append('同 id 内容冲突（未自动取舍）:')
        for conflict in report['conflicts']:
            lines.append('  - rid=%s slice#%s'
                         % (conflict['rid'], conflict['slice_id']))
            lines.append('      既有: %s' % json.dumps(conflict['existing'],
                                                       ensure_ascii=False, sort_keys=True))
            lines.append('      新来: %s' % json.dumps(conflict['incoming'],
                                                       ensure_ascii=False, sort_keys=True))
    lines.append('')
    lines.append('资源来源请求（共 %d 条来源记录）:'
                 % len(report['records']['provenance']))
    for item in report['records']['provenance']:
        provider = '全供方' if item['provider'] is None else item['provider']
        lines.append('  - %s <- %s via %s (gen=%s, slice#%s)'
                     % (item['rid'], item['query'], provider,
                        item['generation'], item['slice_id']))
    return '\n'.join(lines)


def render_tree_dict(node: dict[str, Any], lines: list[str] | None = None,
                     prefix: str = '') -> list[str]:
    """直接渲染 JSON 形态的树，避免从 scope 字符串反解。"""
    if lines is None:
        lines = []
    stats = ''
    if node['state'] in ('complete', 'gap', 'error') and node['total'] is not None:
        stats = ' total=%s got=%s cap=%s' % (node['total'], node['received'], node['cap'])
    marker = {'complete': '✓', 'gap': '✗缺口', 'error': '!错误',
              'pending': '·待采', 'running': '~进行中', 'split': '┬拆分'}.get(
        node['state'], node['state'])
    lines.append('%s%s %s [%s]%s' % (
        prefix, marker, node['scope'], node['state'], stats))
    if node['error']:
        lines.append('%s    └─ %s' % (prefix, node['error']))
    for index, child in enumerate(node['children']):
        last = index == len(node['children']) - 1
        render_tree_dict(child, lines, prefix + ('   ' if last else '│  '))
    return lines


def export_records_jsonl(store: Store, job_id: int) -> str:
    rows = []
    for record in store.list_records(job_id):
        rows.append(json.dumps(record, ensure_ascii=False, sort_keys=True))
    return '\n'.join(rows)
