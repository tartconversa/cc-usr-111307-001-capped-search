"""上游协议客户端。

生产接口仅有：
- GET /providers -> {"providers": [...], "generation": int}
- GET /search?start=&end=&provider= -> {"total", "cap", "generation", "records"}

本模块只做协议解析与传输，所有代际判定、完整判定由 engine 负责。
"""
from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Any, Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen


class SourceError(Exception):
    """请求失败（网络、超时、非 2xx），可重试。"""


class ProtocolError(Exception):
    """响应不符合协议（字段缺失/类型错误/结构异常），属于证据冲突，不应静默吞掉。"""


@dataclass(frozen=True)
class SearchResponse:
    total: int
    cap: int
    generation: int
    records: tuple[dict[str, Any], ...]


class SourceClient(Protocol):
    def providers(self) -> tuple[tuple[str, ...], int]: ...
    def search(self, start: int, end: int, provider: str | None) -> SearchResponse: ...


def _require(mapping: dict[str, Any], key: str, typ: type | tuple[type, ...]) -> Any:
    if key not in mapping:
        raise ProtocolError('响应缺少字段 %r' % key)
    value = mapping[key]
    if not isinstance(value, typ):
        raise ProtocolError('字段 %r 类型错误: %r' % (key, type(value).__name__))
    return value


def parse_search(payload: dict[str, Any]) -> SearchResponse:
    """解析 /search 响应并做协议级校验。"""
    total = _require(payload, 'total', int)
    cap = _require(payload, 'cap', int)
    generation = _require(payload, 'generation', int)
    records = _require(payload, 'records', list)
    clean: list[dict[str, Any]] = []
    for record in records:
        if not isinstance(record, dict):
            raise ProtocolError('records 中存在非对象元素')
        rid = record.get('id')
        at = record.get('registered_at')
        if not isinstance(rid, str) or not rid:
            raise ProtocolError('记录缺少合法 id: %r' % (record,))
        if not isinstance(at, int) or isinstance(at, bool):
            raise ProtocolError('记录 %r 缺少整数 registered_at' % rid)
        clean.append(record)
    if total < 0 or cap < 0 or generation < 0 or len(clean) > cap:
        raise ProtocolError('响应数值越界: total=%r cap=%r generation=%r n=%r'
                            % (total, cap, generation, len(clean)))
    return SearchResponse(total=total, cap=cap, generation=generation,
                          records=tuple(clean))


def parse_providers(payload: dict[str, Any]) -> tuple[tuple[str, ...], int]:
    providers = _require(payload, 'providers', list)
    generation = _require(payload, 'generation', int)
    names: list[str] = []
    for name in providers:
        if not isinstance(name, str) or not name:
            raise ProtocolError('providers 中存在非法供方名: %r' % (name,))
        names.append(name)
    if len(names) != len(set(names)):
        raise ProtocolError('providers 清单存在重复供方名')
    return tuple(names), generation


class HttpClient:
    """基于标准库的 HTTP 客户端，仅依赖 urllib。"""

    def __init__(self, base_url: str, timeout: float = 10.0,
                 user_agent: str = 'catalog-collector/1.0'):
        self.base_url = base_url.rstrip('/')
        self.timeout = timeout
        self.user_agent = user_agent

    def _get(self, path: str, params: dict[str, str] | None = None) -> dict[str, Any]:
        target = self.base_url + path
        if params:
            target += '?' + urlencode(params)
        request = Request(target, headers={'Accept': 'application/json',
                                           'User-Agent': self.user_agent})
        try:
            with urlopen(request, timeout=self.timeout) as response:
                raw = response.read()
                status = response.status
        except HTTPError as error:
            raise SourceError('HTTP %s: %s' % (error.code, target)) from error
        except URLError as error:
            raise SourceError('请求失败 %s: %s' % (target, error.reason)) from error
        except TimeoutError as error:
            raise SourceError('请求超时 %s' % target) from error
        if status < 200 or status >= 300:
            raise SourceError('HTTP %s: %s' % (status, target))
        try:
            payload = json.loads(raw.decode('utf-8'))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ProtocolError('响应不是合法 JSON: %s' % target) from error
        if not isinstance(payload, dict):
            raise ProtocolError('响应顶层不是 JSON 对象: %s' % target)
        return payload

    def providers(self) -> tuple[tuple[str, ...], int]:
        return parse_providers(self._get('/providers'))

    def search(self, start: int, end: int, provider: str | None) -> SearchResponse:
        if not start < end:
            raise ValueError('半开区间必须满足 start < end')
        params = {'start': str(start), 'end': str(end)}
        if provider is not None:
            params['provider'] = provider
        return parse_search(self._get('/search', params))


class InProcessClient:
    """直接驱动 src.source.Source 的客户端，供测试与本机演练使用（不经网络）。

    可选 after_request 钩子在每次请求返回后触发，用于注入换代等故障。
    """

    def __init__(self, source, after_request=None):
        self._source = source
        self._after_request = after_request

    def _decode(self, body: bytes) -> dict[str, Any]:
        try:
            payload = json.loads(body.decode('utf-8'))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ProtocolError(str(error)) from error
        if not isinstance(payload, dict):
            raise ProtocolError('响应顶层不是 JSON 对象')
        return payload

    def providers(self) -> tuple[tuple[str, ...], int]:
        status, _, body = self._source.route('GET', '/providers', {}, b'')
        if status != 200:
            raise SourceError('/providers 返回 %s' % status)
        if self._after_request:
            self._after_request('providers', None)
        return parse_providers(self._decode(body))

    def search(self, start: int, end: int, provider: str | None) -> SearchResponse:
        if not start < end:
            raise ValueError('半开区间必须满足 start < end')
        query = 'start=%d&end=%d' % (start, end)
        if provider is not None:
            query += '&provider=' + quote(provider)
        status, _, body = self._source.route('GET', '/search?' + query, {}, b'')
        if status != 200:
            raise SourceError('/search 返回 %s' % status)
        if self._after_request:
            self._after_request('search', (start, end, provider))
        return parse_search(self._decode(body))
