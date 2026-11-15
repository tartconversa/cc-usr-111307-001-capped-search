"""受截断目录采集器。

模块划分：
- client: 上游 HTTP 协议客户端与响应模型
- store: SQLite 持久化（切片树、资源、来源、冲突、请求日志、事件）
- engine: 查询分割、代际守卫、并发认领与完整/缺口判定
- report: 覆盖树、完成证明、缺口与资源来源报告
- cli: 命令行入口
"""
from .client import HttpClient, ProtocolError, SearchResponse, SourceError
from .engine import Collector, classify
from .store import Store

__all__ = [
    'Collector', 'HttpClient', 'ProtocolError', 'SearchResponse',
    'SourceError', 'Store', 'classify',
]
