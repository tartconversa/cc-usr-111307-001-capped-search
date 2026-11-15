# 受截断目录检索的检索协议与采集器

项目保存供方接口协议、演练数据、回环地址服务，以及一个能区分“已证完整范围”与“仍被截断范围”的目录采集器。所有示例记录均使用本机演练资源，不连接实际供应平台。

运行环境为 Python 3.10 或以上，仅使用标准库。

```sh
python -m unittest discover -s tests -v
python -m compileall -q src tests
python -m src.source --port 8765
```

## 供方演练服务

`GET /providers` 返回完整供方清单。`GET /search?start=10&end=12&provider=甲` 返回指定范围，provider 可省略，cap 在响应内。`POST /__control` 的 generation 字段切换目录代际。数据内容来自 `fixtures/source.json`，可在启动服务前调整记录与 cap；也可用 `--data` 指定其他演练数据：

```sh
python -m src.source --port 8765 --data fixtures/source_gap.json
```

`GET /__state` 可查看当前演练状态与已收到的请求。`POST /__control` 只用于本机供方模拟，不属于生产接口。服务端状态不跨进程保留。

演练数据：

- `fixtures/source.json`：基础协议数据，同刻登记的资源分属甲、乙两家，可通过 provider 拆开并取得完整证明。
- `fixtures/source_gap.json`：甲家在同一刻度的资源超过 cap，应报告一个不可继续拆分的缺口，其余刻度仍可证全。

## 采集器解决什么问题

上游 `/search` 只支持半开整数时间区间 `[start,end)` 与单个 provider 过滤，每次最多返回 cap 条并附带 `total`、`generation`，没有翻页或按 id 过滤。因此：

- 导出永远只有前 cap 条，按日期反复搜索会漏掉同一分钟集中登记的资源；
- 去重后的条数只能说明“拿到了多少不同 id”，不能说明“全量已取得”；
- 上游换代后，旧切片与新切片不能拼出完成证明。

采集器（`src/collector/`）用切片树、代际绑定与持久化进度回答“哪些范围已经完整取得、哪些范围仍被截断”。

### 判定与拆分规则（`engine.classify`，纯函数）

对一次切片查询 `(start, end, provider)`，依据声明 `total`、`cap` 与实际响应条数 `n`：

1. `n > cap` 或 `n > total`：响应与声明冲突，切片记 **error**，不标完整。
2. `total == n` 且 `total <= cap`：**complete**。`total == cap` 并非天然截断——实际收齐 cap 条就是完整切片。
3. `total > cap`（截断）：
   - 区间宽于一个刻度：按时间二分为 `[start,mid)`、`[mid,end)`；
   - 已是单刻度且未限定 provider：按 `/providers` 清单拆为每 provider 一个切片；
   - 单刻度 + 单 provider 仍截断：记为 **gap（不可继续拆分的缺口）**，原样保留查询参数、声明 total 与实际取得数。
4. `total <= cap` 却 `total != n`（只可能缺条）：声明与响应冲突，记 **error**。

请求失败、协议非法、未知 provider 同样只能是 error/gap，永远不会被标记为完整。

### 代际绑定与混代防护

- 每次运行先取 `/providers`，把本次作业绑定到响应中的 generation；作业唯一键为 `(base_url, 窗口, generation)`。
- 任何 `/search` 响应的 generation 与绑定值不符：拒绝写入任何资源，切片记 error，记录 `generation_drift` 事件并停止本轮调度（状态 `aborted_generation_drift`）。
- 上游换代后再次运行会为新 generation 创建独立作业；旧代作业、切片、资源原样保留，不参与当代完成证明。
- 同代际内 provider 清单变化属于协议异常，作业中止为 `aborted_providers`。

### 同 id 合并与冲突

id 在 generation 内全局唯一，同一详情可能重复出现在不同 provider。全局按 id 合并时：

- 内容一致：保留一个资源对象，全部来源请求逐条登记在 `record_sources`；
- 内容不一致：写入 `conflicts` 表（既有内容与新来内容都保留），**不覆盖、不任意取一条**，报告中显式暴露。

### 持久化与并发

进度全部落在 SQLite（WAL）：`jobs / slices / records / record_sources / conflicts / request_log / events`。

- 切片状态机：`pending → running → complete | gap | error | split`；`complete`、`gap` 是终态，重启后绝不重抓。
- 写事务用 `BEGIN IMMEDIATE` 串行化，`claim_slice` 是行级原子认领，支持同进程多线程与多进程并发采集不同切片。
- 进程被杀遗留的 `running` 认领在下次启动时回收为 pending；暂停（Ctrl-C）会优雅释放未完成认领。
- 失败切片按 `max_attempts` 重试，重试用尽留 error，下一次运行可继续补齐。

### 完成证明的含义

报告按刻度给出等价证明：窗口 `[start,end)` 的每个刻度，由覆盖它的全部终端叶子切片证明；只有这些叶子**全部 complete** 时该刻度才 `proven_complete`。含 gap/error/pending/running 叶子的刻度只能证明“已取得部分”。整窗全部刻度证全且无冲突阻断时，`fully_proven` 才为真——此时去重 id 数才能被当作全量。

## 命令行用法

```sh
# 采集（可反复执行；同代际续跑，已证完整切片不重抓）
python -m src.collector.cli collect \
  --db work/catalog.db --url http://127.0.0.1:8765 \
  --start 10 --end 13 --workers 4

# 查看覆盖树、逐刻度证明、缺口、冲突与每条资源的来源请求
python -m src.collector.cli report --db work/catalog.db --job 1
python -m src.collector.cli report --db work/catalog.db --job 1 --json

# 导出已取得的去重资源（注意：证明未闭合时它不是全量）
python -m src.collector.cli export --db work/catalog.db --job 1 --out records.jsonl
```

作业结束状态：`complete`（全量证明闭合）、`complete_with_gaps`（可证范围完整，另有不可拆分缺口）、`paused`（暂停，终态切片已保留）、`completed_with_errors`（存在失败切片）、`aborted_generation_drift`（换代混代被拒）、`aborted_providers`、`failed`。

## 代码与测试

- `src/source.py`：供方源实现（支持 `--data` 自定义演练数据）。
- `src/collector/client.py`：HTTP/进程内客户端与协议校验。
- `src/collector/store.py`：SQLite 持久化、切片树、认领、冲突与来源。
- `src/collector/engine.py`：分类判定、查询分割、代际守卫、并发调度。
- `src/collector/report.py`：覆盖树、逐刻度等价证明、缺口与来源报告。
- `tests/test_collector_unit.py`：判定规则、协议解析、持久化与冲突的单元测试。
- `tests/test_collector_integration.py`：完整证明、缺口、暂停续跑、崩溃回收、并发换代不混代、冲突暴露、请求失败与真实 HTTP 通道的端到端测试。
