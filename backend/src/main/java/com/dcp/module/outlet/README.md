# 变更流出口服务(骨架 v0.1)

实现依据:《数据采集平台V3-变更流出口设计-2026-09-14》——
**§5.5 出口职责 / §6 统一消费模型(§6.1 抽象 · §6.3 执行器 B · §6.4.1 批↔流衔接 · §6.4.2 边界规则)/
§6.5 快照段 · §6.6 增量段 · §6.8 位点 / §7 权限与治理 / §6.9 六个接口**。

## 定位

平台对下游只承诺两件事:**一条带位点的统一数据流** + **一个统一的权限执行点**。
出口服务就是这两件事的唯一实现点:一份变更(L1 内部流)→ 按契约与权限投影 → L2 流 / L3 接口。

## 包结构

| 文件 | 职责 | 对应设计节 |
|---|---|---|
| `OutletController` | 6 个 L3 接口 + 调试接口 | §6.9 |
| `ChangeOutletService` | 编排:契约/权限 → 位点校验 → 取数 → 投影 → 配额 → 审计 | §5.5 / §6 |
| `OutletPermissionEngine` | 五道判定(流/列/行/密级)+ 投影(**越权列连 key 都不出现**) | §7.2 / §7.3 |
| `OutletQuotaLimiter` | QPS / 日行数 / 并发 → `QUOTA_EXCEEDED` | §7.2 ⑤ |
| `OutletAuditService` | `sys_operation_log`(module=`chg-outlet`),记裁剪/脱敏 | §7.2 审计 |
| `OutletRegistry` | 消费方 + 契约(内存版,字段对齐 DDL 的 `consumer`/`contract`) | §7.5 |
| `OutletErrorCode` | 6xxx 错误码(含边界三规则错误码) | §6.4.2 |
| `model/*` | `ChangeEvent` / `ChangeBatch` / `StreamStatus` / `SnapshotPage` / `PositionKey` / `ConsumerAccount` / `Contract` | §6.8 / §6.9 |
| `source/ChangeStreamSource` | **数据源 SPI**(唯一可替换点) | §6.1 |
| `source/MockChangeStreamSource` | 骨架期内存实现(可跑通全部语义) | — |

## 接口一览

```
GET  /api/chg/{stream}/stream-status?consumerId=
GET  /api/chg/{stream}/snapshot?consumerId=&boundaryTso=&pageToken=&limit=
GET  /api/chg/{stream}/changes?consumerId=&after=&limit=
GET  /api/chg/{stream}/key/{bizKey}?consumerId=&asOf=
GET  /api/chg/{stream}/position-at?consumerId=&time=
POST /api/chg/{stream}/commit?consumerId=&position=
# 调试(mock 数据源专用)
POST /api/chg/{stream}/_mock/append?op=INSERT   (body=一行 JSON)
GET  /api/chg/_debug/registry
GET  /api/chg/{stream}/_debug/seen?consumerId=
```

鉴权:骨架支持 `X-Dcp-Token`(等价于 consumerId)或 `?consumerId=`;**生产改为契约 token 解析消费方身份**(§7.5,`contract.token_hash`)。

## 已实现的语义(骨架即验)

1. **位点不透明**:`v1.{epoch}.{tso}` → base64url,消费方只存/回传(`PositionKey`);
2. **边界三规则**:`T0 < earliest` → `BOUNDARY_BELOW_STREAM_START`;`T0 > latest` → `BOUNDARY_IN_FUTURE`;GC 过期由数据源把 `earliest` 收敛后自动生效(§6.4.2);
3. **epoch 拦截**:位点 epoch ≠ 流 epoch → `EPOCH_MISMATCH`(强制重快照,绝不静默续);
4. **位点过期**:`position < earliest` → `POSITION_EXPIRED`(消费方应回落重快照);
5. **权限投影**:契约 ∩ 主体(存在 `DataSecurityService` 时再收窄一层);越权列**连 key 都不出现**;密级超限按 `maskRules` 脱敏;行条件不满足 → 事件转 `DELETE`(条件退出);
6. **快照与增量同一套判定**(铁律 3):两条路径共用 `projectRow/projectEvent`;差别只在行条件不满足时——**增量段发 `DELETE` 墓碑**(消费方可能已有该行),**快照段直接不返回**(消费方还没有),两者终态一致。
7. **WATERMARK 透出**:`ChangeBatch.watermarkTs`,供消费方判追平 / 按 TSO 切批 / 去重;
8. **平台只观测位点**:`commit` 只写 `consumer_offset_seen` 语义的内存观测,权威位点在消费方自己的 `consumer_offset` 表(决策 1)。

## 运行

```bash
# 默认 dcp.outlet.source=mock(无需 TiDB/TiCDC 即可跑)
curl -s 'http://localhost:8081/api/chg/t_order/stream-status?consumerId=demo-sub' | python3 -m json.tool
# 造一条变更
curl -s -X POST 'http://localhost:8081/api/chg/t_order/_mock/append?op=INSERT' \
  -H 'Content-Type: application/json' \
  -d '{"id":"9","name":"delta","amount":"40.00","region":"CN","phone":"13800000009"}'
# 增量拉取
curl -s 'http://localhost:8081/api/chg/t_order/changes?consumerId=demo-sub&limit=10' | python3 -m json.tool
```

## 待建(下一步,按设计节顺序)

| # | 待建 | 依据 | 说明 |
|---|---|---|---|
| 1 | `OutletRegistry` → Mapper 版 | §7.5 / DDL | 读写 `consumer` / `contract`;token 短时化 + 吊销 |
| 2 | `KafkaChangeStreamSource` | §5 / §6 | 消费 L1 `dcp.cdc.{schema}.{table}`,canal-json 解析(整行 + old + `_tidb.commitTs` + WATERMARK),`AS OF TIMESTAMP` 走快照只读入口 |
| 3 | `snapshot_job` 登记与 TTL | §6.5 / DDL | 发首页即登记 boundary;过期作废并要求重来 |
| 4 | 集中配额计数 | §7.2 | 多实例下内存计数 = N 倍配额,需 Redis/计数表 |
| 5 | 行级条件下推 + 脱敏执行器 | §7.6 | 现为契约侧简谓词;生产接 `DataSecurityService` 行规则并下推 |
| 6 | 消费方 SDK(执行器 A) | §6.2 | 位点表 + 同事务提交 + 幂等 upsert + 退避重试 + 自动重快照 |
| 7 | 执行器 B 作业模板 | §6.3 | Spark bootstrap / 增量批 + manifest 原子发布 + 位点收口 |
