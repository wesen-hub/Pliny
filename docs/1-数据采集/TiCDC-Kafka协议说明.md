# TiCDC → Kafka canal-json 协议变体说明(enable-tidb-extension)

> 依据:三机集群 TiCDC changefeed 实测抓包(2026-10 部署实证二.9)。
> 适用对象:消费底座(dcp-synclink)、出口解码器(dcp-outlet-rt)、索引适配(dcp-indexer-rt)与所有解析 TiCDC 消息的组件。

## 1. 与标准 canal-json 的差异

TiCDC 开启 `enable-tidb-extension: true` 后,发往 Kafka 的 canal-json 与阿里 Canal 官方协议有三处关键差异:

| 字段 | 标准 Canal | TiCDC 变体 | 说明 |
|------|-----------|-----------|------|
| `ts` | binlog 时间戳(毫秒) | **毫秒时间戳(墙钟域)** | **不是 TSO**。TSO 物理部分是毫秒,但两者数值域不同,直接当 TSO 用会错一个数量级 |
| `_tidb.commitTs` | 无 | **真实 TSO** | TiDB 扩展字段,与 TiKV 提交时间戳同域;需要 TSO 时取这里 |
| `type` | INSERT/UPDATE/DELETE/DDL | 额外含 **TIDB_WATERMARK** | 每秒一条水位事件,非数据行 |

## 2. 真实抓包样例

### 2.1 数据行(UPDATE,主键修改无关场景)

```json
{
  "id": 0,
  "database": "target_db",
  "table": "t_user",
  "pkNames": ["id"],
  "isDdl": false,
  "type": "UPDATE",
  "es": 1791017161953,
  "ts": 1791017283293,
  "sql": "",
  "sqlType": {"id": -5, "user_id": -5, "name": 12, "status": 12, "amount": 3, "seq": -5, "created_at": 93},
  "mysqlType": {"id": "bigint", "user_id": "bigint", "name": "varchar", "status": "varchar", "amount": "decimal", "seq": "bigint", "created_at": "timestamp"},
  "old": [{"id": "1", "user_id": "1001", "name": "alice", "status": "INACTIVE", "amount": "90.50", "seq": "5", "created_at": "2026-10-01 19:57:32"}],
  "data": [{"id": "1", "user_id": "1001", "name": "alice", "status": "INACTIVE", "amount": "90.50", "seq": "6", "created_at": "2026-10-01 19:57:32"}],
  "_tidb": {"commitTs": 469504402903007247}
}
```

要点:

- `ts=1791017283293` 是**毫秒**;`_tidb.commitTs=469504402903007247` 才是 TSO(≈ 2026-10-02 前后)。
- `old`/`data` 是**单元素数组**(TiCDC 已拆事件,每消息一行;主键修改 = DELETE+INSERT 两条)。
- `sqlType` 数值是 JDBC Types 常量(`-5`=BIGINT、`12`=VARCHAR、`3`=DECIMAL、`93`=TIMESTAMP)。
- 顶层 `id=0` 是 canal 协议事件序号,TiCDC 恒 0,**不要当行 ID**。

### 2.2 水位事件(每秒一条,非数据行)

```json
{
  "id": 0,
  "database": "",
  "table": "",
  "pkNames": null,
  "isDdl": false,
  "type": "TIDB_WATERMARK",
  "es": 1791023906203,
  "ts": 1791023907001,
  "sql": "",
  "sqlType": null,
  "mysqlType": null,
  "data": null,
  "old": null,
  "_tidb": {"watermarkTs": 469506170867679235}
}
```

要点:

- `table` 为空、`data` 为 null —— **不是数据行**,只携带水位 `_tidb.watermarkTs`(TSO 域)。
- 频率约 **每秒一条**(changefeed 运行时持续产出)。
- 处理约定:解码器转为 WATERMARK 事件推进水位;消费底座**直接跳过**(推进 offset 游标),不得落死信。

## 3. 各组件处理约定(已落地)

| 组件 | 约定 |
|------|------|
| dcp-outlet-rt `ProductionMessageDecoder` | tso 优先 `_tidb.commitTs` 回退顶层 `ts`;`TIDB_WATERMARK` → WATERMARK,水位取 `_tidb.watermarkTs` |
| dcp-indexer-rt `CanalJson` | 正则同时识别 `commitTs`/`watermarkTs`/`ts`;`isWatermark` 判 `type` |
| dcp-synclink `SyncConsumer` | `isWatermark` 识别后跳过写库、`ackOffsets` 推进游标(避免死信无限增长与假卡 lag) |

## 4. 测试样例要求

- 单测样例**必须用真实抓包形状**(ts=毫秒 + `_tidb.commitTs`=TSO + `old`/`data` 数组),禁止用标准 canal-json 形状替代,否则 ts 域错配会被样例"验证通过"掩盖。
- 参考样例:`dcp-outlet-rt/src/test/java/com/dcp/outlet/rt/ProductionMessageDecoderTest.java`(TIDB_WATERMARK 用例)、`dcp-indexer-rt/src/test/java/com/dcp/indexer/core/CanalJsonTest.java`。

## 5. 历史事故(ts 域错配)

部署实证二.9:批 OK 后流阶段静默无输出——毫秒 ts ≪ TSO 域 T0,会话过滤 `d.tso() > t0` 全部丢弃;TIDB_WATERMARK 被解码为 OTHER → 水位不推进。消费组位点正常前进、无异常日志,属"静默丢数"类故障。修复后 tso 优先 commitTs + 水位识别,新增单测防回归。
