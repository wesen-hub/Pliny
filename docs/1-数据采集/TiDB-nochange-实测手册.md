# TiDB no-change 实测手册(位点即序号方案的唯一待实测关键点)

- 版本: v1.0
- 日期: 2026-09-27
- 关联: 《路径1-位点即序号与全程正确写入-设计方案》§4.2(流路径模板)· §4.3(批路径模板)· §5.3(六场景验收矩阵)
- 配套脚本: `scripts/verify_tidb_nochange.sh`

---

## 1. 要验证什么

设计方案里只有一处结论依赖引擎实测:

> **TiDB 对「ON DUPLICATE KEY UPDATE 赋回原值」是否零 TiKV 写、零 TiCDC 事件?**

- 若**零写零事件** → 流路径模板(4.2)可直接落地,重放被 `IF(号条件)` 确定性吸收,链路零改动;
- 若**产生写** → 流路径模板必须改为「条件 UPDATE 前置判断」(批路径 4.3 形态),设计文档 §4.2 同步修订。

批路径模板(4.3)不依赖此实测:条件 UPDATE 的 `0 行 = 确定性零写入`,是 SQL 语义保证,不受引擎行为影响。

## 2. 原理

TiCDC 只输出 **TiKV 层真正产生变更的行**:

```
SQL 写入 → TiKV(实际字节变化?) → TiCDC changefeed → Kafka
```

`ON DUPLICATE KEY UPDATE c1 = IF(...false..., c1)` 赋回原值时,若 TiKV 将该写优化为 no-op(同值不产生新 MVCC 版本),TiCDC 就感知不到 → Kafka 零消息。验证口径即 **Kafka 端 end-offset 增量**。

## 3. 环境

`docker-compose.outlet.yml` 生产级数据面栈(TiDB×2 / TiCDC×2 / Kafka×3):

| 组件 | 容器 | 宿主端口 |
|---|---|---|
| TiDB(数据面) | dcp-outlet-tidb1 | 14000 |
| TiCDC | dcp-outlet-ticdc1 | 18300 |
| Kafka | dcp-outlet-kafka1 | 19092 |

前置: outlet 集群已启动且 TiDB 就绪。

## 4. 执行

### 4.1 一键执行(推荐)

```bash
# 环境已起时
bash scripts/verify_tidb_nochange.sh

# 环境未起, 脚本自动拉起并等健康
bash scripts/verify_tidb_nochange.sh --up

# 重建 changefeed 与数据, 全量重测
bash scripts/verify_tidb_nochange.sh --reset
```

脚本流程: 建库建表 → 建 changefeed(`cf-nochange-verify` → topic `t_nochange_verify`)→ 复位数据 → 逐步骤执行 SQL → 每步采样 Kafka end-offset → 增量与期望比对 → 输出结论 A/B/C。

### 4.2 手工分步(排查用)

```bash
TIDB=dpc-outlet-tidb1
# TiDB 交互
docker exec -it $TIDB mysql -h127.0.0.1 -P4000 -uroot

# changefeed 状态
docker exec dcp-outlet-ticdc1 /cdc cli changefeed list   --server=http://127.0.0.1:8300
docker exec dcp-outlet-ticdc1 /cdc cli changefeed query  --server=http://127.0.0.1:8300 --changefeed-id=cf-nochange-verify

# Kafka 采样(每步前后各取一次, 差 = 新事件数)
docker exec dcp-outlet-kafka1 /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server kafka1:9092 --topic t_nochange_verify
```

## 5. 测试矩阵

行初始态(复位后): `(1, 'a', 'b', seq=100)`。所有 SQL 均针对 `t_nochange_verify.t1(id PK, c1, c2, seq)`。

| 步骤 | 场景 | SQL 形态 | 期望事件 | 判定语义 |
|---|---|---|---|---|
| T0 | 基线 INSERT | `INSERT INTO t1 VALUES (1,'a','b',100)` | **1** | 阳性对照:链路通 |
| T1 | 同号同值重放 | 流模板(4.2): `IF(VALUES(seq) > seq, ...)` 全 false, 赋回原值 | **0** | **核心**: 零写零事件则流模板可落地 |
| T2 | 同号值变被挡 | 同上, `VALUES(c1)='a2'` 但 `VALUES(seq)=100` 不高于存量 | **0** | 号条件挡倒退, 且零事件 |
| T3 | 高号值变 | 同上, `VALUES(seq)=200`, `VALUES(c1)='a2'` | **1** | 阳性对照:号进值变必写 |
| T4 | 批模板同号重跑 | `UPDATE ... WHERE id=1 AND 200 > seq`(存量 seq=200, 条件 false) | **0** | B 场景: 批重跑同号不写(0 行 = 确定性) |
| T5 | 批模板号进值同 | `UPDATE ... WHERE id=1 AND 400 > seq AND NOT(c1 <=> 'a2')`(号进但值同, 值条件 false) | **0** | E 场景: 全量重发同值零事件(0 行 = 确定性) |

## 6. 判定与后续动作

| 实测结果 | 结论 | 后续动作 |
|---|---|---|
| 全部通过 | **A+B** | 流模板(4.2)与批模板(4.3)均直接落地, §4.2 的"唯一待实测"标记移除 |
| T1/T2 产生事件 | **C-1** | TiDB 未优化同值赋回; 流路径改「条件 UPDATE 前置判断」(参照批路径 4.3), 修订设计文档 §4.2 与后端 `AtomicUpsertDao` 流路径 |
| T0/T3 无事件 | **C-2** | 阳性对照异常: 检查 changefeed 过滤规则(`t_nochange_verify.*`)、Kafka 连通 |
| T4/T5 产生事件 | **C-3** | 批模板条件拼接有误, 检查 SQL 与数据状态, `--reset` 重测 |

## 7. 数据流示例(通过时的实际演算)

```
复位:  DELETE FROM t1 →(DELETE 事件, 不计入)→ 采样点 prev
T0:    INSERT(1,'a','b',100)      → +1  → offset +1
T1:    ON DUP 同值赋回             → +0  → offset 不变   ← TiKV no-op
T2:    ON DUP 值变但号不高于存量    → +0  → offset 不变
T3:    ON DUP 号进值变             → +1  → offset +1
T4:    UPDATE 号条件 false         → 0 行 → offset 不变
T5:    UPDATE 值条件 false         → 0 行 → offset 不变
最终:  offset 总增量 = 2(仅 T0、T3)
```

## 8. 常见问题

1. **`kafka_offsets` 采样为空**: topic 尚无数据,先确认 changefeed 已建(`changefeed list`)且表名命中过滤规则;TiCDC 初始化约 10~15s,首次运行建议 `--reset` 后等待。
2. **T0 即无事件**: changefeed 建在表创建之前、或 `--server` 指向错误实例(应 127.0.0.1:8300 = ticdc1 自身);检查 `changefeed query` 的 `tso` 是否推进。
3. **重复运行**: 脚本幂等(建表/建 changefeed 均 `IF NOT EXISTS`/先查);`--reset` 会删 changefeed 并重建,数据复位靠 `DELETE FROM t1`。
4. **容器未起**: `dcp-outlet-kafka*` 若处于 `Restarting` 循环,先 `docker compose -f docker-compose.outlet.yml down` 再 `up -d`,观察 PD/TiKV 健康后再执行脚本。
