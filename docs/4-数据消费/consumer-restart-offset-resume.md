# 消费者重启位点续传设计(消费底座)

> 状态:已拍板、已落地(2026-10-13)。适用模块:dcp-outlet-rt(现行计算面)。
> 同步链路(dcp-synclink)语义已等价,无需改动;旧版 outlet-worker/outlet-adapters 未部署,不随本次改造。

## 1. 背景与问题

消费底座消费者(KafkaRecordSource)使用 assign + seek + commitSync:

- **运行期进度记录**:每批处理完 `commitSync(分区, 已写位置+1)` 提交到 Kafka `__consumer_offsets`,键 = (group.id, topic, partition)。每个消费任务(索引器/打标/湖同步等)有独立的 group.id,进度天然隔离。
- **启动期起点注入(改前)**:单元启动仅 `seek(任务单 bizMeta 里的 T0 起点表)`,**不查询 Kafka 已提交位点**。

改前的真实行为与风险:

1. 任务单起点表是批流切换(activate)时控制面按 T0 栅栏反算写死的,etcd 中运行期从不更新。增量阶段跑半年后该 offset 早被 Kafka retention 清理,**"重启从 T0 重扫"在物理上不可能发生**。
2. 实际行为是 Kafka 客户端收到 OffsetOutOfRangeException 后的**隐式兜底**:优先查该 group.id 的 committed 位点续读(半年里 commitSync 每批提交,账是新的),因此"碰巧"能续传。
3. 真正危险的是隐式兜底的下一级:KafkaRecordSource **未显式配置 auto.offset.reset(默认 latest)**。一旦 committed 不可用(首次启动未提交过、提交记录过期(Kafka 默认保留 7 天)、组名变化),客户端**静默跳到分区最新,停机窗口数据丢失**——比重复消费严重得多。

## 2. 设计定论

**启动起点 = max(Kafka 已提交位点 committed,任务单 T0 起点),一条公式替代 if-else;committed 查询不到按不存在处理(值为 null)。**

```
单元启动(每个分区):
  1. assign 本分区(已有)
  2. 查 committed(group.id + topic + partition)   <- 新增:consumer.committed(tp)
  3. start = max(committed ?? 0, 任务单 startOffsets[p] ?? 0)
  4. seek(p, start)                                <- 原逻辑改为 max 后注入
  5. poll 循环(不变)
```

**第三级兜底**:committed 与任务单起点**都被清理**时(停机超过 retention + 增量期很长),poll 触发 out-of-range,由 **auto.offset.reset=earliest(显式配置)** 从分区最早位置重扫,由出口幂等吸收重复。宁可重读,绝不静默跳最新。

**查询失败降级**:`committed()` 查询异常(如 broker 暂不可达)视为 null,回退任务单起点——语义退化为 at-least-once(重复无害),不阻塞启动。

## 3. 场景验证表

| 场景 | committed | 任务单 T0 起点 | 实际起点 | 语义 |
|---|---|---|---|---|
| 批流衔接首启,还没提交过 | null | 有效 | T0 起点 | 正确(设计初衷) |
| 增量运行中正常重启(任何时长) | 有效(最新) | 已清理/有效均可 | committed | **续传,几乎零重放** |
| 停机超 7 天,提交记录过期 | null | 可能已清理 | T0 或最早 | T0 还在则从 T0 重扫;T0 也没了走 earliest 重扫,幂等吸收 |
| 分区扩容后的新分区 | null(新分区无历史) | 扩容时算好的新起点 | 新分区起点 | 正确(扩容已写入 bizMeta) |
| committed 查询异常 | 视为 null | 有效 | T0 起点 | 降级 at-least-once,重复无害 |
| 脏数据 committed < T0(不应发生) | 小于 T0 | 有效 | T0(max 保护) | 位点永不后退 |

## 4. 实现落点(2026-10-13)

| 文件 | 改动 |
|---|---|
| dcp-outlet-rt/RecordSource.java | 接口加 `default Long committed(int partition) { return null; }`(Kafka 特有查询,默认不支持,测试桩无破坏) |
| dcp-outlet-rt/KafkaRecordSource.java | ① 显式 `AUTO_OFFSET_RESET_CONFIG = "earliest"`;② 实现 `committed(partition)`:`consumer.committed(tp)`,无记录/异常返回 null |
| dcp-outlet-rt/UnitRunner.java | 起点注入段:`start = max(committed, 任务单起点)` 后 seek;日志区分「续传」与「任务单起点启动」 |

## 5. 边界说明

- **group.id 稳定性是前提**:组名 = consumer_task.consumer_group(任务级),OutletComponents 工厂后缀为空,重启/迁移不换组名。若未来引入单元级后缀,须同步重算 committed 归属,否则续传失效。
- **同步链路(dcp-synclink SyncWriterExecutor)**:无显式 seek,启动靠 Kafka 自动读该 group 的 committed(有效则续读);已显式配置 `auto.offset.reset=earliest`(查不到则从最早)。语义与本文档一致,不改代码。
- **旧版 outlet-worker/outlet-adapters**:未部署(compose 仅挂载 dcp-outlet-rt bundle),不随本次改造;若重新启用需同步对齐本文档三处改动。
