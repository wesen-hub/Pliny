# SeaTunnel 采集侧方案与作业族设计

> 范围:同步链路中 Kafka 之前的段落(采集/转换/输出);Kafka 之后的五段式消费侧见 `2-同步链路/`。
> 状态:作业族模型、源库单点约束分派、判死迁移已**完成代码改造(2026-10,未部署验证)**;CDC 采集、转换、桶路由、停旧启新为已实现;3 节点 Zeta 组网与 H14-H16 演练属部署演练,未执行。落地细节见「九、已落地实现与决策定论」。

## 一、边界与定位

```
源库(binlog) ──▶ SeaTunnel 采集侧(本文档)──▶ Kafka(平台栈主题)──▶ 五段式消费侧(SyncConsumer)
                    ┌────────────────┐
                    │ ① CDC 采集      │
                    │ ② 转换/位点发号  │
                    │ ③ Kafka 输出    │
                    └────────────────┘
```

采集侧正确性契约:**至少一次投递**,允许重放、乱序、秒级中断——全部由下游幂等吸收(写者列裁判),采集侧不承担精确一次语义。

## 二、一源库一作业族(核心组织模型)

### 2.1 作业族定义与层级分离

| 概念 | 粒度 | 说明 |
|---|---|---|
| 页面任务(sync_task) | 字段级 | 维护各自的字段映射、逻辑表、下游配置;多个并存 |
| 作业族(Job Family) | **源库级** | 一个源库 = 一个 SeaTunnel 作业(job-json),承载该源库全部表的采集 |

- 页面任务与作业族是**多对一**;合并的是作业族内的「表配置 + 列裁剪白名单」,页面任务自身不合并;
- 一个作业族 = 一条 binlog dump 流 = 一个 server-id 区间 = 一个 worker 归属。

### 2.2 为什么必须一源库一作业族(多流代价分析)

**MySQL 场景:多流技术上可行,代价不可接受。**

关键机制:binlog dump **不做按表过滤**——主库把完整 binlog 流(所有库表事件)发给消费者,**过滤发生在客户端**。因此:

| 形态 | 结果 |
|---|---|
| 相同 server-id 的两个连接 | **互踢**:主库视作「同一从库身份重连」,断掉旧连接 |
| 不同 server-id 的两个连接 | 两条独立流、独立位点、互不干扰(一主多从即天然多流) |

同源库拆 N 个作业的代价:**同一份 binlog 被完整传输 N 遍 + N 个 dump 线程 + 主库 N 倍 binlog 读取 IO**。数据不会错(各流客户端过滤各自的表),但资源全浪费。

**PG / SQLServer 场景:多流是真的冲突,不是浪费。**

| 源库 | 多流后果 |
|---|---|
| PG | 复制槽排他:同一槽只能一个消费者;同一张表进两个槽 = WAL 被重放两遍,**数据真重复**;槽位点落后还会撑爆磁盘 |
| SQLServer | CDC 捕获实例库级约束,多实例管理同一捕获实例冲突 |

**三类源库,一类是「浪费」、两类是「冲突」——平台统一设计成「一源库一作业族一流」,换来语义统一 + 资源最优。**

### 2.3 作业族生命周期

```
首表创建 ──▶ 建族 + 绑定 worker + 领 server-id 区间
加表/加列 ──▶ 族内配置变更(停旧启新),绑定不动
末表下线 ──▶ 族销毁 + 释放绑定 + 回收 server-id 区间
```

## 三、源库单点约束与分派设计

### 3.1 分派表 source_assign(元库)

```
source_id | worker_id | server_id_range | slot(复制槽) | 状态 | update_time
mysql-01  | worker-2  | 41-44           | -            | RUNNING
pg-01     | worker-3  | -               | slot_cdc_01  | RUNNING
```

- `worker_id`:调度框架中的执行体实例 ID(etcd 注册),表示「该源库作业族跑在哪个 worker」;
- `server_id_range`:从全局分配表领取的 MySQL server-id 区间(见 3.2);
- **uk(source_id) + 先写者胜**:防两个 worker 并发抢同一源库;
- 判死接管时,`worker_id` 更换、`server_id_range` **不变(不漂移)**。

### 3.2 server-id 全局分配表

- 分配单元:**区间**而非单个——单 server-id 会让主库把同作业的多表并行 dump 视为单线程(串行化);区间内多 server-id 让 binlog 事件并行拉取;
- 领取/回收:建族时登记领取,族销毁时回收进池;
- 与既有机制衔接:`CdcStService` 已保证「单 worker 内每 job 唯一」(sidBase 区间),PB 级升级为「元库登记领取 → 平台全局唯一」——分配器从进程内递增改为元库登记。

### 3.3 首绑流程(创建该源库第一个任务时)

```
① 查 source_assign:该源库是否已有归属?
   ├─ 有 ──▶ 并入该 worker 的作业族(job-json 加表)
   └─ 无 ──▶ 选容量最闲的 worker(作业族数 / CPU 余量)
             ├─ 从分配表领取 server-id 区间(登记:源库 → 区间)
             └─ 写 source_assign(源库 → worker,RUNNING)
```

### 3.4 PG / SQLServer 特化

- PG:复制槽注册表——分派前检查槽未被占用;槽随作业族迁移;槽堆积监控(WAL 保留预警);
- SQLServer:CDC 捕获实例登记,同库多实例禁止。

### 3.5 任务三分类判定(页面任务提交入口)

| 类 | 场景 | 处理 |
|---|---|---|
| **A 新表新列** | 源表未在任何作业中 | 新建采集任务 → 分派 worker + 领 server-id 区间 → 表级快照 + 增量(CDC 连接器全自动) |
| **B 后加列** | 源表已在作业中,新任务采它的其他列 | 并入现有作业(改列裁剪白名单 + 停旧启新);增量自然出现,**存量走字段级回补**(按需) |
| **C 列冲突** | 源列已被其他任务采集 | 拦截(uk_flow:同一源列全局唯一一条流);释放路径 = 停用/删除原任务 |

## 四、页面任务 → SeaTunnel 作业的映射

### 4.1 完整流程

```
① 页面向导提交 → sync_task(field_mapping + row_chain + business_key)
② 校验:字段流唯一(uk_flow)+ 结构校验(SyncMappingRules)+ 源列存在性
③ 三分类判定(§3.5)
④ 组装 job-json:
   env(parallelism)
   + source(MySQL-CDC, server-id 区间)
   + transform(CdcSeqTransform 位点透传 + FieldMappingExecutor 列裁剪白名单)
   + sink(Kafka)
⑤ 停旧启新:同 jobId + restart=true → 旧表 checkpoint 续跑、新表快照衔接
⑥ 落库:sea_tunnel_job_id、snapshot_start_position
   (首次 start 读源库当前位点持久化,重放复用保证 S 恒定)
```

### 4.2 停旧启新的代价与零中断变体

- 默认:停旧 → 启新,**秒级采集中断**,下游 Kafka 缓冲吸收,最终无感;
- 零中断变体(可选):先起新作业(新 jobId)拉平增量 → 再停旧(双跑窗口,下游幂等吸收)——默认不做,复杂优先;
- 失败恢复:旧作业不先停的次序错误会导致无作业运行;正确次序 = 新作业起成功后再停旧(或直接同 jobId 带 restart)。

### 4.3 加表 / 加列的 SeaTunnel 侧机制

**加表(表级,CDC 连接器内建,源自 Flink CDC):**

```
作业重启后:
  旧表:checkpoint 续跑增量(不重跑、不重新快照)
  新表:全量快照(chunk 并行)+「边录边放」:
       快照期间该表事件缓存 buffer → 快照完成先按序回放 buffer → 接实时流
  buffer 单位 = 表(tableId);位点 = 作业级统一(共用一个 binlog 流)
  追平条件:回放吞吐 > 表变更速率(单表变更速率 ≪ 作业整体吞吐,秒级~分钟级追平)
  追不平:状态落盘兜底 + 背压告警,宁慢不错
```

**加列(字段级,平台层):**

```
CDC 层无感:表级流一直在采整行,白名单只是裁掉未声明列
  改 field_mapping 白名单 + 停旧启新 → 新列增量从重启起自然出现(零回补)
存量按需:字段级回补(按主键区间分片 + 限速 + 游标断点续跑)
  回补期间该列增量实时直通,重叠由写者列裁判逐列幂等吸收(LWW)
```

**位点体系(与下游衔接的契约):**

| 阶段 | 位点形态 | 序号 |
|---|---|---|
| 增量行 | 逐行透传源库位点 | seq = f(位点)(PositionSeq.encode) |
| 快照行 | 无逐行位点,统一恒定 S | S = f(快照起点位点),批阶段统一序号 |

## 五、迁移触发与故障接管

### 5.1 触发场景全景

| # | 场景 | 绑定是否变动 | 机制 |
|---|---|---|---|
| 1 | **worker 判死**(宕机/网络分区) | 变动 | 判死 → 交接 → 各作业族独立迁移(§5.2) |
| 2 | worker 主动下架/缩容 | 变动 | 排空追平后优雅迁移:不再接新作业,存量作业族追平后迁出 |
| 3 | 容量再均衡 | 原则上**不做** | 迁移代价高(checkpoint 搬迁 + 秒级中断),靠首绑选最闲预防 |
| 4 | 源库下线(最后一个任务删除) | 释放 | 绑定删除 + server-id 区间回收 |
| 5 | 日常加表/加列/改配置 | **不变动** | 只动作业族内 job-json,worker 绑定纹丝不动 |

### 5.2 worker 判死迁移流程(自动,设计态)

```
worker-2 判死(上面挂着 mysql-01、pg-01、mssql-01 三个作业族)
  │
  ① 调度框架双信号判死(lease 过期 + 快照失效,SUSPECT 防误判)→ 确认死亡
  ② 交接状态机:三个作业族【各自独立】选目标 worker(按容量),
     不是整体搬到一个 worker
  ③ 迁移单位 = 作业族:
     - server-id 区间随族迁移(不漂移)
     - source_assign 更新 worker_id
     - PG 复制槽随族迁移
  ④ 平台重提:submitJob(同 jobId + savepoint/restart=true)
  ⑤ SeaTunnel 从 checkpoint 恢复 → 断点续采
  ⑥ 迁移期间该源库采集中断(秒级),下游 Kafka 缓冲,恢复后追平
```

### 5.3 迁移成立的硬前提

1. **checkpoint 共享存储(S3/NFS)**:本地卷迁不动 → 退化为全量重跑;
2. **提交带 savepoint**:重提时带 savepoint 参数,SeaTunnel 才从 checkpoint 恢复;
3. **双通道监控**:作业状态探测 + 数据面 lag——单通道漏检不致命,双通道互备。

### 5.4 三段式 HA 分工

| 层 | 谁完成 | 说明 |
|---|---|---|
| 集群协调层 | Hazelcast(自动) | 节点宕机自动选新主,集群不散 |
| 作业重提层 | **平台**(设计为自动) | SeaTunnel 2.3.x **不会自己重提作业**(作业与节点绑定)——检测判死 + 交接 + 重提在平台侧 |
| 断点恢复层 | SeaTunnel(自动) | 带 savepoint 重提后从 checkpoint 续跑 |

### 5.5 现状 vs 设计(诚实边界)

| 项 | 现状 | 设计 |
|---|---|---|
| 集群形态 | 2 节点(阿里云+腾讯云),H14-H16 未演练 | Zeta 3 节点(Hazelcast 自动选主) |
| checkpoint | 本地命名卷(容器重启不丢,跨机拿不到) | MinIO S3 共享 |
| 重提 | 无自动逻辑(executeCdc 失败仅抛异常,人工重提) | 调度框架判死 + 自动重提 |
| 保底 | restart: unless-stopped 同机自愈 | 全程自动迁移 |

## 六、并行度规划

| 环节 | 并行度 | 依据 |
|---|---|---|
| 增量读取 | **恒 1** | 单 binlog 流保序,不可拆(parallelism>1 只会空转) |
| 快照读取 | chunk 并行(连接器内部) | min(chunk 数, 源库连接余量),与 env parallelism 解耦 |
| 吞吐扩展轴 | 作业族数(源库数) | 族内不追并行度,族间天然并行 |

- 下游天花板:Kafka 分区数(10 万 QPS ÷ 单分区 1 万 = 128~256),先扩分区后扩实例;
- 采集侧单作业族吞吐上限 ≈ 单 worker 采集上限(16C/64G ≈ 2 万 QPS)。

## 七、作业内部数据流(三层机制)

| 层 | 组件 | 职责 |
|---|---|---|
| source | MySQL-CDC 连接器(源自 Flink CDC) | 表级快照(chunk)+ 单 binlog 流 + 表级快照-增量衔接(buffer 回放) |
| transform | CdcSeqTransform(平台补丁) | 位点透传(改源码行末透传源库位点)→ 位点即序号 + 业务键归一(仅严格位点一态) |
| transform | FieldMappingExecutor | 列裁剪白名单:输出 fields = 声明的 modelField 集合(空映射 fail-fast,无全列直通路径) |
| sink | Kafka Sink | `__bucket = MurmurHash3(主键) % 4096`;路由由 Kafka 默认分区器 `murmur2(bucket) % 分区数` 完成——采集端不维护桶→分区映射,扩容取模自动重路由 |

## 八、落地路线(按依赖顺序)

```
① checkpoint 切 S3 共享存储(MinIO)——迁移成立的第一前提 ✅ 已落地(seatunnel.yaml 切 S3,外层 hdfs SPI)
② 3 节点 Zeta 集群组网 + H14/H15/H16 逐项演练 ⏸ 部署演练,未执行(用户明确不做)
③ 平台调度框架接管作业族生命周期(双通道监控 + 判死 + 自动重提) ✅ 已落地(watchdog 族级巡检)
④ source_assign 分派表 + server-id 分配表落地(全局唯一) ✅ 已落地(cdc_job_family/cdc_worker/server_id_pool)
⑤ 一源库一作业族合并改造(现状「每源表一个 job」→ 同源库多表进一个 job-json) ✅ 已落地
⑥ 字段级回补通道(B 类后加列的存量回补) ✅ 已落地(BackfillService)
```

## 九、已落地实现与决策定论(2026-10 代码改造)

> 二~五章的设计模型已从设计态落地为代码实现,落地过程中产生五项决策定论,均已在代码中固化。仅代码改造,**未部署验证**;H14-H16 演练与 3 节点 Zeta 组网未执行。

### 9.1 五项决策定论总览

| # | 决策点 | 定论 | 代码落点 |
|---|---|---|---|
| D1 | job-json 结构 | **一 job 多流水线**:每表独立 source+transform+sink,共享作业级 env/checkpoint/提交生命周期 | `CdcStService.buildFamilyJobConfig` |
| D2 | 源库→worker 绑定落地 | **独立 SeaTunnel 实例(单节点 Zeta)+ 分派表定向提交 REST 端口**(2.3.x Zeta 集群无法指定运行节点,且集群 failover 不自动恢复) | `CdcJobFamilyService` + `SeaTunnelClient` per-baseUrl 重载 |
| D3 | 回补通道形态 | **backend 内回补任务表 + 定时执行**:JDBC 主键区间分片读源表 → 逐列 upsert → 游标断点续跑 → 限速 | `BackfillService` + `sync_backfill_run` |
| D4 | 回补写者身份/seq | **任务自己的写者列 + 回补专用小号 seq=1**(小于一切流号),同列 LWW 裁判 | `BackfillService.BACKFILL_SEQ=1L` + IF 裁判 SQL |
| D5 | 同族 sync_mode 混编 | **允许混编**(用户拍板):族 job 恒 STREAMING,snapshot-only 表 `snapshot.mode=initial_only` 快照完自行结束;全族 autoStop 且 FINISHED 才收敛 | `buildFamilyJobConfig` + `familyAutoStop` |

### 9.2 D1:一 job 多流水线结构

```json
{
  "env": { "job.mode": "STREAMING", "job.name": "cdc-family-{id}",
           "parallelism": 1, "checkpoint.interval": 10000 },
  "source":    [ "cdc_source_0", "cdc_source_1", ... ],   // 每表一个 CDC source
  "transform": [ "cdc_output_0", "cdc_output_1", ... ],   // CdcSeqTransform 一对一
  "sink":      [ "kafka_sink_0", "kafka_sink_1", ... ]    // 每表写 task.getTopic()
}
```

- **env 作业级共享**:parallelism=1(增量单 binlog 流保序)、checkpoint.interval=10000;停/启/恢复都是族级一个动作;
- **server-id 从族区间按表序派生**:`sidBase = rangeStart + idx × 5`,族区间宽度 = 表数 × 5(与旧实现单 job 宽 5 一致);
- **每表 transform 携带本表全部业务参数**(logical_table/task_id/field_mapping/row_chain/business_key_fields/key_columns/bucket_count/snapshot_start_position),合 job 只合并引擎外壳,业务配置表级隔离;
- **sink**:partition_key_fields=["__bucket"]、AT_LEAST_ONCE;
- **多流水线写同一 topic 仅出现在多源合流**:同一逻辑表多源表任务 topic 相同,靠下游写者列裁判吸收跨流水线乱序;
- **CdcSeqTransform 无需改造**:无状态 transform,单表内有序性由「一条 binlog 流 + parallelism=1」保证,跨表无顺序要求,跨源乱序由消费端写者列裁判解决——合并动作对 transform 透明。

### 9.3 D2:独立实例定向提交与判死迁移

**矛盾**:2.3.x Zeta 集群模式下提交作业无法指定运行节点(Hazelcast 自行调度),且待办 1 已实证集群 master 宕机后 job 消失不自动恢复。

**落地链**:

1. `cdc_worker`(worker_id/rest_url/enabled)登记实例;`cdc_job_family` 以 (datasource_id, source_db) 唯一键记录 族→worker_id/server_id_range/sea_tunnel_job_id/running;
2. 首绑:`ensureFamily` → `pickLeastLoadedWorker`(enabled=1 中作业族数最少,并列取登记最早);日常加表/加列只动族内配置,绑定不动;
3. 定向提交:`SeaTunnelClient` per-baseUrl 重载,`startFamily` 时 baseUrl = workerUrl(worker_id),未登记回退默认 URL(兼容旧部署);
4. 判死迁移:watchdog 每 60s 巡检,per-worker `engineReachable` **连续 3 次不可达** → `migrateFamilyAway`:选最闲 worker(排除死 worker)→ 换 worker_id → startFamily 定向重提;**server-id 区间随族迁移、不变不漂移**(防重领撞车互踢);
5. 可恢复性前提:checkpoint 切 MinIO S3 共享存储(外层 `type: hdfs` SPI + plugin-config `storage.type: s3` + `s3a://` bucket 前缀),迁移后新 worker 才能带 savepoint 续跑;
6. 提交入口 nginx LB 为部署层兜底:worker 未登记时 `dcp.seatunnel.url` 默认值指向 LB 入口。

### 9.4 D3/D4:字段级回补通道与写者裁判

**B 类场景**:同一任务后加列——实时 CDC 从加列起才带新列数据,存量行该列需要回补。

**形态(backend 内,零新组件)**:

```
register() 比对旧/新 field_mapping 源列集 → 新增列登记 sync_backfill_run(PENDING, 查重)
  ↓
runPending: @Scheduled(60s) + @SchedulerLock("backfillRun")  ← ShedLock 防多实例重跑
  ↓
executeOne: 读任务行(business_key_fields/key_columns/bucket_count)
  → 逻辑表(table_prefix/table_count/target_database)
  → 写者列 = SyncMappingRules.writerColOf(taskId, datasourceId, sourceDb, sourceTable)
  → 主键区间分片读源表: WHERE (b1,b2) > (v1,v2) ORDER BY bk LIMIT n(仅 MySQL/PG)
  → 每行: businessKey=join("\u0000") → RouteUtil.route(桶/物理表,与实时链路同路由)
  → 逐列 upsert(见下)
  → cursor_json 断点续跑;单轮上限 MAX_BATCHES_PER_ROUND=100 批,跨轮续跑(天然限速)
```

**写者裁判(回补专用小号)**:

```sql
INSERT INTO db.phys (keyCols..., targetCol, writerCol) VALUES (..., ?, 1)
ON DUPLICATE KEY UPDATE
  targetCol = IF(COALESCE(writerCol,0) < VALUES(writerCol), VALUES(targetCol), targetCol),
  writerCol = IF(COALESCE(writerCol,0) < VALUES(writerCol), VALUES(writerCol), writerCol)
```

- seq = BACKFILL_SEQ = 1L,小于一切流号(实时 seq 是位点即序号的编码,恒 > 1);
- 目标行不存在 → INSERT 回补值先落;已有实时值(writerCol > 1)→ 回补判负不动;
- 回补先落、实时后到 → 实时覆盖;实时先写、回补后到 → 回补判负。**最终值恒等于实时值**,回补职责只是填实时链路永远看不到的存量初值;
- 用任务自己的写者列(不新开身份):回补 = 现有 LWW 裁判体系里的「低优先级写者」,与多源合流共用一套裁判,零侵入。

### 9.5 D5:sync_mode 混编(用户拍板)

- 族 job 恒 `job.mode=STREAMING`,mode 差异下沉表级:snapshot-only 表 source 加 `debezium.snapshot.mode=initial_only`,快照完该 source 自行结束,其余流水线继续;
- 收敛条件族级化:`familyAutoStop` = 族内**全部**任务 autoStop 且族 job FINISHED,watchdog 才收敛标记(不重启);只要有增量型任务,族 job 丢失就带 savepoint 重提;主键告警 OPEN 不重启;
- **风险(未实证)**:Zeta 多 source 场景下「单个 source 完成、job 继续跑」的行为未实证,待部署验证;若 Zeta 误判整 job 失败,族会被 watchdog 反复重提——这是 H 演练的第一个验证点。

### 9.6 落地表名映射(设计文档 → 实际落地)

| 设计文档命名 | 实际落地表 | 说明 |
|---|---|---|
| source_assign 分派表 | `cdc_job_family` + `cdc_worker` | 绑定关系拆两表:族表(源库→worker) + worker 实例登记 |
| server-id 全局分配表 | `server_id_pool` | 区间池:领取/切分/回收/自动扩容(POOL_BASE_START=5400, 扩容步长 100) |
| 回补任务表 | `sync_backfill_run` | 状态机 PENDING→BACKFILLING→DONE/FAILED |
| 任务归属 | `cdc_st_task.family_id` | 加列,0=未归属(存量兼容) |

## 十、面试口径(一页纸)

> 「Kafka 之前的采集侧:源库按作业族组织——一源库一作业族,绑定一个 worker、一个 server-id 区间,消灭多流浪费(binlog 按全量流传输、过滤在客户端,N 作业 = N 倍带宽与 dump 线程;PG 槽排他、SQLServer 实例约束更是硬冲突)。页面任务经三分类判定后合并进 job-json,停旧启新提交:旧表 checkpoint 续跑、新表快照回放追平、共用一个 binlog 流;加列则改白名单重启、增量自然出现、存量按需字段级回补。转换层做位点透传(位点即序号)+ 列裁剪白名单;输出层按主键哈希定桶、Kafka 分区器路由。并行度上增量恒 1 保序、吞吐靠作业族横向扩。迁移触发两类:worker 判死(作业族独立迁移、server-id 随迁、checkpoint 续跑)与主动缩容(排空追平);三段式 HA 中平台重提与 S3 checkpoint 已落地代码,3 节点集群选主属部署态未演练。」
