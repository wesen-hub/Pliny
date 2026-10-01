# dcp-scheduler — 统一调度框架(内核)

基于 etcd 的通用分区调度与线程托管框架(Java 独立进程)。设计文档见
`docs/5-统一调度框架/`(总纲 + 8 个子文档,含 ⑧ 手动钉住与锁机制)。

## 角色与职责边界(内核拍板)

| 关注点 | 边界 |
| --- | --- |
| 位点管理 | **框架不管**。crash 后新实例从提交位点续;重放幂等由消费者自处理 |
| 提交节奏 | 消费者自控(逐条/攒批),框架只拉四计数器快照 |
| 消费逻辑 | 业务执行器(PartitionExecutor 契约:start/stop/metrics) |
| 调度 | 框架:补 PENDING → 打分分配(CAS)→ 判死交接(AGGRESSIVE/CONSERVATIVE) |
| 存量迁移 | 不考虑(开发阶段直接切新方案) |

## 工程结构

```
src/main/java/com/dcp/scheduler/
├── model/   核心模型(TaskSpec/PartRecord/InstanceRecord/NsInstanceRecord/ExecutorSnapshot/AlertEvent/ScoringConfig/...)
├── store/   etcd 封装(KeySpace 键空间 / Json / EtcdStore:lease/watch/CAS/竞选)
├── alloc/   调度纯库(Scoring 打分 / Allocator 分配)
├── exec/    执行器契约(PartitionExecutor 接口 + ExecutorProvider 装载)
├── alert/   告警通道(AlertSink:去重/恢复/滚动 200 条事件流)
├── skeleton/ 实例骨架(InstanceConfig 启动参数 / InstanceSkeleton 多ns注册+对账 / SnapshotReporter)
├── master/  调度主(MasterLoop 竞选 + 决策循环 + 判死交接 + 告警巡检)
└── smoke/   冒烟(SmokeMain + MockExecutor)
```

## 构建

```bash
mvn -s settings.xml package -DskipTests    # 腾讯云镜像(settings.xml)
# 产物: target/dcp-scheduler-0.1.0.jar(shade, mainClass=SmokeMain)
```

## 运行 smoke(本地闭环验证)

```bash
# 1. 起 etcd
docker run -d --name dcp-etcd -p 2379:2379 \
  -e ALLOW_NONE_AUTHENTICATION=yes quay.io/coreos/etcd:v3.5.16 \
  etcd --advertise-client-urls http://0.0.0.0:2379 --listen-client-urls http://0.0.0.0:2379

# 2. 跑实例(同时参选调度主,自动提交冒烟任务 orders/4 分区)
java -jar target/dcp-scheduler-0.1.0.jar --etcd http://localhost:2379 --inst inst-a --ns demo

# 3. 故障接管验证:另起实例(竞选失败监督),kill -9 主实例,观察接管
java -jar target/dcp-scheduler-0.1.0.jar --inst inst-b --ns demo --no-submit
pkill -9 -f -- "--inst inst-a"
# 预期:leader 键 30s 过期 → inst-b 竞选当选 → 保守双窗口(30s)判死 → 交接中间态
#       持久化换主可续 → 回收 PENDING → 分配 → inst-b 装载执行器恢复消费

# 4. 锁机制验证(子文档⑧):
#    a. 钉住 + TTL 自愈:主实例钉住 p0(90s)→ kill -9 → 观察 p0 锁保护/到期解锁接管
java -jar target/dcp-scheduler-0.1.0.jar --inst inst-a --ns demo --pin 0:90
java -jar target/dcp-scheduler-0.1.0.jar --inst inst-b --ns demo --no-submit
pkill -9 -f -- "--inst inst-a"
#    预期:p1-3 正常判死回收;p0 持续告警"锁未过期保持不回收";TTL 到期→解锁→回收→接管
#    b. 定向迁移:运维后台写 MIGRATE_PIN op(p0 → inst-c),源主动停快通道立即推进
java -jar target/dcp-scheduler-0.1.0.jar --inst inst-c --ns demo --no-submit --migrate-pin 0:inst-c
#    预期:MIGRATING → 源停止(stopped 快照)→ 立即回收 → 定向分配(钉住定向)→ 锁保留在目标

# 5. 卡死检测与再平衡验证:
#    a. 卡死检测:inst-a 标记 p0 执行器卡死(快照停更),宽限 30s 后自动迁移该分区
java -jar target/dcp-scheduler-0.1.0.jar --inst inst-a --ns demo --stuck 0
java -jar target/dcp-scheduler-0.1.0.jar --inst inst-b --ns demo --no-submit
#    预期:保守交接开启(起因=stuck)→ 源停 → 回收 → 分配到健康实例恢复消费
#    b. 再平衡:新实例入池后失衡(占用率差>0.3 持续 3 周期)→ 步进迁移(单轮≤1 分区)
java -jar target/dcp-scheduler-0.1.0.jar --inst inst-a --ns demo
java -jar target/dcp-scheduler-0.1.0.jar --inst inst-b --ns demo --no-submit
#    预期:再平衡迁移(最忙>0.8)→ MIGRATING+临时 TTL 锁 → 定向分配 → 锁到期自动解锁

# 6. 自动扩缩容验证(水位=flow/(parts×targetRps),任务声明 mock 钩子 + 短冷却):
#    a. 扩容:每分区 2条/500ms(flow≈16),targetRps=1 → 水位 4.0>0.8 持续 3 周期 → 尾部补分区翻倍
java -jar target/dcp-scheduler-0.1.0.jar --inst inst-a --ns demo \
  --autoscale --rate 2 --target-rps 1 --partitions 4 --capacity 8 --observe 100
#    预期:扩容 4→8(desire 受 maxPartitions 箝位)→ 新 PENDING 全部分配 → 水位回落由负反馈收敛
#    b. 缩容:同样的流量,targetRps=100 → 水位 0.04<0.4 持续 6 周期 → 逐轮尾部缩 1 分区
java -jar target/dcp-scheduler-0.1.0.jar --inst inst-a --ns demo \
  --autoscale --rate 2 --target-rps 100 --partitions 4 --observe 170
#    预期:钩子 routeCut(切路由→流量停)→ DELETING 追平(rps 连续归零)→ 清记录
#          4→3→2→1 收敛到 minPartitions,删尾部分区不被 ensureParts 补回

# 7. 任务注销验证:跑 15s 后删 spec + 停生产(模拟外壳注销)→ DRAINING → 分区追平 → GONE
java -jar target/dcp-scheduler-0.1.0.jar --inst inst-a --ns demo --drain-task --drain-after 15 --observe 55
#    预期:status=DRAINING → 分区 DELETING(线程继续追平)→ 生产停后 rps 归零 → 清全部 parts
#          → status=GONE + events/gone(外壳 watch 后释放业务资源)

# 8. 实例下架验证:DRAIN_INSTANCE op → 不接新单,已有分区逐步迁出 → 迁空可安全关机
java -jar target/dcp-scheduler-0.1.0.jar --inst inst-a --ns demo --drain-inst inst-a --observe 80
java -jar target/dcp-scheduler-0.1.0.jar --inst inst-b --ns demo --no-submit --observe 80
#    预期:下架意图登记 → 每轮迁 1 个分区至 inst-b(MIGRATING 交接)→ "已迁空,可安全关机"

# 9. cpu 驱逐验证(慢实例迁移):inst-a 固定进程 cpu 85>80 持续 3 周期 → 逐轮迁出至 inst-b
java -jar target/dcp-scheduler-0.1.0.jar --inst inst-a --ns demo --mock-cpu 85 --observe 70
java -jar target/dcp-scheduler-0.1.0.jar --inst inst-b --ns demo --no-submit --observe 70
#    预期:cpu 驱逐(与槽位失衡正交,槽位不失衡也迁)→ 迁出其 rps 最低分区 → 全部分区到 inst-b
#          目标选择跳过 cpu>80 实例;槽位失衡(占用率>0.8 差>0.3)路径不变

# 10. lag 实查验证(追平判定升级):注入 mock Kafka 源,endOffset 实查 → lag 归零才清记录
java -jar target/dcp-scheduler-0.1.0.jar --inst inst-a --ns demo --drain-task --drain-after 15 --lag --observe 40
java -jar target/dcp-scheduler-0.1.0.jar --inst inst-a --ns demo --autoscale --rate 2 --target-rps 100 --lag --observe 95
#    预期:注销/缩容追平清记录日志带 "lag=0"(非 rps 回退);无 --lag 时日志为 "(rps 回退)" 护栏路径

# 11. 告警事件流验证(监控子文档 §六:主写 /dcp/{ns}/alerts/{alertId},后台页面可见):
#    a. 执行器卡死:宽限 30s 后自动迁移留痕 → EXEC_STUCK 事件(critical, 实例+任务+分区全维度)
java -jar target/dcp-scheduler-0.1.0.jar --inst inst-a --ns demo --stuck 0 --observe 80 --alerts
#    b. 错误率巡检:错误率 >1% 持续 3min → ERROR_RATE 事件(critical, 差分相邻帧)
java -jar target/dcp-scheduler-0.1.0.jar --inst inst-a --ns demo --err-rate 0.5 --observe 200 --alerts
#    预期:观察行 "告警事件=N";etcdctl get /dcp/demo/alerts/ --prefix 可见事件(10min 去重,恢复时同
#          alertId 覆盖写 resolvedAt;滚动保留 200 条);后台页面 /monitor/alerts 实例维度聚合展示

# 12. 运维 op 验证(运维后台设计):暂停接单 / 调权 / 取消下架
java -jar target/dcp-scheduler-0.1.0.jar --inst inst-a --ns demo --pause-inst inst-b --set-weight inst-b:1.5 --undrain inst-b --observe 60
java -jar target/dcp-scheduler-0.1.0.jar --inst inst-b --ns demo --no-submit --observe 60
#    预期:主侧读意图子键(/dcp/{ns}/instances/{id}/paused|weight) overlay;paused 实例不接新单
#          (已跑分区不动);weight 0.5~2.0 限幅参与迁移目标打分;undrain 后恢复接单
```

参数:`--etcd`(默认 http://localhost:2379)、`--inst`(实例 id)、`--ns`(namespace)、
`--task`/`--partitions`(冒烟任务)、`--capacity`(本 ns 槽位,扩容场景要够)、
`--no-submit`(不提交任务,纯实例)、
`--pin p:ttl`(钉住分区)、`--migrate-pin p:instId`(定向迁移并钉住)、
`--stuck p1,p2`(模拟分区执行器卡死)、
`--autoscale`(自动扩缩:mock 钩子 + 短冷却)、`--rate n`(每 500ms 固定条数,0=随机)、
`--target-rps n`(单分区目标吞吐)、`--drain-task`(任务注销,默认先跑 20s)、
`--drain-after n`(注销前置跑秒数)、`--drain-inst id`(对实例写下架 op)、
`--mock-cpu n`(固定进程 cpu 0-100,验证 cpu 驱逐)、`--lag`(注入 mock Kafka 源,追平改 lag 实查)、
`--pause-inst id`(暂停接单 op)、`--set-weight id:w`(调权 op,0.5~2.0)、`--undrain id`(取消下架 op)、
`--alerts`(观察告警事件流计数)、`--err-rate r`(错误注入率 0~1,验证错误率巡检)、
`--observe`(观察秒数)。

## etcd key 空间

| key | 写入方 | 租约 |
| --- | --- | --- |
| `/dcp/instances/{id}` | 实例(全局身份) | 无(持久) |
| `/dcp/{ns}/instances/{id}` | 实例(每 ns 调度注册) | 30s |
| `/dcp/{ns}/instances/{id}/drain` | 调度主(下架意图,删除=取消下架) | 无(持久) |
| `/dcp/{ns}/instances/{id}/paused` | 调度主(暂停接单意图,删除=恢复接单) | 无(持久) |
| `/dcp/{ns}/instances/{id}/weight` | 调度主(调权意图 {"weight":0.5~2.0}) | 无(持久) |
| `/dcp/{ns}/tasks/{taskId}/spec` | 业务外壳 | 无(持久) |
| `/dcp/{ns}/tasks/{taskId}/status` | 调度主 | 无(持久) |
| `/dcp/{ns}/tasks/{taskId}/parts/{partId}` | 调度主(CAS) | 无(持久) |
| `/dcp/{ns}/tasks/{taskId}/parts/{partId}/snap` | 实例(快照) | 30s |
| `/dcp/{ns}/tasks/{taskId}/events/{name}` | 调度主(扩缩容事件,滚动覆盖) | 无(持久) |
| `/dcp/{ns}/rules` | 运维后台(ns 默认扩缩规则,可选) | 无(持久) |
| `/dcp/{ns}/rebalance-rules` | 运维后台(再平衡规则,可选) | 无(持久) |
| `/dcp/{ns}/scoring` | 运维后台(打分系数 α/β/γ/δ,可选;缺省 0.4/0.3/0.2/0.1,热更新) | 无(持久) |
| `/dcp/{ns}/kafka` | 运维后台(bootstrapServers+probeSec,可选;缺失追平回退 rps) | 无(持久) |
| `/dcp/{ns}/leader` | 竞选(createIfAbsent) | 30s |
| `/dcp/{ns}/ops/{opId}` | 运维后台(PIN/UNPIN/MIGRATE_PIN/DRAIN_INSTANCE/UNDRAIN_INSTANCE/PAUSE_INSTANCE/UNPAUSE_INSTANCE/SET_WEIGHT) | 无(持久) |
| `/dcp/{ns}/alerts/{alertId}` | 调度主(告警事件流,滚动保留 200 条;后台页面只读) | 无(持久) |

## 已验证(smoke)

- 提交 → 补 PENDING → 打分分配 → watch 装载执行 → 快照上报闭环
- 优雅退出:注销/让位/CAS 清理归属,spec/status/全局身份保留
- 崩溃接管:leader 转移 → 判死 → 保守交接双窗口 → 中间态换主可续 → 恢复消费
- 锁机制:钉住分区在 owner 崩溃后不被判死回收(持续告警),TTL 到期自动解锁自愈
- 定向迁移:MIGRATE_PIN → 源主动停(stopped 快照快通道)→ 立即回收 → 定向分配且锁保留在目标
- 卡死检测:快照停更超过宽限(30s)→ 按交接模式迁移该分区(起因=stuck),健康实例恢复消费
- 双确认判死:实例 lease 过期但快照仍在更新 → SUSPECT 观察不判死(防误判双消费)
- 再平衡:失衡(最忙占用率>0.8 且差>0.3 持续 3 周期)→ 步进迁移(临时 TTL 锁定向)→ 锁到期自动解锁
- 自动扩容:水位>0.8 持续 3 周期 → 尾部补 PENDING 分区(步长护栏最多翻倍)→ 分配闭环;写扩容建议事件
- 逻辑缩容:水位<0.4 持续 6 周期(双闸门:downEnabled+钩子声明)→ routeCut 切路由 → DELETING 追平(rps 连续归零)→ 清记录+期望分区数递减;单轮尾部 1 分区,全 ns 同轮≤2 任务
- 缩容收敛:删尾部分区不重建(spec.partitions 变化与 expectedParts 解耦,lastSpecPartitions 对账),4→1 无震荡
- 任务注销:删 spec → DRAINING → PENDING 直删、其余置 DELETING 追平 → 清全部 parts → GONE + gone 事件(外壳可释放资源)
- 实例下架:DRAIN_INSTANCE op → 意图登记(不接新单)→ 每轮每实例迁 1 分区(MIGRATING 交接)→ 迁空提示可安全关机;UNDRAIN 恢复接单,已迁出分区不回流
- cpu 驱逐:进程 cpu>80 持续 3 周期 → 迁出该实例 rps 最低分区(与槽位失衡正交,可独立触发);目标选择跳过 cpu>80 实例;真实 cpu 走 OS 进程级采集(meta 10s 刷新)
- 再平衡目标选择传 meta:cpu 余量参与打分,高负载实例不再成为迁移目标;迁移源分区选择改瞬时 rps(原累计 processedCount 会误选高历史低当前分区)
- lag 实查追平:主侧 Kafka 探针(内置 AdminClient 按 ns 配置懒建,60s 周期实查,DELETING 存在时缩短到决策周期)算 lag=endOffset−committedOffset,<100 条连续 2 周期才清记录;注销靠 status 记忆的 topic 查 lag(spec 已删);无源/查询失败回退 rps 归零近似(护栏不阻塞)
- 告警事件流:主写 /dcp/{ns}/alerts/{alertId}(8 规则:SUSPECT/EXEC_STUCK/DRAIN_TIMEOUT/LAG_HIGH/ERROR_RATE/CAPACITY_FULL/MIGRATE_TIMEOUT/NO_INSTANCE),同对象同类型 10min 去重,恢复同 alertId 覆盖 resolvedAt,滚动 200 条;后台 /monitor/alerts 实例维度聚合展示(backend jetcd 只读透传)
- 告警巡检:lag 高位(lag>max(100,rps×5))与错误率(相邻帧差分>1%)首现持续 3min 才报;容量水位>0.9 且满配;PENDING>3 决策周期区分无实例/有候选失败;恢复时补发恢复事件
- 暂停接单:PAUSE_INSTANCE/UNPAUSE_INSTANCE op 写独立意图键(不碰 30s 注册租约),主侧同前缀一次 getPrefix 收集 overlay;paused 实例不接新单,已跑分区不动
- 调权:SET_WEIGHT op 写 {"weight":0.5~2.0} 意图(限幅),参与迁移目标打分;undo = 删意图键
- 打分系数可配:/dcp/{ns}/scoring 热读(α 槽位 β cpu γ 亲和 δ 权重,缺省 0.4/0.3/0.2/0.1),变化即重建 Scoring 注入分配/再平衡
