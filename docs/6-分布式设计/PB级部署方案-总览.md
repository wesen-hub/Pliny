# PB 级数据平台部署方案(总览)

> 状态:总览定稿(2026-10-06),合并《数据服务平台分布式设计.md》总纲 + 《集群部署架构图.svg》六层架构与实例规格 + 硬件扩容口径。
> 定位:部署方案一页通——设计基准、六层拓扑、实例规格、容量总表、物理资源、HA 矩阵、落地路线、面试口径。
> 配套:详细缺口与分系统设计见同目录《数据服务平台分布式设计.md》;架构图见《集群部署架构图.svg》。

## 一、设计基准与总纲

| 指标 | 口径 |
|---|---|
| 峰值吞吐 | 10 万+ QPS(日均约 86.4 亿条事件) |
| 消息体积 | ~700B/条(CDC 信封)→ 入口带宽 ~70 MB/s |
| 存量规模 | PB 级:目标库 + 湖累计,日增约 6TB |
| 目标库写入 | 10 万 QPS(攒批后),单实例保守 1.5 万 QPS 写 |

**总纲(做减法)**:能用确定性(位点即序号)就不协调;能用单条 SQL 原子性(唯一键)就不用分布式锁;能用无状态重建就不做状态复制;拒绝 Redis/ZK 外部强依赖(etcd 仅作调度底座)。

**核心结论**:调度主备/巡检互斥/索引器分片三大缺口已被 dcp-scheduler 的「etcd 选主 + 分区原子」一次性消化。**真正的新增战场只有两处:SeaTunnel 多 worker、目标库两级桶路由分片**——其余全是护栏与预案。

## 二、部署架构总图(六层)

```
① 接入层   上游业务库(平台外)─ TiCDC ×4 ─┐
           SeaTunnel Zeta(master×3 + worker×6)├──▶ ② 缓冲层
           采集 Agent ×N(侧挂业务机)──────┘
② 缓冲层   Kafka 双栈物理隔离:平台栈 ×3 / 出口栈 ×6(NVMe、RF=3/minISR=2、7d retention)
③ 数据层   TiDB(PD×3 + TiKV×10 热窗口 7 天 + Server×4 无状态)| MySQL MGR ×3 元库
           | MinIO ×16(EC 8+4 → 有效 1.5PB,PB 主载体)| Iceberg+Polaris ×2
           | Redis 哨兵 3主3从(打标在线)| Neo4j 因果集群 ×3(血缘)| etcd ×3(调度底座)
④ 治理层   Nacos ×3 | API 网关 ×2 | Sentinel 内嵌网关 | Zipkin(采样 1%)| 无 Seata
⑤ 应用层   K8s 计算节点 ×10(64C/256G)+ Flink/Spark 专用池 ×4
           控制面 6 服务各 ×2~3 | dcp-worker 执行体池共 50 实例(按 ns 分池)
⑥ 监控层   Prometheus ×2 + Grafana ×2,告警复用网关
```

## 三、分层部署明细(实例规格,来自 SVG)

### ① 接入层

- TiCDC changefeed ×4(8C/16G,按 feed 数横向扩);
- SeaTunnel Zeta master×3 + worker×6(8C/16G);
- 采集 Agent ×N 侧挂业务机(1C/2G,rclone 直推湖,不计入平台机柜);
- 上游业务库在平台之外;10 万 QPS ≈ 100MB/s 行流(Kafka 压缩后 ~25MB/s 落盘)。

### ② 缓冲层(Kafka 双栈物理隔离)

- **平台栈**(采集→同步缓冲)×3 broker,8C/32G,NVMe 3.2T×4,RF=3,V2/V3 双链路;
- **出口栈**(CDC→消费底座主路径)×6 broker,32C/128G,NVMe 3.2T×6,RF=3/minISR=2,7 天保留,128-256 分区,dcp.in.* 入站透传;
- 分区预算:10 万 QPS ÷ 单分区 1 万/s → 热点表 16-32 分区起步。

### ③ 数据层(有状态组件独立裸机,方案 A)

| 组件 | 规划 | 规格 | 说明 |
|---|---|---|---|
| TiDB PD | ×3 | 4C/8G SSD | 调度/元信息(etcd 内嵌) |
| TiDB TiKV | ×10 | 64C/256G · NVMe 2T×4 | 热窗口 7 天,QPS 主导(30 天档需 ×20) |
| TiDB Server | ×4 | 16C/64G 无状态 | SQL 入口,可弹性扩 |
| 元库 MySQL MGR | ×3 单主 | 16C/64G · SSD 2T×2 | shedlock/batch_job 判据载体 |
| MinIO(湖) | ×16 | 16C/64G · HDD 12T×12 | EC 8+4 → 有效 1.5PB,PB 主载体 |
| Iceberg + Polaris | ×2 | 8C/32G | 表元数据/快照,与湖批混部 |
| Redis 哨兵 | 3 主 3 从 | 16C/128G SSD | tag:online:{entity} 命中路径 |
| Neo4j 因果集群 | ×3(2 读 1 写) | 32C/256G NVMe | 资产/血缘图谱 |
| etcd | ×3 | 8C/32G SSD | 判死/迁移/租约 |

### ④ 治理层(生产必补项)

- Nacos ×3 集群(4C/8G),服务发现/配置下发替代直连 URL;
- API 网关 ×2(8C/16G),路由/JWT 鉴权/限流;
- Sentinel 内嵌网关进程,不独立部署;
- 链路追踪:Micrometer Tracing + Zipkin Server ×1(8C/32G),10 万 QPS 全链路采样 1%;
- 最终一致架构,无需 Seata 分布式事务。

### ⑤ 应用服务层

- 控制面六服务(均 LB 多活):backend ×3(8C/16G)、outlet-service ×3(8C/16G)、subscription-service ×3(4C/8G)、tagengine-service ×3(8C/16G)、asset-service ×2(4C/8G)、delivery-service ×2(4C/8G);
- **dcp-worker 执行体池共 50 实例,按 ns 分池**:sync×8 / indexer×8 / outlet×4 / tag×4 / lake×4 / sub×4 / ingest×16 / filesync×2,每实例 4C/8G 承接 3-5K QPS,**实例数与分区数对齐,Kafka 消费组自治扩容**;
- Flink 流打标:JM×2(4C/8G)+ TM×8(8C/32G),HA 高可用,事件流 → Drools → 双写;
- Spark 批作业:按需动态池 ×4(16C/64G),批打标/湖批,作业结束释放不常驻;
- ShedLock 9 把域级锁:backend×3 / 各域服务×2-3 的对账 tick、Supervisor 巡检、湖批收尾自动单实例执行,多实例互斥无需改代码。

### ⑥ 监控运维层

- Prometheus ×2(4C/16G)、Grafana ×2(2C/4G);
- 告警通道复用网关,无独立部署;
- 告警口径:shedlock 锁行、batch_job 行、分区水位、TiKV 写入 QPS 对账健康度。

## 四、容量规划总表(10 万+ QPS)

| 环节 | 规划 | 依据 |
|---|---|---|
| 采集 | SeaTunnel 5-6 worker 起步,余量 8-10 | 16C/64G 每 worker ~2 万 QPS |
| 传输 | Kafka 出口栈 6-9 broker | 3 副本 × 保留 2 天 ≈ 36TB |
| 索引器 | 8-16 实例 | 单线程 5 万/s,起步 3-4 实例 |
| 同步/出口消费 | 随分区数框架分派 | 出口 ns 分区 ~200,20-30 分区/实例 |
| 目标库 | TiDB 路径(TiKV ×10 热窗口)/ MySQL 路径 7 分片实例 | 单实例 1.5 万 QPS 写 |
| 血缘 | Neo4j 单节点起步 + 只读副本 | 节点量 ~60 万级 |
| 湖 | Iceberg 日增 6TB,按天分区 + compaction | 批作业 + 小文件合并 |

## 五、物理资源总览(推荐档)

**60 台物理机 · ≈4000 核 CPU · ≈16TB 内存 · 2.3PB HDD(湖)· 320TB NVMe(热数据)· 6 机柜 · 25GbE 网络 · ≈50kW**。

> 推荐档口径:TiDB 热窗口 7 天 / MinIO 自建 / 治理层新增补项。

## 六、HA 与故障转移矩阵

| 层次 | 机制 |
|---|---|
| 控制面 | LB 多活,重复请求幂等;ShedLock 定时互斥 |
| 执行体 | 框架分派(etcd 租约 + 快照双信号判死),单实例宕机自动迁移 |
| 作业引擎 | SeaTunnel checkpoint 共享存储,节点宕机作业迁移续跑;Flink HA |
| 数据面 | TiDB 多副本 raft、MySQL MGR 半同步、Kafka RF=3/minISR=2、etcd 3 节点、MinIO EC 纠删码 |
| 故障域 | 单实例宕机:消费侧 Kafka rebalance / 无状态 K8s 重启 / ShedLock 到期接管 |

## 七、落地路线与现状

```
1. 容量基线实测(压测校准全部初估数字)         —— 前置
2. SeaTunnel 多 worker(唯一完全空白,第一断点)  ⬜ 未做
3. 目标库分片(核心难点,4096 桶两级映射)       🟡 单实例哈希分表已落地,跨实例未做
4. outlet 规划面 HA                           🟡 框架侧已消化,规划面进程主备未做
5. 治理段容量(Neo4j/Iceberg/Kafka 扩容预案)   ⬜ 未做
6. 打标引擎 TB 级                             ⬜ 随引擎演进
```

每项落地时遵守既有验证纪律:内核纯函数离线断言(不碰 Kafka/库/框架)+ 真栈端到端 + 口径回放对账(ts 一致性)。

## 八、面试口径(怎么讲)

1. **先讲总纲**:10 万 QPS / 日增 6TB 的设计基准 + 做减法原则;
2. **再讲一张图**:六层架构,重点三句话——Kafka 双栈物理隔离、TiDB 热窗口 + MinIO 冷数据分层、dcp-worker 按 ns 分池 50 实例与分区数对齐;
3. **讲两个真正的难点**:SeaTunnel 多 worker 的「源库单点约束」(独立 server-id/复制槽,一个源库不能被多 worker 重复采集)、目标库 4096 桶两级路由(先新写后迁存量);
4. **收口**:调度三大缺口被框架一次性消化,「真正的新增战场只有两处」——这句话最能体现做减法的判断力。
