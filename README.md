# 数据采集平台 DCP (Data Collect Platform)

> 从 0 到 1 建设的一站式数据服务平台,实现百 TB 级、数十万 QPS 流量下「多源采集 → 实时同步 → 资产目录 → 血缘分析 → 规则打标 → 订阅消费」全链路闭环。

[![JDK](https://img.shields.io/badge/JDK-17-orange)](https://adoptium.net/)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.2.5-brightgreen)](https://spring.io/projects/spring-boot)
[![Kafka](https://img.shields.io/badge/Kafka-3.7.0-black)](https://kafka.apache.org/)
[![TiDB](https://img.shields.io/badge/TiDB-7.5-blue)](https://pingcap.com/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue)](LICENSE)

## 项目简介

DCP 是一个**配置驱动的多源异构数据平台**,核心解决「多源数据怎么进、增量流怎么分发、规则怎么算、数据怎么沉淀与看见、谁能看什么」五个问题。区别于普通 ETL 工具,它具备:

- **精确一次幂等入库**:源库位点即序号 + ODKU 写者列裁判,多源合流冲突收敛、乱序消解;
- **批流自动衔接**:全局事务序列锚点索引 + T0 反算,批(≤T0)与流(>T0)严丝合缝;
- **自研调度内核**:基于 etcd 的分布式分区调度,主节点秒级切换、数据面无中断;
- **判定执行分离的权限框架**:XACML 架构,权限过滤 P99 仅占查询总耗时 5%;
- **批流一体打标**:Drools 动态 Fact 编译,同一套规则贯通全量批处理与实时流计算。

## 六大核心模块

| 模块 | 一句话 | 核心技术 |
|---|---|---|
| **数据同步链路** | 数据怎么进来 | SeaTunnel CDC → Kafka → 攒批落 TiDB;写者列裁判精确一次幂等 |
| **数据消费底座** | 增量流怎么分发 | assign+显式续传;一人一本账(独立 group.id)喂多消费者 |
| **任务调度框架** | 分区任务怎么托底 | etcd 分区调度内核;双信号判死 + 分场景交接 + ns 隔离 |
| **数据打标引擎** | 规则怎么算 | Drools 批流一体(Spark 批 + Flink 流);动态 Fact 强一致编译 |
| **数据资产体系** | 数据怎么沉淀与看见 | Iceberg 湖仓 + Neo4j 字段级血缘 + 资产目录 |
| **数据权限服务** | 谁能看什么 | XACML RBAC+ABAC;判定执行分离 + 三级缓存 + 秒级收敛 |

## 系统架构

```
接入层    SeaTunnel Zeta(数据库 CDC)+ 采集 Agent(rclone 文件同步)
          ↓
缓冲层    Kafka 双栈物理隔离(平台栈 同步缓冲 / 出口栈 CDC 作业队列)
          ↓
数据层    TiDB(目标库)| MySQL MGR(元库)| MinIO+Iceberg(湖)| Neo4j(血缘)| Redis(在线标签)| etcd(调度底座)
          ↓
应用层    控制面微服务(backend/outlet/subscription/asset/tagengine/delivery)+ dcp-worker 执行体池
          ↓
治理层    Nacos + Gateway + Sentinel + ShedLock + Zipkin + Prometheus/Grafana/VictoriaMetrics
```

**核心设计主线**:数据面「热-冷分层 + 双栈缓冲」;调度「分区原子 + 批流分离」;执行「统一宿主化 + 分池扩缩容」;权限「判定执行分离」;监控「Prometheus 管健康 + 调度框架管吞吐」双轨。

## 技术栈

| 层 | 技术 |
|---|---|
| 语言/框架 | Java 17 · Spring Boot 3.2.5 · Vue 3 · Flink 1.18 |
| 采集 | SeaTunnel Zeta(CDC)· rclone(文件) |
| 消息 | Kafka 3.7(KRaft,双栈) |
| 存储 | TiDB 7.5 · MySQL 8.0(MGR)· MinIO · Iceberg · Neo4j 5 · Redis |
| 调度 | etcd 3.5(自研分区调度内核)· ShedLock |
| 规则 | Drools(动态 Fact 编译) |
| 权限 | XACML(RBAC + ABAC)自研框架 |
| 治理 | Nacos · Gateway · Sentinel · Zipkin · Prometheus + Grafana + VictoriaMetrics |

## 快速开始

### 环境要求

- Docker Desktop(含 Compose v2)
- 内存 ≥ 16GB(全栈运行)

### 启动

```bash
git clone <repo-url>
cd data-collect-platform
cp .env.example .env        # 按需修改密钥(生产务必用随机值)
docker compose up -d --build
```

访问 **http://localhost:8080**,默认账号 `admin / admin123`。

> 完整端口与账号见 `docs/6-分布式设计/最小集群部署演练方案.md`;本地最小演示只需 `mysql`、`kafka`、`backend`、`frontend` 四个服务即可跑通核心链路。

## 模块结构

```
├── backend/                # 平台主控制面(数据源/同步/监控/系统/权限)
├── outlet-*/               # 消费底座: sdk(编程接口)/control(决策)/worker(执行)/adapters(适配)/service(服务)
├── dcp-scheduler/          # 自研分区调度内核(etcd 引擎 + 状态机 + 调度算法)
├── dcp-worker/             # 执行体宿主(SPI 装载业务 jar,白名单 ClassLoader 依赖隔离)
├── dcp-synclink/           # 同步链路执行器(CDC → Kafka → ODKU 写库)
├── dcp-outlet-rt/          # 出口计算面(消费底座运行时)
├── dcp-indexer-rt/         # 索引器(锚点采样)
├── dcp-filesync-rt/        # 文件同步执行器(rclone)
├── tagengine-*/            # 打标引擎: dcp-tag-kie(编译库)/executor(Spark 批)/dcp-tagstream(Flink 流)
├── asset-service/          # 资产目录 + 血缘(Neo4j)
├── subscription-service/   # 订阅中心
├── delivery-service/       # 出口交付
├── dcp-gateway/            # 统一网关
├── agent/                  # 采集 Agent(文件同步)
├── frontend/               # 主前端(Vue3)
├── scheduler-console/      # 调度框架前端
├── perm/                   # 统一数据权限服务(独立工程,XACML)
├── seatunnel/              # SeaTunnel 定制镜像 + CDC 位点透传补丁
├── docs/                   # 完整设计文档(按域组织)
└── docker-compose.yml      # 全栈编排
```

## 核心设计亮点

- **写者列裁判**:每个「任务×源」一条写者列,同写者同号拒写、号大赢,跨源 LWW——精确一次 + 多源冲突收敛;
- **重启续传**:起点 = max(Kafka 已提交位点,任务单 T0),auto.offset.reset=earliest 三级兜底,绝不静默丢数据;
- **双信号判死 + 分场景交接**:lease+快照双确认防误判,幂等域激进/非幂等域保守(90s 观察窗口),零双消费;
- **白名单 ClassLoader**:业务 jar child-first、三方库 parent-first,多 jar 共宿主依赖隔离;
- **动态 Fact 编译**:按 schema 生成 Fact 源码 + 规则 DRL 一并入 KieModule 编译,批流同源;
- **模板与绑定分离**:权限产物无值、PEP 绑值环节才出现,三级缓存把开销压到 5%。

## 文档

完整设计文档在 `docs/`,按域组织:

- `0-总览/` `1-数据采集/` `2-同步链路/` `3-索引器/` `4-数据消费/` `5-统一调度框架/` `6-分布式设计/` `7-微服务架构/` `8-AI应用/`
- 权限框架文档在 `perm/docs/`

## 安全说明

> ⚠️ 仓库内所有密码均为**演示/开发默认值**(`root123`、`admin123`、`minioadmin` 等)。**生产部署必须通过环境变量覆盖**,至少包括:`MYSQL_ROOT_PASSWORD`、`JWT_SECRET`(≥32 字节随机)、`PLATFORM_ENC_KEY`。密钥配置见 `.env.example`。

## License

[Apache License 2.0](LICENSE)
