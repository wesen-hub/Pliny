# dcp-worker —— 统一调度框架执行体（纯宿主，一镜像一骨架多 ns）

方案 N3 的执行体池：**同一镜像，按 ns 环境变量起多个池实例**（sync / indexer / outlet / tag / lake / sub / ingest / filesync）。

## 职责

- **调度框架宿主**：内嵌 `dcp-scheduler-core`（基于 etcd 的通用分区调度与线程托管：TaskSpec 接收、打分分配、判死交接 AGGRESSIVE/CONSERVATIVE、线程托管），契约 `dcp-scheduler-api` 随之传递
- **业务 jar 加载**：`WhitelistClassLoader` + `BundleAssembler` 加载各 ns 对应的业务 fat jar（如 outlet ns 加载 outlet-worker/dcp-outlet-rt + 消费方业务 jar；tag ns 加载 dcp-tag-kie/dcp-tag-task）
- **TaskSpec 下发入口**：`web/TasksController` 接收控制面（TaskScheduler）下发的 TaskSpec，转调度框架创建分区执行单元
- **运行环境装配**：`WorkerRuntime`/`WorkerEnvironment` 按 ns 装配执行体 SPI（`META-INF/services` 发现执行体工厂）

## 包结构（刻意保持薄：只有 6 个类）

```
com/dcp/worker/
├── WorkerApplication      入口(@SpringBootApplication, scanBasePackages 含 com.dcp.scheduler.rest)
├── WorkerRuntime          ns 运行环境装配
├── WorkerEnvironment      ns 环境参数(名称/etcd/Kafka 等)
├── BundleAssembler        业务 jar 装配
├── WhitelistClassLoader   业务 jar 类加载(白名单)
└── web/TasksController    TaskSpec 下发 REST 入口
```

## 与其他模块的关系

- 上游：控制面（backend/outlet-service 的 TaskScheduler）经 REST 下发 TaskSpec；实例元数据注册到 **etcd**（调度框架判死/迁移底座）
- 下游：加载各 ns 内核（`dcp-indexer-rt`/`dcp-filesync-rt`/`dcp-synclink`/`outlet-worker`/`dcp-outlet-rt`）+ 业务 fat jar（`dcp-tag-task`/`dcp-lake-task`/`dcp-ingest-task`/`dcp-sub-push`）
- 消费：Kafka 消费组自治（实例增减由分区 rebalance 兜底，不依赖注册中心）

## 构建与运行

```bash
mvn -q package -DskipTests
# 以 ns=outlet 为例(同一镜像不同环境变量):
java -Ddcp.worker.ns=outlet -jar target/dcp-worker.jar
```

> 设计文档：`docs/5-统一调度框架/`（总纲 + 8 个子文档）。
