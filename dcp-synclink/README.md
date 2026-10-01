# dcp-synclink —— 同步链路任务内核（sync ns SPI）

被 `dcp-worker`（sync ns）加载的**数据同步链路内核**：消费信封流 → 裁决 → 批写目标库。

## 核心组件

```
com/dcp/synclink/
├── core/
│   ├── SyncConsumer        信封流消费者(裁决 + 批写编排)
│   ├── BatchWriteService   全量批阶段写入
│   ├── AtomicUpsertDao     原子 upsert(已存在行裁决)
│   ├── DltService          死信表服务
│   └── LogicalTableConfig  逻辑表配置
├── model/                  CdcRecord/Json/RouteUtil(路由)
└── exec/                   SPI 工厂(META-INF/services)
```

## 关键机制（见 docs/2-同步链路/）

- **主键裁决**：目标库已存在行按批阶段规则裁决（值不同才写）
- **全量批写入**：批阶段攒批写、批号定义
- **死信**：不可处置行落 Dlt 表，不阻断链路

## 与其他模块的关系

- 依赖 `dcp-scheduler-api`（PartitionExecutor 契约）
- 上游：`dcp-ingest-task` 发出的信封流（dcp.{表}）；下游：目标库（TiDB/MySQL）

## 构建

```bash
mvn -q install -DskipTests
```
