# dcp-indexer-rt —— 索引器内核（indexer ns SPI）

被 `dcp-worker`（indexer ns）加载的**索引器执行体内核**：基于栅栏锚点的增量索引与污染记录（Poison）机制。

## 核心组件

```
com/dcp/indexer/
├── core/
│   ├── AnchorIndexer       栅栏锚点索引(增量消费核心)
│   ├── JdbcAnchorStore     锚点表持久化(chg_anchor)
│   ├── CanalJson           CanalJson 解析
│   ├── Watermark           水位
│   └── Poison              解析失败污染记录(保守处理:不写目标)
└── exec/
    ├── AnchorSamplerExecutor  锚点采样执行器(implements PartitionExecutor)
    └── IndexerFactory         SPI 工厂(META-INF/services 供 worker 装配)
```

## 关键机制（见 docs/3-索引器/）

- **栅栏点定位**：乱序数据按栅栏点锚定，不做全局 seq 行级裁判
- **解析失败保守**：解析失败的记录记 Poison 污染表，不落目标、不影响后续
- **单线程消费模型**：每分区串行，攒批写入

## 与其他模块的关系

- 依赖 `dcp-scheduler-api`（PartitionExecutor 契约）
- 元库：chg_anchor / chg_watermark_latest 表（与控制面 outlet-control 协作）

## 构建

```bash
mvn -q install -DskipTests
```
