# dcp-ingest-task —— 入站消费底座业务 jar（@ConsumerLogicDef kafka-ingest）

被 `dcp-worker`（ingest ns）加载执行的**入站底座任务**：消费 `dcp.in.{表}` → 验证/领号/打信封 → 发 `dcp.{表}`。

## 核心组件

```
com/dcp/ingest/
├── IngestLogic            @ConsumerLogicDef(kafka-ingest):验证/领号/打信封
├── IngestLogicProvider    logic 提供者
├── meta/JdbcMetaStore     元库元数据存储
├── model/
│   ├── CdcRecord          CDC 记录模型
│   ├── RouteUtil          路由(表→topic 映射)
│   └── Json               JSON 工具
├── send/
│   ├── EnvelopeSender     信封发送器
│   └── KafkaEnvelopeSender Kafka 信封发送(发 dcp.{表})
└── IngestParams           任务参数
```

## 关键机制

- **领号**：入站消息打平台信封（序号发号），保证下游 seq 单调
- **验证**：入站格式/契约校验，非法消息隔离
- **透传**：`dcp.in.*` 入站透传到 `dcp.*` 信封流（平台内统一信封格式）

## 与其他模块的关系

- 上游：`dcp.in.{表}`（出口栈 Kafka 入站，TiCDC/SeaTunnel 采集写入）
- 下游：`dcp.{表}` 信封流（sync/lake/tag/sub 各 ns 消费）

## 构建

```bash
mvn -q package -DskipTests   # 产出 fat jar 部署到 logic-repo
```
