# dcp-sub-push —— 订阅推送底座业务 jar（@ConsumerLogicDef sub-push）

被 `dcp-worker`（sub ns）加载执行的**订阅推送底座任务**：消费信封流/打标事件流 → 圈选/裁剪 → webhook/kafka 推送。

## 核心组件

```
com/dcp/subpush/
├── PushLogic              @ConsumerLogicDef(sub-push):圈选/裁剪 → 推送
├── PushLogicProvider      logic 提供者
├── match/CondMatcher      圈选条件匹配(订阅条件评估)
├── Excluder               排除名单(裁剪)
├── channel/
│   ├── PushChannel        推送通道抽象
│   ├── KafkaChannel       Kafka 推送
│   └── WebhookChannel     Webhook 推送
└── PushParams             任务参数
```

## 关键机制

- **圈选**：按订阅契约条件匹配事件流（信封流 + 打标事件流）
- **裁剪**：排除名单/权限裁剪后再推送
- **双通道**：webhook 与 kafka 通道可切换/并存

## 与其他模块的关系

- 上游：dcp.{表} 信封流 / 打标事件流
- 订阅契约管理面：`subscription-service`（N11）

## 构建

```bash
mvn -q package -DskipTests   # 产出 fat jar 部署到 logic-repo
```
