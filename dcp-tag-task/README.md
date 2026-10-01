# dcp-tag-task —— 打标消费底座业务 jar（@ConsumerLogicDef tag-engine）

被 `dcp-worker`（tag ns）加载执行的**打标批底座任务**：按发布快照求值打标规则，双写在线层 + 投影层。

## 核心组件（fat jar 自足：drools/jedis/mysql 驱动打入本 jar）

```
com/dcp/tagtask/
├── TagLogic               @ConsumerLogicDef(tag-engine):按发布快照求值(与 StreamEvaluator 同语义)
├── TagLogicProvider       logic 提供者(worker 加载入口)
├── TagSinks               双写 sink:
│                          · Redis tag:online:{entity}(hash: tag→value + TTL) 在线层
│                          · MySQL tag_fact_view + tag_notify 投影事实层
├── TagParams              任务参数
└── TagBundle              Drools 规则包
```

## 关键机制

- **命中动作**：打标签 → Redis 置位 `tag:online:{entity}` + 投影行
- **DELETE 事件**：清在线标签（Redis hdel）+ 删投影行
- **依赖隔离**：dcp-tag-kie(drools/jackson) + jedis + mysql 驱动全部打入本 jar，业务 jar 自足

## 与其他模块的关系

- 上游：dcp.{表} 信封流（tag ns 消费）
- 下游：Redis 在线层（与 `dcp-tagstream` 流打标同一 key 语义）+ 元库投影层 tag_fact_view
- 读方：`tagengine-service` 的 OnlineTagService

## 构建

```bash
mvn -q package -DskipTests   # 产出 fat jar 部署到 logic-repo
```
