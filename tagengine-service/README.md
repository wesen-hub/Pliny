# tagengine-service —— 打标引擎域独立服务(方案 N9)

承载 `backend/module/tagengine` 迁出的**打标引擎域**全部能力。

## 职责

- **规则管理**：打标规则/规则集定义与发布（Drools 规则源）
- **任务与数据集**：打标任务编排、数据集管理
- **标签与在线打标**：标签定义；`online/OnlineTagService` 读 Redis 在线层 `tag:online:{entity}`（hash: tag→value，命中路径）；P1 在线执行 Redis 置位
- **流作业管理**：`StreamJobService` 管理 Flink 流打标作业（直连 outlet-service REST 登记/启停）
- **数据权限**：`perm/DataSecurityService` 数据权限策略管理面

全部接口走用户 JWT + `@perm.isAdmin`；仅 actuator permitAll。ShedLock `tagTaskReconcile` 域级锁。

## 包结构

```
com/dcp/
├── tagengine/             入口 TagEngineServiceApplication(EnableSchedulerLock)
├── module/tagengine/
│   ├── perm/              数据权限策略(DataSecurityService)
│   ├── online/            在线标签层(OnlineTagService, Redis tag:online:{entity})
│   └── ...                规则/任务/数据集/标签/流作业管理面
├── module/system/         登录/用户体系
├── security/              JWT 过滤器与 @perm 权限套件
└── config/                Redis/数据源/ShedLock 装配
```

## 与其他模块的关系

- **Redis 在线层**：唯一的 Redis 读方（`TAGENGINE_REDIS_HOST/PORT` 配置）；写方是 `dcp-tag-task`（批）与 `dcp-tagstream`（流）双写 sink
- **outlet-service REST**：任务发布后的 v2 登记(`TagV2Deployer`)与 Flink 流作业管理直连 outlet-service
- **投影层**：权威数据在元库 `tag_fact_view`，Redis 只是提速读层

## 构建与运行

```bash
mvn -q package -DskipTests
java -jar target/tagengine-service.jar   # 端口 8100
```
