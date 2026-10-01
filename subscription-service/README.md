# subscription-service —— 数据订阅域独立服务(方案 N11)

承载 `backend/module/datasub` 迁出的**数据订阅域**全部能力。

## 职责

- **契约管理**：订阅契约 CRUD/审批/发布（管理面走用户 JWT + `@perm.isAdmin`，与 backend 同套安全套件）
- **PULL 数据服务**：订阅方拉数四接口 `/schema|snapshot|changes|export`，走 `X-Dcp-Token` 自校验、permitAll 放行
- **push 定时投递**：订阅推送定时任务（ShedLock `subTaskReconcile` 域级锁，多实例互斥）

## 包结构

```
com/dcp/
├── subscription/           入口 SubscriptionServiceApplication(EnableSchedulerLock)
├── module/datasub/         订阅契约管理 + PULL 数据服务 + push 投递
├── module/system/          登录/用户体系(与 backend 同套)
├── security/               JWT 过滤器与 @perm 权限套件
└── config/                 数据源/MyBatis/ShedLock 装配
```

## 与其他模块的关系

- 元库共享：与 backend/outlet-service 共用平台元库（契约表、订阅任务表）
- 推送任务：任务发布后经 outlet 底座执行（订阅推送的消费端是 `dcp-sub-push` 业务 jar）

## 构建与运行

```bash
mvn -q package -DskipTests
java -jar target/subscription-service.jar   # 端口 8098
```
