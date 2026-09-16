# 数据采集平台 (Data Collect Platform)

基于《数据采集平台设计方案 V2.0》实现的可运行版本：**配置驱动的多源异构数据同步平台**。

- **前端**：Vue 3 + Element Plus + Pinia + Vue Router + ECharts（Nginx 托管）
- **后端**：Spring Boot 3.2 (Java 17) + MyBatis-Plus + Spring Security (JWT) + Aviator
- **执行引擎**：Apache SeaTunnel Zeta 2.3.12（REST 模式，自定义 AviatorTransform 插件）
- **数据库**：MySQL 8.0（元数据库）+ 演示源/目标库
- **部署**：Docker Compose 一键编排（5 容器）

## 快速开始

```bash
cd data-collect-platform
docker compose up -d --build
```

启动后访问 **http://localhost:8080**（前端）。

| 组件 | 地址 | 说明 |
|---|---|---|
| 前端 (Nginx) | http://localhost:8080 | 管理界面 |
| 后端 API | http://localhost:8081/api | 可直连调试 |
| SeaTunnel REST | localhost:8082 | Zeta 引擎 REST（submit-job/job-info/stop-job/running-jobs） |
| 元数据库 | localhost:3306 (root/root123, 库 dcp) | 平台配置/状态 |
| 演示源/目标库 | localhost:3307 (root/root123) | source_db / target_db |

默认账号（角色见下）：

| 账号 | 密码 | 角色 |
|---|---|---|
| admin | admin123 | ADMIN |
| operator | operator123 | OPERATOR |
| viewer | viewer123 | VIEWER (只读) |

> 端口冲突可修改 `docker-compose.yml` 中的 `ports` 映射（如 3306 → 33060）。

## 开箱即用的演示

首次启动自动初始化：演示数据源（源库 `source_db.t_order` / 目标库 `target_db.dw_order`）、
字典 `ORDER_STATUS`、以及演示任务 **「订单增量同步演示」**。

1. 登录 → 任务管理 → 找到「订单增量同步演示」→ 点击 **立即执行**；
2. 执行历史中查看结果（成功、8 行数据、耗时）；
3. 查看目标库：

```bash
docker exec -it dcp-demo-db mysql -uroot -proot123 target_db -e "SELECT * FROM dw_order"
```

可看到转换链效果：`status` → 中文 `statusDesc`（字典）、`created_at` → `orderDate`（日期格式化）、
`totalAmount` = 单价×数量×(1-优惠比例)（Aviator 表达式，优惠比例 0.2=打八折）。

### 增量同步演示

任务采用**两段式增量位点**算法：先探测 `MAX(id)` 锁定窗口，成功后推进位点，失败不推进。

```bash
# 在源库插入新订单
docker exec -it dcp-demo-db mysql -uroot -proot123 source_db -e \
"INSERT INTO t_order (user_id, price, quantity, discount, status, created_at) VALUES (1006, 888.00, 1, 1.00, '1', NOW());"
```

再次「立即执行」→ 只同步新数据 1 行。位点可在任务列表「位点」按钮查看/重置。

## 功能清单

- **数据源管理**：MySQL / PostgreSQL / Oracle / SQL Server / MongoDB；密码 AES-256-GCM 加密存储（列表脱敏）；
  测试连接、表/字段元数据拉取、SQL 预览（方言分页）、启用/禁用、删除引用保护
- **任务管理**：全量 / 增量 / CDC(预留) 三种模式；单表 / 自定义 SQL 抽取；Cron 定时 + 手动触发；
  超时控制、失败指数退避重试、复制任务、运行锁防重
- **字段映射**：自动映射（按名称归一化）+ 手动调整；四层转换链：直接映射 → 字典 → Aviator 表达式
  （可引用前置输出字段）；表达式编译校验（安全沙箱：禁反射、白名单函数 `default / dict.lookup / date.format / date.parse`）
- **字典管理**：CRUD + CSV 导入/导出 + 模板下载；被映射引用时禁止删除
- **执行监控**：执行历史（数据量/耗时/重试）、结构化执行日志、失败/超时/成功告警
  （钉钉 / 企业微信 webhook / 邮件）、统计报表（趋势/成功率/TopN，ECharts）
- **系统**：JWT 认证 + RBAC（ADMIN/OPERATOR/VIEWER/AUDITOR）、登录失败锁定、操作审计日志

## 目录结构

```
data-collect-platform/
├── docker-compose.yml        # 一键编排: mysql + demo-db + seatunnel + backend + frontend
├── demo-db/init/             # 演示库初始化 SQL（源库+目标库）
├── seatunnel/                # SeaTunnel Zeta 2.3.12 定制镜像
│   ├── Dockerfile            #   + connector-common + MySQL 驱动 + AviatorTransform 插件
│   ├── dist/                 #   预下载: 发行包/connector/驱动
│   ├── aviator-transform-src/#   自定义 AviatorTransform 插件源码（SPI 注册）
│   └── config/               #   seatunnel.yaml / hazelcast.yaml（REST 8081, 集群 5801）
├── backend/                  # Spring Boot 3 后端
│   ├── Dockerfile
│   ├── settings-docker.xml   # Maven 阿里云镜像加速
│   └── src/main/
│       ├── java/com/dcp/
│       │   ├── common/       # 统一响应/错误码/异常/加密
│       │   ├── config/       # 安全/MyBatis-Plus/线程池/种子数据
│       │   ├── security/     # JWT 过滤器/权限
│       │   ├── engine/       # ★ 执行引擎: SeaTunnelClient/ConfigParser/IncrementalPlanner + 轻量引擎
│       │   └── module/       # datasource/job/dict/execution/alarm/system
│       └── resources/
│           ├── application.yml
│           └── schema.sql    # 元数据库 DDL（幂等）
└── frontend/                 # Vue3 + Element Plus
    ├── Dockerfile
    ├── nginx.conf            # 静态托管 + /api 反代
    └── src/
        ├── api/              # axios 封装
        ├── views/            # 登录/工作台/数据源/任务向导/字典/监控/统计/系统
        └── ...
```

## SeaTunnel 集成说明

平台默认走 **SeaTunnel Zeta 2.3.12** 执行（`dcp.engine.type=seatunnel`），配置在 `seatunnel/`：

```bash
docker compose up -d --build   # 5 容器: mysql + demo-db + seatunnel + backend + frontend
```

**链路**：平台生成 JSON 配置（增量窗口 SQL 由平台侧两段式探测注入）→ `POST /submit-job?jobId=<数字>`
→ 轮询 `GET /job-info/{id}`（10s）→ FINISHED 成功 / FAILED 失败 / 超时 `POST /stop-job`。

**转换链**（Jdbc source → 单个 AviatorTransform → Jdbc sink）：

- 自定义插件 `aviator-transform.jar`（源码 `seatunnel/aviator-transform-src/`）承载 direct / dictionary / Aviator 表达式全链；
- 映射与字典以 **JSON 字符串**传入（`mapping_rules` / `dict_data`），避免 Zeta DAG Java 序列化失败（typesafe Config 嵌套结构不可序列化）；
- 表达式输出统一字符串化，由目标库隐式转换；`fail_on_error=false` 行级容错。

**SeaTunnel 镜像定制**（`seatunnel/Dockerfile`）：

- 发行包解压到 `/opt`（`--strip-components=1`），connectors 在 `/opt/connectors/`；
- 额外 jar 约定目录：`connectors/*.jar`（插件本体）+ `plugins/connector-<name>/`（依赖，如 MySQL 驱动）；
- `plugin-mapping.properties` 追加 `seatunnel.transform.AviatorTransform=aviator-transform`（key 带 `seatunnel.transform.` 前缀，value 是 jar 文件名前缀）。

> 踩坑速记：① connector-jdbc 需 `connector-common` 与 MySQL 驱动（放 `plugins/connector-jdbc/`）；
> ② Jdbc source/sink 配置必须带 `driver`；③ MySQL 8 URL 需 `allowPublicKeyRetrieval=true`；
> ④ 2.3.12 REST 路径无 `hazelcast/rest/maps` 前缀；⑤ transform 实例须可序列化（`SeaTunnelTransform extends Serializable`）；
> ⑥ 嵌套配置（List/Map）序列化失败 → 用 JSON 字符串；⑦ typesafe Config 数字键会合并路径 → 插件内用 Jackson 解析；
> ⑧ 引擎不回调 `open()` → 插件初始化放构造器；⑨ 增量窗口 SQL 需渲染为字面量（SeaTunnel 不支持参数化 query）；
> ⑩ 重试复用同一 executionId 会撞 SeaTunnel jobId → `executionId*100+attempt`；⑪ direct 映射须原始值透传（normalize 仅用于表达式求值, 否则日期/数字类型被破坏）；
> ⑫ 2.3.12 MySQL-CDC 是重构版多表模型（url 模式 + 复数参数）; ⑬ CDC 驱动放 `plugins/connector-cdc-mysql/`; ⑭ classloader-cache-mode=true 改 jar 后需重启容器/换 jobId; ⑮ CDC 写库由 checkpoint 驱动（约 60s 延迟）。

### MongoDB → MySQL 同步

已内置演示：`mongo` 容器（mongo:7，`app_db.users` 8 条种子数据）→ 任务「用户同步演示(MongoDB->MySQL)」→ `target_db.mongo_users`。

- **connector**：`connector-mongodb-2.3.12.jar` 已入镜像（发行包不带, 需从 Maven Central 下载）；
- **schema 必填**：Mongo source 的 `schema.fields` 是必需项, 平台按采样首条文档自动推断 BSON→SeaTunnel 类型（`MongoSchemaUtil`）；
- **认证**：平台统一 `authSource=admin`（root 等超级用户建在 admin 库, 业务库由 `databaseName` 指定）；
- **增量**：Mongo 源不支持 SQL 两段式位点, 按全量同步（`REPLACE INTO` 幂等）; 字段元数据拉取（集合字段）同样走采样推断；
- **类型映射**：ObjectId→STRING(hex)、Int32→INT、Int64→BIGINT、Double→DOUBLE、Date→TIMESTAMP(时区自动转 +8)、Decimal128→DECIMAL(34,18)。

### MySQL CDC 实时同步

已内置演示：任务「CDC实时同步演示(t_order->cdc_orders)」→ `target_db.cdc_orders`（与源表同构），支持 INSERT/UPDATE/DELETE 三种变更实时同步（binlog 捕获）。

- **connector**：`connector-cdc-mysql-2.3.12.jar` 入镜像（CDC source 用 `url` 模式: `database-names`/`table-names` 复数 List, `startup.mode=initial` 先全量快照再增量）;
- **依赖位置**：CDC 插件主 jar 在 `/opt/connectors/`（cdc-mysql + cdc-base 恰好 2 个）; MySQL 驱动须放 `/opt/plugins/connector-cdc-mysql/`（`No suitable driver found` 即此问题）;
- **sink 语义**：Jdbc `generate_sink_sql=true` + `primary_keys`（按 RowKind 自动生成 INSERT/UPDATE/DELETE, 而非 REPLACE INTO）; 写库由 checkpoint 驱动 flush（`checkpoint.interval=60000`, 变更约 60s 落目标表）;
- **任务生命周期**：STREAMING 常驻（不超时）, 提交后保持 RUNNING, 手动停止时优雅 stop-job; 停止态目前记为 SUCCESS;
- **限制**：CDC 直连模式不带转换链（AviatorTransform 重建行会丢 RowKind, 待改造）; 目标表需预建; 不建议给 CDC 任务配 cron;
- **环境要求**：源库需 `log_bin=ON` + `binlog_format=ROW`（MySQL 8 默认已开启, server_id 需唯一）。

## 关键设计说明

| 设计文档要点 | 本实现 |
|---|---|
| 执行引擎 SeaTunnel Zeta | SeaTunnel Zeta 2.3.12 REST 集成（`SeaTunnelClient`/`SeaTunnelConfigParser`/`IncrementalPlanner`/`SeaTunnelEngine`）；后端保留 `LightExecutionEngine`（本地 JDBC 同语义实现），`dcp.engine.type` 可切换 |
| 增量两段式 (K3) | 先 `MAX(inc)` 探测窗口上界 m → 过滤 `(L0, m]` → 成功后才推进位点，失败/中断不推进 |
| 幂等 (K2) | 目标端 `REPLACE INTO` 批量写入（batch 1000），重跑不产生重复 |
| 并发防重 (K5) | `job_definition.version` 乐观锁（无 Redis 兜底方案），RUNNING 禁止重复触发/编辑 |
| 调度 (5.6) | 轻量 Cron 扫描（30s 粒度），misfire DO_NOTHING；生产可换 Quartz JDBC JobStore |
| 表达式安全 (S-6) | Aviator 禁用反射/TCCL/模块导入 + 白名单函数 + 长度限制 + 编译期校验 |
| 密码安全 (K6) | AES-256-GCM 随机 IV，密文 `base64(IV‖CT‖TAG)`，密钥 `PLATFORM_ENC_KEY` 注入 |
| CDC / 大表分片 / MongoDB 源 | 需 SeaTunnel 扩展（STREAMING 模式 / connector-cdc / Mongo connector），UI 已禁用/提示 |

## 环境变量（docker-compose 可覆盖）

| 变量 | 默认 | 说明 |
|---|---|---|
| `PLATFORM_ENC_KEY` | dev-enc-key-0123456789abcdef | 数据源密码加密密钥（生产必须更换） |
| `JWT_SECRET` | dcp-jwt-secret-... | JWT 签名密钥（生产必须更换） |
| `ALARM_WEBHOOK` | 空 | 默认告警 webhook（钉钉/企微） |
| `MAIL_HOST/USERNAME/PASSWORD` | 空 | SMTP 邮件告警（配置后生效） |

## 常见问题

- **后端起不来 / 表不存在**：先确认 `mysql` 容器 healthy（`docker compose ps`），首次启动自动执行 `schema.sql`；
- **端口被占**：修改 compose 中 `ports` 后 `docker compose up -d`；
- **想换 Maven 源**：修改 `backend/settings-docker.xml`；
- **演示数据源连不上**：种子数据里的数据源指向容器 `demo-db:3306`（容器内网），删除重建时请用主机 IP。

---

## 后端模块地图(v2 增量消费通道)

```
平台既有                           v2 增量消费通道(本次新增)
├── backend          平台主后端     ├── outlet-sdk            逻辑编程接口 + TestKit(零依赖)
│   └── outlet/*     出口/契约/权限 ├── outlet-control        控制面内核(锚点/起点/规划/租约/批阶段/巡检)
├── agent            采集 Agent     ├── outlet-worker         计算面内核(单元运行器/裁剪)
├── dcp-lakesync     湖同步(Spark) ├── dcp-tagstream    打标(Flink)   ├── outlet-adapters       生产适配(JDBC/Kafka/HTTP/仓库)
├── tagengine-executor 打标批(Spark)├── outlet-service        控制面服务(REST,独立部署)
└── frontend         Vue3 控制台    └── outlet-worker-service 计算面服务(HTTP,独立部署)
共享: dcp-tag-kie(打标编译库; backend 在线 / dcp-tagstream 流 / tagengine-executor 批 三端共用)
```

| 模块 | 一句话 | 依赖 |
|---|---|---|
| `outlet-sdk` | 逻辑作者**唯一**要依赖的:实现 `ConsumerLogic` + 打 jar | 无 |
| `outlet-control` | 决策层:索引/起点/规划/租约/校验/交接/批阶段/巡检/进度 | sdk |
| `outlet-worker` | 执行层:一个单元怎么跑(提交/重试/死信/停机) | sdk |
| `outlet-adapters` | 真实中间件适配:MySQL/Kafka/平台快照 API/逻辑仓库 | control, worker |
| `outlet-service` | 控制面服务:上述能力的 REST 出口 + 定时巡检 | control, adapters |
| `outlet-worker-service` | 计算面服务:收 `CreateUnit`、报心跳、续租约 | worker, adapters |

**构建顺序**(Maven 依赖方向):`outlet-sdk → outlet-control → outlet-worker → outlet-adapters → {outlet-service, outlet-worker-service}`

**打标编译库(三端共用)**:`dcp-tag-kie`(com.dcp.tag.kie)—— DRL 生成/KieBase 编译/TagActions,
backend(在线)、dcp-tagstream(流)、tagengine-executor(批)统一依赖它(不再各自拷贝)。
三个工程构建前先 `mvn -f dcp-tag-kie/pom.xml install`;backend 容器构建另见 backend/Dockerfile(dist/ 预置 jar)。

**表结构**:`outlet/v2-meta-ddl.sql`(锚点/水位/流登记/任务/单元/单元分区/逻辑/死信)。

**文档**:`~/Desktop/数据采集平台V3文档/` 下《增量消费通道详细设计-v2》《v2 编程指南》《v2 实现进度》。
