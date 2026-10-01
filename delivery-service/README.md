# delivery-service —— 出口交付域独立服务(方案 N10)

承载 `backend/module/outlet`（变更流出口）与 outlet-service 契约视图迁出的**出口交付域**全部能力。

## 职责

- **变更流出口 L3 六接口**：契约视图、快照（`/snapshot?boundaryTso=` 批快照源）、增量拉取、位点查询等出口数据面接口
- **批快照服务**：存量数据快照读取（批阶段快照源）
- **消费方 SDK 运行时**：出口契约的运行时裁剪与数据组装
- **契约视图**：`JdbcContractViewSource` 从元库读契约视图供计算面拉取
- **出口栈指标**：`module/outlet/metrics` 出口栈（changefeed/Kafka）监控指标采集（micrometer-registry-prometheus，`/api/actuator/prometheus`）

## 包结构

```
com/dcp/
├── delivery/              入口 DeliveryServiceApplication(EnableScheduling)
├── module/outlet/
│   ├── entity/mapper/     出口契约/消费方账号等元数据
│   ├── perm/              契约与权限引擎(OutletPermissionEngine, "契约 ∩ 主体")
│   ├── source/            快照/增量数据源读取
│   ├── sdk/               HttpOutletApi(直连 outlet-service REST)
│   └── metrics/           出口栈指标
├── module/tagengine/perm/ 数据权限裁剪复用
├── security/              JWT 与 X-Dcp-Token 双通道校验
└── config/                数据源/ShedLock 装配
```

## 与其他模块的关系

- **outlet-service**：计算面任务编排在 outlet-service（出口底座控制面），数据面读取在本服务；`HttpOutletApi` 直连（治理化后改服务发现）
- **backend**：批阶段快照源历史能力迁出至此；契约视图桥接 v2 计算面
- **元库**：契约/消费方账号/契约视图表

## 构建与运行

```bash
mvn -q package -DskipTests
java -jar target/delivery-service.jar   # 端口 8096
```
