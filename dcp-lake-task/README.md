# dcp-lake-task —— 湖增量底座业务 jar（@ConsumerLogicDef lake-sync）

被 `dcp-worker`（lake ns）加载执行的**湖增量底座任务**：消费信封流 → seq 裁判/字段级合并 → Iceberg 湖表 overwrite。

## 核心组件

```
com/dcp/laketask/
├── LakeLogic              @ConsumerLogicDef(lake-sync):seq 裁判/字段级合并 → 湖表 overwrite
├── LakeLogicProvider      logic 提供者
├── lake/
│   ├── LakeOps/IcebergLakeOps       湖表操作(与批作业 MERGE 同语义)
│   └── LakeCatalogFactory           Iceberg catalog 工厂
├── meta/JdbcMetaStore     元库元数据存储
├── model/Json             JSON 工具
└── LakeParams             任务参数
```

## 关键机制

- **字段级合并**：同一主键多源合流按 seq 裁判，字段级覆盖（非整行覆盖）
- **与批作业同语义**：增量 overwrite 与 `dcp-lakesync` 批作业 MERGE 语义一致，批流可切换
- 湖表为 Iceberg 格式，存储于 MinIO/S3

## 与其他模块的关系

- 上游：dcp.{表} 信封流；下游：Iceberg 湖表（MinIO/S3）
- 与 `dcp-lakesync`（湖批作业）语义对齐

## 构建

```bash
mvn -q package -DskipTests   # 产出 fat jar 部署到 logic-repo
```
