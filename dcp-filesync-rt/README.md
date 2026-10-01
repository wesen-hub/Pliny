# dcp-filesync-rt —— 文件同步内核（filesync ns SPI）

被 `dcp-worker`（filesync ns）加载的**文件同步执行体内核**：rclone 无状态进程同步模式。

## 核心组件

```
com/dcp/filesync/
├── core/
│   ├── RcloneEngine        rclone 无状态进程引擎(推湖 S3)
│   ├── RunRecorder         运行记录(幂等/续传状态)
│   └── FilesyncTaskReader  文件同步任务读取
├── exec/
│   ├── FilesyncExecutor    文件同步执行器(implements PartitionExecutor)
│   └── FileSyncFactory     SPI 工厂
└── util/                   CryptoUtil(加密)/Strategy(策略)
```

## 关键机制

- **rclone 无状态**：每次任务独立拉起 rclone 进程，无长驻状态；文件同步的写者独立序号
- **Agent 配合**：文件采集侧由 `agent/`（AgentMain 长轮询 + rclone）完成，本模块负责平台侧任务编排与执行
- 与湖链路衔接：文件推 MinIO/S3 后由 lake ns（`dcp-lake-task`）做 Iceberg 登记

## 与其他模块的关系

- 依赖 `dcp-scheduler-api`（PartitionExecutor 契约）

## 构建

```bash
mvn -q install -DskipTests
```
