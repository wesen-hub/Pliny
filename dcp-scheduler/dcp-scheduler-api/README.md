# dcp-scheduler-api —— 统一调度框架契约

`dcp-scheduler`（统一调度框架）的契约子模块：模型与执行器接口，供框架内核与各执行体（`dcp-*rt`）共同依赖，保证内核/执行体解耦。

## 契约内容

- **模型**：TaskSpec（任务规格）、PartRecord（分区记录）、InstanceRecord/NsInstanceRecord（实例元数据）、ExecutorSnapshot（执行器四计数器快照）、AlertEvent、ScoringConfig 等
- **执行器契约**：`PartitionExecutor`（start/stop/metrics）——所有执行体 SPI（indexer/filesync/outlet/synclink rt）实现此接口
- 版本随 `dcp-scheduler-core`（shaded jar）发布，由 dcp-worker 依赖传递

## 与各模块的关系

- `dcp-scheduler-core`：框架内核（分区调度/判死交接/线程托管）
- 各 rt 执行体：`dcp-indexer-rt`/`dcp-filesync-rt`/`dcp-outlet-rt`/`dcp-synclink` 依赖本模块实现 PartitionExecutor
- 设计文档：`docs/5-统一调度框架/`

## 构建

```bash
cd ../dcp-scheduler && mvn -q install -DskipTests   # 随父工程安装
```
