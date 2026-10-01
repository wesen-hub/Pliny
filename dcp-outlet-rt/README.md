# dcp-outlet-rt —— 出口执行体运行时（outlet ns SPI 内核）

被 `dcp-worker`（outlet ns）加载的**消费底座执行体内核**：把 Kafka 分区记录解码为消费方业务 jar 能处理的消息，执行后再按契约裁剪/掩码/死信处置。

## 核心组件

```
com/dcp/outlet/rt/
├── PartitionTaskExecutor      分区任务执行器(串行单元循环)
├── KafkaRecordSource          分区记录源(assign + seek 起点)
├── MessageDecoder/DecodedMessage/SdkEventDecoder  记录解码
├── ContractProvider/ContractView/ContractFilter   契约视图拉取与字段裁剪
│   ├── HttpContractProvider   REST 拉取契约(delivery-service)
│   └── DefaultContractFilter  契约裁剪(表达式行条件评估)
├── Masker/PlatformMasker      平台级脱敏
├── DeadLetterSink/InMemoryDeadLetterSink  死信落盘
├── FailurePolicy/FailureSpec  失败策略(重试/跳过/死信)
├── LogicArtifact/LogicRepository/JarLogicLoader/ClasspathLogicLoader/CompositeLogicLoader  业务 jar 加载
├── InitContextImpl            logic.init(ctx) 上下文
└── CreateUnitCmd              建单元指令
```

## 与其他模块的关系

- 依赖 `dcp-scheduler-api`（PartitionExecutor 契约）+ `dcp-outlet-sdk`（业务 jar 开发契约）
- 业务 jar（消费方 logic）由 `outlet-sdk` 定义接口、本模块运行时加载执行
- 契约视图来源：delivery-service 的契约视图接口

## 构建

```bash
mvn -q install -DskipTests   # 安装到本地仓库供 dcp-worker 打包
```
