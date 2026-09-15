# dcp-outlet-worker —— 计算面内核(单元运行器)

**消费单元真正跑起来的地方**。一个单元 = 1 个消费源 + 1 个线程 + 1 个 logic 实例 + 1 份租约;
本模块承载 1..N 个单元(**可跨任务混布**),对外只依赖端口。

## 循环语义

```
poll → 解析 → 水位推进(不进逻辑)→ 契约裁剪 → process() → commitSync
              ↘ 异常:不提交 → 退避重试 N 次 → 死信 + 告警
每轮:续租(失败即自停,防双消费)· 心跳(平台据此判 RUNNING / 停滞)
停机:停 poll → logic.close() → 关消费源 → 落 STOPPED
```

## 关键类

| 类 | 职责 |
|---|---|
| **`UnitRunner`** | `start(CreateUnitCmd)` / `stop` / `awaitTermination` / `state`;幂等(同 unitId 只启一个) |
| `RecordSource`(+Factory) | 消费源端口(**刻意不暴露 KafkaConsumer**,便于用内存桩验语义) |
| `MessageDecoder` | canal-json 解析端口 |
| `DefaultContractFilter` | **列白名单 → 行条件(用原始行判定)→ 脱敏**,发生在 `process()` 之前 |
| `ContractView` / `ContractProvider` | 契约视图与刷新(收窄下一批生效) |
| `ClasspathLogicLoader` / `SpiLogicLoader` / `CompositeLogicLoader` | 逻辑来源:① 写在 worker 里 ② 部署期 SPI(合并顺序可配) |
| `UnitRuntime` / `InitContextImpl` / `UnitContextImpl` / `BatchImpl` / `RecordImpl` | 运行时与 SDK 上下文实现(逻辑只拿到只读视图) |
| `FailurePolicy` | 重试次数/退避/耗尽后 STOP 或 SKIP |

## 自检(内存桩,不需要 Kafka)

```bash
javac --release 17 -d /tmp/out $(find outlet-sdk/src/main outlet-control/src outlet-worker/src -name '*.java')
java -cp /tmp/out com.dcp.outlet.worker.SelfCheck      # 41 项
```
覆盖:提交语义(批后提交 offset+1 · 空批不重复提交 · **注入起点不被覆盖**)· 水位不进逻辑 ·
契约裁剪(含**行条件列不在白名单也不能误丢**)· 失败不提交→重试→死信 · 单线程串行 ·
停机幂等 + `close()` · 租约失效自停 · 单元幂等。
