# dcp-outlet-integration —— 接线层

**唯一同时依赖控制面与计算面的模块**,把两侧接起来(同进程部署形态)。

| 类 | 作用 |
|---|---|
| `UnitDispatcherAdapter` | 控制面 `DispatchRequest` → 计算面 `CreateUnitCmd` → `UnitRunner.start` |
| `LeaseBackedLeaseGuard` | 计算面续约 → 控制面 `LeaseManager`;**续约失败即单元自停** |
| `LeaseBackedHeartbeat` | 心跳落地(offset/tso/时间)→ 供进度与巡检 |
| `SdkEventDecoder` | 极简事件解码(生产替换为 canal-json 完整解析) |

**依赖方向**:`integration → {control, worker}`;`control` 与 `worker` **互不依赖**(各自只依赖 SDK)。
改成"HTTP 下发到远程 worker"时,只需替换 `outlet-adapters` 的 `HttpUnitDispatcher`,本模块可不再使用。

## 自检(端到端,内存 Kafka,验证**不丢不重**)

```bash
javac --release 17 -d /tmp/out $(find outlet-sdk/src/main outlet-control/src outlet-worker/src outlet-integration/src -name '*.java')
java -cp /tmp/out com.dcp.outlet.integration.EndToEndCheck     # 22 项
```
断言:批覆盖 `≤T0` + 流覆盖 `>T0` = 全量且**零重叠** · 重投幂等 · 换新 T0 返工仍不丢不重 ·
批失败不交接 · **未获租约的单元自停且不消费**。
