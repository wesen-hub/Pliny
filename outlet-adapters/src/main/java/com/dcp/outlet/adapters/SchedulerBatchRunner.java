package com.dcp.outlet.adapters;

import com.dcp.outlet.control.*;
import com.dcp.outlet.sdk.ConsumerLogic;
import com.dcp.outlet.sdk.LogicLoader;
import com.dcp.outlet.sdk.LogicRef;
import com.dcp.outlet.sdk.SdkLogger;

import java.util.Optional;

/**
 * 把「批阶段运行器」接进控制面调度器(实现 {@link BatchRunner} 端口)。
 *
 * <pre>
 *   TaskScheduler.onCreated ──▶ 本类.runAsync
 *        ├─ 取流登记(topic/分区数)
 *        ├─ T0 三规则选边界(R1/R2/R3)
 *        ├─ 加载 logic(与增量同一份)
 *        ├─ BatchPhaseRunner.run(mode=SNAPSHOT)
 *        └─ 回调 done(t0, ok) → 成功才交接;失败 → BATCH_FAILED(人工重做批)
 * </pre>
 *
 * <p>异步:在守护线程中执行,不阻塞建任务请求。
 *
 * <p><b>批/增量身份一致性</b>:批段读快照的 consumerId 必须 = 任务契约的 consumer_id
 * (从 {@code contractRef → ContractInfo.consumerId} 解析),与增量段同一契约;
 * 契约缺失/未生效/无 consumer_id → 直接 BATCH_FAILED(fail-closed,不静默用服务级身份)。
 */
public class SchedulerBatchRunner implements BatchRunner {

    private final TaskStore tasks;
    private final StreamRegistry streams;
    private final SnapshotSource snapshotSource;
    private final T0Selector t0Selector;
    private final LogicLoader logicLoader;
    private final ContractLookup contracts;
    private final SdkLogger log;

    public SchedulerBatchRunner(TaskStore tasks, StreamRegistry streams, SnapshotSource snapshotSource,
                                T0Selector t0Selector, LogicLoader logicLoader, ContractLookup contracts,
                                SdkLogger log) {
        this.tasks = tasks; this.streams = streams; this.snapshotSource = snapshotSource;
        this.t0Selector = t0Selector; this.logicLoader = logicLoader; this.contracts = contracts; this.log = log;
    }

    /** 批段身份解析:任务 contractRef → 契约 consumer_id;不可用返回 empty(fail-closed 依据) */
    static java.util.Optional<String> resolveConsumer(TaskRecord task, ContractLookup contracts) {
        String ref = task == null ? null : task.contractRef();
        if (ref == null || ref.isBlank()) return java.util.Optional.empty();
        return contracts.find(ref)
                .filter(ContractInfo::active)
                .map(ContractInfo::consumerId)
                .filter(id -> id != null && !id.isBlank());
    }

    @Override
    public void runAsync(String taskId, BatchCallback callback) {
        Thread t = new Thread(() -> {
            try {
                TaskRecord task = tasks.byId(taskId);
                if (task == null) { callback.done(0L, false); return; }
                String stream = task.streams().get(0);
                Optional<StreamInfo> si = streams.get(stream);
                if (si.isEmpty()) { log.error("批阶段失败:流未登记:" + stream); callback.done(0L, false); return; }

                java.util.Optional<String> identity = resolveConsumer(task, contracts);
                if (identity.isEmpty()) {
                    log.error("批阶段失败:任务契约身份不可用(批/增量身份一致性要求,contractRef={}) —— 契约缺失/未生效/无 consumer_id",
                            task.contractRef());
                    callback.done(0L, false);
                    return;
                }
                String consumerId = identity.get();

                T0Selector.Decision d = t0Selector.select(si.get().topic(), 0, null);
                log.info("批阶段开始 task={} consumer={} {} ({})", taskId, consumerId, d, d.detail());

                ConsumerLogic logic = logicLoader.load(LogicRef.of(task.logicId(), task.logicVersion())).logic();
                BatchPhaseRunner.Outcome o = new BatchPhaseRunner(snapshotSource).run(
                        taskId, consumerId, task.consumerGroup(), si.get().stream(), si.get().topic(), d.tso(),
                        si.get().partitions(), task.logicId(), task.logicVersion(), logic, task.params(), log);
                log.info("批阶段结束 task={} {}", taskId, o);
                callback.done(d.tso(), o.ok);
            } catch (Throwable e) {
                log.error("批阶段异常 task=" + taskId, (Throwable) e);
                callback.done(0L, false);
            }
        }, "outlet-batch-" + taskId);
        t.setDaemon(true);
        t.start();
    }
}
