package com.dcp.outlet.adapters;

import com.dcp.outlet.control.BatchRunner;
import com.dcp.outlet.control.ContractInfo;
import com.dcp.outlet.control.ContractLookup;
import com.dcp.outlet.control.SnapshotSource;
import com.dcp.outlet.control.StreamInfo;
import com.dcp.outlet.control.StreamRegistry;
import com.dcp.outlet.control.T0Selector;
import com.dcp.outlet.control.TaskRecord;
import com.dcp.outlet.control.TaskStore;
import com.dcp.outlet.sdk.SdkLogger;

import java.util.Optional;

/**
 * 批阶段执行体选择 + 委派(设计 §4.3.1~4.3.2)。
 *
 * <p>先算 T0 → 估算行数 → 按 {@link BatchExecutorPolicy} 选执行体,再交给对应 {@link BatchRunner}。
 * 三个执行体都可插拔:local(进程内,开发期)、container(批容器)、spark。
 *
 * <p><b>批/增量身份一致性</b>:行数估算也必须用任务契约身份(contractRef → consumer_id);
 * 身份不可用 → 直接失败(fail-closed),不选执行体。
 */
public class SelectingBatchRunner implements BatchRunner {

    private final BatchRunner local;
    private final BatchRunner container;
    private final BatchRunner spark;
    private final T0Selector t0;
    private final StreamRegistry streams;
    private final TaskStore tasks;
    private final SnapshotSource snapshot;
    private final ContractLookup contracts;
    private final String mode;
    private final SdkLogger log;

    public SelectingBatchRunner(BatchRunner local, BatchRunner container, BatchRunner spark,
                                T0Selector t0, StreamRegistry streams, TaskStore tasks,
                                SnapshotSource snapshot, ContractLookup contracts, String mode, SdkLogger log) {
        this.local = local;
        this.container = container;
        this.spark = spark;
        this.t0 = t0;
        this.streams = streams;
        this.tasks = tasks;
        this.snapshot = snapshot;
        this.contracts = contracts;
        this.mode = mode;
        this.log = log;
    }

    @Override
    public void runAsync(String taskId, BatchCallback callback) {
        try {
            TaskRecord task = tasks.byId(taskId);
            if (task == null) {
                callback.done(0L, false);
                return;
            }
            java.util.Optional<String> identity = SchedulerBatchRunner.resolveConsumer(task, contracts);
            if (identity.isEmpty()) {
                log.error("批阶段失败:任务契约身份不可用(contractRef={}) —— 契约缺失/未生效/无 consumer_id",
                        task.contractRef());
                callback.done(0L, false);
                return;
            }
            String consumerId = identity.get();
            long rows = -1L;
            if (!task.streams().isEmpty()) {
                Optional<StreamInfo> si = streams.get(task.streams().get(0));
                if (si.isPresent()) {
                    T0Selector.Decision d = t0.select(si.get().topic(), 0, null);
                    rows = snapshot.estimateRows(consumerId, si.get().stream(), d.tso());
                }
            }
            BatchExecutorPolicy.Exec pick = BatchExecutorPolicy.pick(mode, rows);
            BatchRunner delegate = switch (pick) {
                case LOCAL -> local;
                case CONTAINER -> container;
                case SPARK -> spark;
            };
            log.info("批阶段执行体={} (rows={}, mode={}, consumer={}) task={}", pick, rows, mode, consumerId, taskId);
            delegate.runAsync(taskId, callback);
        } catch (Throwable e) {
            log.error("批阶段执行体选择失败 task=" + taskId, e);
            callback.done(0L, false);
        }
    }
}
