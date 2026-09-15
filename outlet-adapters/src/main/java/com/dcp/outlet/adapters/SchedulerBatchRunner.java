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
 */
public class SchedulerBatchRunner implements BatchRunner {

    private final TaskStore tasks;
    private final StreamRegistry streams;
    private final SnapshotSource snapshotSource;
    private final T0Selector t0Selector;
    private final LogicLoader logicLoader;
    private final SdkLogger log;

    public SchedulerBatchRunner(TaskStore tasks, StreamRegistry streams, SnapshotSource snapshotSource,
                                T0Selector t0Selector, LogicLoader logicLoader, SdkLogger log) {
        this.tasks = tasks; this.streams = streams; this.snapshotSource = snapshotSource;
        this.t0Selector = t0Selector; this.logicLoader = logicLoader; this.log = log;
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

                T0Selector.Decision d = t0Selector.select(si.get().topic(), 0, null);
                log.info("批阶段开始 task={} {} ({})", taskId, d, d.detail());

                ConsumerLogic logic = logicLoader.load(LogicRef.of(task.logicId(), task.logicVersion())).logic();
                BatchPhaseRunner.Outcome o = new BatchPhaseRunner(snapshotSource).run(
                        taskId, task.consumerGroup(), si.get().stream(), si.get().topic(), d.tso(),
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
