package com.dcp.scheduler.skeleton;

import com.dcp.scheduler.model.ExecutorSnapshot;
import com.dcp.scheduler.store.EtcdStore;
import com.dcp.scheduler.store.KeySpace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 快照上报:周期拉执行器四计数器写 etcd(TTL 30s,共享租约)。
 * 快照键带租约 = 活性信号:实例卡死时快照随租约过期消失,保守交接据此双窗口判死。
 */
public class SnapshotReporter implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(SnapshotReporter.class);

    private final EtcdStore store;
    private final Map<String, InstanceSkeleton.Running> running;
    private final long leaseId;
    private final int intervalSec;
    private final ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor(
            r -> { Thread t = new Thread(r, "snap-reporter"); t.setDaemon(true); return t; });

    public SnapshotReporter(EtcdStore store, Map<String, InstanceSkeleton.Running> running,
                            long leaseId, int intervalSec) {
        this.store = store;
        this.running = running;
        this.leaseId = leaseId;
        this.intervalSec = intervalSec;
    }

    public void start() {
        timer.scheduleAtFixedRate(this::report, intervalSec, intervalSec, TimeUnit.SECONDS);
    }

    private void report() {
        for (InstanceSkeleton.Running r : running.values()) {
            try {
                ExecutorSnapshot s = r.executor.metrics();
                store.putLease(KeySpace.snap(r.ns, r.taskId, r.partId), s, leaseId);
            } catch (Exception e) {
                log.warn("快照上报失败 {}/{}/{}: {}", r.ns, r.taskId, r.partId, e.getMessage());
            }
        }
    }

    @Override
    public void close() {
        timer.shutdownNow();
    }
}
