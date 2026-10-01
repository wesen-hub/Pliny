package com.dcp.scheduler.skeleton;

import com.dcp.scheduler.exec.ExecutorProvider;
import com.dcp.scheduler.exec.HookRegistry;
import com.dcp.scheduler.exec.PartitionExecutor;
import com.dcp.scheduler.master.MasterLoop;
import com.dcp.scheduler.model.Enums;
import com.dcp.scheduler.model.ExecutorSnapshot;
import com.dcp.scheduler.model.InstanceRecord;
import com.dcp.scheduler.model.NsInstanceRecord;
import com.dcp.scheduler.model.PartRecord;
import com.dcp.scheduler.model.TaskSpec;
import com.dcp.scheduler.store.EtcdStore;
import com.dcp.scheduler.store.Json;
import com.dcp.scheduler.store.KeySpace;
import io.etcd.jetcd.Watch;
import io.etcd.jetcd.watch.WatchEvent;
import io.etcd.jetcd.watch.WatchResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 实例骨架(总纲 §八 / 生命周期子文档 §1.4):六步接入 + 稳态循环。
 *   启动:全局身份注册 → 逐 ns 注册 + watch + 参选 → 稳态(周期对账 + 快照上报)。
 *   稳态:期望态(etcd 归属)vs 实际态(本地运行)diff,watch 事件驱动 + 周期兜底。
 *   退出:优雅逐 ns 注销、清理归属、停止执行器;崩溃则租约自然过期由主判死。
 */
public class InstanceSkeleton implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(InstanceSkeleton.class);

    /** 运行中的执行器条目(快照上报器共享此 map)。 */
    public static final class Running {
        public final String ns;
        public final String taskId;
        public final String topic;
        public final int partId;
        public final PartitionExecutor executor;
        public final PartitionExecutor.ExecutorContext ctx;

        Running(PartitionExecutor.ExecutorContext ctx, PartitionExecutor executor) {
            this.ns = ctx.ns;
            this.taskId = ctx.taskId;
            this.topic = ctx.topic;
            this.partId = ctx.partId;
            this.ctx = ctx;
            this.executor = executor;
        }
    }

    private final InstanceConfig cfg;
    private final EtcdStore store;
    private final ExecutorProvider provider;

    /** key = ns/task/part → 运行条目。 */
    private final Map<String, Running> running = new ConcurrentHashMap<>();
    private final Map<String, TaskSpec> specCache = new ConcurrentHashMap<>(); // ns/taskId → spec
    private final Map<String, Watch.Watcher> watchers = new ConcurrentHashMap<>(); // ns → watcher
    private final Map<String, MasterLoop> masters = new ConcurrentHashMap<>();     // ns → 参选循环
    private final List<EtcdStore.LeaseHandle> leases = new CopyOnWriteArrayList<>();

    private final ScheduledExecutorService timer = Executors.newScheduledThreadPool(2,
            r -> { Thread t = new Thread(r, "skeleton-timer"); t.setDaemon(true); return t; });
    private volatile long snapLeaseId;
    private SnapshotReporter reporter;
    private volatile boolean closed;

    public InstanceSkeleton(InstanceConfig cfg, EtcdStore store, ExecutorProvider provider) {
        this.cfg = cfg;
        this.store = store;
        this.provider = provider;
    }

    /** 六步接入:全局身份 → 逐 ns 注册/watch/参选 → 稳态循环。 */
    public void start() {
        // ① 全局身份注册(持久,无租约;进程级只一份,跨 ns 共享机器指标)
        InstanceRecord rec = new InstanceRecord();
        rec.id = cfg.id;
        rec.addr = cfg.addr;
        rec.namespaces = new ArrayList<>();
        for (InstanceConfig.NsDecl d : cfg.namespaces) rec.namespaces.add(d.ns);
        rec.meta = freshMeta();
        store.put(KeySpace.instance(cfg.id), rec);
        log.info("实例 {} 全局注册完成,归属 ns={}", cfg.id, rec.namespaces);
        // 机器指标周期刷新(10s;cpu 为进程级自上次采样以来平均)
        timer.scheduleAtFixedRate(() -> {
            try {
                InstanceRecord cur = store.getObj(KeySpace.instance(cfg.id), InstanceRecord.class).orElse(null);
                if (cur == null) return;
                cur.meta = freshMeta();
                store.put(KeySpace.instance(cfg.id), cur);
            } catch (Exception e) {
                log.warn("meta 刷新失败: {}", e.getMessage());
            }
        }, 10, 10, TimeUnit.SECONDS);

        // ②~④ 逐 ns:独立租约注册 + watch + 参选
        for (InstanceConfig.NsDecl d : cfg.namespaces) {
            EtcdStore.LeaseHandle lease = store.grantLease(cfg.leaseTtlSec);
            leases.add(lease);
            NsInstanceRecord reg = new NsInstanceRecord();
            reg.id = cfg.id;
            reg.ns = d.ns;
            reg.capacity = d.capacity;
            reg.weight = d.weight;
            reg.paused = d.paused;
            store.putLease(KeySpace.nsInstance(d.ns, cfg.id), reg, lease.id);
            watchers.put(d.ns, store.watchPrefix(KeySpace.tasksPrefix(d.ns), watchListener(d.ns)));
            if (d.campaign) {
                HookRegistry hooks = cfg.hooks != null ? cfg.hooks : new HookRegistry();
                MasterLoop m = new MasterLoop(d.ns, cfg.id, store, cfg.masterDecideSec, hooks, cfg.kafkaLagSource);
                masters.put(d.ns, m);
                m.start();
            }
            log.info("实例 {} 在 ns[{}] 注册完成 capacity={} campaign={}", cfg.id, d.ns, d.capacity, d.campaign);
        }

        // ⑤ 稳态循环:周期对账(兜底)+ 快照上报(共享租约)
        timer.scheduleAtFixedRate(this::reconcileAll, cfg.reconcileSec, cfg.reconcileSec, TimeUnit.SECONDS);
        EtcdStore.LeaseHandle snapLease = store.grantLease(cfg.leaseTtlSec);
        leases.add(snapLease);
        snapLeaseId = snapLease.id;
        reporter = new SnapshotReporter(store, running, snapLease.id, cfg.snapshotSec);
        reporter.start();
        log.info("实例 {} 进入稳态循环(reconcile={}s snapshot={}s)", cfg.id, cfg.reconcileSec, cfg.snapshotSec);
    }

    // ── watch ──

    private EtcdStore.Listener watchListener(String ns) {
        return new EtcdStore.Listener() {
            @Override public void onEvent(WatchResponse r) {
                boolean partsChanged = false;
                for (WatchEvent e : r.getEvents()) {
                    String key = e.getKeyValue().getKey().toString(StandardCharsets.UTF_8);
                    if (isPartKey(key)) partsChanged = true;
                    else if (key.endsWith("/spec")) {
                        specCache.remove(ns + "/" + KeySpace.taskIdOfSpecKey(key)); // SOFT 变更下次对账生效
                    }
                }
                if (partsChanged) triggerReconcile(ns);
            }
        };
    }

    // ── 对账:期望态(etcd 归属)vs 实际态(本地运行) ──

    private void triggerReconcile(String ns) {
        timer.execute(() -> reconcile(ns)); // 不阻塞 grpc 回调线程
    }

    private void reconcileAll() {
        if (closed) return;
        for (InstanceConfig.NsDecl d : cfg.namespaces) reconcile(d.ns);
    }

    /** 对账串行化:watch 触发 + 周期兜底可能并发,避免同一分区重复装载执行器。 */
    private synchronized void reconcile(String ns) {
        if (closed) return;
        try {
            // 期望态:本 ns 下归属 == 自己 且 ASSIGNED 的分区;DELETING(缩容追平)且 owner=自己 也继续跑
            Map<String, PartRecord> desired = new HashMap<>();   // runKey → 归属
            Map<String, String> taskOf = new HashMap<>();        // runKey → taskId
            for (Map.Entry<String, byte[]> e : store.getPrefix(KeySpace.tasksPrefix(ns)).entrySet()) {
                if (!isPartKey(e.getKey())) continue;
                PartRecord p = Json.fromBytes(e.getValue(), PartRecord.class);
                if (p.assignedTo(cfg.id)
                        || (Enums.PartState.DELETING.equals(p.state) && cfg.id.equals(p.owner))) {
                    String taskId = KeySpace.taskIdOfPartKey(e.getKey());
                    desired.put(runKey(ns, taskId, p.partId), p);
                    taskOf.put(runKey(ns, taskId, p.partId), taskId);
                }
            }
            // diff:启动新执行器
            for (String key : desired.keySet()) {
                if (!running.containsKey(key)) {
                    startExecutor(ns, taskOf.get(key), desired.get(key));
                }
            }
            // diff:停止多余执行器
            for (Iterator<Map.Entry<String, Running>> it = running.entrySet().iterator(); it.hasNext(); ) {
                Map.Entry<String, Running> e = it.next();
                if (!e.getValue().ns.equals(ns)) continue;
                if (!desired.containsKey(e.getKey())) {
                    stopExecutor(e.getValue());
                    it.remove();
                }
            }
        } catch (Exception e) {
            log.warn("[{}] 对账失败: {}", ns, e.getMessage());
        }
    }

    private void startExecutor(String ns, String taskId, PartRecord part) {
        TaskSpec spec = readSpec(ns, taskId);
        if (spec == null) {
            log.warn("[{}] 任务 {} spec 缺失,暂缓启动分区 {}", ns, taskId, part.partId);
            return;
        }
        if (spec.handlerRef == null || !provider.checkLoadable(spec.handlerRef)) {
            log.error("[{}] 任务 {} 执行器不可装载 {},分区 {} 无法启动",
                    ns, taskId, spec.handlerRef == null ? "?" : spec.handlerRef.type + ":" + spec.handlerRef.ref, part.partId);
            return;
        }
        PartitionExecutor.ExecutorContext ctx = new PartitionExecutor.ExecutorContext();
        ctx.ns = ns;
        ctx.taskId = taskId;
        ctx.topic = spec.topic;
        ctx.partId = part.partId;
        ctx.handover = spec.handover;
        ctx.bizMeta = spec.bizMeta;
        try {
            PartitionExecutor ex = provider.load(spec.handlerRef);
            ex.start(ctx);
            running.put(runKey(ns, taskId, part.partId), new Running(ctx, ex));
            log.info("[{}] 启动执行器 {}/{} handler={}:{}", ns, taskId, part.partId,
                    spec.handlerRef.type, spec.handlerRef.ref);
        } catch (Exception e) {
            log.error("[{}] 执行器 {}/{} 启动失败: {}", ns, taskId, part.partId, e.getMessage());
        }
    }

    private void stopExecutor(Running r) {
        try {
            r.executor.stop();
        } catch (Exception e) {
            log.warn("[{}] 执行器 {}/{} 停止异常: {}", r.ns, r.taskId, r.partId, e.getMessage());
        }
        // 主动停的最后一帧(区分"主动停"与"卡死",保守交接据此立即确认)
        ExecutorSnapshot snap;
        try {
            snap = r.executor.metrics();
            snap.stopped = true;
        } catch (Exception e) {
            snap = ExecutorSnapshot.of(r.taskId, r.partId, 0, 0, 0, 0, true);
        }
        store.putLease(KeySpace.snap(r.ns, r.taskId, r.partId), snap, snapLeaseId);
        log.info("[{}] 停止执行器 {}/{}", r.ns, r.taskId, r.partId);
    }

    // ── 辅助 ──

    private TaskSpec readSpec(String ns, String taskId) {
        return specCache.computeIfAbsent(ns + "/" + taskId,
                k -> store.getObj(KeySpace.spec(ns, taskId), TaskSpec.class).orElse(null));
    }

    private InstanceRecord.Meta freshMeta() {
        InstanceRecord.Meta m = new InstanceRecord.Meta();
        Runtime rt = Runtime.getRuntime();
        m.mem = (int) ((rt.totalMemory() - rt.freeMemory()) * 100 / rt.maxMemory());
        m.jvmThreads = Thread.activeCount();
        if (cfg.mockCpu != null) {
            m.cpu = cfg.mockCpu; // smoke 固定值(验证 cpu 驱逐)
        } else {
            // 进程级 cpu(自上次采样以来平均,首次 -1 置 0);meta 10s 刷新天然采样间隔
            OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
            if (os instanceof com.sun.management.OperatingSystemMXBean sun) {
                double load = sun.getProcessCpuLoad();
                m.cpu = load >= 0 ? (int) Math.round(load * 100) : 0;
            }
        }
        return m;
    }

    private static boolean isPartKey(String key) {
        return key.contains("/parts/") && !key.endsWith("/snap");
    }

    private static String runKey(String ns, String taskId, int partId) {
        return ns + "/" + taskId + "/" + partId;
    }

    public int runningCount() {
        return running.size();
    }

    // ── 优雅退出 ──

    @Override
    public void close() {
        if (closed) return;
        closed = true;
        log.info("实例 {} 优雅退出:停止执行器、逐 ns 注销、清理归属", cfg.id);
        // ① 停止全部执行器(写 stopped 快照)
        for (Running r : running.values()) stopExecutor(r);
        running.clear();
        // ② 停止参选循环(让位)
        for (MasterLoop m : masters.values()) m.close();
        masters.clear();
        // ③ 关 watch
        for (Watch.Watcher w : watchers.values()) w.close();
        watchers.clear();
        // ④ 逐 ns 注销(删注册键;先注销再清归属,避免主把分区分回自己)
        for (InstanceConfig.NsDecl d : cfg.namespaces) {
            store.delete(KeySpace.nsInstance(d.ns, cfg.id));
        }
        // ⑤ 清理归属(CAS:仅当仍属于自己才删;主会补 PENDING 重分)
        for (InstanceConfig.NsDecl d : cfg.namespaces) {
            Map<String, byte[]> parts = store.getPrefix(KeySpace.tasksPrefix(d.ns));
            for (Map.Entry<String, byte[]> e : parts.entrySet()) {
                if (!isPartKey(e.getKey())) continue;
                try {
                    PartRecord p = Json.fromBytes(e.getValue(), PartRecord.class);
                    if (p.assignedTo(cfg.id)) {
                        store.casDelete(KeySpace.part(d.ns, KeySpace.taskIdOfPartKey(e.getKey()), p.partId), e.getValue());
                    }
                } catch (Exception ignored) { /* 坏记录跳过 */ }
            }
        }
        // ⑥ 关租约/上报器/定时器
        for (EtcdStore.LeaseHandle l : leases) l.close();
        leases.clear();
        if (reporter != null) reporter.close();
        timer.shutdownNow();
    }
}
