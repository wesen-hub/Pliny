package com.dcp.scheduler.smoke;

import com.dcp.scheduler.exec.ExecutorProvider;
import com.dcp.scheduler.exec.HookRegistry;
import com.dcp.scheduler.model.Enums;
import com.dcp.scheduler.model.ExecutorSnapshot;
import com.dcp.scheduler.model.OpsRecord;
import com.dcp.scheduler.model.PartRecord;
import com.dcp.scheduler.model.ScaleRules;
import com.dcp.scheduler.model.TaskSpec;
import com.dcp.scheduler.model.TaskStatus;
import com.dcp.scheduler.skeleton.InstanceConfig;
import com.dcp.scheduler.skeleton.InstanceSkeleton;
import com.dcp.scheduler.store.EtcdStore;
import com.dcp.scheduler.store.KeySpace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Optional;
import java.util.UUID;

/**
 * 冒烟:本地 etcd 跑通闭环 —— 提交任务 → 主补 PENDING 分区并分配 → 实例 watch 装载执行 → 快照上报。
 * 用法:
 *   java -jar dcp-scheduler.jar [--etcd http://localhost:2379] [--inst inst-a] [--ns demo] [--no-submit]
 *       [--pin 0:90]              # 提交后钉住 p0,TTL 90s(锁机制冒烟)
 *       [--migrate-pin 0:inst-b]  # 提交后定向迁移 p0 → inst-b 并钉住
 *       [--stuck 0,2]             # 模拟分区执行器卡死(快照停更,验证卡死检测)
 *       [--autoscale --rate 2 --target-rps 1]  # 自动扩缩:固定速率 + 低目标吞吐(验证扩容)
 *       [--autoscale --rate 2 --target-rps 100] # 自动扩缩:高目标吞吐(验证缩容+追平)
 *       [--drain-task --drain-after 20] # 任务注销:跑 20s 后删 spec → DRAINING → 追平 → GONE
 *       [--drain-inst inst-a]            # 实例下架:对 inst-a 写下架 op,分区迁出(需另一终端起 inst-b)
 *       [--lag]                         # 注入 mock Kafka 源:追平判定改 lag 实查(否 则回退 rps 近似)
 *       [--pause-inst inst-b]           # 暂停接单 op:目标实例不接新单,已跑分区不动
 *       [--set-weight inst-a:1.5]       # 调权 op:影响目标打分(0.5~2.0)
 *       [--undrain inst-a]              # 取消下架 op(补验)
 *       [--alerts]                      # 观察告警事件流(/dcp/{ns}/alerts,后台页面可见)
 *       [--err-rate 0.5]                # 错误率调高(验证 ERROR_RATE 告警巡检)
 *       [--observe 60]            # 观察时长(秒)
 * 一个进程同时扮演"实例 + 调度主"(按 ns 参选)。
 */
public class SmokeMain {

    private static final Logger log = LoggerFactory.getLogger(SmokeMain.class);

    public static void main(String[] args) throws Exception {
        String etcd = arg(args, "--etcd", "http://localhost:2379");
        String instId = arg(args, "--inst", "inst-a");
        String ns = arg(args, "--ns", "demo");
        String task = arg(args, "--task", "orders");
        int partitions = Integer.parseInt(arg(args, "--partitions", "4"));
        String pin = arg(args, "--pin", null);                    // "0:90" 钉住 p0 TTL 90s
        String migratePin = arg(args, "--migrate-pin", null);     // "0:inst-b" 定向迁移并钉住
        String stuck = arg(args, "--stuck", null);                // "0,2" 模拟 分区执行器卡死(快照停更)
        boolean autoscale = hasArg(args, "--autoscale");           // 开启自动扩缩(mock 钩子 + 短冷却规则)
        int rate = Integer.parseInt(arg(args, "--rate", "0"));    // 每批固定条数(0=随机)
        long targetRps = Long.parseLong(arg(args, "--target-rps", "1000"));
        int capacity = Integer.parseInt(arg(args, "--capacity", "4"));  // 本 ns 槽位(扩容场景需 ≥ 期望分区数)
        boolean drainTask = hasArg(args, "--drain-task");                // 任务注销:删 spec → DRAINING → GONE
        long drainAfterSec = Long.parseLong(arg(args, "--drain-after", "20")); // 注销前置跑时长
        String drainInst = arg(args, "--drain-inst", null);              // 实例下架目标(id)
        Integer mockCpu = hasArg(args, "--mock-cpu")
                ? Integer.parseInt(arg(args, "--mock-cpu", "0")) : null;  // 固定进程 cpu(验证 cpu 驱逐)
        boolean lag = hasArg(args, "--lag");                        //  注入 mock Kafka:lag 实查(否则回退 rps)
        String pauseInst = arg(args, "--pause-inst", null);          //  暂停接单 op 目标实例
        String setWeight = arg(args, "--set-weight", null);          //  调权 op 目标实例,格式 "inst-a:1.5"
        String undrainInst = arg(args, "--undrain", null);           //  取消下架 op 目标实例
        boolean alerts = hasArg(args, "--alerts");                   //  观察告警事件流(/dcp/{ns}/alerts)
        double errRate = Double.parseDouble(arg(args, "--err-rate", "0.02")); // 错误率(验证 ERROR_RATE 巡检)
        long observeSec = Long.parseLong(arg(args, "--observe", "60"));

        EtcdStore store = EtcdStore.connect(etcd);
        ExecutorProvider provider = new ExecutorProvider();
        provider.registerEmbedded("MOCK", MockExecutor::new);

        InstanceConfig cfg = new InstanceConfig();
        cfg.id = instId;
        cfg.addr = "127.0.0.1";
        cfg.etcd = etcd;
        cfg.ns(ns, capacity, true);          // 本 ns 容量(可配,扩容场景槽位要够),参选调度主
        cfg.reconcileSec = 2;         // smoke 加速
        cfg.snapshotSec = 3;
        cfg.masterDecideSec = 3;
        cfg.mockCpu = mockCpu;
        cfg.kafkaLagSource = lag ? new MockLagSource() : null;
        if (errRate > 0.02) MockExecutor.ERR_RATE = errRate; // 高错误率验证 ERROR_RATE 告警巡检

        log.info("=== smoke 启动: etcd={} inst={} ns={} ===", etcd, instId, ns);
        if (stuck != null) { // 卡死标记要在执行器启动前登记(模拟执行器假死)
            for (String s : stuck.split(",")) {
                MockExecutor.STUCK.add(task + "/" + s.trim());
            }
            log.info("[smoke] 已标记卡死分区: task={} parts={}", task, stuck);
        }
        if (rate > 0) { // 固定速率注册(扩缩容水位冒烟:所有分区同速率,flow 可控)
            for (int p = 0; p < partitions; p++) MockExecutor.RATE.put(task + "/" + p, rate);
            log.info("[smoke] 固定速率: 每 500ms {} 条(task={})", rate, task);
        }
        cfg.hooks = new HookRegistry();
        if (autoscale) {
            cfg.hooks.register("mock", new MockHooks());
        }
        try (InstanceSkeleton skeleton = new InstanceSkeleton(cfg, store, provider)) {
            skeleton.start();

            if (!hasArg(args, "--no-submit")) {
                Thread.sleep(2000); // 等注册完成
                submit(store, ns, task, partitions, autoscale, targetRps);
            }
            if (pin != null) {
                Thread.sleep(14000); // 等注册观察期(10s)过 + 主分配完成(有 owner 才能钉)
                writePinOp(store, ns, task, pin);
            }
            if (migratePin != null) {
                Thread.sleep(14000);
                writeMigratePinOp(store, ns, task, migratePin);
            }
            if (drainTask) {
                Thread.sleep(drainAfterSec * 1000); // 先跑一会产生数据
                deregisterTask(store, ns, task);    // 模拟外壳注销:删 spec + 停生产
            }
            if (drainInst != null) {
                Thread.sleep(14000); // 等观察期过 + 分配完成
                writeDrainInstOp(store, ns, drainInst);
            }
            if (pauseInst != null) {
                Thread.sleep(14000);
                writePauseInstOp(store, ns, pauseInst);
            }
            if (setWeight != null) {
                Thread.sleep(14000);
                writeSetWeightOp(store, ns, setWeight);
            }
            if (undrainInst != null) {
                Thread.sleep(14000);
                writeUndrainOp(store, ns, undrainInst);
            }

            // 观察闭环:任务状态 + 运行执行器数 + 各分区归属/锁 + 快照
            long loops = Math.max(1, observeSec / 5);
            for (int i = 0; i < loops; i++) {
                Thread.sleep(5000);
                Optional<TaskStatus> st = store.getObj(KeySpace.status(ns, task), TaskStatus.class);
                StringBuilder sb = new StringBuilder("任务状态=")
                        .append(st.map(s -> s.state).orElse("-"))
                        .append(" 运行执行器=").append(skeleton.runningCount());
                int shown = Math.max(partitions, currentPartitions(store, ns, task)); // 扩容后动态增长
                for (int p = 0; p < shown; p++) {
                    StringBuilder ps = new StringBuilder("p").append(p).append(":");
                    Optional<PartRecord> pr = store.getObj(KeySpace.part(ns, task, p), PartRecord.class);
                    if (pr.isPresent()) {
                        PartRecord r = pr.get();
                        ps.append(r.state).append("/").append(r.owner == null ? "-" : r.owner);
                        if (r.pin != null) {
                            ps.append(" pin→").append(r.pin.pinnedTo);
                            if (r.pin.expiresAt != null) {
                                long left = (r.pin.expiresAt - System.currentTimeMillis()) / 1000;
                                ps.append(" ttl~").append(Math.max(left, 0)).append("s");
                            }
                        }
                        if (r.drain != null) {
                            long left = (r.drain.deadline - System.currentTimeMillis()) / 1000;
                            ps.append(" drain@").append(Math.max(left, 0)).append("s");
                        }
                    }
                    Optional<ExecutorSnapshot> opt = store.getObj(KeySpace.snap(ns, task, p), ExecutorSnapshot.class);
                    if (opt.isPresent()) {
                        ExecutorSnapshot s = opt.get();
                        ps.append(" proc=").append(s.processedCount);
                        if (s.stopped) ps.append("[stopped]");
                    }
                    sb.append(" | ").append(ps);
                }
                if (alerts) { // 告警事件流观察(监控子文档 §六:后台页面可见)
                    int na = store.getPrefix(KeySpace.alertsPrefix(ns)).size();
                    sb.append(" | 告警事件=").append(na);
                }
                log.info("--- 观察点 {}: {} ---", i + 1, sb);
            }
        } finally {
            store.close();
        }
        log.info("=== smoke 结束(优雅退出已执行逐 ns 注销与归属清理) ===");
    }

    /** 模拟业务外壳提交任务(CONSERVATIVE 交接,演示双窗口判死链路)。 */
    private static void submit(EtcdStore store, String ns, String taskId, int partitions,
                               boolean autoscale, long targetRps) {
        TaskSpec spec = new TaskSpec();
        spec.namespace = ns;
        spec.taskId = taskId;
        spec.topic = "demo-" + taskId; // mock 协议:demo-{taskId}(MockLagSource 按此后缀映射生产水位)
        spec.partitions = partitions;
        spec.minPartitions = autoscale ? 1 : 2;
        spec.maxPartitions = autoscale ? 8 : 16;
        spec.targetRps = targetRps;
        spec.handlerRef = new TaskSpec.HandlerRef();
        spec.handlerRef.type = Enums.HandlerType.EMBEDDED;
        spec.handlerRef.ref = "MOCK";
        spec.handover = Enums.Handover.CONSERVATIVE;
        if (autoscale) { // 任务级规则覆盖 + 缩容钩子声明(双闸门之二)
            spec.scaleDownHooks = "mock";
            spec.scaleRules = new ScaleRules();
            spec.scaleRules.cooldownMinutes = 0; // smoke 加速:无冷却
            spec.scaleRules.upPeriods = 3;
            spec.scaleRules.downPeriods = 6;
            spec.scaleRules.maxStep = 2.0;
            spec.scaleRules.downEnabled = true;
        }
        spec.bizMeta = new HashMap<>();
        spec.bizMeta.put("demo", "true");
        store.put(KeySpace.spec(ns, taskId), spec);
        log.info("[smoke] 已提交任务 {} partitions={} autoscale={} targetRps={}",
                taskId, partitions, autoscale, targetRps);
    }

    /** 当前分区数(扩容后动态增长;观察循环显示范围用)。 */
    private static int currentPartitions(EtcdStore store, String ns, String task) {
        int max = -1;
        for (String key : store.getPrefix(KeySpace.partsPrefix(ns, task)).keySet()) {
            if (key.endsWith("/snap")) continue;
            max = Math.max(max, KeySpace.partIdOf(key));
        }
        return max + 1;
    }

    private static String arg(String[] args, String name, String def) {
        for (int i = 0; i < args.length - 1; i++) {
            if (name.equals(args[i])) return args[i + 1];
        }
        return def;
    }

    private static boolean hasArg(String[] args, String name) {
        for (String a : args) if (name.equals(a)) return true;
        return false;
    }

    // ── 锁机制冒烟:模拟运维后台写 ops 指令(子文档⑧ §六) ──

    /** PIN:钉住指定分区当前 owner,TTL 可选。 */
    private static void writePinOp(EtcdStore store, String ns, String taskId, String arg) {
        String[] kv = arg.split(":");
        int p = Integer.parseInt(kv[0]);
        Long ttl = kv.length > 1 ? Long.parseLong(kv[1]) : null;
        OpsRecord op = OpsRecord.pin(UUID.randomUUID().toString(), taskId, p, ttl, "smoke", "锁机制冒烟");
        store.put(KeySpace.op(ns, op.opId), op);
        log.info("[smoke] 已提交 PIN op: task={} part={} ttl={}s", taskId, p, ttl);
    }

    /** MIGRATE_PIN:定向迁移到目标实例并钉住。 */
    private static void writeMigratePinOp(EtcdStore store, String ns, String taskId, String arg) {
        String[] kv = arg.split(":");
        int p = Integer.parseInt(kv[0]);
        String target = kv[1];
        OpsRecord op = OpsRecord.migratePin(UUID.randomUUID().toString(), taskId, p, target, "smoke", "定向迁移冒烟");
        store.put(KeySpace.op(ns, op.opId), op);
        log.info("[smoke] 已提交 MIGRATE_PIN op: task={} part={} → {}", taskId, p, target);
    }

    /** 模拟业务外壳注销任务(生命周期子文档 3.3):删 spec + 停生产,观察 DRAINING → DELETING → GONE。 */
    private static void deregisterTask(EtcdStore store, String ns, String taskId) {
        MockExecutor.STOPPED.add(taskId); // 生产停,执行器消费完积压后 rps 归零才能追平
        store.delete(KeySpace.spec(ns, taskId));
        log.info("[smoke] 已删 spec + 停生产,模拟外壳注销任务(task={})", taskId);
    }

    /** DRAIN_INSTANCE:实例下架(运维后台 §2.3),已有分区逐步迁出。 */
    private static void writeDrainInstOp(EtcdStore store, String ns, String instId) {
        OpsRecord op = OpsRecord.drainInstance(UUID.randomUUID().toString(), instId, "smoke", "下架冒烟");
        store.put(KeySpace.op(ns, op.opId), op);
        log.info("[smoke] 已提交 DRAIN_INSTANCE op: inst={}", instId);
    }

    /** PAUSE_INSTANCE:暂停接单(运维后台 §2),不接新单,已跑分区不动。 */
    private static void writePauseInstOp(EtcdStore store, String ns, String instId) {
        OpsRecord op = OpsRecord.pauseInstance(UUID.randomUUID().toString(), instId, "smoke", "暂停接单冒烟");
        store.put(KeySpace.op(ns, op.opId), op);
        log.info("[smoke] 已提交 PAUSE_INSTANCE op: inst={}", instId);
    }

    /** SET_WEIGHT:调权(运维后台 §2),影响目标打分。 */
    private static void writeSetWeightOp(EtcdStore store, String ns, String arg) {
        String[] kv = arg.split(":");
        String instId = kv[0];
        double w = Double.parseDouble(kv[1]);
        OpsRecord op = OpsRecord.setWeight(UUID.randomUUID().toString(), instId, w, "smoke", "调权冒烟");
        store.put(KeySpace.op(ns, op.opId), op);
        log.info("[smoke] 已提交 SET_WEIGHT op: inst={} weight={}", instId, w);
    }

    /** UNDRAIN_INSTANCE:取消下架(补验)。 */
    private static void writeUndrainOp(EtcdStore store, String ns, String instId) {
        OpsRecord op = OpsRecord.undrainInstance(UUID.randomUUID().toString(), instId, "smoke", "取消下架冒烟");
        store.put(KeySpace.op(ns, op.opId), op);
        log.info("[smoke] 已提交 UNDRAIN_INSTANCE op: inst={}", instId);
    }
}
