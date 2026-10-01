package com.dcp.outlet.adapters.e2e;

import com.dcp.outlet.control.*;
import com.dcp.outlet.adapters.*;
import com.dcp.outlet.control.support.*;
import com.dcp.outlet.adapters.e2e.support.*;
import com.dcp.outlet.sdk.*;
import com.dcp.outlet.worker.*;

import java.util.*;

/**
 * 端到端自检(内存 Kafka,无需真实 broker/DB):
 * **控制面建任务 → 批打底 → 算起点 → 框架出口(任务级 activate)→ 分区级执行器 → 位点提交**
 * 核心主张:**批(≤T0)与流(>T0)合起来既不丢也不重**;重复投递幂等;返工(换新 T0)后依然不丢不重。
 * 租约/判死/迁移/防双消费由统一调度框架负责(归属键唯一),本自检只验证框架出口之前的控制面语义
 * 与"任务级下发 → 分区级执行"的正确性。
 */
public final class EndToEndCheck {

    private static final String TOPIC = "dcp.cdc.target_db.rt_sync_c";
    private static final String STREAM = "rt_sync_c";
    private static final String LOGIC = "es-sync";
    private static final String VERSION = "1.0.0";
    private static final int PARTITIONS = 3;
    private static final int ROWS = 12;
    private static final long TSO_BASE = 1_000_000L;
    private static final long TSO_STEP = 10L;
    private static int passed, failed;

    public static void main(String[] args) throws Exception {
        testNoLossNoDup();
        testReplayIdempotent();
        testReworkWithNewT0();
        testBatchFailureBlocksHandover();

        System.out.println();
        System.out.println("==================================================");
        System.out.println("  通过 " + passed + " 项, 失败 " + failed + " 项");
        System.out.println("==================================================");
        if (failed > 0) System.exit(1);
    }

    // ============ 1. 不丢不重 ============
    private static void testNoLossNoDup() throws Exception {
        Wire w = new Wire("e2e", 5);                        // T0 = 第 5 条(下标 5)
        w.createTask();
        check("E1 任务进入 RUNNING", w.status(), "RUNNING");
        check("E1 任务级激活一次(单流)", w.activations().size(), 1);
        check("E1 激活覆盖全部分区(3)", w.activationPartitions(), 3);

        boolean done = w.awaitConsumed(10_000);
        check("E1 消费到分区末尾", done, true);

        check("E1 批打底行数(3×6)", w.batchSink.size(), 18);
        check("E1 流消费行数(3×6)", w.streamSink.size(), 18);

        Set<String> union = new HashSet<>(w.batchSink.keySet());
        union.addAll(w.streamSink.keySet());
        check("E1 并集 = 全量(3×12)", union.size(), 36);

        Set<String> overlap = new HashSet<>(w.batchSink.keySet());
        overlap.retainAll(w.streamSink.keySet());
        check("E1 批与流无重叠(不重)", overlap.size(), 0);

        for (int p = 0; p < PARTITIONS; p++) {
            check("E1 p" + p + " 已提交到位点末尾", w.kafka.committedOf(w.group(), p), (long) ROWS);
        }
    }

    // ============ 2. 重复投递幂等 ============
    private static void testReplayIdempotent() throws Exception {
        Wire w = new Wire("e2e-replay", 5);
        w.createTask();
        w.awaitConsumed(10_000);
        w.stopAll();                                        // 先停旧分区执行器
        int before = w.streamSink.size();

        w.redispatch("replay", 6);                          // 从同一批起点重投一遍
        Thread.sleep(120);
        check("E2 重放后目标端行数不变(幂等)", w.streamSink.size(), before);
        check("E2 逻辑确实收到过重复", w.logic.applied() > 18, true);
    }

    // ============ 3. 返工:换新 T0 重做批 ============
    private static void testReworkWithNewT0() throws Exception {
        Wire w = new Wire("e2e-rework", 8);                 // 新 T0 = 下标 8
        w.createTask();
        w.awaitConsumed(10_000);

        check("E3 新批打底行数(3×9)", w.batchSink.size(), 27);
        check("E3 新流消费行数(3×3)", w.streamSink.size(), 9);
        Set<String> union = new HashSet<>(w.batchSink.keySet());
        union.addAll(w.streamSink.keySet());
        check("E3 并集仍为全量", union.size(), 36);
        Set<String> overlap = new HashSet<>(w.batchSink.keySet());
        overlap.retainAll(w.streamSink.keySet());
        check("E3 批与流仍无重叠", overlap.size(), 0);
    }

    // ============ 4. 批失败:不交接(不猜起点) ============
    private static void testBatchFailureBlocksHandover() throws Exception {
        Wire w = new Wire("e2e-fail", 5);
        w.batch.failNextTime();
        w.createTask();
        check("E4 批失败 → BATCH_FAILED", w.status(), "BATCH_FAILED");
        check("E4 未启动任何分区执行器", w.runnerUnitsStarted(), 0);
        check("E4 未提交任何位点", w.kafka.committedOf(w.group(), 0), -1L);
    }

    // ================= 端到端装配 =================
    private static final class Wire {
        final FakeKafka kafka = new FakeKafka();
        final Map<String, Map<String, Object>> batchSink = new LinkedHashMap<>();
        final Map<String, Map<String, Object>> streamSink = new LinkedHashMap<>();
        final MemorySinkLogic logic = new MemorySinkLogic(streamSink);
        final InMemoryStreamRegistry streams = new InMemoryStreamRegistry();
        final InMemoryLogicCatalog catalog = new InMemoryLogicCatalog();
        final InMemoryContractLookup contracts = new InMemoryContractLookup();
        final InMemoryTaskStore tasks = new InMemoryTaskStore();
        final UnitRunner runner;
        final FrameworkTaskSink sink;
        final TaskScheduler scheduler;
        final TaskKernelService kernel;
        final SimulatingBatchRunner batch;
        final String taskId; final long t0;

        /** 一次任务级 activate 的记录(任务 + 流 + 分区覆盖数)。 */
        private final List<Activation> activations = new ArrayList<>();
        private final List<String> batchFailed = new ArrayList<>();

        Wire(String tag, int boundaryIndex) {
            this.taskId = tag;
            this.t0 = TSO_BASE + boundaryIndex * TSO_STEP;

            // 数据:3 分区 × 12 条,commitTs 单调
            for (int p = 0; p < PARTITIONS; p++) {
                for (int i = 0; i < ROWS; i++) {
                    String payload = "EVENT|" + (TSO_BASE + i * TSO_STEP) + "|" + STREAM
                            + "|INSERT|id:u-" + p + "-" + i + ",v:" + i;
                    kafka.append(TOPIC, p, TSO_BASE + i * TSO_STEP,
                            SdkEventDecoder.eventPayload(TSO_BASE + i * TSO_STEP, STREAM, Op.INSERT,
                                    "id:u-" + p + "-" + i + ",v:" + i));
                }
            }

            streams.register(new StreamInfo(STREAM, TOPIC, PARTITIONS));
            catalog.register(LOGIC, VERSION);
            contracts.put("ct-e2e", true, Set.of(STREAM));

            // 起点服务:锚点索引 + 轻量读取
            InMemoryAnchorStore anchors = new InMemoryAnchorStore();
            FakeKafkaPartitionReader reader = new FakeKafkaPartitionReader(kafka);
            java.util.concurrent.atomic.AtomicLong clock = new java.util.concurrent.atomic.AtomicLong();
            AnchorIndexer indexer = new AnchorIndexer(anchors, new InMemoryWatermarkStore(),
                    new AnchorIndexPolicy(3, 10_000L, 7L * 24 * 3600 * 1000, 500),
                    new MonotonicityGuard(), clock::get);
            for (int p = 0; p < PARTITIONS; p++) {
                for (int i = 0; i < ROWS; i++) {
                    clock.addAndGet(1000L);
                    indexer.onEvent(TOPIC, p, 1, i, kafka.commitTsAt(TOPIC, p, i));
                }
            }
            StartOffsetService startOffsets = new StartOffsetService(anchors, reader);

            // 计算面
            InMemoryLogicRegistry registry = new InMemoryLogicRegistry();
            registry.register(LOGIC, VERSION, logic);
            UnitMetrics metrics = new SimpleUnitMetrics();
            runner = new UnitRunner(new ClasspathLogicLoader(registry), new SdkEventDecoder(),
                    DefaultContractFilter.permissive(),
                    new FakeKafkaRecordSource.Factory(kafka),
                    new WatermarkProvider() {
                        public void update(int partition, long watermarkTs) { }
                        public long safeWatermark(int partition) { return 0L; }
                    },
                    new InMemoryDeadLetterSink(), metrics, id -> silent());

            // 框架出口测试实现:任务级 activate → 按分区展开为分区级执行器(unitId = taskId#partId)
            sink = new FrameworkTaskSink() {
                @Override public void submitBatching(TaskRecord task, String topic, String stream, int partitions) {
                    // 批阶段登记(框架只登记不分配),本地直连无框架 → 仅记录
                }
                @Override public void activate(TaskRecord task, String topic, String kafkaBootstrap,
                                               String stream, Map<Integer, Long> startOffsets,
                                               Map<Integer, Long> startTsos) {
                    for (Map.Entry<Integer, Long> e : startOffsets.entrySet()) {
                        int p = e.getKey();
                        String unitId = task.taskId() + "#" + p;
                        runner.start(new CreateUnitCmd()
                                .reqId("act-" + task.taskId() + "-" + p).unitId(unitId).taskId(task.taskId())
                                .topic(topic).stream(stream).contractRef(task.contractRef())
                                .kafkaBootstrap(kafkaBootstrap)
                                .partitions(List.of(p))
                                .startOffsets(Map.of(p, e.getValue()))
                                .startTsos(Map.of(p, startTsos.getOrDefault(p, 0L)))
                                .consumerGroup(task.consumerGroup())
                                .logic(LogicRef.of(task.logicId(), task.logicVersion()))
                                .params(task.params()).failure(toFailure(task.failure())));
                    }
                    activations.add(new Activation(task.taskId(), topic, stream, startOffsets.size()));
                }
                @Override public void setPhase(String namespace, String taskId, String phase) { }
                @Override public void cancelTask(String namespace, String taskId) {
                    for (int p = 0; p < PARTITIONS; p++) runner.stop(taskId + "#" + p);
                }
                @Override public void markBatchFailed(String namespace, String taskId, String detail) { batchFailed.add(taskId); }
            };

            // 控制面
            batch = new SimulatingBatchRunner(kafka, TOPIC, PARTITIONS, batchSink, t0);
            scheduler = new TaskScheduler(tasks, streams, startOffsets,
                    (topic, partition, epoch) -> 0L, batch, null, sink, 1);
            kernel = new TaskKernelService(streams, catalog, contracts, tasks, scheduler);
        }

        String group() { return "task-" + taskId; }

        void createTask() {
            TaskRecord t = new TaskRecord(taskId).name("E2E").consumerType("custom")
                    .stream(STREAM).logic(LOGIC, VERSION)
                    .contractRef("ct-e2e").startPolicy("snapshot")
                    .params(Map.of("idField", "id"));
            kernel.create(t);
        }

        /** 状态派生(与生产 TasksController.deriveStatus 同口径):批失败 > 已激活 > 登记中。 */
        String status() {
            if (!batchFailed.isEmpty()) return "BATCH_FAILED";
            if (!activations.isEmpty()) return "RUNNING";
            return "SCHEDULED";
        }

        List<Activation> activations() { return List.copyOf(activations); }

        int activationPartitions() { return activations.isEmpty() ? 0 : activations.get(0).partitions(); }

        int runnerUnitsStarted() {
            int n = 0;
            for (int p = 0; p < PARTITIONS; p++) if (runner.state(taskId + "#" + p) != null) n++;
            return n;
        }

        boolean awaitConsumed(long timeoutMillis) throws InterruptedException {
            long deadline = System.currentTimeMillis() + timeoutMillis;
            while (System.currentTimeMillis() < deadline) {
                boolean all = true;
                for (int p = 0; p < PARTITIONS; p++) {
                    if (kafka.committedOf(group(), p) < kafka.latest(TOPIC, p)) all = false;
                }
                if (all) return true;
                Thread.sleep(10);
            }
            return false;
        }

        /** 停机:框架出口 cancelTask → 每分区执行器 runner.stop(重投/返工前先停旧执行器)。 */
        void stopAll() throws InterruptedException {
            sink.cancelTask("outlet", taskId);
            Thread.sleep(80);
        }

        /** 用新 unitId 从指定起点重投(模拟重复投递 / 重启),startTsos 缺省 = 不过滤。 */
        void redispatch(String tag, long startOffset) throws InterruptedException {
            for (int p = 0; p < PARTITIONS; p++) {
                String unitId = "u-" + tag + "-" + p;
                runner.start(new CreateUnitCmd()
                        .reqId("req-" + tag + "-" + p).unitId(unitId).taskId(taskId)
                        .topic(TOPIC).stream(STREAM).contractRef("ct-e2e")
                        .partitions(List.of(p)).startOffsets(Map.of(p, startOffset))
                        .consumerGroup(group()).logic(LogicRef.of(LOGIC, VERSION))
                        .params(Map.of("idField", "id")).failure(FailurePolicy.defaults()));
            }
            Thread.sleep(150);
        }

        private static FailurePolicy toFailure(FailureSpec spec) {
            FailurePolicy.OnFailure on = "continue".equalsIgnoreCase(spec.onFailure())
                    ? FailurePolicy.OnFailure.SKIP
                    : FailurePolicy.OnFailure.STOP;
            return new FailurePolicy(spec.maxRetries(), spec.backoffMillis(), on);
        }

        private static SdkLogger silent() {
            return new SdkLogger() {
                public boolean isDebugEnabled() { return false; }
                public void debug(String f, Object... a) { }
                public void info(String f, Object... a) { }
                public void warn(String f, Object... a) { }
                public void error(String m, Throwable t) { }
                public void error(String f, Object... a) { }
            };
        }
    }

    /** 一次任务级激活的快照。 */
    private record Activation(String taskId, String topic, String stream, int partitions) {}

    private static void check(String what, Object actual, Object expected) {
        boolean ok = String.valueOf(actual).equals(String.valueOf(expected));
        if (ok) passed++; else failed++;
        System.out.println((ok ? "  PASS  " : "  FAIL  ") + what + " = " + actual
                + (ok ? "" : "   (期望 " + expected + ")"));
    }
}
