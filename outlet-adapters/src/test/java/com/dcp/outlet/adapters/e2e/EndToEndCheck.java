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
 * **控制面建任务 → 批打底 → 算起点 → 下发单元 → 计算面消费 → 位点提交 → 进度上报**
 * 核心主张:**批(≤T0)与流(>T0)合起来既不丢也不重**;重复投递幂等;返工(换新 T0)后依然不丢不重。
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
        testNoLeaseNoConsume();

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
        check("E1 任务进入 RUNNING", w.status, TaskStatus.RUNNING);
        check("E1 一单元一分区(instances=3)", w.units.size(), 3);

        boolean done = w.awaitConsumed(5000);
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
        w.awaitConsumed(5000);
        w.stopAll();                                        // 先停旧单元并释放租约
        int before = w.streamSink.size();

        w.redispatch("replay", 6, true);                    // 从同一批起点重投一遍(已授租约)
        Thread.sleep(120);
        check("E2 重放后目标端行数不变(幂等)", w.streamSink.size(), before);
        check("E2 逻辑确实收到过重复", w.logic.applied() > 18, true);
    }

    // ============ 3. 返工:换新 T0 重做批 ============
    private static void testReworkWithNewT0() throws Exception {
        Wire w = new Wire("e2e-rework", 8);                 // 新 T0 = 下标 8
        w.createTask();
        w.awaitConsumed(5000);

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
        check("E4 批失败 → BATCH_FAILED", w.status, TaskStatus.BATCH_FAILED);
        check("E4 未下发任何单元", w.runnerUnitsStarted(), 0);
        check("E4 未提交任何位点", w.kafka.committedOf(w.group(), 0), -1L);
    }

    // ============ 5. 防双消费:未获租约的单元自停、不消费 ============
    private static void testNoLeaseNoConsume() throws Exception {
        Wire w = new Wire("e2e-lease", 5);
        w.createTask();
        w.awaitConsumed(5000);
        w.stopAll();
        int appliedBefore = w.logic.applied();
        int sinkBefore = w.streamSink.size();

        w.redispatch("nolease", 6, false);                  // 故意不授租约
        Thread.sleep(150);

        check("E5 未获租约的单元未消费", w.logic.applied(), appliedBefore);
        check("E5 目标端未变化", w.streamSink.size(), sinkBefore);
        check("E5 单元状态为 LEASE_LOST(自停)",
                w.runner.state("u-nolease-0"), UnitRunner.State.LEASE_LOST);
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
        final InMemoryUnitStore unitStore = new InMemoryUnitStore();
        final LeaseManager leases = new LeaseManager();
        final LeaseBackedHeartbeat heartbeat = new LeaseBackedHeartbeat();
        final UnitRunner runner;
        final UnitDispatcherAdapter dispatcher;
        final TaskScheduler scheduler;
        final TaskKernelService kernel;
        final SimulatingBatchRunner batch;
        final String taskId; final long t0;
        TaskStatus status;
        List<PlannedUnit> units = List.of();

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
            AnchorIndexer indexer = new AnchorIndexer(anchors, new InMemoryWatermarkStore(),
                    new AnchorIndexPolicy(3, 10_000L, 7L * 24 * 3600 * 1000, 500),
                    new MonotonicityGuard(), () -> 0L);
            for (int p = 0; p < PARTITIONS; p++) {
                for (int i = 0; i < ROWS; i++) {
                    indexer.onEvent(TOPIC, p, 1, i, kafka.commitTsAt(TOPIC, p, i));
                }
            }
            StartOffsetService startOffsets = new StartOffsetService(anchors, reader,
                    AnchorIndexPolicy.defaults(), null, 200_000);

            // 计算面
            InMemoryLogicRegistry registry = new InMemoryLogicRegistry();
            registry.register(LOGIC, VERSION, logic);
            UnitMetrics metrics = new SimpleUnitMetrics();
            runner = new UnitRunner(new ClasspathLogicLoader(registry), new SdkEventDecoder(),
                    DefaultContractFilter.permissive(),
                    new FakeKafkaRecordSource.Factory(kafka),
                    new LeaseBackedLeaseGuard(leases, 30_000L, () -> 0L),
                    heartbeat, new WatermarkProvider() {
                        public void update(int partition, long watermarkTs) { }
                        public long safeWatermark(int partition) { return 0L; }
                    },
                    new InMemoryDeadLetterSink(), metrics, id -> silent());
            dispatcher = new UnitDispatcherAdapter(runner);

            // 控制面
            batch = new SimulatingBatchRunner(kafka, TOPIC, PARTITIONS, batchSink, t0);
            scheduler = new TaskScheduler(tasks, unitStore, dispatcher, startOffsets, leases,
                    (topic, partition, epoch) -> 0L, batch, () -> 0L, 30_000L);
            kernel = new TaskKernelService(streams, catalog, contracts, tasks, unitStore,
                    new UnitPlanner(), scheduler);
        }

        String group() { return "task-" + taskId; }

        void createTask() {
            TaskRecord t = new TaskRecord(taskId).name("E2E").consumerType("custom")
                    .stream(STREAM).logic(LOGIC, VERSION).instances(PARTITIONS)
                    .contractRef("ct-e2e").startPolicy("snapshot")
                    .params(Map.of("idField", "id"));
            kernel.create(t);
            this.status = t.status();
            this.units = unitStore.byTask(taskId);
        }

        int runnerUnitsStarted() {
            int n = 0;
            for (PlannedUnit u : units) if (runner.state(u.unitId()) != null) n++;
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

        /** 停机:停单元 + 释放租约(重启/返工前必须先做,否则新单元会撞 LEASE_CONFLICT)。 */
        void stopAll() throws InterruptedException {
            for (PlannedUnit u : units) {
                runner.stop(u.unitId());
                leases.release(u.unitId());
            }
            Thread.sleep(80);
        }

        /** 用新单元 ID 从指定起点重投(模拟重复投递 / 重启);withLease=false 时故意不授租约。 */
        void redispatch(String tag, long startOffset, boolean withLease) throws InterruptedException {
            for (int p = 0; p < PARTITIONS; p++) {
                String unitId = "u-" + tag + "-" + p;
                if (withLease) {
                    leases.grant(unitId, taskId, TOPIC, p, 1, 30_000L, 0L);
                }
                dispatcher.dispatch(new DispatchRequest("req-" + tag + "-" + p, unitId,
                        taskId, TOPIC, STREAM, List.of(p), Map.of(p, startOffset), group(),
                        LOGIC, VERSION, Map.of("idField", "id"), FailureSpec.defaults(), 30_000L));
            }
            Thread.sleep(150);
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

    private static void check(String what, Object actual, Object expected) {
        boolean ok = String.valueOf(actual).equals(String.valueOf(expected));
        if (ok) passed++; else failed++;
        System.out.println((ok ? "  PASS  " : "  FAIL  ") + what + " = " + actual
                + (ok ? "" : "   (期望 " + expected + ")"));
    }
}
