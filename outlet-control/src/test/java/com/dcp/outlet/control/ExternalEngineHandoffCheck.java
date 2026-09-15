package com.dcp.outlet.control;

import com.dcp.outlet.control.support.ArrayPartitionReader;
import com.dcp.outlet.control.support.InMemoryAnchorStore;
import com.dcp.outlet.control.support.InMemoryContractLookup;
import com.dcp.outlet.control.support.InMemoryLogicCatalog;
import com.dcp.outlet.control.support.InMemoryStreamRegistry;
import com.dcp.outlet.control.support.InMemoryTaskStore;
import com.dcp.outlet.control.support.InMemoryUnitStore;
import com.dcp.outlet.control.support.InMemoryWatermarkStore;
import com.dcp.outlet.control.support.RecordingBatchRunner;
import com.dcp.outlet.control.support.RecordingDispatcher;
import com.dcp.outlet.control.support.WatermarkLatestTsoProvider;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 方案 C:增量段 = 外部引擎(Flink)—— 交接时不规划/下发 v2 单元,而是把"每分区起点"播给外部引擎适配器。
 */
public final class ExternalEngineHandoffCheck {

    private static int passed, failed;
    private static final String STREAM = "rt_sync_c";
    private static final String TOPIC = "dcp.cdc.target_db.rt_sync_c";

    public static void main(String[] args) {
        System.out.println("=== 外部引擎交接(方案 C,离线) ===");

        InMemoryStreamRegistry streams = new InMemoryStreamRegistry();
        InMemoryLogicCatalog logics = new InMemoryLogicCatalog();
        InMemoryContractLookup contracts = new InMemoryContractLookup();
        InMemoryTaskStore tasks = new InMemoryTaskStore();
        InMemoryUnitStore units = new InMemoryUnitStore();
        RecordingDispatcher unitDispatcher = new RecordingDispatcher();
        RecordingBatchRunner batch = new RecordingBatchRunner();
        InMemoryWatermarkStore watermarks = new InMemoryWatermarkStore();
        ArrayPartitionReader reader = new ArrayPartitionReader();

        streams.register(new StreamInfo(STREAM, TOPIC, 3));
        logics.register("tag-engine", "1.0.0");
        contracts.put("ct-tag-001", true, Set.of(STREAM));

        InMemoryAnchorStore anchors = new InMemoryAnchorStore();
        AnchorIndexer indexer = new AnchorIndexer(anchors, watermarks,
                new AnchorIndexPolicy(100, 10_000L, 7L * 24 * 3600 * 1000, 500),
                new MonotonicityGuard(), () -> 0L);
        for (int p = 0; p < 3; p++) {
            reader.putMonotonic(TOPIC, p, 1200, 1_000_000L + p * 1000L, 10L, 3);
            for (int i = 0; i < 1200; i++) {
                indexer.onEvent(TOPIC, p, 1, i, reader.readFrom(TOPIC, p, i, 1).get(0).commitTs());
            }
        }

        StartOffsetService sos = new StartOffsetService(anchors, reader,
                AnchorIndexPolicy.defaults(), null, 200_000);
        LeaseManager leases = new LeaseManager();

        // 外部引擎适配器:捕获 (task, topic, stream, starts)
        AtomicReference<String> gotTopic = new AtomicReference<>();
        AtomicReference<String> gotStream = new AtomicReference<>();
        AtomicReference<Map<Integer, Long>> gotStarts = new AtomicReference<>();
        AtomicReference<String> gotTaskId = new AtomicReference<>();
        ExternalIncrementalDispatcher external = (task, topic, stream, starts) -> {
            gotTaskId.set(task.taskId());
            gotTopic.set(topic);
            gotStream.set(stream);
            gotStarts.set(starts);
        };

        TaskScheduler scheduler = new TaskScheduler(tasks, units, unitDispatcher, sos, leases,
                new WatermarkLatestTsoProvider(watermarks), batch, external, () -> 50_000L, 30_000L);
        TaskKernelService kernel = new TaskKernelService(streams, logics, contracts, tasks, units,
                new UnitPlanner(), scheduler);

        TaskRecord task = new TaskRecord("tag-task-1")
                .name("打标").consumerType("tag").owner("data-platform")
                .stream(STREAM).logic("tag-engine", "1.0.0").instances(3)
                .contractRef("ct-tag-001").startPolicy("latest")
                .incrementalEngine("external");
        kernel.create(task);

        check("外部引擎适配器被调用", gotTaskId.get(), "tag-task-1");
        check("topic 正确", gotTopic.get(), TOPIC);
        check("stream 正确", gotStream.get(), STREAM);
        check("三个分区起点都算出", gotStarts.get() == null ? 0 : gotStarts.get().size(), 3);
        check("单元下发**未被**调用(不规划 v2 单元)", unitDispatcher.count(), 0);
        check("任务推进到 RUNNING", tasks.byId("tag-task-1").status(), TaskStatus.RUNNING);

        // 对照:默认 unit 引擎,仍走单元下发
        RecordingDispatcher unit2 = new RecordingDispatcher();
        TaskScheduler sched2 = new TaskScheduler(tasks, units, unit2, sos, leases,
                new WatermarkLatestTsoProvider(watermarks), batch, external, () -> 50_000L, 30_000L);
        TaskRecord t2 = new TaskRecord("tag-task-2").name("打标2").consumerType("tag").owner("x")
                .stream(STREAM).logic("tag-engine", "1.0.0").instances(3)
                .contractRef("ct-tag-001").startPolicy("latest");
        TaskRecord created2 = new TaskKernelService(streams, logics, contracts, tasks, units,
                new UnitPlanner(), sched2).create(t2);
        // 注:create 会调 onCreated → handover(unit 路径)。但由于 task-2 的 units 是新建的,应下发 3 个单元
        check("默认 unit 引擎 → 单元下发 3 个", unit2.count(), 3);

        System.out.println("==================================================");
        System.out.println("  通过 " + passed + " 项, 失败 " + failed + " 项");
        System.out.println("==================================================");
        if (failed > 0) System.exit(1);
    }

    private static void check(String what, Object actual, Object expected) {
        boolean ok = String.valueOf(actual).equals(String.valueOf(expected));
        if (ok) passed++; else failed++;
        System.out.println((ok ? "  PASS  " : "  FAIL  ") + what
                + (ok ? "" : "   (实际 " + actual + ",期望 " + expected + ")"));
    }
}
