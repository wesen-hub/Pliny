package com.dcp.outlet.control;

import com.dcp.outlet.control.support.*;

import java.util.*;

/**
 * 控制面编排自检(纯逻辑,内存端口):单元规划 / 租约 / 建任务校验 / 批流交接。
 */
public final class OrchestrationSelfCheck {

    private static final String TOPIC = "dcp.cdc.target_db.rt_sync_c";
    private static final String STREAM = "rt_sync_c";
    private static int passed, failed;

    public static void main(String[] args) {
        testPlanner();
        testLease();
        testKernelValidation();
        testSnapshotFlow();
        testBatchFailure();
        testLatestPolicy();
        testHandoverConflict();

        System.out.println();
        System.out.println("==================================================");
        System.out.println("  通过 " + passed + " 项, 失败 " + failed + " 项");
        System.out.println("==================================================");
        if (failed > 0) System.exit(1);
    }

    // ================= 单元规划 =================
    private static void testPlanner() {
        UnitPlanner planner = new UnitPlanner();
        StreamInfo s3 = new StreamInfo(STREAM, TOPIC, 3);

        List<PlannedUnit> n3 = planner.plan("t", List.of(s3), 3, 1);
        check("P1 N=Σ分区 → 单元数", n3.size(), 3);
        check("P1 每单元 1 分区", n3.stream().allMatch(u -> u.partitions().size() == 1), true);

        List<PlannedUnit> n1 = planner.plan("t", List.of(s3), 1, 1);
        check("P2 N=1 → 单单元托管全部分区", n1.size(), 1);
        check("P2 该单元分区集合", n1.get(0).partitions(), List.of(0, 1, 2));

        List<PlannedUnit> n2 = planner.plan("t", List.of(s3), 2, 1);
        check("P3 N=2 → 单元数", n2.size(), 2);
        check("P3 轮询均分(2+1)", n2.get(0).partitions().size() + n2.get(1).partitions().size(), 3);
        check("P3 无分区丢失", new TreeSet<>(concat(n2)), new TreeSet<>(List.of(0, 1, 2)));

        boolean threw = false;
        try {
            planner.plan("t", List.of(s3), 4, 1);
        } catch (PartitionLimitExceededException e) { threw = true; }
        check("P4 N>Σ分区 → 拒绝", threw, true);

        StreamInfo s2 = new StreamInfo("rt_sync_d", "dcp.cdc.target_db.rt_sync_d", 2);
        List<PlannedUnit> multi = planner.plan("t", List.of(s3, s2), 5, 1);
        check("P5 多流 N=5 → 单元数", multi.size(), 5);
        check("P5 单元不跨流", multi.stream().map(PlannedUnit::stream).distinct().count(), 2L);

        Set<String> ids = new LinkedHashSet<>();
        multi.forEach(u -> ids.add(u.unitId()));
        check("P6 单元 ID 唯一", ids.size(), multi.size());
        check("P6 单元 ID 含代次", multi.get(0).unitId().endsWith("-1"), true);
    }

    // ================= 租约 =================
    private static void testLease() {
        LeaseManager leases = new LeaseManager();
        leases.grant("u1", "t", TOPIC, 0, 1, 1000, 0);
        check("L1 授予后可查", leases.leaseOf(TOPIC, 0, 1) != null, true);

        boolean conflict = false;
        try {
            leases.grant("u2", "t", TOPIC, 0, 1, 1000, 10);
        } catch (LeaseConflictException e) { conflict = true; }
        check("L2 同分区同代另一单元 → LEASE_CONFLICT", conflict, true);

        leases.grant("u1", "t", TOPIC, 0, 1, 5000, 20);          // 同单元续期允许
        check("L3 同单元重复授予(续期)成功", leases.leaseOf(TOPIC, 0, 1).expireAt(), 5020L);
        check("L4 续约成功", leases.renew("u1", 1000, 100), true);

        List<LeaseGrant> expired = leases.expireScan(100_000);
        check("L5 过期巡检返回条数", expired.size(), 1);
        check("L5 过期后活跃租约清零", leases.activeCount(), 0);

        leases.grant("u2", "t", TOPIC, 0, 1, 1000, 100_010);
        check("L6 过期后可被新单元接管", leases.leaseOf(TOPIC, 0, 1).unitId(), "u2");
        check("L6 旧单元续约失败(=自停依据)", leases.renew("u1", 1000, 100_020), false);
    }

    // ================= 建任务六项校验 =================
    private static void testKernelValidation() {
        Fixture f = fixture();

        check("K1 流未登记", firstError(f, task("t1").stream("not_registered")), "流未登记:not_registered");
        check("K2 逻辑未注册", firstError(f, task("t2").logic("nope", "9.9.9")), "逻辑未注册:nope@9.9.9");
        check("K3 契约不存在", firstError(f, task("t3").contractRef("ct-missing")), "契约不存在:ct-missing");
        check("K4 instances 超限", firstError(f, task("t4").instances(9)),
                "instances 必须在 1..3(当前 9)");
        // 另一个任务(id 不同)先占用了 "task-t5" 这个消费组
        f.tasks.insert(new TaskRecord("elsewhere").consumerGroup("task-t5"));
        check("K5 消费组已存在", firstError(f, task("t5")), "消费组已存在:task-t5");
        f.tasks.insert(task("t6"));
        check("K6 任务已存在", firstError(f, task("t6")), "任务已存在:t6");

        TaskRecord ok = f.kernel.create(task("tag-eval"));
        check("K7 校验通过后落库", f.tasks.exists("tag-eval"), true);
        check("K7 消费组自动生成", ok.consumerGroup(), "task-tag-eval");
        check("K7 单元已规划", f.units.byTask("tag-eval").size(), 3);
        check("K7 进入批阶段", ok.status(), TaskStatus.BATCH_RUNNING);
        check("K7 批阶段被触发", f.batch.startedCount(), 1);

        // 生产化:disabled / archived 拦新建任务(不影响运行中,已加载 jar 继续跑)
        f.logics.register(LogicInfo.builder("es-sync", "2.0.0").status(LogicInfo.STATUS_DISABLED).build());
        check("K8 disabled 拒绝新建(含 LOGIC_DISABLED)",
                firstError(f, task("t8").logic("es-sync", "2.0.0")),
                "逻辑已禁用或废弃(LOGIC_DISABLED):es-sync@2.0.0 status=disabled");
        f.logics.register(LogicInfo.builder("es-sync", "3.0.0").status(LogicInfo.STATUS_ARCHIVED).build());
        check("K9 archived 拒绝新建",
                firstError(f, task("t9").logic("es-sync", "3.0.0")),
                "逻辑已禁用或废弃(LOGIC_DISABLED):es-sync@3.0.0 status=archived");
    }

    // ================= 批流交接(成功路径)=================
    private static void testSnapshotFlow() {
        Fixture f = fixture();
        TaskRecord t = f.kernel.create(task("tag-eval"));
        f.batch.complete(1_000_500L, true);          // T0 落在数据中间(有区分度)

        check("S1 批完成 → 状态 BATCH_DONE→RUNNING", t.status(), TaskStatus.RUNNING);
        check("S1 startTso 已写", t.startTso(), 1_000_500L);
        check("S1 下发单元数", f.dispatcher.count(), 3);
        check("S1 单元状态 DISPATCHED",
                f.units.stateOf(f.units.byTask("tag-eval").get(0).unitId()), "DISPATCHED");

        // 起点必须等于"第一个 commitTs > 1000"的 offset(与暴力解一致)
        for (DispatchRequest d : f.dispatcher.dispatched()) {
            int p = d.partitions().get(0);
            long want = f.reader.bruteForce(TOPIC, p, 1_000_500L);
            check("S1 p" + p + " 起点与暴力解一致", d.startOffsets().get(p), want);
            check("S1 p" + p + " 已授予租约", f.scheduler.leases().leaseOf(TOPIC, p, 1).unitId(), d.unitId());
        }
        check("S1 下发携带逻辑版本", f.dispatcher.dispatched().get(0).logicVersion(), "1.0.0");
        check("S1 下发携带业务参数", f.dispatcher.dispatched().get(0).params().get("idField"), "id");
    }

    // ================= 批失败:不交接、不猜起点 =================
    private static void testBatchFailure() {
        Fixture f = fixture();
        TaskRecord t = f.kernel.create(task("tag-eval"));
        f.batch.complete(0L, false);

        check("S2 批失败 → BATCH_FAILED", t.status(), TaskStatus.BATCH_FAILED);
        check("S2 未下发任何单元", f.dispatcher.count(), 0);
        check("S2 startTso 未写", t.startTso(), null);
    }

    // ================= startPolicy=latest:直接从最新起 =================
    private static void testLatestPolicy() {
        Fixture f = fixture();
        // 每个分区的水位都取"≥ 该分区最大 commitTs"(生产里水位只前进,必然满足)
        f.watermarks.upsert(new Watermark(TOPIC, 0, 1, 1_004_000L, 1199, 0));   // p0 最大 ≈ 1_003_990
        f.watermarks.upsert(new Watermark(TOPIC, 1, 1, 1_005_000L, 1199, 0));   // p1 最大 ≈ 1_004_990
        f.watermarks.upsert(new Watermark(TOPIC, 2, 1, 1_006_000L, 1199, 0));   // p2 最大 ≈ 1_005_990

        TaskRecord t = task("t-latest").startPolicy("latest");
        f.kernel.create(t);

        check("S3 latest → 不跑批", f.batch.startedCount(), 0);
        check("S3 直接 RUNNING", t.status(), TaskStatus.RUNNING);
        check("S3 下发单元数", f.dispatcher.count(), 3);
        for (DispatchRequest d : f.dispatcher.dispatched()) {
            int p = d.partitions().get(0);
            check("S3 p" + p + " 起点在分区末尾", d.startOffsets().get(p), (long) 1200);
        }
    }

    // ================= 双消费防护:重复交接 → LEASE_CONFLICT =================
    private static void testHandoverConflict() {
        Fixture f = fixture();
        f.kernel.create(task("tag-eval"));
        f.batch.complete(1000L, true);
        boolean conflict = false;
        try {
            // 租约在 50_000 + 30_000 内有效;此处用 60_000 模拟"另一个单元来抢同一分区"
            f.scheduler.leases().grant("u-intruder", "tag-eval", TOPIC, 0, 1, 60_000, 60_000);
        } catch (LeaseConflictException e) {
            conflict = true;
        }
        check("S4 他单元抢占同分区 → LEASE_CONFLICT(防双消费)", conflict, true);
    }

    // ================= 脚手架 =================
    private static final class Fixture {
        final InMemoryStreamRegistry streams = new InMemoryStreamRegistry();
        final InMemoryLogicCatalog logics = new InMemoryLogicCatalog();
        final InMemoryContractLookup contracts = new InMemoryContractLookup();
        final InMemoryTaskStore tasks = new InMemoryTaskStore();
        final InMemoryUnitStore units = new InMemoryUnitStore();
        final RecordingDispatcher dispatcher = new RecordingDispatcher();
        final RecordingBatchRunner batch = new RecordingBatchRunner();
        final InMemoryWatermarkStore watermarks = new InMemoryWatermarkStore();
        final ArrayPartitionReader reader = new ArrayPartitionReader();
        final TaskKernelService kernel;
        final TaskScheduler scheduler;

        Fixture() {
            streams.register(new StreamInfo(STREAM, TOPIC, 3));
            logics.register("es-sync", "1.0.0");
            contracts.put("ct-es-001", true, Set.of(STREAM));

            // 3 分区 × 1200 行;同事务 3 行共享 commitTs
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
            scheduler = new TaskScheduler(tasks, units, dispatcher, sos, leases,
                    new WatermarkLatestTsoProvider(watermarks), batch, () -> 50_000L, 30_000L);
            kernel = new TaskKernelService(streams, logics, contracts, tasks, units, new UnitPlanner(), scheduler);
        }
    }

    private static Fixture fixture() { return new Fixture(); }

    private static TaskRecord task(String id) {
        return new TaskRecord(id)
                .name("rt_sync_c → ES").consumerType("custom").owner("data-platform")
                .stream(STREAM).logic("es-sync", "1.0.0").instances(3)
                .contractRef("ct-es-001").startPolicy("snapshot")
                .params(Map.of("idField", "id", "index", "crm_rt_sync_c"));
    }

    private static String firstError(Fixture f, TaskRecord t) {
        try {
            f.kernel.create(t);
            return "(无错误)";
        } catch (TaskValidationException e) {
            return e.errors().get(0);
        }
    }

    private static List<Integer> concat(List<PlannedUnit> units) {
        List<Integer> out = new ArrayList<>();
        units.forEach(u -> out.addAll(u.partitions()));
        return out;
    }

    private static void check(String what, Object actual, Object expected) {
        boolean ok = String.valueOf(actual).equals(String.valueOf(expected));
        if (ok) passed++; else failed++;
        System.out.println((ok ? "  PASS  " : "  FAIL  ") + what + " = " + actual
                + (ok ? "" : "   (期望 " + expected + ")"));
    }
}
