package com.dcp.outlet.control;

import com.dcp.outlet.sdk.*;
import com.dcp.outlet.sdk.testkit.FakeRecord;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/** 批阶段自检:切片策略 / T0 三规则 / 同一 logic 两种 mode / 片内重试 / 全片成功才算成功。 */
public final class BatchPhaseSelfCheck {

    private static int passed, failed;
    private static final String STREAM = "rt_sync_c";
    private static final String TOPIC = "dcp.cdc.target_db.rt_sync_c";

    /** 内存快照源:按切片发数据;可指定某片前 N 次读抛异常;记录收到的身份(验证批/增一致性透传)。 */
    private static final class MemSnapshot implements SnapshotSource {
        final long rows;
        final Map<Integer, Integer> failSliceTimes = new HashMap<>();
        final List<String> seenConsumerIds = new ArrayList<>();

        MemSnapshot(long rows) { this.rows = rows; }

        @Override public long estimateRows(String consumerId, String stream, long tso) {
            if (consumerId == null || consumerId.isBlank()) {
                throw new IllegalStateException("身份缺失(fail-closed)");
            }
            seenConsumerIds.add(consumerId);
            return rows;
        }

        @Override public List<ChangeRecord> readSlice(String consumerId, String stream, long tso, Slice slice) {
            if (consumerId == null || consumerId.isBlank()) {
                throw new IllegalStateException("身份缺失(fail-closed)");
            }
            seenConsumerIds.add(consumerId);
            Integer left = failSliceTimes.get(slice.index());
            if (left != null && left > 0) {
                failSliceTimes.put(slice.index(), left - 1);
                throw new IllegalStateException("scripted slice failure #" + slice.index());
            }
            if (rows == 0) return List.of();
            long per = Math.max(1, rows / Math.max(1, slice.total()));
            List<ChangeRecord> out = new ArrayList<>();
            for (long i = 0; i < per; i++) {
                long id = slice.index() * 1_000_000 + i;
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("id", id); row.put("v", "r" + id);
                out.add(FakeRecord.of(Op.SNAPSHOT, tso).key("id", id).after(row));
            }
            return out;
        }
    }

    /** 记录被调用情况的逻辑。 */
    private static final class SpyLogic implements ConsumerLogic {
        final List<Mode> modes = new ArrayList<>();
        final List<String> streams = new ArrayList<>();
        int init, close, rows;
        final Set<Integer> slicesSeen = new LinkedHashSet<>();
        boolean failOnSlice = false;

        @Override public void init(LogicInitContext ctx) { init++; }
        @Override public void process(Batch b, ConsumerContext ctx) {
            modes.add(b.mode());
            streams.add(b.stream());
            rows += b.size();
            slicesSeen.add(((SnapshotBatch) b).sliceIndex());
        }
        @Override public void close() { close++; }
    }

    public static void main(String[] args) {
        testPlanner();
        testT0Rules();
        testSmallBatchSingleSlice();
        testMediumBatchByPartition();
        testLargeBatchRangeSlices();
        testSliceRetryThenSuccess();
        testSliceFailureFailsBatch();
        testEmptySnapshot();
        testBlankIdentityFailClosed();

        System.out.println();
        System.out.println("==================================================");
        System.out.println("  通过 " + passed + " 项, 失败 " + failed + " 项");
        System.out.println("==================================================");
        if (failed > 0) System.exit(1);
    }

    private static void testPlanner() {
        SlotPlanner p = SlotPlanner.defaults();
        check("切片:<10 万 → 单片", p.plan(STREAM, 99_999, 3).size(), 1);
        check("切片:单片类型", p.plan(STREAM, 99_999, 3).get(0).kind(), Slice.Kind.WHOLE);
        check("切片:10 万 → 按分区", p.plan(STREAM, 100_000, 3).size(), 3);
        check("切片:按分区类型", p.plan(STREAM, 100_000, 3).get(0).kind(), Slice.Kind.PARTITION);
        check("切片:100 万 → 区间×分区", p.plan(STREAM, 1_000_000, 3).get(0).kind(), Slice.Kind.RANGE);
        check("切片:600 万/3 分区 → 6 片", p.plan(STREAM, 6_000_000, 3).size(), 6);
        check("切片:行数未知 → 按分区", p.plan(STREAM, -1, 3).size(), 3);
    }

    private static void testT0Rules() {
        T0Selector sel = new T0Selector(topic -> 1000L, () -> 2000L, (t, p) -> 9000L);
        check("R3:请求未来 → 压到 latest", sel.select(TOPIC, 0, 50_000L).tso(), 9000L);
        check("R3 标记", sel.select(TOPIC, 0, 50_000L).clampedByLatest(), true);
        check("正常:请求值在区间内", sel.select(TOPIC, 0, 5000L).tso(), 5000L);
        check("R1:低于 changefeed 起点 → 被抬高", sel.select(TOPIC, 0, 500L).tso(), 2000L);
        check("R1/R2 标记", sel.select(TOPIC, 0, 500L).bumpedByStartTs() || sel.select(TOPIC, 0, 500L).bumpedByGcSafePoint(), true);
        check("R2:低于 GC → 提到 GC", new T0Selector(t -> 100L, () -> 7000L, (t, p) -> 9000L)
                .select(TOPIC, 0, 5000L).tso(), 7000L);
        check("未请求 → 用 latest", sel.select(TOPIC, 0, null).tso(), 9000L);
        check("决策明细含三个值", sel.select(TOPIC, 0, 5000L).detail().contains("latest="), true);
    }

    private static BatchPhaseRunner.Outcome run(long rows, int partitions, SpyLogic spy,
                                                Map<Integer, Integer> failTimes) {
        return run(rows, partitions, spy, failTimes, "c-es-001");
    }

    private static BatchPhaseRunner.Outcome run(long rows, int partitions, SpyLogic spy,
                                                Map<Integer, Integer> failTimes, String consumerId) {
        MemSnapshot src = new MemSnapshot(rows);
        if (failTimes != null) src.failSliceTimes.putAll(failTimes);
        BatchPhaseRunner runner = new BatchPhaseRunner(src, SlotPlanner.defaults(), 1, 1L);
        BatchPhaseRunner.Outcome r = runner.run("t1", consumerId, "task-t1", STREAM, TOPIC, 5000L, partitions,
                "es-sync", "1.0.0", spy, Map.of("idField", "id"), Silent.LOG);
        check("身份透传:快照源收到任务的契约身份",
                src.seenConsumerIds.stream().allMatch(consumerId::equals), true);
        return r;
    }

    private static void testSmallBatchSingleSlice() {
        SpyLogic spy = new SpyLogic();
        BatchPhaseRunner.Outcome r = run(1_000, 3, spy, null);
        check("小批:成功", r.ok, true);
        check("小批:片数=1", r.slices, 1);
        check("小批:一次 process", spy.modes.size(), 1);
        check("小批:mode=SNAPSHOT", spy.modes.get(0), Mode.SNAPSHOT);
        check("小批:stream 传递正确", spy.streams.get(0), STREAM);
        check("小批:init 一次 / close 一次", spy.init + "/" + spy.close, "1/1");
        check("小批:有行落库", r.rows > 0, true);
    }

    private static void testMediumBatchByPartition() {
        SpyLogic spy = new SpyLogic();
        BatchPhaseRunner.Outcome r = run(500_000, 3, spy, null);
        check("中批:片数=分区数", r.slices, 3);
        check("中批:三次 process", spy.modes.size(), 3);
        check("中批:覆盖 3 个切片", spy.slicesSeen.size(), 3);
    }

    private static void testLargeBatchRangeSlices() {
        SpyLogic spy = new SpyLogic();
        BatchPhaseRunner.Outcome r = run(6_000_000, 3, spy, null);
        check("大批:片数=6", r.slices, 6);
        check("大批:每片都处理", spy.modes.size(), 6);
        check("大批:全片成功才 ok", r.ok, true);
    }

    private static void testSliceRetryThenSuccess() {
        SpyLogic spy = new SpyLogic();
        Map<Integer, Integer> fails = new HashMap<>();
        fails.put(1, 1);                                  // 第 1 片第 1 次失败,重试成功
        BatchPhaseRunner.Outcome r = run(500_000, 3, spy, fails);
        check("片重试后成功", r.ok, true);
        check("片重试:仍处理 3 片", spy.modes.size(), 3);
        check("片重试:无 failedSlice", r.failedSlice, -1);
    }

    private static void testSliceFailureFailsBatch() {
        SpyLogic spy = new SpyLogic();
        Map<Integer, Integer> fails = new HashMap<>();
        fails.put(2, 99);                                 // 第 2 片永远失败
        BatchPhaseRunner.Outcome r = run(500_000, 3, spy, fails);
        check("片失败 → 整批失败(调用方重做批)", r.ok, false);
        check("片失败:记录失败片号", r.failedSlice, 2);
        check("片失败:错误信息可解释", r.error != null && r.error.contains("slice"), true);
        check("片失败:仍释放逻辑资源(close)", spy.close, 1);
    }

    private static void testBlankIdentityFailClosed() {
        MemSnapshot src = new MemSnapshot(100);
        BatchPhaseRunner runner = new BatchPhaseRunner(src, SlotPlanner.defaults(), 0, 0L);
        try {
            runner.run("t1", "  ", "task-t1", STREAM, TOPIC, 5000L, 3, "es-sync", "1.0.0",
                    new SpyLogic(), Map.of(), Silent.LOG);
            check("空身份 → 拒绝(fail-closed)", "no-throw", "IllegalStateException");
        } catch (IllegalStateException e) {
            check("空身份 → 拒绝(fail-closed)", "IllegalStateException", "IllegalStateException");
        }
    }

    private static void testEmptySnapshot() {
        SpyLogic spy = new SpyLogic();
        BatchPhaseRunner.Outcome r = run(0, 3, spy, null);
        check("空快照:成功且 0 行", r.ok && r.rows == 0, true);
        check("空快照:不打扰逻辑(0 次 process)", spy.modes.size(), 0);
        check("空快照:init/close 仍各一次", spy.init + "/" + spy.close, "1/1");
    }

    private static final class Silent {
        static final SdkLogger LOG = new SdkLogger() {
            public boolean isDebugEnabled() { return false; }
            public void debug(String f, Object... a) { }
            public void info(String f, Object... a) { }
            public void warn(String f, Object... a) { }
            public void error(String m, Throwable t) { }
            public void error(String f, Object... a) { }
        };
    }

    private static void check(String what, Object actual, Object expected) {
        boolean ok = String.valueOf(actual).equals(String.valueOf(expected));
        if (ok) passed++; else failed++;
        System.out.println((ok ? "  PASS  " : "  FAIL  ") + what + " = " + actual
                + (ok ? "" : "   (期望 " + expected + ")"));
    }
}
