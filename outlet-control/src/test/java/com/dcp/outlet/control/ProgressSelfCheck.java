package com.dcp.outlet.control;

import com.dcp.outlet.control.support.*;
import java.util.List;

/** 进度口径自检:滞后秒数/积压条数/缺单元/MISSING 状态。 */
public final class ProgressSelfCheck {

    private static final String TOPIC = "dcp.cdc.target_db.rt_sync_c";
    private static final long TSO_BASE = 4_691_000_000_000_000_00L;
    private static int passed, failed;

    public static void main(String[] args) {
        InMemoryWatermarkStore wm = new InMemoryWatermarkStore();
        ArrayPartitionReader reader = new ArrayPartitionReader();
        reader.putMonotonic(TOPIC, 0, 1000, TSO_BASE, 1L << 18, 1);      // 每行 1ms
        ProgressService svc = new ProgressService(reader, wm, () -> 0L);

        // 水位 = 已处理 TSO + 5ms → 滞后 5s?不:TSO 差 >>18 = 毫秒
        long processed = reader.readFrom(TOPIC, 0, 900, 1).get(0).commitTs();
        wm.upsert(new Watermark(TOPIC, 0, 1, processed + (5L << 18), 999, 0));   // 差 5ms
        PartitionProgress p1 = svc.progressOfUnit("u1", TOPIC, 0, 1, "RUNNING", 900, processed, 123L);
        check("滞后毫秒→秒(5ms 会四舍五入为 0s)", p1.lagSeconds(), 0L);

        wm.upsert(new Watermark(TOPIC, 0, 1, processed + (3_000L << 18), 999, 0)); // 差 3s
        PartitionProgress p2 = svc.progressOfUnit("u1", TOPIC, 0, 1, "RUNNING", 900, processed, 123L);
        check("滞后 3 秒", p2.lagSeconds(), 3L);
        check("水位透传", p2.watermarkTs() > 0, true);

        PartitionProgress p3 = svc.fullProgressOfUnit("u1", TOPIC, 0, 1, "RUNNING", 900, processed, 123L);
        check("积压条数 = 末端 - 已提交", p3.lagRows(), 100L);

        // 任务级展开:一个单元持 2 个分区
        InMemoryUnitStore units = new InMemoryUnitStore();
        units.insert("t1", List.of(new PlannedUnit("u1", "rt_sync_c", TOPIC, List.of(0, 1))));
        List<UnitStatusStore.UnitStatus> st = List.of(
                new UnitStatusStore.UnitStatus("u1", "t1", "RUNNING", 900, processed, 123L, 0L));
        List<PartitionProgress> all = svc.progressOfTask("t1", units.byTask("t1"), st, 1, false);
        check("按分区展开为 2 行", all.size(), 2);

        List<PartitionProgress> missing = svc.progressOfTask("t1", units.byTask("t1"),
                List.of(), 1, false);
        check("无状态上报 → MISSING", missing.get(0).state(), "MISSING");
        check("MISSING 不抛异常", missing.size(), 2);

        System.out.println();
        System.out.println("==================================================");
        System.out.println("  通过 " + passed + " 项, 失败 " + failed + " 项");
        System.out.println("==================================================");
        if (failed > 0) System.exit(1);
    }

    private static void check(String what, Object actual, Object expected) {
        boolean ok = String.valueOf(actual).equals(String.valueOf(expected));
        if (ok) passed++; else failed++;
        System.out.println((ok ? "  PASS  " : "  FAIL  ") + what + " = " + actual
                + (ok ? "" : "   (期望 " + expected + ")"));
    }
}
