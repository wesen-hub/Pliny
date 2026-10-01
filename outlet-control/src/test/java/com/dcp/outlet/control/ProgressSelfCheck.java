package com.dcp.outlet.control;

import com.dcp.outlet.control.support.*;
import java.util.List;
import java.util.Map;

/** 进度口径自检:滞后秒数/积压条数/无快照 → SCHEDULED 缺省。 */
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

        // 任务级展开:直接按分区列展开(彻底版无单元层)
        List<PartitionProgress> all = svc.progressOfTask("t1", TOPIC, List.of(0, 1), 1,
                Map.of(0, 900L, 1, 500L), Map.of(0, "RUNNING", 1, "RUNNING"), false);
        check("按分区展开为 2 行", all.size(), 2);

        List<PartitionProgress> missing = svc.progressOfTask("t1", TOPIC, List.of(0, 1), 1,
                Map.of(), Map.of(), false);
        check("无快照上报 → SCHEDULED(框架未分配)", missing.get(0).state(), "SCHEDULED");
        check("SCHEDULED 缺省不抛异常", missing.size(), 2);

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
