package com.dcp.outlet.control;

/**
 * 离线自检:R2 GC 安全余量的单位换算(第三期审查 高危-1 修复)。
 *
 * <p>修复前 {@code gcFloor = gc + gcSafeMarginMs} 把毫秒当 TSO 直接相加,
 * 实际只留了 {@code 120000 / 2^18 ≈ 0.46ms} 余量;修复后须按 TSO 换算。
 */
public final class T0GcMarginSelfCheck {

    private static int passed, failed;

    public static void main(String[] args) {
        System.out.println("=== T0 GC 余量单位换算(高危-1 修复,离线) ===");

        // 基准: gc=物理时刻 T(ms)<<18, latest 取 T+10min 之后的 TSO, 保证不触发 R3
        long gcMs = 1_700_000_000_000L;
        long gc = gcMs << T0Selector.MS_TO_TSO_SHIFT;
        long latest = (gcMs + 600_000L) << T0Selector.MS_TO_TSO_SHIFT;

        // 用例 1: margin=120000ms → gcFloor 应精确等于 (gcMs+120000)<<18
        checkMargin(gc, latest, 120_000L, (gcMs + 120_000L) << T0Selector.MS_TO_TSO_SHIFT);

        // 用例 2: margin=0 → gcFloor == gc(t0==gc 不抬; t0=gc-1 抬到 gc 并标记)
        T0Selector sel0 = new T0Selector(t -> 0L, () -> gc, (t, p) -> latest, 0L);
        check("margin=0 → t0=gc 保持不动", sel0.select("t", 0, gc).tso() == gc
                && !sel0.select("t", 0, gc).bumpedByGcSafePoint());
        T0Selector.Decision d0 = sel0.select("t", 0, gc - 1);
        check("margin=0 → t0<gc 抬到 gcFloor 并标记", d0.tso() == gc && d0.bumpedByGcSafePoint());

        // 用例 3: margin=2000ms → gcFloor == (gcMs+2000)<<18
        checkMargin(gc, latest, 2_000L, (gcMs + 2_000L) << T0Selector.MS_TO_TSO_SHIFT);

        // 用例 4: 请求 T0 落在 (gc, gcFloor) 区间 → 必须被抬到 gcFloor 并标记 byGc
        T0Selector sel = new T0Selector(t -> 0L, () -> gc, (t, p) -> latest, 120_000L);
        long inWindow = (gcMs + 60_000L) << T0Selector.MS_TO_TSO_SHIFT;
        T0Selector.Decision d = sel.select("t", 0, inWindow);
        check("请求 T0 在余量窗口内 → 抬到 gcFloor", d.tso() == (gcMs + 120_000L) << T0Selector.MS_TO_TSO_SHIFT);
        check("byGc 标记", d.bumpedByGcSafePoint());
        check("不触发 R3", !d.clampedByLatest());

        // 用例 5: 请求 T0 高于 gcFloor → 不抬
        long above = (gcMs + 300_000L) << T0Selector.MS_TO_TSO_SHIFT;
        d = sel.select("t", 0, above);
        check("请求 T0 高于 gcFloor → 保持", d.tso() == above && !d.bumpedByGcSafePoint());

        System.out.println("passed=" + passed + " failed=" + failed);
        if (failed > 0) {
            throw new AssertionError(failed + " 项断言失败");
        }
    }

    private static void checkMargin(long gc, long latest, long marginMs, long expectFloor) {
        T0Selector sel = new T0Selector(t -> 0L, () -> gc, (t, p) -> latest, marginMs);
        // 请求 T0 = gc(低于 floor)→ 必被抬到 gcFloor
        T0Selector.Decision d = sel.select("t", 0, gc);
        check("margin=" + marginMs + "ms → gcFloor 精确换算", d.tso() == expectFloor);
        check("margin=" + marginMs + "ms → byGc 标记", d.bumpedByGcSafePoint());
    }

    private static void check(String name, boolean ok) {
        System.out.println((ok ? "[PASS] " : "[FAIL] ") + name);
        if (ok) { passed++; } else { failed++; }
    }
}
