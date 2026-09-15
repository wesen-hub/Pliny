package com.dcp.outlet.adapters;

/**
 * 批阶段执行体选择策略的离线自检(设计 §4.3.1)。
 */
public final class BatchExecutorPolicyCheck {

    private static int passed, failed;

    public static void main(String[] args) {
        System.out.println("=== 批阶段执行体选择策略(离线) ===");

        // 开关优先级高于行数
        check("钉 local(哪怕 1000 万)", BatchExecutorPolicy.pick("local", 10_000_000L),
                BatchExecutorPolicy.Exec.LOCAL);
        check("钉 container(哪怕 1000 万)", BatchExecutorPolicy.pick("container", 10_000_000L),
                BatchExecutorPolicy.Exec.CONTAINER);
        check("钉 spark(哪怕 0 行)", BatchExecutorPolicy.pick("spark", 0L),
                BatchExecutorPolicy.Exec.SPARK);

        // auto:按行数
        check("auto · <100万 → 批容器", BatchExecutorPolicy.pick("auto", 999_999L),
                BatchExecutorPolicy.Exec.CONTAINER);
        check("auto · 正好 100万 → Spark", BatchExecutorPolicy.pick("auto", 1_000_000L),
                BatchExecutorPolicy.Exec.SPARK);
        check("auto · >100万 → Spark", BatchExecutorPolicy.pick("auto", 5_000_000L),
                BatchExecutorPolicy.Exec.SPARK);
        // 行数未知(-1)保守取批容器
        check("auto · 行数未知(-1) → 批容器", BatchExecutorPolicy.pick("auto", -1L),
                BatchExecutorPolicy.Exec.CONTAINER);
        check("auto · 行数未知(0) → 批容器", BatchExecutorPolicy.pick("auto", 0L),
                BatchExecutorPolicy.Exec.CONTAINER);
        // 空开关 = auto
        check("空开关 → auto", BatchExecutorPolicy.pick(null, 5_000_000L),
                BatchExecutorPolicy.Exec.SPARK);
        check("空开关 → auto(<100万)", BatchExecutorPolicy.pick("", 5L),
                BatchExecutorPolicy.Exec.CONTAINER);

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
