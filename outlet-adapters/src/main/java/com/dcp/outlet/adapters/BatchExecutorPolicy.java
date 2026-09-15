package com.dcp.outlet.adapters;

/**
 * 批阶段执行体选择策略(设计 §4.3.1)。
 *
 * <p>`auto` 下按估算行数选:≥ 100 万 → Spark;否则 → 批容器。开关 `outlet.batch.executor`
 * 可钉死 `local | container | spark`(优先级高于行数判定)。
 *
 * <p>注:10 万阈值只用于**切片档位**(单片/按分区,见 {@code SlotPlanner}),与本策略无关;
 * 本策略只回答"批容器 vs Spark"。
 */
public final class BatchExecutorPolicy {

    public enum Exec { LOCAL, CONTAINER, SPARK }

    /** ≥ 此值走 Spark(设计 D6:100 万)。 */
    public static final long SPARK_THRESHOLD = 1_000_000L;

    private BatchExecutorPolicy() {
    }

    /** 行数未知(estimateRows 返回 -1)时保守取批容器(不猜)。 */
    public static Exec pick(String mode, long estimateRows) {
        if (mode != null && !mode.isBlank()) {
            switch (mode.trim().toLowerCase()) {
                case "local": return Exec.LOCAL;
                case "container": return Exec.CONTAINER;
                case "spark": return Exec.SPARK;
                default: break;   // auto / 未知 → 按行数
            }
        }
        return estimateRows >= SPARK_THRESHOLD ? Exec.SPARK : Exec.CONTAINER;
    }
}
