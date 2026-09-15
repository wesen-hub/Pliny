package com.dcp.outlet.sdk;

/**
 * 消费模式(D17/D26)。同一份逻辑代码,批阶段与增量阶段共用。
 *
 * <ul>
 *   <li>{@link #SNAPSHOT} —— 批阶段:在 T0 视图上读全量,用于"打底"</li>
 *   <li>{@link #INCREMENTAL} —— 增量阶段:消费变更事件流</li>
 * </ul>
 */
public enum Mode {
    SNAPSHOT,
    INCREMENTAL
}
