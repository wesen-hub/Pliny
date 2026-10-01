package com.dcp.scheduler.model;

/**
 * 再平衡规则(调度算法子文档 §七):ns 级可配,运维后台写 /dcp/{ns}/rebalance-rules。
 * 缺省与设计一致:占用率 0.8 · 差 0.3 · 持续 3 周期 · cpu>80 驱逐 · 冷却 10min。
 */
public class RebalanceRules {

    public double occupancyHi = 0.8;     // 最忙槽位占用率上界
    public double gap = 0.3;             // 最忙最闲占用率差
    public int periods = 3;              // 失衡/cpu 超载持续周期
    public int cpuHi = 80;               // 进程 cpu 驱逐阈值(%,持续 periods 周期)
    public int cooldownMinutes = 10;     // 全局冷却(失衡解除后起算)
    public int partCoolMinutes = 10;     // 分区级冷却(被迁移分区不再迁)
    public int targetCoolMinutes = 10;   // 目标实例冷却(刚接过的实例不迁出)
}
