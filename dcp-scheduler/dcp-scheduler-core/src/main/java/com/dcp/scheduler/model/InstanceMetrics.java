package com.dcp.scheduler.model;

/**
 * 实例级派生指标(监控子文档 §二:Σ 该实例所有分区 + 机器指标;主侧每决策轮写入,后台直读)。
 */
public class InstanceMetrics {

    public long ts;
    public String instanceId;
    public String addr;
    public int parts;               // 该实例承接的分区数
    public double rps;              // Σ 分区 rps
    public long processedCount;     // Σ 累计处理条数
    public long errorCount;         // Σ 累计错误条数
    public double errorRate;        // Δerror / Δprocessed(最近帧)
    public double avgLatencyMs;     // ΔtotalLatency / Δprocessed(最近帧)
    public int cpu;                 // 机器指标(实例自报)
    public int mem;
    public int load;
    public int capacity;            // 槽位上限(实例 ns 归属声明)
    public double occupancy;        // 占用率 = parts / capacity
}
