package com.dcp.scheduler.model;

/**
 * ns 级大盘指标(监控子文档 §二:任务/实例/分区计数 + 总 rps/lag/容量水位;主侧每决策轮写入,后台直读)。
 */
public class NsMetrics {

    public long ts;
    public String ns;
    public int tasks;           // 任务数(spec 存在即计)
    public int instances;       // 存活实例数
    public int parts;           // 分区归属记录总数
    public int assignedParts;   // 有归属(非 PENDING)分区数
    public double rps;          // Σ 分区 rps(相邻帧差分)
    public long processedCount; // Σ 累计处理条数
    public long errorCount;     // Σ 累计错误条数
    public long lag;            // Σ 分区 lag(无 Kafka 源的分区记 0)
    public int totalCapacity;   // Σ 实例槽位
    public double occupancy;    // assignedParts / totalCapacity(0 容量 = 0)
}
