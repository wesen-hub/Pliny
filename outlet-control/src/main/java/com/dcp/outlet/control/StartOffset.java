package com.dcp.outlet.control;

/** 起点计算结果:某分区应当从哪个 offset 开始消费,以及它是怎么算出来的(可观测)。 */
public final class StartOffset {

    /** 起点来源:便于排障与告警区分"快路径"与"慢路径"。 */
    public enum Source {
        /** 命中锚点 + 短扫(正常路径,成本 O(log n) + 短扫) */
        ANCHOR,
        /** 无锚点,用 Kafka 原生时间索引粗定位 + 短扫(冷启动) */
        TIME_INDEX,
        /** 连时间索引也不可用,从分区最早位置回扫(兜底) */
        RESCAN_FROM_EARLIEST,
        /** 索引被判定不可信(单调性回退),强制回扫 */
        RESCAN_DEGRADED,
        /** 已扫到分区末尾(当前没有需要消费的事件) */
        TAIL
    }

    private final int partition;
    private final long offset;
    private final Source source;
    private final int scannedRows;

    public StartOffset(int partition, long offset, Source source, int scannedRows) {
        this.partition = partition;
        this.offset = offset;
        this.source = source;
        this.scannedRows = scannedRows;
    }

    public int partition() { return partition; }

    public long offset() { return offset; }

    public Source source() { return source; }

    /** 为了确定这个起点,额外扫了多少条(锚点命中时通常 ≤ 采样间隔)。 */
    public int scannedRows() { return scannedRows; }

    @Override public String toString() {
        return "p" + partition + " → offset=" + offset + " via=" + source + " scanned=" + scannedRows;
    }
}
