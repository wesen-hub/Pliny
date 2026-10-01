package com.dcp.outlet.control;

/** 起点计算结果:某分区应当从哪个 offset 开始消费(过滤起点), 以及它是怎么算出来的(可观测)。 */
public final class StartOffset {

    /** 起点来源:便于排障与告警区分"命中栅栏"与"退化回扫"。 */
    public enum Source {
        /** 命中栅栏点(正常路径, 纯索引查询 O(log n); 精确边界由出口持续过滤承接) */
        FENCE,
        /** 无栅栏(冷启动/被污染/被删) → 分区最早位置回扫 + 出口过滤兜底(永不失败) */
        FALLBACK_EARLIEST
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

    /** 短扫已退役(栅栏方案):恒 0, 字段保留兼容。 */
    public int scannedRows() { return scannedRows; }

    @Override public String toString() {
        return "p" + partition + " → offset=" + offset + " via=" + source + " scanned=" + scannedRows;
    }
}
