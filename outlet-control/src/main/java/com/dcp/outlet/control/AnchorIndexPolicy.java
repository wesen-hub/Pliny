package com.dcp.outlet.control;

/** 锚点采样策略(平台策略,任务不可配 —— D18 同源)。 */
public final class AnchorIndexPolicy {

    private final int sampleEveryRows;
    private final long sampleEveryMillis;
    private final long retentionMillis;
    private final int readBatchRows;

    public AnchorIndexPolicy(int sampleEveryRows, long sampleEveryMillis,
                             long retentionMillis, int readBatchRows) {
        this.sampleEveryRows = sampleEveryRows;
        this.sampleEveryMillis = sampleEveryMillis;
        this.retentionMillis = retentionMillis;
        this.readBatchRows = readBatchRows;
    }

    /** 默认:每 5000 条或 10s 采一条;保留 7 天;每次读 500 条。 */
    public static AnchorIndexPolicy defaults() {
        return new AnchorIndexPolicy(5000, 10_000L, 7L * 24 * 3600 * 1000, 500);
    }

    public int sampleEveryRows() { return sampleEveryRows; }

    public long sampleEveryMillis() { return sampleEveryMillis; }

    public long retentionMillis() { return retentionMillis; }

    public int readBatchRows() { return readBatchRows; }

    /** 判断是否该采样(行数或时间任一到达)。 */
    public boolean shouldSample(int rowsSinceLastSample, long millisSinceLastSample) {
        return rowsSinceLastSample >= sampleEveryRows
                || millisSinceLastSample >= sampleEveryMillis;
    }
}
