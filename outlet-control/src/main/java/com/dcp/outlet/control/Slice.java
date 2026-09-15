package com.dcp.outlet.control;

/** 批阶段切片(按数据量决定粒度,见 D18)。 */
public final class Slice {

    public enum Kind {
        /** 单片:数据量小,一次读完 */
        WHOLE,
        /** 按分区切:每分区一片 */
        PARTITION,
        /** 区间 × 分区:数据量大,每分区再按主键区间细分 */
        RANGE
    }

    private final String stream;
    private final Kind kind;
    private final int partition;
    private final long loKey;
    private final long hiKey;
    private final int index;
    private final int total;

    public Slice(String stream, Kind kind, int partition, long loKey, long hiKey, int index, int total) {
        this.stream = stream; this.kind = kind; this.partition = partition;
        this.loKey = loKey; this.hiKey = hiKey; this.index = index; this.total = total;
    }

    public String stream() { return stream; }
    public Kind kind() { return kind; }
    public int partition() { return partition; }
    public long loKey() { return loKey; }
    public long hiKey() { return hiKey; }
    public int index() { return index; }
    public int total() { return total; }

    @Override public String toString() {
        return kind + "[p" + partition + " " + (kind == Kind.RANGE ? loKey + ".." + hiKey : "全量")
                + " " + (index + 1) + "/" + total + "]";
    }
}
