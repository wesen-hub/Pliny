package com.dcp.outlet.control;

/** 分区内一条消息的轻量样本(只解析 offset 与 commitTs,不读业务数据)。 */
public final class TsoSample {

    private final long offset;
    private final long commitTs;

    public TsoSample(long offset, long commitTs) {
        this.offset = offset;
        this.commitTs = commitTs;
    }

    public long offset() { return offset; }

    public long commitTs() { return commitTs; }

    @Override public String toString() { return "(" + offset + "," + commitTs + ")"; }
}
