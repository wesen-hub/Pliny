package com.dcp.indexer.core;

/** 水位存储端口(生产实现 = {@code chg_watermark_latest} 表)。 */
public interface WatermarkStore {

    /** 单调写入:新水位 ≤ 旧水位时忽略(防乱序)。 */
    void upsert(Watermark watermark);

    /** 最新水位;无则 {@code null}。 */
    Watermark latest(String topic, int partition, int epoch);
}
