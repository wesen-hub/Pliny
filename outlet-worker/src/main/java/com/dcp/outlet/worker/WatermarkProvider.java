package com.dcp.outlet.worker;

/** 水位提供者:支撑 {@code ConsumerContext.safeWatermark()}("≤ 此 TSO 的事件已全部到齐")。 */
public interface WatermarkProvider {

    void update(int partition, long watermarkTs);

    long safeWatermark(int partition);
}
