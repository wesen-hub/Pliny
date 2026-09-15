package com.dcp.outlet.control.support;

import com.dcp.outlet.control.LatestTsoProvider;
import com.dcp.outlet.control.Watermark;

/** 用内存水位表充当"最新 TSO"来源(startPolicy=latest 用)。 */
public class WatermarkLatestTsoProvider implements LatestTsoProvider {

    private final InMemoryWatermarkStore store;

    public WatermarkLatestTsoProvider(InMemoryWatermarkStore store) { this.store = store; }

    @Override public long latestTso(String topic, int partition, int epoch) {
        Watermark w = store.latest(topic, partition, epoch);
        return w == null ? 0L : w.watermarkTs();
    }
}
