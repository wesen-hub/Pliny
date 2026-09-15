package com.dcp.outlet.worker.support;

import com.dcp.outlet.worker.WatermarkProvider;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryWatermarkProvider implements WatermarkProvider {

    private final Map<Integer, Long> watermarks = new ConcurrentHashMap<>();

    @Override
    public void update(int partition, long watermarkTs) { watermarks.put(partition, watermarkTs); }

    @Override
    public long safeWatermark(int partition) { return watermarks.getOrDefault(partition, 0L); }
}
