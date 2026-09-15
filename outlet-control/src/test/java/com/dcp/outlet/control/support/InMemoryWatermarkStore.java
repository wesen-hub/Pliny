package com.dcp.outlet.control.support;

import com.dcp.outlet.control.Watermark;
import com.dcp.outlet.control.WatermarkStore;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** 内存水位表(模拟 chg_watermark_latest,单调写入)。 */
public class InMemoryWatermarkStore implements WatermarkStore {

    private final Map<String, Watermark> latest = new ConcurrentHashMap<>();

    private String key(String topic, int partition, int epoch) { return topic + "#" + partition + "#" + epoch; }

    @Override
    public void upsert(Watermark w) {
        latest.merge(key(w.topic(), w.partition(), w.epoch()), w,
                (oldW, newW) -> newW.watermarkTs() > oldW.watermarkTs() ? newW : oldW);
    }

    @Override
    public Watermark latest(String topic, int partition, int epoch) {
        return latest.get(key(topic, partition, epoch));
    }
}
