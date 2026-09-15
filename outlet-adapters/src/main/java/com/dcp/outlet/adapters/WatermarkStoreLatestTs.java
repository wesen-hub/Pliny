package com.dcp.outlet.adapters;

import com.dcp.outlet.control.T0Selector;
import com.dcp.outlet.control.Watermark;
import com.dcp.outlet.control.WatermarkStore;

/**
 * R3 适配器:分区最新 TSO = 水位表({@code chg_watermark_latest})。
 *
 * <p>水位语义 = "≤ 该 TSO 的事件已全部到齐"(由索引器持续写入),天然是"能安全取到的最新时点";
 * 取不到(该分区尚未被索引)返回 0 —— 调用方据此判断"无法定 T0"而失败,不猜时点。
 */
public class WatermarkStoreLatestTs implements T0Selector.LatestTs {

    private final WatermarkStore watermarks;
    private final int epoch;

    public WatermarkStoreLatestTs(WatermarkStore watermarks, int epoch) {
        this.watermarks = watermarks;
        this.epoch = epoch;
    }

    public WatermarkStoreLatestTs(WatermarkStore watermarks) {
        this(watermarks, 1);
    }

    @Override
    public long latestTs(String topic, int partition) {
        Watermark w = watermarks.latest(topic, partition, epoch);
        return w == null ? 0L : w.watermarkTs();
    }
}
