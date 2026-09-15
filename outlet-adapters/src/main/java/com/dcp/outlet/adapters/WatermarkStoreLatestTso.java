package com.dcp.outlet.adapters;

import com.dcp.outlet.control.LatestTsoProvider;
import com.dcp.outlet.control.Watermark;
import com.dcp.outlet.control.WatermarkStore;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 控制面生产实现:`startPolicy=latest` 的"最新 TSO"来源 = **水位表**({@code chg_watermark_latest})。
 *
 * <p>为什么不直接问 Kafka:水位是**由索引器持续写进水位表**的("≤ 此 TSO 的事件已全部到齐"),
 * 它天然是"已到齐的最新时点",比自己扫 topic 末尾更安全(末尾可能只有 DML、没有水位,
 * 而"从最新起"必须落在**水位**上,否则会从半途接上 → 空洞)。
 *
 * <p>取不到水位 = 该分区还没被索引过 → 返回 0(调用方 {@code TaskScheduler} 会据此退化,
 * 不猜时点)。
 */
public class WatermarkStoreLatestTso implements LatestTsoProvider {

    private final WatermarkStore watermarks;
    /** 小缓存:同一批单元逐个分区问,别反复打库(水位只前进,缓存旧值只会更保守)。 */
    private final Map<String, Long> cache = new ConcurrentHashMap<>();
    private final long ttlMillis;
    private volatile long cachedAt;

    public WatermarkStoreLatestTso(WatermarkStore watermarks, long ttlMillis) {
        this.watermarks = watermarks;
        this.ttlMillis = ttlMillis <= 0 ? 5_000L : ttlMillis;
    }

    public WatermarkStoreLatestTso(WatermarkStore watermarks) {
        this(watermarks, 5_000L);
    }

    @Override
    public long latestTso(String topic, int partition, int epoch) {
        String key = topic + "#" + partition + "#" + epoch;
        long now = System.currentTimeMillis();
        if (now - cachedAt > ttlMillis) {
            cache.clear();
            cachedAt = now;
        }
        Long hit = cache.get(key);
        if (hit != null) return hit;
        Watermark w = watermarks.latest(topic, partition, epoch);
        long tso = w == null ? 0L : w.watermarkTs();
        cache.put(key, tso);
        return tso;
    }
}
