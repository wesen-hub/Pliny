package com.dcp.outlet.control;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

/**
 * 锚点索引器:常驻消费变更流,只解析 {@code commitTs / watermark},按策略落稀疏锚点。
 *
 * <p><b>成本模型</b>:一次顺序读 + 轻量解析;不存事件、不服务数据查询。
 * 每 {@code N} 条或 {@code T} 毫秒采一条 → 索引大小 ≈ 事件数 / N。
 *
 * <p><b>可重建</b>:索引不是真相源 —— 丢了就重建,查询退化为"时间索引 + 短扫"。
 */
public class AnchorIndexer {

    private static final class State {
        long rowsSinceSample;
        long lastSampleWall;
        long lastOffset = -1L;
    }

    private final AnchorStore anchors;
    private final WatermarkStore watermarks;
    private final AnchorIndexPolicy policy;
    private final MonotonicityGuard guard;
    private final LongSupplier clock;

    private final Map<String, State> states = new ConcurrentHashMap<>();
    private final Map<String, Long> sampled = new ConcurrentHashMap<>();

    public AnchorIndexer(AnchorStore anchors, WatermarkStore watermarks,
                         AnchorIndexPolicy policy, MonotonicityGuard guard, LongSupplier clock) {
        this.anchors = anchors;
        this.watermarks = watermarks;
        this.policy = policy;
        this.guard = guard;
        this.clock = clock;
    }

    public AnchorIndexer(AnchorStore anchors, WatermarkStore watermarks) {
        this(anchors, watermarks, AnchorIndexPolicy.defaults(), new MonotonicityGuard(), System::currentTimeMillis);
    }

    private String key(String topic, int partition) {
        return topic + "#" + partition;
    }

    /**
     * 喂入一条业务事件(DML/DDL)。
     *
     * @return true 表示本次采样落了一条锚点
     */
    public boolean onEvent(String topic, int partition, int epoch, long offset, long commitTs) {
        String k = key(topic, partition);
        State st = states.computeIfAbsent(k, x -> new State());
        long now = clock.getAsLong();

        guard.onSample(topic, partition, commitTs);

        if (st.lastSampleWall == 0L) {
            st.lastSampleWall = now;                      // 首个事件:初始化采样时钟
        }
        st.rowsSinceSample++;
        st.lastOffset = offset;

        if (policy.shouldSample((int) st.rowsSinceSample, now - st.lastSampleWall)) {
            anchors.save(new Anchor(topic, partition, epoch, offset, commitTs, now));
            sampled.merge(k, 1L, Long::sum);
            st.rowsSinceSample = 0;
            st.lastSampleWall = now;
            return true;
        }
        return false;
    }

    /** 喂入水位(TiCDC canal-json 的 TIDB_WATERMARK):"≤ 此 TSO 的事件已全部到齐"。 */
    public void onWatermark(String topic, int partition, int epoch, long offset, long watermarkTs) {
        watermarks.upsert(new Watermark(topic, partition, epoch, watermarkTs, offset, clock.getAsLong()));
    }

    /** 某分区已落锚点数(排障 / 自检)。 */
    public long sampledCount(String topic, int partition) {
        Long v = sampled.get(key(topic, partition));
        return v == null ? 0L : v;
    }

    /** 该分区最后处理到的 offset。 */
    public long lastOffset(String topic, int partition) {
        State st = states.get(key(topic, partition));
        return st == null ? -1L : st.lastOffset;
    }

    public MonotonicityGuard guard() { return guard; }
}
