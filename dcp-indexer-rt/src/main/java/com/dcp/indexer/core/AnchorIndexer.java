package com.dcp.indexer.core;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

/**
 * 锚点索引器:常驻消费变更流, 只解析 {@code commitTs / watermark}, 按栅栏方案 §3.1 落稀疏栅栏点。
 *
 * <p><b>采样规则(栅栏方案, 不再盲采样)</b>:
 * <ul>
 *   <li>只采<b>刷新点</b>:{@code commitTs ≥ 分区历史最大}(等价于前缀上界刷新);乱序点永不入库;</li>
 *   <li>每 {@code sampleEveryMillis} 窗口取<b>最后一个</b>刷新点入库;窗口内无刷新则空缺, 不伪造;</li>
 *   <li>解析失败(C1):调 {@link #onPoison} 污染该分区 —— 此后不再产栅栏, 查询退化 fallback(宁慢勿错)。</li>
 * </ul>
 *
 * <p><b>可重建</b>:索引不是真相源 —— 丢了就重建, 查询退化为 earliest 回扫。
 */
public class AnchorIndexer {

    private static final class State {
        boolean poisoned;          // C1 污染:不再产栅栏
        long lastSampleWall;       // 当前采样窗口起点(毫秒)
        long pendingOffset = -1L;  // 窗口内最后一个刷新点(未落库)
        long pendingTs = -1L;
    }

    private final AnchorStore anchors;
    private final WatermarkStore watermarks;
    private final AnchorIndexPolicy policy;
    private final MonotonicityGuard guard;
    private final LongSupplier clock;
    /** 可空:解析失败污染(C1)与边界审计告警出口 */
    private final java.util.function.BiConsumer<String, String> alert;
    /** 可空:C1 污染死信存储(持久化到元库;未接线则仅内存) */
    private PoisonStore poisons;
    private int poisonEpoch;

    private final Map<String, State> states = new ConcurrentHashMap<>();
    private final Map<String, Long> sampled = new ConcurrentHashMap<>();

    public AnchorIndexer(AnchorStore anchors, WatermarkStore watermarks,
                         AnchorIndexPolicy policy, MonotonicityGuard guard, LongSupplier clock) {
        this(anchors, watermarks, policy, guard, clock, null);
    }

    public AnchorIndexer(AnchorStore anchors, WatermarkStore watermarks,
                         AnchorIndexPolicy policy, MonotonicityGuard guard, LongSupplier clock,
                         java.util.function.BiConsumer<String, String> alert) {
        this.anchors = anchors;
        this.watermarks = watermarks;
        this.policy = policy;
        this.guard = guard;
        this.clock = clock;
        this.alert = alert;
    }

    public AnchorIndexer(AnchorStore anchors, WatermarkStore watermarks) {
        this(anchors, watermarks, AnchorIndexPolicy.defaults(), new MonotonicityGuard(), System::currentTimeMillis);
    }

    /**
     * 接线 C1 污染死信存储(生产):启动时恢复已污染分区(不重复告警), 首次污染时落库(幂等)。
     * 未接线则污染仅进程内存生效, 重启后丢失。
     */
    public AnchorIndexer withPoisonStore(PoisonStore store, int epoch) {
        this.poisons = store;
        this.poisonEpoch = epoch;
        if (store != null) {
            for (Poison p : store.list()) {
                if (p.epoch() == epoch) {
                    states.computeIfAbsent(key(p.topic(), p.partition()), x -> new State()).poisoned = true;
                }
            }
        }
        return this;
    }

    private String key(String topic, int partition) {
        return topic + "#" + partition;
    }

    /**
     * 喂入一条业务事件(DML/DDL)。
     *
     * @return true 表示本次采样落了一条栅栏点
     */
    public boolean onEvent(String topic, int partition, int epoch, long offset, long commitTs) {
        String k = key(topic, partition);
        State st = states.computeIfAbsent(k, x -> new State());
        long now = clock.getAsLong();

        // 守卫:观测指标(回退计数);回退时 lastSeen 不推进, 保持"历史最大"
        boolean regressed = guard.onSample(topic, partition, commitTs);

        if (st.poisoned) {
            return false;                              // C1:已污染, 永不产栅栏
        }
        if (regressed) {
            return false;                              // 乱序点:永不入库(§3.1)
        }

        // commitTs ≥ 历史最大(守卫未推进) → 刷新点:更新窗口内候选
        st.pendingOffset = offset;
        st.pendingTs = commitTs;
        if (st.lastSampleWall == 0L) {
            st.lastSampleWall = now;                   // 首个刷新点:开窗, 窗口内取"最后一个", 首点先不落
            return false;
        }
        if (now - st.lastSampleWall >= policy.sampleEveryMillis()) {
            anchors.save(new Anchor(topic, partition, epoch, st.pendingOffset, st.pendingTs, now, true));
            sampled.merge(k, 1L, Long::sum);
            st.pendingOffset = -1L;
            st.pendingTs = -1L;
            st.lastSampleWall = now;
            return true;
        }
        return false;
    }

    /** C1:解析失败 → 污染该分区 lastMax(置 +∞)并告警;此后该分区不再产栅栏, 查询退化 fallback。 */
    public boolean onPoison(String topic, int partition) {
        String k = key(topic, partition);
        State st = states.computeIfAbsent(k, x -> new State());
        if (st.poisoned) {
            return false;
        }
        st.poisoned = true;
        if (poisons != null) {
            try {
                poisons.mark(topic, partition, poisonEpoch);   // 死信落库:跨重启成立, 人工 DELETE 恢复
            } catch (Exception e) {
                if (alert != null) {
                    alert.accept(k, "C1 污染死信落库失败(" + e + "):重启后可能恢复采样, 需人工确认");
                }
            }
        }
        if (alert != null) {
            alert.accept(k, "C1 解析失败污染:分区栅栏停止产出, 查询退化 fallback(宁慢勿错)");
        }
        return true;
    }

    /** 该分区是否已被污染(C1;排障/自检)。 */
    public boolean poisoned(String topic, int partition) {
        State st = states.get(key(topic, partition));
        return st != null && st.poisoned;
    }

    /** 已污染分区列表(C1 死信;排障/观测端点)。 */
    public java.util.List<String> poisonedPartitions() {
        java.util.List<String> out = new java.util.ArrayList<>();
        states.forEach((k, st) -> {
            if (st.poisoned) out.add(k);
        });
        return out;
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

    public MonotonicityGuard guard() { return guard; }
}
