package com.dcp.outlet.control;

/**
 * 批边界 T0 的三规则(设计 §4.3):**任一不满足都会导致"批读不到 / 流接不上"**。
 *
 * <pre>
 *   R1  T0 ≥ changefeed.start_ts   —— 否则 T0 之前的变更从未进 Kafka,谁也补不回来
 *   R2  T0 ≥ GC safe point + margin —— 否则 TiDB 已无 T0 时刻版本,AS OF T0 读不出来
 *                                     (margin = 安全余量: GC 推进窗口内快照会被回收,
 *                                      紧贴 safe point 选取会在读窗中失败)
 *   R3  T0 ≤ latest                —— 不能取未来时刻
 * </pre>
 * 结果带"被哪条规则调整过",便于排障时解释为什么 T0 不是请求值。
 */
public class T0Selector {

    public interface ChangefeedStartTs { long startTs(String topic); }

    public interface GcSafePoint { long gcSafePointTs(); }

    public interface LatestTs { long latestTs(String topic, int partition); }

    /** 默认 GC 安全余量(毫秒): GC life_time 默认 10 分钟, safe point 每轮才跳一次,
     *  2 分钟余量足够覆盖批快照读窗口与时钟偏差, 又不至于过度后移 T0。 */
    public static final long DEFAULT_GC_SAFE_MARGIN_MS = 120_000L;

    /** 毫秒 → TSO 的换算(与 TiDbGcSafePoint.toTso 同口径: 物理毫秒 << 18, 逻辑位=0)。 */
    public static final int MS_TO_TSO_SHIFT = 18;

    /** T0 决策结果。 */
    public static final class Decision {
        private final long tso;
        private final boolean bumpedByStartTs;
        private final boolean bumpedByGcSafePoint;
        private final boolean clampedByLatest;
        private final String detail;

        Decision(long tso, boolean b1, boolean b2, boolean b3, String detail) {
            this.tso = tso; this.bumpedByStartTs = b1; this.bumpedByGcSafePoint = b2;
            this.clampedByLatest = b3; this.detail = detail;
        }

        public long tso() { return tso; }
        public boolean bumpedByStartTs() { return bumpedByStartTs; }
        public boolean bumpedByGcSafePoint() { return bumpedByGcSafePoint; }
        public boolean clampedByLatest() { return clampedByLatest; }
        public String detail() { return detail; }

        @Override public String toString() {
            return "T0=" + tso + (bumpedByStartTs ? " (R1 提到 changefeed start_ts)" : "")
                    + (bumpedByGcSafePoint ? " (R2 提到 GC safe point)" : "")
                    + (clampedByLatest ? " (R3 压到 latest)" : "");
        }
    }

    private final ChangefeedStartTs changefeed;
    private final GcSafePoint gcSafePoint;
    private final LatestTs latestTs;
    /** GC 安全余量(毫秒): T0 必须严格大于 safe point + margin */
    private final long gcSafeMarginMs;

    public T0Selector(ChangefeedStartTs changefeed, GcSafePoint gcSafePoint, LatestTs latestTs) {
        this(changefeed, gcSafePoint, latestTs, DEFAULT_GC_SAFE_MARGIN_MS);
    }

    public T0Selector(ChangefeedStartTs changefeed, GcSafePoint gcSafePoint, LatestTs latestTs,
                      long gcSafeMarginMs) {
        this.changefeed = changefeed; this.gcSafePoint = gcSafePoint; this.latestTs = latestTs;
        this.gcSafeMarginMs = Math.max(0L, gcSafeMarginMs);
    }

    /**
     * @param requested 请求的 T0(通常为批开始时取的 latest;为 null 则用 latest)
     */
    public Decision select(String topic, int partition, Long requested) {
        long latest = latestTs.latestTs(topic, partition);
        long t0 = requested == null ? latest : requested;

        boolean byStart = false, byGc = false;
        long startTs = changefeed == null ? 0L : changefeed.startTs(topic);
        if (t0 < startTs) { t0 = startTs; byStart = true; }

        long gc = gcSafePoint == null ? 0L : gcSafePoint.gcSafePointTs();
        // R2 加安全余量: 紧贴 safe point 选取时, GC 推进会在快照读窗口内回收版本;
        // 须留出 margin 保证 AS OF T0 全程可读。gc 为 TSO, margin 为毫秒, 须先换算
        // (ms << MS_TO_TSO_SHIFT) 再相加 —— 直接相加等于只留了 0.46ms 余量。
        long gcFloor = gc + (gcSafeMarginMs << MS_TO_TSO_SHIFT);
        if (t0 < gcFloor) { t0 = gcFloor; byGc = true; }

        boolean byLatest = false;
        if (t0 > latest) { t0 = latest; byLatest = true; }

        String detail = "startTs=" + startTs + " gcSafePoint=" + gc + " gcMargin=" + gcSafeMarginMs + " latest=" + latest;
        return new Decision(t0, byStart, byGc, byLatest, detail);
    }
}
