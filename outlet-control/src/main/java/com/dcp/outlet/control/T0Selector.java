package com.dcp.outlet.control;

/**
 * 批边界 T0 的三规则(设计 §4.3):**任一不满足都会导致"批读不到 / 流接不上"**。
 *
 * <pre>
 *   R1  T0 ≥ changefeed.start_ts   —— 否则 T0 之前的变更从未进 Kafka,谁也补不回来
 *   R2  T0 ≥ GC safe point         —— 否则 TiDB 已无 T0 时刻版本,AS OF T0 读不出来
 *   R3  T0 ≤ latest                —— 不能取未来时刻
 * </pre>
 * 结果带"被哪条规则调整过",便于排障时解释为什么 T0 不是请求值。
 */
public class T0Selector {

    public interface ChangefeedStartTs { long startTs(String topic); }

    public interface GcSafePoint { long gcSafePointTs(); }

    public interface LatestTs { long latestTs(String topic, int partition); }

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

    public T0Selector(ChangefeedStartTs changefeed, GcSafePoint gcSafePoint, LatestTs latestTs) {
        this.changefeed = changefeed; this.gcSafePoint = gcSafePoint; this.latestTs = latestTs;
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
        if (t0 < gc) { t0 = gc; byGc = true; }

        boolean byLatest = false;
        if (t0 > latest) { t0 = latest; byLatest = true; }

        String detail = "startTs=" + startTs + " gcSafePoint=" + gc + " latest=" + latest;
        return new Decision(t0, byStart, byGc, byLatest, detail);
    }
}
