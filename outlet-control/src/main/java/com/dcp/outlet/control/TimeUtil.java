package com.dcp.outlet.control;

/** TSO ↔ 墙钟时间换算。TiDB TSO 的低 18 位是逻辑计数器。 */
public final class TimeUtil {

    private static final int LOGICAL_BITS = 18;

    private TimeUtil() { }

    /** TSO → 毫秒(用于 Kafka 原生时间索引粗定位)。 */
    public static long tsoToMillis(long tso) {
        return tso >> LOGICAL_BITS;
    }

    /** 毫秒 → TSO(下界,仅用于粗略比较,不要当作精确 TSO 使用)。 */
    public static long millisToTsoFloor(long millis) {
        return millis << LOGICAL_BITS;
    }
}
