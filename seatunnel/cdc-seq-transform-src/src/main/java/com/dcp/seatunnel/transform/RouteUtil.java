package com.dcp.seatunnel.transform;

/**
 * 业务主键路由工具（设计文档 §4.3 / §4.4）
 *
 * <pre>
 *   Kafka 桶号    = hash(业务主键) % bucketCount    // 固定桶, 默认 4096
 *   MySQL 物理表号 = hash(业务主键) % tableCount     // 分表, 默认 50
 * </pre>
 *
 * 实现要点:
 *  1. <b>稳定散列</b>: 用 MurmurHash3 x86_32 + 固定种子, 任何 JVM / 任何版本结果一致。
 *     严禁用 {@code Object.hashCode()}（依赖实现, 跨版本可能变）;
 *  2. <b>非负</b>: 用 {@code Math.floorMod} 归一, 避免负数桶号;
 *  3. <b>空值兜底</b>: 业务主键为 null/空白时按策略处理 —— ERROR(默认, 暴露配置问题)
 *     或 FALLBACK(落到固定兜底键, 不塌陷)。
 */
public final class RouteUtil {

    /** 固定种子: 一旦定下不可改, 改了历史数据全部路由到新桶/新表 */
    private static final int SEED = 0x9747b28c;

    private RouteUtil() {
    }

    /** 路由结果 */
    public record Route(int bucket, int tableNo) {
    }

    /** 空值策略 */
    public enum NullKeyMode {
        /** 遇到空主键直接抛异常, 让任务失败暴露问题 */
        ERROR,
        /** 落到 fallbackValue, 保证不丢数据不塌陷 */
        FALLBACK
    }

    /**
     * MurmurHash3 x86_32: 纯 Java 实现, 跨 JVM 稳定。
     */
    public static int hash(String key) {
        return murmur3_x86_32(key.getBytes(java.nio.charset.StandardCharsets.UTF_8), SEED);
    }

    /**
     * 计算桶号 + 物理表号。
     *
     * @param businessKey 业务主键(非空, 调用方先用 {@link #resolveKey} 处理空值)
     * @param bucketCount Kafka 固定桶数(如 4096)
     * @param tableCount  目标物理分表数(如 50)
     */
    public static Route route(String businessKey, int bucketCount, int tableCount) {
        int h = hash(businessKey);
        return new Route(Math.floorMod(h, bucketCount), Math.floorMod(h, tableCount));
    }

    /**
     * 空值处理: 返回真正用于路由的键。
     *
     * @throws IllegalArgumentException ERROR 模式下主键为空
     */
    public static String resolveKey(Object raw, NullKeyMode mode, String fallbackValue) {
        if (raw == null || String.valueOf(raw).isBlank()) {
            if (mode == NullKeyMode.ERROR) {
                throw new IllegalArgumentException(
                        "业务主键为空: 检查源数据与字段映射（如需容错请配置 null_key_mode=FALLBACK）");
            }
            return fallbackValue;
        }
        return String.valueOf(raw);
    }

    // ------------------------------------------------------------------
    // MurmurHash3 x86_32 (public domain 算法, 固定种子保证跨 JVM 一致)
    // ------------------------------------------------------------------
    private static int murmur3_x86_32(byte[] data, int seed) {
        int len = data.length;
        int h1 = seed;
        final int c1 = 0xcc9e2d51;
        final int c2 = 0x1b873593;
        int i = 0;
        for (; i + 4 <= len; i += 4) {
            int k1 = (data[i] & 0xff)
                    | ((data[i + 1] & 0xff) << 8)
                    | ((data[i + 2] & 0xff) << 16)
                    | ((data[i + 3] & 0xff) << 24);
            k1 *= c1;
            k1 = Integer.rotateLeft(k1, 15);
            k1 *= c2;

            h1 ^= k1;
            h1 = Integer.rotateLeft(h1, 13);
            h1 = h1 * 5 + 0xe6546b64;
        }
        int k1 = 0;
        switch (len - i) {
            case 3:
                k1 ^= (data[i + 2] & 0xff) << 16;
                // fall through
            case 2:
                k1 ^= (data[i + 1] & 0xff) << 8;
                // fall through
            case 1:
                k1 ^= (data[i] & 0xff);
                k1 *= c1;
                k1 = Integer.rotateLeft(k1, 15);
                k1 *= c2;
                h1 ^= k1;
                // fall through
            default:
                break;
        }
        h1 ^= len;
        h1 ^= h1 >>> 16;
        h1 *= 0x85ebca6b;
        h1 ^= h1 >>> 13;
        h1 *= 0xc2b2ae35;
        h1 ^= h1 >>> 16;
        return h1;
    }
}
