package com.dcp.ingest.model;

/**
 * 业务主键路由工具(与 com.dcp.sync.RouteUtil 完全一致的纯函数复制, 铁律不变):
 * <pre>
 *   Kafka 桶号 = hash(业务主键) % bucketCount    // 固定桶, 默认 4096, 定死不可改
 * </pre>
 * 纯函数: 只吃记录内容(业务主键), 不吃分区号/offset/实例 ID。
 */
public final class RouteUtil {

    /** 固定种子: 与 route-transform 插件 / backend RouteUtil 保持一致 */
    public static final int SEED = 0x9747b28c;

    private RouteUtil() {
    }

    /** MurmurHash3 x86_32: 纯 Java 实现, 固定种子, 跨 JVM/跨版本稳定。严禁用 Object.hashCode()。 */
    public static int hash(String key) {
        return murmur3_x86_32(key.getBytes(java.nio.charset.StandardCharsets.UTF_8), SEED);
    }

    /** 桶号(业务主键须非空, 空值由调用方按策略兜底) */
    public static int bucket(String businessKey, int bucketCount) {
        return Math.floorMod(hash(businessKey), bucketCount);
    }

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
