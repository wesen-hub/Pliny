package com.dcp.sync;

/**
 * 业务主键路由工具(V3 §2.6,与 seatunnel-plugin/route-transform 的 RouteUtil 逻辑一致)。
 *
 * <pre>
 *   Kafka 桶号    = hash(业务主键) % bucketCount    // 固定桶, 默认 4096, 定死不可改
 *   MySQL 物理表号 = hash(业务主键) % tableCount     // 分表, 默认 50, 定死不可改
 * </pre>
 *
 * 铁律: 桶数 / 分表数 / 种子(SEED) 一旦上线不可改 —— 改了 = 同 key 换桶换表 = 实体分裂。
 * 纯函数: 只吃记录内容(业务主键), 不吃分区号/offset/实例 ID。
 */
public final class RouteUtil {

    /** 固定种子: 与 route-transform 插件保持一致 */
    public static final int SEED = 0x9747b28c;

    private RouteUtil() {
    }

    public record Route(int bucket, int tableNo) {
    }

    /** MurmurHash3 x86_32: 纯 Java 实现, 固定种子, 跨 JVM/跨版本稳定。严禁用 Object.hashCode()。 */
    public static int hash(String key) {
        return murmur3_x86_32(key.getBytes(java.nio.charset.StandardCharsets.UTF_8), SEED);
    }

    /** 桶号 + 物理表号(业务主键须非空, 空值由调用方按策略兜底) */
    public static Route route(String businessKey, int bucketCount, int tableCount) {
        int h = hash(businessKey);
        return new Route(Math.floorMod(h, bucketCount), Math.floorMod(h, tableCount));
    }

    /** 物理表名: tableCount<=1 → 逻辑表单物理表(去掉分片, TiDB 自动 split); 否则 t_order_03(兼容遗留) */
    public static String tableName(String prefix, int tableNo, int tableCount) {
        if (tableCount <= 1) {
            return prefix;
        }
        int width = Math.max(2, String.valueOf(tableCount - 1).length());
        return prefix + "_" + String.format("%0" + width + "d", tableNo);
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
