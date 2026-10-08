package com.dcp.sync;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 桶→分区映射(V3 §4.3/§6.2)。
 *
 * <p>记录→桶 永远不变(桶号 = hash(业务主键) % bucketCount),桶→分区 可变:
 * 扩容时重算映射并热更新(采集端重新加载), 旧分区存量数据留在原地消费,
 * 新旧分居的乱序由全局 seq 兜底。
 *
 * <p>P3 映射规则: 顺序映射 partition = bucket % partitionCount(均摊且确定性)。
 *
 * <p>自定义迁移策略(2026-10-09 落地, 推荐方案): {@link #migrateBuckets} 把指定桶迁到目标分区
 * (热点桶优先搬到新分区)。语义定论:
 * <ul>
 *   <li><b>切换点 = 元库原子切换</b>:单条 UPDATE(updatePartitions 带审计 + 消费端热更新),
 *       生产端 Partitioner 60s TTL 窗口内的尾巴消息仍落旧分区, 由消费端在旧分区正常消费完;
 *       不做双写期(与卸压目标矛盾, 写放大 2 倍);</li>
 *   <li><b>分区级迁移无存量搬运</b>:消息不重投, 旧分区上桶的存量由消费端照常消费,
 *       目标库分表路由(桶→分表)不变, 跨新旧分区的乱序由 ODKU seq 裁判兜底;
 *       「先新写后迁存量」是目标库实例级迁移的定论, 不适用于 Kafka 分区级;</li>
 *   <li><b>热点识别</b>:分区级 lag 排行见 ScaleService.suggestMigration;
 *       桶级热度精确排序需消费端桶级埋点上报(当前无通道, 按确定性抽样, TODO)。</li>
 * </ul>
 */
@Slf4j
@Component
public class BucketMapService {

    private final LogicalTableConfigService tableConfigService;
    private final Map<String, Map<Integer, Integer>> cache = new ConcurrentHashMap<>();

    public BucketMapService(LogicalTableConfigService tableConfigService) {
        this.tableConfigService = tableConfigService;
    }

    /** 桶 → 分区(缓存 + 惰性生成: 无配置时顺序映射) */
    public int partitionOf(String logicalTable, int bucket) {
        Map<Integer, Integer> map = cache.computeIfAbsent(logicalTable, this::loadMap);
        return map.getOrDefault(bucket, 0);
    }

    private Map<Integer, Integer> loadMap(String logicalTable) {
        Map<Integer, Integer> map = resolveMap(tableConfigService.load(logicalTable));
        log.info("桶映射就绪: {} ({} 桶 → {} 分区)", logicalTable, map.size(),
                map.values().stream().distinct().count());
        return map;
    }

    /** 解析映射(元库权威): 有配置解析, 无配置顺序展开 */
    private static Map<Integer, Integer> resolveMap(LogicalTableConfigService.LogicalTableConfig cfg) {
        Map<Integer, Integer> map = new HashMap<>();
        if (cfg.getBucketMap() != null && !cfg.getBucketMap().isEmpty()) {
            // 注意: 不能用类型化 forEach(隐式强转 String key → Integer 会 ClassCastException)
            for (Map.Entry<?, ?> e : cfg.getBucketMap().entrySet()) {
                map.put(Integer.valueOf(String.valueOf(e.getKey())),
                        Integer.valueOf(String.valueOf(e.getValue())));
            }
        } else {
            int buckets = cfg.getBucketCount();
            int parts = Math.max(1, cfg.getPartitionCount());
            for (int b = 0; b < buckets; b++) {
                map.put(b, Math.floorMod(b, parts));
            }
        }
        return map;
    }

    /**
     * 自定义迁移: 把指定桶迁到目标分区(热点桶优先搬到新分区)。
     *
     * <p>切换点 = 元库单条 UPDATE 原子切换(updatePartitions 已带审计 + 消费端热更新);
     * 生产端 60s TTL 窗口内仍写旧分区的尾巴由消费端在旧分区照常消费, 目标库 seq 裁判兜底乱序。
     *
     * @param buckets 迁移桶列表(0..bucketCount-1, 自动去重)
     * @param targetPartition 目标分区(0..partitionCount-1; 通常是新扩容的空闲分区)
     */
    public synchronized Map<String, Object> migrateBuckets(String logicalTable, int targetPartition,
                                                            List<Integer> buckets) {
        LogicalTableConfigService.LogicalTableConfig cfg = tableConfigService.load(logicalTable);
        int parts = Math.max(1, cfg.getPartitionCount());
        if (targetPartition < 0 || targetPartition >= parts) {
            throw new IllegalArgumentException("目标分区越界: " + targetPartition + "(分区数 " + parts + ");"
                    + " 新分区场景请先 expandPartitions 扩容");
        }
        List<Integer> list = buckets == null ? List.of()
                : buckets.stream().filter(Objects::nonNull).distinct().toList();
        if (list.isEmpty()) {
            throw new IllegalArgumentException("迁移桶列表为空");
        }
        for (int b : list) {
            if (b < 0 || b >= cfg.getBucketCount()) {
                throw new IllegalArgumentException("桶号越界: " + b + "(桶数 " + cfg.getBucketCount() + ")");
            }
        }
        Map<Integer, Integer> map = resolveMap(cfg);   // 以元库权威为准(不用本地缓存, 防漂移)
        for (int b : list) {
            map.put(b, targetPartition);
        }
        // 元库原子切换(单条 UPDATE) + 审计 + 消费端热更新(submitSyncTask)
        tableConfigService.updatePartitions(logicalTable, parts, toJson(map));
        cache.put(logicalTable, map);
        log.warn("桶迁移已生效: {} {} 个桶 → 分区 {} (元库原子切换; 生产端 TTL 尾巴留在旧分区由消费端消费完, seq 兜底乱序)",
                logicalTable, list.size(), targetPartition);
        return Map.of("logicalTable", logicalTable, "migratedBuckets", list.size(),
                "targetPartition", targetPartition, "partitionCount", parts);
    }

    /** 指定分区上的全部桶(热点拆分候选; 升序返回, 确定性抽样; 元库权威防缓存漂移) */
    public List<Integer> bucketsOf(String logicalTable, int partition) {
        Map<Integer, Integer> map = resolveMap(tableConfigService.load(logicalTable));
        return map.entrySet().stream()
                .filter(e -> e.getValue() == partition)
                .map(Map.Entry::getKey)
                .sorted()
                .toList();
    }

    /** 当前桶映射全量(桶号 → 分区号; 生产侧 BucketMapPartitioner 查询端点用) */
    public Map<Integer, Integer> bucketMapOf(String logicalTable) {
        return resolveMap(tableConfigService.load(logicalTable));
    }

    /** 扩容: 重算映射并落库(旧分区存量不搬, 新数据走新映射) */
    public synchronized void rebuild(String logicalTable, int newPartitionCount) {
        LogicalTableConfigService.LogicalTableConfig cfg = tableConfigService.load(logicalTable);
        int buckets = cfg.getBucketCount();
        Map<Integer, Integer> map = new HashMap<>();
        for (int b = 0; b < buckets; b++) {
            map.put(b, Math.floorMod(b, newPartitionCount));
        }
        String json = toJson(map);
        tableConfigService.updatePartitions(logicalTable, newPartitionCount, json);
        cache.put(logicalTable, map);
        log.warn("桶映射已重算: {} 分区 {} → {}, 新数据按新映射投递(存量留在旧分区, seq 兜底乱序)",
                logicalTable, cfg.getPartitionCount(), newPartitionCount);
    }

    /** 配置变更后失效缓存(热更新) */
    public void invalidate(String logicalTable) {
        cache.remove(logicalTable);
    }

    private static String toJson(Object o) {
        try {
            return new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(o);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
