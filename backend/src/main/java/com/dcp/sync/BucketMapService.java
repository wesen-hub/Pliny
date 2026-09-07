package com.dcp.sync;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 桶→分区映射(V3 §4.3/§6.2)。
 *
 * <p>记录→桶 永远不变(桶号 = hash(业务主键) % bucketCount),桶→分区 可变:
 * 扩容时重算映射并热更新(采集端重新加载), 旧分区存量数据留在原地消费,
 * 新旧分居的乱序由全局 seq 兜底。
 *
 * <p>P3 映射规则: 顺序映射 partition = bucket % partitionCount(均摊且确定性)。
 * TODO(P3+): 支持自定义迁移策略(如把热点桶优先搬到新分区)。
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
        LogicalTableConfigService.LogicalTableConfig cfg = tableConfigService.load(logicalTable);
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
        log.info("桶映射就绪: {} ({} 桶 → {} 分区)", logicalTable, map.size(),
                map.values().stream().distinct().count());
        return map;
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
