package com.dcp.sync;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 逻辑表配置(P1): 从元库 logical_table 加载 分表规则 + 业务主键 + 唯一键列。
 *
 * <p>键构成上线后不可变(变更 = 实体分裂); 未配置时返回默认(演示用)。
 * TODO(P2+): 配置变更走审计 + 热更新通知(采集端/消费端)。
 */
@Slf4j
@Component
public class LogicalTableConfigService {

    /** 逻辑表配置(元库 logical_table 一行) */
    @Data
    public static class LogicalTableConfig {
        private String tableName;
        private String topic;
        private int bucketCount = 4096;
        private int tableCount = 50;
        private String tablePrefix;
        private int partitionCount = 8;
        private String consumerGroup;
        /** {mode, scopeFields, systemNames, keyFields} */
        private Map<String, Object> businessKey;
        private List<String> keyColumns;
        private String syncMode;
        private String fullScope;
        private String cronExpr;
        private String startMode;
        private String startValue;
        /** 桶→分区映射(bucket → partition), null 表示顺序映射 bucket%partitionCount */
        private Map<Integer, Integer> bucketMap;
    }

    private final JdbcTemplate metaJdbc;
    private final Map<String, LogicalTableConfig> cache = new ConcurrentHashMap<>();
    private final SyncTopicRegistry registry;

    public LogicalTableConfigService(JdbcTemplate metaJdbc, SyncTopicRegistry registry) {
        this.metaJdbc = metaJdbc;
        this.registry = registry;
    }

    /** 加载(带缓存); 未配置时返回默认配置并告警日志 */
    public LogicalTableConfig load(String logicalTable) {
        LogicalTableConfig cached = cache.get(logicalTable);
        if (cached != null) {
            return cached;
        }
        LogicalTableConfig cfg = new LogicalTableConfig();
        cfg.setTableName(logicalTable);
        try {
            metaJdbc.query("SELECT topic, bucket_count, table_count, table_prefix, partition_count, "
                            + "consumer_group, business_key, key_columns, bucket_map, sync_mode, full_scope, cron_expr, start_mode, start_value FROM logical_table WHERE table_name = ?",
                    rs -> {
                        cfg.setTopic(rs.getString("topic"));
                        cfg.setBucketCount(rs.getInt("bucket_count"));
                        cfg.setTableCount(rs.getInt("table_count"));
                        cfg.setTablePrefix(rs.getString("table_prefix"));
                        cfg.setPartitionCount(rs.getInt("partition_count"));
                        cfg.setConsumerGroup(rs.getString("consumer_group"));
                        String bk = rs.getString("business_key");
                        if (bk != null) {
                            try {
                                cfg.setBusinessKey(new com.fasterxml.jackson.databind.ObjectMapper()
                                        .readValue(bk, Map.class));
                            } catch (Exception ignore) {
                                log.warn("business_key JSON 解析失败: {}", bk);
                            }
                        }
                        String kc = rs.getString("key_columns");
                        if (kc != null) {
                            try {
                                cfg.setKeyColumns(new com.fasterxml.jackson.databind.ObjectMapper()
                                        .readValue(kc, List.class));
                            } catch (Exception ignore) {
                                log.warn("key_columns JSON 解析失败: {}", kc);
                            }
                        }
                        cfg.setSyncMode(rs.getString("sync_mode"));
                        cfg.setFullScope(rs.getString("full_scope"));
                        cfg.setCronExpr(rs.getString("cron_expr"));
                        cfg.setStartMode(rs.getString("start_mode"));
                        cfg.setStartValue(rs.getString("start_value"));
                        String bm = rs.getString("bucket_map");
                        if (bm != null && !bm.isBlank()) {
                            try {
                                cfg.setBucketMap(new com.fasterxml.jackson.databind.ObjectMapper()
                                        .readValue(bm, Map.class));
                            } catch (Exception ignore) {
                                log.warn("bucket_map JSON 解析失败: {}", bm);
                            }
                        }
                    }, logicalTable);
        } catch (Exception e) {
            log.warn("逻辑表 {} 未在元库配置, 使用默认分表规则(prefix=t_order, count=50)", logicalTable);
            cfg.setTablePrefix("t_order");
        }
        cache.put(logicalTable, cfg);
        return cfg;
    }

    /** 保存配置(幂等 upsert; bucket_map 单独更新走 BucketMapService) */
    public void save(LogicalTableConfig cfg) {
        // 消费组缺省 = dcp-sync-{逻辑表}(与 SyncTopicRegistry 注册规则一致; 缺省会导致
        // lag 查询/监控误查默认组 dcp-sync-demo 而虚高)
        if (cfg.getConsumerGroup() == null || cfg.getConsumerGroup().isBlank()) {
            cfg.setConsumerGroup("dcp-sync-" + cfg.getTableName());
        }
        String bk = cfg.getBusinessKey() == null ? null : toJson(cfg.getBusinessKey());
        String kc = cfg.getKeyColumns() == null ? null : toJson(cfg.getKeyColumns());
        metaJdbc.update(
                "INSERT INTO logical_table (table_name, topic, bucket_count, table_count, table_prefix, "
                        + "partition_count, consumer_group, business_key, key_columns, sync_mode, full_scope, cron_expr, start_mode, start_value) "
                        + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                        + "ON DUPLICATE KEY UPDATE topic=VALUES(topic), bucket_count=VALUES(bucket_count), "
                        + "table_count=VALUES(table_count), table_prefix=VALUES(table_prefix), "
                        + "partition_count=VALUES(partition_count), consumer_group=VALUES(consumer_group), "
                        + "business_key=VALUES(business_key), key_columns=VALUES(key_columns), "
                        + "sync_mode=VALUES(sync_mode), full_scope=VALUES(full_scope), cron_expr=VALUES(cron_expr), "
                        + "start_mode=VALUES(start_mode), start_value=VALUES(start_value)",
                cfg.getTableName(), cfg.getTopic(), cfg.getBucketCount(), cfg.getTableCount(),
                cfg.getTablePrefix(), cfg.getPartitionCount(), cfg.getConsumerGroup(), bk, kc,
                cfg.getSyncMode() == null ? "full-cdc" : cfg.getSyncMode(), cfg.getFullScope(),
                cfg.getCronExpr(), cfg.getStartMode() == null ? "earliest" : cfg.getStartMode(),
                cfg.getStartValue());
        cache.remove(cfg.getTableName());
        // 消费端动态注册该逻辑表的 topic(无需重启即可被监听)
        registry.register(cfg.getTableName(), cfg.getTopic(), cfg.getConsumerGroup(),
                cfg.getPartitionCount() > 0 ? cfg.getPartitionCount() : 8);
    }

    /** 更新分区数与桶映射(扩容时由 BucketMapService 调用) */
    public void updatePartitions(String logicalTable, int partitionCount, String bucketMapJson) {
        metaJdbc.update("UPDATE logical_table SET partition_count = ?, bucket_map = ? WHERE table_name = ?",
                partitionCount, bucketMapJson, logicalTable);
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
