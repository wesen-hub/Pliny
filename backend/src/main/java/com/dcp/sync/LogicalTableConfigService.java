package com.dcp.sync;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import com.dcp.schedulerbridge.SchedulerRuntime;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 逻辑表配置(P1): 从元库 logical_table 加载 分表规则 + 业务主键 + 唯一键列。
 *
 * <p>键构成上线后不可变(变更 = 实体分裂); 未配置时返回默认(演示用)。
 *
 * <p>审计 + 热更新通知(2026-10-09 落地):
 * <ul>
 *   <li>审计: 每次 save/updatePartitions 对比变更字段写 logical_table_audit;</li>
 *   <li>消费端热更新: save/updatePartitions 后统一 submitSyncTask(重复提交 = 更新 TaskSpec);</li>
 *   <li>采集端热更新: 发布 {@link LogicalTableConfigChangedEvent}, CdcStService 监听
 *       topic 变更(TOPIC_CHANGED)重提采集任务换 sink topic。</li>
 * </ul>
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
    private final SchedulerRuntime schedulerRuntime;
    private final ApplicationEventPublisher events;

    public LogicalTableConfigService(JdbcTemplate metaJdbc,
                                     SchedulerRuntime schedulerRuntime,
                                     ApplicationEventPublisher events) {
        this.metaJdbc = metaJdbc;
        this.schedulerRuntime = schedulerRuntime;
        this.events = events;
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

    /** 保存配置(幂等 upsert; bucket_map 单独更新走 BucketMapService); 审计 + 消费端/采集端热更新通知 */
    public void save(LogicalTableConfig cfg) {
        // 消费组缺省 = dcp-sync-{逻辑表}(与 sync-writer 执行器约定一致; 缺省会导致
        // lag 查询/监控误查默认组 dcp-sync-demo 而虚高)
        if (cfg.getConsumerGroup() == null || cfg.getConsumerGroup().isBlank()) {
            cfg.setConsumerGroup("dcp-sync-" + cfg.getTableName());
        }
        // 审计: 对比旧配置(cache 优先, 未命中查库; 无旧配置 = 新建)
        LogicalTableConfig old = cache.get(cfg.getTableName());
        boolean existed = old != null || loadSilently(cfg.getTableName()) != null;
        if (old == null) {
            old = new LogicalTableConfig();
            old.setTableName(cfg.getTableName());
        }
        String oldTopic = existed ? old.getTopic() : null;
        String action = !existed ? "CREATE"
                : Objects.equals(oldTopic, cfg.getTopic()) ? "UPDATE" : "TOPIC_CHANGED";
        writeAudit(cfg.getTableName(), action, null, diff(old, cfg));

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
        // 消费端热更新:逻辑表保存/变更 → 提交 TaskSpec(重复提交 = 更新)
        schedulerRuntime.submitSyncTask(cfg);
        // 采集端热更新通知(TOPIC_CHANGED 时 CdcStService 重提采集任务换 sink topic)
        events.publishEvent(new LogicalTableConfigChangedEvent(
                cfg.getTableName(), action, oldTopic, cfg.getTopic()));
    }

    /** 更新分区数与桶映射(扩容时由 BucketMapService 调用); 审计 + 消费端热更新 */
    public void updatePartitions(String logicalTable, int partitionCount, String bucketMapJson) {
        LogicalTableConfig old = cache.get(logicalTable);
        if (old == null) {
            old = loadSilently(logicalTable);
        }
        LogicalTableConfig after = new LogicalTableConfig();
        after.setTableName(logicalTable);
        after.setPartitionCount(partitionCount);
        after.setBucketMap(old == null ? null : old.getBucketMap());
        Map<String, Object> changes = diff(old == null ? new LogicalTableConfig() : old, after);
        // bucket_map 是本接口的核心变更维度, 单独对比(from=旧映射对象, to=新映射 JSON 串)
        String oldBm = old != null && old.getBucketMap() != null ? toJson(old.getBucketMap()) : null;
        if (!Objects.equals(oldBm, bucketMapJson == null ? null : bucketMapJson)) {
            Map<String, Object> d = new LinkedHashMap<>();
            d.put("from", old == null ? null : old.getBucketMap());
            d.put("to", bucketMapJson);
            changes.put("bucketMap", d);
        }
        writeAudit(logicalTable, "PARTITIONS", null, changes);
        metaJdbc.update("UPDATE logical_table SET partition_count = ?, bucket_map = ? WHERE table_name = ?",
                partitionCount, bucketMapJson, logicalTable);
        cache.remove(logicalTable);
        // 消费端热更新: 扩容后 TaskSpec 分区数/桶映射需同步更新(重复提交 = 更新)
        LogicalTableConfig fresh = load(logicalTable);
        schedulerRuntime.submitSyncTask(fresh);
        events.publishEvent(new LogicalTableConfigChangedEvent(
                logicalTable, "PARTITIONS", fresh.getTopic(), fresh.getTopic()));
    }

    /** 查库加载(不打缓存, 无配置返回 null; 审计对比用) */
    private LogicalTableConfig loadSilently(String logicalTable) {
        try {
            LogicalTableConfig cfg = new LogicalTableConfig();
            cfg.setTableName(logicalTable);
            metaJdbc.query("SELECT topic, bucket_count, table_count, table_prefix, partition_count, "
                            + "consumer_group, business_key, key_columns, bucket_map, sync_mode, full_scope, "
                            + "cron_expr, start_mode, start_value FROM logical_table WHERE table_name = ?",
                    rs -> {
                        cfg.setTopic(rs.getString("topic"));
                        cfg.setBucketCount(rs.getInt("bucket_count"));
                        cfg.setTableCount(rs.getInt("table_count"));
                        cfg.setTablePrefix(rs.getString("table_prefix"));
                        cfg.setPartitionCount(rs.getInt("partition_count"));
                        cfg.setConsumerGroup(rs.getString("consumer_group"));
                        cfg.setSyncMode(rs.getString("sync_mode"));
                        cfg.setFullScope(rs.getString("full_scope"));
                        cfg.setCronExpr(rs.getString("cron_expr"));
                        cfg.setStartMode(rs.getString("start_mode"));
                        cfg.setStartValue(rs.getString("start_value"));
                        String kc = rs.getString("key_columns");
                        if (kc != null) {
                            try {
                                cfg.setKeyColumns(new com.fasterxml.jackson.databind.ObjectMapper()
                                        .readValue(kc, List.class));
                            } catch (Exception ignore) {
                            }
                        }
                        String bm = rs.getString("bucket_map");
                        if (bm != null && !bm.isBlank()) {
                            try {
                                cfg.setBucketMap(new com.fasterxml.jackson.databind.ObjectMapper()
                                        .readValue(bm, Map.class));
                            } catch (Exception ignore) {
                            }
                        }
                    }, logicalTable);
            return cfg.getTopic() == null && cfg.getTablePrefix() == null ? null : cfg;
        } catch (Exception e) {
            return null;
        }
    }

    /** 审计落库: action + 变更字段 {字段: {from, to}} */
    private void writeAudit(String tableName, String action, String operator, Map<String, Object> changes) {
        try {
            metaJdbc.update("INSERT INTO logical_table_audit (table_name, action, operator, changed_fields) "
                    + "VALUES (?, ?, ?, ?)", tableName, action, operator,
                    changes.isEmpty() ? null : toJson(changes));
            if (!changes.isEmpty()) {
                log.info("逻辑表配置变更审计 {} action={} changed={}", tableName, action, changes);
            }
        } catch (Exception e) {
            log.warn("逻辑表配置审计写入失败(不阻断保存): {}", e.getMessage());
        }
    }

    /** 变更字段对比(关键配置维度; 无变化返回空) */
    private static Map<String, Object> diff(LogicalTableConfig old, LogicalTableConfig now) {
        Map<String, Object> changes = new LinkedHashMap<>();
        compare(changes, "topic", old.getTopic(), now.getTopic());
        compare(changes, "bucketCount", old.getBucketCount(), now.getBucketCount());
        compare(changes, "tableCount", old.getTableCount(), now.getTableCount());
        compare(changes, "tablePrefix", old.getTablePrefix(), now.getTablePrefix());
        compare(changes, "partitionCount", old.getPartitionCount(), now.getPartitionCount());
        compare(changes, "consumerGroup", old.getConsumerGroup(), now.getConsumerGroup());
        compare(changes, "businessKey", old.getBusinessKey(), now.getBusinessKey());
        compare(changes, "keyColumns", old.getKeyColumns(), now.getKeyColumns());
        compare(changes, "syncMode", old.getSyncMode(), now.getSyncMode());
        compare(changes, "fullScope", old.getFullScope(), now.getFullScope());
        compare(changes, "cronExpr", old.getCronExpr(), now.getCronExpr());
        compare(changes, "startMode", old.getStartMode(), now.getStartMode());
        compare(changes, "startValue", old.getStartValue(), now.getStartValue());
        return changes;
    }

    private static void compare(Map<String, Object> changes, String field, Object a, Object b) {
        if (!Objects.equals(String.valueOf(a), String.valueOf(b))) {
            Map<String, Object> d = new LinkedHashMap<>();
            d.put("from", a);
            d.put("to", b);
            changes.put(field, d);
        }
    }

    private static String toJson(Object o) {
        try {
            return new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(o);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
