package com.dcp.synclink.core;

import com.dcp.synclink.model.Json;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * 同步死信落库(执行态, V3): 消费失败消息落库留证据。
 *
 * <p>消费端失败不再静默丢弃, 改为逐条写入 dlt_message(提交 offset 不阻塞管道,
 * 但留证据可监控/重放)。重放/查询/忽略等管理能力留在 backend 控制面(DltAdminService)。
 *
 * <p>无 Spring 装配: 元库 JdbcTemplate 由 SynclinkComponents 从 ExecutorEnvironment 注入。
 */
@Slf4j
public class DltService {

    private final JdbcTemplate metaJdbc;

    public DltService(JdbcTemplate metaJdbc) {
        this.metaJdbc = metaJdbc;
    }

    /** 消费失败 → 落死信(逐条) */
    public void save(ConsumerRecord<String, String> record, String errorMsg) {
        String payload = record.value();
        String logicalTable = parseLogicalTable(payload);
        String businessKey = record.key();
        metaJdbc.update(
                "INSERT INTO dlt_message (logical_table, source_table, topic, partition_id, offset_id, "
                        + "business_key, payload, error_msg, fail_count, status, first_fail_time, last_fail_time) "
                        + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, 'PENDING', NOW(), NOW())",
                logicalTable, null, record.topic(), record.partition(), record.offset(),
                businessKey, payload, truncate(errorMsg, 2000));
    }

    /** 批量失败逐条隔离时的防御性汇总死信(无原始 record 对应时; 攒批场景) */
    public void saveRaw(String logicalTable, String errorMsg, int count) {
        metaJdbc.update(
                "INSERT INTO dlt_message (logical_table, source_table, topic, partition_id, offset_id, "
                        + "business_key, payload, error_msg, fail_count, status, first_fail_time, last_fail_time) "
                        + "VALUES (?, 'BATCH', '-', 0, 0, '-', ?, ?, 1, 'PENDING', NOW(), NOW())",
                logicalTable, "[批量攒批失败 " + count + " 条]", truncate(errorMsg, 2000));
    }

    /** 从 payload 解析逻辑表(仅 SeaTunnel envelope 格式, 解析失败尽力而为返回"未知") */
    private String parseLogicalTable(String payload) {
        try {
            var node = Json.readTree(payload);
            if (!node.has("__payload")) {
                return "未知";
            }
            var inner = Json.readTree(node.get("__payload").asText());
            return inner.has("logicalTable") ? inner.get("logicalTable").asText() : "未知";
        } catch (Exception e) {
            return "未知";
        }
    }

    private static String truncate(String s, int max) {
        if (s == null) {
            return null;
        }
        return s.length() > max ? s.substring(0, max) : s;
    }
}
