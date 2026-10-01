package com.dcp.sync;

import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 死信管理服务(控制面): 查询 / 统计 / 重放 / 忽略。
 *
 * <p>执行态落死信(save/saveRaw)已随写路径迁至 dcp-synclink 业务 jar(DltService);
 * 控制面保留查询与重放能力 —— 重放走本服务 KafkaTemplate, 原 payload 原样重发原
 * topic(不领新 seq), 消费端 ODKU 幂等吸收。
 */
@Slf4j
@Service
public class DltAdminService {

    private final JdbcTemplate metaJdbc;
    private final KafkaTemplate<String, String> kafka;

    public DltAdminService(JdbcTemplate metaJdbc, KafkaTemplate<String, String> kafka) {
        this.metaJdbc = metaJdbc;
        this.kafka = kafka;
    }

    /** 分页查询 */
    public Map<String, Object> page(String logicalTable, String status, String keyword,
                                    String sourceTable, int page, int pageSize) {
        StringBuilder where = new StringBuilder(" WHERE 1=1");
        List<Object> args = new ArrayList<>();
        if (logicalTable != null && !logicalTable.isBlank()) {
            where.append(" AND logical_table = ?");
            args.add(logicalTable);
        }
        if (sourceTable != null && !sourceTable.isBlank()) {
            where.append(" AND source_table = ?");
            args.add(sourceTable);
        }
        if (status != null && !status.isBlank()) {
            where.append(" AND status = ?");
            args.add(status);
        }
        if (keyword != null && !keyword.isBlank()) {
            where.append(" AND (business_key LIKE ? OR error_msg LIKE ?)");
            args.add("%" + keyword + "%");
            args.add("%" + keyword + "%");
        }
        long total = metaJdbc.queryForObject("SELECT COUNT(*) FROM dlt_message" + where, Long.class, args.toArray());
        int offset = Math.max(0, (page - 1) * pageSize);
        List<Map<String, Object>> list = metaJdbc.queryForList(
                "SELECT id, logical_table, source_table, topic, partition_id, offset_id, business_key, "
                        + "error_msg, fail_count, status, first_fail_time, last_fail_time, replay_time "
                        + "FROM dlt_message" + where + " ORDER BY id DESC LIMIT ? OFFSET ?",
                join(args, pageSize, offset));
        return Map.of("total", total, "list", list);
    }

    private static Object[] join(List<Object> args, int pageSize, int offset) {
        List<Object> all = new ArrayList<>(args);
        all.add(pageSize);
        all.add(offset);
        return all.toArray();
    }

    /** 统计卡片: 待处理 / 今日新增 / 近7天 / 重放成功数 */
    public Map<String, Object> stats() {
        Long pending = metaJdbc.queryForObject(
                "SELECT COUNT(*) FROM dlt_message WHERE status = 'PENDING'", Long.class);
        Long today = metaJdbc.queryForObject(
                "SELECT COUNT(*) FROM dlt_message WHERE DATE(last_fail_time) = CURDATE()", Long.class);
        Long week = metaJdbc.queryForObject(
                "SELECT COUNT(*) FROM dlt_message WHERE last_fail_time >= DATE_SUB(NOW(), INTERVAL 7 DAY)", Long.class);
        Long replayed = metaJdbc.queryForObject(
                "SELECT COUNT(*) FROM dlt_message WHERE status = 'REPLAYED'", Long.class);
        // 按逻辑表 TOP5
        List<Map<String, Object>> top = metaJdbc.queryForList(
                "SELECT logical_table, COUNT(*) cnt FROM dlt_message GROUP BY logical_table ORDER BY cnt DESC LIMIT 5");
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("pending", pending);
        out.put("today", today);
        out.put("week", week);
        out.put("replayed", replayed);
        out.put("topTables", top);
        return out;
    }

    /** 详情(含 payload 全文) */
    public Map<String, Object> detail(Long id) {
        return metaJdbc.queryForMap(
                "SELECT id, logical_table, source_table, topic, partition_id, offset_id, business_key, "
                        + "payload, error_msg, fail_count, status, first_fail_time, last_fail_time, replay_time "
                        + "FROM dlt_message WHERE id = ?", id);
    }

    /** 重放单条: 原 payload 原样重发原 topic(不领新 seq, 幂等吸收) */
    public Map<String, Object> replay(Long id) {
        Map<String, Object> row = detail(id);
        if (row == null || row.isEmpty()) {
            throw new IllegalArgumentException("死信记录不存在: " + id);
        }
        String topic = String.valueOf(row.get("topic"));
        String businessKey = row.get("business_key") == null ? null : String.valueOf(row.get("business_key"));
        String payload = row.get("payload") == null ? null : String.valueOf(row.get("payload"));
        if (payload == null || payload.isBlank()) {
            throw new IllegalArgumentException("死信记录无 payload, 无法重放: " + id);
        }
        kafka.send(topic, businessKey, payload);
        metaJdbc.update("UPDATE dlt_message SET status = 'REPLAYED', replay_time = NOW() WHERE id = ?", id);
        log.info("死信重放: id={} topic={} key={}", id, topic, businessKey);
        return Map.of("replayed", true, "id", id, "topic", topic);
    }

    /** 批量重放 */
    public int replayBatch(List<Long> ids) {
        int n = 0;
        for (Long id : ids) {
            try {
                replay(id);
                n++;
            } catch (Exception e) {
                log.warn("死信重放失败 id={}: {}", id, e.getMessage());
            }
        }
        return n;
    }

    /** 忽略(标记不处理) */
    public void ignore(Long id) {
        metaJdbc.update("UPDATE dlt_message SET status = 'IGNORED' WHERE id = ? AND status = 'PENDING'", id);
    }
}
