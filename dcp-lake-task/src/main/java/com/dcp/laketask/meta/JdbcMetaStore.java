package com.dcp.laketask.meta;

import com.dcp.laketask.model.Json;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 元库 JDBC 实现(手工组装, 无 Spring; 语义对齐 dcp-ingest-task JdbcMetaStore 的读取部分)。
 *
 * <p>连接策略: 每操作独立连接(配置读带缓存、DLT=异常路径)——湖增量主链路写湖
 * 不经过元库, 开销可接受, 避免自建连接池的复杂度。
 */
public class JdbcMetaStore implements MetaStore {

    private static final Logger log = LoggerFactory.getLogger(JdbcMetaStore.class);

    private final String url;
    private final String user;
    private final String password;
    /** 逻辑表配置缓存(TTL 60s; 与 LogicalTableConfigService 缓存语义对齐) */
    private final Map<String, Object[]> ltCache = new LinkedHashMap<>();

    public JdbcMetaStore(String url, String user, String password) {
        this.url = url;
        this.user = user;
        this.password = password;
    }

    private Connection open() throws Exception {
        Connection c = DriverManager.getConnection(url, user, password);
        c.setAutoCommit(true);
        return c;
    }

    @Override
    public LtConfig loadLt(String logicalTable) {
        synchronized (ltCache) {
            Object[] hit = ltCache.get(logicalTable);
            if (hit != null && System.currentTimeMillis() - (Long) hit[0] < 60_000L) {
                return (LtConfig) hit[1];
            }
        }
        LtConfig cfg = new LtConfig();
        cfg.tableName = logicalTable;
        try (Connection c = open();
             PreparedStatement ps = c.prepareStatement(
                     "SELECT key_columns FROM logical_table WHERE table_name = ?")) {
            ps.setString(1, logicalTable);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    String kc = rs.getString("key_columns");
                    if (kc != null && !kc.isBlank()) {
                        List<String> keys = new ArrayList<>();
                        for (com.fasterxml.jackson.databind.JsonNode n : Json.readTree(kc)) {
                            keys.add(n.asText());
                        }
                        cfg.keyColumns = keys;
                    }
                }
            }
        } catch (Exception e) {
            log.warn("[lake-task] 逻辑表 {} 配置读取失败, 使用默认(业务键为空): {}",
                    logicalTable, e.getMessage());
        }
        synchronized (ltCache) {
            ltCache.put(logicalTable, new Object[]{System.currentTimeMillis(), cfg});
        }
        return cfg;
    }

    @Override
    public void dlt(String logicalTable, String sourceTable, String topic, int partition, long offset,
                    String businessKey, String payload, String error) {
        try (Connection c = open();
             PreparedStatement ps = c.prepareStatement(
                     "INSERT INTO dlt_message (logical_table, source_table, topic, partition_id, offset_id, "
                             + "business_key, payload, error_msg, fail_count, status, first_fail_time, last_fail_time) "
                             + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, 'PENDING', NOW(), NOW())")) {
            ps.setString(1, logicalTable);
            ps.setString(2, sourceTable);
            ps.setString(3, topic);
            ps.setInt(4, partition);
            ps.setLong(5, offset);
            ps.setString(6, businessKey);
            ps.setString(7, payload);
            ps.setString(8, error.length() > 2000 ? error.substring(0, 2000) : error);
            ps.executeUpdate();
        } catch (Exception e) {
            // DLT 落库失败仅告警(与 KafkaIngestEngine 一致, 死信路径不阻断主链路)
            log.error("[lake-task] DLT 落库失败: {}", e.getMessage());
        }
    }

    @Override
    public void close() {
        // 无长连接可释放(每操作独立连接)
    }
}
