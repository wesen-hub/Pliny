package com.dcp.ingest.meta;

import com.dcp.ingest.model.Json;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 元库 JDBC 实现(手工组装, 无 Spring; 语义对齐 KafkaIngestEngine + SeqAllocator + LogicalTableConfigService)。
 *
 * <p>连接策略: 每操作独立连接(领号=批级一次、DLT=异常路径、配置读带缓存)——入站主链路
 * 吞吐由底座分区并发承担, 单实例批量语义下开销可接受, 避免自建连接池的复杂度。
 */
public class JdbcMetaStore implements MetaStore {

    private static final Logger log = LoggerFactory.getLogger(JdbcMetaStore.class);

    private final String url;
    private final String user;
    private final String password;
    /** 逻辑表配置缓存(TTL 60s; 与切流前 BucketMapService 缓存语义对齐, 扩容自动生效) */
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
                     "SELECT topic, bucket_count, partition_count, key_columns, bucket_map "
                             + "FROM logical_table WHERE table_name = ?")) {
            ps.setString(1, logicalTable);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    cfg.topic = rs.getString("topic");
                    cfg.bucketCount = rs.getInt("bucket_count");
                    cfg.partitionCount = rs.getInt("partition_count");
                    String kc = rs.getString("key_columns");
                    if (kc != null && !kc.isBlank()) {
                        cfg.keyColumns = mapStringList(Json.readTree(kc));
                    }
                    String bm = rs.getString("bucket_map");
                    if (bm != null && !bm.isBlank()) {
                        Map<Integer, Integer> map = new LinkedHashMap<>();
                        Json.readTree(bm).fields().forEachRemaining(e ->
                                map.put(Integer.valueOf(e.getKey()),
                                        Integer.valueOf(String.valueOf(e.getValue().asText()))));
                        cfg.bucketMap = map;
                    }
                }
            }
        } catch (Exception e) {
            log.warn("[ingest-task] 逻辑表 {} 配置读取失败, 使用默认(桶={} 分区={}): {}",
                    logicalTable, cfg.bucketCount, cfg.partitionCount, e.getMessage());
        }
        synchronized (ltCache) {
            ltCache.put(logicalTable, new Object[]{System.currentTimeMillis(), cfg});
        }
        return cfg;
    }

    private static List<String> mapStringList(com.fasterxml.jackson.databind.JsonNode arr) {
        java.util.ArrayList<String> out = new java.util.ArrayList<>();
        for (com.fasterxml.jackson.databind.JsonNode n : arr) {
            out.add(n.asText());
        }
        return out;
    }

    @Override
    public Map<String, String> loadColumns(String logicalTable) {
        Map<String, String> cols = new LinkedHashMap<>();
        try (Connection c = open();
             PreparedStatement ps = c.prepareStatement(
                     "SELECT fields FROM model_definition WHERE table_name = ? ORDER BY id DESC LIMIT 1")) {
            ps.setString(1, logicalTable);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    String fieldsJson = rs.getString("fields");
                    if (fieldsJson != null) {
                        for (com.fasterxml.jackson.databind.JsonNode n : Json.readTree(fieldsJson)) {
                            cols.put(n.path("name").asText(), n.path("type").asText());
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.warn("[ingest-task] 模型字段解析失败 {}: {}", logicalTable, e.getMessage());
        }
        return cols;
    }

    @Override
    public long[] allocateSeq(String seqKey, int batch) {
        try (Connection c = open()) {
            int rows;
            try (PreparedStatement ps = c.prepareStatement(
                    "UPDATE seq_counter SET cur = cur + ? WHERE table_name = ?")) {
                ps.setInt(1, batch);
                ps.setString(2, seqKey);
                rows = ps.executeUpdate();
            }
            if (rows == 0) {
                try (PreparedStatement ps = c.prepareStatement(
                        "INSERT INTO seq_counter (table_name, cur) VALUES (?, ?)")) {
                    ps.setString(1, seqKey);
                    ps.setInt(2, batch);
                    ps.executeUpdate();
                }
            }
            try (PreparedStatement ps = c.prepareStatement(
                    "SELECT cur FROM seq_counter WHERE table_name = ?")) {
                ps.setString(1, seqKey);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    long end = rs.getLong("cur");
                    return new long[]{end - batch + 1, end};
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException("[ingest-task] 领号失败 seqKey=" + seqKey + ": " + e.getMessage(), e);
        }
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
            log.error("[ingest-task] DLT 落库失败: {}", e.getMessage());
        }
    }

    @Override
    public void close() {
        // 无长连接可释放(每操作独立连接)
    }
}
