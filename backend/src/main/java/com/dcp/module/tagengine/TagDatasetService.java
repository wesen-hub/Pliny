package com.dcp.module.tagengine;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.stereotype.Component;

import java.util.*;

/**
 * 打标引擎 V2 —— 数据集字段元数据(逻辑表 ↔ 真实物理/权威表列):
 *
 * <p>设计: 数据集 = 逻辑表(逻辑表的 Iceberg 湖表为生产真相, 湖落地外部推进);
 * 本服务当前按"权威副本/分片物理表"代理真实 schema(Iceberg 就绪后切到湖 schema 读取, 接口不变)。
 *
 * <pre>
 *   GET /api/tagengine/datasets                  逻辑表列表(元库 logical_table)
 *   GET /api/tagengine/datasets/{name}/fields    真实字段 + 类型分类(L/D/B/S) + 主键候选
 * </pre>
 *
 * 解析策略: 目标库中按 table_prefix 找 {prefix}_authority(权威副本, 单表全量列) 或分片 {prefix}_00;
 * 列类型按打标分类: bigint/int/... → L(long), decimal/double/float → D, tinyint(1)/bit → B(boolean), 其余 → S(string, 含日期时间)。
 */
@Slf4j
@Component
public class TagDatasetService {

    private final JdbcTemplate metaJdbc;
    private final String targetUrl;
    private final String targetUser;
    private final String targetPassword;
    private volatile JdbcTemplate targetJdbc;

    public TagDatasetService(JdbcTemplate metaJdbc,
                             @Value("${dcp.sync.target.url}") String targetUrl,
                             @Value("${dcp.sync.target.username:root}") String username,
                             @Value("${dcp.sync.target.password:root123}") String password) {
        this.metaJdbc = metaJdbc;
        this.targetUrl = targetUrl;
        this.targetUser = username;
        this.targetPassword = password;
    }

    private JdbcTemplate target() {
        if (targetJdbc == null) {
            synchronized (this) {
                if (targetJdbc == null) {
                    DriverManagerDataSource ds = new DriverManagerDataSource();
                    ds.setDriverClassName("com.mysql.cj.jdbc.Driver");
                    ds.setUrl(targetUrl);
                    ds.setUsername(targetUser);
                    ds.setPassword(targetPassword);
                    targetJdbc = new JdbcTemplate(ds);
                }
            }
        }
        return targetJdbc;
    }

    /** 逻辑表列表(元库 logical_table) */
    public List<Map<String, Object>> listDatasets() {
        return metaJdbc.query("SELECT table_name, table_prefix, table_count, bucket_count FROM logical_table ORDER BY table_name",
                (rs, i) -> {
                    Map<String, Object> m = new LinkedHashMap<>();
                    m.put("tableName", rs.getString("table_name"));
                    m.put("tablePrefix", rs.getString("table_prefix"));
                    m.put("shards", rs.getInt("table_count"));
                    return m;
                });
    }

    /** 逻辑表 → 真实字段元数据; 实体键候选优先业务键(business_key/key_columns), 其次 PRI */
    public Map<String, Object> fieldsOf(String logicalTable) {
        Map<String, Object> cfg = metaJdbc.query(
                "SELECT table_prefix, business_key, key_columns FROM logical_table WHERE table_name=?",
                rs -> {
                    if (!rs.next()) return null;
                    Map<String, Object> m = new LinkedHashMap<>();
                    m.put("prefix", rs.getString("table_prefix"));
                    m.put("businessKey", rs.getString("business_key"));
                    m.put("keyColumns", rs.getString("key_columns"));
                    return m;
                }, logicalTable);
        String prefix = cfg == null ? logicalTable : (String) cfg.get("prefix");
        String foundTable = findTargetTable(prefix);
        if (foundTable == null) {
            throw new IllegalArgumentException("目标库未找到逻辑表「" + logicalTable + "」的物理表(prefix=" + prefix
                    + "): 权威表/分片表不存在 —— 请先跑同步或确认目标库(SYNC_TARGET_URL)");
        }
        List<Map<String, Object>> cols = target().query(
                "SELECT COLUMN_NAME, DATA_TYPE, COLUMN_KEY FROM information_schema.COLUMNS"
                        + " WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION",
                (rs, i) -> {
                    Map<String, Object> m = new LinkedHashMap<>();
                    m.put("name", rs.getString("COLUMN_NAME"));
                    m.put("rawType", rs.getString("DATA_TYPE"));
                    m.put("key", rs.getString("COLUMN_KEY"));
                    m.put("kind", classify(rs.getString("DATA_TYPE")));
                    return m;
                }, foundTable);
        if (cols.isEmpty()) {
            throw new IllegalArgumentException("目标表 " + foundTable + " 无列信息");
        }
        // 实体键候选: 业务键(business_key.keyFields/scopeFields, key_columns) > 真实主键 > 常见 id
        Set<String> pk = new LinkedHashSet<>();
        for (String raw : List.of(cfg == null ? null : (String) cfg.get("businessKey"),
                cfg == null ? null : (String) cfg.get("keyColumns"))) {
            if (raw == null || raw.isBlank()) continue;
            try {
                com.fasterxml.jackson.databind.JsonNode n = new com.fasterxml.jackson.databind.ObjectMapper().readTree(raw);
                List<String> keys = new ArrayList<>();
                if (n.isArray()) n.forEach(x -> keys.add(x.asText()));
                else {
                    for (String f : List.of("keyFields", "scopeFields")) {
                        if (n.has(f) && n.get(f).isArray()) n.get(f).forEach(x -> keys.add(x.asText()));
                    }
                }
                keys.forEach(k -> { if (cols.stream().anyMatch(c -> c.get("name").equals(k))) pk.add(k); });
            } catch (Exception ignore) {
                /* 解析失败忽略 */
            }
        }
        cols.forEach(c -> { if ("PRI".equals(c.get("key"))) pk.add((String) c.get("name")); });
        for (String c : List.of("id", logicalTable + "_id", "_pk_id")) {
            if (pk.isEmpty() && cols.stream().anyMatch(x -> x.get("name").equals(c))) pk.add(c);
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("logicalTable", logicalTable);
        out.put("resolvedTable", foundTable);
        out.put("fields", cols);
        out.put("pkCandidates", new ArrayList<>(pk));
        return out;
    }

    /** 候选表: 权威表优先, 其次分片 00(前缀可能即物理表名) */
    private String findTargetTable(String prefix) {
        List<String> cand = new ArrayList<>();
        cand.add(prefix + "_authority");
        String shard0 = RouteUtilLike.name00(prefix);
        if (shard0 != null) cand.add(shard0);
        cand.add(prefix);
        for (String t : cand) {
            Integer n = target().queryForObject(
                    "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?",
                    Integer.class, t);
            if (n != null && n > 0) return t;
        }
        return null;
    }

    /** MySQL 类型 → 打标分类(L/D/B/S) */
    private static String classify(String type) {
        return switch (type) {
            case "bigint", "int", "integer", "smallint", "mediumint", "tinyint" -> "L";
            case "decimal", "double", "float", "numeric" -> "D";
            case "bit" -> "B";
            default -> "S";   // char/varchar/text/date/datetime/timestamp/time/json → 字符串(规则可判)
        };
    }

    /** 分片命名与 RouteUtil 一致(t{prefix}_00 …); 简化零填充两位(与 RouteUtil.tableName 对齐, 见后端 RouteUtil) */
    private static final class RouteUtilLike {
        static String name00(String prefix) {
            // 与 com.dcp.sync.RouteUtil.tableName(prefix, 0, count) 一致: 00 起步两位
            return prefix + "_00";
        }
    }
}
