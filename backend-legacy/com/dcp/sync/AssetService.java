package com.dcp.sync;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.*;

/**
 * 数据资产目录: 逻辑表(logical_table) + 来源(cdc_st_task/datasource_config)
 * + 目标分表(Schema/预览) 组装成资产视图。
 * 前端"数据资产"三个页面(Catalog/Detail/Lineage)真实数据源。
 */
@Service
public class AssetService {
    private static final Logger log = LoggerFactory.getLogger(AssetService.class);
    private final JdbcTemplate metaJdbc;
    private final JdbcTemplate targetJdbc;
    private final ObjectMapper om = new ObjectMapper();

    public AssetService(JdbcTemplate jdbcTemplate,
                        @Value("${dcp.sync.target.url}") String targetUrl,
                        @Value("${dcp.sync.target.username:root}") String targetUser,
                        @Value("${dcp.sync.target.password:root123}") String targetPassword) {
        this.metaJdbc = jdbcTemplate;
        // 目标库连接内部自建(不暴露 DataSource bean, 避免干扰元库自动配置)
        org.springframework.jdbc.datasource.DriverManagerDataSource ds =
                new org.springframework.jdbc.datasource.DriverManagerDataSource();
        ds.setDriverClassName("com.mysql.cj.jdbc.Driver");
        ds.setUrl(targetUrl);
        ds.setUsername(targetUser);
        ds.setPassword(targetPassword);
        this.targetJdbc = new JdbcTemplate(ds);
    }

    /** 资产列表: 每个逻辑表一条, 含来源/敏感标记 */
    public List<Map<String, Object>> list() {
        List<Map<String, Object>> out = new ArrayList<>();
        List<Map<String, Object>> tables = metaJdbc.queryForList(
                "SELECT table_name, topic, bucket_count, table_count, table_prefix, partition_count, updated_at " +
                        "FROM logical_table ORDER BY table_name");
        for (Map<String, Object> t : tables) {
            String table = String.valueOf(t.get("table_name"));
            List<Map<String, Object>> sources = sourcesOf(table);
            boolean sensitive = fieldsOf(table).stream().anyMatch(f -> Boolean.TRUE.equals(f.get("sensitive")));
            int sc = shardCount(table, t.get("table_prefix") == null ? table : String.valueOf(t.get("table_prefix")));
            Map<String, Object> a = new LinkedHashMap<>();
            a.put("table", table);
            a.put("name", table);
            a.put("topic", t.get("topic"));
            a.put("sources", sources);
            a.put("multiSource", sources.size() > 1);
            a.put("bucketCount", t.get("bucket_count"));
            a.put("tableCount", t.get("table_count"));
            a.put("partitionCount", t.get("partition_count"));
            a.put("updated", t.get("updated_at"));
            a.put("status", sc > 0 ? "就绪" : "未采集");
            a.put("sensitive", sensitive);
            a.put("shardCount", sc);
            out.add(a);
        }
        return out;
    }

    /** 资产详情: 列表信息 + 字段 Schema(映射+类型) + 分片数 */
    public Map<String, Object> detail(String table) {
        List<Map<String, Object>> all = list();
        Map<String, Object> a = all.stream().filter(x -> table.equals(x.get("table"))).findFirst().orElse(null);
        if (a == null) return null;
        a.put("fields", fieldsOf(table));
        return a;
    }

    /** 字段 Schema: cdc_st_task.field_mapping 合并 + 目标分表列类型 */
    public List<Map<String, Object>> fieldsOf(String table) {
        Map<String, Map<String, Object>> byModel = new LinkedHashMap<>();
        List<Map<String, Object>> tasks = metaJdbc.queryForList(
                "SELECT source_db, source_table, field_mapping FROM cdc_st_task WHERE logical_table = ?", table);
        for (Map<String, Object> task : tasks) {
            String srcDb = String.valueOf(task.get("source_db"));
            String srcTable = String.valueOf(task.get("source_table"));
            JsonNode fm = parseJson(task.get("field_mapping"));
            if (fm == null || !fm.isArray()) continue;
            for (JsonNode f : fm) {
                String model = f.path("modelField").asText(null);
                JsonNode src = f.path("source");
                String kind = src.path("kind").asText("");
                String srcField = src.path("srcField").asText("");
                String srcTbl = src.path("srcTable").asText("");
                if (model == null) continue;
                List<String> stepNames = new ArrayList<>();
                boolean sensitive = false;
                for (JsonNode step : f.path("steps")) {
                    String k = step.path("kind").asText("");
                    switch (k) {
                        case "mask" -> { stepNames.add("脱敏"); sensitive = true; }
                        case "trim" -> stepNames.add("去空格");
                        case "enum" -> stepNames.add("枚举映射");
                        case "expression" -> stepNames.add("表达式");
                        default -> stepNames.add(k);
                    }
                }
                String transform = stepNames.isEmpty() ? "直接映射" : String.join("+", stepNames);
                String srcQualified = "系统生成".equals(kind) ? "系统生成" :
                        (srcTbl.isEmpty() ? srcTable : srcTbl) + "." + srcField;
                Map<String, Object> fld = byModel.computeIfAbsent(model, k -> {
                    Map<String, Object> m = new LinkedHashMap<>();
                    m.put("name", k);
                    m.put("source", srcQualified);
                    m.put("transform", transform);
                    m.put("sensitive", false);
                    m.put("nullable", "");
                    m.put("type", "");
                    m.put("srcTables", new LinkedHashSet<String>());
                    return m;
                });
                fld.put("source", srcQualified);
                fld.put("transform", transform);
                if (sensitive) fld.put("sensitive", true);
                ((Set<String>) fld.get("srcTables")).add(srcQualified);
            }
        }
        // 补充目标分表列类型
        Map<String, String[]> colInfo = shardColumnInfo(table);
        for (Map<String, Object> f : byModel.values()) {
            String[] info = colInfo.get(String.valueOf(f.get("name")));
            f.put("type", info == null ? "" : info[0]);
            f.put("nullable", info != null && "YES".equals(info[1]) ? "是" : "否");
        }
        return new ArrayList<>(byModel.values());
    }

    /** 数据预览: 目标分表真实数据(任一存在分片) */
    public Map<String, Object> preview(String table, int limit) {
        String shard = anyShard(table);
        Map<String, Object> out = new LinkedHashMap<>();
        if (shard == null) {
            out.put("columns", List.of());
            out.put("rows", List.of());
            out.put("shard", null);
            return out;
        }
        List<Map<String, Object>> rows = targetJdbc.queryForList(
                "SELECT * FROM " + shard + " ORDER BY seq DESC LIMIT " + Math.min(Math.max(limit, 1), 50));
        List<String> cols = rows.isEmpty() ? shardColumns(shard) : new ArrayList<>(rows.get(0).keySet());
        out.put("columns", cols);
        out.put("rows", rows);
        out.put("shard", shard);
        return out;
    }

    /** 字段级血缘: 模型字段 ← 源字段(含转换步骤) */
    public List<Map<String, Object>> lineage(String table) {
        List<Map<String, Object>> fields = fieldsOf(table);
        List<Map<String, Object>> out = new ArrayList<>();
        for (Map<String, Object> f : fields) {
            if ("系统生成".equals(f.get("source"))) continue;
            String[] parts = String.valueOf(f.get("source")).split("\\.", 2);
            Map<String, Object> e = new LinkedHashMap<>();
            e.put("modelField", f.get("name"));
            e.put("srcTable", parts.length > 1 ? parts[0] : "");
            e.put("srcField", parts.length > 1 ? parts[1] : f.get("source"));
            e.put("transform", f.get("transform"));
            e.put("sensitive", f.get("sensitive"));
            out.add(e);
        }
        return out;
    }

    // ---- 内部 ----

    private List<Map<String, Object>> sourcesOf(String logicalTable) {
        List<Map<String, Object>> out = new ArrayList<>();
        List<Map<String, Object>> tasks = metaJdbc.queryForList(
                "SELECT t.datasource_id, t.source_db, t.source_table, d.name AS ds_name, d.type AS ds_type " +
                        "FROM cdc_st_task t LEFT JOIN datasource_config d ON d.id = t.datasource_id " +
                        "WHERE t.logical_table = ? ORDER BY t.task_id", logicalTable);
        for (Map<String, Object> t : tasks) {
            Map<String, Object> s = new LinkedHashMap<>();
            s.put("db", t.get("source_db"));
            s.put("table", t.get("source_table"));
            s.put("dsName", t.get("ds_name"));
            s.put("dsType", t.get("ds_type"));
            out.add(s);
        }
        return out;
    }

    private JsonNode parseJson(Object o) {
        if (o == null) return null;
        try {
            return om.readTree(o instanceof String ? (String) o : om.writeValueAsString(o));
        } catch (Exception e) {
            return null;
        }
    }

    private String anyShard(String table) {
        List<String> names = targetJdbc.queryForList(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name LIKE ? LIMIT 1",
                String.class, table + "\\_%");
        return names.isEmpty() ? null : names.get(0);
    }

    private int shardCount(String table, String prefix) {
        try {
            Integer n = targetJdbc.queryForObject(
                    "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name LIKE ?",
                    Integer.class, prefix + "\\_%");
            return n == null ? 0 : n;
        } catch (Exception e) {
            return 0;
        }
    }

    private List<String> shardColumns(String shard) {
        try {
            return targetJdbc.queryForList(
                    "SELECT column_name FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = ? ORDER BY ordinal_position",
                    String.class, shard);
        } catch (Exception e) {
            return List.of();
        }
    }

    /** 目标分表列信息 map(列名→[类型, 可空]), 从任一存在分片读取 */
    private Map<String, String[]> shardColumnInfo(String table) {
        Map<String, String[]> map = new HashMap<>();
        String shard = anyShard(table);
        if (shard == null) return map;
        try {
            targetJdbc.query("SELECT column_name, column_type, is_nullable FROM information_schema.columns " +
                            "WHERE table_schema = DATABASE() AND table_name = ? ORDER BY ordinal_position",
                    rs -> {
                        map.put(rs.getString("column_name"),
                                new String[]{rs.getString("column_type"), rs.getString("is_nullable")});
                    }, shard);
        } catch (Exception e) {
            log.warn("shardColumnInfo failed: {}", e.getMessage());
        }
        return map;
    }
}
