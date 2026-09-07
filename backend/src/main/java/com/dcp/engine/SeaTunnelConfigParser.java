package com.dcp.engine;

import com.dcp.module.datasource.DsType;
import com.dcp.module.job.entity.ExtractConfig;
import com.dcp.module.job.entity.FieldMapping;
import com.dcp.module.job.entity.JobDefinition;
import com.dcp.module.job.entity.SyncState;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.stream.Collectors;

/**
 * 配置解析引擎（设计文档 §4.4 / §6）：任务配置 -> SeaTunnel JSON
 * <pre>
 * env:      job.mode BATCH/STREAMING, parallelism
 * source:   Jdbc (query = 抽取SQL, 增量已注入窗口过滤)
 * transform:Sql (direct + dictionary -> CASE WHEN) -> AviatorTransform (expression)
 * sink:     Jdbc (REPLACE INTO 幂等)
 * </pre>
 */
@Slf4j
public class SeaTunnelConfigParser {

    public static final String SOURCE_TABLE = "source_table";
    public static final String DIRECT_TABLE = "direct_data";
    public static final String OUTPUT_TABLE = "output_data";

    /** 生成 SeaTunnel 提交配置 */
    public static Map<String, Object> build(JobDefinition job, ExtractConfig extract,
                                            List<FieldMapping> mappings,
                                            Map<String, String> sourceConn,
                                            Map<String, String> targetConn,
                                            SyncState syncState,
                                            Map<String, Map<String, String>> dictMaps,
                                            String jobId) {
        return build(job, extract, mappings, sourceConn, targetConn, syncState, dictMaps, jobId, null);
    }

    /** 生成 SeaTunnel 提交配置（mongoSchema: Mongo 源字段类型推断结果, JDBC 源传 null） */
    public static Map<String, Object> build(JobDefinition job, ExtractConfig extract,
                                            List<FieldMapping> mappings,
                                            Map<String, String> sourceConn,
                                            Map<String, String> targetConn,
                                            SyncState syncState,
                                            Map<String, Map<String, String>> dictMaps,
                                            String jobId,
                                            Map<String, String> mongoSchema) {
        DsType st = DsType.of(sourceConn.get("type"));
        DsType tt = DsType.of(targetConn.get("type"));
        if ("cdc".equalsIgnoreCase(job.getSyncMode())) {
            // CDC: MySQL-CDC 直连, 不经过 transform（保留 RowKind）, 见 buildCdcConfig
            return buildCdcConfig(job, extract, mappings, sourceConn, targetConn);
        }

        // 抽取 SQL（增量窗口由平台探测后注入, 见 ExecutionRunner）
        String query = buildExtractSql(job, extract, st);

        Map<String, Object> env = new LinkedHashMap<>();
        env.put("job.mode", "BATCH");
        env.put("job.name", job.getJobName());
        env.put("parallelism", job.getParallelism() == null ? 2 : job.getParallelism());
        env.put("checkpoint.interval", 60000);

        // source
        Map<String, Object> source = new LinkedHashMap<>();
        if (st == DsType.MONGODB) {
            // MongoDB source: schema 必填, 类型由平台采样推断（MongoSchemaUtil）
            source.put("plugin_name", "MongoDB");
            source.put("uri", mongoUri(sourceConn));
            source.put("database", mongoDatabase(sourceConn.get("url")));
            source.put("collection", extract.getContent().trim());
            source.put("schema", Map.of("fields", mongoSchema == null ? Map.of() : mongoSchema));
            source.put("result_table_name", SOURCE_TABLE);
        } else {
            source.put("plugin_name", "Jdbc");
            source.put("url", normalizeJdbcUrl(st, sourceConn.get("url")));
            source.put("driver", jdbcDriver(st));
            source.put("user", sourceConn.get("username"));
            source.put("password", sourceConn.get("password"));
            source.put("query", query);
            source.put("fetch_size", 1000);
            source.put("connection_check_timeout_sec", 30);
            source.put("result_table_name", SOURCE_TABLE);
        }

        // transform: AviatorTransform 承载完整转换链（direct/dictionary/expression, §5.4/§6.2）
        List<Map<String, Object>> transforms = new ArrayList<>();
        List<FieldMapping> sorted = mappings.stream()
                .sorted(Comparator.comparing(m -> m.getSortNo() == null ? 0 : m.getSortNo())).toList();
        {
            Map<String, Object> aviatorT = new LinkedHashMap<>();
            aviatorT.put("plugin_name", "AviatorTransform");
            aviatorT.put("source_table_name", SOURCE_TABLE);
            aviatorT.put("result_table_name", OUTPUT_TABLE);
            // 嵌套结构在 Zeta LogicalVertex 序列化时会失败, 故映射/字典以 JSON 字符串承载
            aviatorT.put("mapping_rules", toJson(Map.of("mappings", sorted.stream().map(m -> {
                Map<String, Object> mm = new LinkedHashMap<>();
                mm.put("sourceField", m.getSourceField());
                mm.put("targetField", m.getTargetField());
                mm.put("transformType", m.getTransformType() == null ? "direct" : m.getTransformType());
                if (m.getTransformRule() != null) {
                    mm.put("transformRule", m.getTransformRule());
                }
                mm.put("sortNo", m.getSortNo() == null ? 0 : m.getSortNo());
                return mm;
            }).toList())));
            aviatorT.put("dict_data", toJson(Map.of("data", dictMaps == null ? Map.of() : dictMaps)));
            aviatorT.put("fail_on_error", false);
            transforms.add(aviatorT);
        }

        // sink
        Map<String, Object> sink = new LinkedHashMap<>();
        sink.put("plugin_name", "Jdbc");
        sink.put("url", normalizeJdbcUrl(tt, targetConn.get("url")));
        sink.put("driver", jdbcDriver(tt));
        sink.put("user", targetConn.get("username"));
        sink.put("password", targetConn.get("password"));
        List<String> cols = sorted.stream().map(FieldMapping::getTargetField).toList();
        sink.put("query", tt.upsertSql(job.getTargetTableName(), cols));
        sink.put("batch_size", 1000);
        sink.put("source_table_name", OUTPUT_TABLE);

        Map<String, Object> root = new LinkedHashMap<>();
        root.put("env", env);
        root.put("source", List.of(source));
        root.put("transform", transforms);
        root.put("sink", List.of(sink));
        return root;
    }

    private static String toJson(Object o) {
        try {
            return new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(o);
        } catch (Exception e) {
            throw new IllegalStateException("序列化 SeaTunnel 配置失败: " + e.getMessage(), e);
        }
    }

    /**
     * CDC 配置（MySQL-CDC source -> Jdbc sink, 无 transform）
     * - source: MySQL-CDC, startup.mode=initial（先快照后增量）, 输出带 RowKind 的 SeaTunnelRow
     * - sink:   Jdbc generate_sink_sql=true, 按 RowKind 自动生成 INSERT/UPDATE/DELETE（不能走 query 的 REPLACE INTO）
     * - transform: 空列表（AviatorTransform 用 new SeaTunnelRow(outputNames.length) 重建行会丢 RowKind, CDC 直连）
     * - env:    job.mode=STREAMING（常驻）, 不做超时
     */
    public static Map<String, Object> buildCdcConfig(JobDefinition job, ExtractConfig extract,
                                                     List<FieldMapping> mappings,
                                                     Map<String, String> sourceConn,
                                                     Map<String, String> targetConn) {
        Map<String, Object> env = new LinkedHashMap<>();
        env.put("job.mode", "STREAMING");
        env.put("job.name", job.getJobName());
        env.put("parallelism", job.getParallelism() == null ? 1 : job.getParallelism());
        env.put("checkpoint.interval", 60000);

        // source: MySQL-CDC（2.3.12 重构版多表 CDC: 必填 url/username/password, table-names 与 table_pattern 二选一）
        String srcUrl = normalizeJdbcUrl(DsType.of(sourceConn.get("type")), sourceConn.get("url"));
        String srcDb = jdbcDatabase(srcUrl);
        String table = extract.getContent().trim();
        if (table.contains(".")) {
            // 兼容 content 里带库名前缀的写法
            table = table.substring(table.lastIndexOf('.') + 1);
        }
        Map<String, Object> source = new LinkedHashMap<>();
        source.put("plugin_name", "MySQL-CDC");
        source.put("url", srcUrl);
        source.put("username", sourceConn.get("username"));
        source.put("password", sourceConn.get("password"));
        source.put("database-names", List.of(srcDb));
        source.put("table-names", List.of(srcDb + "." + table));
        source.put("server-id", "5400-5404");
        source.put("startup.mode", "initial");
        source.put("result_table_name", SOURCE_TABLE);

        // 主键列: 取 sortNo=1 的映射 targetField（约定主键排在第一条）
        List<FieldMapping> sorted = mappings.stream()
                .sorted(Comparator.comparing(m -> m.getSortNo() == null ? 0 : m.getSortNo())).toList();
        String pk = sorted.stream()
                .filter(m -> m.getSortNo() != null && m.getSortNo() == 1)
                .map(FieldMapping::getTargetField)
                .findFirst()
                .orElseGet(() -> {
                    String inc = job.getIncrementalField();
                    return sorted.stream()
                            .filter(m -> inc != null && inc.equalsIgnoreCase(m.getSourceField()))
                            .map(FieldMapping::getTargetField)
                            .findFirst()
                            .orElse(sorted.isEmpty() ? "" : sorted.get(0).getTargetField());
                });

        // sink: Jdbc 多表模式（generate_sink_sql + primary_keys + database/table）
        DsType tt = DsType.of(targetConn.get("type"));
        Map<String, Object> sink = new LinkedHashMap<>();
        sink.put("plugin_name", "Jdbc");
        sink.put("url", normalizeJdbcUrl(tt, targetConn.get("url")));
        sink.put("driver", jdbcDriver(tt));
        sink.put("user", targetConn.get("username"));
        sink.put("password", targetConn.get("password"));
        sink.put("generate_sink_sql", true);
        sink.put("primary_keys", List.of(pk));
        sink.put("database", jdbcDatabase(targetConn.get("url")));
        sink.put("table", job.getTargetTableName());
        sink.put("batch_size", 1000);
        sink.put("source_table_name", SOURCE_TABLE);

        Map<String, Object> root = new LinkedHashMap<>();
        root.put("env", env);
        root.put("source", List.of(source));
        root.put("transform", List.of());
        root.put("sink", List.of(sink));
        return root;
    }

    /** 从 JDBC URL 解析 host（jdbc:mysql://host:port/db?x=y） */
    public static String jdbcHost(String url) {
        String hostPort = jdbcHostPort(url);
        int colon = hostPort.indexOf(':');
        return colon >= 0 ? hostPort.substring(0, colon) : hostPort;
    }

    /** 从 JDBC URL 解析 port */
    public static int jdbcPort(String url) {
        String hostPort = jdbcHostPort(url);
        int colon = hostPort.indexOf(':');
        if (colon >= 0) {
            try {
                return Integer.parseInt(hostPort.substring(colon + 1));
            } catch (NumberFormatException ignored) {
                // fallthrough
            }
        }
        return 3306;
    }

    private static String jdbcHostPort(String url) {
        if (url == null) {
            return "";
        }
        String rest = url;
        int scheme = rest.indexOf("://");
        if (scheme >= 0) {
            rest = rest.substring(scheme + 3);
        }
        int slash = rest.indexOf('/');
        return slash >= 0 ? rest.substring(0, slash) : rest;
    }

    /** 从 JDBC URL 解析库名（jdbc:mysql://host:port/db?x=y -> db） */
    public static String jdbcDatabase(String url) {
        if (url == null) {
            return "";
        }
        String rest = url;
        int scheme = rest.indexOf("://");
        if (scheme >= 0) {
            rest = rest.substring(scheme + 3);
        }
        int slash = rest.indexOf('/');
        if (slash < 0) {
            return "";
        }
        String db = rest.substring(slash + 1);
        int q = db.indexOf('?');
        return q >= 0 ? db.substring(0, q) : db;
    }

    /** Mongo URI（含认证, 供 connector 使用）: mongodb://user:pass@host:port/db?authSource=admin&serverSelectionTimeoutMS=10000 */
    public static String mongoUri(Map<String, String> conn) {
        String url = conn.get("url");
        String user = conn.get("username");
        String pass = conn.get("password");
        String base = url == null ? "" : url.replace("mongodb://", "");
        int slash = base.indexOf('/');
        String hostPort = slash >= 0 ? base.substring(0, slash) : base;
        String db = slash >= 0 ? base.substring(slash + 1) : "";
        String auth = (user == null || user.isBlank()) ? "" : user + ":" + pass + "@";
        return "mongodb://" + auth + hostPort
                + (db.isBlank() ? "" : "/" + db) + "?authSource=admin&serverSelectionTimeoutMS=10000";
    }

    /** 从连接 URL 提取 Mongo 库名 */
    public static String mongoDatabase(String url) {
        if (url == null) {
            return "";
        }
        String base = url.replace("mongodb://", "");
        int slash = base.indexOf('/');
        if (slash < 0) {
            return "";
        }
        String db = base.substring(slash + 1);
        int q = db.indexOf('?');
        return q >= 0 ? db.substring(0, q) : db;
    }

    /** MySQL 8 需要 allowPublicKeyRetrieval（caching_sha2_password）; URL 上自动补全 */
    public static String normalizeJdbcUrl(DsType t, String url) {
        if (url == null) {
            return url;
        }
        if (t == DsType.MYSQL && !url.contains("allowPublicKeyRetrieval")) {
            return url + (url.contains("?") ? "&" : "?") + "allowPublicKeyRetrieval=true";
        }
        return url;
    }

    /** 驱动类名（按数据库类型; 需要时扩展） */
    public static String jdbcDriver(DsType t) {
        return switch (t) {
            case MYSQL -> "com.mysql.cj.jdbc.Driver";
            case POSTGRESQL -> "org.postgresql.Driver";
            case ORACLE -> "oracle.jdbc.OracleDriver";
            case SQLSERVER -> "com.microsoft.sqlserver.jdbc.SQLServerDriver";
            default -> "com.mysql.cj.jdbc.Driver";
        };
    }

    /** 抽取 SQL: 表模式 / SQL 模式（增量窗口在外部注入, 这里直接用传入 SQL） */
    public static String buildExtractSql(JobDefinition job, ExtractConfig extract, DsType st) {
        String content = extract.getContent().trim();
        if ("table".equalsIgnoreCase(extract.getSourceType())) {
            return "SELECT * FROM " + st.quote(content);
        }
        if (!content.toLowerCase().startsWith("select")) {
            throw new com.dcp.common.BizException(com.dcp.common.ErrorCode.SQL_VALIDATE_FAIL,
                    "抽取 SQL 必须以 SELECT 开头");
        }
        return content;
    }

    /** Sql Transform: direct 重命名 + dictionary CASE WHEN（§5.4 生成规则） */
    public static String buildSqlTransformQuery(DsType st, List<FieldMapping> mappings,
                                                Map<String, Map<String, String>> dictMaps) {
        List<String> selectItems = new ArrayList<>();
        for (FieldMapping m : mappings) {
            String src = st.quote(m.getSourceField());
            String tgt = st.quote(m.getTargetField());
            switch (m.getTransformType() == null ? "direct" : m.getTransformType()) {
                case "dictionary" -> {
                    Map<String, String> dict = dictMaps == null ? null : dictMaps.get(m.getTransformRule());
                    if (dict == null || dict.isEmpty()) {
                        selectItems.add(src + " AS " + tgt);
                    } else {
                        StringBuilder sb = new StringBuilder("CASE ");
                        dict.forEach((k, v) -> sb.append("WHEN ").append(src).append(" = ")
                                .append(sqlLiteral(k)).append(" THEN ").append(sqlLiteral(v)).append(' '));
                        sb.append("ELSE ").append(src).append(" END AS ").append(tgt);
                        selectItems.add(sb.toString());
                    }
                }
                case "expression" -> {
                    // 表达式由 AviatorTransform 处理, Sql 阶段原样透传
                    selectItems.add(src + " AS " + tgt);
                }
                default -> selectItems.add(src + " AS " + tgt);
            }
        }
        return "SELECT " + String.join(", ", selectItems) + " FROM " + SOURCE_TABLE;
    }

    private static String sqlLiteral(String v) {
        return "'" + v.replace("'", "''") + "'";
    }
}
