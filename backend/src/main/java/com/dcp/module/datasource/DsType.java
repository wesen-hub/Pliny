package com.dcp.module.datasource;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;

import java.util.List;
import java.util.Map;

/** 数据源类型与方言（设计文档 §5.1 / §8 方言适配层 K8） */
@Getter
public enum DsType {
    MYSQL("mysql", "com.mysql.cj.jdbc.Driver", 3306),
    POSTGRESQL("postgresql", "org.postgresql.Driver", 5432),
    ORACLE("oracle", "oracle.jdbc.OracleDriver", 1521),
    SQLSERVER("sqlserver", "com.microsoft.sqlserver.jdbc.SQLServerDriver", 1433),
    MONGODB("mongodb", "", 27017),
    S3("s3", "", 9000),
    LOCAL("local", "", 0);

    private final String code;
    private final String driverClass;
    private final int defaultPort;

    DsType(String code, String driverClass, int defaultPort) {
        this.code = code;
        this.driverClass = driverClass;
        this.defaultPort = defaultPort;
    }

    public static DsType of(String code) {
        for (DsType t : values()) {
            if (t.code.equalsIgnoreCase(code)) {
                return t;
            }
        }
        throw new IllegalArgumentException("不支持的数据源类型: " + code);
    }

    /** 构建 JDBC URL（extraParams 中的键值会追加为查询参数） */
    public String buildJdbcUrl(String host, int port, String db, String extraParams) {
        String url = switch (this) {
            case MYSQL -> "jdbc:mysql://%s:%d/%s".formatted(host, port, db)
                    + "?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Asia/Shanghai"
                    + "&characterEncoding=utf8&rewriteBatchedStatements=true";
            case POSTGRESQL -> "jdbc:postgresql://%s:%d/%s".formatted(host, port, db);
            case ORACLE -> "jdbc:oracle:thin:@//%s:%d/%s".formatted(host, port, db);
            case SQLSERVER -> "jdbc:sqlserver://%s:%d;databaseName=%s;encrypt=false".formatted(host, port, db);
            case MONGODB -> "mongodb://%s:%d/%s".formatted(host, port, db);
            // 文件类源(S3/本地)无 JDBC
            case S3, LOCAL -> throw new IllegalArgumentException("文件类数据源不支持 JDBC: " + this);
        };
        Map<String, String> extra = parseExtraParams(extraParams);
        if (!extra.isEmpty()) {
            StringBuilder sb = new StringBuilder(url);
            for (Map.Entry<String, String> e : extra.entrySet()) {
                sb.append(url.contains("?") ? "&" : "?").append(e.getKey()).append("=").append(e.getValue());
            }
            url = sb.toString();
        }
        return url;
    }

    public static Map<String, String> parseExtraParams(String extraParams) {
        if (extraParams == null || extraParams.isBlank()) {
            return Map.of();
        }
        try {
            ObjectMapper om = new ObjectMapper();
            return om.readValue(extraParams, new TypeReference<Map<String, String>>() {
            });
        } catch (Exception e) {
            return Map.of();
        }
    }

    /** 标识符包裹 */
    public String quote(String identifier) {
        if (identifier == null || identifier.isBlank()) {
            return identifier;
        }
        return switch (this) {
            case MYSQL -> "`" + identifier.replace("`", "``") + "`";
            default -> "\"" + identifier.replace("\"", "\"\"") + "\"";
        };
    }

    /** 预览分页包装 */
    public String wrapPagination(String sql, int limit) {
        String s = sql.trim().endsWith(";") ? sql.trim().substring(0, sql.trim().length() - 1) : sql.trim();
        return switch (this) {
            case MYSQL, POSTGRESQL -> "SELECT * FROM (%s) _p LIMIT %d".formatted(s, limit);
            case SQLSERVER -> "SELECT TOP %d * FROM (%s) _p".formatted(limit, s);
            case ORACLE -> "SELECT * FROM (%s) _p WHERE ROWNUM <= %d".formatted(s, limit);
            case MONGODB -> s;
            case S3, LOCAL -> s;
        };
    }

    /** 目标端幂等写入 SQL（K2：at-least-once + 幂等） */
    public String upsertSql(String table, List<String> columns) {
        String cols = String.join(", ", columns);
        String placeholders = String.join(", ", columns.stream().map(c -> "?").toList());
        String quotedTable = quote(table);
        return switch (this) {
            case MYSQL -> "REPLACE INTO %s (%s) VALUES (%s)".formatted(quotedTable, cols, placeholders);
            case POSTGRESQL -> "INSERT INTO %s (%s) VALUES (%s) ON CONFLICT DO NOTHING".formatted(quotedTable, cols, placeholders);
            default -> "INSERT INTO %s (%s) VALUES (%s)".formatted(quotedTable, cols, placeholders);
        };
    }

    /** 增量过滤条件包裹：对自定义SQL外层包子查询（§4.2）；withLower=false 为首跑全量（只有上界） */
    public String wrapIncremental(String sql, String incField, boolean withLower) {
        String s = sql.trim().endsWith(";") ? sql.trim().substring(0, sql.trim().length() - 1) : sql.trim();
        String f = quote(incField);
        String inner = "SELECT * FROM (%s) _t".formatted(s);
        return withLower
                ? inner + " WHERE %s > ? AND %s <= ?".formatted(f, f)
                : inner + " WHERE %s <= ?".formatted(f);
    }
}
