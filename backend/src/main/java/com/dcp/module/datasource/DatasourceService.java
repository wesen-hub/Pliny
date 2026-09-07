package com.dcp.module.datasource;

import com.dcp.common.BizException;
import com.dcp.common.ErrorCode;
import com.dcp.common.CryptoUtil;
import com.dcp.module.datasource.entity.DatasourceConfig;
import com.dcp.module.datasource.mapper.DatasourceConfigMapper;
import com.dcp.module.system.service.AuditService;
import com.dcp.module.job.entity.JobDefinition;
import com.dcp.module.job.mapper.JobDefinitionMapper;
import com.dcp.security.SecurityUtils;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
@RequiredArgsConstructor
public class DatasourceService {

    private final DatasourceConfigMapper datasourceMapper;
    private final JobDefinitionMapper jobMapper;
    private final AuditService auditService;

    @Value("${dcp.enc-key}")
    private String encKey;

    // ---------- CRUD ----------

    public Page<DatasourceConfig> page(int page, int pageSize, String type, String group, String env, String keyword) {
        LambdaQueryWrapper<DatasourceConfig> qw = new LambdaQueryWrapper<>();
        qw.eq(type != null && !type.isBlank(), DatasourceConfig::getType, type)
          .eq(group != null && !group.isBlank(), DatasourceConfig::getGroupName, group)
          .eq(env != null && !env.isBlank(), DatasourceConfig::getEnv, env)
          .like(keyword != null && !keyword.isBlank(), DatasourceConfig::getName, keyword)
          .orderByDesc(DatasourceConfig::getId);
        return datasourceMapper.selectPage(Page.of(page, pageSize), qw);
    }

    public DatasourceConfig get(Long id) {
        DatasourceConfig ds = datasourceMapper.selectById(id);
        if (ds == null) {
            throw new BizException(ErrorCode.NOT_FOUND, "数据源不存在: id=" + id);
        }
        return ds;
    }

    public Long create(DatasourceConfig ds, String plainPassword) {
        if (plainPassword == null || plainPassword.isBlank()) {
            throw new BizException(ErrorCode.PARAM_ERROR, "密码不能为空");
        }
        ds.setId(null);
        ds.setPasswordCiphertext(CryptoUtil.encrypt(plainPassword, encKey));
        ds.setKeyVersion(1);
        ds.setCreateBy(SecurityUtils.currentUsername());
        ds.setEnabled(ds.getEnabled() == null ? 1 : ds.getEnabled());
        datasourceMapper.insert(ds);
        auditService.record("datasource", "CREATE", "datasource", ds.getId(), "创建数据源: " + ds.getName());
        return ds.getId();
    }

    public void update(Long id, DatasourceConfig req, String plainPassword) {
        DatasourceConfig ds = get(id);
        req.setId(id);
        if (plainPassword != null && !plainPassword.isBlank()) {
            req.setPasswordCiphertext(CryptoUtil.encrypt(plainPassword, encKey));
        } else {
            req.setPasswordCiphertext(ds.getPasswordCiphertext());
        }
        req.setKeyVersion(ds.getKeyVersion());
        req.setCreateBy(null);
        req.setCreateTime(null);
        req.setUpdateBy(SecurityUtils.currentUsername());
        datasourceMapper.updateById(req);
        auditService.record("datasource", "UPDATE", "datasource", id, "更新数据源: " + req.getName());
    }

    public void delete(Long id) {
        DatasourceConfig ds = get(id);
        Long refs = jobMapper.selectCount(new LambdaQueryWrapper<JobDefinition>()
                .and(w -> w.eq(JobDefinition::getSourceDatasourceId, id)
                        .or().eq(JobDefinition::getTargetDatasourceId, id)));
        if (refs != null && refs > 0) {
            throw new BizException(ErrorCode.DS_REFERENCED, "数据源被 " + refs + " 个任务引用, 无法删除");
        }
        datasourceMapper.deleteById(id); // 逻辑删除
        auditService.record("datasource", "DELETE", "datasource", id, "删除数据源: " + ds.getName());
    }

    public void setEnabled(Long id, boolean enabled) {
        DatasourceConfig ds = get(id);
        ds.setEnabled(enabled ? 1 : 0);
        datasourceMapper.updateById(ds);
        auditService.record("datasource", enabled ? "ENABLE" : "DISABLE", "datasource", id, ds.getName());
    }

    /** 获取解密后的连接信息 */
    public Map<String, String> connectionInfo(DatasourceConfig ds) {
        DsType type = DsType.of(ds.getType());
        String password = CryptoUtil.decrypt(ds.getPasswordCiphertext(), encKey);
        return Map.of(
                "type", ds.getType(),
                "driver", type.getDriverClass(),
                "url", type.buildJdbcUrl(ds.getHost(), ds.getPort(), ds.getDatabaseName(), ds.getExtraParams()),
                "username", ds.getUsername(),
                "password", password
        );
    }

    // ---------- 测试连接 ----------

    public void testConnection(Long id) {
        DatasourceConfig ds = get(id);
        DsType type = DsType.of(ds.getType());
        if (type == DsType.MONGODB) {
            testMongo(ds);
            return;
        }
        Map<String, String> info = connectionInfo(ds);
        Properties props = new Properties();
        props.setProperty("user", info.get("username"));
        props.setProperty("password", info.get("password"));
        props.setProperty("connectTimeout", "5000");
        props.setProperty("socketTimeout", "10000");
        props.setProperty("loginTimeout", "5");
        try (Connection conn = DriverManager.getConnection(info.get("url"), props)) {
            try (Statement st = conn.createStatement()) {
                st.execute("SELECT 1");
            }
            ds.setLastHealthCheckTime(LocalDateTime.now());
            ds.setLastHealthCheckResult("OK");
            datasourceMapper.updateById(ds);
        } catch (Exception e) {
            log.warn("测试连接失败 ds={} err={}", ds.getName(), e.getMessage());
            throw new BizException(ErrorCode.DS_CONN_FAIL, "连接失败: " + friendly(e));
        }
    }

    private void testMongo(DatasourceConfig ds) {
        try (com.mongodb.client.MongoClient client = com.mongodb.client.MongoClients.create(
                mongoUri(ds))) {
            client.listDatabaseNames().first();
        } catch (Exception e) {
            throw new BizException(ErrorCode.DS_CONN_FAIL, "MongoDB 连接失败: " + e.getMessage());
        }
    }

    /** Mongo 连接串: 认证库显式 admin（root 等超级用户建在 admin; 业务库由 databaseName 指定） */
    private String mongoUri(DatasourceConfig ds) {
        return "mongodb://%s:%s@%s:%d/?authSource=admin&serverSelectionTimeoutMS=5000".formatted(
                ds.getUsername(), CryptoUtil.decrypt(ds.getPasswordCiphertext(), encKey),
                ds.getHost(), ds.getPort());
    }

    private String friendly(Exception e) {
        String m = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
        String low = m.toLowerCase();
        if (low.contains("access denied") || low.contains("authentication")) return "认证失败, 请检查用户名/密码";
        if (low.contains("unknown database") || low.contains("database") && low.contains("not exist")) return "数据库不存在";
        if (low.contains("connect") || low.contains("timeout") || low.contains("unreachable")) return "网络不可达或连接超时";
        return m;
    }

    // ---------- 元数据 ----------

    public List<Map<String, String>> listTables(Long id) {
        DatasourceConfig ds = get(id);
        DsType type = DsType.of(ds.getType());
        if (type == DsType.MONGODB) {
            return mongoCollections(ds);
        }
        Map<String, String> info = connectionInfo(ds);
        List<Map<String, String>> result = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection(info.get("url"), info.get("username"), info.get("password"))) {
            DatabaseMetaData meta = conn.getMetaData();
            try (ResultSet rs = meta.getTables(conn.getCatalog(), null, "%", new String[]{"TABLE"})) {
                while (rs.next()) {
                    Map<String, String> t = new LinkedHashMap<>();
                    t.put("name", rs.getString("TABLE_NAME"));
                    t.put("type", rs.getString("TABLE_TYPE"));
                    result.add(t);
                }
            }
        } catch (Exception e) {
            throw new BizException(ErrorCode.DS_META_FAIL, "拉取表清单失败: " + e.getMessage());
        }
        return result;
    }

    public List<Map<String, Object>> listColumns(Long id, String table) {
        DatasourceConfig ds = get(id);
        DsType type = DsType.of(ds.getType());
        if (type == DsType.MONGODB) {
            return mongoFields(ds, table);
        }
        Map<String, String> info = connectionInfo(ds);
        List<Map<String, Object>> result = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection(info.get("url"), info.get("username"), info.get("password"))) {
            DatabaseMetaData meta = conn.getMetaData();
            try (ResultSet rs = meta.getColumns(conn.getCatalog(), null, table, "%")) {
                while (rs.next()) {
                    Map<String, Object> c = new LinkedHashMap<>();
                    c.put("name", rs.getString("COLUMN_NAME"));
                    c.put("type", rs.getString("TYPE_NAME"));
                    c.put("size", rs.getInt("COLUMN_SIZE"));
                    c.put("nullable", rs.getInt("NULLABLE") != DatabaseMetaData.columnNoNulls);
                    c.put("primaryKey", false);
                    result.add(c);
                }
            }
            // 主键标记
            try (ResultSet pk = meta.getPrimaryKeys(conn.getCatalog(), null, table)) {
                while (pk.next()) {
                    String pkCol = pk.getString("COLUMN_NAME");
                    result.stream().filter(c -> pkCol.equals(c.get("name"))).forEach(c -> c.put("primaryKey", true));
                }
            }
        } catch (Exception e) {
            throw new BizException(ErrorCode.DS_META_FAIL, "拉取字段清单失败: " + e.getMessage());
        }
        return result;
    }

    // ---------- V3 库/Schema/表 级联浏览(按数据库类型分层) ----------

    /** 库层: MySQL/PG/SQLServer/Mongo 返回数据库列表; Oracle 返回 Schema(用户)列表 */
    public List<String> listDatabases(Long id) {
        DatasourceConfig ds = get(id);
        DsType type = DsType.of(ds.getType());
        if (type == DsType.MONGODB) {
            try (com.mongodb.client.MongoClient client = com.mongodb.client.MongoClients.create(mongoUri(ds))) {
                List<String> dbs = new ArrayList<>();
                client.listDatabaseNames().forEach(dbs::add);
                return dbs;
            } catch (Exception e) {
                throw new BizException(ErrorCode.DS_META_FAIL, "拉取数据库清单失败: " + e.getMessage());
            }
        }
        String sql = switch (type) {
            case MYSQL -> "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA "
                    + "WHERE SCHEMA_NAME NOT IN ('information_schema','mysql','performance_schema','sys') ORDER BY SCHEMA_NAME";
            case POSTGRESQL -> "SELECT datname FROM pg_database WHERE NOT datistemplate ORDER BY datname";
            case SQLSERVER -> "SELECT name FROM sys.databases ORDER BY name";
            case ORACLE -> "SELECT username FROM all_users "
                    + "WHERE username NOT IN ('SYS','SYSTEM','XDB','OUTLN','DBSNMP','AUDSYS','DBSFWUSER','GSMUSER') ORDER BY username";
            default -> throw new BizException(ErrorCode.DS_META_FAIL, "不支持的数据库类型: " + ds.getType());
        };
        Map<String, String> info = connectionInfo(ds);
        List<String> result = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection(info.get("url"), info.get("username"), info.get("password"));
             Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                result.add(rs.getString(1));
            }
        } catch (Exception e) {
            throw new BizException(ErrorCode.DS_META_FAIL, "拉取数据库清单失败: " + e.getMessage());
        }
        return result;
    }

    /** Schema 层: 仅 PG/SQLServer 有; MySQL/Oracle/Mongo 无独立 schema 层返回空 */
    public List<String> listSchemas(Long id, String database) {
        DatasourceConfig ds = get(id);
        DsType type = DsType.of(ds.getType());
        if (type != DsType.POSTGRESQL && type != DsType.SQLSERVER) {
            return List.of();
        }
        Map<String, String> info = connectionInfo(ds);
        String url = type.buildJdbcUrl(ds.getHost(), ds.getPort(), database, ds.getExtraParams());
        String sql = type == DsType.POSTGRESQL
                ? "SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT IN ('pg_catalog','information_schema') ORDER BY schema_name"
                : "SELECT name FROM sys.schemas ORDER BY name";
        List<String> result = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection(url, info.get("username"), info.get("password"));
             Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                result.add(rs.getString(1));
            }
        } catch (Exception e) {
            throw new BizException(ErrorCode.DS_META_FAIL, "拉取 Schema 清单失败: " + e.getMessage());
        }
        return result;
    }

    /** 指定库(及 Schema)下的表清单 */
    public List<Map<String, String>> listTables(Long id, String database, String schema) {
        DatasourceConfig ds = get(id);
        DsType type = DsType.of(ds.getType());
        if (type == DsType.MONGODB) {
            try (com.mongodb.client.MongoClient client = com.mongodb.client.MongoClients.create(mongoUri(ds))) {
                List<Map<String, String>> result = new ArrayList<>();
                client.getDatabase(database).listCollectionNames().forEach(c -> {
                    Map<String, String> t = new LinkedHashMap<>();
                    t.put("name", c);
                    t.put("type", "COLLECTION");
                    result.add(t);
                });
                return result;
            } catch (Exception e) {
                throw new BizException(ErrorCode.DS_META_FAIL, "拉取集合清单失败: " + e.getMessage());
            }
        }
        Map<String, String> info = connectionInfo(ds);
        // Oracle 无 database 层: 连接串不变, database 参数即 schema(用户)
        String url = type == DsType.ORACLE
                ? info.get("url")
                : type.buildJdbcUrl(ds.getHost(), ds.getPort(), database, ds.getExtraParams());
        String catalog = type == DsType.ORACLE ? null : database;
        String schemaPattern = switch (type) {
            case POSTGRESQL -> schema == null || schema.isBlank() ? "public" : schema;
            case SQLSERVER -> schema == null || schema.isBlank() ? "dbo" : schema;
            case ORACLE -> database;
            default -> null;
        };
        List<Map<String, String>> result = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection(url, info.get("username"), info.get("password"))) {
            DatabaseMetaData meta = conn.getMetaData();
            try (ResultSet rs = meta.getTables(catalog, schemaPattern, "%", new String[]{"TABLE", "VIEW"})) {
                while (rs.next()) {
                    Map<String, String> t = new LinkedHashMap<>();
                    t.put("name", rs.getString("TABLE_NAME"));
                    t.put("type", rs.getString("TABLE_TYPE"));
                    result.add(t);
                }
            }
        } catch (Exception e) {
            throw new BizException(ErrorCode.DS_META_FAIL, "拉取表清单失败: " + e.getMessage());
        }
        return result;
    }

    /** 指定库(及 Schema)下的字段清单(含主键标记) */
    public List<Map<String, Object>> listColumns(Long id, String database, String table, String schema) {
        DatasourceConfig ds = get(id);
        DsType type = DsType.of(ds.getType());
        Map<String, String> info = connectionInfo(ds);
        String url = type == DsType.ORACLE
                ? info.get("url")
                : type.buildJdbcUrl(ds.getHost(), ds.getPort(), database, ds.getExtraParams());
        String catalog = type == DsType.ORACLE ? null : database;
        String schemaPattern = switch (type) {
            case POSTGRESQL -> schema == null || schema.isBlank() ? "public" : schema;
            case SQLSERVER -> schema == null || schema.isBlank() ? "dbo" : schema;
            case ORACLE -> database;
            default -> null;
        };
        List<Map<String, Object>> result = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection(url, info.get("username"), info.get("password"))) {
            DatabaseMetaData meta = conn.getMetaData();
            try (ResultSet rs = meta.getColumns(catalog, schemaPattern, table, "%")) {
                while (rs.next()) {
                    Map<String, Object> c = new LinkedHashMap<>();
                    c.put("name", rs.getString("COLUMN_NAME"));
                    c.put("type", rs.getString("TYPE_NAME"));
                    c.put("size", rs.getInt("COLUMN_SIZE"));
                    c.put("nullable", rs.getInt("NULLABLE") != DatabaseMetaData.columnNoNulls);
                    c.put("primaryKey", false);
                    result.add(c);
                }
            }
            try (ResultSet pk = meta.getPrimaryKeys(catalog, schemaPattern, table)) {
                while (pk.next()) {
                    String pkCol = pk.getString("COLUMN_NAME");
                    result.stream().filter(c -> pkCol.equals(c.get("name"))).forEach(c -> c.put("primaryKey", true));
                }
            }
        } catch (Exception e) {
            throw new BizException(ErrorCode.DS_META_FAIL, "拉取字段清单失败: " + e.getMessage());
        }
        return result;
    }

    /** Mongo 集合字段：采样首条文档（BSON 类型 -> 展示类型） */
    private List<Map<String, Object>> mongoFields(DatasourceConfig ds, String collection) {
        try (com.mongodb.client.MongoClient client = com.mongodb.client.MongoClients.create(mongoUri(ds))) {
            Document sample = client.getDatabase(ds.getDatabaseName())
                    .getCollection(collection).find().first();
            List<Map<String, Object>> result = new ArrayList<>();
            if (sample == null) {
                return result;
            }
            for (Map.Entry<String, Object> e : sample.entrySet()) {
                Map<String, Object> c = new LinkedHashMap<>();
                c.put("name", e.getKey());
                c.put("type", bsonTypeName(e.getValue()));
                c.put("size", 0);
                c.put("nullable", true);
                c.put("primaryKey", "_id".equals(e.getKey()));
                result.add(c);
            }
            return result;
        } catch (Exception e) {
            throw new BizException(ErrorCode.DS_META_FAIL, "拉取集合字段失败: " + e.getMessage());
        }
    }

    private String bsonTypeName(Object v) {
        if (v instanceof org.bson.types.ObjectId) return "OBJECTID";
        if (v instanceof String) return "STRING";
        if (v instanceof Boolean) return "BOOLEAN";
        if (v instanceof Integer) return "INT";
        if (v instanceof Long) return "BIGINT";
        if (v instanceof Double || v instanceof Float) return "DOUBLE";
        if (v instanceof java.math.BigDecimal || v instanceof org.bson.types.Decimal128) return "DECIMAL";
        if (v instanceof java.util.Date) return "TIMESTAMP";
        if (v instanceof Document) return "OBJECT";
        if (v instanceof List) return "ARRAY";
        return "STRING";
    }

    private List<Map<String, String>> mongoCollections(DatasourceConfig ds) {
        try (com.mongodb.client.MongoClient client = com.mongodb.client.MongoClients.create(mongoUri(ds))) {
            List<Map<String, String>> result = new ArrayList<>();
            client.getDatabase(ds.getDatabaseName()).listCollectionNames().forEach(n -> {
                Map<String, String> t = new LinkedHashMap<>();
                t.put("name", n);
                t.put("type", "COLLECTION");
                result.add(t);
            });
            return result;
        } catch (Exception e) {
            throw new BizException(ErrorCode.DS_META_FAIL, "拉取集合清单失败: " + e.getMessage());
        }
    }

    // ---------- SQL 预览 ----------

    public Map<String, Object> preview(Long id, String sql, Integer limit) {
        DatasourceConfig ds = get(id);
        DsType type = DsType.of(ds.getType());
        int n = limit == null || limit <= 0 ? 100 : Math.min(limit, 1000);
        long start = System.currentTimeMillis();
        Map<String, String> info = connectionInfo(ds);
        List<String> columns = new ArrayList<>();
        List<List<Object>> rows = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection(info.get("url"), info.get("username"), info.get("password"))) {
            String wrapped = type.wrapPagination(sql, n);
            try (Statement st = conn.createStatement()) {
                st.setQueryTimeout(30);
                try (ResultSet rs = st.executeQuery(wrapped)) {
                    ResultSetMetaData md = rs.getMetaData();
                    for (int i = 1; i <= md.getColumnCount(); i++) {
                        columns.add(md.getColumnLabel(i));
                    }
                    while (rs.next() && rows.size() < n) {
                        List<Object> row = new ArrayList<>();
                        for (int i = 1; i <= md.getColumnCount(); i++) {
                            row.add(rs.getObject(i));
                        }
                        rows.add(row);
                    }
                }
            }
        } catch (SQLException e) {
            throw new BizException(ErrorCode.SQL_VALIDATE_FAIL, "SQL 预览失败: " + e.getMessage());
        }
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("columns", columns);
        result.put("rows", rows);
        result.put("durationMs", System.currentTimeMillis() - start);
        return result;
    }
}
