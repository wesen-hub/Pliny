package com.dcp.sync;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 多目标库注册表: 按逻辑表的目标库(database_name)懒创建连接。
 * 注意: 不暴露 DataSource Bean(会触发元库自动配置跳过), 内部 DriverManagerDataSource。
 */
@Slf4j
@Component
public class TargetDbRegistry {

    private final JdbcTemplate metaJdbc;
    private final String baseUrl;
    private final String username;
    private final String password;
    private final Map<String, AtomicUpsertDao> daos = new ConcurrentHashMap<>();
    /** 逻辑表 → 目标库缓存 */
    private final Map<String, String> tableTarget = new ConcurrentHashMap<>();

    public TargetDbRegistry(JdbcTemplate jdbcTemplate,
                            @Value("${dcp.sync.target.url}") String targetUrl,
                            @Value("${dcp.sync.target.username:root}") String username,
                            @Value("${dcp.sync.target.password:root123}") String password) {
        this.metaJdbc = jdbcTemplate;
        // 基础 url 形如 jdbc:mysql://demo-db:3306/target_db?... → 替换库名得到其他库
        this.baseUrl = targetUrl.replaceAll("(/[^?/]+)\\?", "/%s?").replace("%s", "{db}");
        this.username = username;
        this.password = password;
        register("target_db", targetUrl);
    }

    /** 逻辑表对应的目标库名(默认 target_db) */
    public String databaseOf(String logicalTable) {
        return tableTarget.computeIfAbsent(logicalTable, lt -> {
            try {
                return metaJdbc.queryForObject(
                        "SELECT target_database FROM logical_table WHERE table_name = ?",
                        String.class, lt);
            } catch (Exception e) {
                return "target_db";
            }
        });
    }

    /** 逻辑表对应的写 DAO(按目标库懒创建) */
    public AtomicUpsertDao daoOf(String logicalTable) {
        String db = databaseOf(logicalTable);
        return daos.computeIfAbsent(db, this::create);
    }

    private AtomicUpsertDao create(String databaseName) {
        String url = baseUrl.replace("{db}", databaseName);
        log.info("创建目标库连接: {} ({})", databaseName, url);
        return new AtomicUpsertDao(dataSource(url));
    }

    private javax.sql.DataSource dataSource(String url) {
        DriverManagerDataSource ds = new DriverManagerDataSource();
        ds.setDriverClassName("com.mysql.cj.jdbc.Driver");
        ds.setUrl(url);
        ds.setUsername(username);
        ds.setPassword(password);
        return ds;
    }

    private void register(String db, String url) {
        daos.put(db, new AtomicUpsertDao(dataSource(url)));
    }

    /** 预建物理表骨架(模型发布时): {prefix}_{NN}, 含业务键列 + seq + 唯一键; 业务字段列由消费端写入时自动补 */
    public void ensureShardTables(String tablePrefix, int tableCount, List<String> keyColumns) {
        AtomicUpsertDao dao = daoOf("target_db");
        if (dao == null) {
            dao = create("target_db");
        }
        java.util.Set<String> keys = keyColumns == null ? java.util.Set.of() : new java.util.HashSet<>(keyColumns);
        // 去分片(2026-09-07): tableCount<=1 → 逻辑表单物理表(TiDB 自动 split 承担原分片职责)
        if (tableCount <= 1) {
            dao.ensureTable(tablePrefix, keys.isEmpty() ? java.util.List.of() : new java.util.ArrayList<>(keys),
                    java.util.Map.of());
            return;
        }
        int width = Math.max(2, String.valueOf(Math.max(tableCount - 1, 1)).length());
        for (int i = 0; i < tableCount; i++) {
            String table = tablePrefix + "_" + String.format("%0" + width + "d", i);
            StringBuilder ddl = new StringBuilder("CREATE TABLE IF NOT EXISTS `").append(table).append("` (\n")
                    .append("  `_pk_id` BIGINT AUTO_INCREMENT PRIMARY KEY,\n");
            List<String> uk = new java.util.ArrayList<>();
            for (String k : keys) {
                ddl.append("  `").append(k).append("` VARCHAR(255) NOT NULL DEFAULT '',\n");
                uk.add("`" + k + "`");
            }
            ddl.append("  seq BIGINT NOT NULL DEFAULT 0,\n");
            if (!uk.isEmpty()) {
                ddl.append("  UNIQUE KEY uk_biz (").append(String.join(", ", uk)).append("),\n");
            }
            ddl.setLength(ddl.length() - 2); // 去末尾逗号
            ddl.append("\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='V3 分表(模型预建, 遗留)");
            dao.executeDdl(ddl.toString());
        }
    }

    /** 清空缓存(逻辑表目标库配置变更后调用) */
    public void invalidate(String logicalTable) {
        tableTarget.remove(logicalTable);
    }
}
