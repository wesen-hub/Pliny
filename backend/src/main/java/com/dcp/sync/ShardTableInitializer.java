package com.dcp.sync;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 物理分表预建器(控制面): 模型发布时在目标库预建物理表骨架。
 *
 * <p>写路径的自动建表/补列已随 AtomicUpsertDao 迁至 dcp-synclink 业务 jar;
 * 模型发布预建骨架(建表 DDL 生成)保留在控制面, 与迁出前的
 * TargetDbRegistry.ensureShardTables 语义一致。
 */
@Slf4j
@Component
public class ShardTableInitializer {

    private final JdbcTemplate targetJdbc;

    public ShardTableInitializer(@Value("${dcp.sync.target.url}") String targetUrl,
                                 @Value("${dcp.sync.target.username:root}") String username,
                                 @Value("${dcp.sync.target.password:root123}") String password) {
        this.targetJdbc = new JdbcTemplate(dataSource(targetUrl, username, password));
    }

    private static DataSource dataSource(String url, String username, String password) {
        DriverManagerDataSource ds = new DriverManagerDataSource();
        ds.setDriverClassName("com.mysql.cj.jdbc.Driver");
        ds.setUrl(url);
        ds.setUsername(username);
        ds.setPassword(password);
        return ds;
    }

    /** 预建物理表骨架: {prefix}_{NN}, 含业务键列 + seq + 唯一键; 业务字段列由消费端写入时自动补 */
    public void ensureShardTables(String tablePrefix, int tableCount, List<String> keyColumns) {
        Set<String> keys = keyColumns == null ? Set.of() : new HashSet<>(keyColumns);
        // 去分片(2026-09-07): tableCount<=1 → 逻辑表单物理表(TiDB 自动 split 承担原分片职责)
        if (tableCount <= 1) {
            targetJdbc.execute(singleTableDdl(tablePrefix, new ArrayList<>(keys)));
            return;
        }
        int width = Math.max(2, String.valueOf(Math.max(tableCount - 1, 1)).length());
        for (int i = 0; i < tableCount; i++) {
            String table = tablePrefix + "_" + String.format("%0" + width + "d", i);
            targetJdbc.execute(shardTableDdl(table, keys));
        }
    }

    /** 单物理表 DDL(去分片形态) */
    private String singleTableDdl(String table, List<String> keyColumns) {
        StringBuilder ddl = new StringBuilder("CREATE TABLE IF NOT EXISTS `").append(table).append("` (\n")
                .append("  `_pk_id` BIGINT AUTO_INCREMENT PRIMARY KEY,\n");
        List<String> uk = new ArrayList<>();
        for (String k : keyColumns) {
            ddl.append("  `").append(k).append("` VARCHAR(255) NOT NULL DEFAULT '',\n");
            uk.add("`" + k + "`");
        }
        ddl.append("  seq BIGINT NOT NULL DEFAULT 0,\n");
        if (!uk.isEmpty()) {
            ddl.append("  UNIQUE KEY uk_biz (").append(String.join(", ", uk)).append("),\n");
        }
        ddl.setLength(ddl.length() - 2); // 去末尾逗号
        ddl.append("\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='V3 逻辑表单物理表(模型预建, 去分片)'");
        return ddl.toString();
    }

    /** 分片表 DDL(遗留兼容) */
    private String shardTableDdl(String table, Set<String> keys) {
        StringBuilder ddl = new StringBuilder("CREATE TABLE IF NOT EXISTS `").append(table).append("` (\n")
                .append("  `_pk_id` BIGINT AUTO_INCREMENT PRIMARY KEY,\n");
        List<String> uk = new ArrayList<>();
        for (String k : keys) {
            ddl.append("  `").append(k).append("` VARCHAR(255) NOT NULL DEFAULT '',\n");
            uk.add("`" + k + "`");
        }
        ddl.append("  seq BIGINT NOT NULL DEFAULT 0,\n");
        if (!uk.isEmpty()) {
            ddl.append("  UNIQUE KEY uk_biz (").append(String.join(", ", uk)).append("),\n");
        }
        ddl.setLength(ddl.length() - 2); // 去末尾逗号
        ddl.append("\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='V3 分表(模型预建, 遗留)'");
        return ddl.toString();
    }
}
