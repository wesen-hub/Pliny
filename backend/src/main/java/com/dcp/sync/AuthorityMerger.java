package com.dcp.sync;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 权威副本追平任务(V3 §6.2 / §8, P5)。
 *
 * <p>架构: 消费端写 MySQL 分表(实时查询面 + 双保险); <b>Iceberg 是权威</b>,
 * 由批量任务按<b>权威表自身进度</b>(seq &gt; 权威表 MAX(seq))从分表追平,
 * 按 (PK, max seq) 合并收敛 —— 乱序在追平侧无感知(合并幂等: GREATEST seq)。
 *
 * <p>演示环境无 Iceberg: 权威副本 = 一张"权威表"(target_db.t_{prefix}_authority),
 * 生产将 {@link #mergeIntoAuthority} 替换为 Iceberg MERGE INTO 即可(语义一致)。
 *
 * <p>不维护任何"水位": 追平进度直接从权威表自身 MAX(seq) 读取,
 * 重启/多源均安全(漏合并的行下次周期自然补上, 重复合并被幂等吸收)。
 */
@Slf4j
@Component
public class AuthorityMerger {

    private final JdbcTemplate metaJdbc;
    private final JdbcTemplate targetJdbc;
    private final LogicalTableConfigService tableConfigService;
    private final Map<String, Boolean> authorityEnsured = new ConcurrentHashMap<>();

    public AuthorityMerger(JdbcTemplate metaJdbc, LogicalTableConfigService tableConfigService,
                           @Value("${dcp.sync.target.url}") String targetUrl,
                           @Value("${dcp.sync.target.username:root}") String username,
                           @Value("${dcp.sync.target.password:root123}") String password) {
        this.metaJdbc = metaJdbc;
        org.springframework.jdbc.datasource.DriverManagerDataSource ds =
                new org.springframework.jdbc.datasource.DriverManagerDataSource();
        ds.setDriverClassName("com.mysql.cj.jdbc.Driver");
        ds.setUrl(targetUrl);
        ds.setUsername(username);
        ds.setPassword(password);
        this.targetJdbc = new JdbcTemplate(ds);
        this.tableConfigService = tableConfigService;
    }

    /** 定时追平: 每 30s 扫描所有逻辑表, 把分表中 seq &gt; 权威表 MAX(seq) 的新行合并进权威副本 */
    /** 权威副本(演示)已退役(2026-09-07): 单表即权威, 直读逻辑表单表; 自动追平默认关闭, 手动端点保留 */
    @Value("${dcp.sync.authority.enabled:false}")
    private boolean authorityEnabled;

    @Scheduled(fixedDelayString = "${dcp.sync.lake-flush-ms:30000}")
    public synchronized void flushAll() {
        if (!authorityEnabled) {
            return;
        }
        List<String> tables;
        try {
            tables = metaJdbc.queryForList("SELECT table_name FROM logical_table", String.class);
        } catch (Exception e) {
            return;
        }
        for (String t : tables) {
            try {
                flush(t);
            } catch (Exception e) {
                log.error("权威副本追平失败 {}: {}", t, e.getMessage());
            }
        }
    }

    /** 追平单表: 把分表中 seq &gt; 权威表当前 MAX(seq) 的行按 (PK, max seq) 合并进权威副本。返回本次合并行数 */
    public int flush(String logicalTable) {
        LogicalTableConfigService.LogicalTableConfig cfg = tableConfigService.load(logicalTable);
        String authority = cfg.getTablePrefix() + "_authority";
        ensureAuthorityTable(authority, cfg);
        long lastSynced = maxSeqOf(authority);
        // 从全部分表拉 seq &gt; lastSynced 的行(无上界; 重复/乱序由 (PK,max seq) 合并幂等吸收)
        List<Map<String, Object>> rows = new ArrayList<>();
        for (int i = 0; i < cfg.getTableCount(); i++) {
            String shard = RouteUtil.tableName(cfg.getTablePrefix(), i, cfg.getTableCount());
            rows.addAll(selectNewer(shard, cfg, lastSynced));
        }
        int merged = 0;
        for (Map<String, Object> row : rows) {
            ensureAuthorityColumns(authority, row.keySet());
            mergeIntoAuthority(authority, cfg, row);
            merged++;
        }
        long newMax = maxSeqOf(authority);
        if (merged > 0 || newMax > lastSynced) {
            log.info("权威副本追平 {}: 合并 {} 行, 权威最新 seq {} → {}", logicalTable, merged, lastSynced, newMax);
        }
        return merged;
    }

    /** 分表增量读取(仅取权威副本尚未覆盖的新行) */
    private List<Map<String, Object>> selectNewer(String shard, LogicalTableConfigService.LogicalTableConfig cfg,
                                                  long lastSynced) {
        List<String> cols;
        try {
            cols = targetJdbc.queryForList(
                    "SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?",
                    String.class, shard);
        } catch (Exception ignore) {
            return List.of(); // 表不存在 → 跳过该分表
        }
        if (cols.isEmpty()) {
            return List.of();
        }
        String sql = "SELECT " + cols.stream().map(c -> "`" + c + "`").reduce((a, b) -> a + "," + b).orElse("*") +
                " FROM `" + shard + "` WHERE seq > ?";
        return targetJdbc.query(sql, (rs, i) -> {
            Map<String, Object> row = new LinkedHashMap<>();
            for (String c : cols) {
                row.put(c, rs.getObject(c));
            }
            return row;
        }, lastSynced);
    }

    /** 按 (PK, max seq) 合并进权威表(生产 = Iceberg MERGE INTO) */
    private void mergeIntoAuthority(String authority, LogicalTableConfigService.LogicalTableConfig cfg,
                                    Map<String, Object> row) {
        long seq = ((Number) row.get("seq")).longValue();
        List<String> keyCols = cfg.getKeyColumns().stream().map(c -> "`" + c + "`").toList();
        List<String> valueCols = row.keySet().stream()
                .filter(c -> !cfg.getKeyColumns().contains(c) && !"seq".equals(c) && !"_pk_id".equals(c))
                .map(c -> "`" + c + "`").toList();
        StringBuilder sql = new StringBuilder("INSERT INTO `").append(authority).append("` (")
                .append(String.join(",", keyCols)).append(",").append(String.join(",", valueCols))
                .append(",seq) VALUES (")
                .append(String.join(",", keyCols.stream().map(k -> "?").toList()))
                .append(",").append(String.join(",", valueCols.stream().map(v -> "?").toList()))
                .append(",?) ON DUPLICATE KEY UPDATE ");
        List<Object> args = new ArrayList<>();
        for (String k : keyCols) {
            args.add(row.get(k.replace("`", "")));
        }
        for (String v : valueCols) {
            String name = v.replace("`", "");
            args.add(row.get(name));
            // 字段级部分更新(多表合并): 直接写入本行携带的字段; 水位保证追平有序
            sql.append(v).append(" = VALUES(").append(v).append("), ");
        }
        args.add(seq);
        sql.append("seq = GREATEST(seq, VALUES(seq))");
        targetJdbc.update(sql.toString(), args.toArray());
    }

    private void ensureAuthorityTable(String authority, LogicalTableConfigService.LogicalTableConfig cfg) {
        if (authorityEnsured.getOrDefault(authority, false)) {
            return;
        }
        // 从第一张已存在的分表派生列结构(演示分表由消费端自动建, 列: 唯一键 + 业务列 + seq)
        List<Map<String, Object>> cols = new ArrayList<>();
        for (int i = 0; i < cfg.getTableCount(); i++) {
            String shard = RouteUtil.tableName(cfg.getTablePrefix(), i, cfg.getTableCount());
            try {
                cols = targetJdbc.queryForList(
                        "SELECT COLUMN_NAME, COLUMN_TYPE FROM information_schema.COLUMNS "
                                + "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?", shard);
                if (!cols.isEmpty()) {
                    break;
                }
            } catch (Exception ignore) {
                // 分表不存在, 试下一张
            }
        }
        if (cols.isEmpty()) {
            log.warn("权威表 {} 暂无法创建: 无分表可参考结构", authority);
            authorityEnsured.put(authority, true);
            return;
        }
        List<String> keys = cfg.getKeyColumns().stream().map(c -> "`" + c + "`").toList();
        List<String> lines = new ArrayList<>();
        lines.add("  `_pk_id` BIGINT AUTO_INCREMENT PRIMARY KEY");
        for (Map<String, Object> c : cols) {
            String name = String.valueOf(c.get("COLUMN_NAME"));
            if ("_pk_id".equals(name)) {
                continue;
            }
            if ("seq".equals(name)) {
                lines.add("  `seq` BIGINT NOT NULL DEFAULT 0");
            } else {
                lines.add("  `" + name + "` " + c.get("COLUMN_TYPE") + " NULL");
            }
        }
        lines.add("  UNIQUE KEY uk_biz (" + String.join(",", keys) + ")");
        String ddl = "CREATE TABLE IF NOT EXISTS `" + authority + "` (\n"
                + String.join(",\n", lines)
                + "\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='权威副本(演示, 生产为 Iceberg)'";
        targetJdbc.execute(ddl);
        authorityEnsured.put(authority, true);
        authorityCols.put(authority, loadAuthorityColumns(authority));
        log.info("权威表已就绪: {}", authority);
    }

    /** 权威表缺失列自动补充(宽表场景: 第二个源表字段后到) */
    private void ensureAuthorityColumns(String authority, java.util.Set<String> need) {
        java.util.Set<String> known = authorityCols.get(authority);
        if (known == null || !known.containsAll(need)) {
            synchronized (authority.intern()) {
                java.util.Set<String> fresh = loadAuthorityColumns(authority);
                List<String> missing = new ArrayList<>();
                for (String c : need) {
                    if ("_pk_id".equals(c) || "seq".equals(c)) {
                        continue;
                    }
                    if (!fresh.contains(c)) {
                        missing.add(c);
                    }
                }
                if (!missing.isEmpty()) {
                    targetJdbc.execute("ALTER TABLE `" + authority + "` ADD COLUMN "
                            + String.join(", ADD COLUMN ", missing.stream()
                            .map(c -> "`" + c + "` VARCHAR(255) NULL").toList()));
                    log.info("权威表补列 {}: +{}", authority, missing);
                    fresh.addAll(missing);
                }
                authorityCols.put(authority, fresh);
            }
        }
    }

    private java.util.Set<String> loadAuthorityColumns(String authority) {
        try {
            return new java.util.HashSet<>(targetJdbc.queryForList(
                    "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
                            + "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?", String.class, authority));
        } catch (Exception e) {
            return new java.util.HashSet<>();
        }
    }

    /** 权威表已确认列(多表合并: 后到的新列自动 ALTER ADD COLUMN) */
    private final Map<String, java.util.Set<String>> authorityCols = new ConcurrentHashMap<>();

    /** 权威表当前最大 seq(=已追平进度; 无权威表/空表返回 0) */
    private long maxSeqOf(String authority) {
        try {
            Long m = targetJdbc.query("SELECT MAX(seq) FROM `" + authority + "`",
                    rs -> rs.next() ? rs.getLong(1) : null);
            return m == null ? 0L : m;
        } catch (Exception e) {
            return 0L;
        }
    }

    /** 权威副本实时状态(供 API/页面展示; 全部由权威表实时派生, 无水位存储) */
    public Map<String, Object> statusOf(String logicalTable) {
        LogicalTableConfigService.LogicalTableConfig cfg = tableConfigService.load(logicalTable);
        String authority = cfg.getTablePrefix() + "_authority";
        Long rows;
        try {
            rows = targetJdbc.queryForObject("SELECT COUNT(*) FROM `" + authority + "`", Long.class);
        } catch (Exception e) {
            rows = null;
        }
        Map<String, Object> out = new java.util.LinkedHashMap<>();
        out.put("logicalTable", logicalTable);
        out.put("authorityTable", authority);
        out.put("authorityRows", rows == null ? 0 : rows);
        out.put("authorityMaxSeq", maxSeqOf(authority));
        return out;
    }
}
