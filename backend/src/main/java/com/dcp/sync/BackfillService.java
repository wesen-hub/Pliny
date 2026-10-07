package com.dcp.sync;

import com.dcp.module.datasource.DatasourceService;
import com.dcp.module.datasource.entity.DatasourceConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 字段级回补通道(后加列存量补齐, 后端直读直写)。
 *
 * <p><b>触发场景</b>: 同一采集任务(同 task_id, 同 source, 同写者列)编辑加列时, 族 job
 * 同 jobId 恢复 checkpoint 续采增量, 该列在增量起点之前的存量值永远不会被采 → 必须回补。
 * (新任务后加列由 initial 快照兜底, 不登记回补; 回补 seq=1 小号, 即使与快照并发也被快照 S 覆盖, 幂等无害。)
 *
 * <p><b>执行模型</b>: 源表按业务主键区间分片 JDBC 读(source_col 该列), 按 key_columns
 * 逐行 upsert 写目标库物理表(物理表号 = RouteUtil 桶路由, 与消费端一致); 写者列回补小号
 * seq=1(< 一切流号), IF(writer_col &lt; VALUES(writer_col)) 裁判: 实时增量/快照 LWW 覆盖回补,
 * 回补永不覆盖实时。
 *
 * <p><b>断点续跑</b>: cursor_json 记录最后一行业务主键, 每批推进; 失败/重启后从游标继续。
 * 单轮调度限批(MAX_BATCHES_PER_ROUND), 大批量跨多轮完成。
 *
 * <p><b>支持边界</b>: 源库类型 MySQL/PostgreSQL(行构造器分页); 列名直同名(key/bk 列在
 * 源表同名可取), 改名映射暂不支持(FAILED 标记, 不阻断实时链路)。
 */
@Slf4j
@Service
public class BackfillService {

    private final JdbcTemplate metaJdbc;
    private final DatasourceService datasourceService;
    private final String encKey;

    @Value("${dcp.sync.target.url}")
    private String targetUrl;
    @Value("${dcp.sync.target.username:root}")
    private String targetUser;
    @Value("${dcp.sync.target.password:root123}")
    private String targetPassword;

    /** 回补开关(默认开; 关闭时只登记不执行) */
    @Value("${dcp.sync.backfill.enabled:true}")
    private boolean backfillEnabled;
    /** 单批读取行数(主键区间分片大小) */
    @Value("${dcp.sync.backfill.batch:500}")
    private int batchSize;
    /** 单轮调度最多处理批数(跨轮续跑, 防长事务霸占调度线程) */
    private static final int MAX_BATCHES_PER_ROUND = 100;
    /** 回补小号 seq: 小于一切流号/快照号, 实时增量 LWW 覆盖回补 */
    public static final long BACKFILL_SEQ = 1L;

    private static final ObjectMapper JSON = new ObjectMapper();

    /** 目标库 JdbcTemplate 缓存(key = 库名, 多目标库路由) */
    private final ConcurrentHashMap<String, JdbcTemplate> targetJdbcCache = new ConcurrentHashMap<>();

    public BackfillService(JdbcTemplate metaJdbc, DatasourceService datasourceService,
                           @Value("${dcp.enc-key}") String encKey) {
        this.metaJdbc = metaJdbc;
        this.datasourceService = datasourceService;
        this.encKey = encKey;
    }

    // ───────────────── 后加列登记 ─────────────────

    /**
     * 后加列登记(register 时调用): 任务已存在(oldFieldMapping 非空)且新增 field-map 源列
     * → 每新增列登记一条 PENDING 回补任务(查重: 同 task 同源列未完成存在则跳过)。
     */
    public void registerBackfillsIfNeeded(String logicalTable, long syncTaskId, long taskId,
                                          long datasourceId, String sourceDb, String sourceTable,
                                          String oldFieldMapping, String newFieldMapping) {
        if (oldFieldMapping == null || oldFieldMapping.isBlank()) {
            return;   // 新任务: initial 快照兜底, 不登记
        }
        try {
            Set<String> oldCols = srcColsOf(oldFieldMapping);
            for (Map<String, Object> m : SyncMappingRules.parseMapping(newFieldMapping)) {
                Map<String, Object> source = m.get("source") instanceof Map<?, ?> sm ? (Map<String, Object>) sm : Map.of();
                if (!"field-map".equals(String.valueOf(source.get("kind")))) {
                    continue;
                }
                String srcField = String.valueOf(source.getOrDefault("srcField", ""));
                Object target = m.get("modelField");
                if (srcField.isBlank() || target == null || String.valueOf(target).isBlank()) {
                    continue;
                }
                if (oldCols.contains(srcField)) {
                    continue;   // 存量列, 不登记
                }
                Integer pending = metaJdbc.queryForObject(
                        "SELECT COUNT(*) FROM sync_backfill_run WHERE task_id = ? AND source_col = ? "
                                + "AND status IN ('PENDING', 'BACKFILLING')",
                        Integer.class, taskId, srcField);
                if (pending != null && pending > 0) {
                    continue;   // 已在回补中
                }
                metaJdbc.update("INSERT INTO sync_backfill_run (logical_table, sync_task_id, task_id, "
                                + "datasource_id, source_db, source_table, source_col, target_col, status) "
                                + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'PENDING')",
                        logicalTable, syncTaskId, taskId, datasourceId, sourceDb, sourceTable,
                        srcField, String.valueOf(target));
                log.info("后加列已登记字段级回补: {} ← {}.{}.{} → {} (task={})", logicalTable,
                        sourceDb, sourceTable, srcField, target, taskId);
            }
        } catch (Exception e) {
            log.warn("后加列回补登记失败: {}", e.getMessage());
        }
    }

    /** field_mapping 中全部 field-map 源列集合 */
    private Set<String> srcColsOf(String fieldMapping) {
        Set<String> cols = new LinkedHashSet<>();
        for (Map<String, Object> m : SyncMappingRules.parseMapping(fieldMapping)) {
            Map<String, Object> source = m.get("source") instanceof Map<?, ?> sm ? (Map<String, Object>) sm : Map.of();
            if (!"field-map".equals(String.valueOf(source.get("kind")))) {
                continue;
            }
            String srcField = String.valueOf(source.getOrDefault("srcField", ""));
            if (!srcField.isBlank()) {
                cols.add(srcField);
            }
        }
        return cols;
    }

    // ───────────────── 回补调度 ─────────────────

    /** 巡检调度: 每 interval-ms 取 PENDING 回补任务逐条执行(游标断点, 跨轮续跑) */
    @Scheduled(fixedDelayString = "${dcp.sync.backfill.interval-ms:60000}")
    @SchedulerLock(name = "backfillRun", lockAtMostFor = "10m", lockAtLeastFor = "30s")
    public void runPending() {
        if (!backfillEnabled) {
            return;
        }
        List<Long> ids;
        try {
            ids = metaJdbc.queryForList(
                    "SELECT id FROM sync_backfill_run WHERE status = 'PENDING' ORDER BY id LIMIT 10",
                    Long.class);
        } catch (Exception e) {
            log.warn("回补任务查询失败: {}", e.getMessage());
            return;
        }
        for (long id : ids) {
            try {
                executeOne(id);
            } catch (Exception e) {
                log.warn("回补任务执行失败 run={}: {}", id, e.getMessage());
                metaJdbc.update("UPDATE sync_backfill_run SET status = 'FAILED', error_msg = ? WHERE id = ?",
                        truncate(e.getMessage(), 1024), id);
            }
        }
    }

    private static String truncate(String s, int max) {
        return s == null || s.length() <= max ? s : s.substring(0, max);
    }

    /** 执行单条回补(已由调度串行; 幂等: 断点游标, 失败置 FAILED 由人工重置重跑) */
    void executeOne(long runId) {
        List<Map<String, Object>> rows = metaJdbc.queryForList(
                "SELECT id, logical_table, sync_task_id, task_id, datasource_id, source_db, source_table, "
                        + "source_col, target_col, cursor_json, processed_rows "
                        + "FROM sync_backfill_run WHERE id = ?", runId);
        if (rows.isEmpty()) {
            return;
        }
        Map<String, Object> run = rows.get(0);
        String status = String.valueOf(metaJdbc.queryForMap(
                "SELECT status FROM sync_backfill_run WHERE id = ?", runId).get("status"));
        if (!"PENDING".equals(status) && !"BACKFILLING".equals(status)) {
            return;
        }
        metaJdbc.update("UPDATE sync_backfill_run SET status = 'BACKFILLING', error_msg = NULL WHERE id = ?", runId);

        long taskId = ((Number) run.get("task_id")).longValue();
        long datasourceId = ((Number) run.get("datasource_id")).longValue();
        String logicalTable = String.valueOf(run.get("logical_table"));
        String sourceDb = String.valueOf(run.get("source_db"));
        String sourceTable = String.valueOf(run.get("source_table"));
        String sourceCol = String.valueOf(run.get("source_col"));
        String targetCol = String.valueOf(run.get("target_col"));

        // 任务行: 业务主键(源侧, 路由拼接) + 目标唯一键(key_columns, upsert 冲突依据)
        Map<String, Object> taskRow = metaJdbc.queryForMap(
                "SELECT business_key_fields, key_columns, bucket_count FROM cdc_st_task WHERE task_id = ?", taskId);
        List<String> bkFields = fromJsonList(String.valueOf(taskRow.get("business_key_fields")));
        List<String> keyCols = fromJsonList(String.valueOf(taskRow.get("key_columns")));
        int taskBucketCount = ((Number) taskRow.get("bucket_count")).intValue();
        if (bkFields.isEmpty() || keyCols.isEmpty()) {
            fail(runId, "任务业务主键/唯一键为空, 无法回补");
            return;
        }
        // 逻辑表: 前缀/分表数/桶数/目标库
        Map<String, Object> lt;
        try {
            lt = metaJdbc.queryForMap(
                    "SELECT table_prefix, table_count, bucket_count, target_database FROM logical_table WHERE table_name = ?",
                    logicalTable);
        } catch (Exception e) {
            lt = metaJdbc.queryForMap(
                    "SELECT table_prefix, table_count, bucket_count FROM logical_table WHERE table_name = ?",
                    logicalTable);
        }
        String prefix = String.valueOf(lt.get("table_prefix"));
        int tableCount = ((Number) lt.get("table_count")).intValue();
        int bucketCount = lt.get("bucket_count") == null ? taskBucketCount
                : ((Number) lt.get("bucket_count")).intValue();
        String targetDb = lt.get("target_database") == null ? "target_db" : String.valueOf(lt.get("target_database"));

        // 写者列(回补小号 seq 裁判列)
        String writerCol = SyncMappingRules.writerColOf(String.valueOf(taskId), String.valueOf(datasourceId),
                sourceDb, sourceTable);
        if (writerCol == null || writerCol.isBlank()) {
            fail(runId, "写者列名不可用, 无法回补");
            return;
        }

        // 源数据源(仅 MySQL/PG 支持行构造器分页)
        DatasourceConfig ds = datasourceService.get(datasourceId);
        String dsType = ds.getType() == null ? "mysql" : ds.getType().toLowerCase();
        if (!"mysql".equals(dsType) && !"postgresql".equals(dsType)) {
            fail(runId, "回补暂不支持源类型: " + dsType + "(仅 MySQL/PostgreSQL)");
            return;
        }
        String srcPwd = com.dcp.common.CryptoUtil.decrypt(ds.getPasswordCiphertext(), encKey);

        List<String> cursor = run.get("cursor_json") == null ? null : fromJsonList(String.valueOf(run.get("cursor_json")));
        long processed = run.get("processed_rows") == null ? 0 : ((Number) run.get("processed_rows")).longValue();
        JdbcTemplate target = targetJdbc(targetDb);

        int batches = 0;
        while (batches < MAX_BATCHES_PER_ROUND) {
            List<Map<String, Object>> batch = readBatch(dsType, ds, srcPwd, sourceDb, sourceTable,
                    bkFields, keyCols, sourceCol, cursor, batchSize);
            if (batch.isEmpty()) {
                // 全表扫完: 断点游标之后再无行
                metaJdbc.update("UPDATE sync_backfill_run SET status = 'DONE', processed_rows = ?, cursor_json = NULL WHERE id = ?",
                        processed, runId);
                log.info("字段级回补完成: {} ← {}.{}.{} (共 {} 行)", logicalTable, sourceDb, sourceTable,
                        sourceCol, processed);
                return;
            }
            for (Map<String, Object> row : batch) {
                List<String> bkVals = new ArrayList<>();
                for (String b : bkFields) {
                    bkVals.add(str(row.get(b)));
                }
                String businessKey = String.join("\u0000", bkVals);
                RouteUtil.Route route = RouteUtil.route(businessKey, bucketCount, tableCount);
                String phys = RouteUtil.tableName(prefix, route.tableNo(), tableCount);
                List<String> keyVals = new ArrayList<>();
                for (String k : keyCols) {
                    if (!row.containsKey(k)) {
                        fail(runId, "目标唯一键列 " + k + " 在源表不存在(改名映射暂不支持回补)");
                        return;
                    }
                    keyVals.add(str(row.get(k)));
                }
                upsert(target, targetDb, phys, keyCols, keyVals, targetCol, str(row.get(sourceCol)), writerCol);
                processed++;
                cursor = bkVals;
            }
            metaJdbc.update("UPDATE sync_backfill_run SET processed_rows = ?, cursor_json = ? WHERE id = ?",
                    processed, toJson(cursor), runId);
            if (batch.size() < batchSize) {
                metaJdbc.update("UPDATE sync_backfill_run SET status = 'DONE', cursor_json = NULL WHERE id = ?", runId);
                log.info("字段级回补完成: {} ← {}.{}.{} (共 {} 行)", logicalTable, sourceDb, sourceTable,
                        sourceCol, processed);
                return;
            }
            batches++;
        }
        // 单轮批数上限: 保持 BACKFILLING, 下轮从游标续跑
        log.info("字段级回补续跑中: {} ← {}.{}.{} 已处理 {} 行", logicalTable, sourceDb, sourceTable,
                sourceCol, processed);
    }

    /** 主键区间分片读源表: WHERE (bk1,..) > (v1,..) ORDER BY bk LIMIT n(单列退化为普通比较) */
    private List<Map<String, Object>> readBatch(String dsType, DatasourceConfig ds, String pwd,
                                                String sourceDb, String sourceTable,
                                                List<String> bkFields, List<String> keyCols, String sourceCol,
                                                List<String> cursor, int limit) {
        boolean mysql = "mysql".equals(dsType);
        char q = mysql ? '`' : '"';
        Set<String> sel = new LinkedHashSet<>();
        sel.addAll(bkFields);
        sel.addAll(keyCols);
        sel.add(sourceCol);
        String cols = String.join(", ", sel.stream().map(c -> q + c + q).toList());
        StringBuilder sql = new StringBuilder("SELECT ").append(cols)
                .append(" FROM ").append(q).append(sourceDb).append(q).append(".").append(q).append(sourceTable).append(q);
        List<Object> args = new ArrayList<>();
        if (cursor != null && !cursor.isEmpty() && cursor.size() == bkFields.size()) {
            sql.append(" WHERE ");
            if (bkFields.size() == 1) {
                sql.append(q).append(bkFields.get(0)).append(q).append(" > ?");
                args.add(cursor.get(0));
            } else {
                sql.append("(").append(bkFields.stream().map(c -> q + c + q)
                                .collect(java.util.stream.Collectors.joining(", ")))
                        .append(") > (").append("?, ".repeat(bkFields.size() - 1)).append("?)");
                args.addAll(cursor);
            }
        }
        String order = bkFields.stream().map(c -> q + c + q + " ASC").collect(java.util.stream.Collectors.joining(", "));
        sql.append(" ORDER BY ").append(order);
        if (mysql) {
            sql.append(" LIMIT ?");
        } else {
            sql.append(" LIMIT ?");
        }
        args.add(limit);
        String url = mysql
                ? "jdbc:mysql://%s:%d/%s?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Asia/Shanghai"
                        .formatted(ds.getHost(), ds.getPort(), sourceDb)
                : "jdbc:postgresql://%s:%d/%s".formatted(ds.getHost(), ds.getPort(), sourceDb);
        try (Connection conn = java.sql.DriverManager.getConnection(url, ds.getUsername(), pwd);
             PreparedStatement ps = conn.prepareStatement(sql.toString())) {
            for (int i = 0; i < args.size(); i++) {
                ps.setObject(i + 1, args.get(i));
            }
            List<Map<String, Object>> out = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Map<String, Object> row = new java.util.LinkedHashMap<>();
                    for (String c : sel) {
                        row.put(c, rs.getObject(c));
                    }
                    out.add(row);
                }
            }
            return out;
        } catch (Exception e) {
            throw new IllegalStateException("回补读源表失败: " + e.getMessage(), e);
        }
    }

    /** 逐列 upsert: 回补小号 seq 裁判(仅当现有写者值更小才覆盖; 实时 seq 更大 → 回补永不覆盖) */
    private void upsert(JdbcTemplate target, String targetDb, String phys,
                        List<String> keyCols, List<String> keyVals,
                        String targetCol, String colVal, String writerCol) {
        String k = keyCols.stream().map(c -> "`" + c + "`").collect(java.util.stream.Collectors.joining(", "));
        String kp = keyCols.stream().map(c -> "?").collect(java.util.stream.Collectors.joining(", "));
        String sql = "INSERT INTO `" + targetDb + "`.`" + phys + "` (" + k + ", `" + targetCol + "`, `" + writerCol + "`) "
                + "VALUES (" + kp + ", ?, " + BACKFILL_SEQ + ") "
                + "ON DUPLICATE KEY UPDATE "
                + "`" + targetCol + "` = IF(COALESCE(`" + writerCol + "`, 0) < VALUES(`" + writerCol + "`), VALUES(`" + targetCol + "`), `" + targetCol + "`), "
                + "`" + writerCol + "` = IF(COALESCE(`" + writerCol + "`, 0) < VALUES(`" + writerCol + "`), VALUES(`" + writerCol + "`), `" + writerCol + "`)";
        List<Object> args = new ArrayList<>(keyVals);
        args.add(colVal);
        target.update(sql, args.toArray());
    }

    /** 目标库 JdbcTemplate(多目标库路由, 缓存 per 库) */
    private JdbcTemplate targetJdbc(String database) {
        return targetJdbcCache.computeIfAbsent(database, db -> new JdbcTemplate(dataSource(urlWithDb(db))));
    }

    /** 从 target.url 提取 host:port 前缀并替换库名 */
    private String urlWithDb(String database) {
        int host = targetUrl.indexOf("//") + 2;
        int slash = targetUrl.indexOf('/', host);
        if (slash < 0) {
            return targetUrl;
        }
        int q = targetUrl.indexOf('?', slash);
        String query = q > 0 ? targetUrl.substring(q) : "";
        return targetUrl.substring(0, slash) + "/" + database + query;
    }

    private DataSource dataSource(String url) {
        DriverManagerDataSource ds = new DriverManagerDataSource();
        ds.setDriverClassName("com.mysql.cj.jdbc.Driver");
        ds.setUrl(url);
        ds.setUsername(targetUser);
        ds.setPassword(targetPassword);
        return ds;
    }

    private void fail(long runId, String message) {
        metaJdbc.update("UPDATE sync_backfill_run SET status = 'FAILED', error_msg = ? WHERE id = ?",
                truncate(message, 1024), runId);
        log.warn("字段级回补失败 run={}: {}", runId, message);
    }

    private static String str(Object o) {
        return o == null ? "" : String.valueOf(o);
    }

    private static String toJson(Object o) {
        try {
            return JSON.writeValueAsString(o);
        } catch (Exception e) {
            return "[]";
        }
    }

    @SuppressWarnings("unchecked")
    private static List<String> fromJsonList(String s) {
        if (s == null || s.isBlank()) {
            return List.of();
        }
        try {
            return JSON.readValue(s, List.class);
        } catch (Exception e) {
            return List.of();
        }
    }
}
