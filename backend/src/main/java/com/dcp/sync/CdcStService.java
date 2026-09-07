package com.dcp.sync;

import com.dcp.engine.SeaTunnelClient;
import com.dcp.module.datasource.DatasourceService;
import com.dcp.module.datasource.entity.DatasourceConfig;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * SeaTunnel CDC → Kafka 采集任务管理(架构文档采集端实现, 唯一采集链路)。
 *
 * <p>链路: MySQL binlog(connector-cdc-mysql, startup.mode=initial 全量+增量自动衔接)
 * → CdcSeqTransform(领全局 seq + 桶路由 + 字段映射 + CdcRecord 封装)
 * → Kafka sink(按 __bucket 分区保序)。
 *
 * <p><b>多表合并</b>: 一张源表 = 一个 SeaTunnel job(cdc_st_task 按 task_id 区分,
 * uk_src(logical_table, source_db, source_table)); 同一逻辑表多个 job 共享 seq 计数器/topic/
 * 分表规则/keyColumns, 消费端字段级 ODKU 收敛。
 *
 * <p><b>字段映射</b>: 每个源任务的 field_mapping(向导 Step3 按源表拆分)随 job 配置传给
 * CdcSeqTransform, 采集端把源列转成模型字段(Kafka 里流转的就是模型字段)。
 *
 * <p>位点: SeaTunnel Zeta checkpoint(本地文件); 重启用同一 sea_tunnel_job_id 提交
 * → 从 checkpoint 恢复(崩溃续采)。
 */
@Slf4j
@Service
public class CdcStService {

    private final JdbcTemplate metaJdbc;
    private final DatasourceService datasourceService;
    private final SeaTunnelClient seaTunnelClient;
    private final String encKey;

    @Value("${dcp.sync.cdc-st.seq-db-url}")
    private String seqDbUrl;
    @Value("${dcp.sync.cdc-st.seq-db-user}")
    private String seqDbUser;
    @Value("${dcp.sync.cdc-st.seq-db-password}")
    private String seqDbPassword;
    @Value("${dcp.sync.cdc-st.kafka-bootstrap:kafka:9092}")
    private String kafkaBootstrap;
    @Value("${dcp.sync.cdc-st.seq-batch:1000}")
    private int seqBatch;

    /** SeaTunnel 作业内 table 名(连接 source→transform→sink 的显式接线) */
    private static final String SOURCE_TABLE = "cdc_source";
    private static final String OUTPUT_TABLE = "cdc_output";

    public CdcStService(JdbcTemplate metaJdbc, DatasourceService datasourceService,
                        SeaTunnelClient seaTunnelClient,
                        @org.springframework.beans.factory.annotation.Value("${dcp.enc-key}") String encKey) {
        this.metaJdbc = metaJdbc;
        this.datasourceService = datasourceService;
        this.seaTunnelClient = seaTunnelClient;
        this.encKey = encKey;
    }

    /** 任务注册(幂等 upsert, 按 (sync_task_id, logical_table, source_db, source_table));
     *  同 (逻辑,源) 不同 sync_task = 各自独立任务实例(独立 seq/写者列, 后任务可覆盖先任务)。
     *  syncTaskId<=0(逻辑表直建/脚本): 自动归属该(逻辑,源)已有实例的所属任务(防叠出 0 号重复实例),
     *  无已有实例则 0(直建实例, 每(逻辑,源)至多一条)。 */
    public void register(String logicalTable, Long datasourceId, String sourceDb, String sourceTable,
                         List<String> businessKeyFields, List<String> keyColumns, String topic,
                         String fieldMapping, String rowChain) {
        register(logicalTable, datasourceId, sourceDb, sourceTable, businessKeyFields, keyColumns,
                topic, fieldMapping, rowChain, 0L);
    }

    public void register(String logicalTable, Long datasourceId, String sourceDb, String sourceTable,
                         List<String> businessKeyFields, List<String> keyColumns, String topic,
                         String fieldMapping, String rowChain, long syncTaskId) {
        if (businessKeyFields == null || businessKeyFields.isEmpty()) {
            throw new IllegalArgumentException("businessKeyFields 不能为空");
        }
        if (keyColumns == null || keyColumns.isEmpty()) {
            throw new IllegalArgumentException("keyColumns 不能为空");
        }
        if (topic == null || topic.isBlank()) {
            String cfgTopic = null;
            try {
                cfgTopic = tableConfigTopic(logicalTable);
            } catch (Exception ignore) {
                // 未配置则用默认
            }
            topic = cfgTopic != null ? cfgTopic : "dcp." + logicalTable;
        }
        if (syncTaskId <= 0) {
            // 直建/脚本注册: 若该(逻辑,源)已被某 UI 任务实例占用 → 归属到它(保持实例唯一); 否则 0
            java.util.List<Long> owners = metaJdbc.queryForList(
                    "SELECT sync_task_id FROM cdc_st_task WHERE logical_table = ? AND source_db = ? AND source_table = ? LIMIT 1",
                    Long.class, logicalTable, sourceDb, sourceTable);
            if (!owners.isEmpty() && owners.get(0) != null && owners.get(0) > 0) {
                syncTaskId = owners.get(0);
            }
        }
        metaJdbc.update(
                "INSERT INTO cdc_st_task (logical_table, datasource_id, source_db, source_table, topic, "
                        + "business_key_fields, key_columns, field_mapping, row_chain, sync_task_id) "
                        + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                        + "ON DUPLICATE KEY UPDATE datasource_id=VALUES(datasource_id), "
                        + "topic=VALUES(topic), business_key_fields=VALUES(business_key_fields), "
                        + "key_columns=VALUES(key_columns), field_mapping=VALUES(field_mapping), "
                        + "row_chain=VALUES(row_chain)",
                logicalTable, datasourceId, sourceDb, sourceTable, topic,
                toJson(businessKeyFields), toJson(keyColumns), fieldMapping, rowChain, syncTaskId);
        log.info("SeaTunnel CDC 任务已注册: {} ← {}.{} topic={} syncTaskId={}", logicalTable,
                sourceDb, sourceTable, topic, syncTaskId);
    }

    /** 记录任务执行历史 */
    private void recordHistory(String logicalTable, String mode, String jobId, String action,
                               String status, String message) {
        try {
            if ("start".equals(action)) {
                metaJdbc.update(
                        "INSERT INTO cdc_run_history (logical_table, sync_mode, job_id, action, start_time, status, message) "
                                + "VALUES (?, ?, ?, ?, NOW(), ?, ?)",
                        logicalTable, mode, jobId, action, status == null ? "RUNNING" : status, message);
            } else {
                // 关闭上一次未结束的 RUNNING 记录, 并新写一条完成记录(或更新最新一条)
                metaJdbc.update(
                        "UPDATE cdc_run_history SET end_time = NOW(), status = ? WHERE logical_table = ? AND status = 'RUNNING'",
                        status == null ? "STOPPED" : status, logicalTable);
                metaJdbc.update(
                        "INSERT INTO cdc_run_history (logical_table, sync_mode, job_id, action, start_time, end_time, status, message) "
                                + "VALUES (?, ?, ?, ?, NOW(), NOW(), ?, ?)",
                        logicalTable, mode, jobId, action, status == null ? "STOPPED" : status, message);
            }
        } catch (Exception e) {
            log.warn("记录执行历史失败 {}: {}", logicalTable, e.getMessage());
        }
    }

    /** 同步模式配置(从 sync_task 读取, 任务维度; 缺省 full-cdc 常驻) */
    public record SyncMode(String syncMode, String fullScope, String startMode, String cronExpr, String startValue) {
        public boolean autoStop() {
            return "full-once".equals(syncMode) || "scheduled-batch".equals(syncMode)
                    || "full-scheduled".equals(syncMode);
        }
        public boolean snapshotOnly() {
            return "full-once".equals(syncMode) && "snapshot-only".equals(fullScope);
        }
    }

    /** 查任务(逻辑表)的模式配置: 优先 logical_table(任务级), 回退 sync_task 最新行 */
    public SyncMode modeOf(String logicalTable) {
        try {
            Map<String, Object> row = metaJdbc.queryForMap(
                    "SELECT sync_mode, full_scope, start_mode, cron_expr, start_value FROM logical_table "
                            + "WHERE table_name = ?", logicalTable);
            return new SyncMode(
                    String.valueOf(row.getOrDefault("sync_mode", "full-cdc")),
                    row.get("full_scope") == null ? null : String.valueOf(row.get("full_scope")),
                    String.valueOf(row.getOrDefault("start_mode", "earliest")),
                    row.get("cron_expr") == null ? null : String.valueOf(row.get("cron_expr")),
                    row.get("start_value") == null ? null : String.valueOf(row.get("start_value")));
        } catch (Exception e) {
            try {
                Map<String, Object> row = metaJdbc.queryForMap(
                        "SELECT sync_mode, full_scope, start_mode, cron_expr, start_value FROM sync_task "
                                + "WHERE logical_table = ? ORDER BY id DESC LIMIT 1", logicalTable);
                return new SyncMode(
                        String.valueOf(row.getOrDefault("sync_mode", "full-cdc")),
                        row.get("full_scope") == null ? null : String.valueOf(row.get("full_scope")),
                        String.valueOf(row.getOrDefault("start_mode", "earliest")),
                        row.get("cron_expr") == null ? null : String.valueOf(row.get("cron_expr")),
                        row.get("start_value") == null ? null : String.valueOf(row.get("start_value")));
            } catch (Exception ex) {
                return new SyncMode("full-cdc", null, "earliest", null, null);
            }
        }
    }

    /** 任务所属同步任务(sync_task)的同步模式; 直建(0)→逻辑表配置, 再无 → 默认 */
    private SyncMode modeOfTask(CdcStTask task) {
        if (task.getSyncTaskId() != null && task.getSyncTaskId() > 0) {
            try {
                Map<String, Object> row = metaJdbc.queryForMap(
                        "SELECT sync_mode, full_scope, start_mode, cron_expr, start_value FROM sync_task "
                                + "WHERE id = ?", task.getSyncTaskId());
                return new SyncMode(
                        String.valueOf(row.getOrDefault("sync_mode", "full-cdc")),
                        row.get("full_scope") == null ? null : String.valueOf(row.get("full_scope")),
                        String.valueOf(row.getOrDefault("start_mode", "earliest")),
                        row.get("cron_expr") == null ? null : String.valueOf(row.get("cron_expr")),
                        row.get("start_value") == null ? null : String.valueOf(row.get("start_value")));
            } catch (Exception ignore) {
                // 任务行被删等 → 退回逻辑表配置
            }
        }
        return modeOf(task.getLogicalTable());
    }

    /** 启动某逻辑表的<b>全部</b>采集任务(多表合并 = 每源表一个 SeaTunnel job); 重启复用已存 jobId 恢复 checkpoint */
    public synchronized Map<String, Object> start(String logicalTable) {
        List<CdcStTask> tasks = loadTasks(logicalTable);
        if (tasks.isEmpty()) {
            throw new IllegalArgumentException("任务未注册: " + logicalTable);
        }
        List<String> started = new ArrayList<>();
        for (CdcStTask task : tasks) {
            startOne(task, false);
            started.add(task.getSourceDb() + "." + task.getSourceTable());
        }
        return Map.of("logicalTable", logicalTable, "running", true, "sources", started, "count", started.size());
    }

    /** 启动单个任务实例(同(逻辑,源)多实例并存时精确控制); 模式取所属 sync_task */
    public synchronized Map<String, Object> startInstance(Long taskId) {
        CdcStTask task = loadTask(taskId);
        if (task == null) {
            throw new IllegalArgumentException("采集任务实例不存在: " + taskId);
        }
        startOne(task, false);
        return Map.of("logicalTable", task.getLogicalTable(), "taskId", taskId,
                "source", task.getSourceDb() + "." + task.getSourceTable(), "running", true);
    }

    /** 单实例启动(共享逻辑: 已在跑幂等跳过; 重启复用 jobId 恢复 checkpoint) */
    private void startOne(CdcStTask task, boolean quiet) {
        String logicalTable = task.getLogicalTable();
        // 已在跑 → 幂等跳过(避免重复提交同 jobId 被 Zeta 拒绝)
        if (task.getSeaTunnelJobId() != null && !task.getSeaTunnelJobId().isBlank()) {
            try {
                SeaTunnelClient.JobInfo info = seaTunnelClient.jobInfo(task.getSeaTunnelJobId());
                if ("RUNNING".equals(info.getJobStatus())) {
                    markOwnerRunning(task);
                    if (!quiet) {
                        log.info("SeaTunnel CDC 已在运行, 跳过: {} ← {}.{} jobId={}", logicalTable,
                                task.getSourceDb(), task.getSourceTable(), task.getSeaTunnelJobId());
                    }
                    return;
                }
            } catch (Exception ignore) {
                // jobInfo 查不到(引擎重启等) → 重新提交
            }
        }
        SyncMode mode = modeOfTask(task);
        boolean snapshotOnly = mode.snapshotOnly();
        // startup.mode: full-cdc/full-once=initial(快照+增量); incremental/scheduled-batch=start_mode(位点)
        String startupMode = ("incremental".equals(mode.syncMode()) || "scheduled-batch".equals(mode.syncMode()))
                ? mode.startMode() : "initial";
        DatasourceConfig ds = datasourceService.get(task.getDatasourceId());
        Map<String, Object> config = buildJobConfig(task, ds, "STREAMING", startupMode, snapshotOnly,
                ("incremental".equals(mode.syncMode()) || "scheduled-batch".equals(mode.syncMode()))
                        ? mode.startValue() : null, ds.getType());
        String jobId = task.getSeaTunnelJobId();
        boolean restart = jobId != null && !jobId.isBlank();
        if (!restart) {
            // SeaTunnel jobId 必须是数字; 时间戳*100+taskId 保证并发不撞
            jobId = String.valueOf(System.currentTimeMillis() * 100 + task.getTaskId());
        }
        String seaTunnelJobId = seaTunnelClient.submitJob(jobId, config, restart);
        metaJdbc.update("UPDATE cdc_st_task SET sea_tunnel_job_id = ?, running = 1 WHERE task_id = ?",
                seaTunnelJobId, task.getTaskId());
        markOwnerRunning(task);
        recordHistory(logicalTable, mode.syncMode(), seaTunnelJobId, "start", "RUNNING", null);
        log.info("SeaTunnel CDC 已启动: {} ← {}.{} topic={} jobId={} syncTaskId={}", logicalTable,
                task.getSourceDb(), task.getSourceTable(), task.getTopic(), seaTunnelJobId, task.getSyncTaskId());
    }

    /** 标记所属 sync_task 运行中(直建实例 syncTaskId=0 不更新任务配置) */
    private void markOwnerRunning(CdcStTask task) {
        if (task.getSyncTaskId() != null && task.getSyncTaskId() > 0) {
            try {
                metaJdbc.update("UPDATE sync_task SET run_status = 'RUNNING' WHERE id = ?", task.getSyncTaskId());
            } catch (Exception e) {
                log.warn("更新 sync_task 运行状态失败: {}", e.getMessage());
            }
        }
    }

    private void markOwnerIdle(CdcStTask task) {
        if (task.getSyncTaskId() != null && task.getSyncTaskId() > 0) {
            try {
                metaJdbc.update("UPDATE sync_task SET run_status = 'IDLE' WHERE id = ?", task.getSyncTaskId());
            } catch (Exception e) {
                log.warn("更新 sync_task 运行状态失败: {}", e.getMessage());
            }
        }
    }

    /** 停止某逻辑表的全部采集任务(优雅停止, checkpoint 落盘, 下次启动同 jobId 续采) */
    public synchronized Map<String, Object> stop(String logicalTable) {
        for (CdcStTask task : loadTasks(logicalTable)) {
            stopOne(task);
        }
        return Map.of("logicalTable", logicalTable, "running", false);
    }

    /** 停止单个任务实例 */
    public synchronized Map<String, Object> stopInstance(Long taskId) {
        CdcStTask task = loadTask(taskId);
        if (task == null) {
            throw new IllegalArgumentException("采集任务实例不存在: " + taskId);
        }
        stopOne(task);
        return Map.of("logicalTable", task.getLogicalTable(), "taskId", taskId,
                "source", task.getSourceDb() + "." + task.getSourceTable(), "running", false);
    }

    private void stopOne(CdcStTask task) {
        String logicalTable = task.getLogicalTable();
        if (task.getSeaTunnelJobId() != null && !task.getSeaTunnelJobId().isBlank()) {
            try {
                seaTunnelClient.stopJob(task.getSeaTunnelJobId());
            } catch (Exception e) {
                log.warn("停止 SeaTunnel job 失败 {} ({}): {}", logicalTable, task.getSourceTable(), e.getMessage());
            }
        }
        metaJdbc.update("UPDATE cdc_st_task SET running = 0 WHERE task_id = ?", task.getTaskId());
        markOwnerIdle(task);
        recordHistory(logicalTable, modeOfTask(task).syncMode(), task.getSeaTunnelJobId(),
                "stop", "STOPPED", null);
    }

    private CdcStTask loadTask(Long taskId) {
        java.util.List<CdcStTask> l = metaJdbc.query("SELECT logical_table, datasource_id, source_db, source_table, topic, "
                        + "business_key_fields, key_columns, bucket_count, field_mapping, row_chain, sea_tunnel_job_id, task_id, sync_task_id "
                        + "FROM cdc_st_task WHERE task_id = ?",
                (rs, i) -> mapTask(rs), taskId);
        return l.isEmpty() ? null : l.get(0);
    }

    /** 状态: 全部任务 + SeaTunnel 侧实时状态(未运行/运行中) */
    public List<Map<String, Object>> status() {
        List<Map<String, Object>> out = new ArrayList<>();
        metaJdbc.query("SELECT ct.logical_table, ct.datasource_id, ct.source_db, ct.source_table, ct.topic, "
                        + "ct.business_key_fields, ct.key_columns, ct.bucket_count, ct.field_mapping, ct.row_chain, "
                        + "ct.sea_tunnel_job_id, ct.running, ct.sync_task_id, ct.task_id, st.name AS owner_name "
                        + "FROM cdc_st_task ct LEFT JOIN sync_task st ON st.id = ct.sync_task_id AND ct.sync_task_id > 0 "
                        + "ORDER BY ct.logical_table, ct.task_id",
                (org.springframework.jdbc.core.RowCallbackHandler) rs -> {
                    Map<String, Object> m = new LinkedHashMap<>();
                    m.put("logicalTable", rs.getString("logical_table"));
                    m.put("sourceDb", rs.getString("source_db"));
                    m.put("sourceTable", rs.getString("source_table"));
                    m.put("source", rs.getString("source_db") + "." + rs.getString("source_table"));
                    m.put("taskId", rs.getLong("task_id"));
                    long ownerId = rs.getLong("sync_task_id");
                    m.put("syncTaskId", ownerId);
                    m.put("ownerName", ownerId > 0 ? rs.getString("owner_name") : null);
                    m.put("topic", rs.getString("topic"));
                    m.put("businessKeyFields", fromJsonList(rs.getString("business_key_fields")));
                    m.put("keyColumns", fromJsonList(rs.getString("key_columns")));
                    m.put("bucketCount", rs.getInt("bucket_count"));
                    m.put("fieldMapping", rs.getString("field_mapping"));
                    m.put("rowChain", rs.getString("row_chain"));
                    String stJobId = rs.getString("sea_tunnel_job_id");
                    m.put("seaTunnelJobId", stJobId);
                    boolean localRunning = rs.getInt("running") == 1;
                    boolean live = false;
                    if (stJobId != null && !stJobId.isBlank()) {
                        try {
                            SeaTunnelClient.JobInfo info = seaTunnelClient.jobInfo(stJobId);
                            live = "RUNNING".equals(info.getJobStatus());
                        } catch (Exception ignore) {
                            // 引擎不可达时按本地状态
                        }
                    }
                    m.put("running", localRunning && live);
                    out.add(m);
                });
        return out;
    }


    /**
     * 探测源表列结构并生成 SeaTunnel schema 配置(绕开 2.3.12 CDC catalog 发现缺陷)。
     *
     * <p>返回格式: {fields: {col: {type: "string"|...}}} 供 source 配置的 schema 使用。
     * 目前支持 PostgreSQL(其他类型可扩展)。
     */
    private Map<String, Object> probeSchema(String type, DatasourceConfig ds, CdcStTask task, String pwd) {
        Map<String, Object> fields = new LinkedHashMap<>();
        String url = switch (type) {
            case "postgresql" -> "jdbc:postgresql://%s:%d/%s".formatted(ds.getHost(), ds.getPort(), task.getSourceDb());
            case "sqlserver" -> "jdbc:sqlserver://%s:%d;databaseName=%s;encrypt=false".formatted(ds.getHost(), ds.getPort(), task.getSourceDb());
            case "oracle" -> "jdbc:oracle:thin:@//%s:%d/%s".formatted(ds.getHost(), ds.getPort(), task.getSourceDb());
            default -> throw new IllegalArgumentException("probeSchema 不支持: " + type);
        };
        try (java.sql.Connection conn = java.sql.DriverManager.getConnection(url, ds.getUsername(), pwd)) {
            java.sql.DatabaseMetaData md = conn.getMetaData();
            try (java.sql.ResultSet rs = md.getColumns(null, null, task.getSourceTable(), "%")) {
                while (rs.next()) {
                    String col = rs.getString("COLUMN_NAME");
                    String typeName = rs.getString("TYPE_NAME");
                    // fields 值直接写类型字符串(SeaTunnel schema 格式), 写成 {type:xx} 会被解析成 ROW 嵌套类型
                    fields.put(col, seaTunnelType(type, typeName));
                }
            }
        } catch (Exception e) {
            log.warn("探测源表结构失败({}.{}), 不提供 schema: {}", task.getSourceDb(), task.getSourceTable(), e.getMessage());
            return null;
        }
        if (fields.isEmpty()) {
            return null;
        }
        return Map.of("fields", fields);
    }

    /** 数据库列类型 → SeaTunnel 类型 */
    private static String seaTunnelType(String dbType, String typeName) {
        String t = typeName == null ? "" : typeName.toLowerCase();
        if (t.contains("int") || t.contains("serial") || t.contains("number")) {
            return "long";
        }
        if (t.contains("decimal") || t.contains("numeric") || t.contains("double") || t.contains("float") || t.contains("real")) {
            return "double";
        }
        if (t.contains("timestamp") || t.contains("datetime")) {
            return "timestamp";
        }
        if (t.contains("date")) {
            return "date";
        }
        if (t.contains("bool")) {
            return "boolean";
        }
        return "string";
    }

    /** 按数据源类型构建 CDC source 配置(2026-08-31 扩展: MySQL/PG/SQLServer/MongoDB/Oracle) */
    private Map<String, Object> buildSource(CdcStTask task, DatasourceConfig ds,
                                String startupMode, boolean snapshotOnly, String startValue, String dsType) {
        String type = ds.getType() == null ? "mysql" : ds.getType().toLowerCase();
        String pwd = com.dcp.common.CryptoUtil.decrypt(ds.getPasswordCiphertext(), encKey);
        Map<String, Object> s = new LinkedHashMap<>();
        switch (type) {
            case "mysql" -> {
                s.put("plugin_name", "MySQL-CDC");
                s.put("url", "jdbc:mysql://%s:%d/%s?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Asia/Shanghai&characterEncoding=utf8&useUnicode=true"
                        .formatted(ds.getHost(), ds.getPort(), task.getSourceDb()));
                s.put("username", ds.getUsername());
                s.put("password", pwd);
                s.put("database-names", List.of(task.getSourceDb()));
                s.put("table-names", List.of(task.getSourceDb() + "." + task.getSourceTable()));
                // server-id 必须每 job 唯一(同实例多 job 复用会互踢 → 循环重连重跑快照)
                long sidBase = 5400 + task.getTaskId() * 10;
                s.put("server-id", sidBase + "-" + (sidBase + 4));
                s.put("startup.mode", startupMode);
            }
            case "postgresql" -> {
                s.put("plugin_name", "Postgres-CDC");
                s.put("url", "jdbc:postgresql://%s:%d/%s".formatted(ds.getHost(), ds.getPort(), task.getSourceDb()));
                s.put("username", ds.getUsername());
                s.put("password", pwd);
                s.put("database-names", List.of(task.getSourceDb()));
                s.put("schema-name", List.of("public"));
                // PG: table-names 用于表发现(include list), table 用于 CatalogTable 表名(必须三段与 TableId 对齐)
                s.put("table-names", List.of("public." + task.getSourceTable()));
                // table(单数)三段 + schema_first=false → TablePath 按 db.schema.table 解析, 与 discoverTables 的 TableId(postgres,public,t) 对齐
                s.put("table", task.getSourceDb() + ".public." + task.getSourceTable());
                s.put("schema_first", false);
                s.put("server-time-zone", "Asia/Shanghai");
                // 逻辑复制: 每 job 独立 slot(名字唯一, 重启同 slot 续采); pgoutput 为 PG10+ 内置解码
                s.put("slot.name", "dcp_slot_" + task.getTaskId());
                s.put("decoding.plugin.name", "pgoutput");
                s.put("startup.mode", startupMode);
                // 2.3.12 PG-CDC 的 catalog 发现缺陷(SPI 无 CatalogFactory 注册) → 显式提供表结构
                // 表名必须在 schema 子配置里(buildWithConfig 从 schema 读 table/schema_first, 根配置的 table 不生效)
                Map<String, Object> pgSchema = new LinkedHashMap<>(probeSchema(type, ds, task, pwd));
                pgSchema.put("table", task.getSourceDb() + ".public." + task.getSourceTable());
                pgSchema.put("schema_first", false);
                s.put("schema", pgSchema);
            }
            case "sqlserver" -> {
                s.put("plugin_name", "SqlServer-CDC");
                s.put("url", "jdbc:sqlserver://%s:%d;databaseName=%s;encrypt=false"
                        .formatted(ds.getHost(), ds.getPort(), task.getSourceDb()));
                s.put("username", ds.getUsername());
                s.put("password", pwd);
                s.put("database-names", List.of(task.getSourceDb()));
                s.put("table-names", List.of(task.getSourceDb() + ".dbo." + task.getSourceTable()));
                s.put("startup.mode", startupMode);
            }
            case "oracle" -> {
                s.put("plugin_name", "Oracle-CDC");
                // LogMiner 必须在 CDB 执行(PDB 内报 ORA-65040): url 指向 CDB(数据源 databaseName),
                // database-names=[CDB, PDB];PDB 经 debezium 透传 database.pdb.name(SeaTunnel CDC 官方机制),
                // 引擎按此连接 CDB 后 ALTER SESSION 切 PDB 读表,LogMiner 留在 CDB
                String cdbName = ds.getDatabaseName() == null || ds.getDatabaseName().isBlank()
                        ? task.getSourceDb() : ds.getDatabaseName();
                s.put("url", "jdbc:oracle:thin:@//%s:%d/%s".formatted(ds.getHost(), ds.getPort(), cdbName));
                s.put("username", ds.getUsername());
                s.put("password", pwd);
                s.put("database-names", List.of(task.getSourceDb(), task.getSourceDb()));
                s.put("debezium", java.util.Map.of("database.pdb.name", task.getSourceDb()));
                // Oracle table-names = schema.table; 对象名全大写(DCP.T_ORACLE_USER);
                // 连接用户若是 common user(c##xxx), PDB 内实际 schema 名去掉 c## 前缀
                String oracleSchemaName = ds.getUsername().toUpperCase();
                if (oracleSchemaName.startsWith("C##")) {
                    oracleSchemaName = oracleSchemaName.substring(3);
                }
                s.put("table-names", List.of(oracleSchemaName + "." + task.getSourceTable()));
                s.put("startup.mode", startupMode);
                // 2.3.12 Oracle-CDC catalog 发现同 PG 缺陷 → 显式提供表结构(probeSchema 用 JDBC 探测)
                Map<String, Object> oracleSchema = probeSchema(type, ds, task, pwd);
                if (oracleSchema != null) {
                    oracleSchema = new LinkedHashMap<>(oracleSchema);
                    oracleSchema.put("table", task.getSourceDb() + "." + oracleSchemaName + "." + task.getSourceTable());
                    oracleSchema.put("schema_first", false);
                    s.put("schema", oracleSchema);
                }
            }
            case "mongodb" -> {
                s.put("plugin_name", "MongoDB-CDC");
                s.put("hosts", ds.getHost() + ":" + ds.getPort());
                // 演示 mongo 为单节点副本集 + 无认证(副本集+认证需 keyFile); 生产按需开启认证
                s.put("database", List.of(task.getSourceDb()));
                // Mongo collection include list 用完整名 db.collection(collectionNames 拼接后匹配)
                s.put("collection", List.of(task.getSourceDb() + "." + task.getSourceTable()));
                s.put("startup.mode", startupMode);
                // 2.3.12 Mongo-CDC 快照 fetcher 竞态(数据入队与 poll 错位会误判 split 完成丢数据) → exactly_once 模式忙等到完整水位
                s.put("exactly_once", true);
                // MongoDB-CDC 的 schema/tables_configs 必须配一个(exclusive 语义);
                // Mongo 无 JDBC 探测 → 从 field_mapping 的 modelField 推导字段类型(名称启发式)
                Map<String, Object> mongoSchema = buildMongoSchema(task);
                if (mongoSchema != null) {
                    s.put("schema", mongoSchema);
                }
            }
            default -> throw new IllegalArgumentException("不支持的 CDC 源类型: " + type);
        }
        // 精确起始位点: timestamp 通用; specific-offsets(MySQL binlog file:pos)
        if (startValue != null && !startValue.isBlank()) {
            if ("timestamp".equalsIgnoreCase(startupMode)) {
                s.put("startup.timestamp", startValue);   // epoch ms
            } else if ("specific-offsets".equalsIgnoreCase(startupMode) && "mysql".equalsIgnoreCase(dsType)) {
                String[] fp = startValue.split(":", 2);
                s.put("startup.specific-offset.file", fp[0]);
                if (fp.length > 1) {
                    s.put("startup.specific-offset.pos", fp[1]);
                }
            }
        }
        return s;
    }

    /** 构造 SeaTunnel 作业配置: MySQL-CDC → CdcSeqTransform(含字段映射)→ Kafka */
    private Map<String, Object> buildJobConfig(CdcStTask task, DatasourceConfig ds,
                                               String jobMode, String startupMode, boolean snapshotOnly,
                                               String startValue, String dsType) {
        Map<String, Object> env = new LinkedHashMap<>();
        env.put("job.mode", jobMode);
        env.put("job.name", "cdc-st-" + task.getLogicalTable() + "-" + task.getSourceTable());
        env.put("parallelism", 1);
        env.put("checkpoint.interval", 10000);

        Map<String, Object> source = buildSource(task, ds, startupMode, snapshotOnly, startValue, dsType);
        // 纯快照模式: Debezium snapshot.mode=initial_only(快照完 connector 结束, job 自然完成)
        if (snapshotOnly) {
            source.put("debezium", java.util.Map.of("snapshot.mode", "initial_only"));
        }
        source.put("result_table_name", SOURCE_TABLE);

        Map<String, Object> transform = new LinkedHashMap<>();
        transform.put("plugin_name", "CdcSeqTransform");
        transform.put("source_table_name", SOURCE_TABLE);
        transform.put("result_table_name", OUTPUT_TABLE);
        transform.put("logical_table", task.getLogicalTable());
        if (task.getTaskId() != null) {
            transform.put("task_id", String.valueOf(task.getTaskId()));
        }
        transform.put("datasource_id", String.valueOf(task.getDatasourceId()));
        transform.put("source_db", task.getSourceDb());
        transform.put("source_table", task.getSourceTable());
        transform.put("business_key_fields", task.getBusinessKeyFields());
        transform.put("key_columns", task.getKeyColumns());
        transform.put("bucket_count", task.getBucketCount());
        transform.put("null_key_mode", "ERROR");
        transform.put("seq_db_url", seqDbUrl);
        transform.put("seq_db_user", seqDbUser);
        transform.put("seq_db_password", seqDbPassword);
        transform.put("seq_batch", seqBatch);
        if (task.getFieldMapping() != null && !task.getFieldMapping().isBlank()) {
            transform.put("field_mapping", task.getFieldMapping());
        }
        if (task.getRowChain() != null && !task.getRowChain().isBlank()) {
            transform.put("row_chain", task.getRowChain());
        }

        Map<String, Object> sink = new LinkedHashMap<>();
        sink.put("plugin_name", "Kafka");
        sink.put("bootstrap.servers", kafkaBootstrap);
        sink.put("topic", task.getTopic());
        sink.put("format", "json");
        sink.put("partition_key_fields", List.of("__bucket"));
        sink.put("semantics", "AT_LEAST_ONCE");
        sink.put("source_table_name", OUTPUT_TABLE);

        Map<String, Object> root = new LinkedHashMap<>();
        root.put("env", env);
        root.put("source", List.of(source));
        root.put("transform", List.of(transform));
        root.put("sink", List.of(sink));
        return root;
    }

    private String tableConfigTopic(String logicalTable) {
        try {
            return metaJdbc.queryForObject(
                    "SELECT topic FROM logical_table WHERE table_name = ?", String.class, logicalTable);
        } catch (Exception e) {
            return null;
        }
    }

    /** 加载某逻辑表的全部采集任务 */
    private List<CdcStTask> loadTasks(String logicalTable) {
        return metaJdbc.query("SELECT logical_table, datasource_id, source_db, source_table, topic, "
                        + "business_key_fields, key_columns, bucket_count, field_mapping, row_chain, sea_tunnel_job_id, task_id, sync_task_id "
                        + "FROM cdc_st_task WHERE logical_table = ? ORDER BY task_id",
                (rs, i) -> mapTask(rs), logicalTable);
    }

    private CdcStTask mapTask(java.sql.ResultSet rs) throws java.sql.SQLException {
        CdcStTask t = new CdcStTask();
        t.setLogicalTable(rs.getString(1));
        t.setDatasourceId(rs.getLong(2));
        t.setSourceDb(rs.getString(3));
        t.setSourceTable(rs.getString(4));
        t.setTopic(rs.getString(5));
        t.setBusinessKeyFields(fromJsonList(rs.getString(6)));
        t.setKeyColumns(fromJsonList(rs.getString(7)));
        t.setBucketCount(rs.getInt(8));
        t.setFieldMapping(rs.getString(9));
        t.setRowChain(rs.getString(10));
        t.setSeaTunnelJobId(rs.getString(11));
        t.setTaskId(rs.getLong(12));
        t.setSyncTaskId(rs.getLong(13));
        return t;
    }

    private static String toJson(Object o) {
        try {
            return new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(o);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @SuppressWarnings("unchecked")
    private static List<String> fromJsonList(String s) {
        if (s == null || s.isBlank()) {
            return List.of();
        }
        try {
            return new com.fasterxml.jackson.databind.ObjectMapper().readValue(s, List.class);
        } catch (Exception e) {
            return List.of();
        }
    }

    /** MongoDB: 从 field_mapping 的 modelField 推导 schema 字段(无 JDBC 可探测, 名称启发式类型) */
    private static Map<String, Object> buildMongoSchema(CdcStTask task) {
        if (task.getFieldMapping() == null || task.getFieldMapping().isBlank()) {
            return null;
        }
        try {
            List<?> list = new com.fasterxml.jackson.databind.ObjectMapper()
                    .readValue(task.getFieldMapping(), List.class);
            Map<String, Object> fields = new LinkedHashMap<>();
            for (Object o : list) {
                if (!(o instanceof Map<?, ?> m)) {
                    continue;
                }
                Object modelField = m.get("modelField");
                if (modelField == null) {
                    continue;
                }
                String f = modelField.toString();
                fields.put(f, guessMongoType(f));
            }
            if (fields.isEmpty()) {
                return null;
            }
            // 表名必须显式配在 schema 里(buildWithConfig 用 schema 的 table 生成 tablePath);
            // 否则 fallback 到 result_table_name(cdc_source), 与 deserialize 从 ns 提取的 db.collection 不匹配 → 数据被丢弃
            Map<String, Object> schema = new LinkedHashMap<>();
            schema.put("table", task.getSourceDb() + "." + task.getSourceTable());
            schema.put("fields", fields);
            return schema;
        } catch (Exception e) {
            return null;
        }
    }

    /** Mongo 字段类型启发式: age/num/count→long, at/time→timestamp, date→date, is_/has_/enabled→boolean, 其他→string */
    private static String guessMongoType(String f) {
        String t = f.toLowerCase();
        if (t.equals("age") || t.endsWith("_num") || t.endsWith("_count")
                || t.endsWith("_total") || t.endsWith("_qty")) {
            return "long";
        }
        if (t.endsWith("_at") || t.endsWith("_time") || t.contains("timestamp")) {
            return "timestamp";
        }
        if (t.endsWith("_date") || t.equals("date")) {
            return "date";
        }
        if (t.startsWith("is_") || t.startsWith("has_") || t.equals("enabled")
                || t.equals("active") || t.equals("deleted")) {
            return "boolean";
        }
        return "string";
    }

    /** cdc_st_task 行 */
    @Data
    public static class CdcStTask {
        private Long taskId;
        private String logicalTable;
        private Long datasourceId;
        private String sourceDb;
        private String sourceTable;
        private String topic;
        private List<String> businessKeyFields;
        private List<String> keyColumns;
        private int bucketCount = 4096;
        private String fieldMapping;
        private String rowChain;
        private String seaTunnelJobId;
        private Long syncTaskId;
    }
}
