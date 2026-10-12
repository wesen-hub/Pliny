package com.dcp.sync;

import com.dcp.engine.SeaTunnelClient;
import com.dcp.module.datasource.DatasourceService;
import com.dcp.module.datasource.entity.DatasourceConfig;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
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
    private final CurrentPositionReader currentPositionReader;
    private final CdcTaskDispatchService taskDispatchService;
    private final BackfillService backfillService;
    private final String encKey;

    @Value("${dcp.sync.cdc-st.alert-db-url}")
    private String alertDbUrl;
    @Value("${dcp.sync.cdc-st.alert-db-user}")
    private String alertDbUser;
    @Value("${dcp.sync.cdc-st.alert-db-password}")
    private String alertDbPassword;
    @Value("${dcp.sync.cdc-st.kafka-bootstrap:kafka:9092}")
    private String kafkaBootstrap;
    /** 巡检自动 restore 开关(待办1: H14 实证 master 宕机后 job 消失不自动恢复) */
    @Value("${dcp.sync.cdc-st.watchdog.enabled:true}")
    private boolean watchdogEnabled;

    public CdcStService(JdbcTemplate metaJdbc, DatasourceService datasourceService,
                        SeaTunnelClient seaTunnelClient,
                        CurrentPositionReader currentPositionReader,
                        CdcTaskDispatchService taskDispatchService,
                        BackfillService backfillService,
                        @org.springframework.beans.factory.annotation.Value("${dcp.enc-key}") String encKey) {
        this.metaJdbc = metaJdbc;
        this.datasourceService = datasourceService;
        this.seaTunnelClient = seaTunnelClient;
        this.currentPositionReader = currentPositionReader;
        this.taskDispatchService = taskDispatchService;
        this.backfillService = backfillService;
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
        // 注册强校验(新方案): 映射非空/D2/保留列/kind 约束, 直建/脚本路径与 UI 统一
        List<String> mappingIssues = SyncMappingRules.validateStructure(SyncMappingRules.parseMapping(fieldMapping));
        if (!mappingIssues.isEmpty()) {
            throw new IllegalArgumentException("字段映射校验失败: " + String.join("; ", mappingIssues));
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
        // 后加列检测(回补登记): 任务已存在时对比旧映射, 新增 field-map 源列 → 登记字段级回补
        // (同任务同 source checkpoint 恢复后续采增量, 该列存量永不重采 → 必须回补)
        String oldFieldMapping = null;
        try {
            List<String> old = metaJdbc.queryForList(
                    "SELECT field_mapping FROM cdc_st_task WHERE logical_table = ? AND source_db = ? "
                            + "AND source_table = ? LIMIT 1",
                    String.class, logicalTable, sourceDb, sourceTable);
            if (!old.isEmpty()) {
                oldFieldMapping = old.get(0);
            }
        } catch (Exception ignore) {
            // 查不到旧行 = 新任务, 快照兜底
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
        // N3/N8: 写者列名校验——任务落库后拿 taskId 生成写者列名, 与业务列/其他任务写者列查重 fail-fast
        Long taskId = metaJdbc.queryForObject(
                "SELECT task_id FROM cdc_st_task WHERE logical_table = ? AND source_db = ? AND source_table = ?",
                Long.class, logicalTable, sourceDb, sourceTable);
        validateWriterCol(logicalTable, String.valueOf(taskId), String.valueOf(datasourceId),
                sourceDb, sourceTable, SyncMappingRules.parseMapping(fieldMapping));
        // 后加列 → 字段级回补登记(新任务 oldFieldMapping 为空, 由快照兜底不登记)
        backfillService.registerBackfillsIfNeeded(logicalTable, syncTaskId, taskId, datasourceId,
                sourceDb, sourceTable, oldFieldMapping, fieldMapping);
        // 一任务一 job(2026-10-12 拍板): 注册即分配独立 worker(最闲) + 独立 server-id 区间(池领取,
        // 终身跟随);任务已在运行 → 停旧启新单任务重提(同 jobId 恢复 checkpoint 续采)
        String workerId = taskDispatchService.pickLeastLoadedWorker();
        if (workerId == null) {
            workerId = "default";
        }
        metaJdbc.update("UPDATE cdc_st_task SET worker_id = ? WHERE task_id = ?", workerId, taskId);
        taskDispatchService.ensureRangeForTask(taskId, null);
        log.info("SeaTunnel CDC 任务已注册: {} ← {}.{} topic={} syncTaskId={} worker={}", logicalTable,
                sourceDb, sourceTable, topic, syncTaskId, workerId);
        try {
            Integer running = metaJdbc.queryForObject(
                    "SELECT running FROM cdc_st_task WHERE task_id = ?", Integer.class, taskId);
            if (running != null && running == 1) {
                restartTask(taskId);
            }
        } catch (Exception e) {
            // 注册已成功(配置落库), 停旧启新失败由 watchdog 判死兜底重提
            log.warn("任务重提失败(task={}), watchdog 将兜底重试: {}", taskId, e.getMessage());
        }
    }

    /** 任务是否在运行(任务行 running 标记) */
    private boolean taskRunning(long taskId) {
        Integer n = metaJdbc.queryForObject(
                "SELECT running FROM cdc_st_task WHERE task_id = ?", Integer.class, taskId);
        return n != null && n == 1;
    }

    /**
     * N3(B2')/N8: 写者列名注册查重——
     * ① 写者列名不得与本任务映射输出列(modelField)重名(否则写库 ODKU 撞业务列);
     * ② 写者列名不得与库内其他(任务×源)实例清洗后同名(md5 截断碰撞);
     * ③ ID 永不复用由 task_id/datasource_id AUTO_INCREMENT 不回退天然保证(治理纪律: 禁止重置自增)。
     */
    private void validateWriterCol(String logicalTable, String taskId, String datasourceId,
                                   String sourceDb, String sourceTable, List<Map<String, Object>> fieldMapping) {
        String writer = SyncMappingRules.writerColOf(taskId, datasourceId, sourceDb, sourceTable);
        if (writer == null) {
            return;
        }
        List<String> modelFields = new ArrayList<>();
        for (Map<String, Object> row : fieldMapping) {
            Object mf = row.get("modelField");
            if (mf != null && !String.valueOf(mf).isBlank()) {
                modelFields.add(String.valueOf(mf));
            }
        }
        if (modelFields.contains(writer)) {
            throw new IllegalArgumentException("写者列名 " + writer
                    + " 与业务列重名(写者列 = 目标表 seq 裁判列, 业务列禁止同名): " + logicalTable);
        }
        List<Map<String, Object>> others = metaJdbc.queryForList(
                "SELECT task_id, datasource_id, source_db, source_table FROM cdc_st_task WHERE logical_table = ?",
                logicalTable);
        for (Map<String, Object> other : others) {
            if (String.valueOf(other.get("task_id")).equals(taskId)
                    && String.valueOf(other.getOrDefault("datasource_id", "")).equals(datasourceId)
                    && String.valueOf(other.getOrDefault("source_db", "")).equals(sourceDb)
                    && String.valueOf(other.getOrDefault("source_table", "")).equals(sourceTable)) {
                continue;
            }
            String otherWriter = SyncMappingRules.writerColOf(String.valueOf(other.get("task_id")),
                    String.valueOf(other.getOrDefault("datasource_id", "")),
                    String.valueOf(other.getOrDefault("source_db", "")),
                    String.valueOf(other.getOrDefault("source_table", "")));
            if (writer.equals(otherWriter)) {
                throw new IllegalArgumentException("写者列名碰撞(md5 截断同号): " + writer
                        + " 与逻辑表 " + logicalTable + " 内另一任务(源 " + other.get("source_db") + "."
                        + other.get("source_table") + ")冲突, 请调整任务/数据源 ID");
            }
        }
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

    /** 启动某逻辑表的<b>全部</b>采集任务(一任务一 job: 逐任务独立启, 互不牵连) */
    public synchronized Map<String, Object> start(String logicalTable) {
        List<CdcStTask> tasks = loadTasks(logicalTable);
        if (tasks.isEmpty()) {
            throw new IllegalArgumentException("任务未注册: " + logicalTable);
        }
        List<Long> started = new ArrayList<>();
        for (CdcStTask t : tasks) {
            startTask(t.getTaskId());
            started.add(t.getTaskId());
        }
        return Map.of("logicalTable", logicalTable, "running", true, "taskIds", started, "count", started.size());
    }

    /** 启动单个任务实例(同(逻辑,源)多实例并存时精确控制): 单任务独立启动; 模式取所属 sync_task */
    public synchronized Map<String, Object> startInstance(Long taskId) {
        CdcStTask task = loadTask(taskId);
        if (task == null) {
            throw new IllegalArgumentException("采集任务实例不存在: " + taskId);
        }
        startTask(taskId);
        return Map.of("logicalTable", task.getLogicalTable(), "taskId", taskId,
                "source", task.getSourceDb() + "." + task.getSourceTable(), "running", true);
    }

    // ───────────────── 单任务启动(一任务一 job) ─────────────────

    /** 启动准备产物: 每表一条流水线的已计算参数(源表/模式/位点) */
    private record PreparedTask(CdcStTask task, DatasourceConfig ds, String dsType,
                                String startupMode, boolean snapshotOnly, String startValue,
                                String snapshotStartPosition) {
        PreparedTask withStartupMode(String mode) {
            return new PreparedTask(task, ds, dsType, mode, snapshotOnly, startValue, snapshotStartPosition);
        }
    }

    /** 启动准备(每表): 模式计算 + 快照起点位点(S 恒定) + 缺列告警 */
    private PreparedTask prepareTaskForStart(CdcStTask task) {
        SyncMode mode = modeOfTask(task);
        boolean snapshotOnly = mode.snapshotOnly();
        boolean incrementalStart = "incremental".equals(mode.syncMode()) || "scheduled-batch".equals(mode.syncMode());
        // startup.mode: full-cdc/full-once=initial(快照+增量); incremental/scheduled-batch=start_mode(位点)
        String startupMode = incrementalStart ? mode.startMode() : "initial";
        DatasourceConfig ds = datasourceService.get(task.getDatasourceId());
        String dsType = ds.getType() == null ? "mysql" : ds.getType().toLowerCase();
        // 快照起点位点(方案 A, 设计文档 §2.2): 快照行位点实测为空形态("mysql::0:"),
        // 任务提交前读源库当前位点下发为 snapshot_start_position 编码 S;
        // 已持久化(重启/重放)则复用, 保证 S 恒定(S < 一切流号, 衔接不等式成立)
        boolean needsSnapshot = "initial".equalsIgnoreCase(startupMode) || snapshotOnly;
        String snapshotStartPosition = null;
        if (needsSnapshot) {
            snapshotStartPosition = task.getSnapshotStartPosition();
            if (snapshotStartPosition == null || snapshotStartPosition.isBlank()) {
                String pwd = com.dcp.common.CryptoUtil.decrypt(ds.getPasswordCiphertext(), encKey);
                snapshotStartPosition = currentPositionReader.read(dsType, ds, pwd, task.getSourceDb());
                metaJdbc.update("UPDATE cdc_st_task SET snapshot_start_position = ? WHERE task_id = ?",
                        snapshotStartPosition, task.getTaskId());
                task.setSnapshotStartPosition(snapshotStartPosition);
                log.info("SeaTunnel CDC 快照起点位点已下发并持久化: {} ← {}.{} snap={}",
                        task.getLogicalTable(), task.getSourceDb(), task.getSourceTable(), snapshotStartPosition);
            }
        }
        // 启动级缺列检测(新方案 D6): 映射引用源列已不存在 → 写 sync_alert(missing_column)告警,
        // 不 fail-fast(在线减列后任务正常跑, 重启后照常起), 继续 null 更新
        alertMissingColumns(task, ds);
        return new PreparedTask(task, ds, dsType, startupMode, snapshotOnly,
                incrementalStart ? mode.startValue() : null, snapshotStartPosition);
    }

    /**
     * 启动单任务: 一任务一 job(单 source/transform/sink);
     * 已在跑幂等跳过; 重启复用任务 jobId 恢复 checkpoint; 提交定向到任务绑定的 worker REST 入口。
     */
    private void startTask(long taskId) {
        CdcStTask task = loadTask(taskId);
        if (task == null) {
            throw new IllegalStateException("采集任务不存在: " + taskId);
        }
        String baseUrl = taskDispatchService.workerUrl(task.getWorkerId());
        if (baseUrl == null) {
            baseUrl = taskDispatchService.defaultWorkerUrl();
        }
        String taskJobId = task.getSeaTunnelJobId();
        boolean hasJobId = taskJobId != null && !taskJobId.isBlank();
        // 已在跑 → 幂等跳过(避免重复提交同 jobId 被 Zeta 拒绝)
        if (hasJobId) {
            try {
                SeaTunnelClient.JobInfo info = seaTunnelClient.jobInfo(baseUrl, taskJobId);
                if ("RUNNING".equals(info.getJobStatus())) {
                    markTaskRunning(taskId, taskJobId, task);
                    log.info("任务已在运行, 跳过: task={} jobId={}", taskId, taskJobId);
                    return;
                }
            } catch (Exception ignore) {
                // jobInfo 查不到(引擎重启等) → 重新提交
            }
        }
        // server-id 区间(注册时已领取; 存量任务缺区间 → 启动时补领)
        String range = taskDispatchService.ensureRangeForTask(taskId, task.getServerIdRange());
        // 位点准备 + 缺列告警
        PreparedTask prepared = prepareTaskForStart(task);
        if (!hasJobId) {
            // 启停纪律(问题记录二.2): 无 savepoint(任务 jobId 无 checkpoint)且位点模式 latest
            // → 强制 initial 防静默丢全量历史
            if ("latest".equalsIgnoreCase(prepared.startupMode())) {
                prepared = prepared.withStartupMode("initial");
            }
        }
        Map<String, Object> config = buildTaskJobConfig(prepared, range, false);
        String jobId = hasJobId ? taskJobId : String.valueOf(System.currentTimeMillis() * 100 + taskId);
        try {
            String stJobId = seaTunnelClient.submitJob(baseUrl, jobId, config, hasJobId);
            persistTaskStarted(taskId, stJobId, task, null);
            log.info("任务已启动: task={} worker={} jobId={}", taskId, task.getWorkerId(), stJobId);
        } catch (IllegalStateException e) {
            // 启停纪律(问题记录二.2): restore 失败(旧 jobId 无可用 savepoint/checkpoint, 如 S3
            // 位点丢失)→ 弃旧 jobId, 换新 jobId + 全 initial 全量重采; 二次失败照抛不吞
            if (!hasJobId) {
                throw e;
            }
            log.warn("任务 restore 失败, 回退全 initial 重采: task={} jobId={}: {}", taskId, jobId, e.getMessage());
            String freshJobId = String.valueOf(System.currentTimeMillis() * 100 + taskId);
            Map<String, Object> freshConfig = buildTaskJobConfig(prepared, range, true);
            String stJobId = seaTunnelClient.submitJob(baseUrl, freshJobId, freshConfig, false);
            persistTaskStarted(taskId, stJobId, task, "restore 失败回退全 initial 重采");
            log.info("任务已全量重采启动: task={} jobId={}", taskId, stJobId);
        }
    }

    /** 停旧启新重提单任务(register 加表/加列/映射更新后, 任务在运行 → 自动停旧启新生效) */
    private void restartTask(long taskId) {
        stopTask(taskId);
        startTask(taskId);
    }

    /** 标记任务运行中(任务行 sea_tunnel_job_id + running=1) */
    private void markTaskRunning(long taskId, String jobId, CdcStTask task) {
        metaJdbc.update("UPDATE cdc_st_task SET sea_tunnel_job_id = ?, running = 1 WHERE task_id = ?", jobId, taskId);
        markOwnerRunning(task);
    }

    /** 提交成功: 任务 running=1 + 执行历史 */
    private void persistTaskStarted(long taskId, String stJobId, CdcStTask task, String message) {
        markTaskRunning(taskId, stJobId, task);
        recordHistory(task.getLogicalTable(), modeOfTask(task).syncMode(), stJobId, "start", "RUNNING", message);
    }

    /** 停止单任务(优雅 stop 任务 job → checkpoint 落盘; 下次启动同 jobId 续采) */
    private void stopTask(long taskId) {
        CdcStTask task = loadTask(taskId);
        if (task == null) {
            return;
        }
        String baseUrl = taskDispatchService.workerUrl(task.getWorkerId());
        if (baseUrl == null) {
            baseUrl = taskDispatchService.defaultWorkerUrl();
        }
        String jobId = task.getSeaTunnelJobId();
        if (jobId != null && !jobId.isBlank()) {
            try {
                SeaTunnelClient.JobInfo info = seaTunnelClient.jobInfo(baseUrl, jobId);
                if (info.isRunning() || "UNKNOW".equals(info.getJobStatus())) {
                    seaTunnelClient.stopJob(baseUrl, jobId);
                }
            } catch (Exception e) {
                log.warn("停止任务 job 失败 task={} ({}): {}", taskId, jobId, e.getMessage());
            }
        }
        metaJdbc.update("UPDATE cdc_st_task SET running = 0 WHERE task_id = ?", taskId);
        markOwnerIdle(task);
        recordHistory(task.getLogicalTable(), modeOfTask(task).syncMode(), jobId, "stop", "STOPPED", null);
    }

    /**
     * 启动级缺列检测(新方案 D6): 探测源表列集, 映射引用的源列不存在 → 写 sync_alert(missing_column)。
     * 只告警不 fail-fast(源表在线减列后任务重启照常起, 缺失列按 null 更新);
     * 探测失败/不支持(Mongo 等无 JDBC 探测)→ 跳过, 不阻断启动。
     */
    private void alertMissingColumns(CdcStTask task, DatasourceConfig ds) {
        try {
            String pwd = com.dcp.common.CryptoUtil.decrypt(ds.getPasswordCiphertext(), encKey);
            Map<String, Object> schema = probeSchema(ds.getType() == null ? "mysql" : ds.getType().toLowerCase(),
                    ds, task, pwd);
            if (schema == null || !(schema.get("fields") instanceof Map<?, ?> fm)) {
                return;
            }
            java.util.Set<String> srcCols = new java.util.HashSet<>();
            fm.keySet().forEach(k -> srcCols.add(String.valueOf(k)));
            java.util.Set<String> referenced = new java.util.LinkedHashSet<>();
            for (Map<String, Object> row : SyncMappingRules.parseMapping(task.getFieldMapping())) {
                Map<String, Object> source = row.get("source") instanceof Map<?, ?> m
                        ? (Map<String, Object>) m : Map.of();
                String kind = source.get("kind") == null ? "" : String.valueOf(source.get("kind"));
                String srcField = source.get("srcField") == null ? "" : String.valueOf(source.get("srcField"));
                if ("field-map".equals(kind) && !srcField.isBlank()) {
                    referenced.add(srcField);
                }
            }
            List<String> missing = referenced.stream().filter(f -> !srcCols.contains(f)).toList();
            if (missing.isEmpty()) {
                return;
            }
            metaJdbc.update("INSERT INTO sync_alert (logical_table, task_id, alert_type, detail, status) "
                            + "VALUES (?, ?, 'missing_column', ?, 'OPEN')",
                    task.getLogicalTable(), task.getTaskId(),
                    "任务启动: 映射引用源列缺失 [" + String.join(", ", missing) + "]"
                            + "(源表 " + task.getSourceDb() + "." + task.getSourceTable()
                            + "), 按新方案继续 null 更新");
            log.warn("缺列告警已写 sync_alert: {} 缺失列 [{}]", task.getLogicalTable(), missing);
        } catch (Exception e) {
            log.warn("缺列探测失败(跳过告警, 继续启动): {} - {}", task.getLogicalTable(), e.getMessage());
        }
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

    /**
     * 采集端热更新(LogicalTableConfigService 发布事件通知):topic 变更 → 重提采集任务换 sink topic。
     * 重提 = 优雅停止(同 jobId 续采, checkpoint 落盘)+ 启动, 位点不回退; 异步执行不阻塞配置保存。
     */
    @org.springframework.context.event.EventListener
    public void onLogicalTableConfigChanged(LogicalTableConfigChangedEvent e) {
        if (!"TOPIC_CHANGED".equals(e.action())) {
            return;
        }
        Thread t = new Thread(() -> {
            try {
                List<CdcStTask> tasks = loadTasks(e.tableName());
                if (tasks.isEmpty()) {
                    return; // 无采集任务(纯订阅/文件源等), 无重提对象
                }
                log.info("逻辑表 {} topic 变更({}→{}), 重提采集任务更新 sink topic",
                        e.tableName(), e.oldTopic(), e.newTopic());
                stop(e.tableName());
                start(e.tableName());
            } catch (Exception ex) {
                log.warn("逻辑表 {} topic 变更后采集任务重提失败(请人工确认采集任务状态): {}",
                        e.tableName(), ex.getMessage());
            }
        }, "cdc-topic-change-" + e.tableName());
        t.setDaemon(true);
        t.start();
    }

    /**
     * N6(D1/D2): 源表 DDL 变更(TRUNCATE/DROP/ALTER)告警登记。
     * SeaTunnel CDC 2.3.12 不输出 Debezium TRUNCATE/schema-change 事件(反编译取证:
     * 反序列化链路无 op=t 处理), 采集端无法自动检测 → 按拍板语义「不自动同步 + sync_alert
     * 告警 + 人工处置目标表」, 由 DBA/运维在执行 DDL 后登记告警, watchdog 告警闭环处置。
     */
    public void alertDdlChange(String logicalTable, String ddlKind, String detail) {
        String kind = ddlKind == null ? "DDL" : ddlKind.toUpperCase();
        if (!java.util.Set.of("TRUNCATE", "DROP", "ALTER").contains(kind)) {
            throw new IllegalArgumentException("ddlKind 仅支持 TRUNCATE/DROP/ALTER, 实际: " + ddlKind);
        }
        Long taskId = null;
        try {
            taskId = metaJdbc.queryForObject(
                    "SELECT task_id FROM cdc_st_task WHERE logical_table = ? LIMIT 1", Long.class, logicalTable);
        } catch (Exception ignore) {
            // 无任务也可登记(逻辑表级告警)
        }
        metaJdbc.update("INSERT INTO sync_alert (logical_table, task_id, alert_type, detail, status) "
                        + "VALUES (?, ?, 'ddl_change', ?, 'OPEN')",
                logicalTable, taskId,
                "源表 DDL " + kind + ": " + (detail == null ? "" : detail)
                        + "(拍板语义: 不自动同步, 请人工处置目标表后关闭本条告警)");
        log.warn("DDL 变更告警已写 sync_alert: {} {} {}", logicalTable, kind, detail);
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

    /** 停止某逻辑表的全部采集任务(一任务一 job: 逐任务独立停, 互不牵连; 优雅停止 checkpoint 落盘, 下次启动同 jobId 续采) */
    public synchronized Map<String, Object> stop(String logicalTable) {
        List<CdcStTask> tasks = loadTasks(logicalTable);
        List<Long> stopped = new ArrayList<>();
        for (CdcStTask task : tasks) {
            stopTask(task.getTaskId());
            stopped.add(task.getTaskId());
        }
        return Map.of("logicalTable", logicalTable, "running", false, "taskIds", stopped);
    }

    /** 停止单个任务实例(单任务独立停止) */
    public synchronized Map<String, Object> stopInstance(Long taskId) {
        CdcStTask task = loadTask(taskId);
        if (task == null) {
            throw new IllegalArgumentException("采集任务实例不存在: " + taskId);
        }
        stopTask(taskId);
        return Map.of("logicalTable", task.getLogicalTable(), "taskId", taskId,
                "source", task.getSourceDb() + "." + task.getSourceTable(), "running", false);
    }

    private CdcStTask loadTask(Long taskId) {
        java.util.List<CdcStTask> l = metaJdbc.query("SELECT logical_table, datasource_id, source_db, source_table, topic, "
                        + "business_key_fields, key_columns, bucket_count, field_mapping, row_chain, sea_tunnel_job_id, "
                        + "task_id, sync_task_id, snapshot_start_position, worker_id, server_id_range, running "
                        + "FROM cdc_st_task WHERE task_id = ?",
                (rs, i) -> mapTask(rs), taskId);
        return l.isEmpty() ? null : l.get(0);
    }

    /** 状态: 全部任务 + SeaTunnel 侧实时状态(未运行/运行中) */
    public List<Map<String, Object>> status() {
        List<Map<String, Object>> out = new ArrayList<>();
        metaJdbc.query("SELECT ct.logical_table, ct.datasource_id, ct.source_db, ct.source_table, ct.topic, "
                        + "ct.business_key_fields, ct.key_columns, ct.bucket_count, ct.field_mapping, ct.row_chain, "
                        + "ct.sea_tunnel_job_id, ct.running, ct.sync_task_id, ct.task_id, ct.worker_id, st.name AS owner_name "
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
                    m.put("workerId", rs.getString("worker_id"));
                    boolean localRunning = rs.getInt("running") == 1;
                    boolean live = false;
                    if (stJobId != null && !stJobId.isBlank()) {
                        try {
                            SeaTunnelClient.JobInfo info = seaTunnelClient.jobInfo(
                                    taskDispatchService.workerUrl(rs.getString("worker_id")) == null
                                            ? taskDispatchService.defaultWorkerUrl()
                                            : taskDispatchService.workerUrl(rs.getString("worker_id")),
                                    stJobId);
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
            case "mysql" -> "jdbc:mysql://%s:%d/%s?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Asia/Shanghai"
                    .formatted(ds.getHost(), ds.getPort(), task.getSourceDb());
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

    /** 按数据源类型构建 CDC source 配置(2026-08-31 扩展: MySQL/PG/SQLServer/MongoDB/Oracle; sidBase 由族区间按表序派生) */
    private Map<String, Object> buildSource(CdcStTask task, DatasourceConfig ds,
                                String startupMode, boolean snapshotOnly, String startValue, String dsType,
                                long sidBase) {
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
                // server-id 必须每表唯一(同 job 多 source 复用会互踢 → 循环重连重跑快照);
                // 从族区间按表序派生(每表 5 个宽度, 区间由 server_id_pool 全局分配保证不冲突)
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

    /** 构造单任务 job 配置(一任务一 job: 单 source/transform/sink, 独立 env/checkpoint) */
    private Map<String, Object> buildTaskJobConfig(PreparedTask p, String range, boolean forceInitial) {
        CdcStTask task = p.task();
        Map<String, Object> env = new LinkedHashMap<>();
        env.put("job.mode", "STREAMING");
        env.put("job.name", "cdc-task-" + task.getTaskId());
        env.put("parallelism", 1);
        env.put("checkpoint.interval", 10000);

        String startupMode = forceInitial ? "initial" : p.startupMode();
        // server-id: 本任务独占区间(注册时池领取, 迁移不漂移)
        long sidBase = rangeStart(range);
        Map<String, Object> source = buildSource(task, p.ds(), startupMode, p.snapshotOnly(),
                p.startValue(), p.dsType(), sidBase);
        // 纯快照模式: Debezium snapshot.mode=initial_only(快照完 source 结束)
        if (p.snapshotOnly()) {
            source.put("debezium", java.util.Map.of("snapshot.mode", "initial_only"));
        }
        source.put("result_table_name", "cdc_source_0");

        Map<String, Object> transform = new LinkedHashMap<>();
        transform.put("plugin_name", "CdcSeqTransform");
        transform.put("source_table_name", "cdc_source_0");
        transform.put("result_table_name", "cdc_output_0");
        transform.put("logical_table", task.getLogicalTable());
        transform.put("task_id", String.valueOf(task.getTaskId()));
        transform.put("datasource_id", String.valueOf(task.getDatasourceId()));
        transform.put("source_db", task.getSourceDb());
        transform.put("source_table", task.getSourceTable());
        transform.put("business_key_fields", task.getBusinessKeyFields());
        transform.put("key_columns", task.getKeyColumns());
        transform.put("bucket_count", task.getBucketCount());
        transform.put("null_key_mode", "ERROR");
        // 告警落库(新方案): 主键修改/缺列告警写 sync_alert(元库)
        transform.put("alert_db_url", alertDbUrl);
        transform.put("alert_db_user", alertDbUser);
        transform.put("alert_db_password", alertDbPassword);
        transform.put("source_type", p.dsType());
        // 快照级号 S=f(快照起点位点)(方案 A: 后端读源库当前位点下发; 非快照任务无此配置项)
        if (p.snapshotStartPosition() != null && !p.snapshotStartPosition().isBlank()) {
            transform.put("snapshot_start_position", p.snapshotStartPosition());
        }
        // 新方案强校验必配: 注册级已保证映射非空
        transform.put("field_mapping", task.getFieldMapping());
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
        sink.put("source_table_name", "cdc_output_0");

        Map<String, Object> root = new LinkedHashMap<>();
        root.put("env", env);
        root.put("source", List.of(source));
        root.put("transform", List.of(transform));
        root.put("sink", List.of(sink));
        return root;
    }

    /** 任务 server-id 区间起点(取首段; 多段逗号分隔时用首段) */
    private static long rangeStart(String range) {
        if (range == null || range.isBlank()) {
            return 5400L;
        }
        try {
            String[] parts = range.split(",")[0].trim().split("-");
            return parts.length == 2 ? Long.parseLong(parts[0]) : 5400L;
        } catch (NumberFormatException e) {
            return 5400L;
        }
    }

    private String tableConfigTopic(String logicalTable) {
        try {
            return metaJdbc.queryForObject(
                    "SELECT topic FROM logical_table WHERE table_name = ?", String.class, logicalTable);
        } catch (Exception e) {
            return null;
        }
    }

    /** 巡检 auto-restore 的最近尝试时间(任务级, 失败退避防风暴) */
    private final java.util.concurrent.ConcurrentHashMap<Long, Long> lastRestoreAttempt = new java.util.concurrent.ConcurrentHashMap<>();

    /** restore 失败退避窗口: 5 分钟内不重试同一任务 */
    private static final long RESTORE_BACKOFF_MS = 5 * 60_000L;

    /** worker 判死连续不可达计数(任务级; 达阈值触发迁移, worker 可达即清零) */
    private final java.util.concurrent.ConcurrentHashMap<Long, Integer> workerDownCount = new java.util.concurrent.ConcurrentHashMap<>();

    /** worker 连续不可达多少次判死迁移(每巡检周期计 1 次, 默认 60s → 约 3 分钟) */
    private static final int WORKER_DOWN_MIGRATE_THRESHOLD = 3;

    /**
     * 待办1: SeaTunnel job 丢失巡检自动 restore(问题记录二.1, H14 实证 master 宕机后 job
     * 消失不自动恢复)。每 watchdog-interval-ms 按运行任务扫描(一任务一 job):
     * ① worker 判死: 任务绑定的 worker REST 入口连续不可达达阈值 → 迁移至最闲 worker
     * (server-id 区间随迁不漂移)后定向重提;
     * ② job 丢失判死: worker 可达但任务 job 非 RUNNING → 同任务 jobId 重新提交
     * (isStartWithSavePoint → 从 S3 checkpoint 恢复);
     * autoStop 任务(full-once/scheduled-batch/full-scheduled) FINISHED 属正常完成,
     * 只收敛本地标记不重启。
     *
     * <p>savepoint 定期调度口径: 2.3.12 REST 无 savepoint 端点(实测 404), CLI savepoint 会
     * 触发 checkpoint 后停止任务(不适于周期触发); checkpoint 已迁 MinIO S3 且每 10s 持续
     * 落盘, "可恢复点"由持续 checkpoint 兜底 + 本巡检的 restore 恢复验证守护。
     */
    @Scheduled(fixedDelayString = "${dcp.sync.cdc-st.watchdog.interval-ms:60000}")
    @SchedulerLock(name = "cdcStWatchdog", lockAtMostFor = "10m", lockAtLeastFor = "30s")
    public void watchdogScan() {
        if (!watchdogEnabled) {
            return;
        }
        try {
            // 一任务一 job: 扫描全部运行中的采集任务(worker 判死 → 单任务迁移; job 丢失 → 单任务 restore)
            List<Map<String, Object>> runningTasks = metaJdbc.queryForList(
                    "SELECT task_id, worker_id, sea_tunnel_job_id FROM cdc_st_task WHERE running = 1");
            for (Map<String, Object> row : runningTasks) {
                long taskId = ((Number) row.get("task_id")).longValue();
                String workerId = row.get("worker_id") == null ? null : String.valueOf(row.get("worker_id"));
                String baseUrl = taskDispatchService.workerUrl(workerId);
                if (baseUrl == null) {
                    baseUrl = taskDispatchService.defaultWorkerUrl();
                }
                try {
                    // ① worker 判死: 任务绑定的 REST 入口连续不可达达阈值 → 迁移至最闲 worker 后定向重提
                    if (!seaTunnelClient.engineReachable(baseUrl)) {
                        int downs = workerDownCount.merge(taskId, 1, Integer::sum);
                        log.warn("任务 worker 不可达: task={} worker={} url={} 连续 {} 次",
                                taskId, workerId, baseUrl, downs);
                        if (downs >= WORKER_DOWN_MIGRATE_THRESHOLD) {
                            migrateTaskAway(taskId, workerId);
                            workerDownCount.remove(taskId);
                        }
                        continue;
                    }
                    workerDownCount.remove(taskId);
                    // ② 任务 job 丢失判死: 非 RUNNING → 单任务 restore(带 savepoint)
                    Object jobObj = row.get("sea_tunnel_job_id");
                    String jobId = jobObj == null ? null : String.valueOf(jobObj);
                    if (jobId == null || jobId.isBlank()) {
                        continue;   // 从未启动, 不巡检
                    }
                    SeaTunnelClient.JobInfo info = seaTunnelClient.jobInfo(baseUrl, jobId);
                    if (info != null && info.isRunning()) {
                        continue;   // 健康
                    }
                    CdcStTask task = loadTask(taskId);
                    if (info != null && info.isFinished() && taskAutoStop(task)) {
                        // 一次性/批模式正常跑完: 收敛标记, 不重启
                        metaJdbc.update("UPDATE cdc_st_task SET running = 0 WHERE task_id = ?", taskId);
                        markOwnerIdle(task);
                        continue;
                    }
                    Long lastAttempt = lastRestoreAttempt.get(taskId);
                    if (lastAttempt != null && System.currentTimeMillis() - lastAttempt < RESTORE_BACKOFF_MS) {
                        continue;   // 退避期内不重试(上次 restore 失败)
                    }
                    log.warn("任务 job 丢失, 自动 restore: task={} jobId={} status={}", taskId, jobId,
                            info == null ? "NOT-FOUND" : info.getJobStatus());
                    startTask(taskId);
                    lastRestoreAttempt.remove(taskId);
                } catch (Exception e) {
                    log.warn("任务巡检 restore 失败 task={}: {}", taskId, e.getMessage());
                    lastRestoreAttempt.put(taskId, System.currentTimeMillis());
                }
            }
        } catch (Exception e) {
            log.error("CDC 巡检扫描异常", e);
        }
    }

    /** 单任务是否 autoStop(full-once/scheduled-batch/full-scheduled) */
    private boolean taskAutoStop(CdcStTask task) {
        return modeOfTask(task).autoStop();
    }

    /**
     * worker 判死迁移: 任务换绑最闲 worker(server-id 区间随迁不漂移, 无需重领)→ 新 worker 定向重提。
     * 死 worker 恢复后不回迁(避免反复漂移); 旧 worker 上的残留 job 由 Zeta 端超时/运维清理。
     */
    private void migrateTaskAway(long taskId, String deadWorkerId) {
        String toWorker = taskDispatchService.pickLeastLoadedWorker(deadWorkerId);
        if (toWorker == null) {
            log.warn("无可迁移目标 worker(仅剩死 worker 或未登记), task={} 保持原绑定等待恢复", taskId);
            return;
        }
        taskDispatchService.migrateTask(taskId, toWorker);
        log.warn("任务判死迁移: task={} {} → {}", taskId, deadWorkerId, toWorker);
        try {
            startTask(taskId);
        } catch (Exception e) {
            log.warn("迁移后重启失败 task={}: {} (watchdog 下轮重试)", taskId, e.getMessage());
        }
    }

    /** 加载某逻辑表的全部采集任务 */
    private List<CdcStTask> loadTasks(String logicalTable) {
        return metaJdbc.query("SELECT logical_table, datasource_id, source_db, source_table, topic, "
                        + "business_key_fields, key_columns, bucket_count, field_mapping, row_chain, sea_tunnel_job_id, "
                        + "task_id, sync_task_id, snapshot_start_position, worker_id, server_id_range, running "
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
        t.setSnapshotStartPosition(rs.getString(14));
        t.setWorkerId(rs.getString(15));
        t.setServerIdRange(rs.getString(16));
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
        /** 快照起点位点(方案 A: 首次 start 时读源库当前位点持久化; 重启/重放复用保证 S 恒定) */
        private String snapshotStartPosition;
        /** 绑定的 worker(cdc_worker.worker_id; NULL=默认入口, 存量任务兼容) */
        private String workerId;
        /** 本任务独占 server-id 区间(server_id_pool 领取, 迁移不漂移) */
        private String serverIdRange;
    }
}
