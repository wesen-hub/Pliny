package com.dcp.sync.filesync;

import com.dcp.module.agent.AgentService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 文件同步 V3 —— rclone 引擎(2026-09-10 整体由 mc mirror 切换, 方案见
 * 《数据采集平台V3-文件同步引擎切换rclone方案-2026-09-10.md》):
 *
 * <pre>
 *   任务 = 一次 rclone 会话的描述: 源(文件数据源+路径) + 目标(数据源+bucket/prefix)
 *         + syncMode(三态) + strategy(直模 rclone 参数: 比较/覆盖/删除/过滤/性能/可靠性)
 *         + schedule(none|interval|cron) + execHost(platform|agent)
 *   模式: full_once        一次性全量: sync 镜像一次即止(deleteExtras=false 时退化为 copy)
 *         incr_scheduled   仅定时增量: copy, 只补新增/变更, 从不删目标
 *         full_then_incr   全量+定时增量: 首轮 sync 建基线, 之后按调度 copy 增量
 *   执行 = RcloneRunner: 生成临时 rclone.conf(命名 remote dcpsrc/dcpdst, 免连接串冒号/全局 flag 冲突)
 *          → rclone sync|copy → --use-json-log 逐行解析(Copied/Deleted/Failed) → 落 run/run_file;
 *          末行 stats JSON → run.summary(bytes/deletes/checks/errors/耗时)
 *   watch(常驻实时)随 mc 一并移除: 近实时请把增量周期设短(最小 1min)。
 *   旧 V1/V2 表(file_copy_task/ledger/file_meta)启动检测到即重建。
 * </pre>
 */
@Slf4j
@Component
public class FileSyncTaskService {

    private static final ObjectMapper OM = new ObjectMapper();
    /** 命名 remote(写入临时 rclone.conf): 避免 --s3-* 全局 flag 冲突与连接串冒号截断 */
    private static final String REMOTE_SRC = "dcpsrc";
    private static final String REMOTE_DST = "dcpdst";
    /** 同步模式三态 */
    static final String MODE_FULL_ONCE = "full_once";
    static final String MODE_INCR = "incr_scheduled";
    static final String MODE_FULL_THEN_INCR = "full_then_incr";
    /** 单轮执行上限 */
    private static final int RUN_TIMEOUT_SEC = 1800;

    private final JdbcTemplate metaJdbc;
    private final AgentService agentService;

    @Value("${dcp.enc-key}")
    private String encKey;

    // 平台默认目标 MinIO(演示快捷; 任务也可显式选目标数据源)
    @Value("${dcp.filecopy.target-endpoint:http://host.docker.internal:19000}")
    private String dstEndpoint;
    @Value("${dcp.filecopy.target-access-key:minioadmin}")
    private String dstAccessKey;
    @Value("${dcp.filecopy.target-secret-key:minioadmin123}")
    private String dstSecretKey;

    private final Map<Long, AtomicBoolean> running = new ConcurrentHashMap<>();
    private final ExecutorService executor = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "filesync-runner");
        t.setDaemon(true);
        return t;
    });
    private final Map<Long, Integer> lastCronMinute = new ConcurrentHashMap<>();

    public FileSyncTaskService(JdbcTemplate metaJdbc, AgentService agentService) {
        this.metaJdbc = metaJdbc;
        this.agentService = agentService;
    }

    @PostConstruct
    public void ensureSchema() {
        // V1/V2 遗留(file_copy_*)检测 → 重建(演示数据可弃)
        try {
            Integer legacy = metaJdbc.queryForObject(
                    "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() "
                            + "AND TABLE_NAME = 'file_copy_task'", Integer.class);
            if (legacy != null && legacy > 0) {
                metaJdbc.execute("DROP TABLE IF EXISTS file_copy_task");
                metaJdbc.execute("DROP TABLE IF EXISTS file_copy_ledger");
                metaJdbc.execute("DROP TABLE IF EXISTS file_meta");
                log.warn("检测到 V1/V2 文件同步旧表, 已重建为 V3(mc mirror 原生)");
            }
        } catch (Exception ignored) {
        }
        metaJdbc.execute("CREATE TABLE IF NOT EXISTS file_sync_task ("
                + "id BIGINT AUTO_INCREMENT PRIMARY KEY,"
                + "name VARCHAR(128) NOT NULL, description VARCHAR(255),"
                + "exec_host VARCHAR(16) DEFAULT 'platform', agent_id BIGINT NULL,"
                + "source_datasource_id BIGINT NOT NULL DEFAULT 0,"
                + "source_path VARCHAR(512),"
                + "target_datasource_id BIGINT NOT NULL DEFAULT 0,"
                + "target_bucket VARCHAR(128) NOT NULL, target_prefix VARCHAR(256),"
                + "strategy_json VARCHAR(2048) DEFAULT '{}',"
                + "sync_mode VARCHAR(24) DEFAULT 'full_then_incr',"
                + "baseline_done TINYINT DEFAULT 0,"
                + "schedule_mode VARCHAR(16) DEFAULT 'none', interval_min INT DEFAULT 0, cron_expr VARCHAR(64),"
                + "run_status VARCHAR(16) DEFAULT 'IDLE', last_run_summary VARCHAR(1024), last_run_time DATETIME,"
                + "version INT DEFAULT 1,"
                + "create_time DATETIME DEFAULT CURRENT_TIMESTAMP,"
                + "update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,"
                + "KEY idx_agent (agent_id), KEY idx_sched (schedule_mode))");
        metaJdbc.execute("CREATE TABLE IF NOT EXISTS file_sync_run ("
                + "id BIGINT AUTO_INCREMENT PRIMARY KEY,"
                + "task_id BIGINT NOT NULL,"
                + "trigger_type VARCHAR(16) DEFAULT 'manual' COMMENT 'manual|schedule|agent',"
                + "executor VARCHAR(64) DEFAULT 'platform',"
                + "strategy_snapshot VARCHAR(2048),"
                + "status VARCHAR(16) DEFAULT 'RUNNING' COMMENT 'RUNNING|COMPLETED|FAILED',"
                + "summary VARCHAR(1024), error_msg VARCHAR(2000),"
                + "started_at DATETIME, finished_at DATETIME,"
                + "KEY idx_task (task_id, id))");
        metaJdbc.execute("CREATE TABLE IF NOT EXISTS file_sync_run_file ("
                + "id BIGINT AUTO_INCREMENT PRIMARY KEY,"
                + "run_id BIGINT NOT NULL,"
                + "rel_path VARCHAR(1024),"
                + "status VARCHAR(16) COMMENT 'success|error',"
                + "size BIGINT DEFAULT 0, error_msg VARCHAR(2000),"
                + "event_at DATETIME DEFAULT CURRENT_TIMESTAMP,"
                + "KEY idx_run (run_id))");
        metaJdbc.execute("CREATE TABLE IF NOT EXISTS file_sync_reconcile ("
                + "id BIGINT AUTO_INCREMENT PRIMARY KEY,"
                + "task_id BIGINT NOT NULL,"
                + "trigger_type VARCHAR(16) DEFAULT 'manual',"
                + "source_count BIGINT DEFAULT 0, target_count BIGINT DEFAULT 0,"
                + "missing_count BIGINT DEFAULT 0, extra_count BIGINT DEFAULT 0, size_diff_count BIGINT DEFAULT 0,"
                + "detail_json MEDIUMTEXT, truncated TINYINT DEFAULT 0,"
                + "elapsed_ms BIGINT DEFAULT 0,"
                + "create_time DATETIME DEFAULT CURRENT_TIMESTAMP,"
                + "KEY idx_task (task_id, id))");
        // 引擎切换(mc mirror → rclone): 存量表补列 + 旧任务(mc 语义)清空重建(演示数据可弃)
        try {
            Integer hasMode = metaJdbc.queryForObject(
                    "SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() "
                            + "AND TABLE_NAME = 'file_sync_task' AND COLUMN_NAME = 'sync_mode'", Integer.class);
            if (hasMode != null && hasMode == 0) {
                metaJdbc.execute("ALTER TABLE file_sync_task ADD COLUMN sync_mode VARCHAR(24) DEFAULT 'full_then_incr' AFTER strategy_json");
                metaJdbc.execute("ALTER TABLE file_sync_task ADD COLUMN baseline_done TINYINT DEFAULT 0 AFTER sync_mode");
                metaJdbc.update("DELETE FROM file_sync_run_file");
                int m = metaJdbc.update("DELETE FROM file_sync_run");
                int t = metaJdbc.update("DELETE FROM file_sync_task");
                log.warn("引擎切换 mc mirror → rclone: 清空旧(mc 语义)任务 {} 个 / run {} 条", t, m);
            }
        } catch (Exception e) {
            log.warn("文件同步引擎切换迁移异常: {}", e.getMessage());
        }
        log.info("FileSync V3 schema ensured (engine=rclone)");
    }

    @PreDestroy
    public void close() {
        executor.shutdownNow();
    }

    // ────────────────────────── 任务 CRUD ──────────────────────────

    public Map<String, Object> create(Map<String, Object> body) {
        validate(body);
        String mode = normMode(body.get("syncMode"));
        String execHost = "platform".equals(String.valueOf(body.getOrDefault("execHost", "platform"))) ? "platform" : "agent";
        Number agentId = execHost.equals("agent") ? (Number) body.get("agentId") : null;
        if (execHost.equals("agent") && agentId == null) {
            throw new IllegalArgumentException("execHost=agent 时必须提供 agentId");
        }
        String name = (String) body.get("name");
        // 一次性全量无需调度; 其余两态必须带调度(间隔/cron)
        String sched = MODE_FULL_ONCE.equals(mode) ? "none" : normSched(body.get("scheduleMode"));
        GeneratedKeyHolder kh = new GeneratedKeyHolder();
        metaJdbc.update(con -> {
            PreparedStatement ps = con.prepareStatement(
                    "INSERT INTO file_sync_task (name, description, exec_host, agent_id, source_datasource_id, "
                            + "source_path, target_datasource_id, target_bucket, target_prefix, strategy_json, "
                            + "sync_mode, schedule_mode, interval_min, cron_expr) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    Statement.RETURN_GENERATED_KEYS);
            ps.setString(1, name);
            ps.setString(2, (String) body.get("description"));
            ps.setString(3, execHost);
            ps.setObject(4, agentId);
            ps.setLong(5, ((Number) body.get("sourceDatasourceId")).longValue());
            ps.setString(6, (String) body.get("sourcePath"));
            ps.setLong(7, body.get("targetDatasourceId") == null ? 0 : ((Number) body.get("targetDatasourceId")).longValue());
            ps.setString(8, (String) body.get("targetBucket"));
            ps.setString(9, (String) body.get("targetPrefix"));
            ps.setString(10, jsonOf(body.get("strategy")));
            ps.setString(11, mode);
            ps.setString(12, sched);
            ps.setInt(13, "interval".equals(sched) ? Math.max(body.get("intervalMin") == null ? 5 : ((Number) body.get("intervalMin")).intValue(), 1) : 0);
            ps.setString(14, "cron".equals(sched) ? String.valueOf(body.get("cronExpr")) : null);
            return ps;
        }, kh);
        long id = kh.getKey().longValue();
        if (execHost.equals("agent")) {
            pushAgentCommand(agentId.longValue(), id, "task_upsert");
        }
        log.info("文件同步 V3 任务创建 id={} {} exec={} mode={} ds={} {}", id, name, execHost, mode,
                body.get("sourceDatasourceId"), body.get("sourcePath"));
        return Map.of("id", id, "name", name, "execHost", execHost, "syncMode", mode);
    }

    private void validate(Map<String, Object> body) {
        if (body.get("name") == null || String.valueOf(body.get("name")).isBlank()) throw new IllegalArgumentException("name 必填");
        if (body.get("sourceDatasourceId") == null) throw new IllegalArgumentException("sourceDatasourceId 必填");
        if (body.get("sourcePath") == null) throw new IllegalArgumentException("sourcePath 必填");
        if (body.get("targetBucket") == null || String.valueOf(body.get("targetBucket")).isBlank())
            throw new IllegalArgumentException("targetBucket 必填");
        String mode = normMode(body.get("syncMode"));
        String sched = normSched(body.get("scheduleMode"));
        if (!MODE_FULL_ONCE.equals(mode) && "none".equals(sched)) {
            throw new IllegalArgumentException("syncMode=" + mode + " 必须配置定时调度(interval|cron): 它靠周期触发增量");
        }
        if ("cron".equals(sched) && (body.get("cronExpr") == null || String.valueOf(body.get("cronExpr")).isBlank())) {
            throw new IllegalArgumentException("cron 模式必须提供 cronExpr");
        }
        Map<String, Object> st = new LinkedHashMap<>();
        if (body.get("strategy") instanceof Map<?, ?> sm) {
            sm.forEach((k, v) -> st.put(String.valueOf(k), v));
        }
        String cmp = String.valueOf(st.getOrDefault("compare", "size_mtime"));
        if (!cmp.equals("size_mtime") && !cmp.equals("size_only") && !cmp.equals("checksum")) {
            throw new IllegalArgumentException("strategy.compare 仅支持 size_mtime|size_only|checksum");
        }
        String um = String.valueOf(st.getOrDefault("updateMode", "overwrite"));
        if (!um.equals("overwrite") && !um.equals("update") && !um.equals("ignore_existing")) {
            throw new IllegalArgumentException("strategy.updateMode 仅支持 overwrite|update|ignore_existing");
        }
        if (MODE_INCR.equals(mode) && st.get("deleteExtras") != null && bool(st, "deleteExtras")) {
            throw new IllegalArgumentException("仅定时增量模式从不删除目标文件(deleteExtras 不适用)");
        }
    }

    public List<Map<String, Object>> list() {
        List<Map<String, Object>> rows = metaJdbc.queryForList(
                "SELECT t.id, t.name, t.description, t.exec_host, t.agent_id, t.source_datasource_id, t.source_path, "
                        + "t.target_datasource_id, t.target_bucket, t.target_prefix, t.strategy_json, "
                        + "t.sync_mode, t.baseline_done, "
                        + "t.schedule_mode, t.interval_min, t.cron_expr, t.run_status, t.last_run_summary, t.last_run_time, "
                        + "t.version, t.create_time, "
                        + "s.name AS source_ds_name, s.type AS source_ds_type, d.name AS target_ds_name, "
                        + "a.name AS agent_name, a.status AS agent_status, "
                        + "TIMESTAMPDIFF(SECOND, a.last_heartbeat, NOW()) AS agent_age "
                        + "FROM file_sync_task t "
                        + "LEFT JOIN datasource_config s ON s.id = t.source_datasource_id "
                        + "LEFT JOIN datasource_config d ON d.id = t.target_datasource_id "
                        + "LEFT JOIN dcp_agent a ON a.id = t.agent_id ORDER BY t.id");
        rows.forEach(this::decorate);
        return rows;
    }

    public Map<String, Object> detail(long id) {
        List<Map<String, Object>> rows = metaJdbc.queryForList(
                "SELECT t.*, a.status AS agent_status, "
                        + "TIMESTAMPDIFF(SECOND, a.last_heartbeat, NOW()) AS agent_age "
                        + "FROM file_sync_task t LEFT JOIN dcp_agent a ON a.id = t.agent_id WHERE t.id = ?", id);
        if (rows.isEmpty()) return null;
        Map<String, Object> r = new LinkedHashMap<>(rows.get(0));
        r.put("strategy", parseJson(String.valueOf(r.get("strategy_json"))));
        r.put("online", agentOnline(r));
        return r;
    }

    private void decorate(Map<String, Object> r) {
        r.put("strategy", parseJson(String.valueOf(r.get("strategy_json"))));
        r.put("online", agentOnline(r));
    }

    private boolean agentOnline(Map<String, Object> r) {
        Object age = r.get("agent_age");
        Object st = r.get("agent_status");
        return "online".equals(String.valueOf(st)) && age != null && ((Number) age).longValue() <= 90;
    }

    public Map<String, Object> update(long id, Map<String, Object> body) {
        Map<String, Object> exist = detail(id);
        if (exist == null) throw new IllegalArgumentException("任务不存在: " + id);
        Map<String, Object> task = metaJdbc.queryForList("SELECT exec_host, agent_id FROM file_sync_task WHERE id = ?", id).get(0);
        String sched = normSched(body.getOrDefault("scheduleMode", exist.get("schedule_mode")));
        String mode = body.get("syncMode") == null ? normMode(exist.get("sync_mode")) : normMode(body.get("syncMode"));
        if (MODE_FULL_ONCE.equals(mode)) sched = "none";
        // 合并现有值后走与 create 同一套校验(策略白名单/模式-调度/仅增量禁删等)
        Map<String, Object> merged = new LinkedHashMap<>();
        merged.put("name", body.getOrDefault("name", exist.get("name")));
        merged.put("sourceDatasourceId", exist.get("source_datasource_id"));
        merged.put("sourcePath", body.getOrDefault("sourcePath", exist.get("source_path")));
        merged.put("targetBucket", body.getOrDefault("targetBucket", exist.get("target_bucket")));
        merged.put("syncMode", mode);
        merged.put("scheduleMode", sched);
        merged.put("cronExpr", body.getOrDefault("cronExpr", exist.get("cron_expr")));
        merged.put("strategy", body.getOrDefault("strategy", exist.get("strategy")));
        validate(merged);
        metaJdbc.update("UPDATE file_sync_task SET name = ?, description = ?, source_path = ?, target_bucket = ?, "
                        + "target_prefix = ?, strategy_json = ?, sync_mode = ?, schedule_mode = ?, interval_min = ?, cron_expr = ?, "
                        + "version = version + 1 WHERE id = ?",
                body.getOrDefault("name", exist.get("name")),
                body.containsKey("description") ? body.get("description") : exist.get("description"),
                body.getOrDefault("sourcePath", exist.get("source_path")),
                body.getOrDefault("targetBucket", exist.get("target_bucket")),
                body.containsKey("targetPrefix") ? body.get("targetPrefix") : exist.get("target_prefix"),
                jsonOf(body.getOrDefault("strategy", exist.get("strategy"))),
                mode,
                sched,
                "interval".equals(sched) ? Math.max(body.get("intervalMin") == null ? 5 : ((Number) body.get("intervalMin")).intValue(), 1) : 0,
                "cron".equals(sched) ? String.valueOf(body.getOrDefault("cronExpr", exist.get("cron_expr"))) : null,
                id);
        if ("agent".equals(String.valueOf(task.get("exec_host")))) {
            pushAgentCommand(((Number) task.get("agent_id")).longValue(), id, "task_upsert");
        }
        return Map.of("updated", true, "id", id);
    }

    public void delete(long id) {
        List<Map<String, Object>> rows = metaJdbc.queryForList("SELECT exec_host, agent_id FROM file_sync_task WHERE id = ?", id);
        if (rows.isEmpty()) return;
        if ("agent".equals(String.valueOf(rows.get(0).get("exec_host")))) {
            pushAgentCommand(((Number) rows.get(0).get("agent_id")).longValue(), id, "task_remove");
        }
        metaJdbc.update("DELETE FROM file_sync_run_file WHERE run_id IN (SELECT id FROM file_sync_run WHERE task_id = ?)", id);
        metaJdbc.update("DELETE FROM file_sync_run WHERE task_id = ?", id);
        metaJdbc.update("DELETE FROM file_sync_task WHERE id = ?", id);
    }

    // ────────────────────────── 执行(mirror runner) ──────────────────────────

    /** 手动触发一轮; 已在跑返回 false */
    public boolean runAsync(long taskId) {
        return runAsync(taskId, "manual");
    }

    public boolean runAsync(long taskId, String trigger) {
        AtomicBoolean flag = running.computeIfAbsent(taskId, k -> new AtomicBoolean(false));
        if (!flag.compareAndSet(false, true)) {
            return false;
        }
        executor.submit(() -> {
            try {
                runOnce(taskId, trigger);
            } catch (Exception e) {
                log.error("文件同步 V3 运行异常 task={}", taskId, e);
                metaJdbc.update("UPDATE file_sync_task SET run_status = 'FAILED' WHERE id = ?", taskId);
            } finally {
                flag.set(false);
            }
        });
        return true;
    }

    private void runOnce(long taskId, String trigger) {
        Map<String, Object> task = detail(taskId);
        if (task == null) throw new IllegalStateException("任务不存在: " + taskId);
        if ("agent".equals(String.valueOf(task.get("exec_host")))) {
            // agent 任务由 agent 本地执行, 平台只下发 run_now 指令
            pushAgentCommand(((Number) task.get("agent_id")).longValue(), taskId, "run_now");
            return;
        }
        long runId = insertRun(taskId, trigger, "platform", String.valueOf(task.get("strategy_json")));
        metaJdbc.update("UPDATE file_sync_task SET run_status = 'RUNNING', last_run_time = NOW() WHERE id = ?", taskId);
        long t0 = System.currentTimeMillis();
        int okN = 0, delN = 0, failN = 0;
        long bytes = 0;
        String errorTail = null;
        Map<String, Object> stats = null;
        boolean full = false;
        Path conf = null;
        try {
            Map<String, String> src = sourceConf(((Number) task.get("source_datasource_id")).longValue());
            src.put("path", task.get("source_path") == null ? "" : String.valueOf(task.get("source_path")));
            Map<String, String> dst = targetConf(task);
            Map<String, Object> st = parseJson(String.valueOf(task.get("strategy_json")));
            String mode = normMode(task.get("sync_mode"));
            full = isFullRound(mode, task);
            String op = engineOp(full, st);
            conf = writeRcloneConf(taskId, src, dst);
            List<String> args = buildRcloneArgs(conf, src, dst, op, st);
            log.info("[filesync] run#{} task#{} mode={} op={} {} → {}", runId, taskId, mode, op,
                    srcRemote(src), dstRemote(dst));
            Process p = new ProcessBuilder(args).redirectErrorStream(true).start();
            try (BufferedReader br = new BufferedReader(
                    new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = br.readLine()) != null) {
                    if (line.isBlank()) continue;
                    JsonNode n;
                    try {
                        n = OM.readTree(line);
                    } catch (Exception e) {
                        continue;
                    }
                    if (!n.isObject()) continue;
                    if (n.has("stats")) {   // rclone 周期性/收尾 stats → run.summary
                        stats = OM.convertValue(n.get("stats"), LinkedHashMap.class);
                        continue;
                    }
                    String level = n.path("level").asText();
                    String msg = n.path("msg").asText("");
                    String object = n.path("object").asText("");
                    if (msg.startsWith("Config file")) continue;          // rclone.conf 缺失噪声
                    if ("error".equals(level)) {
                        if (msg.startsWith("Attempt ")) { errorTail = msg; continue; }   // 重试汇总, 非文件级
                        // 文件级错误才有 *Object 的 objectType(目录/桶级错误只进 errorTail)
                        if (n.path("objectType").asText("").endsWith(".Object")) {
                            failN++;
                            insertRunFile(runId, object.isEmpty() ? "-" : object, "error", 0, truncate(msg, 1900));
                        }
                        errorTail = msg;
                    } else if (msg.startsWith("Copied")) {                 // Copied (new) / (replaced existing)
                        long sz = n.path("size").asLong(0);
                        insertRunFile(runId, object.isEmpty() ? "-" : object, "success", sz, null);
                        okN++;
                        bytes += sz;
                    } else if ("Deleted".equals(msg)) {                    // 仅全量(sync)会删目标多余
                        insertRunFile(runId, object.isEmpty() ? "-" : object, "deleted", 0, null);
                        delN++;
                    }
                }
            }
            int code;
            if (!p.waitFor(RUN_TIMEOUT_SEC, TimeUnit.SECONDS)) {
                p.destroyForcibly();
                code = -1;
                if (errorTail == null) errorTail = "rclone 执行超时(" + RUN_TIMEOUT_SEC + "s)";
            } else {
                code = p.exitValue();
            }
            if (stats != null && stats.get("errors") instanceof Number se && se.longValue() > 0) {
                failN = (int) se.longValue();      // stats.errors 为准(含未落 run_file 的错误)
            }
            if (code != 0 && okN == 0 && delN == 0) {
                failN = Math.max(failN, 1);
                if (errorTail == null) errorTail = "rclone 退出码 " + code;
            }
        } catch (Exception e) {
            failN++;
            errorTail = e.getMessage();
        } finally {
            if (conf != null) {
                try {
                    Files.deleteIfExists(conf);   // 临时 rclone.conf 含凭证, 轮次结束即删
                } catch (Exception ignored) {
                }
            }
        }
        String status = failN > 0 && okN == 0 && delN == 0 ? "FAILED" : "COMPLETED";
        if ("COMPLETED".equals(status) && full) {
            // 全量轮成功 → 标记基线就绪(full_then_incr 下一轮转增量)
            metaJdbc.update("UPDATE file_sync_task SET baseline_done = 1 WHERE id = ?", taskId);
        }
        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("op", engineOp(full, parseJson(String.valueOf(task.get("strategy_json")))));
        summary.put("transferred", okN);
        summary.put("deleted", delN);
        summary.put("bytes", stats != null && stats.get("bytes") instanceof Number b ? b.longValue() : bytes);
        summary.put("failed", failN);
        if (stats != null) {
            summary.put("checks", stats.get("checks"));
            summary.put("listed", stats.get("listed"));
        }
        summary.put("durationMs", System.currentTimeMillis() - t0);
        finishRun(runId, status, summary, errorTail, taskId);
    }

    /** 本轮是否为全量: full_once 恒全量; incr_scheduled 恒增量; full_then_incr 看基线 */
    private static boolean isFullRound(String mode, Map<String, Object> task) {
        if (MODE_FULL_ONCE.equals(mode)) return true;
        if (MODE_INCR.equals(mode)) return false;
        return !truthy(task.get("baseline_done"));
    }

    // ────────────────────────── rclone 引擎(命令构造/命名 remote) ──────────────────────────

    /** 本轮实际引擎动作: 全量默认 sync(镜像删多余); deleteExtras=false 时全量退化为 copy; 增量恒 copy */
    private static String engineOp(boolean full, Map<String, Object> st) {
        boolean deleteExtras = st.get("deleteExtras") == null || truthy(st.get("deleteExtras"));
        return full && deleteExtras ? "sync" : "copy";
    }

    /** 拼 rclone 参数: op 由 engineOp 决定(全量=sync 镜像 / 增量=copy), 策略直模 rclone flag */
    private List<String> buildRcloneArgs(Path conf, Map<String, String> src, Map<String, String> dst,
                                        String op, Map<String, Object> st) {
        List<String> args = new ArrayList<>();
        args.add("rclone");
        args.add(op);
        args.add(srcRemote(src));
        args.add(dstRemote(dst));
        args.add("--config");
        args.add(conf.toString());
        // 比较/覆盖口径
        String cmp = String.valueOf(st.getOrDefault("compare", "size_mtime"));
        if ("size_only".equals(cmp)) args.add("--size-only");
        else if ("checksum".equals(cmp)) args.add("--checksum");
        String um = String.valueOf(st.getOrDefault("updateMode", "overwrite"));
        if ("update".equals(um)) args.add("--update");
        else if ("ignore_existing".equals(um)) args.add("--ignore-existing");
        // 过滤(白/黑名单链 + 时间/大小)
        for (Object inc : list(st.get("includes"))) {
            args.add("--include");
            args.add(String.valueOf(inc));
        }
        for (Object ex : list(st.get("excludes"))) {
            args.add("--exclude");
            args.add(String.valueOf(ex));
        }
        addOpt(args, st, "minAge", "--min-age");
        addOpt(args, st, "maxAge", "--max-age");
        addOpt(args, st, "minSize", "--min-size");
        addOpt(args, st, "maxSize", "--max-size");
        // 性能/可靠性
        args.add("--transfers");
        args.add(String.valueOf(intOr(st.get("transfers"), 8)));
        args.add("--checkers");
        args.add(String.valueOf(intOr(st.get("checkers"), 8)));
        addOpt(args, st, "bwlimit", "--bwlimit");
        args.add("--retries");
        args.add(String.valueOf(intOr(st.get("retries"), 3)));
        args.add("--low-level-retries");
        args.add(String.valueOf(intOr(st.get("lowLevelRetries"), 10)));
        if (truthy(st.get("dryRun"))) args.add("--dry-run");
        // 记账必需的日志口径(固定): 逐行 JSON + 每秒 stats
        args.add("--use-json-log");
        args.add("--log-level");
        args.add("INFO");
        args.add("--stats");
        args.add("1s");
        return args;
    }

    /** 生成临时 rclone.conf(命名 remote dcpsrc/dcpdst; 含明文凭证 → 轮次结束即删) */
    private Path writeRcloneConf(long taskId, Map<String, String> src, Map<String, String> dst) throws Exception {
        StringBuilder sb = new StringBuilder();
        if ("s3".equals(src.get("type"))) sb.append(s3Section(REMOTE_SRC, src));
        sb.append(s3Section(REMOTE_DST, dst));
        Path f = Path.of("/tmp/dcp-rclone-" + taskId + "-" + System.nanoTime() + ".conf");
        Files.writeString(f, sb.toString(), StandardCharsets.UTF_8);
        try {
            Files.setPosixFilePermissions(f,
                    java.nio.file.attribute.PosixFilePermissions.fromString("rw-------"));
        } catch (Exception ignored) {
        }
        return f;
    }

    private static String s3Section(String name, Map<String, String> conf) {
        return "[" + name + "]\n"
                + "type = s3\n"
                + "provider = Minio\n"
                + "endpoint = " + String.valueOf(conf.getOrDefault("endpoint", "")) + "\n"
                + "access_key_id = " + String.valueOf(conf.getOrDefault("accessKey", "")) + "\n"
                + "secret_access_key = " + String.valueOf(conf.getOrDefault("secretKey", "")) + "\n"
                + "force_path_style = true\n\n";
    }

    /** 源侧 rclone 路径: local → 容器内绝对路径(不经 remote); s3 → dcpsrc:bucket/path */
    private static String srcRemote(Map<String, String> src) {
        String path = strip(src.get("path") == null ? "" : src.get("path"));
        if ("local".equals(src.get("type"))) {
            String root = src.get("root") == null ? "" : src.get("root");   // 根不能 strip(绝对路径)
            if (path.isEmpty()) return root;
            return (root.endsWith("/") ? root : root + "/") + path;
        }
        String bucket = src.get("bucket") == null ? "" : src.get("bucket");
        return REMOTE_SRC + ":" + bucket + (path.isEmpty() ? "" : "/" + path);
    }

    /** 目标侧 rclone 路径: dcpdst:bucket/prefix */
    private static String dstRemote(Map<String, String> dst) {
        String bucket = dst.get("bucket") == null ? "" : dst.get("bucket");
        String prefix = strip(dst.get("prefix") == null ? "" : dst.get("prefix"));
        return REMOTE_DST + ":" + bucket + (prefix.isEmpty() ? "" : "/" + prefix);
    }

    private static void addOpt(List<String> args, Map<String, Object> st, String key, String flag) {
        Object v = st.get(key);
        if (v == null || String.valueOf(v).isBlank()) return;
        args.add(flag);
        args.add(String.valueOf(v));
    }

    private static int intOr(Object v, int dft) {
        if (v instanceof Number n) return n.intValue();
        try {
            return v == null || String.valueOf(v).isBlank() ? dft : Integer.parseInt(String.valueOf(v).trim());
        } catch (Exception e) {
            return dft;
        }
    }

    private static boolean truthy(Object v) {
        if (v == null) return false;
        if (v instanceof Boolean b) return b;
        if (v instanceof Number n) return n.intValue() != 0;
        String s = String.valueOf(v);
        return "1".equals(s) || "true".equalsIgnoreCase(s);
    }

    static String normMode(Object v) {
        String s = v == null ? "" : String.valueOf(v);
        if (MODE_FULL_ONCE.equals(s) || MODE_INCR.equals(s) || MODE_FULL_THEN_INCR.equals(s)) return s;
        return MODE_FULL_THEN_INCR;   // 缺省/存量 → 最常用的"全量+定时增量"
    }

    /** 平台执行: 源/目标连接解析(s3 走命名 remote; local 源直接绝对路径, 单线程串行执行) */
    private Map<String, String> sourceConf(long datasourceId) {
        List<Map<String, Object>> rows = metaJdbc.queryForList(
                "SELECT type, host, port, database_name, username, password_ciphertext, extra_params "
                        + "FROM datasource_config WHERE id = ?", datasourceId);
        if (rows.isEmpty()) throw new IllegalStateException("源数据源不存在: " + datasourceId);
        Map<String, Object> r = rows.get(0);
        String type = String.valueOf(r.get("type"));
        ObjectNode node = extraNode(r.get("extra_params"));
        Map<String, String> conf = new LinkedHashMap<>();
        conf.put("type", type);
        if ("local".equals(type)) {
            String root = node.path("root").asText("/data");
            conf.put("root", root);
            conf.put("endpoint", "");
            conf.put("accessKey", "");
            conf.put("secretKey", "");
        } else if ("s3".equals(type)) {
            String ak = String.valueOf(r.get("username"));
            String sk = com.dcp.common.CryptoUtil.decrypt(String.valueOf(r.get("password_ciphertext")), encKey);
            conf.put("endpoint", "http://" + r.get("host") + ":" + r.get("port"));
            conf.put("accessKey", ak);
            conf.put("secretKey", sk);
            conf.put("bucket", node.path("bucket").asText(r.get("database_name") == null ? "" : String.valueOf(r.get("database_name"))));
        } else {
            throw new IllegalStateException("文件同步源仅支持 s3/local 数据源: " + type);
        }
        return conf;
    }

    private Map<String, String> targetConf(Map<String, Object> task) {
        Number dsId = (Number) task.get("target_datasource_id");
        Map<String, String> conf = new LinkedHashMap<>();
        String bucket = String.valueOf(task.get("target_bucket"));
        String prefix = task.get("target_prefix") == null ? "" : strip(String.valueOf(task.get("target_prefix")));
        if (dsId == null || dsId.longValue() == 0) {
            conf.put("endpoint", dstEndpoint);
            conf.put("accessKey", dstAccessKey);
            conf.put("secretKey", dstSecretKey);
        } else {
            List<Map<String, Object>> rows = metaJdbc.queryForList(
                    "SELECT host, port, username, password_ciphertext FROM datasource_config WHERE id = ? AND type = 's3'",
                    dsId.longValue());
            if (rows.isEmpty()) throw new IllegalStateException("目标数据源不存在或非 s3: " + dsId);
            Map<String, Object> r = rows.get(0);
            conf.put("endpoint", "http://" + r.get("host") + ":" + r.get("port"));
            conf.put("accessKey", String.valueOf(r.get("username")));
            conf.put("secretKey", com.dcp.common.CryptoUtil.decrypt(String.valueOf(r.get("password_ciphertext")), encKey));
        }
        conf.put("bucket", bucket);
        conf.put("prefix", prefix);
        return conf;
    }

    // 拼 mirror 源/目标路径已由 srcRemote/dstRemote 取代(命名 remote, 无镜像路径状态)

    // ────────────────────────── run 落库 ──────────────────────────

    private long insertRun(long taskId, String trigger, String executor, String strategySnapshot) {
        GeneratedKeyHolder kh = new GeneratedKeyHolder();
        metaJdbc.update(con -> {
            PreparedStatement ps = con.prepareStatement(
                    "INSERT INTO file_sync_run (task_id, trigger_type, executor, strategy_snapshot, status, started_at) "
                            + "VALUES (?, ?, ?, ?, 'RUNNING', NOW())", Statement.RETURN_GENERATED_KEYS);
            ps.setLong(1, taskId);
            ps.setString(2, trigger);
            ps.setString(3, executor);
            ps.setString(4, strategySnapshot);
            return ps;
        }, kh);
        return kh.getKey().longValue();
    }

    private void insertRunFile(long runId, String relPath, String status, long size, String error) {
        metaJdbc.update("INSERT INTO file_sync_run_file (run_id, rel_path, status, size, error_msg) VALUES (?, ?, ?, ?, ?)",
                runId, relPath, status, size, error);
    }

    private void finishRun(long runId, String status, Map<String, Object> summary, String error, long taskId) {
        String summaryJson;
        try {
            summaryJson = OM.writeValueAsString(summary);
        } catch (Exception e) {
            summaryJson = "{}";
        }
        metaJdbc.update("UPDATE file_sync_run SET status = ?, summary = ?, error_msg = ?, finished_at = NOW() WHERE id = ?",
                status, summaryJson, truncate(error, 1900), runId);
        metaJdbc.update("UPDATE file_sync_task SET run_status = ?, last_run_summary = ?, last_run_time = NOW() WHERE id = ?",
                status, summaryJson, taskId);
    }

    // ────────────────────────── 目标端浏览(rclone lsjson) ──────────────────────────

    /** 目标桶实时浏览: path 为相对任务目标前缀的子目录(空=前缀根), 单层列出(rclone lsjson) */
    public Map<String, Object> browse(long taskId, String subPath) {
        Map<String, Object> task = detail(taskId);
        if (task == null) throw new IllegalArgumentException("任务不存在: " + taskId);
        Map<String, String> dst = targetConf(task);
        String bucket = dst.get("bucket");
        String basePrefix = strip(dst.get("prefix"));
        String rel = subPath == null ? "" : strip(String.valueOf(subPath)).replace("../", "").replace("..", "");
        String fullPrefix = joinPrefix(basePrefix, rel);
        Path conf = null;
        try {
            conf = writeRcloneConf(taskId, Map.of("type", "local"), dst);
            String path = REMOTE_DST + ":" + bucket + (fullPrefix.isEmpty() ? "" : "/" + fullPrefix);
            Process p = new ProcessBuilder("rclone", "lsjson", path, "--config", conf.toString(), "--log-level", "ERROR")
                    .redirectErrorStream(true).start();
            StringBuilder out = new StringBuilder();
            try (BufferedReader br = new BufferedReader(
                    new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = br.readLine()) != null) {
                    if (out.length() < 4_000_000) out.append(line).append('\n');
                }
            }
            int code;
            if (!p.waitFor(60, TimeUnit.SECONDS)) {
                p.destroyForcibly();
                code = -1;
            } else {
                code = p.exitValue();
            }
            List<Map<String, Object>> items = new ArrayList<>();
            if (code == 0) {
                JsonNode arr = OM.readTree(out.toString());   // rclone lsjson → JSON 数组
                if (arr != null && arr.isArray()) {
                    for (JsonNode n : arr) {
                        String name = n.path("Name").asText("");
                        if (name.isEmpty()) continue;
                        boolean folder = n.path("IsDir").asBoolean(false);
                        Map<String, Object> it = new LinkedHashMap<>();
                        it.put("name", name);
                        it.put("folder", folder);
                        it.put("size", folder ? 0L : n.path("Size").asLong(0));
                        it.put("lastModified", folder ? "" : n.path("ModTime").asText(""));
                        items.add(it);
                    }
                }
            }
            Map<String, Object> outMap = new LinkedHashMap<>();
            outMap.put("bucket", bucket);
            outMap.put("prefix", fullPrefix);
            outMap.put("parent", parentOf(rel));
            outMap.put("items", items);
            outMap.put("error", code != 0 && items.isEmpty() ? truncate(out.toString().trim(), 500) : null);
            return outMap;
        } catch (Exception e) {
            throw new IllegalArgumentException("浏览失败: " + e.getMessage());
        } finally {
            if (conf != null) {
                try {
                    Files.deleteIfExists(conf);
                } catch (Exception ignored) {
                }
            }
        }
    }

    private static String joinPrefix(String a, String b) {
        a = strip(a);
        b = strip(b);
        if (a.isEmpty()) return b;
        if (b.isEmpty()) return a;
        return a + "/" + b;
    }

    private static String parentOf(String rel) {
        rel = strip(rel);
        int idx = rel.lastIndexOf('/');
        return idx < 0 ? "" : rel.substring(0, idx);
    }

    // ────────────────────────── run 归档与实时同步(watch) ──────────────────────────

    @Value("${dcp.filesync.run-retention-days:30}")
    private int runRetentionDays;

    /** 轮次历史保留: 每日清理超过保留天数的 run 及其文件明细 */
    @Scheduled(cron = "0 30 3 * * ?")
    public void cleanOldRuns() {
        try {
            metaJdbc.update("DELETE rf FROM file_sync_run_file rf JOIN file_sync_run r ON r.id = rf.run_id "
                    + "WHERE r.started_at < NOW() - INTERVAL ? DAY", runRetentionDays);
            int del = metaJdbc.update("DELETE FROM file_sync_run WHERE started_at < NOW() - INTERVAL ? DAY", runRetentionDays);
            log.info("轮次归档清理完成: 删除 {} 天前 run {} 条", runRetentionDays, del);
        } catch (Exception e) {
            log.warn("run 归档清理异常: {}", e.getMessage());
        }
    }

    // ─────────────────── 常驻实时(watch)已随 mc 移除 ───────────────────
    // rclone 无等价常驻监听; 近实时需求用"仅定时增量/全量+定时增量"把周期设短(最小 1min)。
    // 原 startWatch/stopWatch/watchLoop/watchRunning 与 WatchSession 一并删除。

    // ────────────────────────── 定时调度(platform 任务) ──────────────────────────

    @Scheduled(fixedDelay = 15000)
    public void autoTick() {
        try {
            List<Map<String, Object>> rows = metaJdbc.queryForList(
                    "SELECT id, exec_host, schedule_mode, interval_min, cron_expr, last_run_time FROM file_sync_task "
                            + "WHERE sync_mode <> 'full_once' AND schedule_mode IN ('interval','cron')");
            long now = System.currentTimeMillis();
            LocalDateTime cur = LocalDateTime.now();
            for (Map<String, Object> r : rows) {
                if (!"platform".equals(String.valueOf(r.get("exec_host")))) {
                    continue; // agent 任务由 agent 本地调度
                }
                long id = ((Number) r.get("id")).longValue();
                if (running.containsKey(id) && running.get(id).get()) {
                    continue;
                }
                String sched = String.valueOf(r.get("schedule_mode"));
                boolean due;
                if ("interval".equals(sched)) {
                    int interval = r.get("interval_min") == null ? 5 : ((Number) r.get("interval_min")).intValue();
                    long lastMs = toEpoch(r.get("last_run_time"));
                    due = now >= lastMs + Math.max(interval, 1) * 60000L;
                } else {
                    String expr = r.get("cron_expr") == null ? null : String.valueOf(r.get("cron_expr"));
                    // 分钟粒度: tick 几乎不可能精确落在第 0 秒, 用该分钟的 0 秒代表时刻判匹配
                    LocalDateTime rep = cur.withSecond(0);
                    int minuteKey = rep.getYear() * 1000000 + rep.getMonthValue() * 10000
                            + rep.getDayOfMonth() * 100 + rep.getHour() * 100 + rep.getMinute();
                    due = expr != null && cronMatches(expr, rep)
                            && !Integer.valueOf(minuteKey).equals(lastCronMinute.putIfAbsent(id, minuteKey));
                }
                if (due) {
                    runAsync(id, "schedule");
                }
            }
        } catch (Exception e) {
            log.warn("文件同步定时 tick 异常: {}", e.getMessage());
        }
    }

    private static long toEpoch(Object last) {
        if (last == null) return 0;
        if (last instanceof java.sql.Timestamp t) return t.getTime();
        if (last instanceof LocalDateTime l) return l.atZone(java.time.ZoneId.systemDefault()).toInstant().toEpochMilli();
        if (last instanceof java.util.Date d) return d.getTime();
        return 0;
    }

    /** 轻量 6 段 cron 匹配(秒 分 时 日 月 周; ? == *; 支持 *、范围、步进); 5 段时前置秒=0 */
    static boolean cronMatches(String expr, LocalDateTime t) {
        String[] f = expr.trim().split("\\s+");
        if (f.length == 5) {
            String[] g = new String[6];
            g[0] = "0";
            System.arraycopy(f, 0, g, 1, 5);
            f = g;
        }
        if (f.length != 6) return false;
        int[] v = {t.getSecond(), t.getMinute(), t.getHour(), t.getDayOfMonth(),
                t.getMonthValue(), t.getDayOfWeek().getValue()};
        int[] lo = {0, 0, 0, 1, 1, 1};
        int[] hi = {59, 59, 23, 31, 12, 7};
        for (int i = 0; i < 6; i++) {
            if (!inField(f[i], v[i], lo[i], hi[i], i)) return false;
        }
        return true;
    }

    private static boolean inField(String token, int val, int lo, int hi, int idx) {
        if (token == null || token.isBlank()) return false;
        if (token.equals("*") || (idx >= 3 && token.equals("?"))) return true;
        for (String part : token.split(",")) {
            String p = part.trim();
            if (p.isEmpty()) continue;
            int slash = p.indexOf('/');
            int step = slash >= 0 ? Integer.parseInt(p.substring(slash + 1)) : 1;
            String base = slash >= 0 ? p.substring(0, slash) : p;
            int a = lo, b = hi;
            if (!base.equals("*")) {
                int dash = base.indexOf('-');
                if (dash >= 0) {
                    a = Integer.parseInt(base.substring(0, dash));
                    b = Integer.parseInt(base.substring(dash + 1));
                } else {
                    a = b = Integer.parseInt(base);
                }
            }
            if (val >= a && val <= b && (val - a) % step == 0) return true;
        }
        return false;
    }

    // ────────────────────────── run 查询 ──────────────────────────

    public Map<String, Object> runsPage(long taskId, int page, int pageSize) {
        int offset = Math.max(page - 1, 0) * pageSize;
        List<Map<String, Object>> rows = metaJdbc.queryForList(
                "SELECT id, trigger_type, executor, status, summary, error_msg, started_at, finished_at "
                        + "FROM file_sync_run WHERE task_id = ? ORDER BY id DESC LIMIT ? OFFSET ?",
                taskId, pageSize, offset);
        rows.forEach(r -> r.put("summary", parseJson(String.valueOf(r.get("summary")))));
        int total = Optional.ofNullable(metaJdbc.queryForObject(
                "SELECT COUNT(*) FROM file_sync_run WHERE task_id = ?", Integer.class, taskId)).orElse(0);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("items", rows);
        out.put("total", total);
        out.put("page", page);
        out.put("pageSize", pageSize);
        return out;
    }

    public Map<String, Object> runFilesPage(long runId, int page, int pageSize) {
        int offset = Math.max(page - 1, 0) * pageSize;
        List<Map<String, Object>> rows = metaJdbc.queryForList(
                "SELECT rel_path, status, size, error_msg, event_at FROM file_sync_run_file "
                        + "WHERE run_id = ? ORDER BY id DESC LIMIT ? OFFSET ?", runId, pageSize, offset);
        int total = Optional.ofNullable(metaJdbc.queryForObject(
                "SELECT COUNT(*) FROM file_sync_run_file WHERE run_id = ?", Integer.class, runId)).orElse(0);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("items", rows);
        out.put("total", total);
        out.put("page", page);
        out.put("pageSize", pageSize);
        return out;
    }

    // ────────────────────────── Agent 任务下发/上报 ──────────────────────────

    /** agent 任务指令 payload = 完整任务定义(含解密后的连接, agent 不连平台库) */
    public Map<String, Object> buildAgentPayload(long taskId) {
        Map<String, Object> task = detail(taskId);
        if (task == null) throw new IllegalStateException("任务不存在: " + taskId);
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("taskId", taskId);
        payload.put("name", task.get("name"));
        payload.put("version", task.get("version"));
        payload.put("strategy", parseJson(String.valueOf(task.get("strategy_json"))));
        // 引擎切换后: 模式三态 + 基线标记(agent 侧据此决定本轮 sync/copy)
        payload.put("syncMode", normMode(task.get("sync_mode")));
        payload.put("baselineDone", truthy(task.get("baseline_done")));
        Map<String, Object> schedule = new LinkedHashMap<>();
        schedule.put("mode", task.get("schedule_mode"));
        schedule.put("intervalMin", task.get("interval_min"));
        schedule.put("cronExpr", task.get("cron_expr"));
        payload.put("schedule", schedule);
        // 源
        Map<String, String> src = sourceConf(((Number) task.get("source_datasource_id")).longValue());
        Map<String, Object> source = new LinkedHashMap<>();
        source.put("type", src.get("type"));
        if ("local".equals(src.get("type"))) {
            source.put("root", src.get("root"));
            source.put("path", task.get("source_path"));
        } else {
            source.put("endpoint", src.get("endpoint"));
            source.put("accessKey", src.get("accessKey"));
            source.put("secretKey", src.get("secretKey"));
            source.put("bucket", src.get("bucket"));
            source.put("path", task.get("source_path"));
        }
        payload.put("source", source);
        // 目标凭证直下发(受限子账号机制已按拍板移除, 2026-09-10)
        Map<String, String> dst = targetConf(task);
        Map<String, Object> target = new LinkedHashMap<>();
        target.put("endpoint", dst.get("endpoint"));
        target.put("accessKey", dst.get("accessKey"));
        target.put("secretKey", dst.get("secretKey"));
        target.put("bucket", dst.get("bucket"));
        target.put("prefix", dst.get("prefix"));
        payload.put("target", target);
        return payload;
    }

    private void pushAgentCommand(long agentId, long taskId, String cmd) {
        agentService.enqueue(agentId, taskId, cmd, cmd.equals("task_upsert") ? buildAgentPayload(taskId) : null);
    }

    /** 停止 agent 任务的实时监听: 下发 task_stop 指令 */
    public boolean stopAgentWatch(long taskId) {
        List<Map<String, Object>> rows = metaJdbc.queryForList(
                "SELECT agent_id FROM file_sync_task WHERE id = ? AND exec_host = 'agent'", taskId);
        if (rows.isEmpty()) return false;
        agentService.enqueue(((Number) rows.get(0).get("agent_id")).longValue(), taskId, "task_stop", null);
        return true;
    }

    /** Agent 对账用: 校验归属后重发 task_upsert(任务定义可能因指令丢失/应用失败而缺失) */
    public boolean resendAgentTask(long agentId, long taskId) {
        List<Map<String, Object>> rows = metaJdbc.queryForList(
                "SELECT id FROM file_sync_task WHERE id = ? AND exec_host = 'agent' AND agent_id = ?", taskId, agentId);
        if (rows.isEmpty()) {
            return false;
        }
        pushAgentCommand(agentId, taskId, "task_upsert");
        return true;
    }

    // ────────────────────────── 工具 ──────────────────────────

    private static String normSched(Object v) {
        String s = v == null ? "none" : String.valueOf(v);
        return s.equals("interval") || s.equals("cron") ? s : "none";
    }

    private static String jsonOf(Object o) {
        if (o == null) return "{}";
        try {
            return OM.writeValueAsString(o);
        } catch (Exception e) {
            throw new IllegalArgumentException("strategy 序列化失败: " + e.getMessage());
        }
    }

    private static Map<String, Object> parseJson(String s) {
        if (s == null || s.isBlank() || "null".equals(s)) return new LinkedHashMap<>();
        try {
            return OM.readValue(s, LinkedHashMap.class);
        } catch (Exception e) {
            return new LinkedHashMap<>();
        }
    }

    private static boolean bool(Map<String, Object> m, String k) {
        Object v = m.get(k);
        return Boolean.TRUE.equals(v);
    }

    private static List<Object> list(Object o) {
        if (o instanceof List<?> l) return new ArrayList<>(l);
        return List.of();
    }

    private static ObjectNode extraNode(Object extra) {
        try {
            if (extra == null) return OM.createObjectNode();
            String s = String.valueOf(extra);
            if (s.isBlank() || "null".equals(s)) return OM.createObjectNode();
            JsonNode n = OM.readTree(s);
            return n instanceof ObjectNode on ? on : OM.createObjectNode();
        } catch (Exception e) {
            return OM.createObjectNode();
        }
    }

    // ────────────────────────── 一致性对账(只报告, 不修改) ──────────────────────────

    private static final int RECONCILE_MAX_ENTRIES = 200000;
    private static final int RECONCILE_SAMPLE = 200;

    /**
     * 源/目标清单对账: 列源(递归)与目标(递归) → 三类差异(目标缺失/目标多余/大小不一致)。
     * 只报告不修改(与 remove=false 默认一致); 结果落 file_sync_reconcile 供历史/告警联动。
     */
    public Map<String, Object> reconcile(long taskId, String trigger) {
        long t0 = System.currentTimeMillis();
        Map<String, Object> task = detail(taskId);
        if (task == null) throw new IllegalArgumentException("任务不存在: " + taskId);
        Map<String, String> src = sourceConf(((Number) task.get("source_datasource_id")).longValue());
        Map<String, String> dst = targetConf(task);
        src.put("path", task.get("source_path") == null ? "" : String.valueOf(task.get("source_path")));
        try {
            Path conf = writeRcloneConf(taskId, src, dst);
            try {
            Map<String, Long> srcMap = listSide(conf, src, true);
            Map<String, Long> dstMap = listSide(conf, dst, false);
            List<Map<String, Object>> missing = new ArrayList<>();
            List<Map<String, Object>> sizeDiff = new ArrayList<>();
            for (Map.Entry<String, Long> e : srcMap.entrySet()) {
                Long d = dstMap.get(e.getKey());
                if (d == null) {
                    if (missing.size() < RECONCILE_SAMPLE) missing.add(item(e.getKey(), e.getValue(), 0L, "missing_in_target"));
                } else if (!d.equals(e.getValue())) {
                    if (sizeDiff.size() < RECONCILE_SAMPLE) sizeDiff.add(item(e.getKey(), e.getValue(), d, "size_mismatch"));
                }
            }
            List<Map<String, Object>> extra = new ArrayList<>();
            for (Map.Entry<String, Long> e : dstMap.entrySet()) {
                if (!srcMap.containsKey(e.getKey()) && extra.size() < RECONCILE_SAMPLE) {
                    extra.add(item(e.getKey(), 0L, e.getValue(), "extra_in_target"));
                }
            }
            long missingAll = srcMap.keySet().stream().filter(k -> !dstMap.containsKey(k)).count();
            long sizeDiffAll = srcMap.entrySet().stream()
                    .filter(e -> dstMap.containsKey(e.getKey()) && !dstMap.get(e.getKey()).equals(e.getValue())).count();
            long extraAll = dstMap.keySet().stream().filter(k -> !srcMap.containsKey(k)).count();
            boolean truncated = srcMap.size() >= RECONCILE_MAX_ENTRIES || dstMap.size() >= RECONCILE_MAX_ENTRIES;
            Map<String, Object> detail = new LinkedHashMap<>();
            detail.put("missing", missing);
            detail.put("extra", extra);
            detail.put("sizeMismatch", sizeDiff);
            String detailJson = OM.writeValueAsString(detail);
            metaJdbc.update("INSERT INTO file_sync_reconcile(task_id, trigger_type, source_count, target_count, "
                            + "missing_count, extra_count, size_diff_count, detail_json, truncated, elapsed_ms) "
                            + "VALUES(?,?,?,?,?,?,?,?,?,?)",
                    taskId, trigger == null ? "manual" : trigger, srcMap.size(), dstMap.size(),
                    missingAll, extraAll, sizeDiffAll, detailJson, truncated ? 1 : 0, System.currentTimeMillis() - t0);
            Number id = metaJdbc.queryForObject("SELECT LAST_INSERT_ID()", Number.class);
            Map<String, Object> out = new LinkedHashMap<>();
            out.put("id", id == null ? null : id.longValue());
            out.put("taskId", taskId);
            out.put("sourceCount", srcMap.size());
            out.put("targetCount", dstMap.size());
            out.put("missingCount", missingAll);
            out.put("extraCount", extraAll);
            out.put("sizeDiffCount", sizeDiffAll);
            out.put("missing", missing);
            out.put("extra", extra);
            out.put("sizeMismatch", sizeDiff);
            out.put("truncated", truncated);
            out.put("execHost", task.get("exec_host"));
            if ("agent".equals(String.valueOf(task.get("exec_host")))) {
                out.put("note", "该任务执行端为 Agent: 本次对账在平台侧读取源路径(演示环境共享挂载);"
                        + "生产业务机需 Agent 侧上报文件清单后对账");
            }
            out.put("elapsedMs", System.currentTimeMillis() - t0);
            log.info("[filesync] 对账 task#{} 源{} 目标{} 缺{} 多{} 大小差{} ({}ms)",
                    taskId, srcMap.size(), dstMap.size(), missingAll, extraAll, sizeDiffAll, out.get("elapsedMs"));
            return out;
            } finally {
                try {
                    Files.deleteIfExists(conf);
                } catch (Exception ignored) {
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException("对账失败: " + e.getMessage(), e);
        }
    }

    public List<Map<String, Object>> reconciles(long taskId, int limit) {
        int lim = Math.max(1, Math.min(limit <= 0 ? 20 : limit, 200));
        return metaJdbc.queryForList(
                "SELECT id, task_id, trigger_type, source_count, target_count, missing_count, extra_count, "
                        + "size_diff_count, truncated, elapsed_ms, create_time, detail_json "
                        + "FROM file_sync_reconcile WHERE task_id = ? ORDER BY id DESC LIMIT " + lim, taskId);
    }

    /** 列一侧清单: 相对路径→size(源/目标统一 `rclone lsjson --recursive --files-only`) */
    private Map<String, Long> listSide(Path rcloneConf, Map<String, String> conf, boolean isSrc) throws Exception {
        Map<String, Long> out = new LinkedHashMap<>();
        String remote = isSrc ? srcRemote(conf) : dstRemote(conf);
        if (isSrc && "local".equals(conf.get("type")) && !java.nio.file.Files.exists(Path.of(remote))) {
            throw new IllegalStateException("源目录不存在: " + remote);
        }
        Process p = new ProcessBuilder("rclone", "lsjson", remote, "--recursive", "--files-only",
                "--config", rcloneConf.toString(), "--log-level", "ERROR")
                .redirectErrorStream(true).start();
        StringBuilder sb = new StringBuilder();
        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (sb.length() < 64_000_000) sb.append(line).append('\n');
            }
        }
        if (!p.waitFor(300, TimeUnit.SECONDS)) {
            p.destroyForcibly();
            throw new IllegalStateException("rclone lsjson 超时: " + remote);
        }
        if (p.exitValue() != 0) {
            throw new IllegalStateException("rclone lsjson 失败(" + p.exitValue() + "): " + truncate(sb.toString().trim(), 300));
        }
        JsonNode arr = OM.readTree(sb.toString());
        if (arr != null && arr.isArray()) {
            for (JsonNode n : arr) {
                if (out.size() >= RECONCILE_MAX_ENTRIES) break;
                if (n.path("IsDir").asBoolean(false)) continue;   // 目录条目需过滤(lsjson 会返回)
                String rel = n.path("Path").asText("");
                if (rel.isEmpty()) continue;
                out.put(rel, n.path("Size").asLong(0));
            }
        }
        return out;
    }

    private static Map<String, Object> item(String rel, long srcSize, long dstSize, String kind) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("relPath", rel);
        m.put("sourceSize", srcSize);
        m.put("targetSize", dstSize);
        m.put("kind", kind);
        return m;
    }

    private static String strip(String p) {
        return p == null ? "" : p.replaceAll("^/+|/+$", "");
    }

    private static String truncate(String s, int n) {
        if (s == null) return null;
        return s.length() > n ? s.substring(0, n) : s;
    }

}
