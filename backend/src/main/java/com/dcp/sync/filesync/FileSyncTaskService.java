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
 * 文件同步 V3 —— mc mirror 原生(设计定稿 2026-09-07, 取代 V2 的 cp+账本架构):
 *
 * <pre>
 *   任务 = 一次 mc mirror 会话的描述: 源(文件数据源+路径) + 目标(数据源+bucket/prefix)
 *         + strategy(直模 mc mirror 参数: overwrite/remove/preserve/dryRun/retry/exclude/timeFilter/advanced)
 *         + watch(常驻, P1) + schedule(none|interval|cron) + execHost(platform|agent)
 *   执行 = McMirrorRunner: 拼参 → mc mirror ... --json → 逐行解析事件 → 落 run/run_file
 *   增量语义内建于 mc mirror(只补缺失/overwrite 覆盖变更/0 变化轮 transferred=0), 无自研比对。
 *   旧 V1/V2 表(file_copy_task/ledger/file_meta)启动检测到即重建。
 * </pre>
 */
@Slf4j
@Component
public class FileSyncTaskService {

    private static final ObjectMapper OM = new ObjectMapper();
    private static final String ALIAS_SRC = "fsrc";
    private static final String ALIAS_DST = "fdst";

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
    private final Map<Long, WatchSession> watchers = new ConcurrentHashMap<>();

    /** 常驻实时同步会话(platform 执行): mc mirror --watch --json 子进程 + 事件记账线程 */
    private static final class WatchSession {
        final long taskId;
        final long runId;
        final Process process;
        final AtomicBoolean stopped = new AtomicBoolean(false);

        WatchSession(long taskId, long runId, Process process) {
            this.taskId = taskId;
            this.runId = runId;
            this.process = process;
        }
    }

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
                + "watch TINYINT DEFAULT 0,"
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
        log.info("FileSync V3 schema ensured");
    }

    @PreDestroy
    public void close() {
        watchers.values().forEach(s -> s.process.destroyForcibly());
        executor.shutdownNow();
    }

    // ────────────────────────── 任务 CRUD ──────────────────────────

    public Map<String, Object> create(Map<String, Object> body) {
        validate(body);
        boolean watch = Boolean.TRUE.equals(body.get("watch"));
        String execHost = "platform".equals(String.valueOf(body.getOrDefault("execHost", "platform"))) ? "platform" : "agent";
        Number agentId = execHost.equals("agent") ? (Number) body.get("agentId") : null;
        if (execHost.equals("agent") && agentId == null) {
            throw new IllegalArgumentException("execHost=agent 时必须提供 agentId");
        }
        String name = (String) body.get("name");
        String sched = watch ? "none" : normSched(body.get("scheduleMode"));   // watch 任务常驻, 无定时调度
        GeneratedKeyHolder kh = new GeneratedKeyHolder();
        metaJdbc.update(con -> {
            PreparedStatement ps = con.prepareStatement(
                    "INSERT INTO file_sync_task (name, description, exec_host, agent_id, source_datasource_id, "
                            + "source_path, target_datasource_id, target_bucket, target_prefix, strategy_json, "
                            + "watch, schedule_mode, interval_min, cron_expr) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
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
            ps.setInt(11, watch ? 1 : 0);
            ps.setString(12, sched);
            ps.setInt(13, "interval".equals(sched) ? Math.max(body.get("intervalMin") == null ? 5 : ((Number) body.get("intervalMin")).intValue(), 1) : 0);
            ps.setString(14, "cron".equals(sched) ? String.valueOf(body.get("cronExpr")) : null);
            return ps;
        }, kh);
        long id = kh.getKey().longValue();
        if (execHost.equals("agent")) {
            pushAgentCommand(agentId.longValue(), id, "task_upsert");
        }
        log.info("文件同步 V3 任务创建 id={} {} exec={} ds={} {}", id, name, execHost,
                body.get("sourceDatasourceId"), body.get("sourcePath"));
        return Map.of("id", id, "name", name, "execHost", execHost);
    }

    private void validate(Map<String, Object> body) {
        if (body.get("name") == null || String.valueOf(body.get("name")).isBlank()) throw new IllegalArgumentException("name 必填");
        if (body.get("sourceDatasourceId") == null) throw new IllegalArgumentException("sourceDatasourceId 必填");
        if (body.get("sourcePath") == null) throw new IllegalArgumentException("sourcePath 必填");
        if (body.get("targetBucket") == null || String.valueOf(body.get("targetBucket")).isBlank())
            throw new IllegalArgumentException("targetBucket 必填");
        String sched = normSched(body.get("scheduleMode"));
        if ("cron".equals(sched) && (body.get("cronExpr") == null || String.valueOf(body.get("cronExpr")).isBlank())) {
            throw new IllegalArgumentException("cron 模式必须提供 cronExpr");
        }
        Map<String, Object> st = body.get("strategy") instanceof Map<?, ?> m
                ? new LinkedHashMap<>() : new LinkedHashMap<>();
        if (body.get("strategy") instanceof Map<?, ?> sm) {
            sm.forEach((k, v) -> st.put(String.valueOf(k), v));
        }
        Object tf = st.get("timeFilter");
        if (tf instanceof Map<?, ?> t) {
            String dir = String.valueOf(t.get("dir"));
            if (!dir.equals("newer") && !dir.equals("older")) throw new IllegalArgumentException("timeFilter.dir 仅支持 newer|older");
            if (t.get("value") == null || String.valueOf(t.get("value")).isBlank())
                throw new IllegalArgumentException("timeFilter.value 必填(如 7d/2h30m)");
        }
    }

    public List<Map<String, Object>> list() {
        List<Map<String, Object>> rows = metaJdbc.queryForList(
                "SELECT t.id, t.name, t.description, t.exec_host, t.agent_id, t.source_datasource_id, t.source_path, "
                        + "t.target_datasource_id, t.target_bucket, t.target_prefix, t.strategy_json, t.watch, "
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
                "SELECT * FROM file_sync_task WHERE id = ?", id);
        if (rows.isEmpty()) return null;
        Map<String, Object> r = new LinkedHashMap<>(rows.get(0));
        r.put("strategy", parseJson(String.valueOf(r.get("strategy_json"))));
        r.put("online", agentOnline(r));
        r.put("watching", watchRunning(id));
        return r;
    }

    private void decorate(Map<String, Object> r) {
        r.put("strategy", parseJson(String.valueOf(r.get("strategy_json"))));
        r.put("online", agentOnline(r));
        r.put("watching", Number.class.isInstance(r.get("id")) && watchRunning(((Number) r.get("id")).longValue()));
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
        metaJdbc.update("UPDATE file_sync_task SET name = ?, description = ?, source_path = ?, target_bucket = ?, "
                        + "target_prefix = ?, strategy_json = ?, schedule_mode = ?, interval_min = ?, cron_expr = ?, "
                        + "version = version + 1 WHERE id = ?",
                body.getOrDefault("name", exist.get("name")),
                body.containsKey("description") ? body.get("description") : exist.get("description"),
                body.getOrDefault("sourcePath", exist.get("source_path")),
                body.getOrDefault("targetBucket", exist.get("target_bucket")),
                body.containsKey("targetPrefix") ? body.get("targetPrefix") : exist.get("target_prefix"),
                jsonOf(body.getOrDefault("strategy", exist.get("strategy"))),
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
        stopWatch(id);
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
        int okN = 0, failN = 0;
        long bytes = 0;
        String errorTail = null;
        try {
            Map<String, String> src = sourceConf(((Number) task.get("source_datasource_id")).longValue());
            src.put("path", task.get("source_path") == null ? "" : String.valueOf(task.get("source_path")));
            Map<String, String> dst = targetConf(task);
            ensureAliases(src);
            prepareMirrorPaths(src, dst);
            Map<String, Object> st = parseJson(String.valueOf(task.get("strategy_json")));
            List<String> args = buildMirrorArgs(src, dst, st, false);
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
                    String status = n.path("status").asText();
                    if ("success".equals(status)) {
                        String source = n.path("source").asText("");
                        if (!source.isEmpty()) {
                            long sz = n.path("size").asLong(0);
                            insertRunFile(runId, relOf(source, src), "success", sz, null);
                            okN++;
                            bytes += sz;
                        }
                    } else if ("error".equals(status)) {
                        String msg = n.path("error").path("message").asText(n.path("error").asText());
                        insertRunFile(runId, "-", "error", 0, truncate(msg, 1900));
                        failN++;
                        errorTail = msg;
                    }
                    // 汇总行 {total,transferred,duration} 忽略(统计以事件为准)
                }
            }
            int code;
            if (!p.waitFor(600, TimeUnit.SECONDS)) {
                p.destroyForcibly();
                code = -1;
            } else {
                code = p.exitValue();
            }
            if (code != 0 && okN == 0) {
                failN = Math.max(failN, 1);
                if (errorTail == null) errorTail = "mc mirror 退出码 " + code;
            }
        } catch (Exception e) {
            failN++;
            errorTail = e.getMessage();
        }
        String status = failN > 0 && okN == 0 ? "FAILED" : "COMPLETED";
        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("transferred", okN);
        summary.put("bytes", bytes);
        summary.put("failed", failN);
        summary.put("durationMs", System.currentTimeMillis() - t0);
        finishRun(runId, status, summary, errorTail, taskId);
    }

    private List<String> buildMirrorArgs(Map<String, String> src, Map<String, String> dst, Map<String, Object> st, boolean watch) {
        List<String> args = new ArrayList<>();
        args.add("mc");
        args.add("mirror");
        if (watch) args.add("--watch");
        if (bool(st, "overwrite")) args.add("--overwrite");
        if (bool(st, "remove")) args.add("--remove");
        if (bool(st, "preserve")) args.add("--preserve");
        if (bool(st, "dryRun")) args.add("--dry-run");
        if (bool(st, "retry")) args.add("--retry");
        for (Object ex : list(st.get("exclude"))) {
            args.add("--exclude");
            args.add(String.valueOf(ex));
        }
        Object tf = st.get("timeFilter");
        if (tf instanceof Map<?, ?> t) {
            String dir = String.valueOf(t.get("dir"));
            args.add("newer".equals(dir) ? "--newer-than" : "--older-than");
            args.add(String.valueOf(t.get("value")));
        }
        Object adv = st.get("advanced");
        if (adv instanceof Map<?, ?> a && a.get("maxWorkers") != null) {
            args.add("--max-workers");
            args.add(String.valueOf(a.get("maxWorkers")));
        }
        args.add(src.get("mirrorPath"));
        args.add(dst.get("mirrorPath"));
        args.add("--json");
        return args;
    }

    /** 平台执行: 源/目标 mc 连接解析(alias 固定 fsrc/fdst, 单线程串行执行) */
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

    private void ensureAliases(Map<String, String> src) throws Exception {
        if (!"local".equals(src.get("type"))) {
            runMcQuiet(List.of("alias", "set", ALIAS_SRC, src.get("endpoint"), src.get("accessKey"), src.get("secretKey")));
        }
        runMcQuiet(List.of("alias", "set", ALIAS_DST, dstEndpoint, dstAccessKey, dstSecretKey));
    }

    // 拼 mirror 源/目标路径; relRoot 供事件 source 剥离相对路径
    private void prepareMirrorPaths(Map<String, String> src, Map<String, String> dst) {
        if ("local".equals(src.get("type"))) {
            String root = src.get("root") == null ? "" : src.get("root");
            String p = strip(src.get("path") == null ? "" : src.get("path"));
            src.put("mirrorPath", p.isEmpty() ? root : (root.endsWith("/") ? root + p : root + "/" + p));
        } else {
            String bucket = src.get("bucket") == null ? "" : src.get("bucket");
            String p = strip(src.get("path") == null ? "" : src.get("path"));
            src.put("mirrorPath", ALIAS_SRC + "/" + bucket + (p.isEmpty() ? "" : "/" + p));
        }
        String prefix = dst.get("prefix") == null ? "" : dst.get("prefix");
        dst.put("mirrorPath", ALIAS_DST + "/" + dst.get("bucket") + (prefix.isEmpty() ? "" : "/" + prefix));
        src.put("relRoot", src.get("mirrorPath"));
    }

    private void runMcQuiet(List<String> args) throws Exception {
        List<String> cmd = new ArrayList<>();
        cmd.add("mc");
        cmd.addAll(args);
        Process p = new ProcessBuilder(cmd).redirectErrorStream(true).start();
        StringBuilder out = new StringBuilder();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (out.length() < 2000) out.append(line).append('\n');
            }
        }
        if (!p.waitFor(60, TimeUnit.SECONDS) || p.exitValue() != 0) {
            throw new IllegalStateException("mc " + args.get(0) + " 失败: " + out.toString().trim());
        }
    }

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

    // ────────────────────────── 目标端浏览(mc ls) ──────────────────────────

    /** 目标桶实时浏览: path 为相对任务目标前缀的子目录(空=前缀根), 单层列出 */
    public Map<String, Object> browse(long taskId, String subPath) {
        Map<String, Object> task = detail(taskId);
        if (task == null) throw new IllegalArgumentException("任务不存在: " + taskId);
        Map<String, String> dst = targetConf(task);
        String alias = "fbrowse" + System.nanoTime() % 100000;
        String bucket = dst.get("bucket");
        String basePrefix = strip(dst.get("prefix"));
        String rel = subPath == null ? "" : strip(String.valueOf(subPath)).replace("../", "").replace("..", "");
        String fullPrefix = joinPrefix(basePrefix, rel);
        try {
            runMcQuiet(List.of("alias", "set", alias, dst.get("endpoint"), dst.get("accessKey"), dst.get("secretKey")));
            String path = alias + "/" + bucket + (fullPrefix.isEmpty() ? "" : "/" + fullPrefix);
            Process p = new ProcessBuilder("mc", "ls", "--json", path).redirectErrorStream(true).start();
            StringBuilder err = new StringBuilder();
            List<Map<String, Object>> items = new ArrayList<>();
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
                    if ("success".equals(n.path("status").asText())) {
                        String key = n.path("key").asText();
                        if (key.isEmpty()) continue;
                        boolean folder = key.endsWith("/");
                        Map<String, Object> it = new LinkedHashMap<>();
                        it.put("name", folder ? key.substring(0, key.length() - 1) : key);
                        it.put("folder", folder);
                        it.put("size", n.path("size").asLong(0));
                        it.put("lastModified", n.path("lastModified").asText(""));
                        items.add(it);
                    } else {
                        if (err.length() < 500) err.append(n.path("error").path("message").asText("")).append(' ');
                    }
                }
            }
            int code;
            if (!p.waitFor(30, TimeUnit.SECONDS)) {
                p.destroyForcibly();
                code = -1;
            } else {
                code = p.exitValue();
            }
            Map<String, Object> out = new LinkedHashMap<>();
            out.put("bucket", bucket);
            out.put("prefix", fullPrefix);
            out.put("parent", parentOf(rel));
            out.put("items", items);
            out.put("error", code != 0 && items.isEmpty() ? err.toString().trim() : null);
            return out;
        } catch (Exception e) {
            throw new IllegalArgumentException("浏览失败: " + e.getMessage());
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

    /** 启动常驻 watch(platform 任务); 已启动返回 false; 首次启动即全量一轮, 之后事件实时记账 */
    public boolean startWatch(long taskId) {
        Map<String, Object> task = detail(taskId);
        if (task == null) throw new IllegalStateException("任务不存在: " + taskId);
        if (!"platform".equals(String.valueOf(task.get("exec_host")))) {
            throw new IllegalStateException("Agent 任务的实时同步由 Agent 本地执行, 平台无需启动");
        }
        if (watchers.containsKey(taskId)) return false;
        long runId = insertRun(taskId, "watch", "platform", String.valueOf(task.get("strategy_json")));
        metaJdbc.update("UPDATE file_sync_run SET status = 'WATCHING' WHERE id = ?", runId);
        metaJdbc.update("UPDATE file_sync_task SET run_status = 'WATCHING' WHERE id = ?", taskId);
        try {
            Map<String, String> src = sourceConf(((Number) task.get("source_datasource_id")).longValue());
            src.put("path", task.get("source_path") == null ? "" : String.valueOf(task.get("source_path")));
            Map<String, String> dst = targetConf(task);
            ensureAliases(src);
            prepareMirrorPaths(src, dst);
            Map<String, Object> st = parseJson(String.valueOf(task.get("strategy_json")));
            List<String> args = buildMirrorArgs(src, dst, st, true);
            Process p = new ProcessBuilder(args).redirectErrorStream(true).start();
            WatchSession session = new WatchSession(taskId, runId, p);
            watchers.put(taskId, session);
            Thread reader = new Thread(() -> watchLoop(session, src), "filesync-watch-" + taskId);
            reader.setDaemon(true);
            reader.start();
            log.info("实时同步启动 task={} run={}", taskId, runId);
            return true;
        } catch (Exception e) {
            metaJdbc.update("UPDATE file_sync_run SET status = 'FAILED', error_msg = ?, finished_at = NOW() WHERE id = ?",
                    truncate(e.getMessage(), 1900), runId);
            metaJdbc.update("UPDATE file_sync_task SET run_status = 'FAILED' WHERE id = ?", taskId);
            throw new IllegalStateException("watch 启动失败: " + e.getMessage(), e);
        }
    }

    /** 停止常驻 watch(进程销毁后由 watchLoop 收尾 run) */
    public boolean stopWatch(long taskId) {
        WatchSession s = watchers.get(taskId);
        if (s == null) return false;
        s.stopped.set(true);
        s.process.destroy();
        return true;
    }

    /** 事件循环: 逐行解析 mirror --json, 实时落 run_file; 进程退出后收尾 run */
    private void watchLoop(WatchSession session, Map<String, String> src) {
        long okN = 0, failN = 0, bytes = 0;
        String errorTail = null;
        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(session.process.getInputStream(), StandardCharsets.UTF_8))) {
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
                String status = n.path("status").asText();
                if ("success".equals(status)) {
                    String source = n.path("source").asText("");
                    if (!source.isEmpty()) {
                        long sz = n.path("size").asLong(0);
                        insertRunFile(session.runId, relOf(source, src), "success", sz, null);
                        okN++;
                        bytes += sz;
                    }
                } else if ("error".equals(status)) {
                    String msg = n.path("error").path("message").asText(n.path("error").asText());
                    insertRunFile(session.runId, "-", "error", 0, truncate(msg, 1900));
                    failN++;
                    errorTail = msg;
                }
            }
        } catch (Exception e) {
            errorTail = e.getMessage();
            failN = Math.max(failN, 1);
        }
        watchers.remove(session.taskId);
        int code = session.process.isAlive() ? -1 : session.process.exitValue();
        if (!session.stopped.get() && code != 0 && okN == 0) {
            failN = Math.max(failN, 1);
            if (errorTail == null) errorTail = "mc mirror 退出码 " + code;
        }
        String status = session.stopped.get() ? "COMPLETED" : (failN > 0 && okN == 0 ? "FAILED" : "COMPLETED");
        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("transferred", okN);
        summary.put("bytes", bytes);
        summary.put("failed", failN);
        summary.put("watch", true);
        log.info("实时同步结束 task={} status={} ok={} fail={}", session.taskId, status, okN, failN);
        finishRun(session.runId, status, summary, errorTail, session.taskId);
    }

    /** 任务是否处于平台 watch 常驻中 */
    public boolean watchRunning(long taskId) {
        return watchers.containsKey(taskId);
    }

    // ────────────────────────── 定时调度(platform 任务) ──────────────────────────

    @Scheduled(fixedDelay = 15000)
    public void autoTick() {
        try {
            List<Map<String, Object>> rows = metaJdbc.queryForList(
                    "SELECT id, exec_host, schedule_mode, interval_min, cron_expr, last_run_time FROM file_sync_task "
                            + "WHERE schedule_mode IN ('interval','cron')");
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
        payload.put("watch", "1".equals(String.valueOf(task.get("watch"))) || Boolean.TRUE.equals(task.get("watch")));
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
        // 目标(默认平台 MinIO 时下发每任务受限子账号凭证, 不下发 root)
        Map<String, String> dst = targetConf(task);
        boolean restricted = "agent".equals(String.valueOf(task.get("exec_host")))
                && (task.get("target_datasource_id") == null
                || ((Number) task.get("target_datasource_id")).longValue() == 0);
        if (restricted) {
            ensureRestrictedTarget(taskId, String.valueOf(task.get("target_bucket")),
                    task.get("target_prefix") == null ? "" : String.valueOf(task.get("target_prefix")));
            dst.put("accessKey", restrictedAk(taskId));
            dst.put("secretKey", deriveSecret(taskId));
        }
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

    // ───────────── 受限凭证: agent 任务 + 默认目标 MinIO → 每任务子账号(仅桶/前缀) ─────────────

    private static String restrictedAk(long taskId) {
        return "dcp-task-" + taskId;
    }

    private String deriveSecret(long taskId) {
        try {
            byte[] d = java.security.MessageDigest.getInstance("SHA-256")
                    .digest((encKey + "|filesync-task|" + taskId).getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder();
            for (byte b : d) sb.append(String.format("%02x", b));
            return sb.toString().substring(0, 24);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /** 幂等准备受限子账号: policy 仅授目标桶/前缀的写与受限列表; 重建 policy 以反映桶/前缀变更 */
    private void ensureRestrictedTarget(long taskId, String bucket, String prefix) {
        String ak = restrictedAk(taskId);
        String sk = deriveSecret(taskId);
        String p = strip(prefix);
        // s3:prefix 条件是相对桶的 key 前缀(mirror 列表请求 prefix=p/ 或 p); 空前缀=整桶
        String cond = p.isEmpty() ? ""
                : ",\"Condition\":{\"StringLike\":{\"s3:prefix\":[\"" + p + "\",\"" + p + "/*\"]}}";
        String objRes = "arn:aws:s3:::" + bucket + (p.isEmpty() ? "" : "/" + p) + "/*";
        String pol = "{\"Version\":\"2012-10-17\",\"Statement\":["
                + "{\"Effect\":\"Allow\",\"Action\":[\"s3:ListBucket\"],\"Resource\":[\"arn:aws:s3:::" + bucket + "\"]" + cond + "},"
                + "{\"Effect\":\"Allow\",\"Action\":[\"s3:PutObject\",\"s3:GetObject\",\"s3:DeleteObject\"],"
                + "\"Resource\":[\"" + objRes + "\"]}]}";
        try {
            Path polFile = Path.of("/tmp/dcp-pol-" + taskId + ".json");
            Files.writeString(polFile, pol, StandardCharsets.UTF_8);
            runMcCapture(List.of("alias", "set", "adm", dstEndpoint, dstAccessKey, dstSecretKey));
            if (runMcCapture(List.of("admin", "user", "info", "adm", ak)).code() != 0) {
                runMcCapture(List.of("admin", "user", "add", "adm", ak, sk));
            }
            runMcCapture(List.of("admin", "policy", "rm", "adm", "dcp-pol-" + taskId));
            runMcCapture(List.of("admin", "policy", "create", "adm", "dcp-pol-" + taskId, polFile.toString()));
            runMcCapture(List.of("admin", "policy", "attach", "adm", "dcp-pol-" + taskId, "--user", ak));
            log.info("受限凭证就绪 task={} ak={} → s3://{}/{}", taskId, ak, bucket, p);
        } catch (Exception e) {
            throw new IllegalStateException("目标受限凭证准备失败(默认 MinIO 需 admin 凭证): " + e.getMessage(), e);
        }
    }

    private record McOut(int code, String out) {
    }

    /** 执行 mc 返回退出码与输出(容忍失败, 供 admin 幂等判断) */
    private McOut runMcCapture(List<String> args) {
        try {
            List<String> cmd = new ArrayList<>();
            cmd.add("mc");
            cmd.addAll(args);
            Process p = new ProcessBuilder(cmd).redirectErrorStream(true).start();
            StringBuilder out = new StringBuilder();
            try (BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = br.readLine()) != null) {
                    if (out.length() < 3000) out.append(line).append('\n');
                }
            }
            if (!p.waitFor(60, TimeUnit.SECONDS)) {
                p.destroyForcibly();
                return new McOut(-1, out.toString());
            }
            return new McOut(p.exitValue(), out.toString());
        } catch (Exception e) {
            return new McOut(-1, e.getMessage());
        }
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
        prepareMirrorPaths(src, dst);
        try {
            Map<String, Long> srcMap = listSide(src, true);
            Map<String, Long> dstMap = listSide(dst, false);
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

    /** 列一侧清单: src=true(源, s3 经 alias / local 经 Files.walk) / false(目标, 恒 s3); 返回 相对路径→size */
    private Map<String, Long> listSide(Map<String, String> conf, boolean isSrc) throws Exception {
        Map<String, Long> out = new LinkedHashMap<>();
        boolean local = isSrc && "local".equals(conf.get("type"));
        String base;
        if (local) {
            base = conf.get("mirrorPath") == null ? "" : conf.get("mirrorPath");
            java.nio.file.Path root = java.nio.file.Paths.get(base);
            if (!java.nio.file.Files.exists(root)) throw new IllegalStateException("源目录不存在: " + base);
            try (var walk = java.nio.file.Files.walk(root)) {
                walk.filter(java.nio.file.Files::isRegularFile).forEach(p -> {
                    if (out.size() >= RECONCILE_MAX_ENTRIES) return;
                    try {
                        out.put(root.relativize(p).toString().replace('\\', '/'), java.nio.file.Files.size(p));
                    } catch (Exception ignored) {
                    }
                });
            }
            return out;
        }
        String alias = isSrc ? ALIAS_SRC : ALIAS_DST;
        if (!isSrc) {
            runMcQuiet(List.of("alias", "set", alias, conf.get("endpoint"), conf.get("accessKey"), conf.get("secretKey")));
        } else {
            runMcQuiet(List.of("alias", "set", alias, conf.get("endpoint"), conf.get("accessKey"), conf.get("secretKey")));
        }
        base = conf.get("mirrorPath");
        String prefixBase;
        if (isSrc) {
            String bucket = conf.get("bucket") == null ? "" : conf.get("bucket");
            String p = strip(conf.get("path"));
            prefixBase = bucket + (p.isEmpty() ? "" : "/" + p);
        } else {
            prefixBase = conf.get("bucket") + (strip(conf.get("prefix")).isEmpty() ? "" : "/" + strip(conf.get("prefix")));
        }
        Process p = new ProcessBuilder("mc", "ls", "--recursive", "--json", base)
                .redirectErrorStream(true).start();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (line.isBlank() || out.size() >= RECONCILE_MAX_ENTRIES) continue;
                JsonNode n;
                try {
                    n = OM.readTree(line);
                } catch (Exception e) {
                    continue;
                }
                if (!n.isObject() || !"success".equals(n.path("status").asText())) continue;
                String key = n.path("key").asText("");
                if (key.isEmpty() || key.endsWith("/")) continue;
                String rel = key.startsWith(prefixBase) ? key.substring(prefixBase.length()) : key;
                rel = strip(rel);
                if (rel.isEmpty()) continue;
                out.put(rel, n.path("size").asLong(0));
            }
        }
        if (!p.waitFor(120, TimeUnit.SECONDS)) {
            p.destroyForcibly();
            throw new IllegalStateException("mc ls 超时");
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

    /** 事件 source 剥离源根(mirrorPath), 得相对路径(用于 run_file.rel_path) */
    private static String relOf(String source, Map<String, String> src) {
        String r = strip(src.get("relRoot") == null ? "" : src.get("relRoot"));
        String s = strip(source);
        if (!r.isEmpty() && s.startsWith(r + "/")) {
            return s.substring(r.length() + 1);
        }
        return s;
    }
}
