package com.dcp.module.lakesync;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 湖同步平台调度(设计文档 2026-09-08 §4b):
 * lake_sync_task(每逻辑表一个任务, 游标 cursor_json 落任务行) + 15s tick 到期触发
 * → 组装 job-json(数据面 JDBC = SYNC_TARGET_URL 同源; 每次 run 前用 catalog 管理凭证换短时 token)
 * → runner(simulate 演练 | spark 真实提交) → exit0 读 cursor-result 回写游标。
 */
@Slf4j
@Component
public class LakeSyncService {

    private static final ObjectMapper OM = new ObjectMapper();

    private final JdbcTemplate metaJdbc;
    /** 湖增量底座任务登记桥(P3: createTask 登记 / deleteTask 停 / listTasks 指标探针) */
    private final LakeTaskDeployer lakeTaskDeployer;

    @Value("${dcp.lakesync.runner-mode:simulate}")
    private String runnerMode;              // simulate | spark
    @Value("${dcp.lakesync.executor-jar:}")
    private String executorJar;             // dcp-lakesync.jar 路径(spark 节点可见)
    @Value("${dcp.lakesync.runs-dir:/tmp/dcp-lake-sync}")
    private String runsDir;
    @Value("${dcp.lakesync.spark-submit:spark-submit}")
    private String sparkSubmitBin;
    @Value("${dcp.lakesync.catalog.credential:root:s3cr3t}")
    private String catalogCredential;       // REST catalog 管理凭证(clientId:secret, 每次 run 换 token)
    @Value("${dcp.lakesync.catalog.token-url:}")
    private String catalogTokenUrl;         // 如 http://dcp-polaris:8181/api/catalog/v1/oauth/tokens
    @Value("${dcp.lakesync.catalog.uri:}")
    private String catalogUri;              // 如 http://dcp-polaris:8181/api/catalog
    @Value("${dcp.lakesync.catalog.warehouse:demo}")
    private String catalogWarehouse;        // REST: catalog 名
    @Value("${dcp.lakesync.catalog.namespace:dcp}")
    private String catalogNamespace;
    @Value("${dcp.lakesync.catalog.s3.endpoint:http://dcp-minio:9000}")
    private String s3Endpoint;
    @Value("${dcp.lakesync.catalog.s3.access-key:minioadmin}")
    private String s3AccessKey;
    @Value("${dcp.lakesync.catalog.s3.secret-key:minioadmin123}")   // 与 compose dcp-minio/Polaris 一致; 生产用 env 覆盖
    private String s3SecretKey;
    @Value("${dcp.sync.target.url}")
    private String targetUrl;               // 数据面 JDBC(与 AuthorityMerger/TargetDbRegistry 同源)
    @Value("${dcp.sync.target.username:root}")
    private String targetUser;
    @Value("${dcp.sync.target.password:root123}")
    private String targetPassword;

    private final ExecutorService runs = Executors.newSingleThreadExecutor();

    public LakeSyncService(JdbcTemplate metaJdbc, LakeTaskDeployer lakeTaskDeployer) {
        this.metaJdbc = metaJdbc;
        this.lakeTaskDeployer = lakeTaskDeployer;
    }

    @PostConstruct
    public void ensureSchema() {
        metaJdbc.execute("CREATE TABLE IF NOT EXISTS lake_sync_task ("
                + "id BIGINT AUTO_INCREMENT PRIMARY KEY,"
                + "logical_table VARCHAR(128) NOT NULL UNIQUE,"
                + "enabled TINYINT NOT NULL DEFAULT 1,"
                + "schedule VARCHAR(16) NOT NULL DEFAULT 'interval' COMMENT 'interval|cron',"
                + "interval_min INT NOT NULL DEFAULT 15,"
                + "cron_expr VARCHAR(64),"
                + "cursor_json TEXT NULL COMMENT '每源 seq 列游标 {col:max}; NULL/空=待全量首载',"
                + "status VARCHAR(16) NOT NULL DEFAULT 'running' COMMENT 'running(调度中)|manual(仅手动)',"
                + "last_run_at DATETIME NULL,"
                + "last_auto_run_at DATETIME NULL,"
                + "create_time DATETIME DEFAULT CURRENT_TIMESTAMP,"
                + "update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)");
        metaJdbc.execute("CREATE TABLE IF NOT EXISTS lake_sync_run ("
                + "id BIGINT AUTO_INCREMENT PRIMARY KEY,"
                + "task_id BIGINT NOT NULL,"
                + "logical_table VARCHAR(128),"
                + "trigger_type VARCHAR(16) NOT NULL DEFAULT 'manual' COMMENT 'manual|cron',"
                + "status VARCHAR(16) NOT NULL DEFAULT 'submitted' COMMENT 'submitted|running|succeeded|failed',"
                + "payload_path VARCHAR(512),"
                + "submit_cmd VARCHAR(1024),"
                + "exit_code INT NULL,"
                + "cursor_before TEXT NULL, cursor_after TEXT NULL,"
                + "error_msg VARCHAR(2048),"
                + "started_at DATETIME DEFAULT CURRENT_TIMESTAMP,"
                + "finished_at DATETIME NULL,"
                + "KEY idx_task (task_id))");
        log.info("[lakesync] schema ensured: lake_sync_task / lake_sync_run");
    }

    /* ═════════ 任务 CRUD ═════════ */

    public List<Map<String, Object>> listTasks() {
        List<Map<String, Object>> out = metaJdbc.queryForList(
                "SELECT id, logical_table, enabled, schedule, interval_min, cron_expr, cursor_json, status, last_run_at, last_auto_run_at"
                        + " FROM lake_sync_task ORDER BY id");
        out.forEach(r -> {
            try {
                r.put("cursor", r.get("cursor_json") == null ? new LinkedHashMap<String, Object>()
                        : OM.readTree((String) r.get("cursor_json")));
            } catch (Exception e) {
                r.put("cursor", new LinkedHashMap<String, Object>());
            }
            r.remove("cursor_json");
            // 湖增量底座任务指标探针(批任务列表附带增量侧状态; 控制面不可达 → UNKNOWN/-1)
            String lt = String.valueOf(r.get("logical_table"));
            r.put("incrementalStatus", lakeTaskDeployer.statusOf(lt));
            r.put("incrementalLag", lakeTaskDeployer.lagOf(lt));
        });
        return out;
    }

    public Map<String, Object> getTask(long id) {
        List<Map<String, Object>> rows = metaJdbc.queryForList(
                "SELECT id, logical_table, enabled, schedule, interval_min, cron_expr, cursor_json, status, last_run_at, last_auto_run_at"
                        + " FROM lake_sync_task WHERE id=?", id);
        if (rows.isEmpty()) throw new IllegalArgumentException("任务不存在 id=" + id);
        Map<String, Object> r = rows.get(0);
        try {
            r.put("cursor", r.get("cursor_json") == null ? new LinkedHashMap<String, Object>()
                    : OM.readTree((String) r.get("cursor_json")));
        } catch (Exception e) {
            r.put("cursor", new LinkedHashMap<String, Object>());
        }
        r.remove("cursor_json");
        return r;
    }

    public Map<String, Object> createTask(Map<String, Object> body) {
        String logicalTable = String.valueOf(body.get("logicalTable")).trim();
        if (logicalTable.isEmpty()) throw new IllegalArgumentException("logicalTable 必填");
        Integer dup = metaJdbc.queryForObject("SELECT COUNT(*) FROM lake_sync_task WHERE logical_table=?", Integer.class, logicalTable);
        if (dup != null && dup > 0) throw new IllegalArgumentException("该逻辑表已有湖同步任务: " + logicalTable);
        String schedule = String.valueOf(body.getOrDefault("schedule", "interval"));
        int intervalMin = body.get("intervalMin") instanceof Number n ? n.intValue() : 15;
        String cron = String.valueOf(body.getOrDefault("cronExpr", ""));
        if ("cron".equals(schedule) && cron.isEmpty()) throw new IllegalArgumentException("cron 任务需 cronExpr");
        KeyHolder kh = new GeneratedKeyHolder();
        metaJdbc.update(con -> {
            PreparedStatement ps = con.prepareStatement(
                    "INSERT INTO lake_sync_task(logical_table, schedule, interval_min, cron_expr, status) VALUES(?,?,?,?,?)",
                    Statement.RETURN_GENERATED_KEYS);
            ps.setString(1, logicalTable);
            ps.setString(2, schedule);
            ps.setInt(3, intervalMin);
            ps.setString(4, "cron".equals(schedule) ? cron : null);
            ps.setString(5, String.valueOf(body.getOrDefault("status", "running")));
            return ps;
        }, kh);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("id", kh.getKey());
        out.put("logicalTable", logicalTable);
        // 湖增量底座任务登记(只登记不激活; 机会型动作失败不阻断批任务创建)
        try {
            Map<String, Object> d = lakeTaskDeployer.deploy(logicalTable, readKeys(logicalTable), 3);
            out.put("incrementalTask", d);
        } catch (Exception e) {
            log.warn("[lakesync] 湖增量底座任务登记失败 {}: {}", logicalTable, e.getMessage());
        }
        return out;
    }

    public Map<String, Object> updateTask(long id, Map<String, Object> body) {
        getTask(id);
        String schedule = String.valueOf(body.getOrDefault("schedule", "interval"));
        int intervalMin = body.get("intervalMin") instanceof Number n ? n.intValue() : 15;
        String cron = String.valueOf(body.getOrDefault("cronExpr", ""));
        if ("cron".equals(schedule) && cron.isEmpty()) throw new IllegalArgumentException("cron 任务需 cronExpr");
        Integer enabled = body.get("enabled") instanceof Number n ? n.intValue() : 1;
        String cursorJson = body.get("cursor") == null ? null : jstr(body.get("cursor"));
        metaJdbc.update("UPDATE lake_sync_task SET schedule=?, interval_min=?, cron_expr=?, enabled=?, cursor_json=?, status=? WHERE id=?",
                schedule, intervalMin, "cron".equals(schedule) ? cron : null, enabled,
                cursorJson, String.valueOf(body.getOrDefault("status", "running")), id);
        return Map.of("id", id);
    }

    public void deleteTask(long id) {
        Map<String, Object> task = getTask(id);
        String logicalTable = String.valueOf(task.get("logical_table"));
        metaJdbc.update("DELETE FROM lake_sync_task WHERE id=?", id);
        // 湖增量底座任务停(保留登记, 部署方案阶段可复活)
        try {
            lakeTaskDeployer.stop(logicalTable);
        } catch (Exception e) {
            log.warn("[lakesync] 湖增量底座任务停失败 {}: {}", logicalTable, e.getMessage());
        }
    }

    /* ═════════ 触发与执行 ═════════ */

    /** 手动/定时触发: 组装 job-json → 落盘 + lake_sync_run → runner(simulate|spark 异步) */
    public Map<String, Object> startRun(long taskId, String trigger) {
        Map<String, Object> task = getTask(taskId);
        if (((Number) task.get("enabled")).intValue() != 1 && "cron".equals(trigger)) {
            throw new IllegalArgumentException("任务已停用");
        }
        try {
            ObjectNode payload = buildJobJson(task);
            Path dir = Paths.get(runsDir);
            Files.createDirectories(dir);
            KeyHolder kh = new GeneratedKeyHolder();
            metaJdbc.update(con -> {
                PreparedStatement ps = con.prepareStatement(
                        "INSERT INTO lake_sync_run(task_id, logical_table, trigger_type, status, cursor_before) VALUES(?,?,?,'submitted',?)",
                        Statement.RETURN_GENERATED_KEYS);
                ps.setLong(1, taskId);
                ps.setString(2, (String) task.get("logical_table"));
                ps.setString(3, trigger);
                ps.setString(4, jstr(task.get("cursor")));
                return ps;
            }, kh);
            long runId = ((Number) kh.getKey()).longValue();
            Path file = dir.resolve("lake-" + taskId + "-run-" + runId + ".json");
            Files.writeString(file, OM.writerWithDefaultPrettyPrinter().writeValueAsString(payload), StandardCharsets.UTF_8);

            String submitCmd;
            if ("spark".equalsIgnoreCase(runnerMode) && !executorJar.isBlank()) {
                submitCmd = sparkSubmitBin + " --class com.dcp.lakesync.LakeSyncJob " + executorJar
                        + " --job-json " + file + " --cursor-out " + file.getParent() + "/lake-" + taskId + "-run-" + runId + ".cursor.json";
                log.info("[lakesync] run#{} spark 提交: {}", runId, submitCmd);
                metaJdbc.update("UPDATE lake_sync_run SET status='submitted', payload_path=?, submit_cmd=? WHERE id=?",
                        file.toString(), submitCmd, runId);
                runs.submit(() -> executeSpark(runId, file, submitCmd));
            } else {
                submitCmd = "演练(simulate): spark-submit --class com.dcp.lakesync.LakeSyncJob " + executorJar
                        + " --job-json " + file + " | runner-mode=" + runnerMode;
                metaJdbc.update("UPDATE lake_sync_run SET status='submitted', payload_path=?, submit_cmd=? WHERE id=?",
                        file.toString(), submitCmd, runId);
                log.info("[lakesync] run#{} 演练: job-json 已生成 {}", runId, file);
            }
            Map<String, Object> out = new LinkedHashMap<>();
            out.put("runId", runId);
            out.put("taskId", taskId);
            out.put("status", "submitted");
            out.put("payloadPath", file.toString());
            out.put("submitCmd", submitCmd);
            return out;
        } catch (IOException e) {
            throw new IllegalArgumentException("runs 目录写入失败: " + e.getMessage());
        }
    }

    /** spark 模式: 后台等作业退出, exit0 → 读 cursor-result 回写游标 */
    private void executeSpark(long runId, Path file, String cmd) {
        metaJdbc.update("UPDATE lake_sync_run SET status='running' WHERE id=?", runId);
        try {
            List<Map<String, Object>> rows = metaJdbc.queryForList("SELECT task_id FROM lake_sync_run WHERE id=?", runId);
            long taskId = rows.isEmpty() ? -1 : ((Number) rows.get(0).get("task_id")).longValue();
            Process p = new ProcessBuilder(cmd.split(" ")).redirectErrorStream(true).start();
            p.waitFor();
            int exit = p.exitValue();
            String error = null;
            String cursorAfter = null;
            if (exit == 0) {
                Path cursorFile = Paths.get(runsDir).resolve("lake-" + taskId + "-run-" + runId + ".cursor.json");
                if (Files.exists(cursorFile)) {
                    cursorAfter = Files.readString(cursorFile, StandardCharsets.UTF_8);
                    metaJdbc.update("UPDATE lake_sync_task SET cursor_json=?, last_run_at=NOW() WHERE id=?", cursorAfter, taskId);
                }
            } else {
                error = "作业退出码 " + exit + "(游标未推进, 下轮重试)";
            }
            metaJdbc.update("UPDATE lake_sync_run SET status=?, exit_code=?, cursor_after=?, error_msg=?, finished_at=NOW() WHERE id=?",
                    exit == 0 ? "succeeded" : "failed", exit, cursorAfter, error, runId);
            log.info("[lakesync] run#{} 结束 exit={} cursor_after={}", runId, exit, cursorAfter);
        } catch (Exception e) {
            metaJdbc.update("UPDATE lake_sync_run SET status='failed', error_msg=?, finished_at=NOW() WHERE id=?",
                    "提交器异常: " + e.getMessage(), runId);
            log.error("[lakesync] run#{} 提交器异常", runId, e);
        }
    }

    /** 组装 job-json: 数据面 JDBC(SYNC_TARGET_URL 同源)+ 探测单表/seq 列 + 湖配置 + 每次 run 换 token */
    private ObjectNode buildJobJson(Map<String, Object> task) throws IOException {
        String logicalTable = (String) task.get("logical_table");
        Map<String, Object> meta = metaJdbc.query("SELECT table_prefix, business_key, key_columns FROM logical_table WHERE table_name=?",
                rs -> {
                    if (!rs.next()) return null;
                    Map<String, Object> m = new LinkedHashMap<>();
                    m.put("prefix", rs.getString("table_prefix"));
                    m.put("businessKey", rs.getString("business_key"));
                    m.put("keyColumns", rs.getString("key_columns"));
                    return m;
                }, logicalTable);
        String prefix = meta == null ? logicalTable : (String) meta.get("prefix");
        // 目标表探测: 单表 > authority(仅演示回退; 生产=TiDB 单表)
        String dataTable = probeDataTable(prefix);
        // seq 列探测(单表内 _seq% 列)
        List<String> seqCols = probeSeqColumns(dataTable);
        // 业务键(entityKeyColumns): business_key(keyFields/scopeFields) > key_columns > 表内 PK 探测
        List<String> keys = parseKeys(meta);
        if (keys.isEmpty()) {
            keys = probePk(dataTable);
        }
        if (keys.isEmpty()) throw new IllegalArgumentException("逻辑表 " + logicalTable + " 业务键探测失败");

        JsonNode cursorNode = task.get("cursor") == null ? OM.createObjectNode() : OM.valueToTree(task.get("cursor"));
        ArrayNode seqArr = OM.createArrayNode();
        for (String col : seqCols) {
            ObjectNode sc = seqArr.addObject();
            sc.put("name", col);
            sc.put("cursor", cursorNode != null && cursorNode.has(col) ? cursorNode.get(col).asLong(-1) : -1);
        }
        ObjectNode out = OM.createObjectNode();
        out.put("logicalTable", logicalTable);
        ArrayNode keysArr = out.putArray("entityKeyColumns");
        keys.forEach(keysArr::add);
        ObjectNode ds = out.putObject("dataSource");
        ds.put("url", targetUrl);
        ds.put("user", targetUser);
        ds.put("password", targetPassword);
        ds.put("table", dataTable);
        out.set("seqColumns", seqArr);
        ObjectNode tomb = out.putObject("tombstone");
        tomb.put("enabled", false);
        tomb.put("column", "_is_deleted");
        ObjectNode lake = out.putObject("lake");
        lake.put("catalog", catalogWarehouse.isBlank() ? "demo" : catalogWarehouse); // warehouse=REST catalog 名
        lake.put("namespace", catalogNamespace);
        lake.put("table", logicalTable);
        lake.put("type", "rest");
        lake.put("uri", catalogUri);
        lake.put("warehouse", catalogWarehouse);
        String token = fetchToken();
        if (token != null) lake.put("token", token);
        lake.put("ioImpl", "org.apache.iceberg.aws.s3.S3FileIO");
        ObjectNode s3 = lake.putObject("s3");
        s3.put("endpoint", s3Endpoint);
        s3.put("pathStyleAccess", "true");
        s3.put("accessKey", s3AccessKey);
        s3.put("secretKey", s3SecretKey);
        s3.put("region", "us-east-1");
        return out;
    }

    /** 每次 run 前用 catalog 管理凭证换短时 token(dev Polaris; 生产同构 REST oauth) */
    private String fetchToken() {
        if (catalogTokenUrl == null || catalogTokenUrl.isBlank()) return null;
        String[] c = catalogCredential.split(":", 2);
        try {
            HttpURLConnection conn = (HttpURLConnection) URI.create(catalogTokenUrl).toURL().openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
            String body = "grant_type=client_credentials&client_id=" + enc(c[0]) + "&client_secret=" + enc(c[1])
                    + "&scope=" + enc("PRINCIPAL_ROLE:ALL");
            conn.setDoOutput(true);
            try (OutputStream os = conn.getOutputStream()) {
                os.write(body.getBytes(StandardCharsets.UTF_8));
            }
            if (conn.getResponseCode() != 200) {
                log.warn("[lakesync] catalog token 换取失败 http={}", conn.getResponseCode());
                return null;
            }
            JsonNode n = OM.readTree(conn.getInputStream());
            return n.path("access_token").asText(null);
        } catch (Exception e) {
            log.warn("[lakesync] catalog token 换取异常: {}", e.getMessage());
            return null;
        }
    }

    private static String enc(String v) throws IOException {
        return URLEncoder.encode(v, StandardCharsets.UTF_8);
    }

    /* ═════════ 探测(数据面 information_schema) ═════════ */

    private org.springframework.jdbc.core.JdbcTemplate targetJdbc() {
        org.springframework.jdbc.datasource.DriverManagerDataSource ds = new org.springframework.jdbc.datasource.DriverManagerDataSource();
        ds.setUrl(targetUrl);
        ds.setUsername(targetUser);
        ds.setPassword(targetPassword);
        return new org.springframework.jdbc.core.JdbcTemplate(ds);
    }

    private List<String> targetColumns(String table) {
        List<String> out = new ArrayList<>();
        if (table == null) return out;
        try {
            List<Map<String, Object>> rows = targetJdbc().queryForList(
                    "SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=? ORDER BY ORDINAL_POSITION",
                    table);
            rows.forEach(r -> out.add(String.valueOf(r.get("COLUMN_NAME"))));
        } catch (Exception ignore) {
            /* 表不存在 */
        }
        return out;
    }

    private List<String> probeSeqColumns(String table) {
        return targetColumns(table).stream().filter(c -> c.startsWith("_seq")).toList();
    }

    private List<String> probePk(String table) {
        List<String> out = new ArrayList<>();
        if (table == null) return out;
        try {
            List<Map<String, Object>> rows = targetJdbc().queryForList(
                    "SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=? AND COLUMN_KEY='PRI' ORDER BY ORDINAL_POSITION",
                    table);
            rows.forEach(r -> out.add(String.valueOf(r.get("COLUMN_NAME"))));
        } catch (Exception ignore) {
        }
        return out;
    }

    private String probeDataTable(String prefix) {
        org.springframework.jdbc.core.JdbcTemplate tj = targetJdbc();
        for (String t : List.of(prefix, prefix + "_authority")) {
            try {
                Integer n = tj.queryForObject(
                        "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=?",
                        Integer.class, t);
                if (n != null && n > 0) return t;
            } catch (Exception ignore) {
            }
        }
        return prefix;
    }

    /** 湖增量登记用业务键(logical_table.business_key/key_columns; 与批作业 parseKeys 同款探测) */
    private List<String> readKeys(String logicalTable) {
        Map<String, Object> meta = metaJdbc.query("SELECT business_key, key_columns FROM logical_table WHERE table_name=?",
                rs -> {
                    if (!rs.next()) return null;
                    Map<String, Object> m = new LinkedHashMap<>();
                    m.put("businessKey", rs.getString("business_key"));
                    m.put("keyColumns", rs.getString("key_columns"));
                    return m;
                }, logicalTable);
        return parseKeys(meta);
    }

    private List<String> parseKeys(Map<String, Object> meta) {
        List<String> out = new ArrayList<>();
        if (meta == null) return out;
        for (String raw : List.of((String) meta.get("businessKey"), (String) meta.get("keyColumns"))) {
            if (raw == null || raw.isBlank()) continue;
            try {
                JsonNode n = OM.readTree(raw);
                if (n.isArray()) n.forEach(x -> addIfAbsent(out, x.asText()));
                else {
                    for (String f : List.of("keyFields", "scopeFields")) {
                        if (n.has(f) && n.get(f).isArray()) n.get(f).forEach(x -> addIfAbsent(out, x.asText()));
                    }
                }
            } catch (Exception ignore) {
            }
        }
        return out;
    }

    private static void addIfAbsent(List<String> l, String v) {
        if (v != null && !v.isBlank() && !l.contains(v)) l.add(v);
    }

    private static String jstr(Object o) {
        try {
            return OM.writeValueAsString(o == null ? "{}" : o);
        } catch (Exception e) {
            return "{}";
        }
    }

    /* ═════════ run 审计查询 ═════════ */

    public List<Map<String, Object>> listRuns(Long taskId) {
        String sql = "SELECT id, task_id, logical_table, trigger_type, status, payload_path, submit_cmd, exit_code,"
                + " cursor_before, cursor_after, error_msg, started_at, finished_at FROM lake_sync_run";
        List<Object> args = new ArrayList<>();
        if (taskId != null) {
            sql += " WHERE task_id=?";
            args.add(taskId);
        }
        sql += " ORDER BY id DESC LIMIT 100";
        List<Map<String, Object>> rows = metaJdbc.queryForList(sql, args.toArray());
        List<Map<String, Object>> out = new ArrayList<>();
        for (Map<String, Object> r : rows) {
            Map<String, Object> m = new LinkedHashMap<>(r);
            for (String k : List.of("cursor_before", "cursor_after")) {
                try {
                    m.put(k + "Parsed", m.get(k) == null ? null : OM.readTree((String) m.get(k)));
                } catch (Exception e) {
                    m.put(k + "Parsed", null);
                }
                m.remove(k);
            }
            out.add(m);
        }
        return out;
    }

    public Map<String, Object> getRun(long runId) {
        List<Map<String, Object>> rows = metaJdbc.queryForList(
                "SELECT id, task_id, logical_table, trigger_type, status, payload_path, submit_cmd, exit_code,"
                        + " cursor_before, cursor_after, error_msg, started_at, finished_at FROM lake_sync_run WHERE id=?",
                runId);
        if (rows.isEmpty()) throw new IllegalArgumentException("run 不存在 id=" + runId);
        Map<String, Object> m = new LinkedHashMap<>(rows.get(0));
        for (String k : List.of("cursor_before", "cursor_after")) {
            try {
                m.put(k + "Parsed", m.get(k) == null ? null : OM.readTree((String) m.get(k)));
            } catch (Exception e) {
                m.put(k + "Parsed", null);
            }
            m.remove(k);
        }
        String path = (String) m.get("payload_path");
        if (path != null) {
            try {
                String txt = Files.readString(Paths.get(path), StandardCharsets.UTF_8);
                if (txt.length() > 200000) txt = txt.substring(0, 200000) + "…(截断)";
                m.put("payloadText", txt);
            } catch (IOException ignore) {
                m.put("payloadText", null);
            }
        }
        return m;
    }

    /* ═════════ 15s tick: 到期触发(interval/cron; last_auto_run_at 防热环; 停用/手动任务跳过) ═════════ */

    /**
     * 底座任务双向对账(ShedLock 域级锁 lakeTaskReconcile 多实例互斥; 判据=目标态差异, 判据即重试队列):
     * CP0 创建方向:业务任务在 ∧ 底座缺(宽限期 180s 过滤=判活) → 补 deploy(五步幂等登记);
     * CP4 删除方向:业务已删 ∧ 底座在且未停 → 补 stop(desired_state 收敛后判据自然不再命中)。
     */
    @Scheduled(fixedDelay = 15000)
    @SchedulerLock(name = "lakeTaskReconcile", lockAtMostFor = "2m", lockAtLeastFor = "5s")
    public void reconcileBaseTasks() {
        // CP0 创建方向(扫描只读; 宽限期在 SQL 内, 排除“正常登记中”的误判)
        List<Map<String, Object>> missing;
        try {
            missing = metaJdbc.queryForList(
                    "SELECT logical_table FROM lake_sync_task t "
                            + "WHERE t.enabled = 1 AND t.create_time < NOW() - INTERVAL 180 SECOND "
                            + "AND NOT EXISTS (SELECT 1 FROM consumer_task c "
                            + "    WHERE c.task_id = CONCAT('lake-inc-', t.logical_table))");
        } catch (Exception e) {
            log.warn("[lakesync] 底座对账扫描失败(CP0, 下轮重试): {}", e.getMessage());
            return;
        }
        for (Map<String, Object> row : missing) {
            String lt = String.valueOf(row.get("logical_table"));
            try {
                lakeTaskDeployer.deploy(lt, readKeys(lt), 3);
                log.info("[lakesync] 底座登记补发完成(CP0): logical_table={}", lt);
            } catch (Exception e) {
                log.warn("[lakesync] 底座登记补发失败(CP0, 下轮重试) {}: {}", lt, e.getMessage());
            }
        }
        // CP4 删除方向(判据带 desired_state<>stopped: stop 成功后判据收敛, 不重复补发)
        List<Map<String, Object>> orphan;
        try {
            orphan = metaJdbc.queryForList(
                    "SELECT c.task_id FROM consumer_task c "
                            + "WHERE c.namespace = 'lake' AND c.desired_state <> 'stopped' "
                            + "AND NOT EXISTS (SELECT 1 FROM lake_sync_task t "
                            + "    WHERE CONCAT('lake-inc-', t.logical_table) = c.task_id)");
        } catch (Exception e) {
            log.warn("[lakesync] 底座对账扫描失败(CP4, 下轮重试): {}", e.getMessage());
            return;
        }
        for (Map<String, Object> row : orphan) {
            String taskId = String.valueOf(row.get("task_id"));
            String lt = taskId.startsWith("lake-inc-") ? taskId.substring("lake-inc-".length()) : taskId;
            try {
                lakeTaskDeployer.stop(lt);
                log.info("[lakesync] 底座残留注销补发完成(CP4): task_id={}", taskId);
            } catch (Exception e) {
                log.warn("[lakesync] 底座残留注销补发失败(CP4, 下轮重试) {}: {}", taskId, e.getMessage());
            }
        }
    }

    @Scheduled(fixedDelay = 15000)
    @SchedulerLock(name = "lakeAutoTick", lockAtMostFor = "2m", lockAtLeastFor = "5s")
    public void autoTick() {
        List<Map<String, Object>> tasks = metaJdbc.queryForList(
                "SELECT id, enabled, schedule, interval_min, cron_expr, last_auto_run_at, status FROM lake_sync_task");
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime minuteStart = now.withSecond(0).withNano(0);
        for (Map<String, Object> t : tasks) {
            long id = ((Number) t.get("id")).longValue();
            if (((Number) t.get("enabled")).intValue() != 1) continue;
            if (!"running".equals(t.get("status"))) continue;
            // MySQL 驱动 DATETIME 列经 JdbcTemplate 返回 LocalDateTime(非 Timestamp); 兼容两种
            java.sql.Timestamp last;
            Object lastRaw = t.get("last_auto_run_at");
            if (lastRaw instanceof java.sql.Timestamp ts) last = ts;
            else if (lastRaw instanceof LocalDateTime ldt) last = java.sql.Timestamp.valueOf(ldt);
            else last = null;
            boolean due;
            if ("cron".equals(t.get("schedule"))) {
                due = cronMatch(String.valueOf(t.get("cron_expr")), now) && (last == null || last.toLocalDateTime().isBefore(minuteStart));
            } else {
                int min = ((Number) t.get("interval_min")).intValue();
                due = last == null || last.toLocalDateTime().plusMinutes(min).isBefore(now);
            }
            if (!due) continue;
            metaJdbc.update("UPDATE lake_sync_task SET last_auto_run_at=NOW() WHERE id=?", id);   // 防热环: 到期即记尝试
            try {
                startRun(id, "cron");
            } catch (Exception e) {
                log.warn("[lakesync] 任务#{} 自动触发失败: {}", id, e.getMessage());
            }
        }
    }

    /**
     * 湖批孤儿 run 收尾(同文件同步 reconcileOrphanRuns 范式): spark 模式后台线程随进程消亡
     * (backend 重启/spark-submit 失联)后 run 行永停 running → 超 30min 标记 failed 收尾审计。
     * 只收 running(不碰 simulate 演练态的 submitted 行); 实际作业若仍在跑, 审计态标记不影响其完成。
     */
    @Scheduled(fixedDelay = 15000)
    @SchedulerLock(name = "lakeOrphanRuns", lockAtMostFor = "2m", lockAtLeastFor = "5s")
    public void reconcileOrphanRuns() {
        try {
            int n = metaJdbc.update(
                    "UPDATE lake_sync_run SET status = 'failed', "
                            + "error_msg = IFNULL(error_msg, '孤儿 run 收尾: 超 30min 未完成(backend 重启或 spark 进程失联)'), "
                            + "finished_at = NOW() "
                            + "WHERE status = 'running' AND started_at < NOW() - INTERVAL 30 MINUTE");
            if (n > 0) log.warn("[lakesync] 孤儿 run 收尾: {} 条标记 failed", n);
        } catch (Exception e) {
            log.warn("[lakesync] 孤儿 run 收尾失败(下轮重试): {}", e.getMessage());
        }
    }

    private boolean cronMatch(String expr, LocalDateTime now) {
        try {
            String[] f = expr.trim().split("\\s+");
            if (f.length < 6) return false;
            if (!"*".equals(f[1]) && Integer.parseInt(f[1]) != now.getMinute()) return false;
            if (!"*".equals(f[2]) && Integer.parseInt(f[2]) != now.getHour()) return false;
            String dow = f[5];
            if (!"*".equals(dow)) {
                int cur = now.getDayOfWeek().getValue();
                if (!List.of(String.valueOf(cur), String.valueOf(cur % 7)).contains(dow)) return false;
            }
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}
