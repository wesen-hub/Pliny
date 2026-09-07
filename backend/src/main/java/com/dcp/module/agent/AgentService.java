package com.dcp.module.agent;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.*;

/**
 * 采集 Agent(dcp-agent)注册表与指令通道 —— 文件同步 Agent 架构(P0, 设计定稿 2026-09-07):
 *
 * <pre>
 *   控制面在平台, 数据面在 Agent:
 *   - Agent = 数据源(datasource_config type=local 绑定 agentId), 任务创建时再选目录
 *   - 平台建任务 -> 入 agent_command 指令队列(task_upsert/task_stop/task_remove/run_now)
 *   - Agent 长轮询拉指令, 本地调度执行(mc 直推目标 MinIO), 结果经 /runs 上报, runId 幂等
 *   - 平台不可达时 Agent 照常按最后下发策略执行, 重连补报(对账走 expectedTasks)
 * </pre>
 */
@Slf4j
@Component
public class AgentService {

    private static final ObjectMapper OM = new ObjectMapper();
    private static final SecureRandom RND = new SecureRandom();
    /** 心跳 30s, 超过 90s 无心跳视为离线 */
    public static final long OFFLINE_SECONDS = 90;

    private final JdbcTemplate metaJdbc;

    public AgentService(JdbcTemplate metaJdbc) {
        this.metaJdbc = metaJdbc;
    }

    @PostConstruct
    public void ensureSchema() {
        metaJdbc.execute("CREATE TABLE IF NOT EXISTS dcp_agent ("
                + "id BIGINT AUTO_INCREMENT PRIMARY KEY,"
                + "name VARCHAR(128) NOT NULL, host VARCHAR(255),"
                + "os VARCHAR(32), arch VARCHAR(16), agent_version VARCHAR(32),"
                + "status VARCHAR(16) DEFAULT 'pending' COMMENT 'pending|online|offline',"
                + "register_code_hash VARCHAR(64), secret_hash VARCHAR(64),"
                + "last_heartbeat DATETIME,"
                + "create_time DATETIME DEFAULT CURRENT_TIMESTAMP,"
                + "update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)");
        metaJdbc.execute("CREATE TABLE IF NOT EXISTS agent_command ("
                + "id BIGINT AUTO_INCREMENT PRIMARY KEY,"
                + "agent_id BIGINT NOT NULL, task_id BIGINT NULL,"
                + "cmd VARCHAR(32) NOT NULL COMMENT 'task_upsert|task_stop|task_remove|run_now',"
                + "payload JSON NULL,"
                + "status VARCHAR(16) DEFAULT 'pending' COMMENT 'pending|acked',"
                + "ack_at DATETIME NULL, create_time DATETIME DEFAULT CURRENT_TIMESTAMP,"
                + "KEY idx_agent_status (agent_id, status))");
        metaJdbc.execute("CREATE TABLE IF NOT EXISTS agent_run ("
                + "id BIGINT AUTO_INCREMENT PRIMARY KEY,"
                + "agent_id BIGINT NOT NULL, task_id BIGINT NOT NULL, run_id VARCHAR(64) NOT NULL,"
                + "run_pk BIGINT NULL COMMENT '平台 file_sync_run 主键(watch 会话追加上报用)',"
                + "create_time DATETIME DEFAULT CURRENT_TIMESTAMP,"
                + "UNIQUE KEY uk_agent_run (agent_id, run_id))");
        try {
            metaJdbc.execute("ALTER TABLE agent_run ADD COLUMN run_pk BIGINT NULL");
        } catch (Exception ignored) {
        }
        try {
            metaJdbc.execute("ALTER TABLE agent_run ADD COLUMN last_seq BIGINT DEFAULT 0");
        } catch (Exception ignored) {
        }
        // file_copy_task 增加 agent 执行字段(v3: exec_host platform|agent)
        for (String alter : new String[]{
                "ALTER TABLE file_copy_task ADD COLUMN exec_host VARCHAR(16) DEFAULT 'platform'",
                "ALTER TABLE file_copy_task ADD COLUMN agent_id BIGINT NULL",
                "ALTER TABLE file_copy_task ADD COLUMN task_version INT DEFAULT 1",
                "ALTER TABLE file_copy_task ADD COLUMN filter_json VARCHAR(2048)"}) {
            try {
                metaJdbc.execute(alter);
            } catch (Exception ignored) {
            }
        }
        log.info("Agent schema ensured (dcp_agent/agent_command/agent_run)");
    }

    // ────────────────────────── 平台管理端 ──────────────────────────

    /** 创建 Agent 记录并生成一次性注册码(注册码只在此响应中出现一次) */
    public Map<String, Object> createAgent(String name, String host, String description) {
        String code = randomRegisterCode();
        String hash = sha256Hex(code);
        metaJdbc.update("INSERT INTO dcp_agent (name, host, status, register_code_hash) VALUES (?, ?, 'pending', ?)",
                name, host, hash);
        long id = metaJdbc.queryForObject("SELECT LAST_INSERT_ID()", Long.class);
        return Map.of("id", id, "name", name, "status", "pending", "registerCode", code);
    }

    /** Agent 列表: 附带在线状态(按心跳新鲜度)与绑定任务数 */
    public List<Map<String, Object>> listAgents() {
        List<Map<String, Object>> rows = metaJdbc.queryForList(
                "SELECT a.id, a.name, a.host, a.os, a.arch, a.agent_version, a.status, a.last_heartbeat, "
                        + "a.create_time, TIMESTAMPDIFF(SECOND, a.last_heartbeat, NOW()) AS age_sec, "
                        + "(SELECT COUNT(*) FROM file_sync_task t WHERE t.agent_id = a.id AND t.exec_host = 'agent') AS task_cnt "
                        + "FROM dcp_agent a ORDER BY a.id");
        for (Map<String, Object> r : rows) {
            String st = String.valueOf(r.get("status"));
            Object age = r.get("age_sec");
            boolean fresh = age != null && ((Number) age).longValue() <= OFFLINE_SECONDS;
            r.put("online", "online".equals(st) && fresh);
            r.put("display_status", "online".equals(st) && fresh ? "online" : "offline");
            r.remove("age_sec");
        }
        return rows;
    }

    public void deleteAgent(long id) {
        Integer cnt = metaJdbc.queryForObject(
                "SELECT COUNT(*) FROM file_sync_task WHERE agent_id = ? AND exec_host = 'agent'", Integer.class, id);
        if (cnt != null && cnt > 0) {
            throw new IllegalArgumentException("该 Agent 仍绑定 " + cnt + " 个任务, 请先删除任务");
        }
        metaJdbc.update("DELETE FROM dcp_agent WHERE id = ?", id);
        metaJdbc.update("DELETE FROM agent_command WHERE agent_id = ?", id);
    }

    /** 任务变更时入指令队列(FileCopyTaskService 调用) */
    public void enqueue(long agentId, Long taskId, String cmd, Map<String, Object> payload) {
        String json = null;
        if (payload != null) {
            try {
                json = OM.writeValueAsString(payload);
            } catch (Exception e) {
                throw new IllegalArgumentException("指令 payload 序列化失败: " + e.getMessage());
            }
        }
        metaJdbc.update("INSERT INTO agent_command (agent_id, task_id, cmd, payload) VALUES (?, ?, ?, ?)",
                agentId, taskId, cmd, json);
        log.info("Agent 指令入队 agent={} task={} cmd={}", agentId, taskId, cmd);
    }

    // ────────────────────────── Agent 通道(secret 自校验, 不走 JWT) ──────────────────────────

    /** 注册码换 agentId + secret(secret 仅此一次返回) */
    public Map<String, Object> register(String code, String host, String os, String arch, String version) {
        if (code == null || code.isBlank()) {
            throw new IllegalArgumentException("缺少注册码");
        }
        String hash = sha256Hex(code.trim());
        List<Map<String, Object>> rows = metaJdbc.queryForList(
                "SELECT id, name FROM dcp_agent WHERE status = 'pending' AND register_code_hash = ?", hash);
        if (rows.isEmpty()) {
            throw new IllegalArgumentException("注册码无效或已被使用");
        }
        long id = ((Number) rows.get(0).get("id")).longValue();
        String secret = randomSecret();
        metaJdbc.update("UPDATE dcp_agent SET status = 'online', secret_hash = ?, host = ?, os = ?, arch = ?, "
                        + "agent_version = ?, last_heartbeat = NOW() WHERE id = ?",
                sha256Hex(secret), host, os, arch, version, id);
        log.info("Agent 注册成功 id={} name={}", id, rows.get(0).get("name"));
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("agentId", id);
        out.put("secret", secret);
        out.put("name", rows.get(0).get("name"));
        return out;
    }

    /** secret 校验(所有 agent 通道共用) */
    public boolean checkSecret(long agentId, String secret) {
        if (secret == null || secret.isBlank()) {
            return false;
        }
        List<Map<String, Object>> rows = metaJdbc.queryForList(
                "SELECT secret_hash FROM dcp_agent WHERE id = ? AND status IN ('online','offline')", agentId);
        if (rows.isEmpty()) {
            return false;
        }
        return sha256Hex(secret.trim()).equals(String.valueOf(rows.get(0).get("secret_hash")));
    }

    public void heartbeat(long agentId, Map<String, Object> body) {
        Object ver = body == null ? null : body.get("version");
        metaJdbc.update("UPDATE dcp_agent SET status = 'online', agent_version = ?, last_heartbeat = NOW() WHERE id = ?",
                ver == null ? null : String.valueOf(ver), agentId);
    }

    /** 长轮询拉指令: 每 ~800ms 查一次, 直到有 pending 指令或超时 */
    public List<Map<String, Object>> pollCommands(long agentId, int timeoutSeconds) {
        long deadline = System.currentTimeMillis() + timeoutSeconds * 1000L;
        List<Map<String, Object>> out = null;
        while (System.currentTimeMillis() < deadline) {
            out = metaJdbc.queryForList(
                    "SELECT id, task_id, cmd, payload FROM agent_command "
                            + "WHERE agent_id = ? AND status = 'pending' ORDER BY id LIMIT 50", agentId);
            if (!out.isEmpty()) {
                return out;
            }
            try {
                Thread.sleep(800);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        return out == null ? List.of() : out;
    }

    public void ackCommand(long agentId, long commandId) {
        metaJdbc.update("UPDATE agent_command SET status = 'acked', ack_at = NOW() "
                + "WHERE id = ? AND agent_id = ? AND status = 'pending'", commandId, agentId);
    }

    /** 对账: Agent 重连后拉取平台期望的任务列表(与本地增删校正) */
    public List<Map<String, Object>> expectedTasks(long agentId) {
        return metaJdbc.queryForList(
                "SELECT id, name, exec_host, source_datasource_id, source_path, target_datasource_id, "
                        + "target_bucket, target_prefix, strategy_json, schedule_mode, interval_min, cron_expr, "
                        + "version, run_status "
                        + "FROM file_sync_task WHERE agent_id = ? AND exec_host = 'agent' ORDER BY id", agentId);
    }

    /**
     * 执行结果上报(V3: 落 file_sync_run / file_sync_run_file; 幂等: 按 agent+runId 去重)
     * payload: {runId, taskId, status: COMPLETED|FAILED, summary:{transferred,bytes,failed,durationMs},
     *           files:[{relPath,status:success|error,size,error?}]}
     */
    public Map<String, Object> reportRun(long agentId, Map<String, Object> body) {
        Object runIdObj = body.get("runId");
        Number taskIdN = (Number) body.get("taskId");
        if (runIdObj == null || taskIdN == null) {
            throw new IllegalArgumentException("缺少 runId/taskId");
        }
        String runId = String.valueOf(runIdObj);
        long taskId = taskIdN.longValue();
        // 任务归属校验
        Integer owned = metaJdbc.queryForObject(
                "SELECT COUNT(*) FROM file_sync_task WHERE id = ? AND exec_host = 'agent' AND agent_id = ?",
                Integer.class, taskId, agentId);
        if (owned == null || owned == 0) {
            throw new IllegalArgumentException("任务不存在或不属于该 Agent: " + taskId);
        }
        // watch 常驻会话: 首报建 WATCHING run, 追加上报 append, 结束时收尾
        if (Boolean.TRUE.equals(body.get("watch"))) {
            return reportWatchRun(agentId, taskId, String.valueOf(runIdObj), body);
        }
        // runId 幂等
        int inserted = metaJdbc.update("INSERT IGNORE INTO agent_run (agent_id, task_id, run_id) VALUES (?, ?, ?)",
                agentId, taskId, runId);
        if (inserted == 0) {
            return Map.of("duplicate", true, "taskId", taskId, "runId", runId);
        }
        String status = "COMPLETED".equalsIgnoreCase(String.valueOf(body.getOrDefault("status", "COMPLETED"))) ? "COMPLETED" : "FAILED";
        String summaryJson = "{}";
        try {
            summaryJson = body.get("summary") == null ? "{}" : OM.writeValueAsString(body.get("summary"));
        } catch (Exception ignored) {
        }
        String error = body.get("error") == null ? null : String.valueOf(body.get("error"));
        final String fStatus = status;
        final String fSummary = summaryJson;
        final String fError = error == null || error.length() <= 1900 ? error : error.substring(0, 1900);
        // 落 run
        GeneratedKeyHolder kh = new GeneratedKeyHolder();
        metaJdbc.update(con -> {
            PreparedStatement ps = con.prepareStatement(
                    "INSERT INTO file_sync_run (task_id, trigger_type, executor, status, summary, error_msg, started_at, finished_at) "
                            + "VALUES (?, 'agent', ?, ?, ?, ?, NOW(), NOW())", Statement.RETURN_GENERATED_KEYS);
            ps.setLong(1, taskId);
            ps.setString(2, "agent:" + agentId);
            ps.setString(3, fStatus);
            ps.setString(4, fSummary);
            ps.setString(5, fError);
            return ps;
        }, kh);
        long runPk = kh.getKey().longValue();
        // 落文件明细
        int okN = 0, failN = 0;
        Object files = body.get("files");
        if (files instanceof List<?> list) {
            for (Object o : list) {
                if (!(o instanceof Map<?, ?> m)) continue;
                Map<String, Object> f = new LinkedHashMap<>();
                m.forEach((k, v) -> f.put(String.valueOf(k), v));
                String rel = String.valueOf(f.get("relPath"));
                String st = "error".equalsIgnoreCase(String.valueOf(f.get("status"))) ? "error" : "success";
                long size = f.get("size") instanceof Number n ? n.longValue() : 0L;
                String err = f.get("error") == null ? null : String.valueOf(f.get("error"));
                metaJdbc.update("INSERT INTO file_sync_run_file (run_id, rel_path, status, size, error_msg) VALUES (?, ?, ?, ?, ?)",
                        runPk, rel, st, size, err);
                if ("error".equals(st)) failN++; else okN++;
            }
        }
        metaJdbc.update("UPDATE file_sync_task SET run_status = ?, last_run_summary = ?, last_run_time = NOW() WHERE id = ?",
                status, summaryJson, taskId);
        log.info("Agent 上报 task={} run={}: status={} ok={} fail={}", taskId, runId, status, okN, failN);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("duplicate", false);
        out.put("taskId", taskId);
        out.put("runId", runPk);
        out.put("status", status);
        out.put("ok", okN);
        out.put("failed", failN);
        return out;
    }

    /**
     * watch 会话上报: first=true 建 WATCHING run(记 run_pk); 后续 append 文件/滚动 summary;
     * end=true 收尾 run(COMPLETED/FAILED)并更新任务状态。runId = agent 侧会话 id, 幂等。
     */
    private Map<String, Object> reportWatchRun(long agentId, long taskId, String runId, Map<String, Object> body) {
        boolean first = Boolean.TRUE.equals(body.get("first"));
        boolean end = Boolean.TRUE.equals(body.get("end"));
        long runPk = -1;
        long lastSeq = 0;
        if (!first) {
            List<Map<String, Object>> rows = metaJdbc.queryForList(
                    "SELECT run_pk, last_seq FROM agent_run WHERE agent_id = ? AND run_id = ?", agentId, runId);
            if (!rows.isEmpty()) {
                if (rows.get(0).get("run_pk") instanceof Number n) {
                    runPk = n.longValue();
                }
                if (rows.get(0).get("last_seq") instanceof Number n2) {
                    lastSeq = n2.longValue();
                }
            }
        }
        // 批次幂等: append 带递增 seq, 重复(重试)批次直接跳过
        if (!first && !end && lastSeq > 0 && body.get("seq") instanceof Number sn && sn.longValue() <= lastSeq) {
            return Map.of("duplicate", true, "runPk", runPk);
        }
        long batchSeq = body.get("seq") instanceof Number sn2 ? sn2.longValue() : 0;
        if (runPk <= 0) {
            int ins = metaJdbc.update("INSERT IGNORE INTO agent_run (agent_id, task_id, run_id) VALUES (?, ?, ?)",
                    agentId, taskId, runId);
            if (ins == 0) {
                List<Map<String, Object>> rows = metaJdbc.queryForList(
                        "SELECT run_pk FROM agent_run WHERE agent_id = ? AND run_id = ?", agentId, runId);
                if (!rows.isEmpty() && rows.get(0).get("run_pk") instanceof Number n) {
                    runPk = n.longValue();
                }
            }
            if (runPk <= 0) {
                GeneratedKeyHolder kh = new GeneratedKeyHolder();
                metaJdbc.update(con -> {
                    PreparedStatement ps = con.prepareStatement(
                            "INSERT INTO file_sync_run (task_id, trigger_type, executor, status, summary, started_at) "
                                    + "VALUES (?, 'watch', ?, 'WATCHING', ?, NOW())", Statement.RETURN_GENERATED_KEYS);
                    ps.setLong(1, taskId);
                    ps.setString(2, "agent:" + agentId);
                    ps.setString(3, body.get("summary") == null ? null : jsonOf(body.get("summary")));
                    return ps;
                }, kh);
                runPk = kh.getKey().longValue();
                metaJdbc.update("UPDATE agent_run SET run_pk = ?, last_seq = ? WHERE agent_id = ? AND run_id = ?",
                        runPk, batchSeq, agentId, runId);
                metaJdbc.update("UPDATE file_sync_task SET run_status = 'WATCHING' WHERE id = ?", taskId);
            }
        }
        if (!end && batchSeq > 0) {
            metaJdbc.update("UPDATE agent_run SET last_seq = ? WHERE agent_id = ? AND run_id = ?",
                    batchSeq, agentId, runId);
        }
        int okN = 0, failN = 0;
        Object files = body.get("files");
        if (files instanceof List<?> list) {
            for (Object o : list) {
                if (!(o instanceof Map<?, ?> m)) continue;
                Map<String, Object> f = new LinkedHashMap<>();
                m.forEach((k, v) -> f.put(String.valueOf(k), v));
                String rel = String.valueOf(f.get("relPath"));
                String st = "error".equalsIgnoreCase(String.valueOf(f.get("status"))) ? "error" : "success";
                long size = f.get("size") instanceof Number n ? n.longValue() : 0L;
                String err = f.get("error") == null ? null : String.valueOf(f.get("error"));
                metaJdbc.update("INSERT INTO file_sync_run_file (run_id, rel_path, status, size, error_msg) VALUES (?, ?, ?, ?, ?)",
                        runPk, rel, st, size, err);
                if ("error".equals(st)) failN++; else okN++;
            }
        }
        String summaryJson = "{}";
        try {
            summaryJson = body.get("summary") == null ? "{}" : OM.writeValueAsString(body.get("summary"));
        } catch (Exception ignored) {
        }
        if (end) {
            String st = "COMPLETED".equalsIgnoreCase(String.valueOf(body.getOrDefault("status", "COMPLETED"))) ? "COMPLETED" : "FAILED";
            String err = body.get("error") == null ? null : String.valueOf(body.get("error"));
            metaJdbc.update("UPDATE file_sync_run SET status = ?, summary = ?, error_msg = ?, finished_at = NOW() WHERE id = ?",
                    st, summaryJson, truncate(err, 1900), runPk);
            metaJdbc.update("UPDATE file_sync_task SET run_status = ?, last_run_summary = ?, last_run_time = NOW() WHERE id = ?",
                    st, summaryJson, taskId);
        } else {
            metaJdbc.update("UPDATE file_sync_run SET summary = ?, finished_at = NOW() WHERE id = ?", summaryJson, runPk);
            metaJdbc.update("UPDATE file_sync_task SET run_status = 'WATCHING' WHERE id = ?", taskId);
        }
        log.info("Agent watch 上报 task={} session={} first={} end={} ok={} fail={}", taskId, runId, first, end, okN, failN);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("runPk", runPk);
        out.put("first", first);
        out.put("end", end);
        out.put("ok", okN);
        out.put("failed", failN);
        return out;
    }

    private static String truncate(String s, int n) {
        if (s == null) return null;
        return s.length() > n ? s.substring(0, n) : s;
    }

    private static String jsonOf(Object o) {
        try {
            return AgentService.OM.writeValueAsString(o);
        } catch (Exception e) {
            return "{}";
        }
    }

    // ────────────────────────── 工具 ──────────────────────────

    private static String sha256Hex(String s) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] d = md.digest(s.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder();
            for (byte b : d) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static String randomRegisterCode() {
        String alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 12; i++) {
            sb.append(alphabet.charAt(RND.nextInt(alphabet.length())));
        }
        return sb.toString();
    }

    private static String randomSecret() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 32; i++) {
            sb.append("0123456789abcdef".charAt(RND.nextInt(16)));
        }
        return sb.toString();
    }
}
