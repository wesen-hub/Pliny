package com.dcp.agent;

import com.fasterxml.jackson.databind.JsonNode;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;

/**
 * Agent 执行引擎: 任务管理 + 本地调度(interval/cron) + rclone 执行(sync/copy + --use-json-log 解析)
 * + 断线补报(本地 pending 队列) + 对账(清理平台已删任务)
 */
public final class SyncEngine {

    private final AgentConfig cfg;
    private final Store store;
    private final PlatformClient client;

    private volatile long agentId = -1;
    private volatile String secret = null;

    private final ExecutorService runners = Executors.newFixedThreadPool(4, r -> {
        Thread t = new Thread(r, "agent-runner");
        t.setDaemon(true);
        return t;
    });
    private final Set<Long> running = ConcurrentHashMap.newKeySet();


    private volatile boolean started = false;

    public SyncEngine(AgentConfig cfg, Store store, PlatformClient client) {
        this.cfg = cfg;
        this.store = store;
        this.client = client;
    }

    public void setCredentials(long agentId, String secret) {
        this.agentId = agentId;
        this.secret = secret;
    }

    // ────────────────────────── 生命周期 ──────────────────────────

    public void start() {
        if (started) return;
        started = true;
        log("Agent " + agentId + " 启动: 平台 " + cfg.platformUrl + " dataDir=" + cfg.dataDir);
        Thread hb = new Thread(this::heartbeatLoop, "agent-hb");
        Thread poll = new Thread(this::pollLoop, "agent-poll");
        Thread sched = new Thread(this::schedulerLoop, "agent-sched");
        hb.setDaemon(true); poll.setDaemon(true); sched.setDaemon(true);
        hb.start(); poll.start(); sched.start();
        // 启动后 8s 做一次对账
        runners.submit(() -> {
            try { Thread.sleep(8000); } catch (InterruptedException ignored) {}
            reconcile();
        });
    }

    // ────────────────────────── 主循环 ──────────────────────────

    private void heartbeatLoop() {
        int fail = 0;
        while (true) {
            try {
                client.heartbeat(agentId, secret);
                fail = 0;
            } catch (Exception e) {
                fail++;
                log("心跳失败(" + fail + "): " + e.getMessage());
            }
            sleep(cfg.heartbeatSec * 1000L);
        }
    }

    private void pollLoop() {
        while (true) {
            try {
                List<Map<String, Object>> cmds = client.pollCommands(agentId, secret);
                for (Map<String, Object> c : cmds) {
                    handleCommand(c);
                }
                flushPendingRuns();
                sleep(300);
            } catch (PlatformClient.ApiException e) {
                if (e.code == 401) {
                    log("secret 校验失败(401), 请重新注册: " + e.getMessage());
                }
                sleep(5000);
            } catch (Exception e) {
                log("轮询异常: " + e.getMessage());
                sleep(3000);
            }
        }
    }

    private void schedulerLoop() {
        int tick = 0;
        while (true) {
            try {
                tickOnce();
                if (++tick % 20 == 0) reconcile();   // 每 ~5 分钟
            } catch (Exception e) {
                log("调度 tick 异常: " + e.getMessage());
            }
            sleep(15000);
        }
    }

    // ────────────────────────── 指令处理 ──────────────────────────

    private void handleCommand(Map<String, Object> c) {
        long cmdId = ((Number) c.get("id")).longValue();
        String cmd = String.valueOf(c.get("cmd"));
        Object taskIdObj = c.get("task_id");
        try {
            switch (cmd) {
                case "task_upsert" -> {
                    Map<String, Object> payload = parsePayload(c.get("payload"));
                    applyTask(payload);
                    log("任务应用 task=" + payload.get("taskId") + " name=" + payload.get("name") + " v=" + payload.get("version"));
                }
                case "task_remove" -> {
                    removeTask(taskIdObj == null ? -1 : ((Number) taskIdObj).longValue());
                    log("任务移除 task=" + taskIdObj);
                }
                case "run_now" -> {
                    if (taskIdObj != null) {
                        long tid = ((Number) taskIdObj).longValue();
                        runners.submit(() -> execute(tid, "manual"));
                    }
                }
                case "task_stop" -> {
                    // rclone 无常驻会话(引擎切换后 watch 已移除): 一次性轮不在可停范围内, 仅记日志
                    log("收到 task_stop, 但 rclone 全文一次性执行无常驻会话, 忽略 task=" + taskIdObj);
                }
                default -> log("未知指令: " + cmd);
            }
            client.ack(agentId, secret, cmdId);
        } catch (Exception e) {
            // 应用失败不 ack: 指令保留 pending, 下次轮询重试(at-least-once); 坏指令由对账/平台侧修正
            log("指令处理失败(不 ack, 将重试) cmd=" + cmd + " id=" + cmdId + ": " + e.getMessage());
        }
    }

    // ────────────────────────── 任务管理 ──────────────────────────

    private synchronized void applyTask(Map<String, Object> p) {
        if (p == null || p.get("taskId") == null) throw new IllegalArgumentException("task_upsert payload 缺 taskId");
        long tid = ((Number) p.get("taskId")).longValue();
        List<Map<String, Object>> tasks = store.tasks();
        for (Map<String, Object> t : tasks) {
            if (((Number) t.get("taskId")).longValue() == tid) {
                int oldV = t.get("version") instanceof Number n ? n.intValue() : 0;
                int newV = p.get("version") instanceof Number n2 ? n2.intValue() : oldV + 1;
                if (newV < oldV) return; // 旧版本不回退
                p.put("lastRunAt", t.get("lastRunAt"));
                p.put("localStatus", t.get("localStatus"));
                p.put("lastCronKey", t.get("lastCronKey"));
                p.put("baselineDoneLocal", t.get("baselineDoneLocal"));   // 本地基线标记随任务定义变更保留
                tasks.set(tasks.indexOf(t), p);
                store.saveTasks(tasks);
                return;
            }
        }
        p.put("lastRunAt", null);
        p.put("localStatus", "IDLE");
        p.put("baselineDoneLocal", false);
        tasks.add(p);
        store.saveTasks(tasks);
    }

    private synchronized void removeTask(long tid) {
        List<Map<String, Object>> tasks = store.tasks();
        tasks.removeIf(t -> ((Number) t.get("taskId")).longValue() == tid);
        store.saveTasks(tasks);
    }

    /** 全量轮成功后置本地基线(store.tasks() 每次返回新对象, 必须回写持久化) */
    private synchronized void markBaseline(long tid) {
        List<Map<String, Object>> tasks = store.tasks();
        for (Map<String, Object> t : tasks) {
            if (((Number) t.get("taskId")).longValue() == tid) {
                t.put("baselineDoneLocal", true);
                break;
            }
        }
        store.saveTasks(tasks);
    }

    private Map<String, Object> findTask(long tid) {
        for (Map<String, Object> t : store.tasks()) {
            if (((Number) t.get("taskId")).longValue() == tid) return t;
        }
        return null;
    }

    // ────────────────────────── 调度 tick ──────────────────────────

    private void tickOnce() {
        flushPendingRuns();
        long now = System.currentTimeMillis();
        LocalDateTime cur = LocalDateTime.now();
        for (Map<String, Object> t : store.tasks()) {
            long tid = ((Number) t.get("taskId")).longValue();
            if (running.contains(tid)) continue;
            Map<String, Object> sched = mapOf(t.get("schedule"));
            String mode = sched.get("mode") == null ? "none" : String.valueOf(sched.get("mode"));
            boolean due = false;
            if ("interval".equals(mode)) {
                int min = sched.get("intervalMin") instanceof Number n ? Math.max(n.intValue(), 1) : 5;
                Object last = t.get("lastRunAt");
                long lastMs = last == null ? 0 : (last instanceof Number n2 ? n2.longValue() : 0);
                due = now >= lastMs + min * 60000L;
            } else if ("cron".equals(mode)) {
                String expr = sched.get("cronExpr") == null ? null : String.valueOf(sched.get("cronExpr"));
                if (expr != null) {
                    // 分钟粒度: tick 几乎不可能精确落在第 0 秒, 用该分钟 0 秒代表时刻判匹配
                    LocalDateTime rep = cur.withSecond(0);
                    int minuteKey = rep.getYear() * 1000000 + rep.getMonthValue() * 10000
                            + rep.getDayOfMonth() * 100 + rep.getHour() * 100 + rep.getMinute();
                    Object lk = t.get("lastCronKey");
                    due = cronMatches(expr, rep) && !Integer.valueOf(minuteKey).equals(lk == null ? null : Integer.valueOf(String.valueOf(lk)));
                    if (due) t.put("lastCronKey", minuteKey);
                }
            }
            if (due) {
                t.put("lastRunAt", now);
                store.saveTasks(store.tasks());
                runners.submit(() -> execute(tid, "schedule"));
            }
        }
    }

    // ────────────────────────── 执行(rclone sync/copy) ──────────────────────────

    private void execute(long tid, String trigger) {
        if (!running.add(tid)) return;
        try {
            Map<String, Object> task = findTask(tid);
            if (task == null) return;
            doExecute(task, trigger);
        } finally {
            running.remove(tid);
        }
    }

    private void doExecute(Map<String, Object> task, String trigger) {
        long tid = ((Number) task.get("taskId")).longValue();
        long t0 = System.currentTimeMillis();
        brave.Span span = AgentTracing.tracer().nextSpan().name("task-execute").start();
        span.tag("task.id", String.valueOf(tid));
        span.tag("task.trigger", trigger);
        String runId = AgentConfig.newRunId();
        List<Map<String, Object>> files = new ArrayList<>();
        int failN = 0;
        String errorTail = null;
        String runStatus = "COMPLETED";
        String opUsed = null;
        try {
            Map<String, Object> source = mapOf(task.get("source"));
            Map<String, Object> target = mapOf(task.get("target"));
            String type = String.valueOf(source.getOrDefault("type", "local"));
            String srcDir;
            if ("local".equals(type)) {
                String path = String.valueOf(source.getOrDefault("path", ""));
                if (path.startsWith("/")) {
                    srcDir = path;
                } else {
                    String root = String.valueOf(source.getOrDefault("root", "/"));
                    srcDir = root.endsWith("/") ? root + path : root + "/" + path;
                }
            } else {
                throw new IllegalStateException("Agent 仅执行 local 源任务, 收到 " + type);
            }
            Path srcPath = Path.of(srcDir);
            if (!Files.isDirectory(srcPath)) {
                throw new IllegalStateException("源目录不存在: " + srcDir);
            }
            String dstBucket = String.valueOf(target.getOrDefault("bucket", ""));
            String dstPrefix = String.valueOf(target.getOrDefault("prefix", ""));
            String dstEndpoint = String.valueOf(target.getOrDefault("endpoint", ""));
            String dstAk = String.valueOf(target.getOrDefault("accessKey", ""));
            String dstSk = String.valueOf(target.getOrDefault("secretKey", ""));
            Map<String, Object> st = mapOf(task.get("strategy"));
            // 模式三态: full_once 恒全量; incr_scheduled 恒增量; full_then_incr 首次(无基线)全量
            String syncMode = String.valueOf(task.getOrDefault("syncMode", "full_then_incr"));
            boolean baseline = bool(task, "baselineDone") || bool(task, "baselineDoneLocal");
            boolean full = "full_once".equals(syncMode) || ("full_then_incr".equals(syncMode) && !baseline);
            boolean deleteExtras = st.get("deleteExtras") == null || bool(st, "deleteExtras");
            String op = full && deleteExtras ? "sync" : "copy";
            opUsed = op;

            Path conf = writeRcloneConf(tid, dstEndpoint, dstAk, dstSk);
            List<String> args = new ArrayList<>();
            args.add(cfg.rcloneBin);
            args.add(op);
            args.add(srcDir);
            args.add("dcpdst:" + dstBucket + (dstPrefix.isEmpty() ? "" : "/" + dstPrefix));
            args.add("--config");
            args.add(conf.toString());
            String cmp = String.valueOf(st.getOrDefault("compare", "size_mtime"));
            if ("size_only".equals(cmp)) args.add("--size-only");
            else if ("checksum".equals(cmp)) args.add("--checksum");
            String um = String.valueOf(st.getOrDefault("updateMode", "overwrite"));
            if ("update".equals(um)) args.add("--update");
            else if ("ignore_existing".equals(um)) args.add("--ignore-existing");
            for (Object inc : listOf(st.get("includes"))) {
                args.add("--include");
                args.add(String.valueOf(inc));
            }
            for (Object ex : listOf(st.get("excludes"))) {
                args.add("--exclude");
                args.add(String.valueOf(ex));
            }
            addOpt(args, st, "minAge", "--min-age");
            addOpt(args, st, "maxAge", "--max-age");
            addOpt(args, st, "minSize", "--min-size");
            addOpt(args, st, "maxSize", "--max-size");
            args.add("--transfers");
            args.add(String.valueOf(intOf(st.get("transfers"), 8)));
            args.add("--checkers");
            args.add(String.valueOf(intOf(st.get("checkers"), 8)));
            addOpt(args, st, "bwlimit", "--bwlimit");
            args.add("--retries");
            args.add(String.valueOf(intOf(st.get("retries"), 3)));
            args.add("--low-level-retries");
            args.add(String.valueOf(intOf(st.get("lowLevelRetries"), 10)));
            if (bool(st, "dryRun")) args.add("--dry-run");
            args.add("--use-json-log");
            args.add("--log-level");
            args.add("INFO");
            args.add("--stats");
            args.add("1s");

            int delN = 0;
            Process p = new ProcessBuilder(args).redirectErrorStream(true).start();
            try (BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = br.readLine()) != null) {
                    if (line.isBlank()) continue;
                    JsonNode n;
                    try {
                        n = AgentConfig.OM.readTree(line);
                    } catch (Exception e) {
                        continue;
                    }
                    if (!n.isObject()) continue;
                    String level = n.path("level").asText();
                    String msg = n.path("msg").asText("");
                    String object = n.path("object").asText("");
                    if (msg.startsWith("Config file")) continue;   // rclone.conf 缺失噪声
                    if ("error".equals(level)) {
                        if (msg.startsWith("Attempt ")) {
                            errorTail = msg;
                            continue;
                        }
                        if (n.path("objectType").asText("").endsWith(".Object")) {
                            Map<String, Object> f = new LinkedHashMap<>();
                            f.put("relPath", object.isEmpty() ? "-" : object);
                            f.put("status", "error");
                            f.put("error", msg);
                            files.add(f);
                            failN++;
                        }
                        errorTail = msg;
                    } else if (msg.startsWith("Copied")) {
                        Map<String, Object> f = new LinkedHashMap<>();
                        f.put("relPath", object.isEmpty() ? "-" : object);
                        f.put("status", "success");
                        f.put("size", n.path("size").asLong(0));
                        files.add(f);
                    } else if ("Deleted".equals(msg)) {
                        Map<String, Object> f = new LinkedHashMap<>();
                        f.put("relPath", object.isEmpty() ? "-" : object);
                        f.put("status", "deleted");
                        f.put("size", 0L);
                        files.add(f);
                        delN++;
                    }
                }
            }
            int code;
            if (!p.waitFor(1800, TimeUnit.SECONDS)) {
                p.destroyForcibly();
                code = -1;
                if (errorTail == null) errorTail = "rclone 执行超时(1800s)";
            } else {
                code = p.exitValue();
            }
            try {
                Files.deleteIfExists(conf);   // 临时 rclone.conf 含凭证, 轮次结束即删
            } catch (Exception ignored) {
            }
            if (code != 0 && files.stream().noneMatch(f -> "success".equals(f.get("status")))) {
                failN = Math.max(failN, 1);
                if (errorTail == null) errorTail = "rclone 退出码 " + code;
            }
            if (full && failN == 0 && code == 0) {
                markBaseline(tid);   // 全量成功 → 置本地基线(full_then_incr 下次转增量)
            }
        } catch (Exception e) {
            failN++;
            errorTail = e.getMessage();
        }
        if (failN > 0 && files.stream().noneMatch(f -> "success".equals(f.get("status")))) {
            runStatus = "FAILED";
        }
        long okN = files.stream().filter(f -> "success".equals(f.get("status"))).count();
        long bytes = files.stream().filter(f -> "success".equals(f.get("status")))
                .mapToLong(f -> f.get("size") instanceof Number n ? n.longValue() : 0).sum();
        long delN2 = files.stream().filter(f -> "deleted".equals(f.get("status"))).count();
        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("op", opUsed == null ? "copy" : opUsed);
        summary.put("transferred", okN);
        summary.put("deleted", delN2);
        summary.put("bytes", bytes);
        summary.put("failed", failN);
        summary.put("durationMs", System.currentTimeMillis() - t0);
        store.saveTasks(store.tasks());   // 持久化 baselineDoneLocal / 状态

        Map<String, Object> run = new LinkedHashMap<>();
        run.put("runId", runId);
        run.put("taskId", tid);
        run.put("trigger", trigger);
        run.put("status", runStatus);
        run.put("summary", summary);
        run.put("files", files);
        run.put("error", errorTail);
        enqueuePending(run);
        updateLocalStatus(tid, runStatus, t0);
        span.tag("run.status", runStatus);
        span.tag("run.op", opUsed == null ? "copy" : opUsed);
        span.tag("run.ok", String.valueOf(okN));
        span.tag("run.failed", String.valueOf(failN));
        span.tag("run.duration-ms", String.valueOf(System.currentTimeMillis() - t0));
        if (failN > 0) span.tag("error", String.valueOf(failN));
        span.finish();
        log("任务 " + tid + " 执行完 " + trigger + ": " + runStatus + " ok=" + okN + " fail=" + failN);
    }

    private synchronized void enqueuePending(Map<String, Object> run) {
        List<Map<String, Object>> pending = store.pendingRuns();
        pending.add(run);
        store.savePendingRuns(pending);
        flushPendingRuns();
    }

    /** 断线补报: 成功即移除; 失败(平台不可达/401)保留, 下次再试 */
    private synchronized void flushPendingRuns() {
        List<Map<String, Object>> pending = store.pendingRuns();
        if (pending.isEmpty()) return;
        List<Map<String, Object>> left = new ArrayList<>();
        for (Map<String, Object> run : pending) {
            try {
                client.reportRun(agentId, secret, run);
            } catch (Exception e) {
                left.add(run);
            }
        }
        if (left.size() != pending.size()) {
            store.savePendingRuns(left);
        }
    }

    private synchronized void updateLocalStatus(long tid, String status, long atMs) {
        List<Map<String, Object>> tasks = store.tasks();
        for (Map<String, Object> t : tasks) {
            if (((Number) t.get("taskId")).longValue() == tid) {
                t.put("localStatus", status);
                t.put("lastRunAt", atMs);
            }
        }
        store.saveTasks(tasks);
    }

    // ────────────────────────── 工具 ──────────────────────────

    /** 生成临时 rclone.conf(命名 remote dcpdst; 含目标凭证 → 轮次结束即删) */
    private static Path writeRcloneConf(long tid, String endpoint, String ak, String sk) throws Exception {
        String body = "[dcpdst]\n"
                + "type = s3\n"
                + "provider = Minio\n"
                + "endpoint = " + endpoint + "\n"
                + "access_key_id = " + ak + "\n"
                + "secret_access_key = " + sk + "\n"
                + "force_path_style = true\n";
        Path f = Path.of("/tmp/dcp-agent-rclone-" + tid + "-" + System.nanoTime() + ".conf");
        Files.writeString(f, body, StandardCharsets.UTF_8);
        try {
            Files.setPosixFilePermissions(f, java.nio.file.attribute.PosixFilePermissions.fromString("rw-------"));
        } catch (Exception ignored) {
        }
        return f;
    }

    private static void addOpt(List<String> args, Map<String, Object> st, String key, String flag) {
        Object v = st.get(key);
        if (v == null || String.valueOf(v).isBlank()) return;
        args.add(flag);
        args.add(String.valueOf(v));
    }

    private static int intOf(Object v, int dft) {
        if (v instanceof Number n) return n.intValue();
        try {
            return v == null || String.valueOf(v).isBlank() ? dft : Integer.parseInt(String.valueOf(v).trim());
        } catch (Exception e) {
            return dft;
        }
    }

    // ────────────────────────── 对账 ──────────────────────────

    private void reconcile() {
        try {
            List<Map<String, Object>> expected = client.expectedTasks(agentId, secret);
            List<Map<String, Object>> tasks = store.tasks();
            Set<Long> localIds = new HashSet<>();
            for (Map<String, Object> t : tasks) {
                localIds.add(((Number) t.get("taskId")).longValue());
            }
            Set<Long> platformIds = new HashSet<>();
            for (Map<String, Object> e : expected) {
                long tid = ((Number) e.get("id")).longValue();
                platformIds.add(tid);
                Map<String, Object> local = findTask(tid);
                int pv = e.get("version") instanceof Number n ? n.intValue() : 0;
                int lv = local != null && local.get("version") instanceof Number n2 ? n2.intValue() : -1;
                if (local == null) {
                    // 平台有本地无(指令可能已丢): 请求平台重发 task_upsert
                    log("对账: 本地缺任务 " + tid + ", 请求平台重发");
                    try {
                        client.resendTask(agentId, secret, tid);
                    } catch (Exception ex) {
                        log("对账 resend 失败 task=" + tid + ": " + ex.getMessage());
                    }
                } else if (pv > lv) {
                    log("对账: 任务 " + tid + " 版本落后(本地 " + lv + " < 平台 " + pv + "), 请求重发");
                    try {
                        client.resendTask(agentId, secret, tid);
                    } catch (Exception ex) {
                        log("对账 resend 失败 task=" + tid + ": " + ex.getMessage());
                    }
                }
            }
            // 清理平台已删任务
            List<Map<String, Object>> left = new ArrayList<>();
            for (Map<String, Object> t : tasks) {
                long tid = ((Number) t.get("taskId")).longValue();
                if (platformIds.contains(tid)) {
                    left.add(t);
                } else {
                    log("对账: 移除平台已删任务 task=" + tid);
                }
            }
            store.saveTasks(left);
            log("对账完成: 平台 " + platformIds.size() + " 任务, 本地保留 " + left.size());
        } catch (Exception e) {
            log("对账失败: " + e.getMessage());
        }
    }

    // ────────────────────────── 工具 ──────────────────────────


    /** 轻量 6 段 cron(秒 分 时 日 月 周; 5 段前补 0); 仅 * / , - ? 语义 */
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

    @SuppressWarnings("unchecked")
    private static Map<String, Object> mapOf(Object o) {
        if (o instanceof Map<?, ?> m) {
            Map<String, Object> out = new LinkedHashMap<>();
            m.forEach((k, v) -> out.put(String.valueOf(k), v));
            return out;
        }
        return new LinkedHashMap<>();
    }

    /** API 返回的 payload 是 JSON 字符串, 统一解析为 Map */
    private static Map<String, Object> parsePayload(Object o) {
        if (o == null) return new LinkedHashMap<>();
        if (o instanceof String s && !s.isBlank()) {
            try {
                JsonNode n = AgentConfig.OM.readTree(s);
                if (n != null && n.isObject()) {
                    return AgentConfig.OM.convertValue(n,
                            new com.fasterxml.jackson.core.type.TypeReference<LinkedHashMap<String, Object>>() {});
                }
            } catch (Exception ignored) {
            }
            return new LinkedHashMap<>();
        }
        return mapOf(o);
    }

    @SuppressWarnings("unchecked")
    private static List<Object> listOf(Object o) {
        if (o instanceof List<?> l) return new ArrayList<>(l);
        return List.of();
    }

    private static boolean bool(Map<String, Object> m, String k) {
        return Boolean.TRUE.equals(m.get(k));
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void log(String msg) {
        System.out.println("[" + java.time.LocalTime.now().withNano(0) + "] " + msg);
    }
}
