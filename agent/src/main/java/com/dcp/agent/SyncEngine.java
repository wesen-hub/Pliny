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
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Agent 执行引擎: 任务管理 + 本地调度(interval/cron) + mc mirror 执行(--json 解析)
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

    /** 本地 watch 常驻会话(任务级): mc mirror --watch 子进程 + 事件批上报 */
    private static final class AgentWatch {
        final long taskId;
        final String runId;
        final Process p;
        final AtomicBoolean stopped = new AtomicBoolean(false);
        final List<Map<String, Object>> batch = new ArrayList<>();
        long seq = 0;
        boolean reported = false;
        long okN = 0, bytes = 0, failN = 0;

        AgentWatch(long taskId, String runId, Process p) {
            this.taskId = taskId;
            this.runId = runId;
            this.p = p;
        }
    }

    private final Map<Long, AgentWatch> watches = new ConcurrentHashMap<>();

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
                    if (taskIdObj != null) {
                        long tid = ((Number) taskIdObj).longValue();
                        stopAgentWatch(tid, "COMPLETED");
                        log("收到 task_stop, 已停止实时监听 task=" + tid);
                    }
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
                tasks.set(tasks.indexOf(t), p);
                store.saveTasks(tasks);
                syncWatchState(p);
                return;
            }
        }
        p.put("lastRunAt", null);
        p.put("localStatus", "IDLE");
        tasks.add(p);
        store.saveTasks(tasks);
        syncWatchState(p);
    }

    /** 任务定义变更后对齐 watch 常驻状态 */
    private void syncWatchState(Map<String, Object> p) {
        long tid = ((Number) p.get("taskId")).longValue();
        boolean wantWatch = Boolean.TRUE.equals(p.get("watch")) || "1".equals(String.valueOf(p.get("watch")));
        if (wantWatch) {
            startAgentWatch(p);
        } else {
            stopAgentWatchSilent(tid);
        }
    }

    private synchronized void removeTask(long tid) {
        stopAgentWatchSilent(tid);
        List<Map<String, Object>> tasks = store.tasks();
        tasks.removeIf(t -> ((Number) t.get("taskId")).longValue() == tid);
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

    // ────────────────────────── 执行(mc mirror) ──────────────────────────

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
        String runId = AgentConfig.newRunId();
        List<Map<String, Object>> files = new ArrayList<>();
        int failN = 0;
        String errorTail = null;
        String runStatus = "COMPLETED";
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
            runMc(List.of("alias", "set", "tgt", dstEndpoint, dstAk, dstSk));

            List<String> args = new ArrayList<>();
            args.add("mc");
            args.add("mirror");
            Map<String, Object> st = mapOf(task.get("strategy"));
            if (bool(st, "overwrite")) args.add("--overwrite");
            if (bool(st, "remove")) args.add("--remove");
            if (bool(st, "preserve")) args.add("--preserve");
            if (bool(st, "dryRun")) args.add("--dry-run");
            if (bool(st, "retry")) args.add("--retry");
            for (Object ex : listOf(st.get("exclude"))) {
                args.add("--exclude");
                args.add(String.valueOf(ex));
            }
            Map<String, Object> tf = mapOf(st.get("timeFilter"));
            if (!tf.isEmpty()) {
                String dir = String.valueOf(tf.getOrDefault("dir", "newer"));
                args.add("newer".equals(dir) ? "--newer-than" : "--older-than");
                args.add(String.valueOf(tf.get("value")));
            }
            Map<String, Object> adv = mapOf(st.get("advanced"));
            if (adv.get("maxWorkers") instanceof Number mw && mw.intValue() > 0) {
                args.add("--max-workers");
                args.add(String.valueOf(mw.intValue()));
            }
            String dstBase = "tgt/" + dstBucket + (dstPrefix.isEmpty() ? "" : "/" + dstPrefix);
            args.add(srcDir.endsWith("/") ? srcDir : srcDir);
            args.add(dstBase);
            args.add("--json");

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
                    String status = n.path("status").asText();
                    if ("success".equals(status)) {
                        String s = n.path("source").asText("");
                        if (!s.isEmpty()) {
                            Map<String, Object> f = new LinkedHashMap<>();
                            f.put("relPath", relOf(s, srcDir));
                            f.put("status", "success");
                            f.put("size", n.path("size").asLong(0));
                            files.add(f);
                        }
                    } else if ("error".equals(status)) {
                        String msg = n.path("error").path("message").asText(n.path("error").asText());
                        Map<String, Object> f = new LinkedHashMap<>();
                        f.put("relPath", "-");
                        f.put("status", "error");
                        f.put("error", msg);
                        files.add(f);
                        failN++;
                        errorTail = msg;
                    }
                }
            }
            int code;
            if (!p.waitFor(600, TimeUnit.SECONDS)) {
                p.destroyForcibly();
                code = -1;
            } else {
                code = p.exitValue();
            }
            if (code != 0 && files.stream().noneMatch(f -> "success".equals(f.get("status")))) {
                failN = Math.max(failN, 1);
                if (errorTail == null) errorTail = "mc mirror 退出码 " + code;
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
        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("transferred", okN);
        summary.put("bytes", bytes);
        summary.put("failed", failN);
        summary.put("durationMs", System.currentTimeMillis() - t0);

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

    // ────────────────────────── watch 常驻(agent 本地) ──────────────────────────

    /** 启动本地常驻监听(payload.watch=true 的任务); 已运行则忽略 */
    private void startAgentWatch(Map<String, Object> task) {
        long tid = ((Number) task.get("taskId")).longValue();
        AgentWatch w = watches.get(tid);
        if (w != null && w.p.isAlive() && !w.stopped.get()) return;
        try {
            List<String> args = agentMirrorArgs(task, true);
            Process p = new ProcessBuilder(args).redirectErrorStream(true).start();
            AgentWatch aw = new AgentWatch(tid, AgentConfig.newRunId(), p);
            watches.put(tid, aw);
            Thread rd = new Thread(() -> watchReadLoop(aw), "agent-watch-" + tid);
            Thread fl = new Thread(() -> watchFlushLoop(aw), "agent-watchflush-" + tid);
            rd.setDaemon(true);
            fl.setDaemon(true);
            rd.start();
            fl.start();
            log("实时监听已启动 task=" + tid + " session=" + aw.runId);
        } catch (Exception e) {
            log("实时监听启动失败 task=" + tid + ": " + e.getMessage());
        }
    }

    /** 事件读循环: 实时事件入批; 进程退出即收尾 */
    private void watchReadLoop(AgentWatch aw) {
        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(aw.p.getInputStream(), StandardCharsets.UTF_8))) {
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
                String status = n.path("status").asText();
                if ("success".equals(status)) {
                    String s = n.path("source").asText("");
                    if (!s.isEmpty()) {
                        Map<String, Object> f = new LinkedHashMap<>();
                        f.put("relPath", relOf(s, sourceDirOf(aw.taskId)));
                        f.put("status", "success");
                        f.put("size", n.path("size").asLong(0));
                        synchronized (aw.batch) {
                            aw.batch.add(f);
                            aw.okN++;
                            aw.bytes += n.path("size").asLong(0);
                        }
                    }
                } else if ("error".equals(status)) {
                    String msg = n.path("error").path("message").asText(n.path("error").asText());
                    Map<String, Object> f = new LinkedHashMap<>();
                    f.put("relPath", "-");
                    f.put("status", "error");
                    f.put("error", msg);
                    synchronized (aw.batch) {
                        aw.batch.add(f);
                        aw.failN++;
                    }
                }
            }
        } catch (Exception ignored) {
        }
        if (!aw.stopped.get()) {
            // 进程异常退出: 尝试收尾上报
            log("实时监听进程退出 task=" + aw.taskId);
            stopAgentWatch(aw.taskId, aw.failN > 0 && aw.okN == 0 ? "FAILED" : "COMPLETED");
        }
    }

    /** 批次刷盘循环: 每 5s 有事件则上报一批 */
    private void watchFlushLoop(AgentWatch aw) {
        while (!aw.stopped.get()) {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            flushWatchBatch(aw, false);
        }
        flushWatchBatch(aw, false);
    }

    /** 批次上报(watch 会话, first 标记 + seq 幂等); 走 pending 队列, 断线补报 */
    private synchronized void flushWatchBatch(AgentWatch aw, boolean end) {
        List<Map<String, Object>> files;
        synchronized (aw.batch) {
            if (aw.batch.isEmpty() && !end) return;
            files = new ArrayList<>(aw.batch);
            aw.batch.clear();
        }
        if (files.isEmpty() && !end) return;
        Map<String, Object> run = new LinkedHashMap<>();
        run.put("runId", aw.runId);
        run.put("taskId", aw.taskId);
        run.put("watch", true);
        run.put("first", !aw.reported);
        run.put("seq", aw.seq + 1);
        run.put("files", files);
        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("transferred", aw.okN);
        summary.put("bytes", aw.bytes);
        summary.put("failed", aw.failN);
        run.put("summary", summary);
        if (!aw.reported) {
            run.put("status", "WATCHING");
        }
        if (end) {
            run.put("end", true);
            run.put("status", "COMPLETED");
        }
        aw.reported = true;
        aw.seq++;
        enqueuePending(run);
    }

    /** 停止监听并收尾(end 上报, 任务保留) */
    private void stopAgentWatch(long tid, String status) {
        AgentWatch aw = watches.remove(tid);
        if (aw == null) return;
        aw.stopped.set(true);
        aw.p.destroy();
        // 等 reader 退出
        try {
            aw.p.waitFor(3, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        flushWatchBatch(aw, true);
        log("实时监听已停止 task=" + tid + " status=" + status);
    }

    /** 静默停止(任务删除场景, 不上报) */
    private void stopAgentWatchSilent(long tid) {
        AgentWatch aw = watches.remove(tid);
        if (aw == null) return;
        aw.stopped.set(true);
        aw.p.destroy();
        log("实时监听已移除(任务删除/非 watch) task=" + tid);
    }

    /** 源目录(绝对): 供事件 relPath 剥离 */
    private String sourceDirOf(long tid) {
        Map<String, Object> task = findTask(tid);
        if (task == null) return "";
        Map<String, Object> source = mapOf(task.get("source"));
        String path = String.valueOf(source.getOrDefault("path", ""));
        if (path.startsWith("/")) return path;
        String root = String.valueOf(source.getOrDefault("root", "/"));
        return root.endsWith("/") ? root + path : root + "/" + path;
    }

    /** 拼 mirror 参数(task → 命令), watch=true 时带 --watch */
    private List<String> agentMirrorArgs(Map<String, Object> task, boolean watch) throws Exception {
        Map<String, Object> source = mapOf(task.get("source"));
        Map<String, Object> target = mapOf(task.get("target"));
        String type = String.valueOf(source.getOrDefault("type", "local"));
        if (!"local".equals(type)) throw new IllegalStateException("Agent 仅执行 local 源任务, 收到 " + type);
        String srcDir;
        String path = String.valueOf(source.getOrDefault("path", ""));
        if (path.startsWith("/")) {
            srcDir = path;
        } else {
            String root = String.valueOf(source.getOrDefault("root", "/"));
            srcDir = root.endsWith("/") ? root + path : root + "/" + path;
        }
        runMcQuiet(List.of("alias", "set", "tgt", String.valueOf(target.get("endpoint")),
                String.valueOf(target.get("accessKey")), String.valueOf(target.get("secretKey"))));
        List<String> args = new ArrayList<>();
        args.add("mc");
        args.add("mirror");
        if (watch) args.add("--watch");
        Map<String, Object> st = mapOf(task.get("strategy"));
        if (bool(st, "overwrite")) args.add("--overwrite");
        if (bool(st, "remove")) args.add("--remove");
        if (bool(st, "preserve")) args.add("--preserve");
        if (bool(st, "dryRun")) args.add("--dry-run");
        for (Object ex : listOf(st.get("exclude"))) {
            args.add("--exclude");
            args.add(String.valueOf(ex));
        }
        Map<String, Object> tf = mapOf(st.get("timeFilter"));
        if (!tf.isEmpty()) {
            String dir = String.valueOf(tf.getOrDefault("dir", "newer"));
            args.add("newer".equals(dir) ? "--newer-than" : "--older-than");
            args.add(String.valueOf(tf.get("value")));
        }
        Map<String, Object> adv = mapOf(st.get("advanced"));
        if (adv.get("maxWorkers") instanceof Number mw && mw.intValue() > 0) {
            args.add("--max-workers");
            args.add(String.valueOf(mw.intValue()));
        }
        String dstPrefix = String.valueOf(target.getOrDefault("prefix", ""));
        args.add(srcDir);
        args.add("tgt/" + target.get("bucket") + (dstPrefix.isEmpty() ? "" : "/" + dstPrefix));
        args.add("--json");
        return args;
    }

    private void runMcQuiet(List<String> args) throws Exception {
        List<String> cmd = new ArrayList<>();
        cmd.add(cfg.mcBin);
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

    private void runMc(List<String> args) throws Exception {
        List<String> cmd = new ArrayList<>();
        cmd.add(cfg.mcBin);
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

    /** mirror 事件 source(绝对路径) → 相对任务源目录路径 */
    private static String relOf(String source, String srcDir) {
        String s = source.replaceAll("^/+|/+$", "");
        String d = srcDir.replaceAll("^/+|/+$", "");
        if (d.isEmpty()) return s;
        if (s.startsWith(d + "/")) {
            return s.substring(d.length() + 1);
        }
        int idx = s.lastIndexOf('/');
        return idx >= 0 ? s.substring(idx + 1) : s;
    }

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
