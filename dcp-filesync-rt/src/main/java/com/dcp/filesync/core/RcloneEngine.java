package com.dcp.filesync.core;

import com.dcp.filesync.util.Strategy;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * rclone 引擎(RcloneRunner 迁出, 方案 N14): 生成临时 rclone.conf(命名 remote dcpsrc/dcpdst,
 * 免连接串冒号/全局 flag 冲突) → rclone sync|copy → --use-json-log 逐行解析
 * (Copied/Deleted/Failed) → 回调节点(执行器落 run_file); 末行 stats JSON → run.summary。
 *
 * <p>与迁出前 backend FileSyncTaskService.runOnce 同构, 差异: 不直接落库, 经 {@link RunListener}
 * 回调由执行器统一处理(重派幂等/收尾/计数器)。
 */
public class RcloneEngine {

    private static final Logger log = LoggerFactory.getLogger(RcloneEngine.class);
    private static final ObjectMapper OM = new ObjectMapper();

    /** 命名 remote(写入临时 rclone.conf): 避免 --s3-* 全局 flag 冲突与连接串冒号截断 */
    private static final String REMOTE_SRC = "dcpsrc";
    private static final String REMOTE_DST = "dcpdst";
    /** 单轮执行上限 */
    private static final int RUN_TIMEOUT_SEC = 1800;

    /** 执行结果(统计 + 是否全量轮 + 状态/错误)。 */
    public static final class Result {
        public int okN;
        public int delN;
        public int failN;
        public long bytes;
        public String errorTail;
        public Map<String, Object> stats;
        public boolean full;
    }

    /** 执行过程回调(逐文件事件, 执行器落 run_file)。 */
    public interface RunListener {
        void onFile(String relPath, String status, long size, String error);
    }

    /**
     * 执行一轮(阻塞, 由调用方放后台线程)。
     *
     * @param task    任务定义(reader 装配)
     * @param runId   平台 run 号(仅日志用)
     * @param cancel  取消信号载体(执行器 stop 时置位, 引擎轮询)
     * @param listener 逐文件事件回调
     */
    public Result run(FilesyncTaskReader.FilesyncTask task, long runId,
                      AtomicReference<Boolean> cancel, RunListener listener) {
        Result r = new Result();
        long t0 = System.currentTimeMillis();
        Map<String, Object> st = task.strategy();
        boolean full = Strategy.isFullRound(task.syncMode, task.baselineDone);
        r.full = full;
        String op = Strategy.engineOp(full, st);
        Path conf = null;
        Process p = null;
        try {
            conf = writeRcloneConf(task.taskId, task.source, task.target);
            List<String> args = buildRcloneArgs(conf, task.source, task.target, op, st);
            log.info("[filesync] run#{} task#{} mode={} op={} {} → {}",
                    runId, task.taskId, task.syncMode, op, srcRemote(task.source), dstRemote(task.target));
            p = new ProcessBuilder(args).redirectErrorStream(true).start();
            try (BufferedReader br = new BufferedReader(
                    new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = br.readLine()) != null) {
                    if (Boolean.TRUE.equals(cancel.get())) {
                        p.destroyForcibly();
                        r.errorTail = "执行器被框架停止(任务迁移/回收)";
                        r.failN++;
                        return r;
                    }
                    if (line.isBlank()) continue;
                    JsonNode n;
                    try {
                        n = OM.readTree(line);
                    } catch (Exception e) {
                        continue;
                    }
                    if (!n.isObject()) continue;
                    if (n.has("stats")) {   // rclone 周期性/收尾 stats → run.summary
                        r.stats = OM.convertValue(n.get("stats"), LinkedHashMap.class);
                        continue;
                    }
                    String level = n.path("level").asText();
                    String msg = n.path("msg").asText("");
                    String object = n.path("object").asText("");
                    if (msg.startsWith("Config file")) continue;          // rclone.conf 缺失噪声
                    if ("error".equals(level)) {
                        if (msg.startsWith("Attempt ")) { r.errorTail = msg; continue; }   // 重试汇总, 非文件级
                        // 文件级错误才有 *Object 的 objectType(目录/桶级错误只进 errorTail)
                        if (n.path("objectType").asText("").endsWith(".Object")) {
                            r.failN++;
                            listener.onFile(object.isEmpty() ? "-" : object, "error", 0,
                                    Strategy.truncate(msg, 1900));
                        }
                        r.errorTail = msg;
                    } else if (msg.startsWith("Copied")) {                 // Copied (new) / (replaced existing)
                        long sz = n.path("size").asLong(0);
                        listener.onFile(object.isEmpty() ? "-" : object, "success", sz, null);
                        r.okN++;
                        r.bytes += sz;
                    } else if ("Deleted".equals(msg)) {                    // 仅全量(sync)会删目标多余
                        listener.onFile(object.isEmpty() ? "-" : object, "deleted", 0, null);
                        r.delN++;
                    }
                }
            }
            int code;
            if (!p.waitFor(RUN_TIMEOUT_SEC, TimeUnit.SECONDS)) {
                p.destroyForcibly();
                code = -1;
                if (r.errorTail == null) r.errorTail = "rclone 执行超时(" + RUN_TIMEOUT_SEC + "s)";
            } else {
                code = p.exitValue();
            }
            if (r.stats != null && r.stats.get("errors") instanceof Number se && se.longValue() > 0) {
                r.failN = (int) se.longValue();      // stats.errors 为准(含未落 run_file 的错误)
            }
            if (code != 0 && r.okN == 0 && r.delN == 0) {
                r.failN = Math.max(r.failN, 1);
                if (r.errorTail == null) r.errorTail = "rclone 退出码 " + code;
            }
        } catch (Exception e) {
            r.failN++;
            r.errorTail = e.getMessage();
        } finally {
            if (conf != null) {
                try {
                    Files.deleteIfExists(conf);   // 临时 rclone.conf 含凭证, 轮次结束即删
                } catch (Exception ignored) {
                }
            }
        }
        if (r.stats == null) r.stats = Map.of();
        r.stats = new LinkedHashMap<>(r.stats); // 只读 Map 由调用方继续增补 summary 字段
        r.stats.put("durationMs", System.currentTimeMillis() - t0);
        return r;
    }

    // ────────────────────────── 命令构造(迁自 backend) ──────────────────────────

    /** 拼 rclone 参数(public 供单测直验): op 由 engineOp 决定(全量=sync 镜像 / 增量=copy), 策略直模 rclone flag */
    public List<String> buildRcloneArgs(Path conf, Map<String, String> src, Map<String, String> dst,
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
        for (Object inc : Strategy.list(st.get("includes"))) {
            args.add("--include");
            args.add(String.valueOf(inc));
        }
        for (Object ex : Strategy.list(st.get("excludes"))) {
            args.add("--exclude");
            args.add(String.valueOf(ex));
        }
        addOpt(args, st, "minAge", "--min-age");
        addOpt(args, st, "maxAge", "--max-age");
        addOpt(args, st, "minSize", "--min-size");
        addOpt(args, st, "maxSize", "--max-size");
        // 性能/可靠性
        args.add("--transfers");
        args.add(String.valueOf(Strategy.intOr(st.get("transfers"), 8)));
        args.add("--checkers");
        args.add(String.valueOf(Strategy.intOr(st.get("checkers"), 8)));
        addOpt(args, st, "bwlimit", "--bwlimit");
        args.add("--retries");
        args.add(String.valueOf(Strategy.intOr(st.get("retries"), 3)));
        args.add("--low-level-retries");
        args.add(String.valueOf(Strategy.intOr(st.get("lowLevelRetries"), 10)));
        if (Strategy.truthy(st.get("dryRun"))) args.add("--dry-run");
        // 记账必需的日志口径(固定): 逐行 JSON + 每秒 stats
        args.add("--use-json-log");
        args.add("--log-level");
        args.add("INFO");
        args.add("--stats");
        args.add("1s");
        return args;
    }

    /** 生成临时 rclone.conf(命名 remote dcpsrc/dcpdst; 含明文凭证 → 轮次结束即删) */
    Path writeRcloneConf(long taskId, Map<String, String> src, Map<String, String> dst) throws Exception {
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

    /** 源侧 rclone 路径(public 供单测直验): local → 容器内绝对路径(不经 remote); s3 → dcpsrc:bucket/path */
    public static String srcRemote(Map<String, String> src) {
        String path = Strategy.strip(src.get("path") == null ? "" : src.get("path"));
        if ("local".equals(src.get("type"))) {
            String root = src.get("root") == null ? "" : src.get("root");   // 根不能 strip(绝对路径)
            if (path.isEmpty()) return root;
            return (root.endsWith("/") ? root : root + "/") + path;
        }
        String bucket = src.get("bucket") == null ? "" : src.get("bucket");
        return REMOTE_SRC + ":" + bucket + (path.isEmpty() ? "" : "/" + path);
    }

    /** 目标侧 rclone 路径(public 供单测直验): dcpdst:bucket/prefix */
    public static String dstRemote(Map<String, String> dst) {
        String bucket = dst.get("bucket") == null ? "" : dst.get("bucket");
        String prefix = Strategy.strip(dst.get("prefix") == null ? "" : dst.get("prefix"));
        return REMOTE_DST + ":" + bucket + (prefix.isEmpty() ? "" : "/" + prefix);
    }

    private static void addOpt(List<String> args, Map<String, Object> st, String key, String flag) {
        Object v = st.get(key);
        if (v == null || String.valueOf(v).isBlank()) return;
        args.add(flag);
        args.add(String.valueOf(v));
    }
}
