package com.dcp.filesync.exec;

import com.dcp.filesync.core.FilesyncTaskReader;
import com.dcp.filesync.core.RcloneEngine;
import com.dcp.filesync.core.RunRecorder;
import com.dcp.scheduler.exec.PartitionExecutor;
import com.dcp.scheduler.model.ExecutorSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 文件同步分区执行器(一级 SPI, 一次性任务): start 后于后台线程跑一轮 rclone,
 * 完成/失败即上报 done(快照 stopped=true), 框架(F1 改造)识别后置 SUCCEEDED 并回收分区。
 *
 * <pre>
 *   ctx.bizMeta = { taskId, runId } —— backend 提交 TaskSpec 前已 insertRun 创建 run 行,
 *   凭证/任务定义由执行器经元库读取(FilesyncTaskReader), 不进 bizMeta/etcd。
 *   判死重派: 新执行器对同一 runId 重跑, rclone 比较式幂等(§7.4), start 时清旧明细。
 *   四计数器语义: processedCount=文件事件数(ok+del), committedOffset 同, errorCount=failN。
 * </pre>
 */
public class FilesyncExecutor implements PartitionExecutor {

    private static final Logger log = LoggerFactory.getLogger(FilesyncExecutor.class);

    private final FilesyncTaskReader reader;
    private final RunRecorder recorder;
    private final RcloneEngine engine;

    private final AtomicReference<Boolean> cancel = new AtomicReference<>(Boolean.FALSE);
    private final AtomicBoolean done = new AtomicBoolean(false);
    private final AtomicBoolean failed = new AtomicBoolean(false);
    private final AtomicLong processed = new AtomicLong();
    private final AtomicLong errors = new AtomicLong();
    private volatile Thread worker;

    private volatile String taskId = "?";
    private volatile String runId = "?";

    public FilesyncExecutor(FilesyncTaskReader reader, RunRecorder recorder, RcloneEngine engine) {
        this.reader = reader;
        this.recorder = recorder;
        this.engine = engine;
    }

    @Override
    public void start(ExecutorContext ctx) {
        this.taskId = ctx.bizMeta == null ? "?" : String.valueOf(ctx.bizMeta.getOrDefault("taskId", "?"));
        this.runId = ctx.bizMeta == null ? "?" : String.valueOf(ctx.bizMeta.getOrDefault("runId", "?"));
        if ("?".equals(taskId) || "?".equals(runId)) {
            log.error("[filesync] bizMeta 缺 taskId/runId, 无法执行");
            failed.set(true);
            done.set(true);
            return;
        }
        worker = new Thread(() -> run(), "filesync-" + taskId + "-" + runId);
        worker.setDaemon(true);
        worker.start();
    }

    /** 一轮 rclone 执行(读任务 → 清明细 → 跑 → 收尾);任何结局均 done。 */
    private void run() {
        long tid = Long.parseLong(taskId);
        long rid = Long.parseLong(runId);
        Map<String, Object> summary = new LinkedHashMap<>();
        try {
            FilesyncTaskReader.FilesyncTask task = reader.load(tid);
            if (task == null) {
                recorder.finishRun(rid, "FAILED", summary, "任务不存在: " + tid, tid, false);
                failed.set(true);
                return;
            }
            if (!task.isPlatform()) {
                recorder.finishRun(rid, "FAILED", summary, "任务执行端为 agent, 平台侧不执行", tid, false);
                failed.set(true);
                return;
            }
            recorder.resetRunFiles(rid);   // 判死重派幂等
            RcloneEngine.Result r = engine.run(task, rid, cancel, new RcloneEngine.RunListener() {
                @Override
                public void onFile(String relPath, String status, long size, String error) {
                    recorder.insertRunFile(rid, relPath, status, size, error);
                    if ("error".equals(status)) {
                        errors.incrementAndGet();
                    } else {
                        processed.incrementAndGet();
                    }
                }
            });
            String status = r.failN > 0 && r.okN == 0 && r.delN == 0 ? "FAILED" : "COMPLETED";
            summary.put("op", r.stats.containsKey("op") ? r.stats.get("op") : "copy");
            summary.put("transferred", r.okN);
            summary.put("deleted", r.delN);
            summary.put("bytes", r.bytes);
            summary.put("failed", r.failN);
            summary.put("checks", r.stats.get("checks"));
            summary.put("listed", r.stats.get("listed"));
            summary.put("durationMs", r.stats.get("durationMs"));
            recorder.finishRun(rid, status, summary, r.errorTail, tid, r.full);
            if ("FAILED".equals(status)) {
                failed.set(true);
                log.warn("[filesync] run#{} task#{} 失败: {}", rid, tid, r.errorTail);
            } else {
                log.info("[filesync] run#{} task#{} 完成: 传 {} 删 {} 字节 {} ({}ms)",
                        rid, tid, r.okN, r.delN, r.bytes, r.stats.get("durationMs"));
            }
        } catch (Exception e) {
            log.error("[filesync] run#{} task#{} 执行异常", rid, tid, e);
            recorder.finishRun(rid, "FAILED", summary, String.valueOf(e.getMessage()), tid, false);
            failed.set(true);
        } finally {
            done.set(true);
        }
    }

    @Override
    public void stop() {
        cancel.set(true);      // 引擎轮询到后销毁 rclone 进程
        Thread t = worker;
        if (t != null) t.interrupt();
    }

    @Override
    public ExecutorSnapshot metrics() {
        return ExecutorSnapshot.of(taskId, 0, processed.get(), processed.get(),
                errors.get(), 0, done.get());
    }
}
