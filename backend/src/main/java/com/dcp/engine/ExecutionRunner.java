package com.dcp.engine;

import com.dcp.common.BizException;
import com.dcp.common.ErrorCode;
import com.dcp.module.alarm.AlarmService;
import com.dcp.module.datasource.DatasourceService;
import com.dcp.module.datasource.entity.DatasourceConfig;
import com.dcp.module.dict.DictService;
import com.dcp.module.execution.entity.JobExecutionHistory;
import com.dcp.module.execution.entity.JobExecutionLog;
import com.dcp.module.execution.mapper.JobExecutionHistoryMapper;
import com.dcp.module.execution.mapper.JobExecutionLogMapper;
import com.dcp.module.job.entity.FieldMapping;
import com.dcp.module.job.entity.JobDefinition;
import com.dcp.module.job.entity.SyncState;
import com.dcp.module.job.mapper.JobDefinitionMapper;
import com.dcp.module.job.mapper.SyncStateMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 任务执行编排（设计文档 §4.1 主流程 + §4.5 失败重试）
 * 调度器/手动触发 -> 加锁 -> 提交引擎 -> 轮询/回写 -> 位点推进 -> 告警
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ExecutionRunner {

    private final SyncEngine engine;
    private final DatasourceService datasourceService;
    private final DictService dictService;
    private final JobDefinitionMapper jobMapper;
    private final SyncStateMapper syncStateMapper;
    private final JobExecutionHistoryMapper historyMapper;
    private final JobExecutionLogMapper logMapper;
    private final AlarmService alarmService;
    private final ThreadPoolTaskExecutor engineExecutor;

    /** executionId -> 运行中的任务（用于停止） */
    private final ConcurrentHashMap<Long, RunningTask> running = new ConcurrentHashMap<>();

    public static class RunningTask {
        final AtomicBoolean stopFlag = new AtomicBoolean(false);
        volatile Future<?> future;
    }

    /** 启动异步执行（调用方已完成 RUNNING 状态锁） */
    public void startAsync(Long executionId, JobDefinition job,
                           com.dcp.module.job.entity.ExtractConfig extract,
                           List<FieldMapping> mappings) {
        RunningTask task = new RunningTask();
        running.put(executionId, task);
        Future<?> future = engineExecutor.submit(() -> {
            try {
                executeWithRetry(executionId, job, extract, mappings, task);
            } catch (Throwable e) {
                log.error("执行任务异常 job={}", job.getJobName(), e);
                fail(executionId, job, "系统异常: " + e.getMessage(), "FAILED");
            } finally {
                running.remove(executionId);
            }
        });
        task.future = future;
    }

    private void executeWithRetry(Long executionId, JobDefinition job,
                                  com.dcp.module.job.entity.ExtractConfig extract,
                                  List<FieldMapping> mappings, RunningTask task) {
        Map<String, Map<String, String>> dictMaps = dictService.loadDictMaps(
                mappings.stream().filter(m -> "dictionary".equals(m.getTransformType()))
                        .map(FieldMapping::getTransformRule).distinct().toList());
        SyncState syncState = syncStateMapper.selectById(job.getId());
        DatasourceConfig src = datasourceService.get(job.getSourceDatasourceId());
        DatasourceConfig tgt = datasourceService.get(job.getTargetDatasourceId());

        int maxAttempts = Math.max(1, job.getRetryTimes() == null ? 0 : job.getRetryTimes()) + 1;
        int baseInterval = Math.max(5, job.getRetryIntervalSeconds() == null ? 60 : job.getRetryIntervalSeconds());
        boolean retryable = true;

        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            if (task.stopFlag.get()) {
                stop(executionId, job);
                return;
            }
            try {
                info(executionId, "第 " + attempt + "/" + maxAttempts + " 次执行开始");
                JobRunContext ctx = JobRunContext.builder()
                        .job(job)
                        .extract(extract)
                        .mappings(mappings)
                        .sourceConn(datasourceService.connectionInfo(src))
                        .targetConn(datasourceService.connectionInfo(tgt))
                        .syncState(syncState)
                        .dictMaps(dictMaps)
                        .stopFlag(task.stopFlag)
                        .executionId(executionId)
                        .attempt(attempt)
                        .timeoutMillis(job.getTimeoutMinutes() * 60_000L)
                        .infoLog(msg -> info(executionId, msg))
                        .errorLog(msg -> error(executionId, msg))
                        .build();
                EngineResult result = engine.execute(ctx);
                // 成功
                if (attempt > 1) {
                    historyMapper.update(null, com.baomidou.mybatisplus.core.toolkit.Wrappers
                            .<JobExecutionHistory>lambdaUpdate().eq(JobExecutionHistory::getId, executionId)
                            .set(JobExecutionHistory::getRetryCount, attempt - 1));
                }
                success(executionId, job, result, syncState);
                return;
            } catch (InterruptedException e) {
                stop(executionId, job);
                return;
            } catch (BizException be) {
                retryable = isRetryable(be.getMessage());
                error(executionId, "执行失败: " + be.getMessage());
                if (!retryable || attempt >= maxAttempts) {
                    fail(executionId, job, be.getMessage(), "FAILED");
                    return;
                }
            } catch (Exception e) {
                retryable = true;
                error(executionId, "执行异常: " + e.getMessage());
                if (attempt >= maxAttempts) {
                    fail(executionId, job, e.getMessage(), "FAILED");
                    return;
                }
            }
            // 指数退避重试
            long backoff = Math.min(600_000L, baseInterval * 1000L * (1L << (attempt - 1)));
            error(executionId, "可重试失败, " + backoff / 1000 + "s 后重试");
            try {
                Thread.sleep(backoff);
            } catch (InterruptedException ie) {
                stop(executionId, job);
                return;
            }
        }
    }

    /** 连接类错误可重试，配置/表达式类错误不可重试 */
    private boolean isRetryable(String msg) {
        if (msg == null) return true;
        String m = msg.toLowerCase();
        if (m.contains("syntax error") || m.contains("unknown column") || m.contains("unknown table")
                || m.contains("编译") || m.contains("配置") || m.contains("必须以 select")) {
            return false;
        }
        return true;
    }

    private void success(Long executionId, JobDefinition job, EngineResult result, SyncState syncState) {
        if (job.getSyncMode().equalsIgnoreCase("incremental") && result.getWindowMax() != null && !result.isNoData()) {
            advanceOffset(job, syncState, result.getWindowMax());
        }
        finish(executionId, job, "SUCCESS", null, result.getSourceCount(), result.getTargetCount());
        if (job.getAlarmEnabled() != null && job.getAlarmEnabled() == 1) {
            alarmService.notify(job, "SUCCESS", executionId, "任务执行成功, 同步 " + result.getTargetCount() + " 行");
        }
    }

    private void fail(Long executionId, JobDefinition job, String error, String status) {
        error(executionId, "任务最终状态: " + status + (error == null ? "" : " | " + error));
        finish(executionId, job, status, error, null, null);
        if (job.getAlarmEnabled() != null && job.getAlarmEnabled() == 1) {
            alarmService.notify(job, "FAILED", executionId, "任务执行失败: " + error);
        }
    }

    private void stop(Long executionId, JobDefinition job) {
        info(executionId, "任务被手动停止");
        finish(executionId, job, "STOPPED", "任务被手动停止", null, null);
    }

    private void finish(Long executionId, JobDefinition job, String status, String error,
                        Long sourceCount, Long targetCount) {
        JobExecutionHistory h = historyMapper.selectById(executionId);
        if (h != null) {
            h.setStatus(status);
            h.setEndTime(LocalDateTime.now());
            h.setDurationMs(java.time.Duration.between(h.getStartTime(), h.getEndTime()).toMillis());
            h.setSourceCount(sourceCount);
            h.setTargetCount(targetCount);
            h.setErrorMessage(error == null || error.length() > 2000 ? (error == null ? null : error.substring(0, 2000)) : error);
            historyMapper.updateById(h);
        }
        JobDefinition update = new JobDefinition();
        update.setId(job.getId());
        update.setStatus(status);
        update.setLastExecutionStatus(status);
        update.setLastExecutionTime(LocalDateTime.now());
        jobMapper.updateById(update);
    }

    /** 位点推进（K3：只允许任务成功后推进） */
    private void advanceOffset(JobDefinition job, SyncState syncState, String windowMax) {
        SyncState exist = syncStateMapper.selectById(job.getId());
        if (exist == null) {
            SyncState s = new SyncState();
            s.setJobId(job.getId());
            s.setLastSyncValue(windowMax);
            s.setValueType(detectValueType(windowMax));
            syncStateMapper.insert(s);
        } else {
            exist.setLastSyncValue(windowMax);
            if (exist.getValueType() == null) {
                exist.setValueType(detectValueType(windowMax));
            }
            syncStateMapper.updateById(exist);
        }
    }

    private String detectValueType(String v) {
        try {
            Long.parseLong(v);
            return "LONG";
        } catch (Exception e) {
            return "DATETIME";
        }
    }

    // ---------- 日志 ----------

    public void info(Long executionId, String msg) {
        log.info("[execution {}] {}", executionId, msg);
        insertLog(executionId, "INFO", msg);
    }

    public void error(Long executionId, String msg) {
        log.warn("[execution {}] {}", executionId, msg);
        insertLog(executionId, "ERROR", msg);
    }

    private void insertLog(Long executionId, String level, String msg) {
        try {
            JobExecutionLog l = new JobExecutionLog();
            l.setExecutionId(executionId);
            l.setLogLevel(level);
            l.setLogContent(msg.length() > 2000 ? msg.substring(0, 2000) : msg);
            l.setLogTime(LocalDateTime.now());
            logMapper.insert(l);
        } catch (Exception ignored) {
        }
    }

    // ---------- 停止对外 ----------

    public void stopExecution(Long executionId) {
        RunningTask task = running.get(executionId);
        if (task == null) {
            throw new BizException(ErrorCode.ENGINE_STOP_FAIL, "任务不在运行中");
        }
        task.stopFlag.set(true);
        if (task.future != null) {
            task.future.cancel(true);
        }
    }

    public boolean isRunning(Long executionId) {
        return running.containsKey(executionId);
    }

    /** 平台启动对账：清理引擎侧运行但平台无记录的孤儿执行（§6.1） */
    public void reconcileOrphans() {
        List<JobExecutionHistory> orphans = historyMapper.selectList(
                com.baomidou.mybatisplus.core.toolkit.Wrappers.<JobExecutionHistory>lambdaQuery()
                        .eq(JobExecutionHistory::getStatus, "RUNNING"));
        for (JobExecutionHistory h : orphans) {
            if (!running.containsKey(h.getId())) {
                h.setStatus("FAILED");
                h.setEndTime(LocalDateTime.now());
                h.setErrorMessage("进程重启, 执行状态悬空, 已标记失败");
                historyMapper.updateById(h);
                log.warn("孤儿执行标记失败: executionId={}", h.getId());
            }
        }
    }
}
