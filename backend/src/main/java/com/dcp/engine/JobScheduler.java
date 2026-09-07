package com.dcp.engine;

import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.dcp.module.execution.entity.JobExecutionHistory;
import com.dcp.module.execution.entity.JobExecutionLog;
import com.dcp.module.execution.mapper.JobExecutionHistoryMapper;
import com.dcp.module.execution.mapper.JobExecutionLogMapper;
import com.dcp.module.job.entity.JobDefinition;
import com.dcp.module.job.mapper.JobDefinitionMapper;
import com.dcp.module.job.service.JobService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.support.CronExpression;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;

/**
 * 调度器（设计文档 §5.6）
 * 轻量实现：Spring 定时扫描启用任务的 cron 表达式触发；生产可替换 Quartz JDBC JobStore
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class JobScheduler {

    private final JobDefinitionMapper jobMapper;
    private final JobExecutionLogMapper logMapper;
    private final JobExecutionHistoryMapper historyMapper;
    private final JobService jobService;

    /** 每 30s 扫描一次 cron 任务（misfire 策略: DO_NOTHING，错过周期由下周期窗口覆盖） */
    @Scheduled(fixedDelay = 30_000)
    public void scanCronJobs() {
        try {
            List<JobDefinition> jobs = jobMapper.selectList(Wrappers.<JobDefinition>lambdaQuery()
                    .eq(JobDefinition::getEnabled, 1)
                    .isNotNull(JobDefinition::getCronExpr)
                    .ne(JobDefinition::getStatus, "RUNNING"));
            LocalDateTime now = LocalDateTime.now();
            for (JobDefinition job : jobs) {
                try {
                    CronExpression cron = CronExpression.parse(
                            com.dcp.module.job.service.JobService.normalizeCron(job.getCronExpr()));
                    LocalDateTime from = job.getLastExecutionTime() == null
                            ? job.getCreateTime() : job.getLastExecutionTime();
                    LocalDateTime next = cron.next(from);
                    if (next != null && !next.isAfter(now)) {
                        log.info("cron 触发任务: {} ({})", job.getJobName(), job.getCronExpr());
                        jobService.run(job.getId(), "CRON");
                    }
                } catch (Exception e) {
                    log.warn("cron 解析失败 job={} expr={}: {}", job.getJobName(), job.getCronExpr(), e.getMessage());
                }
            }
        } catch (Exception e) {
            log.error("调度扫描异常", e);
        }
    }

    /** 每日 03:00 清理：执行日志 30 天滚动，执行历史 180 天（§13.4） */
    @Scheduled(cron = "0 0 3 * * *")
    public void cleanup() {
        try {
            int logs = logMapper.delete(Wrappers.<JobExecutionLog>lambdaQuery()
                    .lt(JobExecutionLog::getLogTime, LocalDateTime.now().minusDays(30)));
            int history = historyMapper.delete(Wrappers.<JobExecutionHistory>lambdaQuery()
                    .lt(JobExecutionHistory::getStartTime, LocalDateTime.now().minusDays(180)));
            log.info("数据清理完成: 日志 {} 条, 历史 {} 条", logs, history);
        } catch (Exception e) {
            log.error("数据清理异常", e);
        }
    }
}
