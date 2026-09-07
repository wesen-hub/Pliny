package com.dcp.module.execution;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.dcp.common.ApiResponse;
import com.dcp.module.execution.entity.JobExecutionHistory;
import com.dcp.module.execution.entity.JobExecutionLog;
import com.dcp.module.execution.mapper.JobExecutionHistoryMapper;
import com.dcp.module.execution.mapper.JobExecutionLogMapper;
import com.dcp.module.job.entity.JobDefinition;
import com.dcp.module.job.mapper.JobDefinitionMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/executions")
@RequiredArgsConstructor
public class ExecutionController {

    private final JobExecutionHistoryMapper historyMapper;
    private final JobExecutionLogMapper logMapper;
    private final JobDefinitionMapper jobMapper;

    @GetMapping
    public ApiResponse<Map<String, Object>> page(@RequestParam(defaultValue = "1") int page,
                                                 @RequestParam(defaultValue = "10") int pageSize,
                                                 @RequestParam(required = false) Long jobId,
                                                 @RequestParam(required = false) String status,
                                                 @RequestParam(required = false) String startTime,
                                                 @RequestParam(required = false) String endTime) {
        LambdaQueryWrapper<JobExecutionHistory> qw = new LambdaQueryWrapper<>();
        qw.eq(jobId != null, JobExecutionHistory::getJobId, jobId)
          .eq(status != null && !status.isBlank(), JobExecutionHistory::getStatus, status)
          .ge(startTime != null && !startTime.isBlank(), JobExecutionHistory::getStartTime, startTime)
          .le(endTime != null && !endTime.isBlank(), JobExecutionHistory::getStartTime, endTime)
          .orderByDesc(JobExecutionHistory::getId);
        Page<JobExecutionHistory> p = historyMapper.selectPage(Page.of(page, pageSize), qw);
        // 补充任务名
        List<Map<String, Object>> list = p.getRecords().stream().map(h -> {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("id", h.getId());
            m.put("jobId", h.getJobId());
            JobDefinition job = jobMapper.selectById(h.getJobId());
            m.put("jobName", job == null ? "(已删除)" : job.getJobName());
            m.put("triggerType", h.getTriggerType());
            m.put("seaTunnelJobId", h.getSeaTunnelJobId());
            m.put("status", h.getStatus());
            m.put("startTime", h.getStartTime());
            m.put("endTime", h.getEndTime());
            m.put("durationMs", h.getDurationMs());
            m.put("sourceCount", h.getSourceCount());
            m.put("targetCount", h.getTargetCount());
            m.put("errorMessage", h.getErrorMessage());
            m.put("retryCount", h.getRetryCount());
            return m;
        }).toList();
        return ApiResponse.ok(Map.of("total", p.getTotal(), "list", list));
    }

    @GetMapping("/{id}")
    public ApiResponse<Map<String, Object>> detail(@PathVariable Long id) {
        JobExecutionHistory h = historyMapper.selectById(id);
        if (h == null) {
            return ApiResponse.error(1004, "执行记录不存在");
        }
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("history", h);
        JobDefinition job = jobMapper.selectById(h.getJobId());
        m.put("jobName", job == null ? null : job.getJobName());
        return ApiResponse.ok(m);
    }

    @GetMapping("/{id}/logs")
    public ApiResponse<List<JobExecutionLog>> logs(@PathVariable Long id) {
        return ApiResponse.ok(logMapper.selectList(new LambdaQueryWrapper<JobExecutionLog>()
                .eq(JobExecutionLog::getExecutionId, id)
                .orderByAsc(JobExecutionLog::getId)));
    }
}
