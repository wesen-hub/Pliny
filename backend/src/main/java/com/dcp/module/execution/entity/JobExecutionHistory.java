package com.dcp.module.execution.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@TableName("job_execution_history")
public class JobExecutionHistory {
    @TableId(type = IdType.AUTO)
    private Long id;
    private Long jobId;
    /** MANUAL|CRON */
    private String triggerType;
    private String seaTunnelJobId;
    /** RUNNING|SUCCESS|FAILED|STOPPED|TIMEOUT */
    private String status;
    private LocalDateTime startTime;
    private LocalDateTime endTime;
    private Long durationMs;
    private Long sourceCount;
    private Long targetCount;
    private String errorMessage;
    private Integer retryCount;
    private LocalDateTime createTime;
}
