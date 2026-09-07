package com.dcp.module.execution.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@TableName("job_execution_log")
public class JobExecutionLog {
    @TableId(type = IdType.AUTO)
    private Long id;
    private Long executionId;
    private String logLevel;
    private String logContent;
    private LocalDateTime logTime;
}
