package com.dcp.module.job.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@TableName("job_definition")
public class JobDefinition {
    @TableId(type = IdType.AUTO)
    private Long id;
    private String jobName;
    private Long sourceDatasourceId;
    private Long targetDatasourceId;
    private String targetTableName;
    /** full|incremental|cdc */
    private String syncMode;
    private String incrementalField;
    private String cronExpr;
    private String misfirePolicy;
    private Integer enabled;
    /** READY|RUNNING|SUCCESS|FAILED|STOPPED */
    private String status;
    private Integer timeoutMinutes;
    private Integer parallelism;
    private Integer retryTimes;
    private Integer retryIntervalSeconds;
    private Integer alarmEnabled;
    private String description;
    private LocalDateTime lastExecutionTime;
    private String lastExecutionStatus;
    private Integer version;
    private String createBy;
    private String updateBy;
    private LocalDateTime createTime;
    private LocalDateTime updateTime;
    private Integer deleted;
}
