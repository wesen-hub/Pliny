package com.dcp.module.job.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@TableName("sync_state")
public class SyncState {
    @TableId(type = IdType.AUTO)
    private Long jobId;
    private String lastSyncValue;
    /** LONG|DATETIME|STRING */
    private String valueType;
    private LocalDateTime updateTime;
}
