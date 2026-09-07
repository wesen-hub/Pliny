package com.dcp.module.alarm.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@TableName("alarm_config")
public class AlarmConfig {
    @TableId(type = IdType.AUTO)
    private Long id;
    private Long jobId;
    /** EMAIL|DINGTALK|WECHAT */
    private String alarmType;
    private String receiver;
    /** FAILED|TIMEOUT|SUCCESS */
    private String triggerEvent;
    private Integer enabled;
    private LocalDateTime createTime;
    private LocalDateTime updateTime;
}
