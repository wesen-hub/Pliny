package com.dcp.module.outlet.entity;

import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDateTime;

/**
 * 消费方服务账号(表:consumer;DDL 见《变更流出口-表结构DDL-2026-09-14.sql》)。
 * 决策 5:统一新建,不复用 kafka_app。
 */
@Data
@TableName("consumer")
public class OutletConsumerEntity {

    @TableId
    private String consumerId;
    private String name;
    private String consumerType;
    private String owner;
    private String status;
    private String channel;
    private String executor;
    private String subjectId;
    private Integer quotaQps;
    private Long quotaRowsDay;
    private Integer quotaConn;
    private LocalDateTime createTime;
    private LocalDateTime updateTime;
}
