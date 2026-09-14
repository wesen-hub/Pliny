package com.dcp.module.outlet.model;

import lombok.Data;

/**
 * 消费方服务账号(决策 5:统一新建,不复用 kafka_app;kafka_app 只是它的一种凭证来源)。
 * 表结构见《变更流出口-表结构DDL-2026-09-14.sql》consumer 表。
 */
@Data
public class ConsumerAccount {

    public static final String TYPE_SUBSCRIBE = "subscribe";
    public static final String TYPE_TAG = "tag";
    public static final String TYPE_CLEAN = "clean";
    public static final String TYPE_LAKE = "lake";

    public static final String EXEC_STREAM = "stream";
    public static final String EXEC_BATCH = "batch";

    private String consumerId;
    private String name;
    private String consumerType;
    /** 执行器:stream(A 流式) | batch(B Spark 批) */
    private String executor = EXEC_STREAM;
    private String status = "active";
    /** 通道:api(对外唯一) | internal | kafka(仅可信) */
    private String channel = "api";
    /** 绑定的平台主体/角色 id —— "契约 ∩ 主体" 的主体侧 */
    private String subjectId;

    private int quotaQps = 10;
    private long quotaRowsPerDay = 1_000_000L;
    private int quotaConcurrent = 4;
}
