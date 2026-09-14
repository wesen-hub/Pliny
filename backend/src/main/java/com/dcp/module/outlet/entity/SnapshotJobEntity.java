package com.dcp.module.outlet.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDateTime;

/**
 * 快照任务(表:snapshot_job;DDL 见《变更流出口-表结构DDL-2026-09-14.sql》)。
 *
 * <p>作用(设计 §6.5):把"大表快照"任务化 —— 记录 boundary、游标、页令牌、TTL,
 * 使快照**可中断重入**且**同一 boundary 页序列稳定**;超过 TTL 即作废(必须用新 T0 重来)。
 */
@Data
@TableName("snapshot_job")
public class SnapshotJobEntity {

    public static final String STATUS_READY = "READY";
    public static final String STATUS_PAGING = "PAGING";
    public static final String STATUS_DONE = "DONE";
    public static final String STATUS_EXPIRED = "EXPIRED";
    public static final String STATUS_FAILED = "FAILED";

    @TableId(type = IdType.AUTO)
    private Long id;
    private String consumerId;
    private String streamId;
    private String contractId;
    /** ★ 快照边界(= 流的起点);必须 ≥ changefeed.start_ts */
    private Long boundaryTso;
    /** 分页游标(复合主键用元组序列化) */
    private String cursorPk;
    /** 对外分页令牌 */
    private String pageToken;
    private String status;
    private Long rowsEstimate;
    private Long rowsExported;
    /** 过期时间;超过即作废 */
    private LocalDateTime expireAt;
    private LocalDateTime createTime;
    private LocalDateTime updateTime;
}
