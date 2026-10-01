package com.dcp.module.outlet.entity;

import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDateTime;

/**
 * 物化清单(表:snapshot_manifest;DDL 见《变更流出口-表结构DDL-2026-09-14.sql》§6)。
 *
 * <p>执行器 B 的**原子发布凭据**:只有 manifest 变成 {@code PUBLISHED},批产物才对消费方可见;
 * **位点只能在发布之后提交**(设计 §6.3 技术要点 4/5、§6.4.1 衔接契约 3)。
 */
@Data
@TableName("snapshot_manifest")
public class SnapshotManifestEntity {

    public static final String STATUS_RUNNING = "RUNNING";
    public static final String STATUS_PUBLISHED = "PUBLISHED";
    public static final String STATUS_FAILED = "FAILED";
    public static final String STATUS_SUPERSEDED = "SUPERSEDED";

    public static final String TYPE_BOOTSTRAP = "bootstrap";
    public static final String TYPE_INCR_BATCH = "incr_batch";
    public static final String TYPE_MIXED = "mixed";

    @TableId
    private String manifestId;
    private String consumerId;
    private String streamId;
    /** bootstrap | incr_batch | mixed */
    private String jobType;
    /** bootstrap:T0;增量批:区间右端(hi) */
    private Long boundaryTso;
    private Long loTso;
    private Long hiTso;
    /** 不可变 staging 路径/表(按 boundary 版本化) */
    private String stagingPath;
    private Long rowsWritten;
    private String checksum;
    private String status;
    private Integer epoch;
    /** 原子发布时间;此前产物不可见 */
    private LocalDateTime publishTime;
    private LocalDateTime createTime;
}
