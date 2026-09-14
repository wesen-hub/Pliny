package com.dcp.module.outlet.model;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 快照分页(设计 §6.5 快照段实现)。
 *
 * <p>不变量:整段快照读的是**同一个 T0 视图**;同一 boundary 的页序列稳定(可中断重入)。
 * 分页游标按主键(绝不用 LIMIT/OFFSET),并带每页行数上限。
 */
@Data
public class SnapshotPage {

    private String streamId;
    /** 快照边界 T0(必须 ≥ changefeed.start_ts,且 ≥ GC safe point) */
    private long boundaryTso;
    private int epoch;
    /** 下一页令牌(为空表示拉完) */
    private String pageToken;
    /** 本页行(已按契约裁列/脱敏) */
    private List<Map<String, Object>> rows = new ArrayList<>();
    /** 已完成行数 / 预估总行数 */
    private long rowsExported;
    private Long rowsEstimate;
    private boolean done;
}
