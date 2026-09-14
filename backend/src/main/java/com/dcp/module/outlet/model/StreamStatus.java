package com.dcp.module.outlet.model;

import lombok.Data;

/**
 * 流状态(设计 §6.9 接口 ① stream-status / 已由原型脚本 outlet/chg-status.sh 实测)。
 */
@Data
public class StreamStatus {

    private String streamId;
    private String changefeedId;
    /** 流代次 */
    private int epoch;
    /** 可消费的最早位点 = max(changefeed.start_ts, GC 可读起点) */
    private long earliest;
    /** 当前最新位点 */
    private long latest;
    /** 当前 watermark(可能为空:尚无水位) */
    private Long watermarkTs;
    /** 保留窗口(小时,决策 12:≥168) */
    private int retentionHours;
}
