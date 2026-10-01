package com.dcp.module.outlet.batch;

import lombok.AllArgsConstructor;
import lombok.Data;

/** 一次批作业的结果(供调度侧与观测使用)。 */
@Data
@AllArgsConstructor
public class BatchJobOutcome {

    private String manifestId;
    private String jobType;
    /** bootstrap:T0;增量批:区间 [lo,hi] */
    private Long boundaryTso;
    private Long loTso;
    private Long hiTso;
    private String stagingPath;
    private long rowsWritten;
    private boolean published;
    /** 是否复用了已发布产物(重跑幂等) */
    private boolean reused;
    /** 位点是否已收口(发布后提交) */
    private long offsetSubmitted;
    private String note;
}
