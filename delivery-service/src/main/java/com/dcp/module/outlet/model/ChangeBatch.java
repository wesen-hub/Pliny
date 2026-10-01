package com.dcp.module.outlet.model;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

/**
 * 一批变更(设计 §6.9 接口 changes/slice)。
 *
 * <p>{@code nextPosition} 以"**扫到哪**"为准(而不是"处理到哪"),
 * 因此同一位点重复拉取拿到的事件序列是稳定的(可重放、可对账)。
 */
@Data
public class ChangeBatch {

    private String streamId;
    private int epoch;
    /** 本次扫描区间左端(不含) */
    private long scannedFrom;
    /** 本次扫描区间右端(含) */
    private Long scannedTo;
    /** 是否还有更多 */
    private boolean hasMore;
    /** 下次调用的位点(不透明串) */
    private String nextPosition;
    /** WATERMARK(设计 §6.7):所有 commitTs < watermarkTs 的事件均已发出 */
    private Long watermarkTs;
    private List<ChangeEvent> events = new ArrayList<>();
}
