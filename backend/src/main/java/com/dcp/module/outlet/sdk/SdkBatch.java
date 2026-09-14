package com.dcp.module.outlet.sdk;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * SDK 交给消费方 handler 的批次(设计 §6.1:消费方**感知不到**快照段与增量段的差异)。
 *
 * <p>两段的差异已在 SDK 内部消化:快照段是 {@code rows}(可并行、可重入),增量段是 {@code events}(有序、按位点)。
 */
public class SdkBatch {

    public enum Kind { SNAPSHOT, CHANGE }

    private Kind kind;
    private String streamId;
    private int epoch;
    private List<Map<String, Object>> rows = new ArrayList<>();
    private List<com.dcp.module.outlet.model.ChangeEvent> events = new ArrayList<>();
    /** 本批已推进到的位点(TSO) */
    private long lastTso;
    /** 本批的不透明位点串(提交用) */
    private String nextPosition;
    /** WATERMARK(所有 commitTs < watermarkTs 的事件均已发出) */
    private Long watermarkTs;
    private boolean hasMore;
    /** 该流的快照段是否已结束(转入常驻增量) */
    private boolean snapshotDone;

    public Kind getKind() {
        return kind;
    }

    public void setKind(Kind kind) {
        this.kind = kind;
    }

    public String getStreamId() {
        return streamId;
    }

    public void setStreamId(String streamId) {
        this.streamId = streamId;
    }

    public int getEpoch() {
        return epoch;
    }

    public void setEpoch(int epoch) {
        this.epoch = epoch;
    }

    public List<Map<String, Object>> getRows() {
        return rows;
    }

    public void setRows(List<Map<String, Object>> rows) {
        this.rows = rows;
    }

    public List<com.dcp.module.outlet.model.ChangeEvent> getEvents() {
        return events;
    }

    public void setEvents(List<com.dcp.module.outlet.model.ChangeEvent> events) {
        this.events = events;
    }

    public long getLastTso() {
        return lastTso;
    }

    public void setLastTso(long lastTso) {
        this.lastTso = lastTso;
    }

    public String getNextPosition() {
        return nextPosition;
    }

    public void setNextPosition(String nextPosition) {
        this.nextPosition = nextPosition;
    }

    public Long getWatermarkTs() {
        return watermarkTs;
    }

    public void setWatermarkTs(Long watermarkTs) {
        this.watermarkTs = watermarkTs;
    }

    public boolean isHasMore() {
        return hasMore;
    }

    public void setHasMore(boolean hasMore) {
        this.hasMore = hasMore;
    }

    public boolean isSnapshotDone() {
        return snapshotDone;
    }

    public void setSnapshotDone(boolean snapshotDone) {
        this.snapshotDone = snapshotDone;
    }

    public int size() {
        return kind == Kind.SNAPSHOT ? rows.size() : events.size();
    }
}
