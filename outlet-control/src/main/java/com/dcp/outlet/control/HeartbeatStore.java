package com.dcp.outlet.control;

/** 心跳落地端口:把计算面上报的 offset/TSO/时间写进 {@code consumer_unit},供进度与巡检使用。 */
public interface HeartbeatStore {

    void record(String unitId, int partition, long offset, long tso, long at);
}
