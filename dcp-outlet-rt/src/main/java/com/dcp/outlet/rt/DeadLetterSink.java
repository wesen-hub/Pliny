package com.dcp.outlet.rt;

/** 死信出口:重试耗尽后落这里(同时告警)。绝不静默丢弃。 */
public interface DeadLetterSink {

    void record(String taskId, String unitId, int partition, long offset, long tso,
                DecodedMessage message, Throwable error);
}
