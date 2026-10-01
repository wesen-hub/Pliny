package com.dcp.module.outlet.sdk;

/** 死信出口:重试耗尽后,把批次交出去(落库/落文件/告警),不让它静默消失。 */
public interface DeadLetterSink {

    void accept(String streamId, SdkBatch batch, Throwable error);
}
