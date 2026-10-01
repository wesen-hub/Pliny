package com.dcp.module.outlet.sdk;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

/** 死信的默认实现:记内存(便于观测)+ 打日志。生产替换为落库/落对象存储。 */
@Slf4j
public class LoggingDeadLetterSink implements DeadLetterSink {

    private final List<String> records = new ArrayList<>();

    @Override
    public void accept(String streamId, SdkBatch batch, Throwable error) {
        String rec = "stream=" + streamId + " position=" + (batch == null ? "-" : batch.getNextPosition())
                + " err=" + (error == null ? "-" : error.getMessage());
        records.add(rec);
        log.error("[outlet-sdk] 死信:{}", rec);
    }

    public List<String> records() {
        return records;
    }
}
