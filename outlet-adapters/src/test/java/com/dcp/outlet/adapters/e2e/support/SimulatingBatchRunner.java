package com.dcp.outlet.adapters.e2e.support;

import com.dcp.outlet.control.BatchRunner;

import java.util.Map;

/**
 * 批阶段模拟器:把 {@code commitTs ≤ T0} 的行写进"批打底表"(模拟 `AS OF T0` 快照读),
 * 然后回调"批完成" —— 与生产里"批不碰 Kafka、只产 T0"的语义一致。
 */
public class SimulatingBatchRunner implements BatchRunner {

    private final FakeKafka kafka;
    private final String topic;
    private final int partitions;
    private final Map<String, Map<String, Object>> batchSink;
    private long t0;
    private boolean failInstead;

    public SimulatingBatchRunner(FakeKafka kafka, String topic, int partitions,
                                 Map<String, Map<String, Object>> batchSink, long t0) {
        this.kafka = kafka; this.topic = topic; this.partitions = partitions;
        this.batchSink = batchSink; this.t0 = t0;
    }

    public void setT0(long t0) { this.t0 = t0; }

    public void failNextTime() { this.failInstead = true; }

    @Override
    public void runAsync(String taskId, BatchCallback callback) {
        if (failInstead) { failInstead = false; callback.done(0L, false); return; }
        for (int p = 0; p < partitions; p++) {
            long latest = kafka.latest(topic, p);
            for (long i = 0; i < latest; i++) {
                long ts = kafka.commitTsAt(topic, p, i);
                if (ts > t0) break;                            // 只取 ≤ T0 的
                String id = "u-" + p + "-" + i;
                batchSink.put(id, Map.of("id", id, "v", String.valueOf(i)));
            }
        }
        callback.done(t0, true);
    }
}
