package com.dcp.outlet.adapters.e2e.support;

import com.dcp.outlet.control.PartitionReader;
import com.dcp.outlet.control.TimeUtil;
import com.dcp.outlet.control.TsoSample;

import java.util.ArrayList;
import java.util.List;

/** 起点计算所需的轻量读取(只取 commitTs)。 */
public class FakeKafkaPartitionReader implements PartitionReader {

    private final FakeKafka kafka;

    public FakeKafkaPartitionReader(FakeKafka kafka) { this.kafka = kafka; }

    private String topic;

    public FakeKafkaPartitionReader forTopic(String topic) { this.topic = topic; return this; }

    @Override public long earliestOffset(String t, int p) { return 0; }

    @Override public long latestOffset(String t, int p) { return kafka.latest(t, p); }

    @Override
    public List<TsoSample> readFrom(String t, int p, long fromOffset, int maxRows) {
        List<TsoSample> out = new ArrayList<>();
        for (FakeKafka.Rec r : kafka.read(t, p, fromOffset, maxRows)) {
            out.add(new TsoSample(r.offset, r.commitTs));
        }
        return out;
    }

    @Override
    public Long offsetForTime(String t, int p, long timeMillis) {
        long latest = kafka.latest(t, p);
        for (long i = 0; i < latest; i++) {
            if (TimeUtil.tsoToMillis(kafka.commitTsAt(t, p, i)) >= timeMillis) return i;
        }
        return latest;
    }
}
