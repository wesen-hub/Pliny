package com.dcp.outlet.adapters.e2e.support;

import com.dcp.outlet.worker.CreateUnitCmd;
import com.dcp.outlet.worker.RawMessage;
import com.dcp.outlet.worker.RecordSource;
import com.dcp.outlet.worker.RecordSourceFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** {@link RecordSource} 的内存实现:从 {@link FakeKafka} 读、把位点提交回同一 fake。 */
public class FakeKafkaRecordSource implements RecordSource {

    private final FakeKafka kafka;
    private final String topic;
    private final String group;
    private final Map<Integer, Long> positions = new LinkedHashMap<>();
    private volatile boolean stopped;

    public FakeKafkaRecordSource(FakeKafka kafka, String topic, String group) {
        this.kafka = kafka; this.topic = topic; this.group = group;
    }

    @Override
    public List<RawMessage> poll(long timeoutMillis) {
        if (stopped) return List.of();
        List<RawMessage> out = new ArrayList<>();
        for (Map.Entry<Integer, Long> e : positions.entrySet()) {
            List<FakeKafka.Rec> recs = kafka.read(topic, e.getKey(), e.getValue(), 500);
            for (FakeKafka.Rec r : recs) {
                out.add(new RawMessage(e.getKey(), r.offset, r.payload));
            }
            if (!recs.isEmpty()) positions.put(e.getKey(), recs.get(recs.size() - 1).offset + 1);
        }
        return out;
    }

    @Override
    public void seek(int partition, long offset) { positions.put(partition, offset); }

    @Override
    public void commitSync(int partition, long offset) { kafka.commit(group, partition, offset); }

    @Override
    public void wakeup() { stopped = true; }

    @Override
    public void close() { stopped = true; }

    /** 工厂:按单元的分区集合建源。 */
    public static class Factory implements RecordSourceFactory {
        private final FakeKafka kafka;

        public Factory(FakeKafka kafka) { this.kafka = kafka; }

        @Override public RecordSource create(CreateUnitCmd cmd) {
            return new FakeKafkaRecordSource(kafka, cmd.topic(), cmd.consumerGroup());
        }
    }
}
