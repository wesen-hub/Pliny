package com.dcp.outlet.adapters.e2e.support;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 内存版 Kafka:每 (topic, partition) 一条不可变日志 + 每消费组一份已提交位点。
 * 用来把"消费通道"在本地完整跑通(不需要真 broker)。
 */
public class FakeKafka {

    public static final class Rec {
        public final long offset;
        public final long commitTs;
        public final byte[] payload;

        Rec(long offset, long commitTs, byte[] payload) {
            this.offset = offset; this.commitTs = commitTs; this.payload = payload;
        }
    }

    private final Map<String, Map<Integer, List<Rec>>> log = new LinkedHashMap<>();
    private final Map<String, Map<Integer, Long>> committed = new LinkedHashMap<>();

    public long append(String topic, int partition, long commitTs, byte[] payload) {
        List<Rec> list = log.computeIfAbsent(topic, k -> new LinkedHashMap<>())
                .computeIfAbsent(partition, k -> new ArrayList<>());
        long offset = list.size();
        list.add(new Rec(offset, commitTs, payload));
        return offset;
    }

    public List<Rec> read(String topic, int partition, long fromOffset, int maxRows) {
        List<Rec> list = log.getOrDefault(topic, Map.of()).getOrDefault(partition, List.of());
        List<Rec> out = new ArrayList<>();
        for (long i = Math.max(0, fromOffset); i < list.size() && out.size() < maxRows; i++) {
            out.add(list.get((int) i));
        }
        return out;
    }

    public long latest(String topic, int partition) {
        return log.getOrDefault(topic, Map.of()).getOrDefault(partition, List.of()).size();
    }

    public long commitTsAt(String topic, int partition, long offset) {
        return log.get(topic).get(partition).get((int) offset).commitTs;
    }

    public void commit(String group, int partition, long offset) {
        committed.computeIfAbsent(group, k -> new LinkedHashMap<>()).put(partition, offset);
    }

    public long committedOf(String group, int partition) {
        return committed.getOrDefault(group, Map.of()).getOrDefault(partition, -1L);
    }
}
