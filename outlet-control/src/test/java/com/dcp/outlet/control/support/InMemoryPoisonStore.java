package com.dcp.outlet.control.support;

import com.dcp.outlet.control.Poison;
import com.dcp.outlet.control.PoisonStore;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** 内存死信存储(自检用):行为对齐 JdbcPoisonStore(chg_anchor_poison 表)。 */
public class InMemoryPoisonStore implements PoisonStore {

    private final Map<String, Poison> poisons = new ConcurrentHashMap<>();

    @Override
    public void mark(String topic, int partition, int epoch) {
        poisons.put(key(topic, partition, epoch), new Poison(topic, partition, epoch));
    }

    @Override
    public boolean poisoned(String topic, int partition, int epoch) {
        return poisons.containsKey(key(topic, partition, epoch));
    }

    @Override
    public List<Poison> list() {
        return new ArrayList<>(poisons.values());
    }

    private String key(String topic, int partition, int epoch) {
        return topic + "#" + partition + "@" + epoch;
    }
}
