package com.dcp.outlet.control.support;

import com.dcp.outlet.control.Anchor;
import com.dcp.outlet.control.AnchorStore;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/** 内存锚点表(模拟 chg_anchor + 索引 (topic, partition, epoch, commit_ts))。 */
public class InMemoryAnchorStore implements AnchorStore {

    private final Map<String, List<Anchor>> byKey = new ConcurrentHashMap<>();
    private long saveCalls;

    private String key(String topic, int partition, int epoch) {
        return topic + "#" + partition + "#" + epoch;
    }

    @Override
    public void save(Anchor a) {
        saveCalls++;
        byKey.computeIfAbsent(key(a.topic(), a.partition(), a.epoch()), k -> new ArrayList<>()).add(a);
    }

    @Override
    public Anchor lastAtOrBefore(String topic, int partition, int epoch, long tso) {
        List<Anchor> list = byKey.get(key(topic, partition, epoch));
        if (list == null || list.isEmpty()) return null;
        List<Anchor> sorted = list.stream()
                .sorted(Comparator.comparingLong(Anchor::commitTs))
                .collect(Collectors.toList());
        // 二分:最后一个 commitTs <= tso
        int lo = 0, hi = sorted.size() - 1, ans = -1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            if (sorted.get(mid).commitTs() <= tso) { ans = mid; lo = mid + 1; }
            else hi = mid - 1;
        }
        return ans < 0 ? null : sorted.get(ans);
    }

    @Override
    public int prune(long before) {
        int[] removed = {0};
        byKey.values().forEach(l -> {
            int size = l.size();
            l.removeIf(a -> a.wallTime() < before);
            removed[0] += size - l.size();
        });
        return removed[0];
    }

    @Override
    public List<Anchor> list(String topic, int partition, int epoch) {
        List<Anchor> l = byKey.get(key(topic, partition, epoch));
        return l == null ? List.of() : List.copyOf(l);
    }

    public long saveCalls() { return saveCalls; }
}
