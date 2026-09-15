package com.dcp.outlet.control;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 分区内 {@code commitTs} 单调性守护。
 *
 * <p>锚点二分的前提是"同分区 commitTs 单调不减"(已实验验证零反例)。
 * 一旦出现回退,**索引不可信** → 必须告警并降级(该分区标记为"索引不可用",起点走回扫)。
 */
public final class MonotonicityGuard {

    private final Map<String, Long> lastCommitTs = new ConcurrentHashMap<>();
    private final Map<String, Long> violations = new ConcurrentHashMap<>();

    private String key(String topic, int partition) {
        return topic + "#" + partition;
    }

    /** 喂入一条样本;返回 true 表示发生回退(调用方应告警)。 */
    public boolean onSample(String topic, int partition, long commitTs) {
        String k = key(topic, partition);
        Long prev = lastCommitTs.get(k);
        if (prev != null && commitTs < prev) {
            violations.merge(k, 1L, Long::sum);
            // 不推进 lastCommitTs:保持"已见过的最大 commitTs"
            return true;
        }
        lastCommitTs.put(k, commitTs);
        return false;
    }

    public long lastSeen(String topic, int partition) {
        Long v = lastCommitTs.get(key(topic, partition));
        return v == null ? -1L : v;
    }

    public long violationCount(String topic, int partition) {
        Long v = violations.get(key(topic, partition));
        return v == null ? 0L : v;
    }

    /** 该分区是否出现过回退(出现即"索引不可信")。 */
    public boolean tainted(String topic, int partition) {
        return violationCount(topic, partition) > 0;
    }
}
