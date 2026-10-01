package com.dcp.indexer.core;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 分区内 {@code commitTs} 单调性守护(观测指标)。
 *
 * <p>守卫维护"分区历史最大 commitTs", 并统计回退次数。栅栏方案(§3.6)下<b>不再驱动起点降级</b>:
 * 乱序点由 {@link AnchorIndexer} 判定后永不入库(守卫未推进的历史最大即刷新点判定基准),
 * 守卫仅承载观测与告警。
 */
public final class MonotonicityGuard {

    private final Map<String, Long> lastCommitTs = new ConcurrentHashMap<>();
    private final Map<String, Long> violations = new ConcurrentHashMap<>();

    private String key(String topic, int partition) {
        return topic + "#" + partition;
    }

    /** 喂入一条样本;返回 true 表示发生回退(调用方应告警/判定乱序不入库)。 */
    public boolean onSample(String topic, int partition, long commitTs) {
        String k = key(topic, partition);
        Long prev = lastCommitTs.get(k);
        if (prev != null && commitTs < prev) {
            violations.merge(k, 1L, Long::sum);
            // 不推进 lastCommitTs:保持"已见过的最大 commitTs"(即刷新点判定基准)
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

    /** 该分区是否出现过回退(观测指标;栅栏方案下不驱动起点降级)。 */
    public boolean tainted(String topic, int partition) {
        return violationCount(topic, partition) > 0;
    }
}
