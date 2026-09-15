package com.dcp.outlet.control;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 单元规划器:把「流的 N 个分区」映射成「M 个消费单元」(D9:单元是调度原子)。
 *
 * <p>规则:
 * <ul>
 *   <li>{@code instances == Σ分区数} → **一单元一分区**(最优)</li>
 *   <li>{@code instances < Σ分区数} → 分区**轮询均分**,每单元持有分区组(单元内单线程顺序处理)</li>
 *   <li>{@code instances > Σ分区数} → 拒绝({@link PartitionLimitExceededException})</li>
 *   <li>**单元不跨流**:多 stream 时按分区数比例分配单元数,每流至少 1 个单元</li>
 * </ul>
 */
public class UnitPlanner {

    public List<PlannedUnit> plan(String taskId, List<StreamInfo> streams, int instances, int epoch) {
        if (streams == null || streams.isEmpty()) throw new IllegalArgumentException("streams 为空");
        if (instances < 1) throw new IllegalArgumentException("instances 必须 ≥ 1");

        int total = streams.stream().mapToInt(StreamInfo::partitions).sum();
        if (instances > total) throw new PartitionLimitExceededException(instances, total);

        Map<StreamInfo, Integer> unitsPerStream = allocate(streams, instances, total);
        List<PlannedUnit> out = new ArrayList<>(instances);
        int seq = 0;
        for (StreamInfo s : streams) {
            int n = unitsPerStream.getOrDefault(s, 0);
            if (n <= 0) continue;
            List<List<Integer>> buckets = new ArrayList<>();
            for (int i = 0; i < n; i++) buckets.add(new ArrayList<>());
            for (int p = 0; p < s.partitions(); p++) {
                buckets.get(p % n).add(p);                     // 轮询均分
            }
            for (List<Integer> ps : buckets) {
                out.add(new PlannedUnit("u-" + taskId + "-" + seq++ + "-" + epoch, s.stream(), s.topic(), List.copyOf(ps)));
            }
        }
        return out;
    }

    /** 按分区数比例分配单元数(最大余数法),每流至少 1 个且不超过其分区数。 */
    private Map<StreamInfo, Integer> allocate(List<StreamInfo> streams, int instances, int total) {
        Map<StreamInfo, Integer> result = new LinkedHashMap<>();
        if (streams.size() == 1) {
            result.put(streams.get(0), instances);
            return result;
        }
        // 初始:每流 1 个
        for (StreamInfo s : streams) result.put(s, 1);
        int assigned = streams.size();
        int remaining = instances - assigned;

        // 按比例补足
        while (remaining > 0) {
            StreamInfo best = null;
            double bestScore = -1;
            for (StreamInfo s : streams) {
                int cur = result.get(s);
                if (cur >= s.partitions()) continue;           // 单元数不得超过该流分区数
                double ideal = (double) instances * s.partitions() / total;
                double score = ideal - cur;
                if (score > bestScore) { bestScore = score; best = s; }
            }
            if (best == null) break;                           // 无法再分(理论上不会发生)
            result.put(best, result.get(best) + 1);
            remaining--;
        }
        return result;
    }
}
