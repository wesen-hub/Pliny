package com.dcp.scheduler.alloc;

import com.dcp.scheduler.model.InstanceRecord;
import com.dcp.scheduler.model.NsInstanceRecord;
import com.dcp.scheduler.model.PartRecord;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 实例打分(纯函数,总纲 §6.2):
 *   score(i) = α·(1 − cpu/100) + β·(1 − assigned/capacity) + γ·affinity + δ·weight
 * 候选过滤:ONLINE、未暂停、槽位未满、迁移目标 ≠ 当前 owner。
 */
public final class Scoring {

    private final double alpha;
    private final double beta;
    private final double gamma;
    private final double delta;

    public Scoring(double alpha, double beta, double gamma, double delta) {
        this.alpha = alpha; this.beta = beta; this.gamma = gamma; this.delta = delta;
    }

    /** 缺省系数 0.4/0.3/0.2/0.1。 */
    public static Scoring defaults() {
        return new Scoring(0.4, 0.3, 0.2, 0.1);
    }

    /** 系数比较(热更新判定:配置变化才重建)。 */
    public boolean same(Scoring o) {
        return o != null && alpha == o.alpha && beta == o.beta && gamma == o.gamma && delta == o.delta;
    }

    /** 选最高分实例。assigned:实例当前已分配分区数(本 ns)。part 可为 null(新分区,无亲和)。 */
    public Optional<String> pick(List<NsInstanceRecord> candidates,
                                 Map<String, InstanceRecord> metas,
                                 Map<String, Integer> assigned,
                                 PartRecord part) {
        String best = null;
        double bestScore = -Double.MAX_VALUE;
        for (NsInstanceRecord i : candidates) {
            if (!i.acceptable()) continue;                       // 状态/暂停过滤
            int cur = assigned.getOrDefault(i.id, 0);
            if (cur >= i.capacity) continue;                     // 槽位满
            if (part != null && part.owner != null
                    && part.owner.equals(i.id)
                    && !EnumsHolder.isMigrating(part)) continue; // 迁移目标 ≠ 当前 owner
            double cpu = 0;
            InstanceRecord m = metas.get(i.id);
            if (m != null && m.meta != null) cpu = m.meta.cpu;
            double affinity = (part != null && i.id.equals(part.owner)) ? 1.0 : 0.0;
            double score = alpha * (1 - cpu / 100.0)
                    + beta * (1 - (double) cur / i.capacity)
                    + gamma * affinity
                    + delta * i.weight;
            if (score > bestScore) { bestScore = score; best = i.id; }
        }
        return Optional.ofNullable(best);
    }

    /** 内部小助手:避免直接依赖 Enums(保持纯函数风格)。 */
    private static final class EnumsHolder {
        static boolean isMigrating(PartRecord p) {
            return p != null && "MIGRATING".equals(p.state);
        }
    }
}
