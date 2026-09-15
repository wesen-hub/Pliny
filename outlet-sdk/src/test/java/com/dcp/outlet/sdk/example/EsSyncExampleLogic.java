package com.dcp.outlet.sdk.example;

import com.dcp.outlet.sdk.*;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 参考实现(编译验证用,不依赖任何第三方库):把变更按业务主键幂等"写"到目标端。
 *
 * <p>真实 ES 版本只需把 {@code sink} 换成 {@code RestHighLevelClient} 的同步 bulk
 * (见《编程指南》§7.3)。
 */
public class EsSyncExampleLogic implements ConsumerLogic {

    /** 模拟目标端:docId → 行。真实实现换成 ES bulk。 */
    private final Map<String, Map<String, Object>> sink = new LinkedHashMap<>();
    private final List<String> applied = new ArrayList<>();

    private String idField;

    @Override
    public void init(LogicInitContext ctx) {                    // ① 单元启动一次
        this.idField = String.valueOf(ctx.params().getOrDefault("idField", "id"));
        ctx.log().info("example ready idField={} partitions={}", idField, ctx.partitions());
    }

    @Override
    public void process(Batch batch, ConsumerContext ctx) {     // ② 每批一次
        for (ChangeRecord r : batch.records()) {
            String id = docId(r);
            if (id == null) {
                ctx.log().warn("skip: no {} in record op={}", idField, r.op());
                continue;
            }
            switch (r.op()) {
                case SNAPSHOT, INSERT, UPDATE -> sink.put(id, r.after());   // 幂等:覆盖
                case DELETE -> sink.remove(id);                             // 幂等:Delete Idempotent
            }
            applied.add(batch.stream() + ":" + id + "@" + r.tso());
        }
        // 不写 ctx.commit():正常返回后平台自动提交
        if (!ctx.caughtUp()) {
            ctx.log().debug("still catching up, safeWatermark={}", ctx.safeWatermark());
        }
    }

    @Override
    public void close() {                                        // ③ 停机一次
        sink.clear();
    }

    private String docId(ChangeRecord r) {
        Map<String, Object> m = (r.key() != null && !r.key().isEmpty()) ? r.key()
                : (r.after() != null ? r.after() : r.before());
        if (m == null) return null;
        Object v = m.get(idField);
        return v == null ? null : String.valueOf(v);
    }

    // —— 验证用 ——
    public int docCount() { return sink.size(); }

    public List<String> applied() { return applied; }
}
