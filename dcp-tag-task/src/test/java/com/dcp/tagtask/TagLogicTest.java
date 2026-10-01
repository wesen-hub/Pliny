package com.dcp.tagtask;

import com.dcp.outlet.sdk.Op;
import com.dcp.outlet.sdk.testkit.FakeBatch;
import com.dcp.outlet.sdk.testkit.FakeRecord;
import com.dcp.outlet.sdk.testkit.LogicHarness;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * TagLogic 底座任务单测(无 Redis/MySQL/Kafka 依赖: 覆写 sink 工厂注入 fake 收集写调用)。
 *
 * <p>覆盖: init 计划构建(visual 规则 + 规则流) / INSERT 命中多写 / DELETE 清理 /
 * 幂等重复批 / 缺 entityKey 跳过 / 多写失败抛异常(底座"失败要抛"契约)。
 */
class TagLogicTest {

    /** 可观察 fake: 记录在线层写调用 */
    static final class FakeOnline implements TagSinks.TagOnline {
        final List<String[]> puts = new ArrayList<>();
        final List<String> removes = new ArrayList<>();

        @Override public void put(String entity, Map<String, String> tags) {
            for (Map.Entry<String, String> e : tags.entrySet()) puts.add(new String[]{entity, e.getKey(), e.getValue()});
        }

        @Override public void remove(String entity) { removes.add(entity); }

        @Override public void close() { }
    }

    /** 可观察 fake: 记录投影层写调用 */
    static final class FakeView implements TagSinks.TagView {
        final List<String[]> puts = new ArrayList<>();
        final List<String> removes = new ArrayList<>();

        @Override public void put(String entity, Map<String, String> tags, long ts) {
            for (Map.Entry<String, String> e : tags.entrySet()) puts.add(new String[]{entity, e.getKey(), e.getValue()});
        }

        @Override public void remove(String entity) { removes.add(entity); }

        @Override public void close() { }
    }

    /** 可观察 fake: 记录打标事件流发射调用(可注入失败) */
    static final class FakeEvents implements TagSinks.TagEvents {
        final List<String[]> emits = new ArrayList<>();
        boolean fail;

        @Override public void emit(String entity, Map<String, String> tags, long ts) {
            if (fail) throw new IllegalStateException("kafka down");
            for (Map.Entry<String, String> e : tags.entrySet()) {
                emits.add(new String[]{entity, e.getKey(), e.getValue()});
            }
        }

        @Override public void close() { }
    }

    /** 测试子类: sink 工厂注入 fake */
    static class TestableTagLogic extends TagLogic {
        final FakeOnline online = new FakeOnline();
        final FakeView view = new FakeView();
        final FakeEvents events = new FakeEvents();

        @Override protected TagSinks.TagOnline newOnlineSink(TagParams cfg) { return online; }

        @Override protected TagSinks.TagView newViewSink(TagParams cfg) { return view; }

        @Override protected TagSinks.TagEvents newEventsSink(TagParams cfg) { return events; }
    }

    /* ---------- 参数构造 ---------- */

    /** 基础 params: status=='vip' → tag customer/vip(visual 规则, 与平台发布快照同形状) */
    private static Map<String, Object> params() {
        Map<String, Object> p = new LinkedHashMap<>();
        p.put("taskId", 5L);
        p.put("entityKey", "id");
        p.put("dataset", "rt_sync_c");
        List<Map<String, Object>> dsFields = new ArrayList<>();
        dsFields.add(Map.of("name", "id", "kind", "L"));
        dsFields.add(Map.of("name", "status", "kind", "S"));
        p.put("datasetFields", dsFields);
        List<Map<String, Object>> rules = new ArrayList<>();
        Map<String, Object> asset = new LinkedHashMap<>();
        asset.put("id", 10L);
        asset.put("kind", "rule");
        asset.put("name", "vip-rule");
        asset.put("ver", 2);
        asset.put("def", visualDef());
        rules.add(asset);
        p.put("rules", rules);
        p.put("tagIds", Map.of("customer/vip", 77L));
        Map<String, Object> online = new LinkedHashMap<>();
        online.put("enabled", true);
        online.put("redisHost", "127.0.0.1");
        online.put("redisPort", 6379);
        online.put("ttlSeconds", 3600L);
        p.put("online", online);
        Map<String, Object> factView = new LinkedHashMap<>();
        factView.put("enabled", true);
        factView.put("jdbcUrl", "jdbc:mysql://unused/dcp");
        factView.put("user", "root");
        factView.put("password", "unused");
        p.put("factView", factView);
        Map<String, Object> tagEvents = new LinkedHashMap<>();
        tagEvents.put("enabled", true);
        tagEvents.put("kafkaBootstrap", "kafka:9092");
        tagEvents.put("topic", "dcp.tag.events");
        p.put("tagEvents", tagEvents);
        return p;
    }

    private static Map<String, Object> visualDef() {
        Map<String, Object> def = new LinkedHashMap<>();
        def.put("mode", "visual");
        Map<String, Object> visual = new LinkedHashMap<>();
        List<Map<String, Object>> when = new ArrayList<>();
        Map<String, Object> g = new LinkedHashMap<>();
        g.put("logic", "AND");
        List<Map<String, Object>> conds = new ArrayList<>();
        conds.add(Map.of("field", "status", "op", "==", "val", "vip"));
        g.put("conds", conds);
        when.add(g);
        visual.put("when", when);
        List<Map<String, Object>> actions = new ArrayList<>();
        actions.add(Map.of("type", "tag", "tag", "customer/vip"));
        visual.put("actions", actions);
        def.put("visual", visual);
        return def;
    }

    /** INSERT 一行(status=vip) */
    private static FakeRecord vipRow() {
        return FakeRecord.of(Op.INSERT, 100L)
                .key("id", 1L)
                .after(Map.of("id", 1L, "status", "vip"));
    }

    /* ---------- 用例 ---------- */

    @Test
    void insertHitWritesAllSinks() {
        TestableTagLogic logic = new TestableTagLogic();
        LogicHarness.Run run = LogicHarness.run(logic, params(),
                FakeBatch.incremental(vipRow()));
        assertTrue(run.allOk(), run::toString);
        assertEquals(1, logic.online.puts.size());
        assertEquals("1", logic.online.puts.get(0)[0]);
        assertEquals("customer/vip", logic.online.puts.get(0)[1]);
        assertEquals(1, logic.view.puts.size());
        assertEquals("1", logic.view.puts.get(0)[0]);
        assertEquals("customer/vip", logic.view.puts.get(0)[1]);
        assertEquals(1, logic.events.emits.size());
        assertEquals("1", logic.events.emits.get(0)[0]);
        assertEquals("customer/vip", logic.events.emits.get(0)[1]);
    }

    @Test
    void missWritesNothing() {
        TestableTagLogic logic = new TestableTagLogic();
        LogicHarness.Run run = LogicHarness.run(logic, params(),
                FakeBatch.incremental(FakeRecord.of(Op.INSERT, 100L)
                        .key("id", 2L).after(Map.of("id", 2L, "status", "normal"))));
        assertTrue(run.allOk(), run::toString);
        assertEquals(0, logic.online.puts.size());
        assertEquals(0, logic.view.puts.size());
    }

    @Test
    void deleteClearsBothSinks() {
        TestableTagLogic logic = new TestableTagLogic();
        LogicHarness.Run run = LogicHarness.run(logic, params(),
                FakeBatch.incremental(FakeRecord.of(Op.DELETE, 101L)
                        .key("id", 1L).before("id", 1L).before("status", "vip")));
        assertTrue(run.allOk(), run::toString);
        assertEquals(1, logic.online.removes.size());
        assertEquals("1", logic.online.removes.get(0));
        assertEquals(1, logic.view.removes.size());
        assertEquals("1", logic.view.removes.get(0));
        assertEquals(0, logic.online.puts.size());
    }

    @Test
    void idempotentReplay() {
        TestableTagLogic logic = new TestableTagLogic();
        LogicHarness.Run run = LogicHarness.run(logic, params(),
                FakeBatch.incremental(vipRow()),      // 首投
                FakeBatch.incremental(vipRow()));     // 平台重放(至少一次投递)
        assertTrue(run.allOk(), run::toString);
        assertEquals(2, logic.online.puts.size());   // 幂等 = 覆盖写, 每次 put 相同结果
        assertEquals("customer/vip", logic.online.puts.get(1)[1]);
        assertEquals(2, logic.view.puts.size());
    }

    @Test
    void missingEntityKeySkips() {
        TestableTagLogic logic = new TestableTagLogic();
        LogicHarness.Run run = LogicHarness.run(logic, params(),
                FakeBatch.incremental(FakeRecord.of(Op.INSERT, 100L).after(Map.of("status", "vip"))));
        assertTrue(run.allOk(), run::toString);
        assertEquals(0, logic.online.puts.size());
        assertEquals(0, logic.view.puts.size());
    }

    @Test
    void flowBindsAndHits() {
        // 规则流: 步骤1 vip-rule 命中 → hit=stop 终止(返回命中)
        Map<String, Object> p = params();
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> rules = (List<Map<String, Object>>) p.get("rules");
        Map<String, Object> flow = new LinkedHashMap<>();
        flow.put("id", 20L);
        flow.put("kind", "flow");
        flow.put("name", "vip-flow");
        flow.put("ver", 1);
        Map<String, Object> def = new LinkedHashMap<>();
        List<Map<String, Object>> steps = new ArrayList<>();
        Map<String, Object> step = new LinkedHashMap<>();
        step.put("refId", 10L);
        step.put("hit", "stop");
        step.put("miss", "next");
        steps.add(step);
        def.put("steps", steps);
        flow.put("def", def);
        rules.add(flow);
        List<Map<String, Object>> bindings = new ArrayList<>();
        bindings.add(Map.of("kind", "flow", "id", 20L, "name", "vip-flow", "ver", 1));
        p.put("bindings", bindings);

        TestableTagLogic logic = new TestableTagLogic();
        LogicHarness.Run run = LogicHarness.run(logic, p,
                FakeBatch.incremental(vipRow()));
        assertTrue(run.allOk(), run::toString);
        assertEquals(1, logic.online.puts.size());
        assertEquals("customer/vip", logic.online.puts.get(0)[1]);
    }

    @Test
    void badParamsFailInit() {
        TestableTagLogic logic = new TestableTagLogic();
        Map<String, Object> p = params();
        p.put("datasetFields", List.of());   // 空字段域 → init 抛
        assertThrows(IllegalStateException.class, () -> LogicHarness.run(logic, p));
    }

    @Test
    void windowDerivedAccumulates() {
        // 派生字段 amt_sum=sum(amount, 60s): 规则 amt_sum>=10 → tag customer/big
        Map<String, Object> p = params();
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> dsFields = (List<Map<String, Object>>) p.get("datasetFields");
        dsFields.add(Map.of("name", "amount", "kind", "D"));
        p.put("datasetFields", dsFields);
        List<Map<String, Object>> derived = new ArrayList<>();
        Map<String, Object> d = new LinkedHashMap<>();
        d.put("name", "amt_sum");
        d.put("func", "sum");
        d.put("field", "amount");
        d.put("windowSeconds", 60L);
        derived.add(d);
        p.put("derived", derived);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> rules = (List<Map<String, Object>>) p.get("rules");
        Map<String, Object> asset = new LinkedHashMap<>();
        asset.put("id", 30L);
        asset.put("kind", "rule");
        asset.put("name", "big-rule");
        asset.put("ver", 1);
        Map<String, Object> def = new LinkedHashMap<>();
        def.put("mode", "visual");
        Map<String, Object> visual = new LinkedHashMap<>();
        List<Map<String, Object>> when = new ArrayList<>();
        Map<String, Object> g = new LinkedHashMap<>();
        g.put("logic", "AND");
        g.put("conds", List.of(Map.of("field", "amt_sum", "op", ">=", "val", "10")));
        when.add(g);
        visual.put("when", when);
        visual.put("actions", List.of(Map.of("type", "tag", "tag", "customer/big")));
        def.put("visual", visual);
        asset.put("def", def);
        rules.add(asset);

        TestableTagLogic logic = new TestableTagLogic();
        LogicHarness.Run run = LogicHarness.run(logic, p,
                FakeBatch.incremental(
                        FakeRecord.of(Op.INSERT, 100L).key("id", 1L)
                                .after(Map.of("id", 1L, "status", "vip", "amount", 6.0)),
                        FakeRecord.of(Op.INSERT, 101L).key("id", 1L)
                                .after(Map.of("id", 1L, "status", "vip", "amount", 4.0))));
        assertTrue(run.allOk(), run::toString);
        // 第一笔 amt_sum=6 命中 vip; 第二笔 amt_sum=10 命中 vip+big(平铺序: vip 在前)
        assertEquals(3, logic.online.puts.size());
        assertEquals("customer/vip", logic.online.puts.get(0)[1]);
        assertEquals("customer/vip", logic.online.puts.get(1)[1]);
        assertEquals("customer/big", logic.online.puts.get(2)[1]);
    }

    @Test
    void sinkFailurePropagates() {
        TestableTagLogic logic = new TestableTagLogic() {
            @Override protected TagSinks.TagOnline newOnlineSink(TagParams cfg) {
                return new TagSinks.TagOnline() {
                    @Override public void put(String entity, Map<String, String> tags) {
                        throw new IllegalStateException("redis down");
                    }

                    @Override public void remove(String entity) { }

                    @Override public void close() { }
                };
            }
        };
        LogicHarness.Run run = LogicHarness.run(logic, params(),
                FakeBatch.incremental(vipRow()));
        assertEquals(0, run.processedBatches);   // 失败抛 → 平台不提交
        assertEquals(1, run.errors.size());
    }

    @Test
    void eventsFailurePropagates() {
        TestableTagLogic logic = new TestableTagLogic();
        logic.events.fail = true;
        LogicHarness.Run run = LogicHarness.run(logic, params(),
                FakeBatch.incremental(vipRow()));
        assertEquals(0, run.processedBatches);   // 事件流失败抛 → 与投影层同级: 平台不提交
        assertEquals(1, run.errors.size());
    }
}
