package com.dcp.outlet.rt;

import com.dcp.outlet.sdk.ConsumerLogic;
import com.dcp.outlet.sdk.LoadedLogic;
import com.dcp.outlet.sdk.LogicRef;
import com.dcp.outlet.sdk.Op;
import com.dcp.outlet.sdk.testkit.FakeBatch;
import com.dcp.outlet.sdk.testkit.FakeInitContext;
import com.dcp.outlet.sdk.testkit.FakeRecord;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 底座加载集成验证(N8): JarLogicLoader 从 logic-repo 加载 dcp-tag-task fat jar
 * (tag-engine@1.0.0), 跑通 init(KieBase 编译) → process(求值) → close 全生命周期,
 * 并反射断言命中标签(双写未启用时 TagLogic.collected() 收集);
 * 另验证打标事件流发射(KafkaTagEvents)失败时的 fail-closed 传播。
 *
 * <p>前置: dcp-tag-task 已 package 并部署到 logic-repo/tag-engine/1.0.0.jar + .sha256
 * (见仓库根 deploy 说明; 缺 jar 时本测试按 fail-closed 语义抛 LogicNotFoundException)。
 */
class TagTaskJarLoadCheck {

    private static final Path LOGIC_REPO = Path.of("logic-repo");

    /** 底座任务 params(与 tagengine-service v2TaskParams 同形状; 双写 disabled 免外部依赖) */
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
        Map<String, Object> def = new LinkedHashMap<>();
        def.put("mode", "visual");
        Map<String, Object> visual = new LinkedHashMap<>();
        List<Map<String, Object>> when = new ArrayList<>();
        Map<String, Object> g = new LinkedHashMap<>();
        g.put("logic", "AND");
        g.put("conds", List.of(Map.of("field", "status", "op", "==", "val", "vip")));
        when.add(g);
        visual.put("when", when);
        visual.put("actions", List.of(Map.of("type", "tag", "tag", "customer/vip")));
        def.put("visual", visual);
        asset.put("def", def);
        rules.add(asset);
        p.put("rules", rules);
        p.put("tagIds", Map.of("customer/vip", 77L));
        p.put("online", Map.of("enabled", false));
        p.put("factView", Map.of("enabled", false));
        return p;
    }

    @Test
    void jarLoadsAndEvaluates() throws Exception {
        LogicRef ref = LogicRef.of("tag-engine", "1.0.0");
        JarLogicLoader loader = new JarLogicLoader(new FileLogicRepository(LOGIC_REPO));
        LoadedLogic loaded = loader.load(ref);
        assertNotNull(loaded, "logic-repo 缺少 tag-engine/1.0.0.jar(先 package dcp-tag-task 并部署)");
        ConsumerLogic logic = loaded.logic();

        // init: params 解析 + KieBase 编译(drools shade 服务发现失败会在此暴露)
        FakeInitContext init = FakeInitContext.of(params());
        logic.init(init);

        // process: INSERT status=vip → 命中 customer/vip(无 sink → collected 收集)
        FakeBatch batch = FakeBatch.incremental(
                FakeRecord.of(Op.INSERT, 100L).key("id", 1L)
                        .after(Map.of("id", 1L, "status", "vip")));
        logic.process(batch, com.dcp.outlet.sdk.testkit.FakeConsumerContext.of(
                com.dcp.outlet.sdk.Mode.INCREMENTAL, 0));

        // 反射断言命中(跨类加载器访问: TagLogic 由 jar ClassLoader 加载)
        Method collected = logic.getClass().getMethod("collected");
        @SuppressWarnings("unchecked")
        List<String[]> hits = (List<String[]>) collected.invoke(logic);
        assertEquals(1, hits.size(), "应命中 1 条标签");
        assertEquals("customer/vip", hits.get(0)[0]);

        logic.close();
        loaded.close();
        assertTrue(true);
    }

    @Test
    void tagEventsFailClosedWhenKafkaUnreachable() throws Exception {
        Map<String, Object> p = params();
        // 事件流启用但 Kafka 不可达(127.0.0.1 无 listener): init 惰性成功 → emit 失败抛(主链路)
        p.put("tagEvents", Map.of("enabled", true, "kafkaBootstrap", "127.0.0.1:59998",
                "topic", "dcp.tag.events"));
        LogicRef ref = LogicRef.of("tag-engine", "1.0.0");
        JarLogicLoader loader = new JarLogicLoader(new FileLogicRepository(LOGIC_REPO));
        LoadedLogic loaded = loader.load(ref);
        ConsumerLogic logic = loaded.logic();
        logic.init(FakeInitContext.of(p));
        FakeBatch batch = FakeBatch.incremental(
                FakeRecord.of(Op.INSERT, 100L).key("id", 1L)
                        .after(Map.of("id", 1L, "status", "vip")));
        IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
                logic.process(batch, com.dcp.outlet.sdk.testkit.FakeConsumerContext.of(
                        com.dcp.outlet.sdk.Mode.INCREMENTAL, 0)));
        assertTrue(ex.getMessage().contains("处理失败"), ex::getMessage);
        logic.close();
        loaded.close();
    }
}
