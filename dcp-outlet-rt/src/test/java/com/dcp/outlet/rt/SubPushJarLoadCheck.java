package com.dcp.outlet.rt;

import com.dcp.outlet.sdk.ConsumerLogic;
import com.dcp.outlet.sdk.LoadedLogic;
import com.dcp.outlet.sdk.LogicRef;
import com.dcp.outlet.sdk.Mode;
import com.dcp.outlet.sdk.Op;
import com.dcp.outlet.sdk.testkit.FakeBatch;
import com.dcp.outlet.sdk.testkit.FakeConsumerContext;
import com.dcp.outlet.sdk.testkit.FakeInitContext;
import com.dcp.outlet.sdk.testkit.FakeRecord;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 底座加载集成验证(P3 订阅推送): JarLogicLoader 从 logic-repo 加载 dcp-sub-push fat jar
 * (sub-push@1.0.0), 跑通 init → process fail-closed → close 生命周期。
 *
 * <p>断言口径:
 * <ul>
 *   <li>jar 加载 + SPI 定位成功(JarLogicLoader 从 logic-repo 解析 sub-push@1.0.0);</li>
 *   <li>logical/webhook 契约 init 成功(webhook 通道惰性建 HttpClient, 不连网) →
 *       process 推不可达 URL → 抛 IllegalStateException(fail-closed, 平台不提交→重试→死信);</li>
 *   <li>init fail-closed: params 缺 viewType → 抛 IllegalStateException(不带病运行);
 *       tag 契约含 exclude 但缺 jdbcUrl → 抛(剔除语义不能静默退化);</li>
 *   <li>圈选/裁剪/payload 形状由 dcp-sub-push 仓库内 10 个单测覆盖(fake channel/excluder)。</li>
 * </ul>
 *
 * <p>前置: dcp-sub-push 已 package 并部署到 logic-repo/sub-push/1.0.0.jar + .sha256。
 */
class SubPushJarLoadCheck {

    private static final Path LOGIC_REPO = Path.of("logic-repo");

    private static Map<String, Object> logicalParams() {
        Map<String, Object> p = new LinkedHashMap<>();
        p.put("contractId", 7L);
        p.put("contractName", "订单订阅");
        p.put("subscriber", "appA");
        p.put("viewType", "logical");
        p.put("view", Map.of("tables", List.of("orders"), "cols", List.of("id")));
        p.put("targetType", "webhook");
        p.put("targetUrl", "http://127.0.0.1:59999/hook");   // 不可达: process 推送失败抛
        p.put("webhookSecret", "s3cr3t");
        return p;
    }

    private static Map<String, Object> tagParamsWithoutJdbc() {
        Map<String, Object> p = new LinkedHashMap<>();
        p.put("contractId", 8L);
        p.put("contractName", "会员订阅");
        p.put("subscriber", "appB");
        p.put("viewType", "tag");
        p.put("view", Map.of("tags", List.of(Map.of("path", "会员等级")),
                "exclude", List.of(Map.of("path", "黑名单"))));   // 有 exclude 但没配 jdbcUrl
        p.put("targetType", "kafka");
        p.put("kafkaBootstrap", "kafka:9092");
        return p;
    }

    @Test
    void jarLoadsInitOkAndProcessFailsClosedWhenWebhookUnreachable() throws Exception {
        LogicRef ref = LogicRef.of("sub-push", "1.0.0");
        JarLogicLoader loader = new JarLogicLoader(new FileLogicRepository(LOGIC_REPO));
        LoadedLogic loaded = loader.load(ref);
        assertNotNull(loaded, "logic-repo 缺少 sub-push/1.0.0.jar(先 package dcp-sub-push 并部署)");
        ConsumerLogic logic = loaded.logic();

        // init 成功(webhook 通道不连网)
        logic.init(FakeInitContext.of(logicalParams()));

        // process: 推不可达 webhook → 抛(fail-closed; 平台据此不提交→重试→死信)
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("id", "1");
        Map<String, Object> envelope = new LinkedHashMap<>();
        envelope.put("logicalTable", "orders");
        envelope.put("sourceTable", "orders");
        envelope.put("rowKind", "INSERT");
        envelope.put("seq", 10L);
        envelope.put("fields", fields);
        FakeBatch batch = FakeBatch.incremental(FakeRecord.of(Op.INSERT, 1).after(envelope));
        IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
                logic.process(batch, FakeConsumerContext.of(Mode.INCREMENTAL, 0)));
        assertTrue(ex.getMessage().contains("推送失败"), "实际: " + ex.getMessage());

        logic.close();
        loaded.close();
    }

    @Test
    void initFailsClosedWhenViewTypeMissing() throws Exception {
        LogicRef ref = LogicRef.of("sub-push", "1.0.0");
        JarLogicLoader loader = new JarLogicLoader(new FileLogicRepository(LOGIC_REPO));
        LoadedLogic loaded = loader.load(ref);
        assertNotNull(loaded);
        ConsumerLogic logic = loaded.logic();

        Map<String, Object> p = new LinkedHashMap<>(logicalParams());
        p.remove("viewType");
        IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
                logic.init(FakeInitContext.of(p)));
        assertTrue(ex.getMessage().contains("viewType"), "实际: " + ex.getMessage());

        loaded.close();
    }

    @Test
    void initFailsClosedWhenTagExcludeLacksJdbc() throws Exception {
        LogicRef ref = LogicRef.of("sub-push", "1.0.0");
        JarLogicLoader loader = new JarLogicLoader(new FileLogicRepository(LOGIC_REPO));
        LoadedLogic loaded = loader.load(ref);
        assertNotNull(loaded);
        ConsumerLogic logic = loaded.logic();

        // tag 契约带 exclude 规格但缺元库连接 → 剔除语义不能静默退化, init 显式拒绝
        IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
                logic.init(FakeInitContext.of(tagParamsWithoutJdbc())));
        assertTrue(ex.getMessage().contains("exclude"), "实际: " + ex.getMessage());

        loaded.close();
    }
}
