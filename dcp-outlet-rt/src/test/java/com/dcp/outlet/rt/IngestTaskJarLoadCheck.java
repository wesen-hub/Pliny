package com.dcp.outlet.rt;

import com.dcp.outlet.sdk.ConsumerLogic;
import com.dcp.outlet.sdk.LoadedLogic;
import com.dcp.outlet.sdk.LogicRef;
import com.dcp.outlet.sdk.testkit.FakeBatch;
import com.dcp.outlet.sdk.testkit.FakeConsumerContext;
import com.dcp.outlet.sdk.testkit.FakeInitContext;
import com.dcp.outlet.sdk.testkit.FakeRecord;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 底座加载集成验证(P3 入站): JarLogicLoader 从 logic-repo 加载 dcp-ingest-task fat jar
 * (kafka-ingest@1.0.0), 跑通 init → process → close 全生命周期。
 *
 * <p>断言口径:
 * <ul>
 *   <li>jar 加载 + init(params 解析 + 装配)成功(不依赖外部 MySQL/Kafka: JdbcMetaStore
 *       构造不连库, KafkaEnvelopeSender 构造不连 broker);</li>
 *   <li>fail-closed 主链路: 目标 topic 缺失(元库不可达回落)时 process 必须抛
 *       IllegalStateException(底座据此不提交 offset 重试), 防止静默丢数据;</li>
 *   <li>正路径信封断言由 dcp-ingest-task 仓库内 11 个单测覆盖(fake MetaStore/Sender 注入)。</li>
 * </ul>
 *
 * <p>前置: dcp-ingest-task 已 package 并部署到 logic-repo/kafka-ingest/1.0.0.jar + .sha256。
 */
class IngestTaskJarLoadCheck {

    private static final Path LOGIC_REPO = Path.of("logic-repo");

    /** 底座任务 params(与 backend 提交时组装同形状; 指向不可达元库/Kafka, 只验装载与 fail-closed) */
    private static Map<String, Object> params() {
        Map<String, Object> p = new LinkedHashMap<>();
        p.put("logicalTable", "orders");
        p.put("jdbcUrl", "jdbc:mysql://127.0.0.1:3306/dcp?connectTimeout=1000");
        p.put("user", "root");
        p.put("password", "x");
        p.put("kafkaBootstrap", "127.0.0.1:9092");
        return p;
    }

    @Test
    void jarLoadsAndFailsClosedOnMissingTopic() throws Exception {
        LogicRef ref = LogicRef.of("kafka-ingest", "1.0.0");
        JarLogicLoader loader = new JarLogicLoader(new FileLogicRepository(LOGIC_REPO));
        LoadedLogic loaded = loader.load(ref);
        assertNotNull(loaded, "logic-repo 缺少 kafka-ingest/1.0.0.jar(先 package dcp-ingest-task 并部署)");
        ConsumerLogic logic = loaded.logic();

        // init: params 解析 + 装配(不连外部)
        logic.init(FakeInitContext.of(params()));

        // process: 元库不可达 → 目标 topic 缺失 → fail-closed 抛(防静默丢数据)
        FakeBatch batch = FakeBatch.incremental(
                FakeRecord.of(com.dcp.outlet.sdk.Op.INSERT, 100L).key("id", "1")
                        .after(Map.of("op", "INSERT", "submitter", "app1", "data", Map.of("id", 1L)))
                        .partition(0).offset(0));
        IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
                logic.process(batch, FakeConsumerContext.of(com.dcp.outlet.sdk.Mode.INCREMENTAL, 0)));
        assertTrue(ex.getMessage().contains("未配置 topic"), "实际: " + ex.getMessage());

        logic.close();
        loaded.close();
    }
}
