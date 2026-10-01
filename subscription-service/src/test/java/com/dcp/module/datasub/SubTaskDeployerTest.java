package com.dcp.module.datasub;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * SubTaskDeployer 登记桥单测(无 DB/HTTP 依赖): 档位判定 + params 组装形状
 * (与 dcp-sub-push PushParams 同源同键), 是底座任务激活前登记正确性的最后一道闸。
 */
class SubTaskDeployerTest {

    private static final ObjectMapper OM = new ObjectMapper();

    private static SubTaskDeployer deployer() {
        return new SubTaskDeployer(null, "http://127.0.0.1:8095",
                "jdbc:mysql://mysql:3306/dcp", "root", "root123", "dcp-kafka:9092", "outlet-kafka:19092");
    }

    private static Map<String, Object> contract(String viewType, String freq, String targetType,
                                                 String targetUrl, String topic) throws Exception {
        Map<String, Object> c = new LinkedHashMap<>();
        c.put("id", 7L);
        c.put("name", "订单订阅");
        c.put("subscriber", "appA");
        c.put("view_type", viewType);
        c.put("push", "push");
        c.put("freq", freq);
        c.put("status", "active");
        c.put("target_type", targetType);
        c.put("target_url", targetUrl);
        c.put("topic", topic);
        c.put("webhook_secret", "s3cr3t-abc");
        String view = "logical".equals(viewType)
                ? "{\"tables\":[\"rt_orders_b\",\"rt_sync_c\"],\"cols\":[\"id\",\"amt\"]}"
                : "{\"tags\":[{\"tagDictId\":77}],\"op\":\"AND\",\"exclude\":[{\"tagDictId\":88}]}";
        c.put("view", OM.readTree(view));
        return c;
    }

    /* ---------- 档位判定 ---------- */

    @Test
    void freqGuardBase() {
        assertTrue(SubTaskDeployer.baseRelevant("实时"));
        assertTrue(SubTaskDeployer.baseRelevant("流式"));
        assertFalse(SubTaskDeployer.baseRelevant("定时"));
        assertFalse(SubTaskDeployer.baseRelevant("批量"));
        assertFalse(SubTaskDeployer.baseRelevant("manual"));
    }

    @Test
    void namingConvention() {
        assertEquals("sub-push-7", SubTaskDeployer.taskIdOf(7));
        assertEquals("ct-sub-push-7", SubTaskDeployer.contractRefOf(7));
        assertEquals("rt_orders_b", SubTaskDeployer.streamOf("rt_orders_b"));
    }

    /* ---------- params 组装(与 PushParams 对齐) ---------- */

    @Test
    void logicalWebhookParams() throws Exception {
        Map<String, Object> p = deployer().pushParamsOf(7,
                contract("logical", "实时", "webhook", "https://app.example.com/hook", ""));
        assertEquals(7L, p.get("contractId"));
        assertEquals("订单订阅", p.get("contractName"));
        assertEquals("appA", p.get("subscriber"));
        assertEquals("logical", p.get("viewType"));
        assertEquals("webhook", p.get("targetType"));
        assertEquals("https://app.example.com/hook", p.get("targetUrl"));
        assertEquals("s3cr3t-abc", p.get("webhookSecret"));
        assertNull(p.get("topic"));   // webhook 契约不带 kafka topic
        assertEquals("jdbc:mysql://mysql:3306/dcp", p.get("jdbcUrl"));
        assertEquals("root", p.get("user"));
        assertEquals("root123", p.get("password"));
        JsonNode view = (JsonNode) p.get("view");
        assertTrue(view.path("tables").isArray());
        assertEquals("rt_orders_b", view.path("tables").get(0).asText());
    }

    @Test
    void tagKafkaParamsWithDefaultTopic() throws Exception {
        Map<String, Object> p = deployer().pushParamsOf(7,
                contract("tag", "流式", "kafka", null, ""));
        assertEquals("tag", p.get("viewType"));
        assertEquals("kafka", p.get("targetType"));
        assertEquals("dcp.sub.appA.7", p.get("topic"));   // 缺省 topic = dcp.sub.{subscriber}.{id}
        assertEquals("dcp-kafka:9092", p.get("kafkaBootstrap"));
        assertNull(p.get("targetUrl"));   // kafka 契约不带 webhook 目标
        JsonNode view = (JsonNode) p.get("view");
        assertEquals(77, view.path("tags").get(0).path("tagDictId").asLong());
        assertTrue(view.path("exclude").isArray());
    }

    @Test
    void kafkaParamsKeepExplicitTopic() throws Exception {
        Map<String, Object> p = deployer().pushParamsOf(7,
                contract("logical", "流式", "kafka", null, "custom.topic.orders"));
        assertEquals("custom.topic.orders", p.get("topic"));
    }
}
