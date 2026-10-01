package com.dcp.subpush;

import com.dcp.outlet.sdk.Mode;
import com.dcp.outlet.sdk.Op;
import com.dcp.outlet.sdk.testkit.FakeBatch;
import com.dcp.outlet.sdk.testkit.FakeInitContext;
import com.dcp.outlet.sdk.testkit.FakeRecord;
import com.dcp.subpush.channel.PushChannel;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * PushLogic 单测(语义对齐 DataSubService.deliver 增量段; 11 用例):
 * logical 行数据过滤/裁剪/merge/DELETE 丢弃/seqFloor + tag 圈选/exclude/tsFloor
 * + payload 形状 + 推送失败抛。
 */
class PushLogicTest {

    private static final ObjectMapper OM = new ObjectMapper();

    /* ─────────── fakes ─────────── */

    static final class FakeChannel implements PushChannel {
        final List<String> bodies = new ArrayList<>();
        boolean fail;

        @Override public void send(String body) {
            if (fail) throw new IllegalStateException("webhook down");
            bodies.add(body);
        }

        JsonNode payload(int i) {
            try {
                return OM.readTree(bodies.get(i));
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }
    }

    static final class FakeExcluder extends Excluder {
        final Map<String, Boolean> m = new LinkedHashMap<>();

        FakeExcluder() {
            super(null, null, null, null);   // specs=null, 不连库
        }

        @Override public boolean excluded(String entity) {
            return m.getOrDefault(entity, false);
        }
    }

    static final class TestLogic extends PushLogic {
        final FakeChannel channel = new FakeChannel();
        final FakeExcluder excluder = new FakeExcluder();

        @Override protected PushChannel newChannel(PushParams p) { return channel; }
        @Override protected Excluder newExcluder() { return excluder; }
    }

    private TestLogic logic;

    @BeforeEach
    void setUp() {
        logic = new TestLogic();
    }

    /* ─────────── helpers ─────────── */

    /** 行数据夹具(TiDB 行形态): 业务字段 + seq 行级序列(写者列由用例自行拼入) */
    private static Map<String, Object> rowData(long seq, Map<String, Object> fields) {
        Map<String, Object> m = new LinkedHashMap<>(fields);
        m.put("seq", seq);
        return m;
    }

    private static Map<String, Object> tagEvent(String entity, String tagPath, String tagDictId,
                                                String tagValue, long ts) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("taskId", 5L);
        m.put("entity", entity);
        m.put("tagPath", tagPath);
        m.put("tagDictId", tagDictId);
        m.put("tagValue", tagValue);
        m.put("ts", ts);
        return m;
    }

    private static Map<String, Object> fieldsOf(Object... kv) {
        Map<String, Object> m = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put(String.valueOf(kv[i]), kv[i + 1]);
        }
        return m;
    }

    private static Map<String, Object> logicalParams(String viewJson) throws Exception {
        Map<String, Object> p = new LinkedHashMap<>();
        p.put("contractId", 7L);
        p.put("contractName", "订单订阅");
        p.put("subscriber", "appA");
        p.put("viewType", "logical");
        p.put("view", OM.readTree(viewJson));
        p.put("targetType", "webhook");
        p.put("targetUrl", "https://hook.example.com/x");
        p.put("webhookSecret", "s3cr3t");
        return p;
    }

    private static Map<String, Object> tagParams(String viewJson, Long tsFloor) throws Exception {
        Map<String, Object> p = new LinkedHashMap<>();
        p.put("contractId", 8L);
        p.put("contractName", "会员订阅");
        p.put("subscriber", "appB");
        p.put("viewType", "tag");
        p.put("view", OM.readTree(viewJson));
        p.put("targetType", "kafka");
        p.put("kafkaBootstrap", "kafka:9092");
        if (tsFloor != null) p.put("tsFloor", tsFloor);
        return p;
    }

    private void init(Map<String, Object> params) {
        FakeInitContext init = FakeInitContext.of(params);
        logic.init(init);
    }

    /* ─────────── logical 契约 ─────────── */

    @Test
    void logicalFiltersTrimsAndPushes() throws Exception {
        init(logicalParams("{\"tables\":[\"orders\"],\"cols\":[\"id\",\"amount\"],"
                + "\"groups\":[{\"conds\":[{\"field\":\"status\",\"op\":\"=\",\"val\":\"paid\"}]}]}"));
        FakeBatch batch = FakeBatch.incremental(
                FakeRecord.of(Op.INSERT, 1).after(rowData(10,
                        fieldsOf("id", "1", "amount", 100, "status", "paid", "_s1_d1_orders", 5))),
                FakeRecord.of(Op.UPDATE, 2).after(rowData(11,
                        fieldsOf("id", "2", "amount", 50, "status", "new"))),
                FakeRecord.of(Op.DELETE, 3)
                        .before("id", "3").before("amount", 20).before("status", "paid").before("seq", 12),
                FakeRecord.of(Op.INSERT, 4).after(rowData(13,
                        fieldsOf("id", "4", "amount", 80, "status", "new"))));
        logic.process(batch.stream("orders"),
                com.dcp.outlet.sdk.testkit.FakeConsumerContext.of(Mode.INCREMENTAL, 0));

        assertEquals(1, logic.channel.bodies.size());
        JsonNode payload = logic.channel.payload(0);
        assertEquals("订单订阅", payload.path("contract").asText());
        assertEquals("stream", payload.path("mode").asText());
        assertEquals(1, payload.path("count").asInt());
        JsonNode row = payload.path("rows").get(0);
        assertEquals("1", row.path("id").asText());
        assertEquals(100, row.path("amount").asInt());
        assertEquals(10, row.path("seq").asLong());
        assertTrue(row.path("status").isMissingNode(), "非交付列不推");
        assertTrue(row.path("_s1_d1_orders").isMissingNode(), "写者列不进交付载荷");
    }

    @Test
    void streamFilterSkipsOtherTables() throws Exception {
        init(logicalParams("{\"tables\":[\"orders\"]}"));
        logic.process(FakeBatch.incremental(
                FakeRecord.of(Op.INSERT, 1).after(rowData(1, fieldsOf("id", "1")))).stream("other_t"),
                com.dcp.outlet.sdk.testkit.FakeConsumerContext.of(Mode.INCREMENTAL, 0));
        assertEquals(0, logic.channel.bodies.size());
    }

    @Test
    void seqFloorSkipsDeliveredWindow() throws Exception {
        Map<String, Object> p = logicalParams("{\"tables\":[\"orders\"]}");
        p.put("seqFloor", 10L);
        init(p);
        FakeBatch batch = FakeBatch.incremental(
                FakeRecord.of(Op.INSERT, 1).after(rowData(10, fieldsOf("id", "1", "amount", 100))),
                FakeRecord.of(Op.INSERT, 2).after(rowData(11, fieldsOf("id", "2", "amount", 50))));
        logic.process(batch.stream("orders"),
                com.dcp.outlet.sdk.testkit.FakeConsumerContext.of(Mode.INCREMENTAL, 0));

        assertEquals(1, logic.channel.bodies.size());
        assertEquals(1, logic.channel.payload(0).path("count").asInt());
        assertEquals("2", logic.channel.payload(0).path("rows").get(0).path("id").asText());
    }

    @Test
    void mergeAddsSourceTable() throws Exception {
        init(logicalParams("{\"tables\":[\"a\",\"b\"],\"cols\":[\"id\"],\"merge\":\"merge\"}"));
        var ctx = com.dcp.outlet.sdk.testkit.FakeConsumerContext.of(Mode.INCREMENTAL, 0);
        // 一单元一流: 两张表各来一批, 各推一次
        logic.process(FakeBatch.incremental(
                FakeRecord.of(Op.INSERT, 1).after(rowData(1, fieldsOf("id", "1")))).stream("a"), ctx);
        logic.process(FakeBatch.incremental(
                FakeRecord.of(Op.INSERT, 2).after(rowData(2, fieldsOf("id", "2")))).stream("b"), ctx);

        assertEquals(2, logic.channel.bodies.size());
        assertEquals("a", logic.channel.payload(0).path("rows").get(0).path("source_table").asText());
        assertEquals("b", logic.channel.payload(1).path("rows").get(0).path("source_table").asText());
    }

    @Test
    void noMatchNoPush() throws Exception {
        init(logicalParams("{\"tables\":[\"orders\"],"
                + "\"groups\":[{\"conds\":[{\"field\":\"status\",\"op\":\"=\",\"val\":\"never\"}]}]}"));
        logic.process(FakeBatch.incremental(
                FakeRecord.of(Op.INSERT, 1).after(rowData(1, fieldsOf("id", "1", "status", "paid")))).stream("orders"),
                com.dcp.outlet.sdk.testkit.FakeConsumerContext.of(Mode.INCREMENTAL, 0));
        assertEquals(0, logic.channel.bodies.size());
    }

    @Test
    void pushFailureThrows() throws Exception {
        init(logicalParams("{\"tables\":[\"orders\"]}"));
        logic.channel.fail = true;
        FakeBatch batch = FakeBatch.incremental(
                FakeRecord.of(Op.INSERT, 1).after(rowData(1, fieldsOf("id", "1"))));
        IllegalStateException e = assertThrows(IllegalStateException.class, () ->
                logic.process(batch.stream("orders"),
                        com.dcp.outlet.sdk.testkit.FakeConsumerContext.of(Mode.INCREMENTAL, 0)));
        assertTrue(e.getMessage().contains("推送失败"));
    }

    /* ─────────── tag 契约 ─────────── */

    @Test
    void tagCircleExcludeAndShape() throws Exception {
        init(tagParams("{\"tags\":[{\"path\":\"会员等级\"}],\"exclude\":[{\"path\":\"黑名单\"}]}", null));
        logic.excluder.m.put("E3", true);
        FakeBatch batch = FakeBatch.incremental(
                FakeRecord.of(Op.INSERT, 1).after(tagEvent("E1", "会员等级", "11", "金牌", 100)),
                FakeRecord.of(Op.INSERT, 2).after(tagEvent("E2", "消费偏好", "12", "高", 101)),
                FakeRecord.of(Op.INSERT, 3).after(tagEvent("E3", "会员等级", "11", "银牌", 102)));
        logic.process(batch, com.dcp.outlet.sdk.testkit.FakeConsumerContext.of(Mode.INCREMENTAL, 0));

        assertEquals(1, logic.channel.bodies.size());
        JsonNode payload = logic.channel.payload(0);
        assertEquals("会员订阅", payload.path("contract").asText());
        assertEquals(1, payload.path("count").asInt());
        JsonNode ev = payload.path("events").get(0);
        assertEquals("E1", ev.path("entity").asText());
        assertEquals("会员等级", ev.path("tag").asText());
        assertEquals("金牌", ev.path("value").asText());
        assertEquals(100, ev.path("ts").asLong());
    }

    @Test
    void tagValueMatch() throws Exception {
        init(tagParams("{\"tags\":[{\"path\":\"会员等级\",\"value\":\"金牌\"}]}", null));
        logic.process(FakeBatch.incremental(
                FakeRecord.of(Op.INSERT, 1).after(tagEvent("E1", "会员等级", "11", "金牌", 100)),
                FakeRecord.of(Op.INSERT, 2).after(tagEvent("E2", "会员等级", "11", "银牌", 101))),
                com.dcp.outlet.sdk.testkit.FakeConsumerContext.of(Mode.INCREMENTAL, 0));
        assertEquals(1, logic.channel.payload(0).path("count").asInt());
        assertEquals("E1", logic.channel.payload(0).path("events").get(0).path("entity").asText());
    }

    @Test
    void tagDictIdMatchesEvenIfPathRenamed() throws Exception {
        init(tagParams("{\"tags\":[{\"path\":\"会员等级\",\"tagDictId\":11}]}", null));
        logic.process(FakeBatch.incremental(
                FakeRecord.of(Op.INSERT, 1).after(tagEvent("E1", "会员等级V2", "11", "金牌", 100))),
                com.dcp.outlet.sdk.testkit.FakeConsumerContext.of(Mode.INCREMENTAL, 0));
        assertEquals(1, logic.channel.payload(0).path("count").asInt());
    }

    @Test
    void tsFloorSkips() throws Exception {
        init(tagParams("{\"tags\":[{\"path\":\"会员等级\"}]}", 100L));
        logic.process(FakeBatch.incremental(
                FakeRecord.of(Op.INSERT, 1).after(tagEvent("E1", "会员等级", "11", "金牌", 100)),
                FakeRecord.of(Op.INSERT, 2).after(tagEvent("E2", "会员等级", "11", "金牌", 101))),
                com.dcp.outlet.sdk.testkit.FakeConsumerContext.of(Mode.INCREMENTAL, 0));
        assertEquals(1, logic.channel.payload(0).path("count").asInt());
        assertEquals("E2", logic.channel.payload(0).path("events").get(0).path("entity").asText());
    }

    @Test
    void emptyBatchNoPush() throws Exception {
        init(logicalParams("{\"tables\":[\"orders\"]}"));
        logic.process(FakeBatch.incremental().stream("orders"),
                com.dcp.outlet.sdk.testkit.FakeConsumerContext.of(Mode.INCREMENTAL, 0));
        assertEquals(0, logic.channel.bodies.size());
    }
}
