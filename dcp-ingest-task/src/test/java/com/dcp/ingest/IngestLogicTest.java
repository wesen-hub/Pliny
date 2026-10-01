package com.dcp.ingest;

import com.dcp.ingest.meta.MetaStore;
import com.dcp.ingest.send.EnvelopeSender;
import com.dcp.outlet.sdk.Mode;
import com.dcp.outlet.sdk.Op;
import com.dcp.outlet.sdk.testkit.FakeBatch;
import com.dcp.outlet.sdk.testkit.FakeConsumerContext;
import com.dcp.outlet.sdk.testkit.FakeInitContext;
import com.dcp.outlet.sdk.testkit.FakeRecord;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * IngestLogic 单测(语义对齐 KafkaIngestEngine.handleBatch; 9 用例):
 * 信封内容 / 分组领号 / DLT / op 归一 / 形状兼容 / 主链路失败抛。
 */
class IngestLogicTest {

    /* ─────────── fakes ─────────── */

    static final class FakeMetaStore implements MetaStore {
        final LtConfig lt = new LtConfig();
        final Map<String, String> columns = new LinkedHashMap<>();
        final Map<String, Long> seqs = new LinkedHashMap<>();
        final List<Object[]> dlt = new ArrayList<>();   // {lt, sourceTable, topic, partition, offset, businessKey, payload, error}
        final List<String> seqCalls = new ArrayList<>();

        FakeMetaStore(String topic) {
            lt.tableName = "orders";
            lt.topic = topic;
            lt.bucketCount = 4096;
            lt.partitionCount = 8;
            lt.keyColumns = List.of("id");
            columns.put("id", "bigint");
            columns.put("amount", "decimal");
            columns.put("status", "varchar");
        }

        @Override public LtConfig loadLt(String logicalTable) { return lt; }
        @Override public Map<String, String> loadColumns(String logicalTable) { return columns; }

        @Override public long[] allocateSeq(String seqKey, int batch) {
            seqCalls.add(seqKey + ":" + batch);
            long cur = seqs.getOrDefault(seqKey, 0L) + batch;
            seqs.put(seqKey, cur);
            return new long[]{cur - batch + 1, cur};
        }

        @Override public void dlt(String logicalTable, String sourceTable, String topic, int partition,
                                  long offset, String businessKey, String payload, String error) {
            dlt.add(new Object[]{logicalTable, sourceTable, topic, partition, offset, businessKey, payload, error});
        }

        @Override public void close() { }
    }

    static final class FakeSender implements EnvelopeSender {
        final List<Object[]> sent = new ArrayList<>();   // {topic, partition, key, json}
        boolean fail;

        @Override public void send(String topic, int partition, String businessKey, String envelopeJson) {
            if (fail) throw new IllegalStateException("kafka down");
            sent.add(new Object[]{topic, partition, businessKey, envelopeJson});
        }

        @Override public void close() { }

        JsonNode envelope(int i) {
            try {
                return new com.fasterxml.jackson.databind.ObjectMapper().readTree((String) sent.get(i)[3]);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }
    }

    /** 测试用: 覆写工厂注入 fakes */
    static class TestableIngestLogic extends IngestLogic {
        final FakeMetaStore meta;
        final FakeSender sender;

        TestableIngestLogic(FakeMetaStore meta, FakeSender sender) {
            this.meta = meta;
            this.sender = sender;
        }

        @Override protected MetaStore newMetaStore(IngestParams p) { return meta; }
        @Override protected EnvelopeSender newSender(IngestParams p) { return sender; }
    }

    /* ─────────── 装配 ─────────── */

    private FakeMetaStore meta;
    private FakeSender sender;
    private TestableIngestLogic logic;

    @BeforeEach
    void setUp() {
        meta = new FakeMetaStore("dcp.orders");
        sender = new FakeSender();
        logic = new TestableIngestLogic(meta, sender);
        Map<String, Object> p = new LinkedHashMap<>();
        p.put("logicalTable", "orders");
        p.put("jdbcUrl", "jdbc:mysql://fake/dcp");
        p.put("user", "root");
        p.put("password", "x");
        p.put("kafkaBootstrap", "fake:9092");
        logic.init(FakeInitContext.of(p));
    }

    private static FakeRecord inbound(Op op, String submitter, String opStr, Map<String, Object> data) {
        Map<String, Object> after = new LinkedHashMap<>();
        if (opStr != null) after.put("op", opStr);
        after.put("ts", "2026-09-09T10:00:00");
        if (submitter != null) after.put("submitter", submitter);
        if (data != null) after.put("data", data);
        return FakeRecord.of(op, 100L).key("id", "1").after(after).partition(0).offset(0);
    }

    private void process(com.dcp.outlet.sdk.Batch b) {
        logic.process(b, FakeConsumerContext.of(Mode.INCREMENTAL, 0));
    }

    /* ─────────── 用例 ─────────── */

    @Test
    void insertOkEnvelope() {
        process(FakeBatch.incremental(inbound(Op.INSERT, "app1", "INSERT",
                Map.of("id", 1L, "amount", "9.9", "status", "ok"))));
        assertEquals(1, sender.sent.size());
        Object[] s = sender.sent.get(0);
        assertEquals("dcp.orders", s[0]);
        assertEquals(com.dcp.ingest.model.RouteUtil.bucket("1", 4096) % 8, (Integer) s[1]);
        assertEquals("1", s[2]);
        JsonNode e = sender.envelope(0);
        assertEquals("orders", e.path("logicalTable").asText());
        assertEquals("kapp1", e.path("taskId").asText());
        assertEquals("kafka", e.path("sourceDb").asText());
        assertEquals("ingest", e.path("sourceTable").asText());
        assertEquals("k", e.path("datasourceId").asText());
        assertEquals("INSERT", e.path("rowKind").asText());
        assertEquals(1L, e.path("seq").asLong());
        assertEquals("1", e.path("businessKey").asText());
        assertEquals("9.9", e.path("fields").path("amount").asText());
        assertEquals(0, meta.dlt.size());
        assertEquals(List.of("kafka|orders|app1:1"), meta.seqCalls);
    }

    @Test
    void multiAppIndependentSeq() {
        process(FakeBatch.incremental(
                inbound(Op.INSERT, "app1", "INSERT", Map.of("id", 1L)),
                inbound(Op.INSERT, "app1", "INSERT", Map.of("id", 2L)),
                inbound(Op.INSERT, "app2", "INSERT", Map.of("id", 3L)),
                inbound(Op.INSERT, "app2", "INSERT", Map.of("id", 4L))));
        assertEquals(4, sender.sent.size());
        assertEquals(List.of("kafka|orders|app1:2", "kafka|orders|app2:2"), meta.seqCalls);
        // app1 领 [1,2], app2 领 [1,2](独立序列)
        assertEquals(1L, sender.envelope(0).path("seq").asLong());
        assertEquals(2L, sender.envelope(1).path("seq").asLong());
        assertEquals(1L, sender.envelope(2).path("seq").asLong());
        assertEquals(2L, sender.envelope(3).path("seq").asLong());
        assertEquals("kapp1", sender.envelope(0).path("taskId").asText());
        assertEquals("kapp2", sender.envelope(2).path("taskId").asText());
    }

    @Test
    void badOpGoesDlt() {
        process(FakeBatch.incremental(inbound(Op.INSERT, "app1", "X", Map.of("id", 1L))));
        assertEquals(0, sender.sent.size());
        assertEquals(1, meta.dlt.size());
        assertTrue(String.valueOf(meta.dlt.get(0)[7]).contains("op 非法"));
        assertEquals("app1", meta.dlt.get(0)[1]);
        assertTrue(meta.seqCalls.isEmpty());
    }

    @Test
    void missingBusinessKeyGoesDlt() {
        process(FakeBatch.incremental(inbound(Op.INSERT, "app1", "INSERT", Map.of("amount", "9.9"))));
        assertEquals(0, sender.sent.size());
        assertEquals(1, meta.dlt.size());
        assertTrue(String.valueOf(meta.dlt.get(0)[7]).contains("业务键字段缺失"));
    }

    @Test
    void numericMismatchGoesDlt() {
        process(FakeBatch.incremental(inbound(Op.INSERT, "app1", "INSERT",
                Map.of("id", 1L, "amount", "abc"))));
        assertEquals(0, sender.sent.size());
        assertEquals(1, meta.dlt.size());
        assertTrue(String.valueOf(meta.dlt.get(0)[7]).contains("字段类型不可转换"));
    }

    @Test
    void deleteOpEnvelope() {
        process(FakeBatch.incremental(inbound(Op.DELETE, "app1", "D", Map.of("id", 1L))));
        assertEquals(1, sender.sent.size());
        assertEquals("DELETE", sender.envelope(0).path("rowKind").asText());
        assertEquals("1", sender.envelope(0).path("fields").path("id").asText());
    }

    @Test
    void unknownSubmitterFallback() {
        process(FakeBatch.incremental(inbound(Op.INSERT, null, "INSERT", Map.of("id", 1L))));
        assertEquals(1, sender.sent.size());
        assertEquals("kunknown", sender.envelope(0).path("taskId").asText());
        assertEquals(List.of("kafka|orders|unknown:1"), meta.seqCalls);
    }

    @Test
    void pureDataShapeFallback() {
        // after = 纯 data(无 op/submitter/data 信封键) → 缺省 INSERT + unknown
        FakeRecord r = FakeRecord.of(Op.INSERT, 100L).key("id", "1")
                .after(Map.of("id", 1L, "amount", "9.9")).partition(0).offset(0);
        process(FakeBatch.incremental(r));
        assertEquals(1, sender.sent.size());
        assertEquals("INSERT", sender.envelope(0).path("rowKind").asText());
        assertEquals("kunknown", sender.envelope(0).path("taskId").asText());
        assertEquals("9.9", sender.envelope(0).path("fields").path("amount").asText());
    }

    @Test
    void sendFailureThrows() {
        sender.fail = true;
        assertThrows(IllegalStateException.class, () ->
                process(FakeBatch.incremental(inbound(Op.INSERT, "app1", "INSERT", Map.of("id", 1L)))));
    }

    @Test
    void missingTopicFails() {
        FakeMetaStore noTopic = new FakeMetaStore(null);
        TestableIngestLogic l2 = new TestableIngestLogic(noTopic, new FakeSender());
        Map<String, Object> p = new LinkedHashMap<>();
        p.put("logicalTable", "orders");
        p.put("jdbcUrl", "jdbc:mysql://fake/dcp");
        p.put("user", "root");
        p.put("password", "x");
        p.put("kafkaBootstrap", "fake:9092");
        l2.init(FakeInitContext.of(p));
        assertThrows(IllegalStateException.class, () ->
                l2.process(FakeBatch.incremental(inbound(Op.INSERT, "app1", "INSERT", Map.of("id", 1L))),
                        FakeConsumerContext.of(Mode.INCREMENTAL, 0)));
    }

    @Test
    void bucketMapPriority() {
        // 桶映射优先于顺序映射(扩容场景)
        meta.lt.partitionCount = 4;
        meta.lt.bucketMap = new LinkedHashMap<>();
        int bucket = com.dcp.ingest.model.RouteUtil.bucket("1", 4096);
        meta.lt.bucketMap.put(bucket, 3);
        process(FakeBatch.incremental(inbound(Op.INSERT, "app1", "INSERT", Map.of("id", 1L))));
        assertEquals(3, (Integer) sender.sent.get(0)[1]);
        assertNotNull(sender.sent.get(0));
    }
}
