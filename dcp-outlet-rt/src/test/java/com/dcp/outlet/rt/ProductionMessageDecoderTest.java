package com.dcp.outlet.rt;

import com.dcp.outlet.sdk.Op;
import org.junit.jupiter.api.Test;

import static com.dcp.outlet.rt.ProductionMessageDecoder.cdcPayload;
import static com.dcp.outlet.rt.ProductionMessageDecoder.tagEventPayload;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 生产解码器单测:三类格式(按 topic 分发)+ 解析失败纪律。
 */
class ProductionMessageDecoderTest {

    private final ProductionMessageDecoder decoder = new ProductionMessageDecoder();

    /* ─────────────── TiCDC canal-json 行数据(dcp.cdc.*) ─────────────── */

    @Test
    void cdcInsert() {
        RawMessage raw = new RawMessage("dcp.cdc.target_db.t_user", 0, 7,
                cdcPayload(445160641929379842L,
                        "{\"id\":100231,\"name\":\"alice\",\"seq\":42,\"_s_t1\":99}", null));
        DecodedMessage d = decoder.decode(raw);
        assertEquals(DecodedMessage.Kind.EVENT, d.kind());
        assertEquals(Op.INSERT, d.op());
        assertEquals(445160641929379842L, d.tso());
        assertNull(d.before());
        assertEquals("alice", d.after().get("name"));
        assertEquals(42L, d.after().get("seq"));
        assertEquals(99L, d.after().get("_s_t1"));
        assertEquals(7L, d.offset());
    }

    @Test
    void cdcUpdate() {
        RawMessage raw = new RawMessage("dcp.cdc.target_db.t_user", 0, 8,
                cdcPayload(445160641929379843L,
                        "{\"id\":100231,\"name\":\"bob\",\"seq\":43}",
                        "{\"id\":100231,\"name\":\"alice\",\"seq\":42}"));
        DecodedMessage d = decoder.decode(raw);
        assertEquals(DecodedMessage.Kind.EVENT, d.kind());
        assertEquals(Op.UPDATE, d.op());
        assertEquals("bob", d.after().get("name"));
        assertEquals("alice", d.before().get("name"));
    }

    @Test
    void cdcDelete() {
        RawMessage raw = new RawMessage("dcp.cdc.target_db.t_user", 0, 9,
                cdcPayload(445160641929379844L, null,
                        "{\"id\":100231,\"name\":\"bob\",\"seq\":43}"));
        DecodedMessage d = decoder.decode(raw);
        assertEquals(Op.DELETE, d.op());
        assertNull(d.after());
        assertEquals("bob", d.before().get("name"));
        assertEquals(43L, d.before().get("seq"));
    }

    @Test
    void cdcDdl() {
        RawMessage raw = new RawMessage("dcp.cdc.target_db.t_user", 0, 10,
                ("{\"ts\":445160641929379845,\"isDdl\":true}").getBytes());
        assertEquals(DecodedMessage.Kind.DDL, decoder.decode(raw).kind());
    }

    /** 真实 TiCDC canal-json 消息(H15 演练集群 Kafka dcp.cdc.target_db 拉取): type/pkNames 字段 + 字符串数值 */
    @Test
    void cdcRealTiCdcShape() {
        String real = "{\"id\":0,\"database\":\"target_db\",\"table\":\"t_user\",\"pkNames\":[\"id\"],"
                + "\"isDdl\":false,\"type\":\"INSERT\",\"es\":1790884652730,\"ts\":1790884654148,\"sql\":\"\","
                + "\"old\":null,\"data\":[{\"id\":\"1\",\"user_id\":\"1001\",\"name\":\"alice\","
                + "\"status\":\"ACTIVE\",\"amount\":\"88.50\",\"seq\":\"0\","
                + "\"created_at\":\"2026-10-01 19:57:32\"}]}";
        DecodedMessage d = decoder.decode(new RawMessage("dcp.cdc.target_db.t_user", 0, 5, real.getBytes()));
        assertEquals(DecodedMessage.Kind.EVENT, d.kind());
        assertEquals(Op.INSERT, d.op());
        assertEquals(1790884654148L, d.tso());
        assertEquals("t_user", d.stream());
        assertEquals("alice", d.after().get("name"));
        assertEquals("0", d.after().get("seq"));        // 真实 TiCDC 数值序列化为字符串, 原样保留
        assertEquals("1", d.key().get("id"));            // pkNames 提取主键 → r.key()
        assertNull(d.before());
    }

    @Test
    void cdcRealTiCdcDeleteShape() {
        String real = "{\"table\":\"t_user\",\"pkNames\":[\"id\"],\"isDdl\":false,\"type\":\"DELETE\","
                + "\"ts\":1790884655000,\"data\":null,"
                + "\"old\":[{\"id\":\"1\",\"name\":\"alice\",\"seq\":\"5\"}]}";
        DecodedMessage d = decoder.decode(new RawMessage("dcp.cdc.target_db.t_user", 0, 6, real.getBytes()));
        assertEquals(Op.DELETE, d.op());
        assertNull(d.after());
        assertEquals("alice", d.before().get("name"));
        assertEquals("1", d.key().get("id"));            // DELETE 无 data → 从 old 行提取主键
    }

    @Test
    void cdcMalformedJsonYieldsOther() {
        RawMessage raw = new RawMessage("dcp.cdc.target_db.t_user", 0, 11, "{not-json".getBytes());
        DecodedMessage d = decoder.decode(raw);
        assertEquals(DecodedMessage.Kind.OTHER, d.kind());   // 跳过不阻断
    }

    /* ─────────── enable-tidb-extension 变体(ts=毫秒, TSO 在 _tidb) ─────────── */

    /** 真实 TiCDC(enable-tidb-extension=true)UPDATE: 顶层 ts=毫秒, _tidb.commitTs=TSO → tso 取 TSO(与批 T0 同域) */
    @Test
    void cdcTidbExtensionUpdate() {
        String real = "{\"table\":\"t_user\",\"pkNames\":[\"id\"],\"isDdl\":false,\"type\":\"UPDATE\","
                + "\"ts\":1790957387382,"
                + "\"old\":[{\"id\":\"3\",\"status\":\"ACTIVE\"}],"
                + "\"data\":[{\"id\":\"3\",\"status\":\"INACTIVE\"}],"
                + "\"_tidb\":{\"commitTs\":469488733088120839}}";
        DecodedMessage d = decoder.decode(new RawMessage("dcp.cdc.target_db.t_user", 0, 12, real.getBytes()));
        assertEquals(DecodedMessage.Kind.EVENT, d.kind());
        assertEquals(Op.UPDATE, d.op());
        assertEquals(469488733088120839L, d.tso());         // TSO 优先, 非毫秒
        assertEquals("INACTIVE", d.after().get("status"));
    }

    /** TIDB_WATERMARK(enable-tidb-extension 每秒一条)→ WATERMARK, 水位取 _tidb.watermarkTs(TSO) */
    @Test
    void cdcTidbWatermark() {
        String real = "{\"type\":\"TIDB_WATERMARK\",\"ts\":1790957227616,"
                + "\"_tidb\":{\"watermarkTs\":469488691145080837}}";
        DecodedMessage d = decoder.decode(new RawMessage("dcp.cdc.target_db.t_user", 0, 13, real.getBytes()));
        assertEquals(DecodedMessage.Kind.WATERMARK, d.kind());
        assertEquals(469488691145080837L, d.tso());         // 水位 TSO 域(与索引器水位一致)
    }

    /** 无 _tidb 扩展键 → 回退顶层 ts(旧变体/手工样例, 原行为不变) */
    @Test
    void cdcNoExtensionFallsBackToTopLevelTs() {
        RawMessage raw = new RawMessage("dcp.cdc.target_db.t_user", 0, 14,
                ("{\"ts\":445160641929379842,\"type\":\"INSERT\",\"data\":[{\"id\":1}]}").getBytes());
        DecodedMessage d = decoder.decode(raw);
        assertEquals(DecodedMessage.Kind.EVENT, d.kind());
        assertEquals(445160641929379842L, d.tso());
    }

    /* ─────────────── 打标事件流(dcp.tag.events) ─────────────── */

    @Test
    void tagEvent() {
        RawMessage raw = new RawMessage("dcp.tag.events", 2, 33,
                ("{\"taskId\":\"t1\",\"entity\":\"100231\",\"tagPath\":\"user.level\","
                        + "\"tagDictId\":12,\"tagValue\":\"VIP\",\"ts\":1750000000000}").getBytes());
        DecodedMessage d = decoder.decode(raw);
        assertEquals(DecodedMessage.Kind.EVENT, d.kind());
        assertEquals(Op.INSERT, d.op());
        assertEquals(1750000000000L, d.tso());
        assertEquals("100231", d.after().get("entity"));
        assertEquals("user.level", d.after().get("tagPath"));
        assertEquals(12L, d.after().get("tagDictId"));
        assertEquals("VIP", d.after().get("tagValue"));
    }

    @Test
    void tagEventCustomTopicSuffix() {
        RawMessage raw = new RawMessage("dcp.sub.tag.events", 0, 1,
                tagEventPayload(1750000000001L, "e1", "p1", "v1"));
        assertEquals(DecodedMessage.Kind.EVENT, decoder.decode(raw).kind());
    }

    @Test
    void tagEventMalformedYieldsOther() {
        RawMessage raw = new RawMessage("dcp.tag.events", 0, 2, "[]".getBytes());
        assertEquals(DecodedMessage.Kind.OTHER, decoder.decode(raw).kind());
    }

    /* ─────────────── 入站消息(dcp.in.*) ─────────────── */

    @Test
    void inboundJsonBecomesAfter() {
        RawMessage raw = new RawMessage("dcp.in.t_user", 0, 0,
                ("{\"op\":\"INSERT\",\"ts\":\"2026-10-02 21:55:00\",\"submitter\":\"dcp-app1\","
                        + "\"data\":{\"id\":\"9201\",\"name\":\"ingest-e2e-1\"}}").getBytes());
        DecodedMessage d = decoder.decode(raw);
        assertEquals(DecodedMessage.Kind.EVENT, d.kind());
        assertEquals(Op.INSERT, d.op());
        assertTrue(d.tso() > 0);                              // 通过 T0=0 的会话过滤边界
        assertEquals("dcp-app1", d.after().get("submitter"));
        assertEquals("INSERT", d.after().get("op"));
        assertTrue(d.after().get("data") instanceof java.util.Map);
    }

    @Test
    void inboundEpochTs() {
        RawMessage raw = new RawMessage("dcp.in.t_user", 0, 1,
                ("{\"op\":\"I\",\"ts\":1790884654148,\"submitter\":\"dcp-app1\",\"data\":{\"id\":1}}").getBytes());
        DecodedMessage d = decoder.decode(raw);
        assertEquals(DecodedMessage.Kind.EVENT, d.kind());
        assertEquals(1790884654148L, d.tso());
    }

    @Test
    void inboundNoTsFallsBackToNow() {
        RawMessage raw = new RawMessage("dcp.in.t_user", 0, 2,
                ("{\"op\":\"INSERT\",\"submitter\":\"dcp-app1\",\"data\":{\"id\":1}}").getBytes());
        DecodedMessage d = decoder.decode(raw);
        assertEquals(DecodedMessage.Kind.EVENT, d.kind());
        assertTrue(d.tso() > 0);                              // 缺 ts → 当前时间, 不落 0(防 T0 误滤)
    }

    @Test
    void inboundMalformedYieldsOther() {
        RawMessage raw = new RawMessage("dcp.in.t_user", 0, 3, "[1,2]".getBytes());
        assertEquals(DecodedMessage.Kind.OTHER, decoder.decode(raw).kind());
    }

    @Test
    void textFallbackMalformedYieldsOther() {
        // 脏数据进文本回退(如未识别的旧链路 topic) → OTHER 不抛, 不因一条脏数据打停 unit
        RawMessage raw = new RawMessage("legacy.topic", 0, 0, "{\"plain\":\"json\"}".getBytes());
        DecodedMessage d = decoder.decode(raw);
        assertEquals(DecodedMessage.Kind.OTHER, d.kind());
    }

    /* ─────────────── 文本回退(联调通道) ─────────────── */

    @Test
    void textFallbackForUnknownTopic() {
        RawMessage raw = new RawMessage("other.topic", 0, 0,
                SdkEventDecoder.eventPayload(123L, "stream", Op.INSERT, "id:1"));
        DecodedMessage d = decoder.decode(raw);
        assertEquals(DecodedMessage.Kind.EVENT, d.kind());
        assertEquals(Op.INSERT, d.op());
        assertEquals("1", d.after().get("id"));
    }

    @Test
    void textFallbackForNullTopic() {
        RawMessage raw = new RawMessage(0, 1, SdkEventDecoder.watermarkPayload(999L));
        DecodedMessage d = decoder.decode(raw);
        assertEquals(DecodedMessage.Kind.WATERMARK, d.kind());
        assertEquals(999L, d.tso());
    }

    /* ─────────────── 便捷构造自检 ─────────────── */

    @Test
    void conveniencePayloadsRoundTrip() {
        DecodedMessage d1 = decoder.decode(new RawMessage("dcp.cdc.t.orders", 0, 0,
                cdcPayload(1L, "{\"id\":1,\"seq\":1}", null)));
        assertEquals(Op.INSERT, d1.op());
        assertTrue(d1.after().containsKey("id"));

        DecodedMessage d2 = decoder.decode(new RawMessage("dcp.tag.events", 0, 0,
                tagEventPayload(2L, "e", "p", "v")));
        assertEquals(Op.INSERT, d2.op());
        assertEquals("e", d2.after().get("entity"));
    }
}
