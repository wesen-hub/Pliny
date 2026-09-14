package com.dcp.module.outlet.source;

import com.dcp.module.outlet.model.ChangeEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * canal-json 协议解析锁(报文样例取自 2026-09-14 实测:
 * 《变更流出口-新集群部署与冒烟记录-2026-09-14》§2 L1 事件形态)。
 *
 * <p>锁的是设计依赖的三件事:①事件带整行(不回表)②old = 变更前值 ③_tidb.commitTs = 位点;
 * 外加 WATERMARK 与 DDL 的处理口径。
 */
class CanalJsonParserTest {

    private final CanalJsonParser parser = new CanalJsonParser();

    @Test
    @DisplayName("INSERT:data = 变更后整行,commitTs 即位点")
    void parseInsert() {
        String msg = """
            {"id":0,"database":"target_db","table":"t_outlet_probe","pkNames":["id"],"isDdl":false,
             "type":"INSERT","ts":1789370823370,
             "data":[{"id":"1","name":"alpha","amount":"10.00"}],"old":null,
             "_tidb":{"commitTs":469072829392879624}}""";
        var p = parser.parse(msg);
        assertEquals(CanalJsonParser.Parsed.Type.ROW, p.type());
        assertEquals(1, p.events().size());
        ChangeEvent e = p.events().get(0);
        assertEquals("INSERT", e.getOp());
        assertEquals("t_outlet_probe", e.getStreamId());
        assertEquals("alpha", e.getRow().get("name"));
        assertEquals("1", e.getKey().get("id"));
        assertEquals(469072829392879624L, e.getTso());
        assertNull(e.getBefore());
    }

    @Test
    @DisplayName("UPDATE:old = 变更前值(裁剪/主键变更识别都靠它)")
    void parseUpdate() {
        String msg = """
            {"database":"target_db","table":"t_outlet_probe","pkNames":["id"],"isDdl":false,
             "type":"UPDATE","ts":1789370823370,
             "data":[{"id":"1","name":"alpha","amount":"99.00"}],
             "old":[{"id":"1","name":"alpha","amount":"10.00"}],
             "_tidb":{"commitTs":469072829392879630}}""";
        var p = parser.parse(msg);
        assertEquals("UPDATE", p.events().get(0).getOp());
        assertEquals("99.00", p.events().get(0).getRow().get("amount"));
        assertEquals("10.00", p.events().get(0).getBefore().get("amount"));
    }

    @Test
    @DisplayName("DELETE:data = 被删行(墓碑语义可用)")
    void parseDelete() {
        String msg = """
            {"database":"target_db","table":"t_outlet_probe","pkNames":["id"],"isDdl":false,
             "type":"DELETE","data":[{"id":"2","name":"beta","amount":"20.00"}],
             "_tidb":{"commitTs":469072829392879633}}""";
        var p = parser.parse(msg);
        assertEquals("DELETE", p.events().get(0).getOp());
        assertEquals("beta", p.events().get(0).getRow().get("name"));
    }

    @Test
    @DisplayName("WATERMARK:watermarkTs 透出(追平/切批/去重的标尺)")
    void parseWatermark() {
        String msg = """
            {"type":"TIDB_WATERMARK","_tidb":{"watermarkTs":469072825711329284}}""";
        var p = parser.parse(msg);
        assertEquals(CanalJsonParser.Parsed.Type.WATERMARK, p.type());
        assertEquals(469072825711329284L, p.watermarkTs());
        assertTrue(p.events().isEmpty());
    }

    @Test
    @DisplayName("DDL(CREATE)不产出数据事件,但保留 sql 供审计")
    void parseDdl() {
        String msg = """
            {"database":"target_db","table":"t_outlet_probe","isDdl":true,"type":"CREATE",
             "sql":"CREATE TABLE `t_outlet_probe` (...)","_tidb":{"commitTs":469072829274914820}}""";
        var p = parser.parse(msg);
        assertEquals(CanalJsonParser.Parsed.Type.DDL, p.type());
        assertTrue(p.ddlSql().startsWith("CREATE TABLE"));
    }

    @Test
    @DisplayName("缺 commitTs → 忽略(没有位点就不能消费)")
    void parseMissingCommitTs() {
        String msg = """
            {"table":"t_x","type":"INSERT","data":[{"id":"1"}]}""";
        var p = parser.parse(msg);
        assertEquals(CanalJsonParser.Parsed.Type.IGNORED, p.type());
    }

    @Test
    @DisplayName("data 多行 → 展开成多个事件(共享同一 commitTs)")
    void parseMultiRow() {
        String msg = """
            {"table":"t_x","type":"INSERT","data":[{"id":"1"},{"id":"2"}],
             "_tidb":{"commitTs":100}}""";
        var p = parser.parse(msg);
        assertEquals(2, p.events().size());
        assertEquals(100L, p.events().get(1).getTso());
    }

    @Test
    @DisplayName("坏报文不抛异常(仅忽略),不得影响消费循环")
    void parseGarbage() {
        var p = parser.parse("not-a-json");
        assertEquals(CanalJsonParser.Parsed.Type.IGNORED, p.type());
        assertEquals(CanalJsonParser.Parsed.Type.IGNORED, parser.parse(null).type());
    }
}
