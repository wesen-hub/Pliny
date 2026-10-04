package com.dcp.indexer.core;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** CanalJson 轻量解析:官方 canal-json 与 TiCDC 变体两套键名。 */
class CanalJsonTest {

    private static final String OFFICIAL_DML = "{\"type\":\"UPDATE\",\"commitTs\":469487460064428032,"
            + "\"table\":\"t_user\",\"data\":[{\"id\":\"1\"}]}";

    private static final String TICDC_DML = "{\"id\":0,\"database\":\"target_db\",\"table\":\"t_user\","
            + "\"pkNames\":[\"id\"],\"isDdl\":false,\"type\":\"UPDATE\",\"es\":1790953683953,"
            + "\"ts\":1790953685228,\"sql\":\"\",\"data\":[{\"id\":\"1\",\"status\":\"INACTIVE\"}]}";

    private static final String TICDC_WATERMARK = "{\"type\":\"TIDB_WATERMARK\",\"ts\":469487490000000000}";

    @Test
    void officialCommitTs() {
        assertEquals(469487460064428032L, CanalJson.commitTs(OFFICIAL_DML));
    }

    @Test
    void ticdcVariantCommitTsFallsBackToTs() {
        assertEquals(1790953685228L, CanalJson.commitTs(TICDC_DML));
    }

    @Test
    void ticdcVariantTypeAndTable() {
        assertEquals("UPDATE", CanalJson.type(TICDC_DML));
        assertEquals("t_user", CanalJson.table(TICDC_DML));
        assertTrue(CanalJson.isDml(TICDC_DML));
        assertFalse(CanalJson.isWatermark(TICDC_DML));
    }

    @Test
    void ticdcWatermarkTsFallsBackToTs() {
        assertTrue(CanalJson.isWatermark(TICDC_WATERMARK));
        assertEquals(469487490000000000L, CanalJson.watermarkTs(TICDC_WATERMARK));
    }

    @Test
    void unknownKeepsNegative() {
        assertEquals(-1L, CanalJson.commitTs("{\"type\":\"UNKNOWN\"}"));
        assertEquals(-1L, CanalJson.watermarkTs("{\"type\":\"TIDB_WATERMARK\",\"x\":1}"));
    }

    /* ───── 低危-4: 顶层 ts 锚定 —— data 行内同名列不得被误当 commit/watermark 时间 ───── */

    @Test
    void rowTsColumnNotMistakenWhenTopLevelAbsent() {
        // 无顶层 ts, data 行内有 ts 列 → 修复前非锚定正则会误取行内值, 现在必须 -1
        String json = "{\"type\":\"UPDATE\",\"table\":\"t\",\"data\":[{\"id\":\"1\",\"ts\":12345}]}";
        assertEquals(-1L, CanalJson.commitTs(json));
    }

    @Test
    void rowTsColumnBeforeTopLevelNotMistaken() {
        // data 行内 ts 列排在顶层 ts 之前 → 必须取顶层值而非行内值
        String json = "{\"type\":\"UPDATE\",\"table\":\"t\","
                + "\"data\":[{\"ts\":111,\"id\":\"1\"}],\"ts\":1790953685228}";
        assertEquals(1790953685228L, CanalJson.commitTs(json));
    }

    @Test
    void topLevelTsAfterDataStillRecognized() {
        // 顶层 ts 键序在 data 之后 → 仍须识别(方案不是靠前缀截断)
        String json = "{\"type\":\"UPDATE\",\"table\":\"t\",\"data\":[{\"id\":\"1\"}],\"ts\":1790953685228}";
        assertEquals(1790953685228L, CanalJson.commitTs(json));
    }

    @Test
    void stringValueFakeTsKeyIgnored() {
        // sql/字符串字段内含 \"ts\":999 的转义内容 → 不得误匹配
        String json = "{\"type\":\"UPDATE\",\"table\":\"t\",\"sql\":\"set ts=1 where id=2\",\"ts\":1790953685228}";
        assertEquals(1790953685228L, CanalJson.commitTs(json));
    }
}
