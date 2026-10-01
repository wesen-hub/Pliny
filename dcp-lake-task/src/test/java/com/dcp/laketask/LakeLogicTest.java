package com.dcp.laketask;

import com.dcp.laketask.lake.LakeOps;
import com.dcp.laketask.meta.MetaStore;
import com.dcp.outlet.sdk.Mode;
import com.dcp.outlet.sdk.Op;
import com.dcp.outlet.sdk.testkit.FakeBatch;
import com.dcp.outlet.sdk.testkit.FakeConsumerContext;
import com.dcp.outlet.sdk.testkit.FakeInitContext;
import com.dcp.outlet.sdk.testkit.FakeRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * LakeLogic 单测(行数据模型: 消费出口栈 TiCDC 流, after/before = TiDB 整行; 12 用例):
 * 建行 / 字段级合并 / 乱序拒收 / 删行 / 旧删除重放 / 主键修改(拆两事件) / 多写者独立裁判 /
 * 单源 seq 回退 / 键列缺失 DLT / DELETE 缺 before DLT / 湖写失败抛 / init 校验。
 */
class LakeLogicTest {

    /* ─────────── fakes ─────────── */

    static final class FakeMetaStore implements MetaStore {
        final LtConfig lt = new LtConfig();
        final List<Object[]> dlt = new ArrayList<>();   // {logicalTable, sourceTable, topic, partition, offset, businessKey, payload, error}

        FakeMetaStore() {
            lt.tableName = "orders";
            lt.keyColumns = List.of("id");
        }

        @Override public LtConfig loadLt(String logicalTable) { return lt; }

        @Override public void dlt(String logicalTable, String sourceTable, String topic, int partition,
                                   long offset, String businessKey, String payload, String error) {
            dlt.add(new Object[]{logicalTable, sourceTable, topic, partition, offset, businessKey, payload, error});
        }

        @Override public void close() { }
    }

    /** 内存湖表(fake LakeOps): 语义 = 覆盖写(touched 先删, upsert 写回) */
    static final class FakeLakeOps implements LakeOps {
        final Map<String, Map<String, String>> rows = new LinkedHashMap<>();   // bizKey → row
        final List<List<List<String>>> rewrites = new ArrayList<>();           // touchKeys 历史
        final Set<String> ensuredFields = new LinkedHashSet<>();
        final Set<String> ensuredSeqCols = new LinkedHashSet<>();
        boolean failRewrite;

        @Override public void ensureTable(List<String> keyColumns, List<String> fieldCols, List<String> seqCols) {
            ensuredFields.addAll(fieldCols);
            ensuredSeqCols.addAll(seqCols);
        }

        @Override public List<Map<String, String>> loadRows(List<String> keyColumns, List<List<String>> touchKeys) {
            List<Map<String, String>> out = new ArrayList<>(touchKeys.size());
            for (List<String> kv : touchKeys) {
                Map<String, String> row = rows.get(String.join("|", kv));
                out.add(row == null ? null : new LinkedHashMap<>(row));
            }
            return out;
        }

        @Override public void rewrite(List<String> keyColumns, List<List<String>> touchKeys, List<Map<String, String>> upsertRows) {
            if (failRewrite) throw new IllegalStateException("lake down");
            rewrites.add(touchKeys);
            for (int i = 0; i < touchKeys.size(); i++) {
                String bizKey = String.join("|", touchKeys.get(i));
                if (upsertRows.get(i) == null) {
                    rows.remove(bizKey);
                } else {
                    rows.put(bizKey, new LinkedHashMap<>(upsertRows.get(i)));
                }
            }
        }

        @Override public void close() { }

        Map<String, String> row(String bizKey) { return rows.get(bizKey); }
    }

    /** 测试用: 覆写工厂注入 fakes */
    static class TestableLakeLogic extends LakeLogic {
        final FakeMetaStore meta;
        final FakeLakeOps lakeOps;

        TestableLakeLogic(FakeMetaStore meta, FakeLakeOps lakeOps) {
            this.meta = meta;
            this.lakeOps = lakeOps;
        }

        @Override protected MetaStore newMetaStore(LakeParams p) { return meta; }
        @Override protected LakeOps newLakeOps(LakeParams p, String lakeTable) { return lakeOps; }
    }

    /* ─────────── 装配 ─────────── */

    private FakeMetaStore meta;
    private FakeLakeOps lakeOps;
    private TestableLakeLogic logic;

    @BeforeEach
    void setUp() {
        meta = new FakeMetaStore();
        lakeOps = new FakeLakeOps();
        logic = new TestableLakeLogic(meta, lakeOps);
        Map<String, Object> p = new LinkedHashMap<>();
        p.put("logicalTable", "orders");
        p.put("jdbcUrl", "jdbc:mysql://fake/dcp");
        p.put("user", "root");
        p.put("password", "x");
        p.put("lake", Map.of("catalog", "demo", "namespace", "dcp", "table", "orders",
                "uri", "http://fake:8181/api/catalog"));
        logic.init(FakeInitContext.of(p));
    }

    private void process(com.dcp.outlet.sdk.Batch b) {
        logic.process(b, FakeConsumerContext.of(Mode.INCREMENTAL, 0));
    }

    /** 写者列: writerColOf(kapp1, null, kafka, ingest) 清洗后(点号→下划线) */
    private static final String JA = "_skapp1_d_kafka_ingest";
    /** 写者列: writerColOf(kapp2, null, kafka, ingest) 清洗后 */
    private static final String JB = "_skapp2_d_kafka_ingest";

    /* ─────────── TiDB 行构造(TiCDC 行数据模型) ─────────── */

    /** TiDB 行(写者 app1 写入): 业务字段 + 写者列 + 行级 seq */
    private static Map<String, Object> rowA(long seq, Map<String, Object> fields) {
        Map<String, Object> m = new LinkedHashMap<>(fields);
        m.put(JA, String.valueOf(seq));
        m.put("seq", String.valueOf(seq));
        return m;
    }

    /** TiDB 行(写者 app2 后写): 业务字段 + A/B 写者列 + 行级 seq(GREATEST) */
    private static Map<String, Object> rowAB(long seqA, long seqB, long seq, Map<String, Object> fields) {
        Map<String, Object> m = new LinkedHashMap<>(fields);
        m.put(JA, String.valueOf(seqA));
        m.put(JB, String.valueOf(seqB));
        m.put("seq", String.valueOf(seq));
        return m;
    }

    /** 单源旧链路 TiDB 行(无写者列, 仅行级 seq) */
    private static Map<String, Object> rowSeq(long seq, Map<String, Object> fields) {
        Map<String, Object> m = new LinkedHashMap<>(fields);
        m.put("seq", String.valueOf(seq));
        return m;
    }

    private static FakeRecord rec(Op op, Map<String, Object> before, Map<String, Object> after) {
        FakeRecord r = FakeRecord.of(op, 100L).after(after).partition(0).offset(0);
        if (before != null) {
            before.forEach(r::before);
        }
        return r;
    }

    /* ─────────── 用例 ─────────── */

    @Test
    void insertBuildsRow() {
        process(FakeBatch.incremental(rec(Op.INSERT, null,
                rowA(10L, Map.of("id", "1", "amount", "9.9")))));
        Map<String, String> row = lakeOps.row("1");
        assertEquals("9.9", row.get("amount"));
        assertEquals("1", row.get("id"));
        assertEquals("10", row.get(JA));
        assertEquals("10", row.get("seq"));
        assertTrue(lakeOps.ensuredFields.contains("amount"));
        assertTrue(lakeOps.ensuredSeqCols.contains(JA));
        assertEquals(0, meta.dlt.size());
    }

    @Test
    void updateMergesFieldLevel() {
        // 写者 app1 先建行(amount/status), app2 只带自己的列(部分更新) → 旧列保留
        process(FakeBatch.incremental(rec(Op.INSERT, null,
                rowA(10L, Map.of("id", "1", "amount", "9.9", "status", "ok")))));
        process(FakeBatch.incremental(rec(Op.UPDATE,
                rowA(10L, Map.of("id", "1", "amount", "9.9", "status", "ok")),
                rowAB(10L, 5L, 10L, Map.of("id", "1", "amount", "9.9", "status", "ok", "addr", "SH")))));
        Map<String, String> row = lakeOps.row("1");
        assertEquals("9.9", row.get("amount"));
        assertEquals("ok", row.get("status"));
        assertEquals("SH", row.get("addr"));
        assertEquals("5", row.get(JB));
        // seq 列取 GREATEST; 对方写者列不回退
        assertEquals("10", row.get("seq"));
        assertEquals("10", row.get(JA));
    }

    @Test
    void staleSeqSkipped() {
        process(FakeBatch.incremental(rec(Op.INSERT, null,
                rowA(10L, Map.of("id", "1", "amount", "9.9")))));
        // 乱序/重放: 同写者 seq=5 晚到 → 不倒退
        process(FakeBatch.incremental(rec(Op.UPDATE,
                rowA(10L, Map.of("id", "1", "amount", "9.9")),
                rowA(5L, Map.of("id", "1", "amount", "0.1")))));
        assertEquals("9.9", lakeOps.row("1").get("amount"));
        assertEquals("10", lakeOps.row("1").get(JA));
    }

    @Test
    void deleteRemovesRow() {
        process(FakeBatch.incremental(rec(Op.INSERT, null,
                rowA(10L, Map.of("id", "1", "amount", "9.9")))));
        process(FakeBatch.incremental(rec(Op.DELETE,
                rowA(10L, Map.of("id", "1", "amount", "9.9")),
                Map.of("id", "1"))));
        assertNull(lakeOps.row("1"));
    }

    @Test
    void staleDeleteIgnored() {
        process(FakeBatch.incremental(rec(Op.INSERT, null,
                rowA(10L, Map.of("id", "1", "amount", "9.9")))));
        // 新写把行 seq 顶到 12 → 旧删除(旧行 seq=10)重放不误删新行
        process(FakeBatch.incremental(rec(Op.UPDATE,
                rowA(10L, Map.of("id", "1", "amount", "9.9")),
                rowA(12L, Map.of("id", "1", "amount", "2.0")))));
        process(FakeBatch.incremental(rec(Op.DELETE,
                rowA(10L, Map.of("id", "1", "amount", "9.9")),
                Map.of("id", "1"))));
        assertEquals("2.0", lakeOps.row("1").get("amount"));
    }

    @Test
    void pkChangeDeletePlusInsert() {
        // TiCDC 已拆事件: 主键修改 = DELETE 旧行 + INSERT 新行
        process(FakeBatch.incremental(rec(Op.INSERT, null,
                rowA(10L, Map.of("id", "1", "amount", "9.9")))));
        process(FakeBatch.incremental(
                rec(Op.DELETE, rowA(10L, Map.of("id", "1", "amount", "9.9")), Map.of("id", "1")),
                rec(Op.INSERT, null, rowA(11L, Map.of("id", "2", "amount", "9.9")))));
        // 旧键行删, 新键行写
        assertNull(lakeOps.row("1"));
        assertEquals("9.9", lakeOps.row("2").get("amount"));
        assertEquals("11", lakeOps.row("2").get(JA));
    }

    @Test
    void multiWriterIndependentJudge() {
        // 两写者独立裁判: app2 的旧事件(seq=5)不压制 app1 的新事件(seq=10)同列更新
        process(FakeBatch.incremental(rec(Op.INSERT, null,
                rowA(10L, Map.of("id", "1", "amount", "9.9")))));
        process(FakeBatch.incremental(rec(Op.UPDATE,
                rowA(10L, Map.of("id", "1", "amount", "9.9")),
                rowAB(10L, 5L, 10L, Map.of("id", "1", "amount", "1.1")))));
        // app2 seq=5 > 其写者列初值(-1) → 应用(后到写者覆盖; 与数据面语义一致: 跨写者不裁判)
        assertEquals("1.1", lakeOps.row("1").get("amount"));
        assertEquals("5", lakeOps.row("1").get(JB));
        // app2 重放 seq=5 → 被拒(写者列已 5)
        process(FakeBatch.incremental(rec(Op.UPDATE,
                rowA(10L, Map.of("id", "1", "amount", "9.9")),
                rowAB(10L, 5L, 10L, Map.of("id", "1", "amount", "0.5")))));
        assertEquals("1.1", lakeOps.row("1").get("amount"));
    }

    @Test
    void singleSourceSeqFallback() {
        // 旧链路单源行(无写者列): 裁判回退行级 seq
        process(FakeBatch.incremental(rec(Op.INSERT, null,
                rowSeq(10L, Map.of("id", "1", "amount", "9.9")))));
        process(FakeBatch.incremental(rec(Op.UPDATE,
                rowSeq(10L, Map.of("id", "1", "amount", "9.9")),
                rowSeq(11L, Map.of("id", "1", "amount", "2.0")))));
        assertEquals("2.0", lakeOps.row("1").get("amount"));
        assertEquals("11", lakeOps.row("1").get("seq"));
        // 乱序/重放: seq=9 晚到 → 不倒退
        process(FakeBatch.incremental(rec(Op.UPDATE,
                rowSeq(10L, Map.of("id", "1", "amount", "9.9")),
                rowSeq(9L, Map.of("id", "1", "amount", "0.1")))));
        assertEquals("2.0", lakeOps.row("1").get("amount"));
    }

    @Test
    void missingKeyColumnsGoesDlt() {
        // 行内与主键均无键列 → DLT(丢弃不阻断)
        process(FakeBatch.incremental(rec(Op.INSERT, null,
                rowA(10L, Map.of("amount", "3.3")))));
        assertEquals(0, lakeOps.rewrites.size());
        assertEquals(1, meta.dlt.size());
        assertTrue(String.valueOf(meta.dlt.get(0)[7]).contains("业务键列缺失"));
    }

    @Test
    void deleteWithoutBeforeGoesDlt() {
        // DELETE 缺 before(EnableOldValue 未开) → DLT, 不误删
        process(FakeBatch.incremental(rec(Op.INSERT, null,
                rowA(10L, Map.of("id", "1", "amount", "9.9")))));
        process(FakeBatch.incremental(rec(Op.DELETE, null, Map.of("id", "1"))));
        assertEquals(1, meta.dlt.size());
        assertTrue(String.valueOf(meta.dlt.get(0)[7]).contains("缺 before"));
        assertEquals("9.9", lakeOps.row("1").get("amount"));
    }

    @Test
    void rewriteFailureThrows() {
        lakeOps.failRewrite = true;
        assertThrows(IllegalStateException.class, () ->
                process(FakeBatch.incremental(rec(Op.INSERT, null,
                        rowA(10L, Map.of("id", "1"))))));
    }

    @Test
    void initFailsWithoutKeys() {
        FakeMetaStore noKeys = new FakeMetaStore();
        noKeys.lt.keyColumns = List.of();
        TestableLakeLogic l2 = new TestableLakeLogic(noKeys, new FakeLakeOps());
        Map<String, Object> p = new LinkedHashMap<>();
        p.put("logicalTable", "orders");
        p.put("jdbcUrl", "jdbc:mysql://fake/dcp");
        p.put("user", "root");
        p.put("password", "x");
        p.put("lake", Map.of("catalog", "demo", "namespace", "dcp", "uri", "http://fake:8181"));
        assertThrows(IllegalStateException.class, () -> l2.init(FakeInitContext.of(p)));
    }
}
