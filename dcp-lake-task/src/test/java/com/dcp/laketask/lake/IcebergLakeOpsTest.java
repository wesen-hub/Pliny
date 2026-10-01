package com.dcp.laketask.lake;

import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TestTables;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * IcebergLakeOps 真 Iceberg 语义单测(TestTables 内存表 + Parquet 读写):
 * 覆盖写(rewrite) / 只删不写 / 读旧行 / schema evolution 补列 / 表不存在抛。
 *
 * <p>验证的是 iceberg-core 1.7.1 Java API 用法本身(overwriteByRowFilter 覆盖式
 * 单表达式 + GenericAppenderFactory Parquet 写 + IcebergGenerics 读),
 * LakeLogic 层裁判/合并语义由 LakeLogicTest(fake) 覆盖。
 */
class IcebergLakeOpsTest {

    private static final List<String> KEYS = List.of("id");

    private static Schema schema() {
        return new Schema(
                Types.NestedField.optional(1, "id", Types.StringType.get()),
                Types.NestedField.optional(2, "amount", Types.StringType.get()),
                Types.NestedField.optional(3, "seq", Types.LongType.get()));
    }

    private static IcebergLakeOps ops(Table table) {
        return new IcebergLakeOps(table);
    }

    private static List<List<String>> keysOf(String... ids) {
        List<List<String>> out = new java.util.ArrayList<>();
        for (String id : ids) {
            out.add(List.of(id));
        }
        return out;
    }

    private static Map<String, String> row(String id, String amount, String seq) {
        Map<String, String> m = new LinkedHashMap<>();
        m.put("id", id);
        m.put("amount", amount);
        m.put("seq", seq);
        return m;
    }

    /** 直接读表当前行(绕过 LakeOps, 用 IcebergGenerics 全表扫) */
    private static Map<String, Map<String, String>> allRows(Table t) {
        Map<String, Map<String, String>> out = new LinkedHashMap<>();
        try (CloseableIterable<Record> rows = IcebergGenerics.read(t).build()) {
            for (Record r : rows) {
                Map<String, String> m = new LinkedHashMap<>();
                for (Types.NestedField f : t.schema().columns()) {
                    Object v = r.getField(f.name());
                    if (v != null) m.put(f.name(), String.valueOf(v));
                }
                out.put(String.valueOf(r.getField("id")), m);
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        return out;
    }

    @Test
    void rewriteUpsertsAndDeletes(@TempDir File dir) {
        Table t = TestTables.create(dir, "t1", schema(), org.apache.iceberg.PartitionSpec.unpartitioned(), 2);
        IcebergLakeOps ops = ops(t);
        ops.ensureTable(KEYS, List.of("id", "amount"), List.of("seq"));

        // ① 写入 1/2 两行(覆盖写)
        ops.rewrite(KEYS, keysOf("1", "2"), List.of(row("1", "9.9", "10"), row("2", "1.1", "11")));
        Map<String, Map<String, String>> rows = allRows(t);
        assertEquals(2, rows.size());
        assertEquals("9.9", rows.get("1").get("amount"));
        assertEquals("10", rows.get("1").get("seq"));

        // ② 1 更新 + 2 只删不写(List.of 不允许 null 元素, 用 Arrays.asList)
        ops.rewrite(KEYS, keysOf("1", "2"), java.util.Arrays.asList(row("1", "8.8", "12"), null));
        rows = allRows(t);
        assertEquals(1, rows.size());
        assertEquals("8.8", rows.get("1").get("amount"));
        assertEquals("12", rows.get("1").get("seq"));
        assertNull(rows.get("2"));
    }

    @Test
    void loadRowsReturnsOnlyTouched(@TempDir File dir) {
        Table t = TestTables.create(dir, "t2", schema(), org.apache.iceberg.PartitionSpec.unpartitioned(), 2);
        IcebergLakeOps ops = ops(t);
        ops.rewrite(KEYS, keysOf("1", "2", "3"),
                List.of(row("1", "9.9", "10"), row("2", "1.1", "11"), row("3", "3.3", "12")));

        List<Map<String, String>> loaded = ops.loadRows(KEYS, keysOf("2", "9"));
        assertEquals(2, loaded.size());
        assertEquals("1.1", loaded.get(0).get("amount"));
        assertNull(loaded.get(1));
    }

    @Test
    void ensureTableAddsMissingColumns(@TempDir File dir) {
        Table t = TestTables.create(dir, "t3", schema(), org.apache.iceberg.PartitionSpec.unpartitioned(), 2);
        IcebergLakeOps ops = ops(t);
        // 新字段 + 新写者列 → schema evolution 补列(写者列名已清洗: 点号→下划线)
        ops.ensureTable(KEYS, List.of("id", "amount", "status", "addr"), List.of("seq", "_skapp1_d_kafka_ingest"));
        java.util.Set<String> cols = new java.util.LinkedHashSet<>();
        t.schema().columns().forEach(c -> cols.add(c.name()));
        assertTrue(cols.contains("status"));
        assertTrue(cols.contains("addr"));
        assertTrue(cols.contains("_skapp1_d_kafka_ingest"));
        // 补列后可直接写含新列的行
        Map<String, String> r = row("1", "9.9", "10");
        r.put("status", "ok");
        ops.rewrite(KEYS, keysOf("1"), List.of(r));
        assertEquals("ok", allRows(t).get("1").get("status"));
    }

    @Test
    void rewriteNoopOnEmpty(@TempDir File dir) {
        Table t = TestTables.create(dir, "t4", schema(), org.apache.iceberg.PartitionSpec.unpartitioned(), 2);
        IcebergLakeOps ops = ops(t);
        ops.rewrite(KEYS, List.of(), List.of());
        assertTrue(allRows(t).isEmpty());
    }

    @Test
    void partialUpdateInMultiKeyFile(@TempDir File dir) {
        // 批作业 Spark 遗留的多键同文件(键 1/2/3 一个 Parquet) → 单键更新必须整文件重写, 未 touched 行原样
        Table t = TestTables.create(dir, "t5", schema(), org.apache.iceberg.PartitionSpec.unpartitioned(), 2);
        IcebergLakeOps ops = ops(t);
        ops.rewrite(KEYS, keysOf("1", "2", "3"),
                List.of(row("1", "9.9", "10"), row("2", "1.1", "11"), row("3", "3.3", "12")));
        // 只更新键 2: 键 1/3 原样, 键 2 新值
        ops.rewrite(KEYS, keysOf("2"), List.of(row("2", "2.2", "13")));
        Map<String, Map<String, String>> rows = allRows(t);
        assertEquals(3, rows.size());
        assertEquals("9.9", rows.get("1").get("amount"));
        assertEquals("10", rows.get("1").get("seq"));
        assertEquals("2.2", rows.get("2").get("amount"));
        assertEquals("13", rows.get("2").get("seq"));
        assertEquals("3.3", rows.get("3").get("amount"));
        assertEquals("12", rows.get("3").get("seq"));
        // 再删键 1: 1 消失, 2/3 保留
        ops.rewrite(KEYS, keysOf("1"), java.util.Arrays.asList(new Map[]{null}));
        rows = allRows(t);
        assertEquals(2, rows.size());
        assertNull(rows.get("1"));
        assertEquals("2.2", rows.get("2").get("amount"));
        assertEquals("3.3", rows.get("3").get("amount"));
    }
}
