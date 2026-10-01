package com.dcp.laketask.lake;

import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 湖写生产实现(iceberg-core Java API 直写; 与批作业 dcp-lakesync MERGE 同语义)。
 *
 * <p>写入模型: RewriteFiles.rewriteFiles(显式旧文件集合, 新文件集合) 一次 commit ——
 * 含 touched 键的旧文件被整文件重写(touched 行替换/删除, 其他行原样), 湖表保持
 * "每业务键一行"权威态(与 Spark MERGE INTO ON 业务键一致, 非 append 多版本)。
 *
 * <p>为什么不用 overwriteByRowFilter: 它按 deleteExpression 删文件时要求被删文件的
 * metrics 严格证明"全部行匹配 filter"(StrictMetricsEvaluator), 而业务键 OR 表达式
 * 对含多键的文件(批作业 Spark 写的文件一个含多键)无法成立 → 抛 "some, but not all,
 * rows match filter"。RewriteFiles 走显式文件集合, 无此限制, 且天然覆盖批作业遗留
 * 的多键文件。整文件重写后未 touched 行原样写回(键/字段/seq/写者列不变)。
 *
 * <p>并发: rewriteFiles 校验 deleted 文件属于当前 base 快照, 冲突抛异常 → 平台
 * 按"失败要抛"重试(重试时重读旧行重算裁判, 幂等)。Parquet 读写(与批作业同格式)。
 */
public class IcebergLakeOps implements LakeOps {

    private static final Logger log = LoggerFactory.getLogger(IcebergLakeOps.class);

    private final Catalog catalog;
    private final TableIdentifier id;
    private Table table;

    public IcebergLakeOps(Catalog catalog, String namespace, String tableName) {
        this.catalog = catalog;
        this.id = TableIdentifier.of(namespace, tableName);
    }

    /** 测试用: 直接绑定已存在表(TestTables 内存表), 跳过 catalog 装配 */
    IcebergLakeOps(Table table) {
        this.catalog = null;
        this.id = TableIdentifier.of("test", "t");
        this.table = table;
    }

    private synchronized Table table() {
        if (table == null) {
            if (!catalog.tableExists(id)) {
                throw new IllegalStateException("[lake-task] 湖表不存在: " + id
                        + " (先跑批作业 dcp-lakesync 打底建表)");
            }
            table = catalog.loadTable(id);
        }
        return table;
    }

    @Override
    public synchronized void ensureTable(List<String> keyColumns, List<String> fieldCols, List<String> seqCols) {
        if (table == null) {
            if (!catalog.tableExists(id)) {
                createTable(keyColumns, fieldCols, seqCols);
            } else {
                table = catalog.loadTable(id);
            }
        }
        // 缺列补列(schema evolution; 与批作业 ensureColumns 同向)
        Set<String> existing = new LinkedHashSet<>();
        table.schema().columns().forEach(c -> existing.add(c.name()));
        List<String> addString = new ArrayList<>();
        List<String> addLong = new ArrayList<>();
        for (String f : fieldCols) {
            if (!existing.contains(f)) addString.add(f);
        }
        for (String s : seqCols) {
            if (!existing.contains(s)) addLong.add(s);
        }
        if (!addString.isEmpty() || !addLong.isEmpty()) {
            org.apache.iceberg.UpdateSchema us = table.updateSchema();
            addString.forEach(f -> us.addColumn(f, Types.StringType.get()));
            addLong.forEach(s -> us.addColumn(s, Types.LongType.get()));
            us.commit();
            table.refresh();
            log.info("[lake-task] 湖表 {} schema evolution 补列: 字段={} seq={}", id, addString, addLong);
        }
    }

    /** 湖表不存在 → 建表(键列/字段列 string + seq·写者列 long; 无分区, 与批作业 writeTo.create 同形态) */
    private void createTable(List<String> keyColumns, List<String> fieldCols, List<String> seqCols) {
        if (catalog instanceof SupportsNamespaces sn && !sn.namespaceExists(id.namespace())) {
            sn.createNamespace(id.namespace());
        }
        List<Types.NestedField> fields = new ArrayList<>();
        int next = 1;
        for (String k : keyColumns) {
            fields.add(Types.NestedField.optional(next++, k, Types.StringType.get()));
        }
        for (String f : fieldCols) {
            if (!keyColumns.contains(f)) {
                fields.add(Types.NestedField.optional(next++, f, Types.StringType.get()));
            }
        }
        for (String s : seqCols) {
            fields.add(Types.NestedField.optional(next++, s, Types.LongType.get()));
        }
        Schema schema = new Schema(fields);
        catalog.buildTable(id, schema).create();
        table = catalog.loadTable(id);
        log.info("[lake-task] 湖表已建: {} 键={} 字段={} seq={}", id, keyColumns, fieldCols, seqCols);
    }

    @Override
    public List<Map<String, String>> loadRows(List<String> keyColumns, List<List<String>> touchKeys) {
        Table t = table();
        Map<String, Map<String, String>> byKey = new LinkedHashMap<>();
        if (touchKeys.isEmpty()) {
            return List.of();
        }
        Expression or = orExpr(keyColumns, touchKeys);
        try (CloseableIterable<Record> rows = IcebergGenerics.read(t).where(or).build()) {
            for (Record r : rows) {
                byKey.put(keyOf(r, keyColumns), rowOf(r, t.schema()));
            }
        } catch (Exception e) {
            throw new IllegalStateException("[lake-task] 湖表读旧行失败 " + id + ": " + e.getMessage(), e);
        }
        List<Map<String, String>> out = new ArrayList<>(touchKeys.size());
        for (List<String> kv : touchKeys) {
            out.add(byKey.get(String.join("|", kv)));
        }
        return out;
    }

    @Override
    public void rewrite(List<String> keyColumns, List<List<String>> touchKeys, List<Map<String, String>> upsertRows) {
        if (touchKeys.isEmpty()) {
            return;
        }
        Table t = table();
        t.refresh();
        Expression or = orExpr(keyColumns, touchKeys);

        // ① scan 找含 touched 键的旧文件(filter 仅剪枝; 行级精确归属在重写时逐行判键)
        Set<DataFile> filesToDelete = new LinkedHashSet<>();
        try (CloseableIterable<FileScanTask> tasks = t.newScan().filter(or).planFiles()) {
            for (FileScanTask task : tasks) {
                filesToDelete.add(task.file());
            }
        } catch (Exception e) {
            throw new IllegalStateException("[lake-task] 湖表 scan 失败 " + id + ": " + e.getMessage(), e);
        }

        // ② touched 键 → 新行索引(null = 删行)
        Map<String, Map<String, String>> upsertByKey = new LinkedHashMap<>();
        for (int i = 0; i < touchKeys.size(); i++) {
            upsertByKey.put(String.join("|", touchKeys.get(i)), upsertRows.get(i));
        }

        // ③ 逐旧文件整文件重写: touched 行替换/删除, 其他行原样; 同键多行去重(后者胜)
        Set<DataFile> filesToAdd = new LinkedHashSet<>();
        Set<String> handledKeys = new LinkedHashSet<>();
        for (DataFile file : filesToDelete) {
            Map<String, Map<String, String>> merged = new LinkedHashMap<>();
            for (Map<String, String> row : readFileRows(t, file)) {
                String k = keyOf(row, keyColumns);
                if (upsertByKey.containsKey(k)) {
                    handledKeys.add(k);
                    if (upsertByKey.get(k) != null) {
                        merged.put(k, upsertByKey.get(k));
                    }
                } else {
                    merged.put(k, row);
                }
            }
            if (!merged.isEmpty()) {
                try {
                    filesToAdd.add(writeParquet(t, new ArrayList<>(merged.values())));
                } catch (Exception e) {
                    throw new IllegalStateException("[lake-task] 湖表重写写 Parquet 失败 " + id + ": " + e.getMessage(), e);
                }
            }
        }

        // ④ 无旧文件的新键 → 单独新文件
        List<Map<String, String>> fresh = new ArrayList<>();
        for (Map.Entry<String, Map<String, String>> e : upsertByKey.entrySet()) {
            if (!handledKeys.contains(e.getKey()) && e.getValue() != null) {
                fresh.add(e.getValue());
            }
        }
        if (!fresh.isEmpty()) {
            try {
                filesToAdd.add(writeParquet(t, fresh));
            } catch (Exception e) {
                throw new IllegalStateException("[lake-task] 湖表写新行 Parquet 失败 " + id + ": " + e.getMessage(), e);
            }
        }

        // ⑤ commit: 有旧文件则 RewriteFiles(显式文件集合), 否则纯 append
        try {
            if (filesToDelete.isEmpty()) {
                AppendFiles ap = t.newAppend();
                for (DataFile f : filesToAdd) {
                    ap.appendFile(f);
                }
                ap.commit();
            } else {
                RewriteFiles rf = t.newRewrite();
                rf.rewriteFiles(filesToDelete, filesToAdd);
                rf.commit();
            }
        } catch (Exception e) {
            throw new IllegalStateException("[lake-task] 湖表 commit 失败 " + id + ": " + e.getMessage(), e);
        }
    }

    /* ---------- 表达式/记录 ---------- */

    /** 全部业务键合成一个 OR 表达式(每键 = 各键列 equal 的 AND) */
    private static Expression orExpr(List<String> keyColumns, List<List<String>> touchKeys) {
        Expression or = null;
        for (List<String> kv : touchKeys) {
            Expression and = null;
            for (int i = 0; i < keyColumns.size(); i++) {
                Expression eq = Expressions.equal(keyColumns.get(i), kv.get(i));
                and = and == null ? eq : Expressions.and(and, eq);
            }
            or = or == null ? and : Expressions.or(or, and);
        }
        return or;
    }

    /** 行 → GenericRecord(全列遍历; string 直写, long 解析 seq·写者列) */
    private static GenericRecord toRecord(Schema schema, Map<String, String> row) {
        GenericRecord rec = GenericRecord.create(schema);
        for (Types.NestedField f : schema.columns()) {
            String v = row.get(f.name());
            if (v == null) {
                continue;
            }
            if (f.type().typeId() == Types.LongType.get().typeId()) {
                rec.setField(f.name(), Long.parseLong(v));
            } else {
                rec.setField(f.name(), v);
            }
        }
        return rec;
    }

    /** 记录 → 业务键(与信封归一化格式一致 "v1|v2") */
    private static String keyOf(Record r, List<String> keyColumns) {
        StringBuilder sb = new StringBuilder();
        for (String k : keyColumns) {
            if (sb.length() > 0) sb.append('|');
            Object v = r.getField(k);
            sb.append(v == null ? "" : v);
        }
        return sb.toString();
    }

    /** 行 → 业务键 */
    private static String keyOf(Map<String, String> row, List<String> keyColumns) {
        StringBuilder sb = new StringBuilder();
        for (String k : keyColumns) {
            if (sb.length() > 0) sb.append('|');
            String v = row.get(k);
            sb.append(v == null ? "" : v);
        }
        return sb.toString();
    }

    /** 记录 → 行(全列 string 化, null 不落 map) */
    private static Map<String, String> rowOf(Record r, Schema schema) {
        Map<String, String> row = new LinkedHashMap<>();
        for (Types.NestedField f : schema.columns()) {
            Object v = r.getField(f.name());
            if (v != null) row.put(f.name(), String.valueOf(v));
        }
        return row;
    }

    /** 读单个 data file 全部行(整文件重写用; project 当前 schema, 旧文件缺列自动 null) */
    private static List<Map<String, String>> readFileRows(Table t, DataFile file) {
        List<Map<String, String>> out = new ArrayList<>();
        InputFile input = t.io().newInputFile(file.path().toString());
        Parquet.ReadBuilder rb = Parquet.read(input)
                .project(t.schema())
                .createReaderFunc(ms -> GenericParquetReaders.buildReader(t.schema(), ms));
        try (CloseableIterable<Record> rows = rb.build()) {
            for (Record r : rows) {
                out.add(rowOf(r, t.schema()));
            }
        } catch (Exception e) {
            throw new IllegalStateException("[lake-task] 读湖表文件失败 " + file.path() + ": " + e.getMessage(), e);
        }
        return out;
    }

    /** 行集写 Parquet data file(与批作业同格式; 表级 FileIO 由 catalog 配置提供) */
    private static DataFile writeParquet(Table t, List<Map<String, String>> rows) throws Exception {
        GenericAppenderFactory factory = new GenericAppenderFactory(t.schema(), t.spec());
        OutputFileFactory fileFactory = OutputFileFactory.builderFor(t, 1, 1).build();
        // 1.7.1 签名: newDataWriter 返回 DataWriter<Record>(GenericRecord 为其子接口)
        DataWriter<Record> writer = factory.newDataWriter(
                fileFactory.newOutputFile(), FileFormat.PARQUET, null);
        for (Map<String, String> row : rows) {
            writer.write(toRecord(t.schema(), row));
        }
        writer.close();
        return writer.toDataFile();
    }

    @Override
    public void close() {
        if (catalog instanceof AutoCloseable c) {
            try {
                c.close();
            } catch (Exception ignore) {
            }
        }
    }
}
