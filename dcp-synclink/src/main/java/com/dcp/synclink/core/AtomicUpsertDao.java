package com.dcp.synclink.core;

import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 目标分表原子幂等写(V3 §4.5)。
 *
 * <p>核心: seq 裁判与写入合成一条 SQL, 杜绝「先查再写」的竞态
 * (跨分区/重放时同一行可能并发写, 小 seq 后落盘覆盖大 seq)。
 *
 * <pre>
 *   INSERT ... ON DUPLICATE KEY UPDATE
 *     col = IF(seq < VALUES(seq), VALUES(col), col),
 *     seq = IF(seq < VALUES(seq), VALUES(seq), seq)
 *   DELETE FROM t WHERE 唯一键 AND seq < ?
 * </pre>
 *
 * P1: 支持批量(一批一个事务, 主键修改拆出的 DELETE+INSERT 同批原子)。
 * TODO(P2+): 列类型从模型定义生成; 字段级部分更新(多表合并场景)。
 */
@Slf4j
public class AtomicUpsertDao {

    /** 目标侧单条操作(已路由到物理表); judge=字段级 seq 裁判开关(单源逻辑表启用);
     *  writer=写者 seq 列名(如 _s{taskId}; 非空时按写者列裁判, 每个任务/源各比各的) */
    public record TableOp(String table, String rowKind, List<String> keyColumns,
                          Map<String, Object> fields, long seq, boolean judge, String writer) {
    }

    private final JdbcTemplate targetJdbc;
    private final TransactionTemplate tx;
    /** 已确认存在的物理表(避免每次写都查 information_schema) */
    private final ConcurrentHashMap<String, Boolean> ensured = new ConcurrentHashMap<>();
    /** 已确认存在的列(表名 → 列集合; 多表合并时新任务带新列 → 自动 ALTER ADD COLUMN) */
    private final ConcurrentHashMap<String, java.util.Set<String>> knownCols = new ConcurrentHashMap<>();

    public AtomicUpsertDao(DataSource targetDataSource) {
        this.targetJdbc = new JdbcTemplate(targetDataSource);
        this.tx = new TransactionTemplate(new DataSourceTransactionManager(targetDataSource));
    }

    /** 执行 DDL(模型预建物理表等) */
    public void executeDdl(String ddl) {
        targetJdbc.execute(ddl);
    }

    /** 建表缓存失效(表被删时由外部调用) */
    public void invalidate(String table) {
        ensured.remove(table);
        knownCols.remove(table);
    }

    /** 首次写入自动建表: 唯一键列 + 业务字段列(统一 VARCHAR(255)) + seq */
    public void ensureTable(String table, List<String> keyColumns, Map<String, Object> fields) {
        if (!ensured.getOrDefault(table, false)) {
            synchronized (table.intern()) {
                if (!ensured.getOrDefault(table, false)) {
                    StringBuilder ddl = new StringBuilder("CREATE TABLE IF NOT EXISTS `").append(table).append("` (\n");
                    ddl.append("  `_pk_id` BIGINT AUTO_INCREMENT PRIMARY KEY,\n");   // 物理主键, 与业务键列(可能也叫 id)解耦
                    List<String> keys = new ArrayList<>();
                    for (String k : keyColumns) {
                        ddl.append("  `").append(k).append("` VARCHAR(255) NOT NULL DEFAULT '',\n");
                        keys.add("`" + k + "`");
                    }
                    for (String f : fields.keySet()) {
                        if (keyColumns.contains(f)) {
                            continue;
                        }
                        ddl.append("  `").append(f).append("` VARCHAR(255) NULL,\n");
                    }
                    ddl.append("  seq BIGINT NOT NULL DEFAULT 0,\n");
                    ddl.append("  UNIQUE KEY uk_biz (").append(String.join(", ", keys)).append(")\n");
                    ddl.append(") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='V3 同步分表(自动建)'");
                    targetJdbc.execute(ddl.toString());
                    ensured.put(table, true);
                    knownCols.put(table, loadColumns(table));
                    log.info("目标分表已就绪: {}", table);
                }
            }
        }
        // 多表合并: 宽表列 = A∪B 全集; 后到的任务带新列 → ALTER ADD COLUMN(幂等)
        ensureColumns(table, fields.keySet());
    }

    /** 缺失列自动补充(VARCHAR(255) NULL; 一条 ALTER 批量加列) */
    private void ensureWriterCol(String table, String writer) {
        if (writer == null || writer.isBlank()) {
            return;
        }
        synchronized (table.intern()) {
            java.util.Set<String> fresh = knownCols.get(table);
            if (fresh == null || !fresh.contains(writer)) {
                fresh = loadColumns(table);
                if (!fresh.contains(writer)) {
                    try {
                        targetJdbc.execute("ALTER TABLE `" + table + "` ADD COLUMN `" + writer
                                + "` BIGINT NOT NULL DEFAULT 0 COMMENT '写者(任务×源)seq 裁判列'");
                        log.info("目标分表补写者列 {}: +{}", table, writer);
                        fresh.add(writer);
                    } catch (Exception e) {
                        // 多实例并发 ALTER 同列 → 重复加列失败; 重新拉取列集合确认后继续
                        java.util.Set<String> reload = loadColumns(table);
                        if (reload.contains(writer)) {
                            fresh = reload;
                        } else {
                            throw e;
                        }
                    }
                }
                knownCols.put(table, fresh);
            }
        }
    }

    private void ensureColumns(String table, java.util.Set<String> need) {
        java.util.Set<String> known = knownCols.get(table);
        if (known == null || !known.containsAll(need)) {
            synchronized (table.intern()) {
                java.util.Set<String> fresh = loadColumns(table); // 重新查, 防缓存过期
                List<String> missing = new ArrayList<>();
                for (String c : need) {
                    if (!fresh.contains(c)) {
                        missing.add(c);
                    }
                }
                if (!missing.isEmpty()) {
                    String alter = "ALTER TABLE `" + table + "` ADD COLUMN "
                            + String.join(", ADD COLUMN ", missing.stream()
                            .map(c -> "`" + c + "` VARCHAR(255) NULL").toList());
                    targetJdbc.execute(alter);
                    log.info("目标分表补列 {}: +{}", table, missing);
                    fresh.addAll(missing);
                }
                knownCols.put(table, fresh);
            }
        }
    }

    private java.util.Set<String> loadColumns(String table) {
        try {
            return new java.util.HashSet<>(targetJdbc.queryForList(
                    "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
                            + "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?", String.class, table));
        } catch (Exception e) {
            return new java.util.HashSet<>();
        }
    }

    /**
     * 批量原子写入: 一批一个事务(含主键修改拆出的 DELETE+INSERT, 成对原子)。
     * 建表 DDL 在事务外执行(MySQL DDL 隐式提交)。
     */
    public void applyBatch(List<TableOp> ops) {
        if (ops == null || ops.isEmpty()) {
            return;
        }
        for (TableOp op : ops) {
            ensureTable(op.table(), op.keyColumns(), op.fields());
            ensureWriterCol(op.table(), op.writer());
        }
        tx.executeWithoutResult(status -> {
            for (TableOp op : ops) {
                if ("DELETE".equalsIgnoreCase(op.rowKind())) {
                    deleteInternal(op);
                } else {
                    upsertInternal(op);
                }
            }
        });
    }

    /** 单条(兼容 P0 调用, 内部走批量事务) */
    public void apply(TableOp op) {
        applyBatch(List.of(op));
    }

    /**
     * 同表多值批量 upsert(攒批写入):
     * INSERT INTO t (cols) VALUES (...),(...),... ON DUPLICATE KEY UPDATE
     * 字段级部分更新(只写本批带来的列) + seq 行级裁判(GREATEST, 只增不减)。
     * DELETE 无法多值(逐行条件不同) → 循环单条。
     */
    public void applyMultiValue(List<TableOp> ops) {
        if (ops == null || ops.isEmpty()) {
            return;
        }
        // 确保表存在/补列(取全部 op 的列并集)
        String table = ops.get(0).table();
        List<String> keyCols = ops.get(0).keyColumns();
        java.util.Set<String> allCols = new java.util.LinkedHashSet<>();
        for (TableOp op : ops) {
            allCols.addAll(op.fields().keySet());
        }
        allCols.addAll(keyCols);
        Map<String, Object> sample = new LinkedHashMap<>();
        for (String c : allCols) {
            sample.put(c, "");
        }
        ensureTable(table, keyCols, sample);
        List<TableOp> upserts = ops.stream().filter(op -> !"DELETE".equalsIgnoreCase(op.rowKind())).toList();
        List<TableOp> deletes = ops.stream().filter(op -> "DELETE".equalsIgnoreCase(op.rowKind())).toList();
        // 攒批桶可能混入多个写者(多源/跨任务同表): 按写者列分组, 各自成一条多值 SQL, 各自比各自的 seq
        Map<String, List<TableOp>> writerGroups = new LinkedHashMap<>();
        for (TableOp u : upserts) {
            String k = (u.writer() != null && !u.writer().isBlank())
                    ? "W:" + u.writer() : "L:" + u.judge();
            writerGroups.computeIfAbsent(k, x -> new ArrayList<>()).add(u);
        }
        tx.executeWithoutResult(status -> {
            for (Map.Entry<String, List<TableOp>> g : writerGroups.entrySet()) {
                List<TableOp> gl = g.getValue();
                TableOp first = gl.get(0);
                String writer = (first.writer() != null && !first.writer().isBlank())
                        ? first.writer() : null;
                if (writer != null) {
                    ensureWriterCol(table, writer);
                }
                multiUpsertInternal(gl, keyCols, writer);
            }
            for (TableOp d : deletes) {
                ensureWriterCol(table, d.writer());
                deleteInternal(d);
            }
        });
    }

    /**
     * 批路径三步式(快照行, 设计方案 §4.3):
     * ① 轻探存在(SELECT 键列) ② 已存在行条件 UPDATE(号条件 + 值条件, NULL 安全 <=>)
     * ③ 不存在行批量 INSERT(双条件 upsert 形态, 并发窗口安全; 值同不写、号不推进)。
     * 0 行 = 确定性零写入, 不依赖引擎隐式行为; 号列 = 写者列(多源)或行 seq(单源), 与流路径同裁判规则。
     */
    public void applySnapshotBatch(List<TableOp> ops) {
        if (ops == null || ops.isEmpty()) {
            return;
        }
        for (TableOp op : ops) {
            ensureTable(op.table(), op.keyColumns(), op.fields());
            ensureWriterCol(op.table(), op.writer());
        }
        // 快照无 DELETE 语义; 防御性兼容: DELETE 走删除闸门(带号条件, 不误删)
        List<TableOp> upserts = new ArrayList<>();
        for (TableOp op : ops) {
            if ("DELETE".equalsIgnoreCase(op.rowKind())) {
                tx.executeWithoutResult(status -> deleteInternal(op));
            } else {
                upserts.add(op);
            }
        }
        if (upserts.isEmpty()) {
            return;
        }
        // 分子批: 探存在 IN 列表不宜过大
        final int chunk = 500;
        for (int from = 0; from < upserts.size(); from += chunk) {
            snapshotChunk(upserts.subList(from, Math.min(upserts.size(), from + chunk)));
        }
    }

    /** 单子批三步式: 探存在 → 更新组 + 插入组(一个事务) */
    private void snapshotChunk(List<TableOp> part) {
        String table = part.get(0).table();
        List<String> keyColumns = part.get(0).keyColumns();
        java.util.Set<String> exists = probeExisting(table, keyColumns, part);
        List<TableOp> updates = new ArrayList<>();
        List<TableOp> inserts = new ArrayList<>();
        for (TableOp op : part) {
            if (exists.contains(keyOf(op.fields(), keyColumns))) {
                updates.add(op);
            } else {
                inserts.add(op);
            }
        }
        tx.executeWithoutResult(status -> {
            for (TableOp op : updates) {
                snapshotUpdate(table, keyColumns, op);
            }
            if (!inserts.isEmpty()) {
                snapshotInsert(table, keyColumns, inserts);
            }
        });
    }

    /** 轻探存在: 单键 IN; 多键 OR 拼接(键值统一 str 化, 与写入口径一致) */
    private java.util.Set<String> probeExisting(String table, List<String> keyColumns, List<TableOp> part) {
        StringBuilder sql = new StringBuilder("SELECT ").append(keysSql(keyColumns))
                .append(" FROM `").append(table).append("` WHERE ");
        List<Object> args = new ArrayList<>();
        if (keyColumns.size() == 1) {
            sql.append("`").append(keyColumns.get(0)).append("` IN (");
            for (int i = 0; i < part.size(); i++) {
                if (i > 0) {
                    sql.append(", ");
                }
                sql.append("?");
                args.add(str(part.get(i).fields().get(keyColumns.get(0))));
            }
            sql.append(")");
        } else {
            for (int i = 0; i < part.size(); i++) {
                if (i > 0) {
                    sql.append(" OR ");
                }
                sql.append("(");
                for (int j = 0; j < keyColumns.size(); j++) {
                    if (j > 0) {
                        sql.append(" AND ");
                    }
                    sql.append("`").append(keyColumns.get(j)).append("` = ?");
                    args.add(str(part.get(i).fields().get(keyColumns.get(j))));
                }
                sql.append(")");
            }
        }
        return targetJdbc.query(sql.toString(), rs -> {
            java.util.Set<String> found = new java.util.HashSet<>();
            while (rs.next()) {
                StringBuilder k = new StringBuilder();
                for (int j = 0; j < keyColumns.size(); j++) {
                    if (j > 0) {
                        k.append('|');
                    }
                    String v = rs.getString(j + 1);
                    k.append(v == null ? "" : v);
                }
                found.add(k.toString());
            }
            return found;
        }, args.toArray());
    }

    /** 已存在行: 条件 UPDATE(号条件 ? > 号列 且 任一值列有差异); 0 行 = 确定性零写入 */
    private void snapshotUpdate(String table, List<String> keyColumns, TableOp op) {
        boolean perWriter = op.writer() != null && !op.writer().isBlank();
        String judgeCol = perWriter ? op.writer() : "seq";
        Map<String, Object> fields = op.fields();
        List<String> dataCols = new ArrayList<>(fields.keySet());
        dataCols.removeAll(keyColumns);
        if (dataCols.isEmpty()) {
            return;
        }
        StringBuilder sql = new StringBuilder("UPDATE `").append(table).append("` SET ");
        List<Object> args = new ArrayList<>();
        for (int i = 0; i < dataCols.size(); i++) {
            if (i > 0) {
                sql.append(", ");
            }
            sql.append("`").append(dataCols.get(i)).append("` = ?");
            args.add(str(fields.get(dataCols.get(i))));
        }
        sql.append(", `").append(judgeCol).append("` = ?");
        args.add(op.seq());
        sql.append(" WHERE ");
        appendKeyCond(sql, args, keyColumns, fields);
        sql.append(" AND ? > `").append(judgeCol).append("`");
        args.add(op.seq());
        sql.append(" AND (");
        for (int i = 0; i < dataCols.size(); i++) {
            if (i > 0) {
                sql.append(" OR ");
            }
            sql.append("NOT (`").append(dataCols.get(i)).append("` <=> ?)");
            args.add(str(fields.get(dataCols.get(i))));
        }
        sql.append(")");
        targetJdbc.update(sql.toString(), args.toArray());
    }

    /** 不存在行: 批量 INSERT(双条件 upsert 形态, 并发窗口安全); 号列仅在"号进且值变"时推进 */
    private void snapshotInsert(String table, List<String> keyColumns, List<TableOp> ops) {
        boolean perWriter = ops.get(0).writer() != null && !ops.get(0).writer().isBlank();
        String judgeCol = perWriter ? ops.get(0).writer() : "seq";
        java.util.Set<String> fieldCols = new java.util.LinkedHashSet<>();
        for (TableOp op : ops) {
            fieldCols.addAll(op.fields().keySet());
        }
        fieldCols.removeAll(keyColumns);
        List<String> cols = new ArrayList<>(keyColumns);
        cols.addAll(fieldCols);
        cols.add(judgeCol);
        StringBuilder sql = new StringBuilder("INSERT INTO `").append(table).append("` (")
                .append(String.join(", ", cols.stream().map(c -> "`" + c + "`").toList()))
                .append(") VALUES ");
        List<Object> args = new ArrayList<>();
        for (int i = 0; i < ops.size(); i++) {
            if (i > 0) {
                sql.append(", ");
            }
            TableOp op = ops.get(i);
            sql.append("(");
            for (int c = 0; c < cols.size(); c++) {
                if (c > 0) {
                    sql.append(", ");
                }
                sql.append("?");
                String col = cols.get(c);
                if (col.equals(judgeCol)) {
                    args.add(op.seq());
                } else {
                    args.add(str(op.fields().get(col)));
                }
            }
            sql.append(")");
        }
        sql.append(" ON DUPLICATE KEY UPDATE ");
        List<String> sets = new ArrayList<>();
        StringBuilder changed = new StringBuilder();
        for (String c : fieldCols) {
            sets.add("`" + c + "` = IF(`" + judgeCol + "` < VALUES(`" + judgeCol
                    + "`) AND NOT (`" + c + "` <=> VALUES(`" + c + "`)), VALUES(`" + c + "`), `" + c + "`)");
            if (changed.length() > 0) {
                changed.append(" OR ");
            }
            changed.append("NOT (`").append(c).append("` <=> VALUES(`").append(c).append("`))");
        }
        sets.add("`" + judgeCol + "` = IF(`" + judgeCol + "` < VALUES(`" + judgeCol + "`) AND ("
                + changed + "), VALUES(`" + judgeCol + "`), `" + judgeCol + "`)");
        sql.append(String.join(", ", sets));
        targetJdbc.update(sql.toString(), args.toArray());
    }

    private void appendKeyCond(StringBuilder sql, List<Object> args,
                               List<String> keyColumns, Map<String, Object> fields) {
        for (int j = 0; j < keyColumns.size(); j++) {
            if (j > 0) {
                sql.append(" AND ");
            }
            sql.append("`").append(keyColumns.get(j)).append("` = ?");
            args.add(str(fields.get(keyColumns.get(j))));
        }
    }

    private String keysSql(List<String> keyColumns) {
        return keyColumns.stream().map(c -> "`" + c + "`")
                .collect(java.util.stream.Collectors.joining(", "));
    }

    private String keyOf(Map<String, Object> fields, List<String> keyColumns) {
        StringBuilder k = new StringBuilder();
        for (String c : keyColumns) {
            if (k.length() > 0) {
                k.append('|');
            }
            k.append(str(fields.get(c)));
        }
        return k.toString();
    }

    /** 多值 INSERT ... ON DUPLICATE KEY UPDATE(同表, 同写者) */
    private void multiUpsertInternal(List<TableOp> ops, List<String> keyCols, String writer) {
        String table = ops.get(0).table();
        boolean perWriter = writer != null && !writer.isBlank();
        // 列集合: key 列 + 字段并集 + [写者列] + seq
        List<String> cols = new ArrayList<>(new LinkedHashSet<>(keyCols));
        java.util.Set<String> fieldCols = new LinkedHashSet<>();
        for (TableOp op : ops) {
            fieldCols.addAll(op.fields().keySet());
        }
        fieldCols.removeAll(keyCols);
        cols.addAll(fieldCols);
        if (perWriter) {
            cols.add(writer);
        }
        cols.add("seq");

        StringBuilder sql = new StringBuilder("INSERT INTO `").append(table).append("` (")
                .append(String.join(", ", cols.stream().map(c -> "`" + c + "`").toList()))
                .append(") VALUES ");
        List<Object> args = new ArrayList<>();
        for (int i = 0; i < ops.size(); i++) {
            TableOp op = ops.get(i);
            if (i > 0) {
                sql.append(", ");
            }
            sql.append("(");
            boolean first = true;
            for (String c : cols) {
                if (!first) {
                    sql.append(", ");
                }
                first = false;
                if ("seq".equals(c) || (perWriter && writer.equals(c))) {
                    sql.append("?");
                    args.add(op.seq());
                } else {
                    Object v = op.fields().get(c);
                    sql.append("?");
                    args.add(v == null ? "" : v);
                }
            }
            sql.append(")");
        }
        sql.append(" ON DUPLICATE KEY UPDATE ");
        List<String> sets = new ArrayList<>();
        // 单源逻辑表启用整行 seq 字段裁判(防乱序/重放倒流);
        // 多源表(多表合并)禁用: 各源写不同字段, 行 seq 会被对方顶高, 整行裁判会误拒本源的合法更新
        boolean judge = ops.get(0).judge();
        for (String c : fieldCols) {
            if (perWriter) {
                // 写者列裁判: 只与本写者(任务×源)自己的 seq 比, 别的写者写高行 seq 不再误拒
                sets.add("`" + c + "` = IF(" + writer + " < VALUES(" + writer
                        + "), VALUES(`" + c + "`), `" + c + "`)");
            } else if (judge) {
                sets.add("`" + c + "` = IF(seq < VALUES(seq), VALUES(`" + c + "`), `" + c + "`)");
            } else {
                sets.add("`" + c + "` = VALUES(`" + c + "`)");
            }
        }
        if (perWriter) {
            sets.add(writer + " = GREATEST(" + writer + ", VALUES(" + writer + "))");
        }
        sets.add("seq = GREATEST(seq, VALUES(seq))");
        sql.append(String.join(", ", sets));
        targetJdbc.update(sql.toString(), args.toArray());
    }

    private void upsertInternal(TableOp op) {
        String table = op.table();
        List<String> keyColumns = op.keyColumns();
        Map<String, Object> fields = op.fields();
        long seq = op.seq();
        boolean perWriter = op.writer() != null && !op.writer().isBlank();
        String writer = perWriter ? op.writer() : null;
        List<String> cols = new ArrayList<>();
        List<Object> args = new ArrayList<>();
        for (String k : keyColumns) {
            cols.add("`" + k + "`");
            args.add(str(fields.get(k)));
        }
        for (Map.Entry<String, Object> e : fields.entrySet()) {
            if (keyColumns.contains(e.getKey())) {
                continue;
            }
            cols.add("`" + e.getKey() + "`");
            args.add(str(e.getValue()));
        }
        if (perWriter) {
            cols.add("`" + writer + "`");
            args.add(seq);
        }
        cols.add("`seq`");
        args.add(seq);

        StringBuilder sql = new StringBuilder("INSERT INTO `").append(table).append("` (")
                .append(String.join(", ", cols)).append(") VALUES (")
                .append(String.join(", ", cols.stream().map(c -> "?").toList()))
                .append(") ON DUPLICATE KEY UPDATE ");
        List<String> sets = new ArrayList<>();
        for (Map.Entry<String, Object> e : fields.entrySet()) {
            if (keyColumns.contains(e.getKey())) {
                continue;
            }
            // 单源表启用整行 seq 字段裁判(防乱序/重放倒流); 多源表禁用(行 seq 被对方顶高会误拒)
            if (perWriter) {
                sets.add("`" + e.getKey() + "` = IF(" + writer + " < VALUES(" + writer
                        + "), VALUES(`" + e.getKey() + "`), `" + e.getKey() + "`)");
            } else if (op.judge()) {
                sets.add("`" + e.getKey() + "` = IF(seq < VALUES(seq), VALUES(`" + e.getKey()
                        + "`), `" + e.getKey() + "`)");
            } else {
                sets.add("`" + e.getKey() + "` = VALUES(`" + e.getKey() + "`)");
            }
        }
        if (perWriter) {
            sets.add("`" + writer + "` = GREATEST(`" + writer + "`, VALUES(`" + writer + "`))");
        }
        sets.add("seq = GREATEST(seq, VALUES(seq))");
        sql.append(String.join(", ", sets));
        targetJdbc.update(sql.toString(), args.toArray());
    }

    private void deleteInternal(TableOp op) {
        List<String> conds = new ArrayList<>();
        List<Object> args = new ArrayList<>();
        for (String k : op.keyColumns()) {
            conds.add("`" + k + "` = ?");
            args.add(str(op.fields().get(k)));
        }
        if (op.writer() != null && !op.writer().isBlank()) {
            // 只允许本写者的新版本删行: 写者列 < 本次 seq 才删, 防旧删除重放误删新行
            conds.add("`" + op.writer() + "` < ?");
            args.add(op.seq());
        } else {
            conds.add("seq < ?");
            args.add(op.seq());
        }
        targetJdbc.update("DELETE FROM `" + op.table() + "` WHERE " + String.join(" AND ", conds), args.toArray());
    }

    /** 写者 seq 列名: 同一(任务实例 × 数据源 × 库 × 表)流共用一列, 各自比各自的 → 跨任务/跨源互不压制 */
    public static String writerColOf(String taskId, String datasourceId, String sourceDb, String sourceTable) {
        if (taskId == null || taskId.isBlank() || sourceDb == null || sourceDb.isBlank()
                || sourceTable == null || sourceTable.isBlank()) {
            return null;
        }
        String raw = "_s" + taskId + "_d" + (datasourceId == null ? "" : datasourceId)
                + "_" + sourceDb + "." + sourceTable;
        String col = raw.replaceAll("[^A-Za-z0-9_]", "_");
        if (col.length() > 60) {
            col = col.substring(0, 52) + "_" + md5Short(raw);
        }
        return col;
    }

    private static String md5Short(String s) {
        try {
            byte[] d = java.security.MessageDigest.getInstance("MD5")
                    .digest(s.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            StringBuilder h = new StringBuilder();
            for (byte b : d) {
                h.append(String.format("%02x", b & 0xff));
            }
            return h.substring(0, 8);
        } catch (Exception e) {
            return Integer.toHexString(s.hashCode());
        }
    }

    private static String str(Object o) {
        return o == null ? "" : String.valueOf(o);
    }
}
