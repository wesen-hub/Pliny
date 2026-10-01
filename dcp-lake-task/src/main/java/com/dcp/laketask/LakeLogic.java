package com.dcp.laketask;

import com.dcp.laketask.lake.IcebergLakeOps;
import com.dcp.laketask.lake.LakeCatalogFactory;
import com.dcp.laketask.lake.LakeOps;
import com.dcp.laketask.meta.JdbcMetaStore;
import com.dcp.laketask.meta.MetaStore;
import com.dcp.laketask.model.Json;
import com.dcp.outlet.sdk.Batch;
import com.dcp.outlet.sdk.ChangeRecord;
import com.dcp.outlet.sdk.ConsumerContext;
import com.dcp.outlet.sdk.ConsumerLogic;
import com.dcp.outlet.sdk.ConsumerLogicDef;
import com.dcp.outlet.sdk.LogicInitContext;
import com.dcp.outlet.sdk.Op;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * 湖增量消费底座任务(@ConsumerLogicDef lake-sync@1.0.0)。
 *
 * <p>消费出口栈 TiCDC 流(topic {@code dcp.cdc.{库}.{逻辑表}},**行数据模型**):after/before =
 * TiDB 目标表整行 —— 含业务列与写者列 {@code _s{taskId}_d{datasourceId}_{db}.{table}} 及行级
 * {@code seq}(均为同步链路 AtomicUpsertDao 落库的真实列)。语义与数据面消费端逐一对齐:
 * <ol>
 *   <li>INSERT/UPDATE 取 after, DELETE 取 before(TiCDC EnableOldValue 全旧行;缺 before → DLT);</li>
 *   <li>键值定位: 行内键列直取, 缺列回退解码器主键 r.key(), 仍缺 → DLT;
 *       TiCDC 已拆事件, 主键修改 = DELETE 旧行 + INSERT 新行两事件, 无需再拆;</li>
 *   <li>写者 seq 裁判: 事件内 after≠before 的写者列(_s 前缀)即本次写者,
 *       新值 &gt; 湖表旧值才应用(防重放/乱序); 无写者列回退行级 {@code seq};
 *       DELETE 按行级 {@code seq} 裁判(湖表行 seq ≤ 旧行 seq 才删 —— TiDB 行已删,
 *       旧删除重放不误删新行; 缺 seq 降级为无条件删, 依赖 EnableOldValue);</li>
 *   <li>字段级部分更新(多表合并语义): 只覆盖 after 相对 before 变化的业务列, 其余保留
 *       (与数据面 ODKU IF(writer&lt;VALUES) 同构);</li>
 *   <li>湖表不存在以行 schema 建表, 新字段/新写者缺列补列(schema evolution 与批作业同向);
 *       覆盖写 = OverwriteFiles.overwriteByRowFilter + add 一次 commit(等价 Spark MERGE);</li>
 *   <li>解析失败 → 落 dlt_message(PENDING, 丢弃不阻断); 湖写失败抛 → 底座不提交 offset
 *       整批重投(at-least-once, seq 裁判幂等)。</li>
 * </ol>
 */
@ConsumerLogicDef(logicId = "lake-sync", version = "1.0.0",
        description = "湖增量底座任务: 消费出口栈 TiCDC 流(dcp.cdc.*) → 写者 seq 裁判/字段级合并 → Iceberg 湖表覆盖写(与批作业 MERGE 同语义)")
public class LakeLogic implements ConsumerLogic {

    private static final Logger log = LoggerFactory.getLogger(LakeLogic.class);
    private static final ObjectMapper OM = new ObjectMapper();

    private LakeParams cfg;
    private MetaStore meta;
    private LakeOps lakeOps;
    private List<String> keys;
    private boolean closed;

    @Override
    public void init(LogicInitContext ctx) {
        this.cfg = OM.convertValue(ctx.params(), LakeParams.class);   // 失败抛
        if (cfg == null || cfg.logicalTable == null || cfg.logicalTable.isBlank()
                || cfg.jdbcUrl == null || cfg.jdbcUrl.isBlank()
                || cfg.lake == null || cfg.lake.namespace == null || cfg.lake.namespace.isBlank()) {
            throw new IllegalStateException("[lake-task] params 缺必填: logicalTable/jdbcUrl/lake.namespace");
        }
        this.meta = newMetaStore(cfg);      // 连接失败抛
        this.keys = cfg.entityKeyColumns == null || cfg.entityKeyColumns.isEmpty()
                ? meta.loadLt(cfg.logicalTable).keyColumns : cfg.entityKeyColumns;
        if (keys == null || keys.isEmpty()) {
            throw new IllegalStateException("[lake-task] 逻辑表 " + cfg.logicalTable
                    + " 未配置业务键(key_columns 为空), 请先发布模型/配置逻辑表");
        }
        String lakeTable = cfg.lake.table == null || cfg.lake.table.isBlank()
                ? cfg.logicalTable : cfg.lake.table;
        this.lakeOps = newLakeOps(cfg, lakeTable);   // catalog 连接失败抛
        ctx.log().info("[lake-task] ready table={} lake={}.{}.{} keys={}",
                cfg.logicalTable, cfg.lake.catalog, cfg.lake.namespace, lakeTable, keys);
    }

    /** 单测覆写点(LogicHarness 注入 fake 实现) */
    protected MetaStore newMetaStore(LakeParams p) {
        return new JdbcMetaStore(p.jdbcUrl, p.user, p.password);
    }

    /** 单测覆写点 */
    protected LakeOps newLakeOps(LakeParams p, String lakeTable) {
        return new IcebergLakeOps(LakeCatalogFactory.open(p.lake), p.lake.namespace, lakeTable);
    }

    @Override
    public void process(Batch batch, ConsumerContext ctx) {
        if (batch.records().isEmpty()) {
            return;
        }
        // ① 解析 TiCDC 行数据事件(INSERT/UPDATE 取 after, DELETE 取 before[EnableOldValue 全旧行])
        List<Ev> evs = new ArrayList<>();
        for (ChangeRecord r : batch.records()) {
            try {
                evs.add(parse(r));
            } catch (EvDlt d) {
                dlt(r, batch, "unknown", d.reason);
            }
        }
        if (evs.isEmpty()) {
            return;
        }

        // ② 写者 seq 裁判 + 字段级合并(read-modify-write, 数据面 ODKU 同构)
        //    touchedKeys = 事件涉及业务键(去重保序) → 读旧行 → 逐事件应用
        LinkedHashMap<String, List<String>> touched = new LinkedHashMap<>();
        Set<String> fieldCols = new LinkedHashSet<>();
        Set<String> seqCols = new LinkedHashSet<>();
        for (Ev ev : evs) {
            touched.putIfAbsent(ev.businessKey, ev.keyValues);
            fieldCols.addAll(ev.fields.keySet());
            fieldCols.addAll(keys);
            seqCols.add(ev.judgeCol);
            seqCols.add("seq");
        }
        List<List<String>> touchKeys = new ArrayList<>(touched.values());
        lakeOps.ensureTable(keys, new ArrayList<>(fieldCols), new ArrayList<>(seqCols));
        List<Map<String, String>> current = lakeOps.loadRows(keys, touchKeys);
        Map<String, Map<String, String>> working = new LinkedHashMap<>();
        for (int i = 0; i < touchKeys.size(); i++) {
            String bizKey = String.join("|", touchKeys.get(i));
            if (current.get(i) != null) {
                working.put(bizKey, new LinkedHashMap<>(current.get(i)));
            }
        }
        for (Ev ev : evs) {
            Map<String, String> row = working.get(ev.businessKey);
            if (ev.delete) {
                // TiDB 行已删: 湖表行级 seq ≤ 事件旧行 seq 才删(防旧删除重放误删新行)
                if (judgeOf(row, "seq") <= ev.judgeVal) {
                    working.put(ev.businessKey, null);
                }
            } else {
                long oldJudge = judgeOf(row, ev.judgeCol);
                if (oldJudge < ev.judgeVal) {
                    // 字段级部分更新: 只覆盖 after 相对 before 变化的列, 其余保留(多表合并时对方写者列不回退)
                    Map<String, String> base = row == null ? new LinkedHashMap<>() : row;
                    for (String k : keys) {
                        base.put(k, ev.keyValues.get(keys.indexOf(k)));
                    }
                    for (Map.Entry<String, Object> e : ev.fields.entrySet()) {
                        base.put(e.getKey(), e.getValue() == null ? null : String.valueOf(e.getValue()));
                    }
                    base.put(ev.judgeCol, String.valueOf(Math.max(oldJudge, ev.judgeVal)));
                    base.put("seq", String.valueOf(Math.max(seqOf(row), ev.seq)));
                    working.put(ev.businessKey, base);
                }
                // else: 旧事件重放/乱序 → 跳过(幂等, 不倒退)
            }
        }

        // ③ 覆盖写(一次 commit; 失败抛 → 底座不提交 offset 整批重投)
        List<Map<String, String>> upsertRows = new ArrayList<>(touchKeys.size());
        for (List<String> kv : touchKeys) {
            upsertRows.add(working.get(String.join("|", kv)));
        }
        lakeOps.rewrite(keys, touchKeys, upsertRows);
    }

    /* ─────────────── 事件解析/裁判 ─────────────── */

    /** 解析失败信号(落 DLT, 丢弃不阻断)。 */
    private static final class EvDlt extends RuntimeException {
        final String reason;

        EvDlt(String reason) {
            super(reason);
            this.reason = reason;
        }
    }

    /** 单条事件(解析后; judgeCol = 事件写者列/回退 seq) */
    private static final class Ev {
        final boolean delete;
        final String businessKey;
        final List<String> keyValues;
        final Map<String, Object> fields;
        final String judgeCol;
        final long judgeVal;
        final long seq;

        Ev(boolean delete, String businessKey, List<String> keyValues,
           Map<String, Object> fields, String judgeCol, long judgeVal, long seq) {
            this.delete = delete;
            this.businessKey = businessKey;
            this.keyValues = keyValues;
            this.fields = fields;
            this.judgeCol = judgeCol;
            this.judgeVal = judgeVal;
            this.seq = seq;
        }
    }

    /** 解析一条 TiCDC 行数据事件; 异常 → EvDlt(落 DLT)。 */
    private Ev parse(ChangeRecord r) throws EvDlt {
        boolean delete = r.op() == Op.DELETE;
        Map<String, Object> row = delete ? r.before() : r.after();
        if (row == null || row.isEmpty()) {
            throw new EvDlt(delete ? "DELETE 缺 before(TiCDC 需开启 EnableOldValue)"
                    : "行事件缺 after(变更数据)");
        }
        // 键值定位: 行内键列直取; 缺列回退解码器主键; 仍缺 → DLT
        List<String> keyValues = new ArrayList<>(keys.size());
        for (String k : keys) {
            Object v = row.get(k);
            if (v == null) {
                v = r.key() == null ? null : r.key().get(k);
            }
            keyValues.add(v == null ? null : String.valueOf(v));
        }
        if (keyValues.stream().anyMatch(Objects::isNull)) {
            throw new EvDlt("业务键列缺失(行内与主键均无): " + keys);
        }
        String businessKey = String.join("|", keyValues);

        if (delete) {
            // 裁判 = 旧行行级 seq(GREATEST): 湖表行 seq ≤ 旧行 seq 才删; 缺失 → 无条件删(降级, 依赖 EnableOldValue)
            long jv = seqOfRow(row);
            if (jv < 0) {
                log.warn("[lake-task] DELETE 旧行缺 seq 列(降级为无条件删): key={}", businessKey);
                jv = Long.MAX_VALUE;
            }
            return new Ev(true, businessKey, keyValues, Map.of(), "seq", jv, jv);
        }

        // INSERT/UPDATE: after 相对 before 变化的业务列 + 变化的写者列(_s 前缀)即本次写者
        Map<String, Object> fields = new LinkedHashMap<>();
        List<String> changedJudges = new ArrayList<>();
        for (Map.Entry<String, Object> e : row.entrySet()) {
            String c = e.getKey();
            if (isJudgeCol(c)) {
                if (r.before() == null || !Objects.equals(e.getValue(), r.before().get(c))) {
                    changedJudges.add(c);
                }
                continue;
            }
            if ("seq".equals(c)) {
                continue;
            }
            if (r.before() == null || !Objects.equals(e.getValue(), r.before().get(c))) {
                fields.put(c, e.getValue());
            }
        }
        String judgeCol = changedJudges.size() == 1 ? changedJudges.get(0) : "seq";
        long judgeVal = judgeCol.equals("seq") ? seqOfRow(row) : parseLong(row.get(judgeCol));
        if (judgeVal < 0) {
            log.warn("[lake-task] 裁判列 {} 缺失/非法, 按 0 处理(仅新行会应用): key={}",
                    judgeCol, businessKey);
            judgeVal = 0;
        }
        long seq = seqOfRow(row);
        if (seq < 0) {
            seq = judgeVal;
        }
        return new Ev(false, businessKey, keyValues, fields, judgeCol, judgeVal, seq);
    }

    /** 写者 seq 裁判列(同步链路 AtomicUpsertDao 落库形态: _s 前缀) */
    private static boolean isJudgeCol(String col) {
        return col != null && col.startsWith("_s");
    }

    /** 行内 seq(GREATEST)列值; 缺失/非法 → -1 */
    private static long seqOfRow(Map<String, Object> row) {
        return parseLong(row.get("seq"));
    }

    private static long parseLong(Object v) {
        if (v == null) {
            return -1;
        }
        try {
            return Long.parseLong(String.valueOf(v));
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    /** 行内裁判列旧值(无行/无列/非法 → -1, 即任何 seq 都是新事件) */
    private static long judgeOf(Map<String, String> row, String judgeCol) {
        if (row == null) {
            return -1;
        }
        String v = judgeCol == null ? null : row.get(judgeCol);
        if (v == null) {
            return -1;
        }
        try {
            return Long.parseLong(v);
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    private static long seqOf(Map<String, String> row) {
        if (row == null) {
            return -1;
        }
        String v = row.get("seq");
        if (v == null) {
            return -1;
        }
        try {
            return Long.parseLong(v);
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    private void dlt(ChangeRecord r, Batch batch, String businessKey, String error) {
        meta.dlt(cfg.logicalTable, batch.stream(), batch.stream(), r.partition(), r.offset(),
                businessKey, payloadOf(r), error);
    }

    private static String payloadOf(ChangeRecord r) {
        try {
            Map<String, Object> row = r.after() != null && !r.after().isEmpty() ? r.after() : r.before();
            return Json.toJson(row);
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        if (lakeOps != null) {
            try {
                lakeOps.close();
            } catch (Exception ignore) {
            }
        }
        if (meta != null) {
            try {
                meta.close();
            } catch (Exception ignore) {
            }
        }
    }
}
