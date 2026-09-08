package com.dcp.tagjob.spark;

import com.dcp.tagjob.config.TaskConfig;
import com.dcp.tagjob.util.Log;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * 标签服务投影写(设计 §9 P1: 订阅/圈选/实体查标签查询面)。
 *
 * <p>tag_fact(Iceberg 湖) 为权威+历史; 平台后端无湖读通道, 交互查询(圈选/实体标签/订阅命中)
 * 走 JDBC 投影表 tag_fact_view(task_id, entity, tag, value, run_ts)。每轮执行完:
 *  全量: DELETE 本 task 旧行 + 全量插(当前值快照);
 *  增量: DELETE 本 task 变更实体旧行 + 只插变更实体(与湖增量同实体集, 见调用方传入)。
 * 投影为可查副本, 湖仍唯一事实源(time travel 留史)。
 */
public final class TagFactViewWriter {

    private TagFactViewWriter() {
    }

    /** 投影 tag_fact 当前值到 JDBC 表; rows 已按 (entity,tag) 去重。全量覆盖 task; 增量按实体集删+插。 */
    public static void write(Dataset<Row> rows, TaskConfig cfg, long taskId, IncrementalSnapshot incr) {
        TaskConfig.ViewRef v = cfg.tagFactView;
        if (!v.enabled()) {
            return;
        }
        java.util.List<Row> data = rows.select("entity", "tag", "value", "run_ts").collectAsList();
        if (data.isEmpty()) {
            return;
        }
        long t0 = System.currentTimeMillis();
        try (Connection conn = DriverManager.getConnection(v.url, v.user, v.password)) {
            ensureTable(conn, v.table);
            // 删除范围: 全量=整 task; 增量=task+变更实体(含墓碑 → 其旧行即被清, 无新行)
            if (incr != null && incr.active && incr.hasChanged()) {
                deleteByEntities(conn, v.table, taskId, incr.changedEntities);
            } else {
                try (PreparedStatement ps = conn.prepareStatement("DELETE FROM `" + v.table + "` WHERE task_id=?")) {
                    ps.setLong(1, taskId);
                    ps.executeUpdate();
                }
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO `" + v.table + "`(task_id, entity, tag, value, run_ts) VALUES(?,?,?,?,?)")) {
                for (Row r : data) {
                    ps.setLong(1, taskId);
                    ps.setString(2, r.getString(0));
                    ps.setString(3, r.getString(1));
                    Object val = r.get(2);
                    ps.setString(4, val == null ? null : String.valueOf(val));
                    Object ts = r.get(3);
                    ps.setTimestamp(5, ts instanceof java.sql.Timestamp t ? t
                            : ts instanceof java.time.LocalDateTime ldt ? java.sql.Timestamp.valueOf(ldt)
                            : new java.sql.Timestamp(System.currentTimeMillis()));
                    ps.addBatch();
                }
                ps.executeBatch();
            }
            Log.info("[投影] tag_fact_view 已同步: task=" + taskId + " 行=" + data.size()
                    + (incr != null && incr.active ? "(增量实体 " + (incr.changedEntities == null ? 0 : incr.changedEntities.size()) + ")" : "(全量)")
                    + " 耗时=" + (System.currentTimeMillis() - t0) + "ms");
        } catch (Exception e) {
            // 投影失败不阻断主流程(湖已写成功); 记错误
            Log.error("[投影] tag_fact_view 同步失败(不影响湖写): " + e.getMessage());
        }
    }

    private static void ensureTable(Connection conn, String table) throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE IF NOT EXISTS `" + table + "` ("
                    + "task_id BIGINT NOT NULL,"
                    + "entity VARCHAR(191) NOT NULL,"
                    + "tag VARCHAR(191) NOT NULL,"
                    + "value VARCHAR(255) NULL,"
                    + "run_ts DATETIME NULL,"
                    + "PRIMARY KEY (task_id, entity, tag))");
        }
    }

    private static void deleteByEntities(Connection conn, String table, long taskId, List<String> entities) throws Exception {
        if (entities == null || entities.isEmpty()) {
            return;
        }
        // 分批 IN(单批 500); entity 单引号转义
        List<String> batch = new ArrayList<>();
        for (String e : entities) {
            batch.add(e.replace("'", "\\'"));
            if (batch.size() >= 500) {
                deleteBatch(conn, table, taskId, batch);
                batch.clear();
            }
        }
        if (!batch.isEmpty()) {
            deleteBatch(conn, table, taskId, batch);
        }
    }

    private static void deleteBatch(Connection conn, String table, long taskId, List<String> entities) throws Exception {
        StringBuilder in = new StringBuilder();
        for (String e : entities) {
            if (in.length() > 0) in.append(',');
            in.append('\'').append(e).append('\'');
        }
        try (Statement st = conn.createStatement()) {
            st.executeUpdate("DELETE FROM `" + table + "` WHERE task_id=" + taskId
                    + " AND entity IN (" + in + ")");
        }
    }
}
