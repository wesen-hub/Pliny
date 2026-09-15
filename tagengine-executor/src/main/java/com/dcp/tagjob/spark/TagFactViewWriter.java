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
                    "INSERT INTO `" + v.table + "`(task_id, entity, tag, tag_dict_id, value, run_ts) VALUES(?,?,?,?,?,?)")) {
                for (Row r : data) {
                    ps.setLong(1, taskId);
                    ps.setString(2, r.getString(0));
                    String tagPath = r.getString(1);
                    ps.setString(3, tagPath);
                    Long tid = cfg.tagIds == null ? null : cfg.tagIds.get(tagPath);
                    if (tid == null) ps.setNull(4, java.sql.Types.BIGINT); else ps.setLong(4, tid);
                    Object val = r.get(2);
                    ps.setString(5, val == null ? null : String.valueOf(val));
                    Object ts = r.get(3);
                    ps.setTimestamp(6, ts instanceof java.sql.Timestamp t ? t
                            : ts instanceof java.time.LocalDateTime ldt ? java.sql.Timestamp.valueOf(ldt)
                            : new java.sql.Timestamp(System.currentTimeMillis()));
                    ps.addBatch();
                }
                ps.executeBatch();
            }
            // P1.2④: 变更审计(与投影同轮; 失败仅记日志, 不阻断投影)
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO tag_notify(entity, tag_path, tag_dict_id, tag_value, task_id, task_mode)"
                            + " VALUES(?,?,?,?,?,'batch')"
                            + " ON DUPLICATE KEY UPDATE tag_value=VALUES(tag_value), tag_dict_id=VALUES(tag_dict_id), task_mode='batch', occur_ts=CURRENT_TIMESTAMP(3)")) {
                for (Row r : data) {
                    ps.setString(1, r.getString(0));
                    String tagPath = r.getString(1);
                    ps.setString(2, tagPath);
                    Long tid = cfg.tagIds == null ? null : cfg.tagIds.get(tagPath);
                    if (tid == null) ps.setNull(3, java.sql.Types.BIGINT); else ps.setLong(3, tid);
                    Object val = r.get(2);
                    ps.setString(4, val == null ? null : String.valueOf(val));
                    ps.setLong(5, taskId);
                    ps.addBatch();
                }
                ps.executeBatch();
            } catch (Exception ne) {
                Log.error("[投影] tag_notify 审计写失败(不影响投影): " + ne.getMessage());
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
                    + "tag_dict_id BIGINT NULL,"
                    + "value VARCHAR(255) NULL,"
                    + "run_ts DATETIME NULL,"
                    + "PRIMARY KEY (task_id, entity, tag))");
            // P1.2④: 标签变更审计(订阅消费端); 与 backend/stream 同一 tag_notify 契约
            st.execute("CREATE TABLE IF NOT EXISTS tag_notify ("
                    + "entity VARCHAR(128) NOT NULL, tag_path VARCHAR(512) NOT NULL,"
                    + "tag_dict_id BIGINT NULL,"
                    + "tag_value VARCHAR(128) NULL, task_id BIGINT NOT NULL,"
                    + "task_mode VARCHAR(16) NOT NULL,"
                    + "occur_ts DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),"
                    + "PRIMARY KEY (entity, tag_path, task_id))");
        }
        // 已存在的表补列(幂等; 不支持 ADD COLUMN IF NOT EXISTS 的方言靠异常忽略)
        addColumnIfMissing(conn, table, "tag_dict_id", "BIGINT NULL");
        addColumnIfMissing(conn, "tag_notify", "tag_dict_id", "BIGINT NULL");
    }

    private static void addColumnIfMissing(Connection conn, String table, String col, String type) {
        try (Statement st = conn.createStatement()) {
            st.execute("ALTER TABLE `" + table + "` ADD COLUMN " + col + " " + type);
        } catch (Exception ignored) {
            // 列已存在
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
