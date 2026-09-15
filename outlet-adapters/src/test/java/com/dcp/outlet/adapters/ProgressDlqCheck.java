package com.dcp.outlet.adapters;

import com.dcp.outlet.control.*;
import com.dcp.outlet.sdk.Op;
import com.dcp.outlet.worker.DecodedMessage;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

/** 真栈:进度口径(单元状态源 + 水位 + 分区末端)与死信落库。 */
public final class ProgressDlqCheck {

    private static final String BOOTSTRAP = "127.0.0.1:19092";
    private static final String JDBC = "jdbc:mysql://127.0.0.1:3306/dcp?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Asia/Shanghai";
    private static final String TOPIC = "dcp.cdc.target_db.rt_sync_c";
    private static final String TASK = "selfcheck-progress";
    private static int passed, failed;

    public static void main(String[] args) throws Exception {
        DriverManagerDataSource ds = new DriverManagerDataSource(JDBC, "root", "root123");
        JdbcUnitStatusStore statuses = new JdbcUnitStatusStore(ds);
        JdbcUnitStore units = new JdbcUnitStore(ds);
        JdbcWatermarkStore watermarks = new JdbcWatermarkStore(ds);
        JdbcDeadLetterSink dlq = new JdbcDeadLetterSink(ds);

        // 造一个任务 + 单元(带分区归属与心跳时间)
        String unitId = "u-" + TASK + "-0-1";
        try (Connection c = ds.getConnection(); Statement st = c.createStatement()) {
            st.executeUpdate("DELETE FROM consumer_unit_partition WHERE task_id='" + TASK + "'");
            st.executeUpdate("DELETE FROM consumer_unit WHERE task_id='" + TASK + "'");
            st.executeUpdate("DELETE FROM consumer_dlq WHERE task_id='" + TASK + "'");
            st.executeUpdate("INSERT INTO consumer_unit(unit_id,task_id,topic,stream,epoch,state,partitions,"
                    + "last_offset,last_tso,last_seen_at,dispatched_at) VALUES('" + unitId + "','" + TASK + "','"
                    + TOPIC + "','rt_sync_c',1,'RUNNING','2',3500,469104100000000000,NOW(),NOW())");
            st.executeUpdate("INSERT INTO consumer_unit_partition(unit_id,task_id,topic,partition_,epoch) "
                    + "VALUES('" + unitId + "','" + TASK + "','" + TOPIC + "',2,1)");
        }

        // 水位(真实写入)
        KafkaPartitionReader reader = new KafkaPartitionReader(BOOTSTRAP);
        long latest0 = reader.latestOffset(TOPIC, 2);
        watermarks.upsert(new Watermark(TOPIC, 2, 1, 469104200000000000L, latest0, 0));

        ProgressService progress = new ProgressService(reader, watermarks, () -> System.currentTimeMillis());
        List<PlannedUnit> pu = units.byTask(TASK);
        check("单元从库里读出", pu.size(), 1);
        check("分区归属读出", pu.get(0).partitions(), List.of(2));

        List<UnitStatusStore.UnitStatus> st = statuses.byTask(TASK);
        check("单元状态读出", st.size(), 1);
        check("心跳时间已读出(>0)", st.get(0).lastSeenAt > 0, true);
        check("下发时间已读出(>0)", st.get(0).dispatchedAt > 0, true);

        List<PartitionProgress> rows = progress.progressOfTask(TASK, pu, st, 1, true);
        check("进度按分区展开", rows.size(), 1);
        PartitionProgress r = rows.get(0);
        System.out.println("  进度: " + r);
        check("滞后秒数已算出(>0)", r.lagSeconds() > 0, true);
        check("积压条数已算出(≥0)", r.lagRows() >= 0, true);

        // 死信落库
        DecodedMessage msg = DecodedMessage.event(2, 3501L, 469104100000000000L, "rt_sync_c", Op.INSERT,
                Map.of("id", "x"), Map.of(), Map.of("id", "x"));
        dlq.record(TASK, unitId, TOPIC, 2, 3501L, 469104100000000000L, msg,
                new IllegalStateException("scripted sink failure"), 3);
        int dlqCount;
        try (Connection c = ds.getConnection(); Statement st2 = c.createStatement();
             ResultSet rs = st2.executeQuery("SELECT COUNT(*) FROM consumer_dlq WHERE task_id='" + TASK + "'")) {
            rs.next();
            dlqCount = rs.getInt(1);
        }
        check("死信已落库", dlqCount, 1);
        reader.close();

        System.out.println();
        System.out.println("==================================================");
        System.out.println("  通过 " + passed + " 项, 失败 " + failed + " 项");
        System.out.println("==================================================");
        if (failed > 0) System.exit(1);
    }

    private static void check(String what, Object actual, Object expected) {
        boolean ok = String.valueOf(actual).equals(String.valueOf(expected));
        if (ok) passed++; else failed++;
        System.out.println((ok ? "  PASS  " : "  FAIL  ") + what + " = " + actual
                + (ok ? "" : "   (期望 " + expected + ")"));
    }
}
