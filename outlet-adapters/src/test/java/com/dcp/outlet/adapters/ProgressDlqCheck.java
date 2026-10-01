package com.dcp.outlet.adapters;

import com.dcp.outlet.control.*;
import com.dcp.outlet.sdk.Op;
import com.dcp.outlet.worker.DecodedMessage;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

/** 真栈:进度口径(框架快照位点 + 水位 + 分区末端)与死信落库。 */
public final class ProgressDlqCheck {

    private static final String BOOTSTRAP = "127.0.0.1:19092";
    private static final String JDBC = "jdbc:mysql://127.0.0.1:3306/dcp?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Asia/Shanghai";
    private static final String TOPIC = "dcp.cdc.target_db.rt_sync_c";
    private static final String TASK = "selfcheck-progress";
    private static int passed, failed;

    public static void main(String[] args) throws Exception {
        DriverManagerDataSource ds = new DriverManagerDataSource(JDBC, "root", "root123");
        JdbcWatermarkStore watermarks = new JdbcWatermarkStore(ds);
        JdbcDeadLetterSink dlq = new JdbcDeadLetterSink(ds);

        // 清理死信残留(彻底版无单元表)
        try (Connection c = ds.getConnection(); Statement st = c.createStatement()) {
            st.executeUpdate("DELETE FROM consumer_dlq WHERE task_id='" + TASK + "'");
        }

        // 水位(真实写入)
        KafkaPartitionReader reader = new KafkaPartitionReader(BOOTSTRAP);
        long latest0 = reader.latestOffset(TOPIC, 2);
        watermarks.upsert(new Watermark(TOPIC, 2, 1, 469104200000000000L, latest0, 0));

        ProgressService progress = new ProgressService(reader, watermarks, () -> System.currentTimeMillis());

        // 任务级进度:直接按分区列展开;分区状态/位点由框架快照派生,此处模拟框架快照的 committedOffset
        List<PartitionProgress> rows = progress.progressOfTask(TASK, TOPIC, List.of(2), 1,
                Map.of(2, 3500L), Map.of(2, "RUNNING"), true);
        check("进度按分区展开", rows.size(), 1);
        PartitionProgress r = rows.get(0);
        System.out.println("  进度: " + r);
        check("框架口径:无 TSO → 滞后归 0", r.lagSeconds(), 0L);
        check("积压条数已算出(≥0)", r.lagRows() >= 0, true);

        // 死信落库(unit_id 语义已变:填 taskId)
        DecodedMessage msg = DecodedMessage.event(2, 3501L, 469104100000000000L, "rt_sync_c", Op.INSERT,
                Map.of("id", "x"), Map.of(), Map.of("id", "x"));
        dlq.record(TASK, TASK, TOPIC, 2, 3501L, 469104100000000000L, msg,
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
