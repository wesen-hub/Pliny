package com.dcp.outlet.adapters;

import com.dcp.outlet.control.*;
import com.dcp.outlet.worker.RawMessage;
import com.dcp.outlet.worker.RecordSource;

import java.sql.Connection;
import java.sql.Statement;
import java.util.List;

/**
 * 消费基层自检(**真 Kafka + 真 MySQL**):
 * ① 锚点索引器常驻组件能持续消费并落锚点/水位;② 计算面真消费源(assign/seek/commitSync)可用;
 * ③ 由锚点算出的起点能被真实消费源精确接上(半开语义)。
 */
public final class FoundationCheck {

    private static final String BOOTSTRAP = "127.0.0.1:19092";
    private static final String JDBC = "jdbc:mysql://127.0.0.1:3306/dcp?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Asia/Shanghai";
    private static final String TOPIC = "dcp.cdc.target_db.rt_sync_c";
    private static final int EPOCH = 1;
    private static int passed, failed;

    public static void main(String[] args) throws Exception {
        DriverManagerDataSource ds = new DriverManagerDataSource(JDBC, "root", "root123");
        JdbcAnchorStore anchors = new JdbcAnchorStore(ds);
        JdbcWatermarkStore watermarks = new JdbcWatermarkStore(ds);
        try (Connection c = ds.getConnection(); Statement st = c.createStatement()) {
            st.executeUpdate("DELETE FROM chg_anchor WHERE topic='" + TOPIC + "'");
            st.executeUpdate("DELETE FROM chg_watermark_latest WHERE topic='" + TOPIC + "'");
        }

        // ① 索引器常驻组件:以"实时模式"启动(从 latest 起,索引**新增**事件 —— 这正是它的运行形态)
        AnchorIndexer indexer = new AnchorIndexer(anchors, watermarks,
                new AnchorIndexPolicy(5, 1000L, 7L * 24 * 3600 * 1000, 500),
                new MonotonicityGuard(), () -> System.currentTimeMillis());
        IndexerRunner runner = new IndexerRunner(BOOTSTRAP, "dcp.cdc.target_db.", indexer, EPOCH, false);
        runner.startAsync();
        Thread.sleep(2000);                       // 等订阅生效
        System.out.println("  索引器已常驻启动(实时模式),开始写入新 DML…");
        writeDml(30);
        Thread.sleep(15_000);                     // 等 changefeed → kafka → 索引器

        IndexerRunner.Stats stats = runner.stats();
        System.out.println("  索引器统计: " + stats);
        check("索引器消费到新增事件(>0)", stats.events > 0, true);
        check("索引器消费到水位(>0)", stats.watermarks > 0, true);
        check("索引器落锚点(>0)", stats.anchors > 0, true);
        runner.close();

        int anchorRows = anchors.list(TOPIC, 0, EPOCH).size() + anchors.list(TOPIC, 1, EPOCH).size()
                + anchors.list(TOPIC, 2, EPOCH).size();
        System.out.println("  MySQL 锚点行数 = " + anchorRows);
        check("锚点已落 MySQL", anchorRows > 0, true);
        check("水位已落 MySQL", watermarks.latest(TOPIC, 0, EPOCH) != null, true);

        // ② + ③ 起点计算 + 真消费源:用"锚点里的真实 commitTs"作 T0
        KafkaPartitionReader reader = new KafkaPartitionReader(BOOTSTRAP);
        long t0 = 1L;
        outer:
        for (int p = 0; p < 3; p++) {
            List<Anchor> list = anchors.list(TOPIC, p, EPOCH);
            if (list.size() >= 3) { t0 = list.get(list.size() / 2).commitTs(); break outer; }
        }
        System.out.println("  T0 = " + t0 + "(取自锚点,真实 commitTs)");
        check("锚点数量足以选 T0", t0 > 1L, true);

        StartOffsetService svc = new StartOffsetService(anchors, reader,
                new AnchorIndexPolicy(5, 1000L, 7L * 24 * 3600 * 1000, 500), null, 200_000);
        StartOffset so = svc.startOffset(TOPIC, 0, EPOCH, t0);
        System.out.println("  起点: " + so);
        check("走锚点快路径", so.source() == StartOffset.Source.ANCHOR, true);

        try (RecordSource source = new KafkaRecordSource(BOOTSTRAP, "foundation-check-" + System.nanoTime(),
                TOPIC, List.of(0))) {
            source.seek(0, so.offset());
            long firstDmlTs = -1L;
            long firstDmlOffset = -1L;
            long deadline = System.currentTimeMillis() + 10_000;
            while (firstDmlTs < 0 && System.currentTimeMillis() < deadline) {
                for (RawMessage m : source.poll(500)) {
                    long ts = CanalJson.commitTs(new String(m.payload(), java.nio.charset.StandardCharsets.UTF_8));
                    if (ts > 0) { firstDmlTs = ts; firstDmlOffset = m.offset(); break; }
                }
            }
            check("真消费源能读到数据", firstDmlTs > 0 || so.source() == StartOffset.Source.TAIL, true);
            System.out.println("  起点后首条 DML: offset=" + firstDmlOffset + " commitTs=" + firstDmlTs);
            if (firstDmlTs > 0) {
                check("起点之后第一条 DML 的 commitTs > T0(半开语义)", firstDmlTs > t0, true);
            }
            source.commitSync(0, so.offset() + 1);
            check("位点提交无异常", true, true);
        }
        reader.close();

        System.out.println();
        System.out.println("==================================================");
        System.out.println("  通过 " + passed + " 项, 失败 " + failed + " 项");
        System.out.println("==================================================");
        if (failed > 0) System.exit(1);
    }

    /** 用**多条独立事务**写 DML(同一事务会共享 commitTs,压不到快路径)。 */
    private static void writeDml(int rows) {
        StringBuilder sb = new StringBuilder();
        long base = System.currentTimeMillis() % 100000;
        for (int i = 1; i <= rows; i++) {
            sb.append("INSERT INTO target_db.rt_sync_c (id,v,seq) VALUES (")
              .append(40000 + base + i).append(",'found-").append(i).append("',").append(i)
              .append(") ON DUPLICATE KEY UPDATE v=VALUES(v),seq=VALUES(seq);\n");
        }
        try {
            Process p = new ProcessBuilder("docker", "exec", "-i", "dcp-mysql",
                    "mysql", "-h", "dcp-outlet-tidb1", "-P", "4000", "-uroot", "-proot123")
                    .redirectErrorStream(true).start();
            p.getOutputStream().write(sb.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8));
            p.getOutputStream().close();
            p.waitFor();
        } catch (Exception e) {
            System.out.println("  写 DML 失败: " + e);
        }
    }

    private static void check(String what, Object actual, Object expected) {
        boolean ok = String.valueOf(actual).equals(String.valueOf(expected));
        if (ok) passed++; else failed++;
        System.out.println((ok ? "  PASS  " : "  FAIL  ") + what + " = " + actual
                + (ok ? "" : "   (期望 " + expected + ")"));
    }
}
