package com.dcp.outlet.adapters;

import com.dcp.outlet.control.*;

import java.sql.Connection;
import java.sql.Statement;
import java.util.*;

/**
 * 真栈自检:**真实 Kafka(19092)+ 真实 MySQL(3306)**。
 *
 * <p>验三件事:① 新表感知扫描(Kafka Admin → chg_stream 落库);
 * ② 锚点索引落到 MySQL 并可二分查询;③ **起点计算在真实数据上成立**
 * —— "从算出的 offset 读到的第一条,其 commitTs 一定 > T0;前一条一定 ≤ T0"。
 */
public final class RealStackCheck {

    private static final String BOOTSTRAP = "127.0.0.1:19092";
    private static final String JDBC = "jdbc:mysql://127.0.0.1:3306/dcp?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Asia/Shanghai";
    private static final String TOPIC = "dcp.cdc.target_db";      // 当前真实 sink topic(默认 topic)
    private static final int EPOCH = 1;
    private static int passed, failed;

    public static void main(String[] args) throws Exception {
        DriverManagerDataSource ds = new DriverManagerDataSource(JDBC, "root", "root123");
        try (Connection c = ds.getConnection()) {
            System.out.println("MySQL 连接 OK: " + c.getMetaData().getDatabaseProductVersion());
        }

        JdbcAnchorStore anchors = new JdbcAnchorStore(ds);
        JdbcWatermarkStore watermarks = new JdbcWatermarkStore(ds);
        JdbcStreamRegistry registry = new JdbcStreamRegistry(ds);
        KafkaPartitionReader reader = new KafkaPartitionReader(BOOTSTRAP);

        try {
            testScanner(registry);
            testTopicNameParsing();
            testAnchorsAndStartOffset(anchors, reader);
            testWatermark(watermarks, reader);
        } finally {
            reader.close();
        }

        System.out.println();
        System.out.println("==================================================");
        System.out.println("  通过 " + passed + " 项, 失败 " + failed + " 项");
        System.out.println("==================================================");
        if (failed > 0) System.exit(1);
    }

    // ---------- ① 新表感知 ----------
    private static void testScanner(JdbcStreamRegistry registry) {
        KafkaAdminStreamScanner scanner = new KafkaAdminStreamScanner(BOOTSTRAP, registry, "dcp.cdc.");
        int n = scanner.scanOnce();
        List<StreamInfo> streams = registry.list();
        System.out.println("  扫描到可登记的流(topic 形如 dcp.cdc.{schema}.{table}): " + n + " 个 " + streams);
        check("扫描器可对真实 Kafka 运行(无异常)", true, true);
        check("默认 topic(dcp.cdc.target_db)不被登记为流", streams.stream().noneMatch(s -> s.topic().equals(TOPIC)), true);
    }

    private static void testTopicNameParsing() {
        KafkaAdminStreamScanner s = new KafkaAdminStreamScanner(BOOTSTRAP, null, "dcp.cdc.");
        check("topic → 流名:dcp.cdc.target_db.rt_sync_c", s.streamOf("dcp.cdc.target_db.rt_sync_c"), "rt_sync_c");
        check("topic → 流名:默认 topic 返回 null", s.streamOf("dcp.cdc.target_db"), null);
        check("topic → 流名:无关 topic 返回 null", s.streamOf("dcp.rt_sync_c"), null);
    }

    // ---------- ②③ 锚点索引 + 起点计算(真实数据)----------
    private static void testAnchorsAndStartOffset(JdbcAnchorStore anchors, KafkaPartitionReader reader)
            throws Exception {
        // 清掉本 topic 的旧锚点,保证可重复
        try (Connection c = new DriverManagerDataSource(JDBC, "root", "root123").getConnection();
             Statement st = c.createStatement()) {
            st.executeUpdate("DELETE FROM chg_anchor WHERE topic='" + TOPIC + "'");
        }

        int partitions = 3;
        AnchorIndexer indexer = new AnchorIndexer(anchors, new JdbcWatermarkStore(
                new DriverManagerDataSource(JDBC, "root", "root123")),
                new AnchorIndexPolicy(5, 1000L, 7L * 24 * 3600 * 1000, 200),
                new MonotonicityGuard(), () -> System.currentTimeMillis());

        List<List<Long>> tsByPartition = new ArrayList<>();
        int totalSamples = 0;
        for (int p = 0; p < partitions; p++) {
            List<Long> dmlTs = new ArrayList<>();
            long latest = reader.latestOffset(TOPIC, p);
            long from = Math.max(0, latest - 4000);              // 只看尾部窗口(真实 DML 在这)
            long pos = from;
            while (pos < latest) {
                List<TsoSample> batch = reader.readFrom(TOPIC, p, pos, 500);
                if (batch.isEmpty()) break;
                for (TsoSample s : batch) {
                    indexer.onEvent(TOPIC, p, EPOCH, s.offset(), s.commitTs());
                    totalSamples++;
                    dmlTs.add(s.commitTs());
                }
                pos = batch.get(batch.size() - 1).offset() + 1;
            }
            tsByPartition.add(dmlTs);
            System.out.println("  p" + p + " 尾部窗口 [" + from + "," + latest + ") 采样 " + dmlTs.size() + " 条 DML");
        }

        check("真实数据上采到样本(>0)", totalSamples > 0, true);
        int anchorRows = anchors.list(TOPIC, 0, EPOCH).size()
                + anchors.list(TOPIC, 1, EPOCH).size() + anchors.list(TOPIC, 2, EPOCH).size();
        check("锚点已落到 MySQL(>0)", anchorRows > 0, true);
        System.out.println("  锚点行数=" + anchorRows + " 采样条数=" + totalSamples);

        StartOffsetService svc = new StartOffsetService(anchors, reader,
                new AnchorIndexPolicy(5, 1000L, 7L * 24 * 3600 * 1000, 200), null, 200_000);

        int fastPath = 0;
        for (int p = 0; p < partitions; p++) {
            List<Long> ts = tsByPartition.get(p);
            if (ts.size() < 3) { System.out.println("  p" + p + " 样本过少,跳过"); continue; }
            Collections.sort(ts);
            long t0 = ts.get(ts.size() / 2);                 // ★ 按分区取 T0(落在数据中间)
            StartOffset so = svc.startOffset(TOPIC, p, EPOCH, t0);
            if (so.source() == StartOffset.Source.ANCHOR) fastPath++;
            System.out.println("  p" + p + " T0=" + t0 + " → " + so + " (latest=" + reader.latestOffset(TOPIC, p) + ")");

            if (so.source() == StartOffset.Source.TAIL) {
                check("P" + p + " 无更新事件 → 起点=末尾", so.offset(), reader.latestOffset(TOPIC, p));
                continue;
            }
            // 属性 1:从起点读到的第一条 commitTs > T0
            List<TsoSample> at = reader.readFrom(TOPIC, p, so.offset(), 1);
            if (at.isEmpty()) {
                check("P" + p + " 起点处可读到消息", false, true);
            } else {
                check("P" + p + " 起点第一条 commitTs > T0", at.get(0).commitTs() > t0, true);
            }
            // 属性 2:前一条(若存在且能读到)commitTs ≤ T0
            if (so.offset() > 0) {
                List<TsoSample> prev = reader.readFrom(TOPIC, p, so.offset() - 1, 1);
                if (!prev.isEmpty()) {
                    check("P" + p + " 起点前一条 commitTs ≤ T0", prev.get(0).commitTs() <= t0, true);
                }
            }
        }
        check("至少一个分区走了锚点快路径(能力被真正压到)", fastPath > 0, true);
        System.out.println("  快路径(ANCHOR)命中分区数 = " + fastPath);
    }

    // ---------- ④ 水位落库 ----------
    private static void testWatermark(JdbcWatermarkStore watermarks, KafkaPartitionReader reader) throws Exception {
        try (Connection c = new DriverManagerDataSource(JDBC, "root", "root123").getConnection();
             Statement st = c.createStatement()) {
            st.executeUpdate("DELETE FROM chg_watermark_latest WHERE topic='" + TOPIC + "'");
        }
        long latest = reader.latestOffset(TOPIC, 0);
        watermarks.upsert(new Watermark(TOPIC, 0, EPOCH, 1_000L, latest - 10, 0));
        watermarks.upsert(new Watermark(TOPIC, 0, EPOCH, 500L, latest - 20, 0));      // 更旧 → 应被忽略
        check("水位单调不回退", watermarks.latest(TOPIC, 0, EPOCH).watermarkTs(), 1_000L);
        watermarks.upsert(new Watermark(TOPIC, 0, EPOCH, 2_000L, latest - 5, 0));
        check("水位可推进", watermarks.latest(TOPIC, 0, EPOCH).watermarkTs(), 2_000L);
        check("水位 offset 同步更新", watermarks.latest(TOPIC, 0, EPOCH).offset(), latest - 5);
    }

    private static void check(String what, Object actual, Object expected) {
        boolean ok = String.valueOf(actual).equals(String.valueOf(expected));
        if (ok) passed++; else failed++;
        System.out.println((ok ? "  PASS  " : "  FAIL  ") + what + " = " + actual
                + (ok ? "" : "   (期望 " + expected + ")"));
    }
}
