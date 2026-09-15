package com.dcp.outlet.control;

import com.dcp.outlet.control.support.ArrayPartitionReader;
import com.dcp.outlet.control.support.InMemoryAnchorStore;
import com.dcp.outlet.control.support.InMemoryWatermarkStore;

import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * 起点计算 + 锚点索引的离线自检(零依赖,不需要 Kafka / 数据库)。
 *
 * <p>核心主张:**"锚点二分 + 短扫"得到的起点,必须与"暴力扫描"逐个一致**;
 * 并且在无锚点时能优雅退化,在回扫超限时明确失败(而不是静默给错起点)。
 */
public final class SelfCheck {

    private static int passed, failed;

    public static void main(String[] args) {
        String topic = "dcp.cdc.target_db.rt_sync_c";
        int epoch = 1;
        int rows = 20_000;
        long startTso = 4_691_003_002_443_530_27L;
        long step = 1000L;
        int txSize = 3;

        System.out.println("== 建数据:3 个分区 × " + rows + " 行(txSize=" + txSize + ", 同事务共享 commitTs)==");
        ArrayPartitionReader reader = new ArrayPartitionReader();
        for (int p = 0; p < 3; p++) {
            reader.putMonotonic(topic, p, rows, startTso + p * 77_000L, step, txSize);
        }

        // ---------- 用例 1:锚点索引(每 100 行采一条)----------
        InMemoryAnchorStore anchors = new InMemoryAnchorStore();
        MonotonicityGuard guard = new MonotonicityGuard();
        AnchorIndexer indexer = new AnchorIndexer(anchors, new InMemoryWatermarkStore(),
                new AnchorIndexPolicy(100, 10_000L, 7L * 24 * 3600 * 1000, 500), guard,
                () -> System.currentTimeMillis());
        for (int p = 0; p < 3; p++) {
            long[] ts = tsOf(reader, topic, p);
            for (int i = 0; i < ts.length; i++) indexer.onEvent(topic, p, epoch, i, ts[i]);
        }
        check("索引器落锚点数(3 × 20000 / 100)", indexer.sampledCount(topic, 0), 200);
        check("单调性回退次数", guard.violationCount(topic, 0), 0);

        StartOffsetService svc = new StartOffsetService(anchors, reader,
                new AnchorIndexPolicy(100, 10_000L, 7L * 24 * 3600 * 1000, 500), guard, 200_000);

        // ---------- 用例 2:模糊测试 —— 与暴力解逐个一致 ----------
        Random rnd = new Random(20260915L);
        int mismatches = 0, cases = 0, maxScanned = 0, anchorHits = 0;
        StartOffset.Source[] seen = new StartOffset.Source[1];
        for (int p = 0; p < 3; p++) {
            long[] ts = tsOf(reader, topic, p);
            for (int i = 0; i < 300; i++) {
                int off = rnd.nextInt(rows);
                long tso = ts[off] + (rnd.nextBoolean() ? 0 : rnd.nextInt(3));
                StartOffset got = svc.startOffset(topic, p, epoch, tso);
                long want = reader.bruteForce(topic, p, tso);
                cases++;
                if (got.offset() != want) {
                    mismatches++;
                    if (mismatches <= 3)
                        System.out.println("    MISMATCH p" + p + " tso=" + tso
                                + " got=" + got.offset() + " want=" + want);
                }
                if (got.source() == StartOffset.Source.ANCHOR) {
                    anchorHits++;
                    maxScanned = Math.max(maxScanned, got.scannedRows());
                }
                seen[0] = got.source();
            }
        }
        check("模糊测试用例数", cases, 900);
        check("与暴力解不一致数", mismatches, 0);
        check("锚点命中占比 ≥ 95%", anchorHits >= (int) (cases * 0.95), true);
        check("短扫长度有界(≤ 采样间隔+事务)120", maxScanned <= 120, true);
        System.out.println("    最长短扫 = " + maxScanned + " 行(全量 " + rows + " 行/分区)");

        // ---------- 用例 3:冷启动 —— 无锚点走时间索引,仍然正确 ----------
        StartOffsetService cold = new StartOffsetService(new InMemoryAnchorStore(), reader,
                AnchorIndexPolicy.defaults(), null, 200_000);
        long probe = tsOf(reader, topic, 0)[12_345];
        StartOffset coldRes = cold.startOffset(topic, 0, epoch, probe);
        check("冷启动来源", coldRes.source(), StartOffset.Source.TIME_INDEX);
        check("冷启动起点正确", coldRes.offset(), reader.bruteForce(topic, 0, probe));

        // ---------- 用例 4:兜底回扫(时间索引不可用)----------
        PartitionReader noTime = new PartitionReader() {
            @Override public long earliestOffset(String t, int p) { return 0; }
            @Override public long latestOffset(String t, int p) { return reader.latestOffset(t, p); }
            @Override public List<TsoSample> readFrom(String t, int p, long f, int m) { return reader.readFrom(t, p, f, m); }
            @Override public Long offsetForTime(String t, int p, long ms) { return null; }
        };
        StartOffset fallback = new StartOffsetService(new InMemoryAnchorStore(), noTime,
                AnchorIndexPolicy.defaults(), null, 200_000).startOffset(topic, 1, epoch, probe);
        check("兜底来源", fallback.source(), StartOffset.Source.RESCAN_FROM_EARLIEST);
        check("兜底起点正确", fallback.offset(), reader.bruteForce(topic, 1, probe));

        // ---------- 用例 5:回扫超限 → START_TOO_OLD(明确失败,不静默给错)----------
        boolean threw = false;
        try {
            new StartOffsetService(new InMemoryAnchorStore(), noTime,
                    AnchorIndexPolicy.defaults(), null, 50).startOffset(topic, 0, epoch, probe);
        } catch (StartTooOldException e) {
            threw = true;
            check("异常携带已扫条数", e.scannedRows(), 50);
        }
        check("超限抛 START_TOO_OLD", threw, true);

        // ---------- 用例 6:TSO 超过所有事件 → TAIL ----------
        long future = tsOf(reader, topic, 2)[rows - 1] + 10_000L;
        StartOffset tail = svc.startOffset(topic, 2, epoch, future);
        check("无新事件 → TAIL", tail.source(), StartOffset.Source.TAIL);
        check("TAIL 起点 = 分区末尾", tail.offset(), (long) rows);

        // ---------- 用例 7:单调性回退 → 索引不可信 → 强制降级回扫 ----------
        MonotonicityGuard bad = new MonotonicityGuard();
        bad.onSample(topic, 0, 5_000_000_000L);
        bad.onSample(topic, 0, 4_000_000_000L);          // 回退
        check("回退被记录", bad.violationCount(topic, 0), 1L);
        check("分区被标记为索引不可信", bad.tainted(topic, 0), true);
        StartOffset degraded = new StartOffsetService(anchors, reader,
                AnchorIndexPolicy.defaults(), bad, 200_000).startOffset(topic, 0, epoch, probe);
        check("降级来源", degraded.source(), StartOffset.Source.RESCAN_DEGRADED);
        check("降级后起点仍正确", degraded.offset(), reader.bruteForce(topic, 0, probe));

        // ---------- 用例 8:水位单调写入 ----------
        InMemoryWatermarkStore wm = new InMemoryWatermarkStore();
        AnchorIndexer idx2 = new AnchorIndexer(new InMemoryAnchorStore(), wm);
        idx2.onWatermark(topic, 0, epoch, 999, 5_000L);
        idx2.onWatermark(topic, 0, epoch, 1000, 4_000L);      // 乱序更旧
        check("水位不被旧值回退", wm.latest(topic, 0, epoch).watermarkTs(), 5_000L);
        idx2.onWatermark(topic, 0, epoch, 1001, 6_000L);
        check("水位可推进", wm.latest(topic, 0, epoch).watermarkTs(), 6_000L);

        // ---------- 用例 9:保留期清理 ----------
        InMemoryAnchorStore prunable = new InMemoryAnchorStore();
        prunable.save(new Anchor(topic, 0, epoch, 1, 10, 1_000L));
        prunable.save(new Anchor(topic, 0, epoch, 2, 20, 90_000L));
        check("清理过期锚点条数", prunable.prune(50_000L), 1);
        check("剩余锚点数", prunable.list(topic, 0, epoch).size(), 1);

        System.out.println();
        System.out.println("==================================================");
        System.out.println("  通过 " + passed + " 项, 失败 " + failed + " 项");
        System.out.println("==================================================");
        if (failed > 0) System.exit(1);
    }

    private static long[] tsOf(ArrayPartitionReader r, String topic, int p) {
        // 复用读接口把 commitTs 取出来(测试便利)
        int rows = (int) r.latestOffset(topic, p);
        long[] out = new long[rows];
        for (int i = 0; i < rows; i++) {
            out[i] = r.readFrom(topic, p, i, 1).get(0).commitTs();
        }
        return out;
    }

    private static void check(String what, Object actual, Object expected) {
        boolean ok = String.valueOf(actual).equals(String.valueOf(expected));
        if (ok) passed++; else failed++;
        System.out.println((ok ? "  PASS  " : "  FAIL  ") + what + " = " + actual
                + (ok ? "" : "   (期望 " + expected + ")"));
    }
}
