package com.dcp.outlet.control;

import com.dcp.outlet.control.support.ArrayPartitionReader;
import com.dcp.outlet.control.support.InMemoryAnchorStore;
import com.dcp.outlet.control.support.InMemoryPoisonStore;
import com.dcp.outlet.control.support.InMemoryWatermarkStore;

import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 起点计算 + 栅栏索引的离线自检(零依赖,不需要 Kafka / 数据库)。
 *
 * <p>核心主张(栅栏方案 §3):
 * ① 索引器只采刷新点(乱序点永不入库), 10s 窗口取最后一个, 不伪造;
 * ② 起点 = "commitTs ≤ T0 的最大栅栏" 的 offset(纯索引查询, 永不失败);
 * ③ 从起点起"第一条 commitTs > T0"的位置(出口过滤语义)与暴力解逐个一致;
 * ④ 解析失败污染(C1)后不再产栅栏; 守卫仅观测, 不驱动起点降级。
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

        // ---------- 用例 1:栅栏采样(只采刷新点; 可控时钟每事件 +1s, 10s 窗口取最后一个)----------
        InMemoryAnchorStore anchors = new InMemoryAnchorStore();
        MonotonicityGuard guard = new MonotonicityGuard();
        AtomicLong clock = new AtomicLong();
        AnchorIndexer indexer = new AnchorIndexer(anchors, new InMemoryWatermarkStore(),
                new AnchorIndexPolicy(5000, 10_000L, 7L * 24 * 3600 * 1000, 500), guard, clock::get);
        for (int p = 0; p < 3; p++) {
            long[] ts = tsOf(reader, topic, p);
            for (int i = 0; i < ts.length; i++) {
                clock.addAndGet(1000L);              // 每事件 1s:20k 事件 = 20ks = 2000 个 10s 窗口
                indexer.onEvent(topic, p, epoch, i, ts[i]);
            }
        }
        // 单调流全部是刷新点:首个刷新点开窗(窗口内取"最后", 首点不落), 窗口到点落"窗口内最后刷新点";
        // 末窗口无后继刷新点 → 不伪造 → 每分区 1999 个栅栏点
        long expectedFences = (rows - 1) / 10;
        check("单调流栅栏点数(每分区 " + expectedFences + ")", indexer.sampledCount(topic, 0), expectedFences);
        check("单调流无回退", guard.violationCount(topic, 0), 0L);
        check("入库点均为栅栏", anchors.list(topic, 0, epoch).stream().allMatch(Anchor::fence), true);

        StartOffsetService svc = new StartOffsetService(anchors, reader);

        // ---------- 用例 2:模糊测试 —— 从起点首扫"第一条 >T0"与暴力解逐个一致 ----------
        Random rnd = new Random(20260915L);
        int mismatches = 0, cases = 0, fenceHits = 0, fallbacks = 0;
        for (int p = 0; p < 3; p++) {
            long[] ts = tsOf(reader, topic, p);
            for (int i = 0; i < 300; i++) {
                int off = rnd.nextInt(rows);
                long tso = ts[off] + (rnd.nextBoolean() ? 0 : rnd.nextInt(3));
                StartOffset got = svc.startOffset(topic, p, epoch, tso);
                long firstAbove = scanFirstAbove(reader, topic, p, got.offset(), tso);
                long want = reader.bruteForce(topic, p, tso);
                cases++;
                if (firstAbove != want) {
                    mismatches++;
                    if (mismatches <= 3)
                        System.out.println("    MISMATCH p" + p + " tso=" + tso
                                + " got=" + got.offset() + " firstAbove=" + firstAbove + " want=" + want);
                }
                if (got.source() == StartOffset.Source.FENCE) fenceHits++;
                else fallbacks++;
            }
        }
        check("模糊测试用例数", cases, 900);
        check("与暴力解不一致数", mismatches, 0);
        check("栅栏命中占比 ≥ 95%", fenceHits >= (int) (cases * 0.95), true);
        System.out.println("    栅栏命中 " + fenceHits + " / " + cases + ", 退化回扫 " + fallbacks);

        // ---------- 用例 3:冷启动(无栅栏)→ FALLBACK_EARLIEST, 出口过滤兜底仍正确 ----------
        StartOffsetService cold = new StartOffsetService(new InMemoryAnchorStore(), reader);
        long probe = tsOf(reader, topic, 0)[12_345];
        StartOffset coldRes = cold.startOffset(topic, 0, epoch, probe);
        check("冷启动来源", coldRes.source(), StartOffset.Source.FALLBACK_EARLIEST);
        check("冷启动起点 = 0", coldRes.offset(), 0L);
        check("冷启动首扫仍正确", scanFirstAbove(reader, topic, 0, 0L, probe), reader.bruteForce(topic, 0, probe));

        // ---------- 用例 4:文档 §3.5 乱序流示例(T0=102 → 栅栏 offset4 → 首扫命中 6)----------
        InMemoryAnchorStore docStore = new InMemoryAnchorStore();
        long[] tsArr = {0, 80, 90, 85, 100, 95, 110, 101, 120};   // 文档是 offset 1..8, index 0 为占位
        AnchorIndexer docIdx = new AnchorIndexer(docStore, new InMemoryWatermarkStore(),
                new AnchorIndexPolicy(5000, 10_000L, 7L * 24 * 3600 * 1000, 500),
                new MonotonicityGuard(), () -> 0L);
        for (int i = 1; i < tsArr.length; i++) {
            docIdx.onEvent(topic, 0, epoch, i, tsArr[i]);       // 时钟恒 0:窗口永不闭合
        }
        check("乱序流窗口未闭合不伪造(0 落库)", docStore.saveCalls(), 0L);
        // 手铺栅栏点(真实索引器产出形态)+ 一条"旧盲采样残留"的非栅栏点(offset 7, ts=101)
        docStore.save(new Anchor(topic, 0, epoch, 1, 80, 1L, true));
        docStore.save(new Anchor(topic, 0, epoch, 2, 90, 1L, true));
        docStore.save(new Anchor(topic, 0, epoch, 4, 100, 1L, true));
        docStore.save(new Anchor(topic, 0, epoch, 6, 110, 1L, true));
        docStore.save(new Anchor(topic, 0, epoch, 8, 120, 1L, true));
        docStore.save(new Anchor(topic, 0, epoch, 7, 101, 1L, false));   // 非栅栏:若被选中会漏 110
        ArrayPartitionReader docReader = new ArrayPartitionReader();
        docReader.put(topic, 0, tsArr);
        StartOffset docRes = new StartOffsetService(docStore, docReader).startOffset(topic, 0, epoch, 102);
        check("乱序流起点来源", docRes.source(), StartOffset.Source.FENCE);
        check("栅栏 = offset 4(commitTs 100)", docRes.offset(), 4L);
        check("首扫第一条 >102 = offset 6(110)", scanFirstAbove(docReader, topic, 0, 4L, 102), 6L);

        // ---------- 用例 5:乱序点永不入库 + 守卫仅观测(不驱动起点降级)----------
        InMemoryAnchorStore oooStore = new InMemoryAnchorStore();
        MonotonicityGuard oooGuard = new MonotonicityGuard();
        AtomicLong oooClock = new AtomicLong();
        AnchorIndexer ooo = new AnchorIndexer(oooStore, new InMemoryWatermarkStore(),
                new AnchorIndexPolicy(5000, 10_000L, 7L * 24 * 3600 * 1000, 500), oooGuard, oooClock::get);
        long[] oooTs = {100, 200, 150, 300};                     // offset 2(150)为乱序点
        for (int i = 0; i < oooTs.length; i++) {
            oooClock.addAndGet(20_000L);                         // 每事件跨 2 个窗口:刷新点必触发落库
            ooo.onEvent(topic, 0, epoch, i, oooTs[i]);
        }
        // 事件0 开窗;事件1 落 (0,100);事件2 乱序不入库也不更新窗口候选;事件3 落 (1,200)
        List<Anchor> oooAnchors = oooStore.list(topic, 0, epoch);
        check("乱序流落库栅栏数", (long) oooAnchors.size(), 2L);
        check("乱序点(offset 2)不在库中", oooAnchors.stream().noneMatch(a -> a.offset() == 2), true);
        check("回退被记录(仅观测)", oooGuard.violationCount(topic, 0), 1L);
        check("守卫标记 tainted 保留(不驱动降级)", oooGuard.tainted(topic, 0), true);

        // ---------- 用例 6:解析失败污染(C1)→ 不再产栅栏 ----------
        InMemoryAnchorStore psnStore = new InMemoryAnchorStore();
        AtomicLong psnClock = new AtomicLong();
        AnchorIndexer psn = new AnchorIndexer(psnStore, new InMemoryWatermarkStore(),
                new AnchorIndexPolicy(5000, 10_000L, 7L * 24 * 3600 * 1000, 500),
                new MonotonicityGuard(), psnClock::get);
        psn.onPoison(topic, 0);
        check("污染标记生效", psn.poisoned(topic, 0), true);
        psnClock.addAndGet(20_000L);
        check("污染后刷新点不落库", psn.onEvent(topic, 0, epoch, 0, 500L), false);
        check("污染后落库数为 0", psnStore.saveCalls(), 0L);

        // ---------- 用例 6b:污染死信持久化(C1 跨重启)---------
        InMemoryPoisonStore dead = new InMemoryPoisonStore();
        InMemoryAnchorStore deadStore = new InMemoryAnchorStore();
        AnchorIndexer deadA = new AnchorIndexer(deadStore, new InMemoryWatermarkStore(),
                new AnchorIndexPolicy(5000, 10_000L, 7L * 24 * 3600 * 1000, 500),
                new MonotonicityGuard(), () -> 0L).withPoisonStore(dead, epoch);
        deadA.onPoison(topic, 1);
        check("污染已落死信库", dead.poisoned(topic, 1, epoch), true);
        check("死信库记录条数", (long) dead.list().size(), 1L);
        // 模拟进程重启:新实例从死信库恢复(不重复 mark / 告警)
        AnchorIndexer deadB = new AnchorIndexer(deadStore, new InMemoryWatermarkStore(),
                new AnchorIndexPolicy(5000, 10_000L, 7L * 24 * 3600 * 1000, 500),
                new MonotonicityGuard(), () -> 0L).withPoisonStore(dead, epoch);
        check("重启后污染恢复", deadB.poisoned(topic, 1), true);
        check("重启后污染分区不产栅栏", deadB.onEvent(topic, 1, epoch, 0, 500L), false);
        check("重启后污染分区落库数为 0", deadStore.saveCalls(), 0L);
        check("观测列出污染分区", deadB.poisonedPartitions().contains(topic + "#1"), true);

        // ---------- 用例 7:TSO 超过所有事件 → 命中最后栅栏 ----------
        long future = tsOf(reader, topic, 2)[rows - 1] + 10_000L;
        StartOffset tail = svc.startOffset(topic, 2, epoch, future);
        check("TSO 超尾 → FENCE(最后栅栏)", tail.source(), StartOffset.Source.FENCE);
        check("最后栅栏 offset = 19990", tail.offset(), (long) (rows - 10));
        check("首扫到末尾(无 >T0 事件)", scanFirstAbove(reader, topic, 2, tail.offset(), future), (long) rows);

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

    /** 从 from 起顺序扫, 返回第一条 commitTs > tso 的 offset(出口过滤语义); 无则返回分区末尾。 */
    private static long scanFirstAbove(PartitionReader reader, String topic, int p, long from, long tso) {
        long cur = from;
        long latest = reader.latestOffset(topic, p);
        while (cur < latest) {
            List<TsoSample> batch = reader.readFrom(topic, p, cur, 500);
            if (batch.isEmpty()) break;
            for (TsoSample s : batch) {
                if (s.commitTs() > tso) return s.offset();
            }
            cur = batch.get(batch.size() - 1).offset() + 1;
        }
        return latest;
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
