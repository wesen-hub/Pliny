package com.dcp.outlet.adapters;

import com.dcp.outlet.control.T0Selector;
import com.dcp.outlet.control.Watermark;
import com.dcp.outlet.control.WatermarkStore;
import com.sun.net.httpserver.HttpServer;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 批阶段 T0 三适配器的离线自检:验证三个生产适配器的**输入 → 输出**,不含真栈。
 *
 * <ul>
 *   <li>{@link TiCdcChangefeedStartTs}:自建 HTTP 扮演 TiCDC,验 start_ts 解析 + 缓存 + 失败抛异常</li>
 *   <li>{@link TiDbGcSafePoint}:验时间字符串 → TSO 的纯转换(硬编码真实 TSO 断言)+ 空值拒绝</li>
 *   <li>{@link WatermarkStoreLatestTs}:内存水位表,验按 epoch 取最新 + 缺失返回 0</li>
 *   <li>{@link T0Selector}:用三个真实适配器装配,验 happy path 与 R1 抬升</li>
 * </ul>
 *
 * <p>真栈(真 TiCDC / TiDB / 水位表)由 {@code T0AdaptersLiveCheck} 覆盖。
 */
public final class T0AdaptersCheck {

    private static int passed, failed;

    // 真实环境里的三个值(2026-09-16 环境实测):gc < start_ts < latest
    private static final long START_TS = 469104275733348361L;
    private static final long GC_TS = 469072638945263616L;      // = toTso("20260914-07:15:13.164 +0000")
    private static final long LATEST = 469105476951867395L;

    public static void main(String[] args) throws Exception {
        System.out.println("=== 批阶段 T0 三适配器(离线)===");

        checkTiCdc();
        checkGcToTso();
        checkGcSupplier();
        checkLatestTs();
        checkSelector();

        System.out.println("==================================================");
        System.out.println("  通过 " + passed + " 项, 失败 " + failed + " 项");
        System.out.println("==================================================");
        if (failed > 0) System.exit(1);
    }

    private static void checkTiCdc() throws Exception {
        AtomicInteger hits = new AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/api/v2/changefeeds/chg-target-db-v4", ex -> {
            hits.incrementAndGet();
            String body = "{\"id\":\"chg-target-db-v4\",\"state\":\"normal\",\"start_ts\":" + START_TS + ",\"config\":{}}";
            byte[] b = body.getBytes(StandardCharsets.UTF_8);
            ex.sendResponseHeaders(200, b.length);
            ex.getResponseBody().write(b);
            ex.close();
        });
        server.start();
        String base = "http://127.0.0.1:" + server.getAddress().getPort();

        TiCdcChangefeedStartTs cdc = new TiCdcChangefeedStartTs(base, "chg-target-db-v4");
        check("TiCDC start_ts 解析", cdc.startTs("dcp.cdc.target_db.rt_sync_c"), START_TS);
        check("TiCDC 第二次命中缓存", cdc.startTs("dcp.cdc.target_db.biz_orders"), START_TS);
        check("TiCDC 只请求一次(缓存生效)", hits.get(), 1);

        // 不存在的 changefeed → 404 → 抛异常
        TiCdcChangefeedStartTs bad = new TiCdcChangefeedStartTs(base, "no-such-cf");
        check("TiCDC 404 抛异常", expectThrow(() -> bad.startTs("t")), true);

        server.stop(0);
    }

    private static void checkGcToTso() {
        check("toTso 带毫秒", TiDbGcSafePoint.toTso("20260914-07:15:13.164 +0000"), GC_TS);
        check("toTso 无毫秒(= .000)", TiDbGcSafePoint.toTso("20260914-07:15:13 +0000"),
                469072638902272000L);
        check("toTso 前后空白容忍", TiDbGcSafePoint.toTso("  20260914-07:15:13.164 +0000  "), GC_TS);
    }

    private static void checkGcSupplier() {
        TiDbGcSafePoint gc = new TiDbGcSafePoint(() -> "20260914-07:15:13.164 +0000");
        check("GC supplier 正常 → TSO", gc.gcSafePointTs(), GC_TS);
        check("GC 空串抛异常", expectThrow(() -> new TiDbGcSafePoint(() -> "").gcSafePointTs()), true);
        check("GC null 抛异常", expectThrow(() -> new TiDbGcSafePoint(() -> null).gcSafePointTs()), true);
    }

    private static void checkLatestTs() {
        WatermarkStore mem = new MemWatermarkStore();
        mem.upsert(new Watermark("dcp.cdc.target_db.rt_sync_c", 0, 1, LATEST, 123L, 0L));
        mem.upsert(new Watermark("dcp.cdc.target_db.rt_sync_c", 0, 2, 999L, 0L, 0L));

        WatermarkStoreLatestTs latest = new WatermarkStoreLatestTs(mem);
        check("水位表取最新(epoch=1)", latest.latestTs("dcp.cdc.target_db.rt_sync_c", 0), LATEST);
        check("缺失分区返回 0", latest.latestTs("dcp.cdc.target_db.rt_sync_c", 5), 0L);
        check("自定义 epoch", new WatermarkStoreLatestTs(mem, 2)
                .latestTs("dcp.cdc.target_db.rt_sync_c", 0), 999L);
    }

    private static void checkSelector() {
        // 三个真实适配器装配(GC 用 supplier 注入真实值)
        T0Selector sel = new T0Selector(
                topic -> START_TS,
                new TiDbGcSafePoint(() -> "20260914-07:15:13.164 +0000"),
                (topic, p) -> LATEST);

        T0Selector.Decision happy = sel.select("t", 0, null);
        check("happy path:T0=latest 无调整", happy.tso(), LATEST);
        check("happy path 无 R1/R2/R3 标记",
                happy.bumpedByStartTs() || happy.bumpedByGcSafePoint() || happy.clampedByLatest(), false);

        T0Selector.Decision r1 = sel.select("t", 0, START_TS - 1);
        check("R1:请求低于 start_ts → 抬到 start_ts", r1.tso(), START_TS);
        check("R1 标记", r1.bumpedByStartTs(), true);
    }

    private static boolean expectThrow(Runnable r) {
        try {
            r.run();
            return false;
        } catch (RuntimeException e) {
            return true;
        }
    }

    private static void check(String what, Object actual, Object expected) {
        boolean ok = String.valueOf(actual).equals(String.valueOf(expected));
        if (ok) passed++; else failed++;
        System.out.println((ok ? "  PASS  " : "  FAIL  ") + what
                + (ok ? "" : "   (实际 " + actual + ",期望 " + expected + ")"));
    }

    private static final class MemWatermarkStore implements WatermarkStore {
        private final Map<String, Watermark> m = new LinkedHashMap<>();

        @Override public void upsert(Watermark w) {
            m.put(w.topic() + "#" + w.partition() + "#" + w.epoch(), w);
        }

        @Override public Watermark latest(String topic, int partition, int epoch) {
            return m.get(topic + "#" + partition + "#" + epoch);
        }
    }
}
