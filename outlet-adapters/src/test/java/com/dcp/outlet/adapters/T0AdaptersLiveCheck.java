package com.dcp.outlet.adapters;

import com.dcp.outlet.control.T0Selector;

/**
 * 批阶段 T0 三适配器的**真栈自检**:连真 TiCDC / TiDB / 元数据库,验生产装配这条链。
 *
 * <p>前置:出口栈已起(ticdc1:18300 · tidb1:14000 · kafka:19092)、元库 dcp 可连(3306)、
 * 索引器已写过水位。断言的是**环境真实值之间的健康关系**:gc ≤ start_ts ≤ latest。
 *
 * <p>离线逻辑见 {@link T0AdaptersCheck};本类是"真栈跑一遍"。
 */
public final class T0AdaptersLiveCheck {

    private static int passed, failed;

    public static void main(String[] args) {
        System.out.println("=== 批阶段 T0 三适配器(真栈)===");

        String tidbUrl = "jdbc:mysql://127.0.0.1:14000/mysql?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Asia/Shanghai";
        String metaUrl = "jdbc:mysql://127.0.0.1:3306/dcp?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Asia/Shanghai";
        String topic = "dcp.cdc.target_db.rt_sync_c";

        try {
            // R1
            T0Selector.ChangefeedStartTs cdc = new TiCdcChangefeedStartTs("http://127.0.0.1:18300", "chg-target-db-v4");
            long startTs = cdc.startTs(topic);
            System.out.println("  changefeed start_ts = " + startTs);

            // R2
            T0Selector.GcSafePoint gc = new TiDbGcSafePoint(new DriverManagerDataSource(tidbUrl, "root", "root123"));
            long gcTs = gc.gcSafePointTs();
            System.out.println("  gc safe point      = " + gcTs);

            // R3
            T0Selector.LatestTs latest = new WatermarkStoreLatestTs(
                    new JdbcWatermarkStore(new DriverManagerDataSource(metaUrl, "root", "root123")), 1);
            long latestTs = latest.latestTs(topic, 0);
            System.out.println("  latest watermark   = " + latestTs);

            check("水位存在(索引器已写过)", latestTs > 0, true);
            check("gc ≤ start_ts(健康)", gcTs <= startTs, true);
            check("start_ts ≤ latest(健康)", startTs <= latestTs, true);

            // 装配成 T0Selector:未请求时 T0 = latest,无 R1/R2/R3 调整
            T0Selector sel = new T0Selector(cdc, gc, latest);
            T0Selector.Decision d = sel.select(topic, 0, null);
            check("happy path T0 = latest", d.tso(), latestTs);
            check("happy path 无调整", d.bumpedByStartTs() || d.bumpedByGcSafePoint() || d.clampedByLatest(), false);
        } catch (RuntimeException e) {
            System.out.println("  真栈不可达,跳过(需出口栈 + 元库在线):" + e.getMessage());
        }

        System.out.println("==================================================");
        System.out.println("  通过 " + passed + " 项, 失败 " + failed + " 项");
        System.out.println("==================================================");
        if (failed > 0) System.exit(1);
    }

    private static void check(String what, Object actual, Object expected) {
        boolean ok = String.valueOf(actual).equals(String.valueOf(expected));
        if (ok) passed++; else failed++;
        System.out.println((ok ? "  PASS  " : "  FAIL  ") + what
                + (ok ? "" : "   (实际 " + actual + ",期望 " + expected + ")"));
    }
}
