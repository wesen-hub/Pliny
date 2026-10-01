package com.dcp.outlet.adapters;

import com.dcp.outlet.sdk.ChangeRecord;
import com.dcp.outlet.sdk.InMemoryLogicRegistry;
import com.dcp.outlet.sdk.Op;
import com.dcp.outlet.sdk.SdkLogger;
import com.dcp.outlet.worker.ClasspathLogicLoader;
import com.dcp.outlet.worker.ContractProvider;
import com.dcp.outlet.worker.ContractProviderFactory;
import com.dcp.outlet.worker.CreateUnitCmd;
import com.dcp.outlet.worker.DecodedMessage;
import com.dcp.outlet.worker.DefaultContractFilter;
import com.dcp.outlet.worker.DeadLetterSink;
import com.dcp.outlet.worker.RecordSource;
import com.dcp.outlet.worker.RecordSourceFactory;
import com.dcp.outlet.worker.UnitMetrics;
import com.dcp.outlet.worker.UnitRunner;
import com.dcp.outlet.worker.WatermarkProvider;
import com.sun.net.httpserver.HttpServer;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * **契约刷新端到端自检**:权限「收窄 / 吊销」在**一个刷新周期内**生效,且全程 fail-closed。
 *
 * <p>验的是**生产装配的那条链**:
 * {@link HttpContractProviderFactory}(按单元 contractRef 拉视图)
 * → {@link HttpContractProvider}(缓存 + 刷新 + 失败沿用)
 * → {@link DefaultContractFilter} + {@link ExpressionRowConditionEvaluator} + {@link PlatformMasker}(真裁剪)
 * → {@link UnitRunner}(单元级 fail-closed:拉不到契约就**不开消费源**)。
 *
 * <p>编排:自建 HTTP 服务扮演控制面(JDK 内置,零依赖),服务端视图可在运行中切换:
 * 正常 → **收窄**(去列 + 换行条件)→ **吊销**(空视图)→ **500**(刷新失败)。
 * 校验点是"生效延迟 = 一个刷新周期",而不是具体 30 秒 —— 生产周期由
 * {@code HttpContractProviderFactory} 默认值 30s 给出(本自检也断言该默认值)。
 */
public final class ContractRefreshCheck {

    private static int passed, failed;
    private static final String STREAM = "rt_sync_c";
    private static final String REF = "ct-e2e";

    /** 服务端当前视图(可运行中切换)。 */
    private static volatile Set<String> serverCols = new LinkedHashSet<>(List.of("u_id", "v", "phone"));
    private static volatile String serverRowCond = "status = OK";
    private static volatile int failNext = 0;              // >0 → 下 N 次请求返回 500
    private static final AtomicInteger hits = new AtomicInteger();

    public static void main(String[] args) throws Exception {
        System.out.println("=== 契约刷新端到端:收窄 / 吊销 / 刷新失败 / fail-closed ===");

        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/api/outlet/contracts", ex -> {
            hits.incrementAndGet();
            if (failNext > 0) {
                failNext--;
                ex.sendResponseHeaders(500, -1);
                ex.close();
                return;
            }
            Map<String, String> kv = ContractViewCodec.encode(REF, "c-e2e", STREAM, serverCols,
                    Map.of("phone", "MASK_PHONE"), 2, serverRowCond);
            byte[] b = ContractViewCodec.toQuery(kv).getBytes(StandardCharsets.UTF_8);
            ex.sendResponseHeaders(200, b.length);
            ex.getResponseBody().write(b);
            ex.close();
        });
        server.start();
        String base = "http://127.0.0.1:" + server.getAddress().getPort();

        DefaultContractFilter filter = new DefaultContractFilter(
                new ExpressionRowConditionEvaluator(), new PlatformMasker(), null);

        // ---------- ① 长周期提供者:缓存命中 + TTL 内改权限不生效 ----------
        ContractProviderFactory slow = new HttpContractProviderFactory(base, Duration.ofSeconds(2), 3_000L);
        ContractProvider p1 = slow.forUnit(unit(REF));
        var v1 = p1.current();
        check("① 首次拉取:可见列", v1.columnWhitelist(STREAM),
                new LinkedHashSet<>(List.of("u_id", "v", "phone")));
        check("① 首次拉取:行条件", v1.rowCondition(), "status = OK");
        check("① 首次拉取用了 1 次 HTTP", hits.get(), 1);

        p1.current();
        check("① TTL 内重复取用缓存(不再请求)", hits.get(), 1);

        serverCols = new LinkedHashSet<>(List.of("u_id", "phone"));      // 收窄:去掉 v
        serverRowCond = "status IN (OK, NEW)";                           // 换成集合条件
        var stillOld = p1.current();
        check("① TTL 内权限收窄**尚未**生效(仍是旧视图)", stillOld.columnWhitelist(STREAM).contains("v"), true);
        check("① TTL 内不产生额外请求", hits.get(), 1);

        // ---------- ② 短周期提供者:收窄 / 吊销在下一个刷新周期生效 ----------
        ContractProviderFactory fast = new HttpContractProviderFactory(base, Duration.ofSeconds(2), 300L);
        ContractProvider p2 = fast.forUnit(unit(REF));
        var v2 = p2.current();
        check("② 新提供者拿到收窄后的视图(无 v)", v2.columnWhitelist(STREAM).contains("v"), false);
        check("② 新视图行条件已更新", v2.rowCondition(), "status IN (OK, NEW)");

        // 真裁剪:集合条件生效(OK/NEW 放行,BLOCKED 丢弃)+ 脱敏 + 越权列消失
        check("② 行条件 IN:OK 行放行", cut(filter, v2, "OK").size(), 1);
        check("② 行条件 IN:NEW 行放行", cut(filter, v2, "NEW").size(), 1);
        check("② 行条件 IN:BLOCKED 行丢弃", cut(filter, v2, "BLOCKED").size(), 0);
        ChangeRecord kept = cut(filter, v2, "OK").get(0);
        check("② 越权列(secret)连 key 都不出现", kept.after().containsKey("secret"), false);
        check("② 行条件列被裁掉但仍可判(用原始行)", kept.after().containsKey("status"), false);
        check("② 脱敏生效", kept.after().get("phone"), "138****11");

        // 吊销:服务端下发放空视图(等价"契约被吊销")
        int hitsBeforeRevoke = hits.get();
        serverCols = new LinkedHashSet<>();
        check("② 吊销后 TTL 内仍用旧缓存", p2.current().columnWhitelist(STREAM).size(), 2);
        check("② 吊销后 TTL 内不产生请求", hits.get(), hitsBeforeRevoke);

        Thread.sleep(400);                                               // 等一个刷新周期
        var revoked = p2.current();
        check("② 下一个刷新周期后吊销生效:可见列为空", revoked.columnWhitelist(STREAM).size(), 0);
        check("② 吊销生效后请求确实发出", hits.get(), hitsBeforeRevoke + 1);
        check("② 吊销后所有行被丢(不得回退全放行)", cut(filter, revoked, "OK").size(), 0);

        // ---------- ③ 刷新失败:沿用缓存(不放大权限) ----------
        failNext = 1;
        Thread.sleep(400);
        var afterFail = p2.current();
        check("③ 刷新失败(500)沿用上次视图(仍是吊销后的空视图)",
                afterFail.columnWhitelist(STREAM).size(), 0);
        check("③ 失败时确实尝试过刷新", hits.get(), hitsBeforeRevoke + 2);

        // ---------- ④ 冷启动失败:无缓存 → 拒绝消费 ----------
        failNext = 5;
        boolean cold = false;
        try {
            ContractProvider coldProvider = fast.forUnit(unit(REF));
            coldProvider.current();
        } catch (RuntimeException e) {
            cold = true;
            check("④ 冷启动失败原因明确", e.getMessage().contains("无缓存"), true);
        }
        check("④ 无缓存且拉不到 → 抛异常(拒绝消费)", cold, true);

        // ---------- ⑤ 单元没带 contractRef:直接拒 ----------
        boolean noRef = false;
        try {
            fast.forUnit(new CreateUnitCmd().unitId("u-noref").stream(STREAM)).current();
        } catch (RuntimeException e) {
            noRef = true;
            check("⑤ 缺 contractRef 的报错可读", e.getMessage().contains("contractRef"), true);
        }
        check("⑤ 单元缺 contractRef → 拒绝消费(fail-closed)", noRef, true);

        // ---------- ⑥ 生产口径:默认刷新周期 30s ----------
        check("⑥ 生产默认刷新周期 = 30s",
                new HttpContractProviderFactory(base).refreshMillis(), 30_000L);

        // ---------- ⑦ 单元级 fail-closed:契约不可得时**连消费源都不建** ----------
        checkUnitRefuses();

        server.stop(0);
        System.out.println();
        System.out.println("==================================================");
        System.out.println("  通过 " + passed + " 项, 失败 " + failed + " 项");
        System.out.println("==================================================");
        if (failed > 0) System.exit(1);
    }

    /** 单元级:契约拉不到 → 单元 FAILED,且消费源未被创建(不消费任何一条消息)。 */
    private static void checkUnitRefuses() throws Exception {
        AtomicInteger sourceCreated = new AtomicInteger();
        InMemoryLogicRegistry registry = new InMemoryLogicRegistry();
        registry.register("e2e-check", "1.0.0", (batch, ctx) -> { });

        RecordSourceFactory sources = cmd -> {
            sourceCreated.incrementAndGet();
            return new RecordSource() {
                @Override public List<com.dcp.outlet.worker.RawMessage> poll(long timeoutMillis) { return List.of(); }
                @Override public void seek(int partition, long offset) { }
                @Override public void commitSync(int partition, long offset) { }
                @Override public void wakeup() { }
                @Override public void close() { }
            };
        };

        ContractProviderFactory bad = cmd -> () -> {
            throw new IllegalStateException("控制面不可达且无缓存 —— 拒绝消费(fail-closed)");
        };

        UnitRunner runner = new UnitRunner(new ClasspathLogicLoader(registry), new SdkEventDecoder(),
                new DefaultContractFilter(new ExpressionRowConditionEvaluator(), new PlatformMasker(), null),
                sources,
                new WatermarkProvider() {
                    @Override public void update(int partition, long watermarkTs) { }
                    @Override public long safeWatermark(int partition) { return 0L; }
                },
                new DeadLetterSink() {
                    @Override public void record(String taskId, String unitId, int partition, long offset,
                                                 long tso, DecodedMessage message, Throwable error) { }
                },
                new UnitMetrics() {
                    @Override public void batchProcessed(int partition, int rows, long durationMillis, long tso) { }
                    @Override public void emptyBatch(int partition) { }
                    @Override public void failure(int partition, Throwable error) { }
                    @Override public void retry(int partition, int attempt) { }
                    @Override public void deadLetter(int partition) { }
                    @Override public void commit(int partition, long offset) { }
                },
                id -> silent(),
                bad);

        CreateUnitCmd cmd = new CreateUnitCmd()
                .unitId("u-nocontract").taskId("t-e2e").topic("dcp.cdc.target_db.rt_sync_c")
                .stream(STREAM).contractRef(REF).consumerGroup("task-e2e")
                .logic(com.dcp.outlet.sdk.LogicRef.of("e2e-check", "1.0.0"));
        runner.start(cmd);
        runner.awaitTermination("u-nocontract", 3_000);

        check("⑦ 契约不可得 → 单元 FAILED", runner.state("u-nocontract").name(), "FAILED");
        check("⑦ 契约不可得 → 未创建消费源(不消费)", sourceCreated.get(), 0);
        check("⑦ 失败原因可见", runner.lastError("u-nocontract") != null, true);
    }

    /* ------------------------------------------------------------------ 工具 */

    private static CreateUnitCmd unit(String contractRef) {
        return new CreateUnitCmd().unitId("u-e2e-0").taskId("t-e2e")
                .topic("dcp.cdc.target_db.rt_sync_c").stream(STREAM)
                .contractRef(contractRef).consumerGroup("task-e2e")
                .logic(com.dcp.outlet.sdk.LogicRef.of("e2e-check", "1.0.0"));
    }

    /** 造一条事件(含越权列 secret、脱敏列 phone、行条件列 status)并按视图裁剪。 */
    private static List<ChangeRecord> cut(DefaultContractFilter filter,
                                          com.dcp.outlet.worker.ContractView view, String status) {
        Map<String, Object> after = new LinkedHashMap<>();
        after.put("u_id", "u-1");
        after.put("v", "x");
        after.put("phone", "13800001111");
        after.put("secret", "S3CR3T");
        after.put("status", status);
        DecodedMessage e = DecodedMessage.event(0, 1L, 1000L, STREAM, Op.INSERT,
                Map.of("u_id", "u-1"), new LinkedHashMap<>(), after);
        return filter.apply(view, STREAM, List.of(e));
    }

    private static SdkLogger silent() {
        return new SdkLogger() {
            @Override public boolean isDebugEnabled() { return false; }
            @Override public void debug(String format, Object... args) { }
            @Override public void info(String format, Object... args) { }
            @Override public void warn(String format, Object... args) { }
            @Override public void error(String message, Throwable t) { }
            @Override public void error(String format, Object... args) { }
        };
    }

    private static void check(String what, Object actual, Object expected) {
        boolean ok = String.valueOf(actual).equals(String.valueOf(expected));
        if (ok) passed++; else failed++;
        System.out.println((ok ? "  PASS  " : "  FAIL  ") + what
                + (ok ? "" : "   (实际 " + actual + ",期望 " + expected + ")"));
    }
}
