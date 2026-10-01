package com.dcp.outlet.adapters;

import com.dcp.outlet.worker.ContractView;
import com.dcp.outlet.worker.DefaultContractFilter;
import com.dcp.outlet.sdk.ChangeRecord;
import com.dcp.outlet.worker.DecodedMessage;
import com.dcp.outlet.sdk.Op;
import com.sun.net.httpserver.HttpServer;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/** 契约视图走 HTTP 下发:往返、裁剪生效、缓存、**失败安全(fail-closed)**。 */
public final class ContractViewHttpCheck {

    private static int passed, failed;
    private static final String STREAM = "rt_sync_c";

    public static void main(String[] args) throws Exception {
        AtomicInteger hits = new AtomicInteger();
        Map<String, String> viewKv = ContractViewCodec.encode("ct-es-001", "c-es-001", STREAM,
                new LinkedHashSet<>(List.of("u_id", "v", "phone")),
                Map.of("phone", "MASK_PHONE"), 2, "status = OK");
        String body = ContractViewCodec.toQuery(viewKv);

        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/api/outlet/contracts", ex -> {
            hits.incrementAndGet();
            byte[] b = body.getBytes(StandardCharsets.UTF_8);
            ex.sendResponseHeaders(200, b.length);
            ex.getResponseBody().write(b);
            ex.close();
        });
        server.start();
        String base = "http://127.0.0.1:" + server.getAddress().getPort();

        HttpContractProvider provider = new HttpContractProvider(base, "ct-es-001", "c-es-001", STREAM);
        ContractView v = provider.current();
        check("HTTP 拉取:允许列", v.columnWhitelist(STREAM), new LinkedHashSet<>(List.of("u_id", "v", "phone")));
        check("HTTP 拉取:脱敏规则", v.maskRules(), Map.of("phone", "MASK_PHONE"));
        check("HTTP 拉取:行条件", v.rowCondition(), "status = OK");
        check("HTTP 拉取:密级上限", v.maxLevel(), 2);

        provider.current();
        check("第二次命中缓存(不再请求)", hits.get(), 1);

        // 计算面裁剪:越权列(secret)不出现 + 脱敏生效 + 行条件用原始行
        DefaultContractFilter filter = new DefaultContractFilter(
                (cond, row) -> "OK".equals(String.valueOf(row.get("status"))),
                new PlatformMasker());
        Map<String, Object> after = new LinkedHashMap<>();
        after.put("u_id", "u-1"); after.put("v", "x"); after.put("phone", "13800001111");
        after.put("secret", "S3CR3T"); after.put("status", "OK");
        DecodedMessage e = DecodedMessage.event(0, 1L, 1000L, STREAM, Op.INSERT,
                Map.of("u_id", "u-1"), new LinkedHashMap<>(), after);
        List<ChangeRecord> kept = filter.apply(v, STREAM, List.of(e));
        check("计算面裁剪:保留 1 条", kept.size(), 1);
        check("计算面裁剪:越权列消失", kept.get(0).after().containsKey("secret"), false);
        check("计算面裁剪:脱敏生效", kept.get(0).after().get("phone"), "138****11");
        check("计算面裁剪:行条件列被裁掉但仍可用", kept.get(0).after().containsKey("status"), false);

        Map<String, Object> blocked = new LinkedHashMap<>(after);
        blocked.put("status", "BLOCKED");
        DecodedMessage e2 = DecodedMessage.event(0, 2L, 1001L, STREAM, Op.UPDATE,
                Map.of("u_id", "u-1"), new LinkedHashMap<>(), blocked);
        check("行外事件被丢弃(墓碑语义由逻辑侧承接)", filter.apply(v, STREAM, List.of(e2)).size(), 0);

        // 失败安全:服务停掉 + 新 provider(无缓存)→ 必须抛,不得全放行
        server.stop(0);
        boolean threw = false;
        try {
            new HttpContractProvider(base, "ct-es-001", "c-es-001", STREAM).current();
        } catch (RuntimeException ex) {
            threw = true;
        }
        check("fail-closed:拉不到契约且无缓存 → 拒绝消费", threw, true);

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
