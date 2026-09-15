package com.dcp.outlet.adapters;

import com.dcp.outlet.control.DispatchRequest;
import com.dcp.outlet.control.FailureSpec;
import com.dcp.outlet.worker.CreateUnitCmd;
import com.sun.net.httpserver.HttpServer;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * **真 HTTP 往返**自检(不用 Spring):起一个 JDK 内置 HttpServer 冒充计算面,
 * 让控制面的 {@link HttpUnitDispatcher} 真发一次 CreateUnit/StopUnit,验证线上格式与落点。
 */
public final class HttpDispatchCheck {

    private static int passed, failed;

    public static void main(String[] args) throws Exception {
        Map<String, String> received = new LinkedHashMap<>();
        List<String> deleted = new ArrayList<>();

        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/units", exchange -> {
            String method = exchange.getRequestMethod();
            if ("DELETE".equals(method)) {
                deleted.add(exchange.getRequestURI().getPath());
                respond(exchange, 200, "{\"stopped\":true}");
                return;
            }
            for (String pair : exchange.getRequestURI().getQuery().split("&")) {
                int i = pair.indexOf('=');
                received.put(java.net.URLDecoder.decode(pair.substring(0, i), StandardCharsets.UTF_8),
                        java.net.URLDecoder.decode(pair.substring(i + 1), StandardCharsets.UTF_8));
            }
            respond(exchange, 200, "{\"accepted\":true}");
        });
        server.start();
        int port = server.getAddress().getPort();
        System.out.println("  假装的计算面监听 :" + port);

        Map<Integer, Long> starts = new LinkedHashMap<>();
        starts.put(1, 31785L);
        DispatchRequest req = new DispatchRequest("req-http-1", "u-http-1-1", "t1",
                "dcp.cdc.target_db.rt_sync_c", "rt_sync_c", List.of(1), starts, "task-t1",
                "es-sync", "1.0.0", Map.of("index", "crm_rt_sync_c"),
                new FailureSpec(3, 1000L, "dlq", "stop"), 30_000L);

        HttpUnitDispatcher dispatcher = new HttpUnitDispatcher("http://127.0.0.1:" + port);
        dispatcher.dispatch(req);                                    // 真发 HTTP
        check("HTTP 下发成功(2xx)", true, true);
        check("计算面收到 unitId", received.get("unitId"), "u-http-1-1");
        check("计算面收到 stream", received.get("stream"), "rt_sync_c");
        check("计算面收到起点", received.get("startOffset.1"), "31785");
        check("计算面收到业务参数", received.get("param.index"), "crm_rt_sync_c");

        CreateUnitCmd cmd = UnitCommandCodec.decode(received);        // 计算面侧还原
        check("还原:logic", cmd.logic().toString(), "es-sync@1.0.0");
        check("还原:分区组", cmd.partitions(), List.of(1));
        check("还原:失败策略重试次数", cmd.failure().maxRetries(), 3);

        dispatcher.stop("u-http-1-1");                               // StopUnit
        check("StopUnit 走到计算面", deleted.get(0), "/units/u-http-1-1");

        server.stop(0);
        System.out.println();
        System.out.println("==================================================");
        System.out.println("  通过 " + passed + " 项, 失败 " + failed + " 项");
        System.out.println("==================================================");
        if (failed > 0) System.exit(1);
    }

    private static void respond(com.sun.net.httpserver.HttpExchange ex, int code, String body) throws java.io.IOException {
        byte[] b = body.getBytes(StandardCharsets.UTF_8);
        ex.sendResponseHeaders(code, b.length);
        ex.getResponseBody().write(b);
        ex.close();
    }

    private static void check(String what, Object actual, Object expected) {
        boolean ok = String.valueOf(actual).equals(String.valueOf(expected));
        if (ok) passed++; else failed++;
        System.out.println((ok ? "  PASS  " : "  FAIL  ") + what + " = " + actual
                + (ok ? "" : "   (期望 " + expected + ")"));
    }
}
