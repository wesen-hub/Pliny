package com.dcp.outlet.adapters;

import com.dcp.outlet.control.*;
import com.dcp.outlet.sdk.ChangeRecord;
import com.dcp.outlet.sdk.ConsumerLogic;
import com.dcp.outlet.sdk.Mode;
import com.dcp.outlet.sdk.SdkLogger;
import com.sun.net.httpserver.HttpServer;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/** 真实快照源(走平台快照 API)+ 批阶段运行器:分页、切片、两 mode 同一逻辑。 */
public final class SnapshotSourceCheck {

    private static int passed, failed;
    private static final String STREAM = "rt_sync_c";

    public static void main(String[] args) throws Exception {
        AtomicInteger requests = new AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/api/outlet/" + STREAM + "/snapshot", ex -> {
            requests.incrementAndGet();
            String q = ex.getRequestURI().getQuery() == null ? "" : ex.getRequestURI().getQuery();
            String body = q.contains("pageToken=p2")
                    ? "{\"rows\":[{\"id\":\"11\",\"v\":\"c\"},{\"id\":\"12\",\"v\":\"d\"}]}"
                    : "{\"rows\":[{\"id\":\"1\",\"v\":\"a\"},{\"id\":\"2\",\"v\":\"b\"}],\"nextPageToken\":\"p2\"}";
            byte[] b = body.getBytes(StandardCharsets.UTF_8);
            ex.sendResponseHeaders(200, b.length);
            ex.getResponseBody().write(b);
            ex.close();
        });
        server.start();
        String base = "http://127.0.0.1:" + server.getAddress().getPort();

        // ① 分页读取
        HttpSnapshotSource src = new HttpSnapshotSource(base, "c-es-001", 2, java.time.Duration.ofSeconds(3));
        List<ChangeRecord> rows = src.readSlice(STREAM, 5000L, new Slice(STREAM, Slice.Kind.WHOLE, 0, 0, 0, 0, 1));
        check("分页:累计行数", rows.size(), 4);
        check("分页:请求次数(2 页)", requests.get(), 2);
        check("行 op=SNAPSHOT", rows.get(0).op().name(), "SNAPSHOT");
        check("行 tso=T0", rows.get(0).tso(), 5000L);
        check("行内容解析", rows.get(3).after().get("v"), "d");
        check("行数估算未知 → -1(退化为按分区)", src.estimateRows(STREAM, 5000L), -1L);

        // ② 批阶段走"平台快照 API":同一 logic, mode=SNAPSHOT, 按分区切片
        SpyLogic spy = new SpyLogic();
        SlotPlanner planner = SlotPlanner.defaults();
        BatchPhaseRunner runner = new BatchPhaseRunner(src, planner, 1, 1L);
        BatchPhaseRunner.Outcome o = runner.run("t1", "task-t1", STREAM,
                "dcp.cdc.target_db.rt_sync_c", 5000L, 3, "es-sync", "1.0.0",
                spy, Map.of("idField", "id"), Silent.LOG);
        check("批阶段:成功", o.ok, true);
        check("批阶段:按分区 3 片", o.slices, 3);
        check("批阶段:三片共 12 行(每片 4)", o.rows, 12);
        check("批阶段:mode=SNAPSHOT", spy.modes.stream().distinct().toList(), List.of(Mode.SNAPSHOT));
        check("批阶段:init 一次 close 一次", spy.init + "/" + spy.close, "1/1");

        server.stop(0);
        System.out.println();
        System.out.println("==================================================");
        System.out.println("  通过 " + passed + " 项, 失败 " + failed + " 项");
        System.out.println("==================================================");
        if (failed > 0) System.exit(1);
    }

    private static final class SpyLogic implements ConsumerLogic {
        final List<Mode> modes = new ArrayList<>();
        int init, close, rows;
        @Override public void init(com.dcp.outlet.sdk.LogicInitContext ctx) { init++; }
        @Override public void process(com.dcp.outlet.sdk.Batch b, com.dcp.outlet.sdk.ConsumerContext ctx) {
            modes.add(b.mode()); rows += b.size();
        }
        @Override public void close() { close++; }
    }

    private static final class Silent {
        static final SdkLogger LOG = new SdkLogger() {
            public boolean isDebugEnabled() { return false; }
            public void debug(String f, Object... a) { }
            public void info(String f, Object... a) { }
            public void warn(String f, Object... a) { }
            public void error(String m, Throwable t) { }
            public void error(String f, Object... a) { }
        };
    }

    private static void check(String what, Object actual, Object expected) {
        boolean ok = String.valueOf(actual).equals(String.valueOf(expected));
        if (ok) passed++; else failed++;
        System.out.println((ok ? "  PASS  " : "  FAIL  ") + what + " = " + actual
                + (ok ? "" : "   (期望 " + expected + ")"));
    }
}
