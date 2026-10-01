package com.dcp.outlet.worker;

import com.dcp.outlet.sdk.InMemoryLogicRegistry;
import com.dcp.outlet.sdk.LogicRef;
import com.dcp.outlet.sdk.Op;
import com.dcp.outlet.sdk.SdkLogger;
import com.dcp.outlet.worker.support.*;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 计算面单元运行器自检(**内存桩,不需要 Kafka**):把"提交语义 / 失败语义 / 单线程 / 停机"验穿。
 */
public final class SelfCheck {

    private static final String TOPIC = "dcp.cdc.target_db.rt_sync_c";
    private static final String STREAM = "rt_sync_c";
    private static int passed, failed;
    private static final SdkLogger SILENT = new SdkLogger() {
        public boolean isDebugEnabled() { return false; }
        public void debug(String f, Object... a) { }
        public void info(String f, Object... a) { }
        public void warn(String f, Object... a) { }
        public void error(String m, Throwable t) { }
        public void error(String f, Object... a) { }
    };

    /** 测试脚手架:一次性组装一个单元运行环境。 */
    private static final class Harness {
        final InMemoryLogicRegistry registry = new InMemoryLogicRegistry();
        final StubDecoder decoder = new StubDecoder();
        final StubRecordSource source = new StubRecordSource();
        final InMemoryWatermarkProvider watermarks = new InMemoryWatermarkProvider();
        final InMemoryDeadLetterSink dlq = new InMemoryDeadLetterSink();
        final SimpleUnitMetrics metrics = new SimpleUnitMetrics();
        final UnitRunner runner;
        final ScriptedLogic logic;

        Harness(ScriptedLogic logic, ContractFilter filter, FailurePolicy failure, long startOffset) {
            this.logic = logic;
            registry.register("es-sync", "1.0.0", logic);
            runner = new UnitRunner(new ClasspathLogicLoader(registry), decoder, filter, cmd -> source,
                    watermarks, dlq, metrics, id -> SILENT);
            this.failure = failure;
            this.startOffset = startOffset;
        }

        final FailurePolicy failure;
        final long startOffset;

        void start() {
            Map<Integer, Long> starts = new LinkedHashMap<>();
            starts.put(0, startOffset);
            runner.start(new CreateUnitCmd()
                    .reqId("req-1").unitId("u-test-0-1").taskId("task-test")
                    .topic(TOPIC).stream(STREAM).partitions(List.of(0))
                    .startOffsets(starts).consumerGroup("task-task-test")
                    .logic(LogicRef.of("es-sync", "1.0.0"))
                    .params(Map.of("idField", "id"))
                    .failure(failure)
                    .pollMillis(1));
        }

        boolean awaitExit(long millis) throws InterruptedException {
            return runner.awaitTermination("u-test-0-1", millis);
        }
    }

    private static RawMessage raw(long offset) { return new RawMessage(0, offset, new byte[]{1}); }

    private static DecodedMessage event(long offset, long tso, Map<String, Object> after) {
        return DecodedMessage.event(0, offset, tso, STREAM, Op.INSERT,
                Map.of("id", String.valueOf(after.getOrDefault("id", "u-1"))), Map.of(), after);
    }

    public static void main(String[] args) throws Exception {
        testNormalBatchCommits();
        testEmptyBatchDoesNotReCommit();
        testWatermarkNotDeliveredButAdvances();
        testContractFiltering();
        testFailureDoesNotCommitThenDeadLetters();
        testSkipPolicyCommitsAndContinues();
        testSingleThreadSerial();
        testGracefulStop();
        testStartOffsetInjected();
        testDuplicateCreateUnitIsIdempotent();
        testRowConditionUsesRawRow();

        System.out.println();
        System.out.println("==================================================");
        System.out.println("  通过 " + passed + " 项, 失败 " + failed + " 项");
        System.out.println("==================================================");
        if (failed > 0) System.exit(1);
    }

    // ---------- 1. 正常批:process 返回即提交,提交"下一条要读的位置" ----------
    private static void testNormalBatchCommits() throws Exception {
        ScriptedLogic logic = new ScriptedLogic(0);
        Harness h = new Harness(logic, DefaultContractFilter.permissive(), FailurePolicy.defaults(), 100);
        h.decoder.put(100, event(100, 1000, Map.of("id", "u-1", "v", "a")));
        h.decoder.put(101, event(101, 1001, Map.of("id", "u-2", "v", "b")));
        h.decoder.put(102, event(102, 1002, Map.of("id", "u-3", "v", "c")));
        h.source.script(List.of(raw(100), raw(101), raw(102)));
        h.start();
        h.awaitExit(5000);

        check("T1 process 调用次数", logic.processCount(), 1);
        check("T1 逻辑收到记录数", logic.received().size(), 3);
        check("T1 提交位置 = 末条 offset+1", h.source.committed().get(0), 103L);
        check("T1 提交次数(批后一次)", h.source.commitCalls(), 1);
    }

    // ---------- 2. 空批:起点提交一次,之后不重复提交 ----------
    private static void testEmptyBatchDoesNotReCommit() throws Exception {
        ScriptedLogic logic = new ScriptedLogic(0);
        Harness h = new Harness(logic, DefaultContractFilter.permissive(), FailurePolicy.defaults(), 500);
        h.start();
        h.awaitExit(5000);
        check("T2 无数据时 process 未被调用", logic.processCount(), 0);
        check("T2 轮询次数 > 5", h.source.pollCount() > 5, true);
        check("T2 提交次数恒为 1(不重复提交)", h.source.commitCalls(), 1);
        check("T2 提交的是注入的起点", h.source.committed().get(0), 500L);
    }

    // ---------- 3. 水位:不进逻辑,但推进 safeWatermark ----------
    private static void testWatermarkNotDeliveredButAdvances() throws Exception {
        ScriptedLogic logic = new ScriptedLogic(0);
        Harness h = new Harness(logic, DefaultContractFilter.permissive(), FailurePolicy.defaults(), 50);
        h.decoder.put(50, DecodedMessage.watermark(0, 50, 7777));
        h.decoder.put(51, event(51, 8000, Map.of("id", "u-1")));
        h.decoder.put(52, event(52, 8001, Map.of("id", "u-2")));
        h.source.script(List.of(raw(50), raw(51), raw(52)));
        h.start();
        h.awaitExit(5000);

        check("T3 水位消息未进入逻辑(只收到 2 条)", logic.received().size(), 2);
        check("T3 水位已写入", h.watermarks.safeWatermark(0), 7777L);
        check("T3 逻辑看到的 safeWatermark", logic.safeWatermarks().get(0), 7777L);
    }

    // ---------- 4. 契约裁剪:越权列在进逻辑前被丢弃 ----------
    private static void testContractFiltering() throws Exception {
        ScriptedLogic logic = new ScriptedLogic(0);
        Harness h = new Harness(logic, DefaultContractFilter.ofColumns("u_id", "v"),
                FailurePolicy.defaults(), 10);
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("u_id", "u-1"); row.put("v", "a"); row.put("secret", "S3CR3T"); row.put("phone", "138xxxx");
        h.decoder.put(10, event(10, 1000, row));
        h.source.script(List.of(raw(10)));
        h.start();
        h.awaitExit(5000);

        check("T4 逻辑收到的列(仅白名单)", logic.columnsOfFirstRecord(), "u_id,v");
        check("T4 敏感列未泄漏", logic.columnsOfFirstRecord().contains("secret"), false);
    }

    // ---------- 5. 失败:不提交 → 重试 → 死信 → STOP(数据不丢,需人工介入) ----------
    private static void testFailureDoesNotCommitThenDeadLetters() throws Exception {
        ScriptedLogic logic = new ScriptedLogic(99);          // 永远失败
        Harness h = new Harness(logic, DefaultContractFilter.permissive(),
                new FailurePolicy(1, 1L, FailurePolicy.OnFailure.STOP), 200);
        h.decoder.put(200, event(200, 1000, Map.of("id", "u-1")));
        h.source.script(List.of(raw(200)));
        h.start();
        h.awaitExit(5000);

        check("T5 单批被处理次数(1 + 1 次重试)", logic.processCount(), 2);
        check("T5 位点未推进(不提交)", h.runner.lastCommittedOffset("u-test-0-1"), -1L);
        check("T5 死信记录数", h.dlq.size(), 1);
        check("T5 单元状态", h.runner.state("u-test-0-1"), UnitRunner.State.FAILED);
        check("T5 失败计数", h.metrics.count("failure"), 2L);
        check("T5 重试计数", h.metrics.count("retry"), 1L);
    }

    // ---------- 6. SKIP 策略:跳过并提交,继续消费后续 ----------
    private static void testSkipPolicyCommitsAndContinues() throws Exception {
        ScriptedLogic logic = new ScriptedLogic(1);           // 只失败第一次
        Harness h = new Harness(logic, DefaultContractFilter.permissive(),
                new FailurePolicy(0, 1L, FailurePolicy.OnFailure.SKIP), 300);
        h.decoder.put(300, event(300, 1000, Map.of("id", "u-bad")));
        h.decoder.put(301, event(301, 1001, Map.of("id", "u-1")));
        h.decoder.put(302, event(302, 1002, Map.of("id", "u-2")));
        h.source.script(List.of(raw(300)));
        h.source.script(List.of(raw(301), raw(302)));
        h.start();
        h.awaitExit(5000);

        check("T6 处理批数(失败批 + 成功批)", logic.processCount(), 2);
        check("T6 死信记录数", h.dlq.size(), 1);
        check("T6 后续批已消费", logic.received().size(), 2);
        check("T6 位点推进到 303", h.source.committed().get(0), 303L);
    }

    // ---------- 7. 单线程串行:同一实例、同一线程、无并发 ----------
    private static void testSingleThreadSerial() throws Exception {
        ScriptedLogic logic = new ScriptedLogic(0);
        Harness h = new Harness(logic, DefaultContractFilter.permissive(), FailurePolicy.defaults(), 0);
        for (int i = 0; i < 5; i++) {
            h.decoder.put(i, event(i, 1000 + i, Map.of("id", "u-" + i)));
            h.source.script(List.of(raw(i)));
        }
        h.start();
        h.awaitExit(5000);

        check("T7 处理批数", logic.processCount(), 5);
        check("T7 只有 1 个线程执行 process", logic.threadNames().size(), 1);
        check("T7 最大并发 = 1(串行)", logic.maxConcurrent(), 1);
        check("T7 逻辑 init 只调用一次", logic.initCount(), 1);
    }

    // ---------- 8. 优雅停机:停止消费 → 收尾 → close ----------
    private static void testGracefulStop() throws Exception {
        ScriptedLogic logic = new ScriptedLogic(0);
        Harness h = new Harness(logic, DefaultContractFilter.permissive(),
                FailurePolicy.defaults(), 0);
        h.decoder.put(0, event(0, 1000, Map.of("id", "u-1")));
        h.source.script(List.of(raw(0)));
        h.start();
        Thread.sleep(80);
        check("T8 stop 前状态为 RUNNING", h.runner.state("u-test-0-1"), UnitRunner.State.RUNNING);
        h.runner.stop("u-test-0-1");
        h.runner.stop("u-test-0-1");                    // 停机幂等
        boolean exited = h.awaitExit(5000);

        check("T8 单元已退出", exited, true);
        check("T8 状态为 STOPPED", h.runner.state("u-test-0-1"), UnitRunner.State.STOPPED);
        check("T8 逻辑 close 被调用", logic.closeCount(), 1);
        check("T8 消费源被唤醒/关闭", h.source.woken(), true);
    }

    // ---------- 9. 起点注入:单元从控制面给的 offset 开始,而不是 0 ----------
    private static void testStartOffsetInjected() throws Exception {
        ScriptedLogic logic = new ScriptedLogic(0);
        Harness h = new Harness(logic, DefaultContractFilter.permissive(), FailurePolicy.defaults(), 31785);
        h.start();
        h.awaitExit(5000);

        check("T10 消费源被 seek 到起点", h.source.seeked().get(0), 31785L);
    }

    // ---------- 11. 幂等:同 unitId 重复下发只跑一个单元 ----------
    private static void testDuplicateCreateUnitIsIdempotent() throws Exception {
        ScriptedLogic logic = new ScriptedLogic(0);
        Harness h = new Harness(logic, DefaultContractFilter.permissive(), FailurePolicy.defaults(), 0);
        h.decoder.put(0, event(0, 1000, Map.of("id", "u-1")));
        h.source.script(List.of(raw(0)));
        h.start();
        h.start();                                   // 重复下发
        h.awaitExit(5000);

        check("T11 只处理 1 批(未重复启动)", logic.processCount(), 1);
        check("T11 init 只调用一次", logic.initCount(), 1);
    }

    // ---------- 12. 行条件用"原始行"判定(条件列不在列白名单内也不能被误丢) ----------
    private static void testRowConditionUsesRawRow() throws Exception {
        // 契约:白名单只有 u_id;行条件引用 status(不在白名单里)
        ContractView view = new ContractView() {
            @Override public java.util.Set<String> columnWhitelist(String stream) {
                return java.util.Set.of("u_id");
            }
            @Override public int maxLevel() { return -1; }
            @Override public Map<String, String> maskRules() { return Map.of(); }
            @Override public String rowCondition() { return "status = OK"; }
        };
        ContractFilter filter = new DefaultContractFilter(
                (cond, row) -> "OK".equals(String.valueOf(row.get("status"))),   // 极简行条件求值
                (col, v, rule, lvl) -> v, view);

        ScriptedLogic logic = new ScriptedLogic(0);
        Harness h = new Harness(logic, filter, FailurePolicy.defaults(), 10);
        h.decoder.put(10, event(10, 1000, Map.of("u_id", "u-1", "status", "OK")));
        h.decoder.put(11, event(11, 1001, Map.of("u_id", "u-2", "status", "BLOCKED")));
        h.source.script(List.of(raw(10), raw(11)));
        h.start();
        h.awaitExit(5000);

        check("T12 行内事件保留(条件列不在白名单)", logic.received().size(), 1);
        check("T12 保留事件已按白名单裁列", logic.columnsOfFirstRecord(), "u_id");
    }

    private static void check(String what, Object actual, Object expected) {
        boolean ok = String.valueOf(actual).equals(String.valueOf(expected));
        if (ok) passed++; else failed++;
        System.out.println((ok ? "  PASS  " : "  FAIL  ") + what + " = " + actual
                + (ok ? "" : "   (期望 " + expected + ")"));
    }
}
