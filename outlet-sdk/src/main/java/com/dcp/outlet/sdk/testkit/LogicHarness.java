package com.dcp.outlet.sdk.testkit;

import com.dcp.outlet.sdk.Batch;
import com.dcp.outlet.sdk.ConsumerContext;
import com.dcp.outlet.sdk.ConsumerLogic;
import com.dcp.outlet.sdk.LogicRef;
import com.dcp.outlet.sdk.LogicRegistry;
import com.dcp.outlet.sdk.Mode;
import com.dcp.outlet.sdk.SdkLogger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 逻辑测试台:模拟平台侧的调用序列(init → process×N → close),
 * 便于在**没有 Kafka、没有平台**的情况下验证逻辑语义(尤其幂等)。
 *
 * <pre>{@code
 * LogicHarness.run(new EsSyncLogic(), Map.of("idField","id"),
 *     FakeBatch.snapshot(FakeRecord.of(Op.SNAPSHOT, 100L).key("id","u-1").after("id","u-1")),
 *     FakeBatch.incremental(FakeRecord.of(Op.DELETE, 101L).key("id","u-1")));
 * }</pre>
 */
public final class LogicHarness {

    /** 一次运行的记录。 */
    public static final class Run {
        public final int batches;
        public final int records;
        public final int commits;              // 逻辑显式 ctx.commit() 次数
        public final int processedBatches;     // 正常返回的批数
        public final List<Throwable> errors = new ArrayList<>();

        Run(int batches, int records, int commits, int processedBatches) {
            this.batches = batches;
            this.records = records;
            this.commits = commits;
            this.processedBatches = processedBatches;
        }

        public boolean allOk() { return errors.isEmpty(); }

        @Override public String toString() {
            return "batches=" + batches + " records=" + records
                    + " processed=" + processedBatches + " commits=" + commits
                    + " errors=" + errors.size();
        }
    }

    private LogicHarness() { }

    /**
     * 跑 init → process×N → <b>close</b>;异常被收集(不中断后续批,便于观察失败语义)。
     *
     * <p><b>注意</b>:末尾会调用 {@code close()}。若要断言逻辑的内部状态,请改用
     * {@link #runKeepOpen} 并自行调用 {@code close()}。
     */
    public static Run run(ConsumerLogic logic, Map<String, Object> params, Batch... batches) {
        return run(logic, params, new ConsoleSdkLogger("[harness] ", false), 0, batches);
    }

    public static Run run(ConsumerLogic logic, Map<String, Object> params, SdkLogger logger,
                          int partition, Batch... batches) {
        return doRun(logic, params, logger, partition, true, batches);
    }

    /** 同上,但**不**调用 {@code close()} —— 便于在断言内部状态后再手动 close。 */
    public static Run runKeepOpen(ConsumerLogic logic, Map<String, Object> params, Batch... batches) {
        return doRun(logic, params, new ConsoleSdkLogger("[harness] ", false), 0, false, batches);
    }

    private static Run doRun(ConsumerLogic logic, Map<String, Object> params, SdkLogger logger,
                             int partition, boolean closeAtEnd, Batch... batches) {
        FakeInitContext init = FakeInitContext.of(params);
        init.partitions(List.of(partition));
        init.logger(logger);

        int records = 0, commits = 0, processed = 0;
        List<Throwable> errors = new ArrayList<>();
        Run result;

        logic.init(init);
        for (Batch b : batches) {
            records += b.size();
            FakeConsumerContext ctx = FakeConsumerContext.of(b.mode(), partition);
            ctx.logger(logger).params(params);
            try {
                logic.process(b, ctx);
                processed++;
            } catch (Throwable t) {                  // 模拟平台"失败不提交 → 重试/死信"
                errors.add(t);
                logger.error("process failed: {}", t.toString());
            }
            commits += ctx.commitCount();
        }
        if (closeAtEnd) logic.close();

        result = new Run(batches.length, records, commits, processed);
        result.errors.addAll(errors);
        return result;
    }

    /** 注册到注册表(模拟 worker 侧注册)。 */
    public static void register(LogicRegistry registry, LogicRef ref, ConsumerLogic logic) {
        registry.register(ref, logic);
    }

}
