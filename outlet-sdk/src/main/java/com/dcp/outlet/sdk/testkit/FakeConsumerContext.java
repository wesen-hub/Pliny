package com.dcp.outlet.sdk.testkit;

import com.dcp.outlet.sdk.ConsumerContext;
import com.dcp.outlet.sdk.Mode;

import java.util.Map;

/** 测试替身:每批的 {@code ConsumerContext};记录 {@link #commit()} 调用次数便于断言。 */
public class FakeConsumerContext extends FakeInitContext implements ConsumerContext {

    private Mode mode = Mode.INCREMENTAL;
    private int partition;
    private long safeWatermark = Long.MAX_VALUE;
    private boolean caughtUp = true;
    private String logicId = "test-logic";
    private int commitCount;

    public static FakeConsumerContext of(Mode mode, int partition) {
        FakeConsumerContext c = new FakeConsumerContext();
        c.mode = mode;
        c.partition = partition;
        return c;
    }

    public FakeConsumerContext safeWatermark(long tso) { this.safeWatermark = tso; return this; }

    public FakeConsumerContext caughtUp(boolean v) { this.caughtUp = v; return this; }

    public FakeConsumerContext logicId(String v) { this.logicId = v; return this; }

    /** 断言用:逻辑显式调用了几次 {@code ctx.commit()}。 */
    public int commitCount() { return commitCount; }

    @Override public String consumerGroup() { return "task-" + taskId(); }
    @Override public Mode mode() { return mode; }
    @Override public int partition() { return partition; }
    @Override public long safeWatermark() { return safeWatermark; }
    @Override public boolean caughtUp() { return caughtUp; }
    @Override public Map<String, Object> params() { return super.params(); }
    @Override public String logicId() { return logicId; }
    @Override public String logicVersion() { return super.logicVersion(); }
    @Override public void commit() { commitCount++; }
}
