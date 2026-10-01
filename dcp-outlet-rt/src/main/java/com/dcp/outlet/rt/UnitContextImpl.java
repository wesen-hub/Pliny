package com.dcp.outlet.rt;

import com.dcp.outlet.sdk.ConsumerContext;
import com.dcp.outlet.sdk.Mode;
import com.dcp.outlet.sdk.SdkLogger;

import java.util.Map;

/** SDK {@link ConsumerContext} 的运行时实现(每批一个,只读视图)。 */
public class UnitContextImpl implements ConsumerContext {

    private final UnitRuntime rt;
    private final Mode mode;
    private final int partition;
    private final Runnable explicitCommit;

    public UnitContextImpl(UnitRuntime rt, Mode mode, int partition, Runnable explicitCommit) {
        this.rt = rt; this.mode = mode; this.partition = partition; this.explicitCommit = explicitCommit;
    }

    @Override public String taskId() { return rt.taskId(); }
    @Override public String consumerGroup() { return rt.consumerGroup(); }
    @Override public Mode mode() { return mode; }
    @Override public int partition() { return partition; }
    @Override public long safeWatermark() { return rt.watermarks().safeWatermark(partition); }
    @Override public boolean caughtUp() { return rt.watermarks().safeWatermark(partition) <= 0; }
    @Override public Map<String, Object> params() { return rt.params(); }
    @Override public String logicId() { return rt.logicId(); }
    @Override public String logicVersion() { return rt.logicVersion(); }
    @Override public SdkLogger log() { return rt.log(); }
    @Override public void commit() { if (explicitCommit != null) explicitCommit.run(); }
}
