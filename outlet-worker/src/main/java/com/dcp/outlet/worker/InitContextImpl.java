package com.dcp.outlet.worker;

import com.dcp.outlet.sdk.LogicInitContext;
import com.dcp.outlet.sdk.SdkLogger;

import java.util.List;
import java.util.Map;

/** SDK {@link LogicInitContext} 的运行时实现。 */
public class InitContextImpl implements LogicInitContext {

    private final UnitRuntime rt;

    public InitContextImpl(UnitRuntime rt) { this.rt = rt; }

    @Override public Map<String, Object> params() { return rt.params(); }
    @Override public String taskId() { return rt.taskId(); }
    @Override public String logicVersion() { return rt.logicVersion(); }
    @Override public String topic() { return rt.topic(); }
    @Override public List<Integer> partitions() { return rt.partitions(); }
    @Override public SdkLogger log() { return rt.log(); }
}
