package com.dcp.outlet.sdk.testkit;

import com.dcp.outlet.sdk.LogicInitContext;
import com.dcp.outlet.sdk.SdkLogger;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** 测试替身:{@code init()} 的入参。 */
public class FakeInitContext implements LogicInitContext {

    private final Map<String, Object> params = new LinkedHashMap<>();
    private String taskId = "test-task";
    private String logicVersion = "1.0.0";
    private String topic = "dcp.cdc.target_db.rt_sync_c";
    private List<Integer> partitions = List.of(0);
    private SdkLogger logger = new ConsoleSdkLogger("[test] ", false);

    public static FakeInitContext of(Map<String, Object> params) {
        FakeInitContext c = new FakeInitContext();
        if (params != null) c.params.putAll(params);
        return c;
    }

    public FakeInitContext param(String k, Object v) { params.put(k, v); return this; }

    /** 批量设值(与 getter {@link #params()} 重载)。 */
    public FakeInitContext params(Map<String, Object> values) {
        if (values != null) params.putAll(values);
        return this;
    }

    public FakeInitContext taskId(String v) { this.taskId = v; return this; }

    public FakeInitContext partitions(List<Integer> p) { this.partitions = p; return this; }

    public FakeInitContext logger(SdkLogger l) { this.logger = l; return this; }

    @Override public Map<String, Object> params() { return params; }
    @Override public String taskId() { return taskId; }
    @Override public String logicVersion() { return logicVersion; }
    @Override public String topic() { return topic; }
    @Override public List<Integer> partitions() { return partitions; }
    @Override public SdkLogger log() { return logger; }
}
