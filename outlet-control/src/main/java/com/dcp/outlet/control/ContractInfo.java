package com.dcp.outlet.control;

import java.util.Set;

/** 契约摘要(控制面校验用);完整契约仍由出口模块的 Contract + 权限引擎负责。 */
public final class ContractInfo {

    private final String contractRef;
    private final boolean active;
    private final Set<String> streams;
    /** 契约所属消费方(platform consumer_id)——批阶段读快照的身份(与增量段同契约,保证批/增视图一致) */
    private final String consumerId;

    public ContractInfo(String contractRef, boolean active, Set<String> streams) {
        this(contractRef, active, streams, null);
    }

    public ContractInfo(String contractRef, boolean active, Set<String> streams, String consumerId) {
        this.contractRef = contractRef; this.active = active; this.streams = streams; this.consumerId = consumerId;
    }

    public String contractRef() { return contractRef; }
    public boolean active() { return active; }
    public Set<String> streams() { return streams; }
    public String consumerId() { return consumerId; }
}
