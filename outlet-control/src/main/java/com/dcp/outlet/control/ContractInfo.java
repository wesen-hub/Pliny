package com.dcp.outlet.control;

import java.util.Set;

/** 契约摘要(控制面校验用);完整契约仍由出口模块的 Contract + 权限引擎负责。 */
public final class ContractInfo {

    private final String contractRef;
    private final boolean active;
    private final Set<String> streams;

    public ContractInfo(String contractRef, boolean active, Set<String> streams) {
        this.contractRef = contractRef; this.active = active; this.streams = streams;
    }

    public String contractRef() { return contractRef; }
    public boolean active() { return active; }
    public Set<String> streams() { return streams; }
}
