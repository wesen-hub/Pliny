package com.perm.pdp.sync;

import com.perm.common.dto.PolicyDsl;

import java.util.List;

/**
 * 管理面快照（PDP 版本比对兜底通道的数据模型）
 */
public class AdminSnapshotDto {

    private int version;
    private int policyCount;
    private List<PolicyDsl> policies;

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public int getPolicyCount() {
        return policyCount;
    }

    public void setPolicyCount(int policyCount) {
        this.policyCount = policyCount;
    }

    public List<PolicyDsl> getPolicies() {
        return policies;
    }

    public void setPolicies(List<PolicyDsl> policies) {
        this.policies = policies;
    }
}
