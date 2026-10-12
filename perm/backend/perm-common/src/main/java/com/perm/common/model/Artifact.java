package com.perm.common.model;

import java.util.ArrayList;
import java.util.List;

/**
 * 判定产物（PDP 输出）
 * v1 实现核心字段：判定结论 + 合并约束 + 标签范围 + 义务 + 元信息。
 * 签名/键空间版本化等安全字段在 PDP 服务层叠加，核心引擎只保证语义。
 */
public class Artifact {

    private final Decision decision;
    /** 合并后行谓词 AST（AND 连接，R-D7）；null = 不注入 */
    private final PredicateNode rowPredicate;
    /** 可见列集合（交集 − exclude，R-D8）；null = 不裁剪 */
    private final List<String> visibleColumns;
    /** 合并后掩码列表（最严者胜，R-D9） */
    private final List<PolicyConstraints.MaskSpec> masks;
    /** 标签范围（R-T7） */
    private final PolicyConstraints.TagScope tagScope;
    /** 合并后义务（并集取最强，R-D16） */
    private final List<Policy.Obligation> obligations;
    /** DENY 时的人读原因（审计追溯） */
    private final String denyReason;
    /** 策略版本（缓存键空间版本化） */
    private final int planVersion;
    /** 判定元信息（命中策略 id 列表等） */
    private final List<String> matchedPolicies;

    private Artifact(Builder b) {
        this.decision = b.decision;
        this.rowPredicate = b.rowPredicate;
        this.visibleColumns = b.visibleColumns == null ? null : List.copyOf(b.visibleColumns);
        this.masks = List.copyOf(b.masks);
        this.tagScope = b.tagScope;
        this.obligations = List.copyOf(b.obligations);
        this.denyReason = b.denyReason;
        this.planVersion = b.planVersion;
        this.matchedPolicies = List.copyOf(b.matchedPolicies);
    }

    public Decision decision() {
        return decision;
    }

    public PredicateNode rowPredicate() {
        return rowPredicate;
    }

    public List<String> visibleColumns() {
        return visibleColumns;
    }

    public List<PolicyConstraints.MaskSpec> masks() {
        return masks;
    }

    public PolicyConstraints.TagScope tagScope() {
        return tagScope;
    }

    public List<Policy.Obligation> obligations() {
        return obligations;
    }

    public String denyReason() {
        return denyReason;
    }

    public int planVersion() {
        return planVersion;
    }

    public List<String> matchedPolicies() {
        return matchedPolicies;
    }

    public boolean permitted() {
        return decision == Decision.PERMIT;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private Decision decision;
        private PredicateNode rowPredicate;
        private List<String> visibleColumns;
        private final List<PolicyConstraints.MaskSpec> masks = new ArrayList<>();
        private PolicyConstraints.TagScope tagScope;
        private final List<Policy.Obligation> obligations = new ArrayList<>();
        private String denyReason;
        private int planVersion = 1;
        private final List<String> matchedPolicies = new ArrayList<>();

        public Builder decision(Decision v) {
            this.decision = v;
            return this;
        }

        public Builder rowPredicate(PredicateNode v) {
            this.rowPredicate = v;
            return this;
        }

        public Builder visibleColumns(List<String> v) {
            this.visibleColumns = v;
            return this;
        }

        public Builder masks(List<PolicyConstraints.MaskSpec> v) {
            this.masks.addAll(v);
            return this;
        }

        public Builder tagScope(PolicyConstraints.TagScope v) {
            this.tagScope = v;
            return this;
        }

        public Builder obligations(List<Policy.Obligation> v) {
            this.obligations.addAll(v);
            return this;
        }

        public Builder denyReason(String v) {
            this.denyReason = v;
            return this;
        }

        public Builder planVersion(int v) {
            this.planVersion = v;
            return this;
        }

        public Builder matchedPolicy(String v) {
            this.matchedPolicies.add(v);
            return this;
        }

        public Builder matchedPolicies(List<String> v) {
            this.matchedPolicies.addAll(v);
            return this;
        }

        public Artifact build() {
            return new Artifact(this);
        }
    }
}
