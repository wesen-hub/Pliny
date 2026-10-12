package com.perm.common.model;

import java.util.ArrayList;
import java.util.List;

/**
 * 策略（管理面 PolicyVersion 的编译前语义模型）
 * subject.resourceId 三段式；resourceId v1 精确匹配。
 */
public class Policy {

    private final String policyId;
    private final String policyName;
    private final Effect effect;
    /** { type: role/user/group, ids: [...] } */
    private final String subjectType;
    private final List<String> subjectIds;
    private final String resourceId;
    private final List<String> actions;
    private final PolicyConstraints constraints;
    /** 义务列表：{ type: AUDIT, mode: sync/async } */
    private final List<Obligation> obligations;
    /** 数值越大优先级越高 */
    private final int priority;
    /** 可选有效期 */
    private final EffectivePeriod effectivePeriod;

    private Policy(Builder b) {
        this.policyId = b.policyId;
        this.policyName = b.policyName;
        this.effect = b.effect;
        this.subjectType = b.subjectType;
        this.subjectIds = List.copyOf(b.subjectIds);
        this.resourceId = b.resourceId;
        this.actions = List.copyOf(b.actions);
        this.constraints = b.constraints == null ? PolicyConstraints.builder().build() : b.constraints;
        this.obligations = List.copyOf(b.obligations);
        this.priority = b.priority;
        this.effectivePeriod = b.effectivePeriod;
    }

    public String policyId() {
        return policyId;
    }

    public String policyName() {
        return policyName;
    }

    public Effect effect() {
        return effect;
    }

    public String subjectType() {
        return subjectType;
    }

    public List<String> subjectIds() {
        return subjectIds;
    }

    public String resourceId() {
        return resourceId;
    }

    public List<String> actions() {
        return actions;
    }

    public PolicyConstraints constraints() {
        return constraints;
    }

    public List<Obligation> obligations() {
        return obligations;
    }

    public int priority() {
        return priority;
    }

    public EffectivePeriod effectivePeriod() {
        return effectivePeriod;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String policyId;
        private String policyName;
        private Effect effect;
        private String subjectType;
        private final List<String> subjectIds = new ArrayList<>();
        private String resourceId;
        private final List<String> actions = new ArrayList<>();
        private PolicyConstraints constraints;
        private final List<Obligation> obligations = new ArrayList<>();
        private int priority = 100;
        private EffectivePeriod effectivePeriod;

        public Builder policyId(String v) {
            this.policyId = v;
            return this;
        }

        public Builder policyName(String v) {
            this.policyName = v;
            return this;
        }

        public Builder effect(Effect v) {
            this.effect = v;
            return this;
        }

        public Builder subjectType(String v) {
            this.subjectType = v;
            return this;
        }

        public Builder subjectId(String v) {
            this.subjectIds.add(v);
            return this;
        }

        public Builder resourceId(String v) {
            this.resourceId = v;
            return this;
        }

        public Builder action(String v) {
            this.actions.add(v);
            return this;
        }

        public Builder constraints(PolicyConstraints v) {
            this.constraints = v;
            return this;
        }

        public Builder obligation(String type, String mode) {
            this.obligations.add(new Obligation(type, mode));
            return this;
        }

        public Builder priority(int v) {
            this.priority = v;
            return this;
        }

        public Builder effectivePeriod(String from, String to) {
            this.effectivePeriod = new EffectivePeriod(from, to);
            return this;
        }

        public Policy build() {
            return new Policy(this);
        }
    }

    /** 义务 */
    public record Obligation(String type, String mode) {
    }

    /** 有效期 */
    public record EffectivePeriod(String from, String to) {
    }
}
