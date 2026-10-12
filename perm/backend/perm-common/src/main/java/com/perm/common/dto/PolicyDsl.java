package com.perm.common.dto;

import java.util.List;
import java.util.Map;

/**
 * 策略 DSL（管理面九要素结构 + 简化约束 DSL）
 * 通用策略载体：perm-admin 接收/回显、snapshot 接口输出、perm-pdp 热更新输入，三方共用。
 * rowRules 简化 DSL：{column, op, ref|value}——不直接暴露 AST，
 * op 支持 EQ/IN；值来源二选一：ref（主体属性引用）或 value（字面量，IN 时传列表）。
 */
public class PolicyDsl {

    private String policyId;
    private String policyName;
    private String effect;
    private String subjectType;
    private List<String> subjectIds;
    private String resourceId;
    private List<String> actions;
    private int priority = 100;
    private String effectiveFrom;
    private String effectiveTo;
    private ConstraintDsl constraints;
    private List<ObligationDsl> obligations;

    public record RowRuleDsl(String column, String op, String ref, Object value) {
    }

    public record ConstraintDsl(
            String visible,
            List<String> columns,
            Map<String, String> masks,
            List<RowRuleDsl> rowRules,
            TagScopeDsl tagScope) {
    }

    public record TagScopeDsl(String tagPath, String expandMode, String snapshotVersion,
                              String targetKey, Integer requiredLevel, List<String> entities) {
    }

    public record ObligationDsl(String type, String mode) {
    }

    // ---- 标准 JavaBean getter/setter（Jackson 序列化契约） ----

    public String getPolicyId() {
        return policyId;
    }

    public void setPolicyId(String policyId) {
        this.policyId = policyId;
    }

    public String getPolicyName() {
        return policyName;
    }

    public void setPolicyName(String policyName) {
        this.policyName = policyName;
    }

    public String getEffect() {
        return effect;
    }

    public void setEffect(String effect) {
        this.effect = effect;
    }

    public String getSubjectType() {
        return subjectType;
    }

    public void setSubjectType(String subjectType) {
        this.subjectType = subjectType;
    }

    public List<String> getSubjectIds() {
        return subjectIds;
    }

    public void setSubjectIds(List<String> subjectIds) {
        this.subjectIds = subjectIds;
    }

    public String getResourceId() {
        return resourceId;
    }

    public void setResourceId(String resourceId) {
        this.resourceId = resourceId;
    }

    public List<String> getActions() {
        return actions;
    }

    public void setActions(List<String> actions) {
        this.actions = actions;
    }

    public int getPriority() {
        return priority;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    public String getEffectiveFrom() {
        return effectiveFrom;
    }

    public void setEffectiveFrom(String effectiveFrom) {
        this.effectiveFrom = effectiveFrom;
    }

    public String getEffectiveTo() {
        return effectiveTo;
    }

    public void setEffectiveTo(String effectiveTo) {
        this.effectiveTo = effectiveTo;
    }

    public ConstraintDsl getConstraints() {
        return constraints;
    }

    public void setConstraints(ConstraintDsl constraints) {
        this.constraints = constraints;
    }

    public List<ObligationDsl> getObligations() {
        return obligations;
    }

    public void setObligations(List<ObligationDsl> obligations) {
        this.obligations = obligations;
    }
}
