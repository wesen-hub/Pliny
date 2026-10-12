package com.perm.pep;

import java.util.List;

/**
 * 验签通过后的判定产物（SDK 消费视图，不可变）
 * 模板无值（铁律 C1）：行谓词只含占位符，值由绑定阶段注入。
 * 与 PDP 响应契约一致，验签失败时不会产生本对象（fail-closed）。
 */
public final class VerifiedArtifact {

    /** PERMIT / DENY */
    private final String decision;
    /** DENY 时的人读原因 */
    private final String denyReason;
    /** 策略版本（缓存键空间版本化） */
    private final int planVersion;

    /** 行谓词方言模板（含 :pN / :listN 占位）；null = 不注入 */
    private final String rowPredicateTemplate;
    /** 占位符列表（ref 供绑定） */
    private final List<Placeholder> placeholders;

    /** 可见列（null = 不裁剪） */
    private final List<String> visibleColumns;
    /** 掩码列表（最严者胜，R-D9） */
    private final List<Mask> masks;
    /** 标签范围 */
    private final TagScope tagScope;
    /** 合并后义务 */
    private final List<Obligation> obligations;
    /** 命中策略 id 列表 */
    private final List<String> matchedPolicies;

    public record Placeholder(String name, String ref, String kind, Object value) {
    }

    public record Mask(String column, String type) {
    }

    public record TagScope(String tagPath, String expandMode, String snapshotVersion,
                           String targetKey, int requiredLevel, List<String> entities) {
    }

    public record Obligation(String type, String mode) {
    }

    public VerifiedArtifact(String decision, String denyReason, int planVersion,
                            String rowPredicateTemplate, List<Placeholder> placeholders,
                            List<String> visibleColumns, List<Mask> masks, TagScope tagScope,
                            List<Obligation> obligations, List<String> matchedPolicies) {
        this.decision = decision;
        this.denyReason = denyReason;
        this.planVersion = planVersion;
        this.rowPredicateTemplate = rowPredicateTemplate;
        this.placeholders = placeholders == null ? List.of() : List.copyOf(placeholders);
        this.visibleColumns = visibleColumns;
        this.masks = masks == null ? List.of() : List.copyOf(masks);
        this.tagScope = tagScope;
        this.obligations = obligations == null ? List.of() : List.copyOf(obligations);
        this.matchedPolicies = matchedPolicies == null ? List.of() : List.copyOf(matchedPolicies);
    }

    public boolean permitted() {
        return "PERMIT".equals(decision);
    }

    public String decision() {
        return decision;
    }

    public String denyReason() {
        return denyReason;
    }

    public int planVersion() {
        return planVersion;
    }

    public String rowPredicateTemplate() {
        return rowPredicateTemplate;
    }

    public List<Placeholder> placeholders() {
        return placeholders;
    }

    public List<String> visibleColumns() {
        return visibleColumns;
    }

    public List<Mask> masks() {
        return masks;
    }

    public TagScope tagScope() {
        return tagScope;
    }

    public List<Obligation> obligations() {
        return obligations;
    }

    public List<String> matchedPolicies() {
        return matchedPolicies;
    }
}
