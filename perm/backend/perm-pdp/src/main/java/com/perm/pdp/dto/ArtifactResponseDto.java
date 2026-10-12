package com.perm.pdp.dto;

import java.util.List;

/**
 * 判定响应 DTO（PDP → PEP）
 * 渲染后的模板无值（铁律 C1）：行谓词只含占位符，值由 PEP 绑定阶段注入。
 * signature 覆盖除 signature/ts 外全部字段（HMAC，R-Sec1）。
 */
public class ArtifactResponseDto {

    /** PERMIT / DENY */
    private String decision;
    /** DENY 时的人读原因（审计追溯） */
    private String denyReason;
    /** 策略版本（缓存键空间版本化） */
    private int planVersion;

    /** 行谓词方言模板（含 :pN / :listN 占位）；null = 不注入 */
    private String rowPredicateTemplate;
    /** 占位符列表（ref 供 PEP 绑定） */
    private List<PlaceholderDto> placeholders;

    /** 可见列（交集 − exclude，R-D8）；null = 不裁剪 */
    private List<String> visibleColumns;
    /** 掩码列表（最严者胜，R-D9） */
    private List<MaskDto> masks;
    /** 标签范围（R-T7） */
    private TagScopeDto tagScope;
    /** 合并后义务（R-D16） */
    private List<ObligationDto> obligations;

    /** 命中策略 id 列表（元信息） */
    private List<String> matchedPolicies;

    /** 签名（HmacSHA256，hex） */
    private String signature;
    /** 签名时间戳（秒，防重放窗口校验 R-Sec7） */
    private long ts;

    public record PlaceholderDto(String name, String ref, String kind, Object value) {
        public PlaceholderDto(String name, String ref, String kind) {
            this(name, ref, kind, null);
        }
    }

    public record MaskDto(String column, String type) {
    }

    public record TagScopeDto(String tagPath, String expandMode, String snapshotVersion,
                              String targetKey, int requiredLevel, List<String> entities) {
    }

    public record ObligationDto(String type, String mode) {
    }

    public String getDecision() {
        return decision;
    }

    public void setDecision(String decision) {
        this.decision = decision;
    }

    public String getDenyReason() {
        return denyReason;
    }

    public void setDenyReason(String denyReason) {
        this.denyReason = denyReason;
    }

    public int getPlanVersion() {
        return planVersion;
    }

    public void setPlanVersion(int planVersion) {
        this.planVersion = planVersion;
    }

    public String getRowPredicateTemplate() {
        return rowPredicateTemplate;
    }

    public void setRowPredicateTemplate(String rowPredicateTemplate) {
        this.rowPredicateTemplate = rowPredicateTemplate;
    }

    public List<PlaceholderDto> getPlaceholders() {
        return placeholders;
    }

    public void setPlaceholders(List<PlaceholderDto> placeholders) {
        this.placeholders = placeholders;
    }

    public List<String> getVisibleColumns() {
        return visibleColumns;
    }

    public void setVisibleColumns(List<String> visibleColumns) {
        this.visibleColumns = visibleColumns;
    }

    public List<MaskDto> getMasks() {
        return masks;
    }

    public void setMasks(List<MaskDto> masks) {
        this.masks = masks;
    }

    public TagScopeDto getTagScope() {
        return tagScope;
    }

    public void setTagScope(TagScopeDto tagScope) {
        this.tagScope = tagScope;
    }

    public List<ObligationDto> getObligations() {
        return obligations;
    }

    public void setObligations(List<ObligationDto> obligations) {
        this.obligations = obligations;
    }

    public List<String> getMatchedPolicies() {
        return matchedPolicies;
    }

    public void setMatchedPolicies(List<String> matchedPolicies) {
        this.matchedPolicies = matchedPolicies;
    }

    public String getSignature() {
        return signature;
    }

    public void setSignature(String signature) {
        this.signature = signature;
    }

    public long getTs() {
        return ts;
    }

    public void setTs(long ts) {
        this.ts = ts;
    }

    /** 浅拷贝（列表字段不可变，共享引用安全）——缓存命中后拷贝再签名，缓存对象永不 mutate */
    public ArtifactResponseDto copy() {
        ArtifactResponseDto c = new ArtifactResponseDto();
        c.decision = decision;
        c.denyReason = denyReason;
        c.planVersion = planVersion;
        c.rowPredicateTemplate = rowPredicateTemplate;
        c.placeholders = placeholders;
        c.visibleColumns = visibleColumns;
        c.masks = masks;
        c.tagScope = tagScope;
        c.obligations = obligations;
        c.matchedPolicies = matchedPolicies;
        return c;
    }
}
