package com.perm.pdp.dto;

import java.util.List;
import java.util.Map;

/**
 * 判定请求 DTO（PEP → PDP）
 * 与 perm-common 的 AuthorizationRequest 语义一致，网络层用原始集合承载上下文。
 * 标准 JavaBean（Jackson 序列化契约）。
 */
public class AuthorizeRequestDto {

    private String userId;
    private List<String> roles;
    private Map<String, Object> attributes;
    private Map<String, Integer> tagLevels;
    private String resourceId;
    private String action;
    private String dialect;

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public List<String> getRoles() {
        return roles;
    }

    public void setRoles(List<String> roles) {
        this.roles = roles;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
    }

    public Map<String, Integer> getTagLevels() {
        return tagLevels;
    }

    public void setTagLevels(Map<String, Integer> tagLevels) {
        this.tagLevels = tagLevels;
    }

    public String getResourceId() {
        return resourceId;
    }

    public void setResourceId(String resourceId) {
        this.resourceId = resourceId;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public String getDialect() {
        return dialect;
    }

    public void setDialect(String dialect) {
        this.dialect = dialect;
    }
}
