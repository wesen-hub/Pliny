package com.perm.pep;

import java.util.List;
import java.util.Map;

/**
 * PEP 判定上下文（业务侧构造，语义与 PDP 判定请求一致）
 * 上下文提取（userId/roles/attributes 从请求/会话构造）由业务方实现，SDK 提供数据载体。
 */
public final class PepContext {

    private final String userId;
    private final List<String> roles;
    private final Map<String, Object> attributes;
    private final Map<String, Integer> tagLevels;
    private final String resourceId;
    private final String action;
    private final String dialect;

    private PepContext(Builder builder) {
        this.userId = builder.userId;
        this.roles = builder.roles == null ? List.of() : List.copyOf(builder.roles);
        this.attributes = builder.attributes == null ? Map.of() : Map.copyOf(builder.attributes);
        this.tagLevels = builder.tagLevels == null ? Map.of() : Map.copyOf(builder.tagLevels);
        this.resourceId = builder.resourceId;
        this.action = builder.action;
        this.dialect = builder.dialect == null ? "mysql" : builder.dialect;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String userId() {
        return userId;
    }

    public List<String> roles() {
        return roles;
    }

    public Map<String, Object> attributes() {
        return attributes;
    }

    public Map<String, Integer> tagLevels() {
        return tagLevels;
    }

    public String resourceId() {
        return resourceId;
    }

    public String action() {
        return action;
    }

    public String dialect() {
        return dialect;
    }

    public static final class Builder {
        private String userId;
        private List<String> roles;
        private Map<String, Object> attributes;
        private Map<String, Integer> tagLevels;
        private String resourceId;
        private String action;
        private String dialect;

        public Builder userId(String userId) {
            this.userId = userId;
            return this;
        }

        public Builder roles(List<String> roles) {
            this.roles = roles;
            return this;
        }

        public Builder attributes(Map<String, Object> attributes) {
            this.attributes = attributes;
            return this;
        }

        public Builder tagLevels(Map<String, Integer> tagLevels) {
            this.tagLevels = tagLevels;
            return this;
        }

        public Builder resourceId(String resourceId) {
            this.resourceId = resourceId;
            return this;
        }

        public Builder action(String action) {
            this.action = action;
            return this;
        }

        public Builder dialect(String dialect) {
            this.dialect = dialect;
            return this;
        }

        public PepContext build() {
            if (userId == null || resourceId == null || action == null) {
                throw new IllegalArgumentException("userId/resourceId/action 必填");
            }
            return new PepContext(this);
        }
    }
}
