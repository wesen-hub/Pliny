package com.perm.common.model;

/**
 * 判定请求（PEP → PDP）
 */
public class AuthorizationRequest {

    private final AuthorizationContext context;
    /** 三段式 resourceId：ns.type.name（如 sdm.sales_order），v1 精确匹配 */
    private final String resourceId;
    /** 动作：select / insert / update / delete / export ... */
    private final String action;
    /** 目标方言：mysql / tidb / trino / es / neo4j / spark / flink */
    private final String dialect;

    private AuthorizationRequest(Builder b) {
        this.context = b.context;
        this.resourceId = b.resourceId;
        this.action = b.action;
        this.dialect = b.dialect == null ? "mysql" : b.dialect;
    }

    public AuthorizationContext context() {
        return context;
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

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private AuthorizationContext context;
        private String resourceId;
        private String action;
        private String dialect;

        public Builder context(AuthorizationContext v) {
            this.context = v;
            return this;
        }

        public Builder resourceId(String v) {
            this.resourceId = v;
            return this;
        }

        public Builder action(String v) {
            this.action = v;
            return this;
        }

        public Builder dialect(String v) {
            this.dialect = v;
            return this;
        }

        public AuthorizationRequest build() {
            return new AuthorizationRequest(this);
        }
    }
}
