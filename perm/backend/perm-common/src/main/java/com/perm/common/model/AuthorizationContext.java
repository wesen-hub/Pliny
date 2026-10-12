package com.perm.common.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 授权上下文（PEP 侧 Provider 提取后上报 PDP）
 * 主体身份真实性由业务认证系统保证，PDP 只消费上报值（R-Sec1）。
 */
public class AuthorizationContext {

    private final String userId;
    private final List<String> roles;
    /** 属性（region / depts / allowedStatus / groups ...），行谓词 ref 的解析来源 */
    private final Map<String, Object> attributes;
    /** tagPath -> level（R-T2 门槛输入） */
    private final Map<String, Integer> tagLevels;

    private AuthorizationContext(Builder b) {
        this.userId = b.userId;
        this.roles = List.copyOf(b.roles);
        this.attributes = new HashMap<>(b.attributes);
        this.tagLevels = new HashMap<>(b.tagLevels);
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

    /** 标签门槛查询（R-T2）：无记录默认 T0 */
    public int tagLevelOf(String tagPath) {
        return tagLevels.getOrDefault(tagPath, 0);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String userId;
        private final List<String> roles = new ArrayList<>();
        private final Map<String, Object> attributes = new HashMap<>();
        private final Map<String, Integer> tagLevels = new HashMap<>();

        public Builder userId(String v) {
            this.userId = v;
            return this;
        }

        public Builder role(String v) {
            this.roles.add(v);
            return this;
        }

        public Builder attribute(String key, Object value) {
            this.attributes.put(key, value);
            return this;
        }

        public Builder tagLevel(String tagPath, int level) {
            this.tagLevels.put(tagPath, level);
            return this;
        }

        public AuthorizationContext build() {
            return new AuthorizationContext(this);
        }
    }
}
