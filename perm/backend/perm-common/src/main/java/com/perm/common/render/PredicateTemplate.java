package com.perm.common.render;

import java.util.ArrayList;
import java.util.List;

/**
 * 渲染产物：SQL 模板 + 占位符列表
 * 占位符模型：
 *   { name: ":p0", ref: "subject.region", kind: SCALAR }   —— 单值绑定
 *   { name: ":list0", ref: "subject.depts", kind: LIST }   —— 列表绑定（注入阶段展开为 n 个 ?）
 */
public class PredicateTemplate {

    public enum Kind {
        /** 单值绑定 */
        SCALAR,
        /** 列表绑定（绑定后按数组长度展开为 n 个 ?） */
        LIST
    }

    /**
     * 占位符
     * @param name  占位名（:p0 / :list0）
     * @param ref   绑定引用（subject.xxx）；null = 字面量（用 value）
     * @param kind  绑定类型
     * @param value 字面量值（仅 ref == null 时有效）
     */
    public record Placeholder(String name, String ref, Kind kind, Object value) {
        public Placeholder(String name, String ref, Kind kind) {
            this(name, ref, kind, null);
        }
    }

    private final String template;
    private final List<Placeholder> placeholders;

    public PredicateTemplate(String template, List<Placeholder> placeholders) {
        this.template = template;
        this.placeholders = List.copyOf(placeholders);
    }

    public String template() {
        return template;
    }

    public List<Placeholder> placeholders() {
        return placeholders;
    }
}
