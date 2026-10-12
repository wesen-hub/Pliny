package com.perm.common.bind;

import com.perm.common.model.AuthorizationContext;
import com.perm.common.render.PredicateTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * PEP 绑定阶段（执行管线 ② ParamBinder）
 * 把渲染占位符的 ref 解析为实际值，产出 bindOrder。
 * 铁律 C1：值唯一出现点——判定链上模板无值，绑定才注入值。
 * fail-closed：任何 ref 解析失败 → BindException → 上层降为 DENY。
 */
public final class ParamBinder {

    private ParamBinder() {
    }

    /** 绑定项：占位名 × 解析值 × 类型 */
    public record Binding(String name, Object value, PredicateTemplate.Kind kind) {
    }

    /**
     * 绑定入口
     * @param placeholders 渲染输出的占位符
     * @param context      授权上下文（Provider 提取）
     */
    public static List<Binding> bind(List<PredicateTemplate.Placeholder> placeholders,
                                     AuthorizationContext context) {
        List<Binding> bindOrder = new ArrayList<>();
        for (PredicateTemplate.Placeholder ph : placeholders) {
            if (ph.ref() == null) {
                // 字面量占位（渲染期直接绑定）
                bindOrder.add(new Binding(ph.name(), ph.value(), ph.kind()));
                continue;
            }
            Object value = resolveRef(ph.ref(), context);
            if (ph.kind() == PredicateTemplate.Kind.LIST && !(value instanceof List<?>)) {
                throw new BindException(ph.ref(), "引用 " + ph.ref() + " 应为列表");
            }
            bindOrder.add(new Binding(ph.name(), value, ph.kind()));
        }
        return bindOrder;
    }

    /** 解析 subject ref（subject.region / subject.roles / subject.userId） */
    private static Object resolveRef(String ref, AuthorizationContext context) {
        String[] path = ref.split("\\.", 2);
        if (path.length < 2 || !"subject".equals(path[0])) {
            throw new BindException(ref, "仅支持 subject 引用");
        }
        String field = path[1];
        switch (field) {
            case "userId":
                return context.userId();
            case "roles":
                return context.roles();
            default:
                Map<String, Object> attrs = context.attributes();
                if (attrs.containsKey(field)) {
                    return attrs.get(field);
                }
                // 浅层对象查找（如 subject.tagLevels 走 attributes.tagLevels）
                String first = field.split("\\.")[0];
                if (attrs.containsKey(first)) {
                    return attrs.get(first);
                }
                throw new BindException(ref, "无法解析引用 " + ref + "（fail-closed）");
        }
    }
}
