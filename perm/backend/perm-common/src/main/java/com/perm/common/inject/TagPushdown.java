package com.perm.common.inject;

import com.perm.common.model.PolicyConstraints;
import com.perm.common.model.TagExpandMode;

import java.util.List;

/**
 * 标签下推（跨库定稿 R-T5）
 * 模式 A（list）：实体列表 → `targetKey` IN (?,...)——实体列表是值非表引用，跨库可下推；
 * 校验式（verify）：不产谓词，只保留授权声明（权限锚定圈选动作 + 审计后置）。
 */
public final class TagPushdown {

    private TagPushdown() {
    }

    /** 渲染标签谓词；null = 不注入 */
    public static String renderTagPredicate(PolicyConstraints.TagScope tagScope) {
        if (tagScope == null) {
            return null;
        }
        if (tagScope.expandMode() == TagExpandMode.VERIFY) {
            return null;
        }
        if (tagScope.expandMode() != TagExpandMode.LIST
                || tagScope.entities() == null || tagScope.entities().isEmpty()) {
            return null;
        }
        String marks = String.join(", ",
                tagScope.entities().stream().map(e -> "?").toList());
        return "`" + tagScope.targetKey() + "` IN (" + marks + ")";
    }

    /** 提取标签实体绑定值（追加到 bindOrder 尾部） */
    public static List<String> tagBindValues(PolicyConstraints.TagScope tagScope) {
        if (tagScope == null || tagScope.expandMode() != TagExpandMode.LIST) {
            return List.of();
        }
        return tagScope.entities();
    }
}
