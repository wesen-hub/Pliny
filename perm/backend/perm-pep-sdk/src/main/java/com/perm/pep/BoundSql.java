package com.perm.pep;

import java.util.List;

/**
 * 改写结果：最终 SQL + 绑定值（bindOrder 业务参数在前，权限参数在后）+ 列裁剪/掩码元信息。
 * 业务侧执行时：JDBC 层按 bindValues 顺序绑定占位符；ORM 层按 visibleColumns/masks 做投影与脱敏。
 */
public record BoundSql(
        /** 改写后的 SQL（行谓词已注入 WHERE） */
        String sql,
        /** 绑定值（bindOrder 契约：业务参数在前，权限参数在后） */
        List<Object> bindValues,
        /** 可见列（null = 不裁剪） */
        List<String> visibleColumns,
        /** 掩码列表（最严者胜） */
        List<VerifiedArtifact.Mask> masks,
        /** 标签扩展模式（标签模式 A 快照语义由业务适配器实现） */
        String tagExpandMode) {

    public BoundSql {
        bindValues = bindValues == null ? List.of() : List.copyOf(bindValues);
        visibleColumns = visibleColumns == null ? null : List.copyOf(visibleColumns);
        masks = masks == null ? List.of() : List.copyOf(masks);
    }
}
