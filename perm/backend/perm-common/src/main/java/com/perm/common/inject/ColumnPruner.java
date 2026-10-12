package com.perm.common.inject;

import com.perm.common.model.Artifact;
import com.perm.common.model.MaskType;
import com.perm.common.model.PolicyConstraints;
import com.perm.common.render.DialectRenderer;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * PEP 投影阶段（执行管线 ④ ColumnPruner）
 * SELECT * 展开 + 可见列裁剪（R-D8）+ 掩码列表达式替换（R-D9）。
 * 说明：v1 为演示级字符串改写，生产实现由接入适配器基于 AST 解析器完成。
 */
public final class ColumnPruner {

    private ColumnPruner() {
    }

    /**
     * 列投影
     * @param sql           注入后的 SQL
     * @param artifact      判定产物（可见列 + 掩码）
     * @param schemaColumns 表全列（资源元数据，用于 SELECT * 展开）
     * @param renderer      方言渲染器（掩码表达式）
     */
    public static String projectColumns(String sql, Artifact artifact,
                                        List<String> schemaColumns,
                                        DialectRenderer renderer) {
        // SELECT ... FROM 之间的列表
        int fromIdx = indexOfFrom(sql);
        if (fromIdx < 0) {
            throw new IllegalArgumentException("无法定位 FROM 子句: " + sql);
        }
        String head = sql.substring(0, fromIdx);
        String selectBody = sql.substring(fromIdx);
        String selectList = head.replaceFirst("(?is)^select\\s+", "").trim();

        List<String> cols;
        if ("*".equals(selectList)) {
            cols = new ArrayList<>(schemaColumns);
        } else {
            cols = new ArrayList<>();
            for (String part : selectList.split(",")) {
                cols.add(part.trim());
            }
        }

        // 可见列裁剪（R-D8）；null = 不裁剪
        List<String> visible = artifact.visibleColumns();
        if (visible != null) {
            Set<String> visibleSet = new HashSet<>(visible);
            cols = cols.stream()
                    .filter(c -> visibleSet.contains(columnName(c)))
                    .collect(Collectors.toList());
        }

        // 掩码列替换（R-D9）：FULL 剔除；其余替换表达式
        Map<String, PolicyConstraints.MaskSpec> maskMap = artifact.masks().stream()
                .collect(Collectors.toMap(PolicyConstraints.MaskSpec::column, Function.identity()));
        List<String> projected = new ArrayList<>();
        for (String c : cols) {
            String name = columnName(c);
            PolicyConstraints.MaskSpec mask = maskMap.get(name);
            if (mask == null) {
                projected.add(c);
            } else if (mask.type() != MaskType.FULL) {
                projected.add(renderer.renderMask(name, mask));
            }
            // FULL → 剔除（不投影）
        }

        return "SELECT " + String.join(", ", projected) + " " + selectBody;
    }

    /** 提取列名（去反引号与别名） */
    private static String columnName(String part) {
        String name = part.replaceAll("^`|`$", "").split("\\s+")[0];
        return name;
    }

    /** 定位 FROM 关键字（演示级：首个 FROM） */
    private static int indexOfFrom(String sql) {
        return sql.toLowerCase().indexOf("from ");
    }
}
