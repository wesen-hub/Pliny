package com.perm.common.inject;

import com.perm.common.bind.ParamBinder;
import com.perm.common.render.PredicateTemplate;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * PEP 注入阶段（执行管线 ③ PredicateInjector）
 * 权限谓词注入 WHERE；权限 ? 一律追加在原业务 ? 之后（bindOrder 顺序契约）。
 * 说明：v1 核心引擎用正则级简化改写（演示级），生产实现由各接入适配器
 * （MyBatis 拦截器 / JDBC 代理）基于 AST 解析器完成同等语义。
 */
public final class PredicateInjector {

    /** FROM 之后到 WHERE/GROUP/ORDER/LIMIT 之前的切割点 */
    private static final Pattern FROM_TAIL = Pattern.compile(
            "^(?is)(.*?\\bfrom\\s+\\S+\\s*)", Pattern.CASE_INSENSITIVE);

    private PredicateInjector() {
    }

    /**
     * 把渲染模板 + 绑定值拼成最终权限谓词 SQL（占位展开）
     * @param template 含 :p0 / :list0 占位的模板
     * @param bindings binder 输出
     */
    public static String renderBoundPredicate(String template, List<ParamBinder.Binding> bindings) {
        String sql = template;
        for (ParamBinder.Binding b : bindings) {
            if (b.kind() == PredicateTemplate.Kind.LIST) {
                List<?> values = (List<?>) b.value();
                String marks = String.join(", ", values.stream().map(v -> "?").toList());
                sql = sql.replace(b.name(), marks);
            } else {
                sql = sql.replace(b.name(), "?");
            }
        }
        return sql;
    }

    /**
     * 谓词注入：权限谓词注入 WHERE；无 WHERE 自动补（R-S 支持范围内的标准形态）
     * @param sql           原始业务 SQL
     * @param permPredicate 权限谓词 SQL 文本（含 ? 占位，list 已展开）；null = 不注入
     */
    public static String injectWhere(String sql, String permPredicate) {
        if (permPredicate == null || permPredicate.isBlank()) {
            return sql;
        }
        Matcher m = FROM_TAIL.matcher(sql);
        if (!m.find()) {
            throw new IllegalArgumentException("无法定位 FROM 子句（演示级改写仅支持简单 SELECT）: " + sql);
        }
        // stripTrailing：FROM_TAIL 的 \s* 会贪婪吃掉表名后的空格，标准化后再统一补空格，
        // 避免无尾空格 SQL（如 "FROM sales_order"）拼接出 "sales_orderWHERE" 粘连
        String head = m.group(1).stripTrailing();
        String tail = sql.substring(m.end());

        Matcher where = Pattern.compile("(?is)^\\s*(where)\\s+").matcher(tail);
        if (where.find()) {
            String orig = tail.substring(where.end());
            // 权限谓词注入在原 WHERE 之后：原 ? 位置不动，权限 ? 自然追加（bindOrder 顺序契约：业务参数在前）
            return head + " WHERE (" + orig + ") AND (" + permPredicate + ")";
        }
        return head + " WHERE " + permPredicate + " " + tail.stripLeading();
    }
}
