package com.perm.common.render;

import com.perm.common.model.AndNode;
import com.perm.common.model.ConstNode;
import com.perm.common.model.EqNode;
import com.perm.common.model.InNode;
import com.perm.common.model.OrNode;
import com.perm.common.model.PolicyConstraints;
import com.perm.common.model.PredicateNode;
import com.perm.common.model.RangeNode;
import com.perm.common.model.RegexNode;
import com.perm.common.model.Value;

import java.util.ArrayList;
import java.util.List;

/**
 * MySQL 方言渲染器
 * 翻译规则：
 *   EQ     → `col` = ?
 *   IN     → `col` IN (:listN)     —— 列表占位标记，注入阶段按绑定数组长度展开
 *   RANGE  → `col` >= ? AND `col` <= ?（单边只产一个占位）
 *   REGEX  → `col` REGEXP ?（字面量绑定为参数）
 *   AND/OR → (A AND B) / (A OR B)
 *   CONST  → 1=1 / 1=0
 * 铁律 C1：模板无值——引用只产占位符，字面量绑定为参数（值不进 SQL 文本，R-Sec2）。
 */
public class MySqlRenderer implements DialectRenderer {

    public static final String DIALECT = "mysql";

    @Override
    public String dialect() {
        return DIALECT;
    }

    @Override
    public PredicateTemplate renderPredicate(PredicateNode predicate) {
        List<PredicateTemplate.Placeholder> placeholders = new ArrayList<>();
        String template = renderNode(predicate, placeholders);
        return new PredicateTemplate(template, placeholders);
    }

    private String renderNode(PredicateNode node, List<PredicateTemplate.Placeholder> placeholders) {
        if (node instanceof ConstNode c) {
            return c.value() ? "1=1" : "1=0";
        }
        if (node instanceof EqNode e) {
            return "`" + e.column() + "` = " + bindValue(e.value(), placeholders);
        }
        if (node instanceof InNode in) {
            return "`" + in.column() + "` IN (" + bindList(in.value(), placeholders) + ")";
        }
        if (node instanceof RangeNode r) {
            List<String> parts = new ArrayList<>();
            if (r.min() != null) {
                parts.add("`" + r.column() + "` >= " + bindValue(r.min(), placeholders));
            }
            if (r.max() != null) {
                parts.add("`" + r.column() + "` <= " + bindValue(r.max(), placeholders));
            }
            if (parts.isEmpty()) {
                throw new RenderException("RANGE 节点 min/max 均为空: " + r.column());
            }
            return String.join(" AND ", parts);
        }
        if (node instanceof RegexNode rx) {
            return "`" + rx.column() + "` REGEXP "
                    + bindValue(new Value.Lit(rx.pattern()), placeholders);
        }
        if (node instanceof AndNode a) {
            List<String> parts = a.children().stream()
                    .map(c -> renderNode(c, placeholders))
                    .toList();
            return "(" + String.join(" AND ", parts) + ")";
        }
        if (node instanceof OrNode o) {
            List<String> parts = o.children().stream()
                    .map(c -> renderNode(c, placeholders))
                    .toList();
            return "(" + String.join(" OR ", parts) + ")";
        }
        throw new RenderException("未实现的行谓词节点: " + node.getClass().getSimpleName());
    }

    /** 单值绑定：ref → 占位符；字面量 → 参数占位（值不进 SQL 文本） */
    private String bindValue(Value value, List<PredicateTemplate.Placeholder> placeholders) {
        if (value instanceof Value.Ref ref) {
            String name = ":p" + placeholders.size();
            placeholders.add(new PredicateTemplate.Placeholder(name, ref.ref(),
                    PredicateTemplate.Kind.SCALAR));
            return "?";
        }
        if (value instanceof Value.Lit lit) {
            String name = ":p" + placeholders.size();
            placeholders.add(new PredicateTemplate.Placeholder(name, null,
                    PredicateTemplate.Kind.SCALAR, lit.value()));
            return "?";
        }
        throw new RenderException("未知谓词值类型: " + value.getClass().getSimpleName());
    }

    /** 列表绑定：占位标记 :listN，注入阶段展开为 n 个 ? */
    private String bindList(Value value, List<PredicateTemplate.Placeholder> placeholders) {
        if (!(value instanceof Value.Ref ref)) {
            throw new RenderException("IN 节点值必须为列表引用，实际: " + value.getClass().getSimpleName());
        }
        String name = ":list" + placeholders.size();
        placeholders.add(new PredicateTemplate.Placeholder(name, ref.ref(),
                PredicateTemplate.Kind.LIST));
        return name;
    }

    @Override
    public String renderMask(String column, PolicyConstraints.MaskSpec mask) {
        String c = "`" + column + "`";
        switch (mask.type()) {
            case NULL:
                return "NULL AS " + c;
            case HASH:
                return "SHA2(" + c + ", 256) AS " + c;
            case PARTIAL:
                return "CONCAT(LEFT(" + c + ", 3), '****') AS " + c;
            case REPLACE:
                return "'***' AS " + c;
            default:
                throw new RenderException("未实现的掩码类型: " + mask.type());
        }
    }
}
