package com.perm.common.model;

import java.util.Arrays;

/**
 * 行谓词 AST 构造工厂
 */
public final class Predicates {

    private Predicates() {
    }

    /** 等值：col = ref（如 region = ${subject.region}） */
    public static PredicateNode eq(String column, String ref) {
        return new EqNode(column, new Value.Ref(ref));
    }

    /** 等值：col = 字面量 */
    public static PredicateNode eqLit(String column, Object value) {
        return new EqNode(column, new Value.Lit(value));
    }

    /** 成员：col IN ref（ref 为列表） */
    public static PredicateNode in(String column, String ref) {
        return new InNode(column, new Value.Ref(ref));
    }

    /** 范围：min/max 为引用；null 表示单边 */
    public static PredicateNode range(String column, String minRef, String maxRef) {
        return new RangeNode(column,
                minRef == null ? null : new Value.Ref(minRef),
                maxRef == null ? null : new Value.Ref(maxRef));
    }

    /** 范围：min/max 为字面量 */
    public static PredicateNode rangeLit(String column, Object min, Object max) {
        return new RangeNode(column,
                min == null ? null : new Value.Lit(min),
                max == null ? null : new Value.Lit(max));
    }

    /** 正则（字面量） */
    public static PredicateNode regex(String column, String pattern) {
        return new RegexNode(column, pattern);
    }

    /** 逻辑与 */
    public static PredicateNode and(PredicateNode... children) {
        return new AndNode(Arrays.asList(children));
    }

    /** 逻辑或 */
    public static PredicateNode or(PredicateNode... children) {
        return new OrNode(Arrays.asList(children));
    }
}
