package com.perm.common.model;

/** 常量：恒真（无行级约束）或恒假 */
public record ConstNode(boolean value) implements PredicateNode {

    public static final ConstNode TRUE = new ConstNode(true);

    public static final ConstNode FALSE = new ConstNode(false);
}
