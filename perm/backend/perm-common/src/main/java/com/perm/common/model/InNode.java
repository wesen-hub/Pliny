package com.perm.common.model;

/** 成员：col IN value（value 为列表引用，绑定后展开为 n 个 ?） */
public record InNode(String column, Value value) implements PredicateNode {
}
