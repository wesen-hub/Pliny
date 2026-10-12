package com.perm.common.model;

/** 等值：col = value（如 region = ${subject.region}） */
public record EqNode(String column, Value value) implements PredicateNode {
}
