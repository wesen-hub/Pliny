package com.perm.common.model;

/** 范围：min <= col <= max（min/max 可为 null 表示单边） */
public record RangeNode(String column, Value min, Value max) implements PredicateNode {
}
