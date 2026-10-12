package com.perm.common.model;

/** 正则：col REGEXP pattern（字面量） */
public record RegexNode(String column, String pattern) implements PredicateNode {
}
