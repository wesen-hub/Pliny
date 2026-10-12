package com.perm.common.model;

import java.util.List;

/** 逻辑与（子节点列表） */
public record AndNode(List<PredicateNode> children) implements PredicateNode {

    public AndNode {
        children = List.copyOf(children);
    }
}
