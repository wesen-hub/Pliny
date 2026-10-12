package com.perm.common.model;

import java.util.List;

/** 逻辑或（子节点列表） */
public record OrNode(List<PredicateNode> children) implements PredicateNode {

    public OrNode {
        children = List.copyOf(children);
    }
}
