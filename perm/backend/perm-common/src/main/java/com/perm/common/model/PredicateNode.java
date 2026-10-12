package com.perm.common.model;

/**
 * 行谓词 AST（引擎无关语义模型，设计文档 §3.3）
 * 各节点由 Predicates 工厂构造；渲染器（render 包）负责翻译为目标方言模板。
 */
public sealed interface PredicateNode
        permits EqNode, InNode, RangeNode, RegexNode, AndNode, OrNode, ConstNode {
}
