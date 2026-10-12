package com.perm.common.render;

import com.perm.common.model.PolicyConstraints;
import com.perm.common.model.PredicateNode;

/**
 * 方言渲染器 SPI（新引擎接入扩展点）
 * 职责：把行谓词 AST 翻译为目标方言模板 + 占位符（铁律 C1：模板无值）。
 * 官方内置：mysql（MySqlRenderer）；trino/es/neo4j/spark/flink 由各自 Renderer 提供。
 */
public interface DialectRenderer {

    /** 方言标识（与 AuthorizationRequest.dialect 对应） */
    String dialect();

    /**
     * 渲染行谓词 → 模板 + 占位符
     * @param predicate 行谓词 AST
     */
    PredicateTemplate renderPredicate(PredicateNode predicate);

    /**
     * 渲染掩码列投影表达式（FULL 由投影器剔除，不进本方法）
     */
    String renderMask(String column, PolicyConstraints.MaskSpec mask);
}
