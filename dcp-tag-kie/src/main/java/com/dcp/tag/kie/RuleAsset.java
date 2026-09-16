package com.dcp.tag.kie;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * 资产(规则/规则流)的统一最小形状 —— 编译库收敛的枢纽。
 *
 * <p>三端各自的规则快照类结构相同(id/kind/name/ver/def)但类型不同:
 * <ul>
 *   <li>backend:{@code OnlineRule}(com.dcp.module.tagengine.online)</li>
 *   <li>dcp-tagstream:{@code StreamConfig.RuleRef}</li>
 *   <li>tagengine-executor:{@code TaskConfig.RuleSnapshot}</li>
 * </ul>
 * 它们各自 {@code implements RuleAsset} 后,{@link DrlGenerator} 等只需一份实现,不再三处拷贝。
 * 字段与方法同名合法(Java 字段/方法不同命名空间),各实现类保留 public 字段即可加访问器。
 */
public interface RuleAsset {

    long id();

    /** rule | flow */
    String kind();

    String name();

    int ver();

    /** rule: {mode: visual|table|drl, ...}; flow: {steps:[...]} */
    JsonNode def();
}
