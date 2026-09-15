package com.dcp.outlet.worker;

/**
 * 契约提供者工厂:**每个单元**一个 {@link ContractProvider}(单元的契约不同,不能全局共用)。
 *
 * <p>生产实现(`outlet-adapters.HttpContractProviderFactory`)从控制面拉契约视图并自带缓存与
 * **30s 刷新**:权限收窄 / 吊销在下一批生效,不必重启单元。
 *
 * <p>{@link #permissive()} 是**缺省值,只给骨架与自检用**:它下发无限制视图(不裁任何列/行)。
 * <b>生产装配不得使用</b> —— 那等于契约校验不存在;正确做法是让单元携带 `contractRef`,
 * 由工厂向控制面拉视图(拉不到且无缓存 → 拒绝消费,fail-closed)。
 */
@FunctionalInterface
public interface ContractProviderFactory {

    ContractProvider forUnit(CreateUnitCmd cmd);

    /** 缺省(仅骨架/自检):无限制视图。 */
    static ContractProviderFactory permissive() {
        ContractView view = DefaultContractFilter.permissive().currentView();
        return cmd -> () -> view;
    }
}
