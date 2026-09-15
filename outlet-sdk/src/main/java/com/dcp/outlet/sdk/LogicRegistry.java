package com.dcp.outlet.sdk;

import java.util.List;
import java.util.Optional;

/**
 * 逻辑注册表(运行在**计算面 worker 进程内**)。
 *
 * <p>三种注册方式:① 注解 + Bean 自动扫描;② 手动 {@link #register};③ 部署期 SPI
 * ({@code META-INF/services/com.dcp.outlet.sdk.ConsumerLogicProvider})。
 *
 * <p><b>重要</b>:注册只影响**当前进程**。若在控制面或你自己的后台服务里注册,worker 看不到。
 */
public interface LogicRegistry {

    /** 注册一个逻辑实例(同 id@version 重复注册会覆盖)。 */
    void register(LogicRef ref, ConsumerLogic logic);

    default void register(String logicId, String version, ConsumerLogic logic) {
        register(LogicRef.of(logicId, version), logic);
    }

    Optional<ConsumerLogic> find(LogicRef ref);

    /** 已注册的逻辑清单(worker 启动时上报,供控制面对账"任务绑定的版本 vs 实际跑的版本")。 */
    List<LogicRef> list();
}
