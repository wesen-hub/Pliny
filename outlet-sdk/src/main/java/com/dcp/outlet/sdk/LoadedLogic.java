package com.dcp.outlet.sdk;

/**
 * 已加载的逻辑句柄:逻辑实例 + 释放资源的钩子。
 *
 * <p>"逻辑从哪来"是**可替换**的(同 classpath / SPI / 逻辑仓库 jar),由 {@link LogicLoader} 屏蔽。
 */
public interface LoadedLogic {

    LogicRef ref();

    ConsumerLogic logic();

    /** 关闭 ClassLoader / 释放 jar 句柄(jar 方式需要;classpath 方式为空实现)。 */
    void close();
}
