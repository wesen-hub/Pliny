package com.dcp.outlet.sdk;

/**
 * 逻辑加载器(D14):屏蔽"逻辑从哪来"。
 *
 * <p>三种来源对同一份逻辑代码无差异:① 与 worker 同 classpath(注解 / 手动注册);
 * ② 扩展包 SPI;③ 逻辑仓库 jar(按 {@code logicRef} 拉取 + 校验 hash + 独立 ClassLoader 加载)。
 */
public interface LogicLoader {

    /**
     * 按引用加载逻辑。
     *
     * @throws LogicNotFoundException 仓库 / 注册表里没有该 {@code logicId@version}
     */
    LoadedLogic load(LogicRef ref);
}
