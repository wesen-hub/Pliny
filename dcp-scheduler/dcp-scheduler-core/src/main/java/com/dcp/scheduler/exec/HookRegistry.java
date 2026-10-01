package com.dcp.scheduler.exec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 缩容钩子注册表(扩缩容子文档 §五):业务启动期按名注册,任务用 TaskSpec.scaleDownHooks 引用。
 * 主决策时按名取钩子;取不到 = 该任务无缩容能力(与 hooks=null 同义),只扩不缩。
 */
public class HookRegistry {

    private static final Logger log = LoggerFactory.getLogger(HookRegistry.class);

    private final Map<String, ScaleDownHooks> hooks = new ConcurrentHashMap<>();

    /** 注册钩子(按引用名)。 */
    public void register(String name, ScaleDownHooks impl) {
        hooks.put(name, impl);
        log.info("注册缩容钩子: {}", name);
    }

    /** 按名取钩子;未注册返回 null(任务视为只扩不缩)。 */
    public ScaleDownHooks get(String name) {
        return name == null || name.isEmpty() ? null : hooks.get(name);
    }
}
