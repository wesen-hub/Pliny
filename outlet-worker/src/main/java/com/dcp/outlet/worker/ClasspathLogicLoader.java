package com.dcp.outlet.worker;

import com.dcp.outlet.sdk.ConsumerLogic;
import com.dcp.outlet.sdk.LoadedLogic;
import com.dcp.outlet.sdk.LogicLoader;
import com.dcp.outlet.sdk.LogicNotFoundException;
import com.dcp.outlet.sdk.LogicRef;
import com.dcp.outlet.sdk.LogicRegistry;

/** 同 classpath 加载:逻辑写在 worker 工程里(注解 / 手动注册),零分发。 */
public class ClasspathLogicLoader implements LogicLoader {

    private final LogicRegistry registry;

    public ClasspathLogicLoader(LogicRegistry registry) { this.registry = registry; }

    @Override
    public LoadedLogic load(LogicRef ref) {
        ConsumerLogic logic = registry.find(ref)
                .orElseThrow(() -> new LogicNotFoundException(ref));
        return new SimpleLoadedLogic(ref, logic);
    }
}
