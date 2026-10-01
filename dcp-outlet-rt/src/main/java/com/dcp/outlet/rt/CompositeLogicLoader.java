package com.dcp.outlet.rt;

import com.dcp.outlet.sdk.LoadedLogic;
import com.dcp.outlet.sdk.LogicLoader;
import com.dcp.outlet.sdk.LogicNotFoundException;
import com.dcp.outlet.sdk.LogicRef;

import java.util.ArrayList;
import java.util.List;

/**
 * 组合加载器:按顺序尝试多种来源(如 逻辑仓库 jar → SPI → 同 classpath)。
 *
 * <p>"逻辑从哪来"因此**可替换**:从"写在 worker 里"演进到"逻辑仓库 jar",
 * 只加一条加载路径,业务代码一行不改。
 */
public class CompositeLogicLoader implements LogicLoader {

    private final List<LogicLoader> loaders = new ArrayList<>();

    public CompositeLogicLoader(LogicLoader... loaders) {
        this.loaders.addAll(List.of(loaders));
    }

    public CompositeLogicLoader add(LogicLoader loader) {
        loaders.add(loader);
        return this;
    }

    @Override
    public LoadedLogic load(LogicRef ref) {
        for (LogicLoader l : loaders) {
            try {
                return l.load(ref);
            } catch (LogicNotFoundException ignored) {
                // 换下一个来源
            }
        }
        throw new LogicNotFoundException(ref);
    }
}
