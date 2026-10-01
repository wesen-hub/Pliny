package com.dcp.outlet.rt;

import com.dcp.outlet.sdk.ConsumerLogic;
import com.dcp.outlet.sdk.LoadedLogic;
import com.dcp.outlet.sdk.LogicRef;

/** classpath / SPI 来源的加载结果(无需关闭 ClassLoader)。 */
public class SimpleLoadedLogic implements LoadedLogic {

    private final LogicRef ref;
    private final ConsumerLogic logic;

    public SimpleLoadedLogic(LogicRef ref, ConsumerLogic logic) {
        this.ref = ref; this.logic = logic;
    }

    @Override public LogicRef ref() { return ref; }

    @Override public ConsumerLogic logic() { return logic; }

    @Override public void close() { /* classpath 来源无需释放 */ }
}
