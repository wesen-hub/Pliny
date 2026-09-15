package com.dcp.outlet.workersvc;

import com.dcp.outlet.sdk.SdkLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 把 SDK 日志门面接到 worker 自身的 slf4j(日志带 unitId 前缀,便于多单元混布时排查)。 */
public class Slf4jSdkLogger implements SdkLogger {

    private final Logger log;

    public Slf4jSdkLogger(String unitId) {
        this.log = LoggerFactory.getLogger("outlet-unit." + unitId);
    }

    @Override public boolean isDebugEnabled() { return log.isDebugEnabled(); }
    @Override public void debug(String f, Object... a) { log.debug(f, a); }
    @Override public void info(String f, Object... a) { log.info(f, a); }
    @Override public void warn(String f, Object... a) { log.warn(f, a); }
    @Override public void error(String m, Throwable t) { log.error(m, t); }
    @Override public void error(String f, Object... a) { log.error(f, a); }
}
