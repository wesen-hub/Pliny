package com.dcp.outlet.service.config;

import com.dcp.outlet.sdk.SdkLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 控制面批阶段用的 SDK 日志门面:接到 slf4j(与计算面的 worker-service 同名类等价,
 * 但 worker-service 是独立部署单元,不在此依赖内)。
 */
public class Slf4jSdkLogger implements SdkLogger {

    private final Logger log;

    public Slf4jSdkLogger(String name) {
        this.log = LoggerFactory.getLogger(name);
    }

    @Override public boolean isDebugEnabled() { return log.isDebugEnabled(); }
    @Override public void debug(String f, Object... a) { log.debug(f, a); }
    @Override public void info(String f, Object... a) { log.info(f, a); }
    @Override public void warn(String f, Object... a) { log.warn(f, a); }
    @Override public void error(String m, Throwable t) { log.error(m, t); }
    @Override public void error(String f, Object... a) { log.error(f, a); }
}
