package com.dcp.outlet.sdk;

/**
 * SDK 自带日志门面:逻辑作者**不需要**引入 slf4j/logback,避免与业务日志框架冲突。
 * 平台侧实现会把日志带上 {@code taskId/unitId/partition} 前缀。
 *
 * <p><b>占位符风格与 slf4j 一致:使用 {@code {}}</b>(不是 {@code %s}):
 * {@code log.info("index={} id={}", index, id);}
 */
public interface SdkLogger {

    boolean isDebugEnabled();

    void debug(String format, Object... args);

    void info(String format, Object... args);

    void warn(String format, Object... args);

    /** 带异常栈的错误日志。 */
    void error(String message, Throwable t);

    void error(String format, Object... args);
}
