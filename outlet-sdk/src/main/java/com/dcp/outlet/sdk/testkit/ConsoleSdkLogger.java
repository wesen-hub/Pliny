package com.dcp.outlet.sdk.testkit;

import com.dcp.outlet.sdk.SdkLogger;

/** 控制台日志实现(slf4j 风格的 {@code {}} 占位符),供本地测试使用。 */
public class ConsoleSdkLogger implements SdkLogger {

    private final String prefix;
    private final boolean debug;

    public ConsoleSdkLogger() { this("", false); }

    public ConsoleSdkLogger(String prefix, boolean debug) {
        this.prefix = prefix == null ? "" : prefix;
        this.debug = debug;
    }

    static String format(String f, Object... args) {
        StringBuilder sb = new StringBuilder();
        int idx = 0, ai = 0, k;
        while ((k = f.indexOf("{}", idx)) >= 0) {
            sb.append(f, idx, k);
            sb.append(ai < args.length ? String.valueOf(args[ai++]) : "{}");
            idx = k + 2;
        }
        return sb.append(f.substring(idx)).toString();
    }

    private void out(String level, String f, Object... a) {
        System.out.println("[" + level + "] " + prefix + format(f, a));
    }

    @Override public boolean isDebugEnabled() { return debug; }
    @Override public void debug(String f, Object... a) { if (debug) out("debug", f, a); }
    @Override public void info(String f, Object... a) { out("info", f, a); }
    @Override public void warn(String f, Object... a) { out("warn", f, a); }
    @Override public void error(String m, Throwable t) { out("error", m + " : " + t); }
    @Override public void error(String f, Object... a) { out("error", f, a); }
}
