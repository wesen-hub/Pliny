package com.dcp.tagjob.util;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/** 结构化中文日志(最小实现, 避免引入 slf4j/log4j 与 spark 运行环境冲突) */
public final class Log {
    private static final DateTimeFormatter FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    private Log() {
    }

    public static void info(String msg) {
        println("INFO ", msg);
    }

    public static void warn(String msg) {
        println("WARN ", msg);
    }

    public static void error(String msg) {
        println("ERROR", msg);
    }

    public static void info(String tag, Object... kv) {
        StringBuilder sb = new StringBuilder(tag);
        for (Object o : kv) sb.append(' ').append(o);
        println("INFO ", sb.toString());
    }

    private static synchronized void println(String lvl, String msg) {
        System.out.println(LocalDateTime.now().format(FMT) + " [" + lvl + "] [tagengine-executor] " + msg);
        if ("ERROR".equals(lvl) || "WARN ".equals(lvl)) System.err.flush();
    }
}
