package com.dcp.tagjob.run;

import org.kie.api.runtime.KieSession;

import java.util.ArrayList;
import java.util.List;

/**
 * 流执行动作收集器(dcp-tagstream 内嵌; 与 executor/backend 同 FQCN —— 流 DRL 复用
 * DrlGenerator 生成的 function 包装, 动作 tag()/halt() 落在 ThreadLocal 收集器)。
 */
public final class TagActions {
    private static final ThreadLocal<Collector> COLLECTOR = new ThreadLocal<>();
    private static final ThreadLocal<KieSession> SESSION = new ThreadLocal<>();

    private TagActions() {
    }

    public static void bindCollector(Collector c) {
        COLLECTOR.set(c);
    }

    public static void bindSession(KieSession session) {
        SESSION.set(session);
    }

    public static void clearThread() {
        COLLECTOR.remove();
        SESSION.remove();
    }

    public static void tag(Object fact, String path) {
        tag(fact, path, null);
    }

    public static void tag(Object fact, String path, String value) {
        Collector c = COLLECTOR.get();
        if (c != null) c.add(path, value);
    }

    public static void halt() {
        Collector c = COLLECTOR.get();
        if (c != null) c.halt();
        KieSession s = SESSION.get();
        if (s != null) {
            try {
                s.halt();
            } catch (Throwable ignore) {
            }
        }
    }

    public static class Collector {
        private final List<String[]> tags = new ArrayList<>();
        private boolean halted = false;

        public void add(String path, String value) {
            tags.add(new String[]{path, value});
        }

        public void halt() {
            halted = true;
        }

        public List<String[]> tags() {
            return tags;
        }
    }
}
