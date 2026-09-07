package com.dcp.seatunnel.transform;

import com.googlecode.aviator.AviatorEvaluatorInstance;

import java.util.Map;

/** Aviator 安全求值器（与平台端一致的白名单函数，§10 S-6） */
public final class ExpressionUtil {
    private ExpressionUtil() {
    }

    public static AviatorEvaluatorInstance newSafeEvaluator(Map<String, Map<String, String>> dictData) {
        AviatorEvaluatorInstance ev = com.googlecode.aviator.AviatorEvaluator.newInstance();
        ev.addFunction(new com.googlecode.aviator.runtime.function.AbstractFunction() {
            public String getName() { return "default"; }
            public com.googlecode.aviator.runtime.type.AviatorObject call(
                    Map<String, Object> env, com.googlecode.aviator.runtime.type.AviatorObject a1,
                    com.googlecode.aviator.runtime.type.AviatorObject a2) {
                Object v = a1.getValue(env);
                return v == null ? a2 : a1;
            }
        });
        ev.addFunction(new com.googlecode.aviator.runtime.function.AbstractFunction() {
            public String getName() { return "dict.lookup"; }
            public com.googlecode.aviator.runtime.type.AviatorObject call(
                    Map<String, Object> env, com.googlecode.aviator.runtime.type.AviatorObject a1,
                    com.googlecode.aviator.runtime.type.AviatorObject a2) {
                Object name = a1.getValue(env);
                Object code = a2.getValue(env);
                if (name == null || code == null) return com.googlecode.aviator.runtime.type.AviatorNil.NIL;
                Map<String, String> m = dictData.get(String.valueOf(name));
                if (m == null) return com.googlecode.aviator.runtime.type.AviatorNil.NIL;
                String v = m.get(String.valueOf(code));
                return v == null ? com.googlecode.aviator.runtime.type.AviatorNil.NIL
                        : new com.googlecode.aviator.runtime.type.AviatorString(v);
            }
        });
        ev.addFunction(new com.googlecode.aviator.runtime.function.AbstractFunction() {
            public String getName() { return "date.format"; }
            public com.googlecode.aviator.runtime.type.AviatorObject call(
                    Map<String, Object> env, com.googlecode.aviator.runtime.type.AviatorObject a1,
                    com.googlecode.aviator.runtime.type.AviatorObject a2) {
                Object v = a1.getValue(env);
                Object pattern = a2.getValue(env);
                if (v == null || pattern == null) return com.googlecode.aviator.runtime.type.AviatorNil.NIL;
                java.util.Date d = toDate(v);
                if (d == null) return com.googlecode.aviator.runtime.type.AviatorNil.NIL;
                try {
                    return new com.googlecode.aviator.runtime.type.AviatorString(
                            new java.text.SimpleDateFormat(String.valueOf(pattern)).format(d));
                } catch (Exception e) {
                    return com.googlecode.aviator.runtime.type.AviatorNil.NIL;
                }
            }
        });
        return ev;
    }

    public static Object normalize(Object v) {
        if (v == null) return null;
        // BigDecimal 保持原类型（sink 端按 Decimal 绑定参数, 转 Double 会 ClassCastException）
        if (v instanceof java.sql.Timestamp ts) return new java.util.Date(ts.getTime());
        if (v instanceof java.time.LocalDateTime ldt) {
            return java.util.Date.from(ldt.atZone(java.time.ZoneId.systemDefault()).toInstant());
        }
        if (v instanceof byte[] b) return new String(b, java.nio.charset.StandardCharsets.UTF_8);
        return v;
    }

    /** 对 env 逐字段 normalize 的副本（仅供 Aviator 表达式求值, 不修改原始 env） */
    public static java.util.Map<String, Object> normalizeEnv(java.util.Map<String, Object> raw) {
        java.util.Map<String, Object> out = new java.util.LinkedHashMap<>();
        if (raw == null) {
            return out;
        }
        for (java.util.Map.Entry<String, Object> e : raw.entrySet()) {
            out.put(e.getKey(), normalize(e.getValue()));
        }
        return out;
    }

    private static java.util.Date toDate(Object v) {
        if (v instanceof java.util.Date d) return d;
        if (v instanceof Number n) return new java.util.Date(n.longValue());
        if (v instanceof java.time.LocalDateTime ldt) {
            return java.util.Date.from(ldt.atZone(java.time.ZoneId.systemDefault()).toInstant());
        }
        return null;
    }
}
