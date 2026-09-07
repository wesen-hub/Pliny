package com.dcp.engine;

import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.AviatorEvaluatorInstance;
import com.googlecode.aviator.Expression;
import com.googlecode.aviator.runtime.function.AbstractFunction;
import com.googlecode.aviator.runtime.type.AviatorNil;
import com.googlecode.aviator.runtime.type.AviatorObject;
import com.googlecode.aviator.runtime.type.AviatorString;
import lombok.extern.slf4j.Slf4j;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Aviator 表达式安全沙箱（设计文档 §5.4 / §10 S-6）
 * - 禁用反射 / TCCL / 模块导入
 * - 仅白名单自定义函数: default / dict.lookup / date.format / date.parse
 * - 表达式长度限制 2000 字符
 */
@Slf4j
public final class ExpressionUtil {
    public static final int MAX_EXPR_LENGTH = 2000;

    private ExpressionUtil() {
    }

    /** 构建安全求值器（每个任务一个实例，dictData 注入字典映射） */
    public static AviatorEvaluatorInstance newSafeEvaluator(Map<String, Map<String, String>> dictData) {
        AviatorEvaluatorInstance ev = AviatorEvaluator.newInstance();
        // 安全默认: Aviator 默认不开启反射/TCCL/模块导入（Feature.EnableReflection 等需显式开启）
        // 因此这里无需额外设置 Feature，仅注册白名单函数即可（S-6）
        ev.addFunction(new DefaultFunction());
        ev.addFunction(new CoalesceFunction());
        ev.addFunction(new DictLookupFunction(dictData == null ? Map.of() : dictData));
        ev.addFunction(new DateFormatFunction());
        ev.addFunction(new DateParseFunction());
        return ev;
    }

    /** 编译校验；返回 null 表示通过，否则返回错误信息 */
    public static String validate(String expression, Set<String> availableFields) {
        if (expression == null || expression.isBlank()) {
            return "表达式不能为空";
        }
        if (expression.length() > MAX_EXPR_LENGTH) {
            return "表达式长度超过限制 " + MAX_EXPR_LENGTH;
        }
        AviatorEvaluatorInstance ev = newSafeEvaluator(null);
        try {
            Expression compiled = ev.compile(expression);
            if (availableFields != null && !availableFields.isEmpty()) {
                Set<String> vars = new HashSet<>(compiled.getVariableNames());
                if (vars != null) {
                    for (String v : vars) {
                        if (!availableFields.contains(v)) {
                            return "未定义变量 '" + v + "'";
                        }
                    }
                }
            }
            return null;
        } catch (Exception e) {
            return e.getMessage() == null ? "表达式编译失败" : e.getMessage();
        }
    }

    /** 编译一组表达式（field -> expression），供逐行执行 */
    public static Map<String, Expression> compileAll(AviatorEvaluatorInstance ev, Map<String, String> fieldExpressions) {
        Map<String, Expression> result = new HashMap<>();
        for (Map.Entry<String, String> e : fieldExpressions.entrySet()) {
            result.put(e.getKey(), ev.compile(e.getValue()));
        }
        return result;
    }

    /** 转换环境中的值：BigDecimal -> Double, Timestamp/LocalDateTime -> Date, byte[] -> String */
    public static Object normalizeForAviator(Object v) {
        if (v == null) return null;
        if (v instanceof java.math.BigDecimal bd) return bd.doubleValue();
        if (v instanceof java.math.BigInteger bi) return bi.longValue();
        if (v instanceof java.sql.Timestamp ts) return new Date(ts.getTime());
        if (v instanceof java.sql.Date d) return new Date(d.getTime());
        if (v instanceof java.sql.Time t) return new Date(t.getTime());
        if (v instanceof java.time.LocalDateTime ldt) {
            return Date.from(ldt.atZone(java.time.ZoneId.systemDefault()).toInstant());
        }
        if (v instanceof java.time.LocalDate ld) {
            return Date.from(ld.atStartOfDay(java.time.ZoneId.systemDefault()).toInstant());
        }
        if (v instanceof byte[] b) return new String(b, java.nio.charset.StandardCharsets.UTF_8);
        return v;
    }

    // ---------- 自定义函数 ----------

    /** default(x, v): x 为 nil/null 时返回 v */
    static class DefaultFunction extends AbstractFunction {
        @Override
        public String getName() {
            return "default";
        }

        @Override
        public AviatorObject call(Map<String, Object> env, AviatorObject arg1, AviatorObject arg2) {
            Object v = arg1.getValue(env);
            return v == null ? arg2 : arg1;
        }
    }

    static class CoalesceFunction extends AbstractFunction {
        @Override
        public String getName() {
            return "coalesce";
        }

        @Override
        public AviatorObject call(Map<String, Object> env, AviatorObject arg1, AviatorObject arg2) {
            Object v = arg1.getValue(env);
            return v == null ? arg2 : arg1;
        }
    }

    /** dict.lookup(name, code): 按字典名查转换值（字典数据由配置解析引擎注入，无外部依赖） */
    static class DictLookupFunction extends AbstractFunction {
        private final Map<String, Map<String, String>> dictData;

        DictLookupFunction(Map<String, Map<String, String>> dictData) {
            this.dictData = dictData;
        }

        @Override
        public String getName() {
            return "dict.lookup";
        }

        @Override
        public AviatorObject call(Map<String, Object> env, AviatorObject arg1, AviatorObject arg2) {
            Object name = arg1.getValue(env);
            Object code = arg2.getValue(env);
            if (name == null || code == null) return AviatorNil.NIL;
            Map<String, String> m = dictData.get(String.valueOf(name));
            if (m == null) return AviatorNil.NIL;
            String v = m.get(String.valueOf(code));
            return v == null ? AviatorNil.NIL : new AviatorString(v);
        }
    }

    /** date.format(v, pattern): v 支持 Date / 时间戳 / 字符串 */
    static class DateFormatFunction extends AbstractFunction {
        @Override
        public String getName() {
            return "date.format";
        }

        @Override
        public AviatorObject call(Map<String, Object> env, AviatorObject arg1, AviatorObject arg2) {
            Object v = arg1.getValue(env);
            Object pattern = arg2.getValue(env);
            if (v == null || pattern == null) return AviatorNil.NIL;
            Date d = toDate(v);
            if (d == null) return AviatorNil.NIL;
            try {
                return new AviatorString(new SimpleDateFormat(String.valueOf(pattern)).format(d));
            } catch (Exception e) {
                return AviatorNil.NIL;
            }
        }
    }

    /** date.parse(s, pattern) -> 时间戳毫秒 */
    static class DateParseFunction extends AbstractFunction {
        @Override
        public String getName() {
            return "date.parse";
        }

        @Override
        public AviatorObject call(Map<String, Object> env, AviatorObject arg1, AviatorObject arg2) {
            Object v = arg1.getValue(env);
            Object pattern = arg2.getValue(env);
            if (v == null || pattern == null) return AviatorNil.NIL;
            try {
                Date d = new SimpleDateFormat(String.valueOf(pattern)).parse(String.valueOf(v));
                return com.googlecode.aviator.runtime.type.AviatorLong.valueOf(d.getTime());
            } catch (Exception e) {
                return AviatorNil.NIL;
            }
        }
    }

    private static Date toDate(Object v) {
        if (v instanceof Date d) return d;
        if (v instanceof Number n) return new Date(n.longValue());
        if (v instanceof java.time.LocalDateTime ldt) {
            return Date.from(ldt.atZone(java.time.ZoneId.systemDefault()).toInstant());
        }
        if (v instanceof String s) {
            try {
                return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(s);
            } catch (Exception e) {
                try {
                    return new SimpleDateFormat("yyyy-MM-dd").parse(s);
                } catch (Exception e2) {
                    return null;
                }
            }
        }
        return null;
    }
}
