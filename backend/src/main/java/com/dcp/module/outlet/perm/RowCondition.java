package com.dcp.module.outlet.perm;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

/**
 * 行条件编译:把行条件字符串编译成**逐行谓词**(进程内求值 —— 不回调、不查库、不逐行打接口)。
 *
 * <h3>语法</h3>
 * <pre>
 *   cond := cmp ( AND cmp )*                     // 空 / 空白 → 无行限制(放行)
 *   cmp  := col '=' val                          // 等值
 *         | col '!=' val                         // 不等(空值视为"不满足")
 *         | col 'IN' '(' v (',' v)* ')'          // 集合
 *         | col 'NOT IN' '(' v (',' v)* ')'
 *         | col 'IS' 'NULL'  |  col 'IS' 'NOT' 'NULL'
 * </pre>
 * 字面量两侧引号可选({@code 'x'} / {@code "x"} / {@code x});比较按**字符串等值**
 * ({@code String.valueOf(rowValue).equals(val)}),与出口引擎原有语义一致(不做隐式数值转换)。
 *
 * <h3>失败语义(铁律,两端必须一致)</h3>
 * <ul>
 *   <li>空 / 空白条件 → <b>放行</b>(契约本来就没有行限制)。</li>
 *   <li><b>非空白但解析失败 → 拒绝</b>(fail-closed):条件写了却读不懂,若静默放行就等于
 *       <b>放大权限</b>。调用方应把 {@link IllegalArgumentException} 上抛成"不能消费/不能下发"。</li>
 * </ul>
 *
 * <p><b>同源同语义约束</b>:计算面独立部署(不依赖本模块),其实现
 * {@code com.dcp.outlet.adapters.ExpressionRowConditionEvaluator} 是本类的镜像 ——
 * <b>语法或语义任何改动必须两处同步</b>,并由 `RowConditionParityCheck`(条件 × 行矩阵逐对比对)
 * 在回归里兜住。
 */
public final class RowCondition {

    private RowCondition() { }

    /** 编译行条件。空 → 恒真;非法 → {@link IllegalArgumentException}。 */
    public static Predicate<Map<String, Object>> compile(String condition) {
        if (condition == null || condition.isBlank()) {
            return row -> true;
        }
        List<Cmp> cmps = new ArrayList<>();
        for (String part : splitTopLevelAnd(condition)) {
            if (part.isBlank()) {
                throw new IllegalArgumentException("行条件含空片段:" + condition);
            }
            cmps.add(parseCmp(part, condition));
        }
        if (cmps.isEmpty()) {
            throw new IllegalArgumentException("行条件解析为空:" + condition);
        }
        return row -> {
            for (Cmp c : cmps) {
                if (!c.test(row == null ? null : row.get(c.column))) {
                    return false;
                }
            }
            return true;
        };
    }

    /** 只判语法:合法返回 null,非法返回原因(供校验期报错,避免"存得进去、跑起来才炸")。 */
    public static String validate(String condition) {
        try {
            compile(condition);
            return null;
        } catch (IllegalArgumentException e) {
            return e.getMessage();
        }
    }

    /* ------------------------------------------------------------------ 解析 */

    /** 顶层 AND 切分:尊重括号与引号(`col IN ('a AND b')` 不能被切开)。 */
    private static List<String> splitTopLevelAnd(String s) {
        List<String> out = new ArrayList<>();
        int depth = 0;
        char quote = 0;
        StringBuilder cur = new StringBuilder();
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (quote != 0) {
                cur.append(c);
                if (c == quote) quote = 0;
                continue;
            }
            if (c == '\'' || c == '"') {
                quote = c;
                cur.append(c);
                continue;
            }
            if (c == '(') depth++;
            if (c == ')') depth--;
            if (depth == 0 && (c == 'A' || c == 'a') && i + 3 <= s.length()
                    && s.regionMatches(true, i, "AND", 0, 3)
                    && (i == 0 || Character.isWhitespace(s.charAt(i - 1)))
                    && (i + 3 == s.length() || Character.isWhitespace(s.charAt(i + 3)))) {
                out.add(cur.toString());
                cur.setLength(0);
                i += 2;
                continue;
            }
            cur.append(c);
        }
        out.add(cur.toString());
        return out;
    }

    private static Cmp parseCmp(String raw, String whole) {
        String part = raw.trim();

        // IS NULL / IS NOT NULL
        String upper = part.toUpperCase();
        int isIdx = indexOfKeyword(part, "IS");
        if (isIdx > 0) {
            String col = part.substring(0, isIdx).trim();
            String rest = part.substring(isIdx + 2).trim();
            if (col.isEmpty() || col.contains("(") || col.contains(")")) {
                throw new IllegalArgumentException("行条件片段非法:" + part + "(完整条件:" + whole + ")");
            }
            if ("NULL".equalsIgnoreCase(rest)) return Cmp.isNull(col);
            if ("NOT NULL".equalsIgnoreCase(rest)) return Cmp.isNotNull(col);
            throw new IllegalArgumentException("只支持 IS NULL / IS NOT NULL,收到:" + part);
        }

        // IN / NOT IN
        int notIn = indexOfKeyword(part, "NOT IN");
        int in = notIn < 0 ? indexOfKeyword(part, "IN") : -1;
        if (notIn > 0 || in > 0) {
            boolean negate = notIn > 0;
            int idx = negate ? notIn : in;
            String col = part.substring(0, idx).trim();
            String rest = part.substring(idx + (negate ? 6 : 2)).trim();
            if (col.isEmpty() || !rest.startsWith("(") || !rest.endsWith(")")) {
                throw new IllegalArgumentException("IN 片段非法(需要 col IN (a, b)):" + part);
            }
            Set<String> vals = new LinkedHashSet<>();
            for (String v : splitTopLevelComma(rest.substring(1, rest.length() - 1))) {
                if (!v.isBlank()) vals.add(literal(v));
            }
            if (vals.isEmpty()) {
                throw new IllegalArgumentException("IN 集合为空:" + part);
            }
            return Cmp.in(col, vals, negate);
        }

        // = / !=
        int ne = part.indexOf("!=");
        if (upper.contains("<>")) {
            throw new IllegalArgumentException("请用 != 而不是 <>:" + part);
        }
        if (ne > 0) {
            String col = part.substring(0, ne).trim();
            String val = literal(part.substring(ne + 2));
            if (col.isEmpty()) throw new IllegalArgumentException("列名为空:" + part);
            return Cmp.notEq(col, val);
        }
        int eq = part.indexOf('=');
        if (eq > 0) {
            String col = part.substring(0, eq).trim();
            String val = literal(part.substring(eq + 1));
            if (col.isEmpty()) throw new IllegalArgumentException("列名为空:" + part);
            return Cmp.eq(col, val);
        }
        if (upper.contains(">") || upper.contains("<")) {
            throw new IllegalArgumentException("不支持大小比较(只支持 = != IN NOT IN IS NULL):" + part);
        }
        throw new IllegalArgumentException("无法解析的行条件片段:" + part + "(完整条件:" + whole + ")");
    }

    /** 关键字定位(要求前面是空白,避免把列名里的字母误认成关键字)。 */
    private static int indexOfKeyword(String s, String kw) {
        String u = s.toUpperCase();
        String k = kw.toUpperCase();
        int from = 0;
        while (true) {
            int i = u.indexOf(k, from);
            if (i < 0) return -1;
            boolean leftOk = i == 0 || Character.isWhitespace(s.charAt(i - 1));
            int after = i + k.length();
            boolean rightOk = after == s.length() || Character.isWhitespace(s.charAt(after)) || s.charAt(after) == '(';
            if (leftOk && rightOk) return i;
            from = i + 1;
        }
    }

    /** 顶层逗号切分(尊重括号/引号)。 */
    private static List<String> splitTopLevelComma(String s) {
        List<String> out = new ArrayList<>();
        int depth = 0;
        char quote = 0;
        StringBuilder cur = new StringBuilder();
        for (char c : s.toCharArray()) {
            if (quote != 0) {
                cur.append(c);
                if (c == quote) quote = 0;
                continue;
            }
            if (c == '\'' || c == '"') { quote = c; cur.append(c); continue; }
            if (c == '(') depth++;
            if (c == ')') depth--;
            if (c == ',' && depth == 0) { out.add(cur.toString()); cur.setLength(0); continue; }
            cur.append(c);
        }
        out.add(cur.toString());
        return out;
    }

    /** 字面量归一:去首尾空白 + 去成对外层引号。 */
    private static String literal(String raw) {
        String v = raw.trim();
        if (v.length() >= 2
                && ((v.charAt(0) == '\'' && v.charAt(v.length() - 1) == '\'')
                || (v.charAt(0) == '"' && v.charAt(v.length() - 1) == '"'))) {
            v = v.substring(1, v.length() - 1);
        }
        return v;
    }

    /* ------------------------------------------------------------------ 谓词 */

    /** 单条比较(求值语义在此集中,两端必须一致)。 */
    private static final class Cmp {
        private final String column;
        private final String op;          // eq | ne | in | notin | isnull | notnull
        private final String value;
        private final Set<String> values;

        private Cmp(String column, String op, String value, Set<String> values) {
            this.column = column; this.op = op; this.value = value; this.values = values;
        }

        static Cmp eq(String c, String v) { return new Cmp(c, "eq", v, null); }
        static Cmp notEq(String c, String v) { return new Cmp(c, "ne", v, null); }
        static Cmp in(String c, Set<String> vs, boolean negate) {
            return new Cmp(c, negate ? "notin" : "in", null, vs);
        }
        static Cmp isNull(String c) { return new Cmp(c, "isnull", null, null); }
        static Cmp isNotNull(String c) { return new Cmp(c, "notnull", null, null); }

        boolean test(Object v) {
            switch (op) {
                case "isnull":
                    return v == null;
                case "notnull":
                    return v != null;
                case "eq":
                    return v != null && String.valueOf(v).equals(value);
                case "ne":
                    return v != null && !String.valueOf(v).equals(value);
                case "in":
                    return v != null && values.contains(String.valueOf(v));
                case "notin":
                    return v != null && !values.contains(String.valueOf(v));
                default:
                    return false;
            }
        }
    }
}
