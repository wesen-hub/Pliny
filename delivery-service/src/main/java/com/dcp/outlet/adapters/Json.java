package com.dcp.outlet.adapters;

import java.util.List;
import java.util.Map;

/** 极简 JSON 序列化(仅用于把 streams / params 写进 JSON 列);避免为适配层引入 JSON 库。 */
public final class Json {

    private Json() { }

    public static String write(Object v) {
        StringBuilder sb = new StringBuilder();
        writeTo(sb, v);
        return sb.toString();
    }

    private static void writeTo(StringBuilder sb, Object v) {
        if (v == null) { sb.append("null"); return; }
        if (v instanceof String s) { quote(sb, s); return; }
        if (v instanceof Number || v instanceof Boolean) { sb.append(v); return; }
        if (v instanceof Map<?, ?> m) {
            sb.append('{');
            boolean first = true;
            for (Map.Entry<?, ?> e : m.entrySet()) {
                if (!first) sb.append(',');
                first = false;
                quote(sb, String.valueOf(e.getKey()));
                sb.append(':');
                writeTo(sb, e.getValue());
            }
            sb.append('}');
            return;
        }
        if (v instanceof List<?> l) {
            sb.append('[');
            for (int i = 0; i < l.size(); i++) {
                if (i > 0) sb.append(',');
                writeTo(sb, l.get(i));
            }
            sb.append(']');
            return;
        }
        quote(sb, String.valueOf(v));
    }

    private static void quote(StringBuilder sb, String s) {
        sb.append('"');
        for (char c : s.toCharArray()) {
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) sb.append(String.format("\\u%04x", (int) c));
                    else sb.append(c);
                }
            }
        }
        sb.append('"');
    }

    /** 解析**扁平**字符串对象 {k:v,k:v}(掩码规则用;嵌套结构不支持)。 */
    public static java.util.Map<String, String> readStringMap(String json) {
        java.util.Map<String, String> out = new java.util.LinkedHashMap<>();
        if (json == null) return out;
        String body = json.trim();
        if (body.startsWith("{")) body = body.substring(1, body.length() - 1);
        if (body.isBlank()) return out;
        for (String pair : body.split(",")) {
            int i = pair.indexOf(':');
            if (i < 0) continue;
            String k = pair.substring(0, i).trim().replaceAll("^\"|\"$", "");
            String v = pair.substring(i + 1).trim().replaceAll("^\"|\"$", "");
            if (!k.isEmpty()) out.put(k, v);
        }
        return out;
    }

    /** 仅解析字符串数组 / 字符串(够用即可)。 */
    public static List<String> readStringArray(String json) {
        if (json == null || json.isBlank()) return List.of();
        String body = json.trim();
        if (body.startsWith("[")) body = body.substring(1, body.length() - 1);
        if (body.isBlank()) return List.of();
        List<String> out = new java.util.ArrayList<>();
        for (String part : body.split(",")) {
            String s = part.trim();
            if (s.startsWith("\"") && s.endsWith("\"") && s.length() >= 2) s = s.substring(1, s.length() - 1);
            if (!s.isEmpty()) out.add(s);
        }
        return out;
    }

    /**
     * 解析完整(可嵌套)JSON → Map / List / String / Long / Double / Boolean / null。
     * <p>用于 params 列回读(打标配置桥的 rules/tagIds 是嵌套结构,扁平 parser 读不出来)。
     */
    @SuppressWarnings("unchecked")
    public static Object readValue(String json) {
        if (json == null || json.isBlank()) return null;
        P p = new P(json);
        Object v = p.value();
        p.ws();
        return v;
    }

    public static Map<String, Object> readMap(String json) {
        Object v = readValue(json);
        return v instanceof Map ? (Map<String, Object>) v : new java.util.LinkedHashMap<>();
    }

    /** 递归下降解析器(字符串/数字/布尔/null/对象/数组)。 */
    private static final class P {
        private final String s;
        private int i;

        P(String s) { this.s = s; }

        void ws() { while (i < s.length() && Character.isWhitespace(s.charAt(i))) i++; }

        Object value() {
            ws();
            if (i >= s.length()) return null;
            char c = s.charAt(i);
            return switch (c) {
                case '{' -> object();
                case '[' -> array();
                case '"' -> string();
                case 't' -> { expect("true"); yield Boolean.TRUE; }
                case 'f' -> { expect("false"); yield Boolean.FALSE; }
                case 'n' -> { expect("null"); yield null; }
                default -> number();
            };
        }

        private void expect(String lit) {
            if (!s.startsWith(lit, i)) throw new IllegalStateException("JSON 语法错误@" + i);
            i += lit.length();
        }

        private Map<String, Object> object() {
            Map<String, Object> m = new java.util.LinkedHashMap<>();
            i++;  // {
            ws();
            if (i < s.length() && s.charAt(i) == '}') { i++; return m; }
            while (true) {
                ws();
                String k = (String) value();   // key 必须是字符串
                ws();
                if (i >= s.length() || s.charAt(i) != ':') throw new IllegalStateException("期待 ':' @" + i);
                i++;
                m.put(k, value());
                ws();
                if (i < s.length() && s.charAt(i) == ',') { i++; continue; }
                if (i < s.length() && s.charAt(i) == '}') { i++; return m; }
                throw new IllegalStateException("期待 ',' 或 '}' @" + i);
            }
        }

        private List<Object> array() {
            List<Object> l = new java.util.ArrayList<>();
            i++;  // [
            ws();
            if (i < s.length() && s.charAt(i) == ']') { i++; return l; }
            while (true) {
                l.add(value());
                ws();
                if (i < s.length() && s.charAt(i) == ',') { i++; continue; }
                if (i < s.length() && s.charAt(i) == ']') { i++; return l; }
                throw new IllegalStateException("期待 ',' 或 ']' @" + i);
            }
        }

        private String string() {
            i++;  // "
            StringBuilder sb = new StringBuilder();
            while (i < s.length()) {
                char c = s.charAt(i++);
                if (c == '"') return sb.toString();
                if (c == '\\' && i < s.length()) {
                    char e = s.charAt(i++);
                    switch (e) {
                        case '"' -> sb.append('"');
                        case '\\' -> sb.append('\\');
                        case '/' -> sb.append('/');
                        case 'n' -> sb.append('\n');
                        case 'r' -> sb.append('\r');
                        case 't' -> sb.append('\t');
                        case 'b' -> sb.append('\b');
                        case 'f' -> sb.append('\f');
                        case 'u' -> {
                            if (i + 4 <= s.length()) {
                                sb.append((char) Integer.parseInt(s.substring(i, i + 4), 16));
                                i += 4;
                            }
                        }
                        default -> sb.append(e);
                    }
                } else {
                    sb.append(c);
                }
            }
            throw new IllegalStateException("未闭合字符串");
        }

        private Object number() {
            int start = i;
            while (i < s.length() && "-+.eE0123456789".indexOf(s.charAt(i)) >= 0) i++;
            String t = s.substring(start, i);
            try {
                if (t.indexOf('.') >= 0 || t.indexOf('e') >= 0 || t.indexOf('E') >= 0) {
                    return Double.parseDouble(t);
                }
                return Long.parseLong(t);
            } catch (NumberFormatException e) {
                return t;
            }
        }
    }
}
