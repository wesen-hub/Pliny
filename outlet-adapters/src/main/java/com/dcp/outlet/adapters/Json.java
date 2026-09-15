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
}
