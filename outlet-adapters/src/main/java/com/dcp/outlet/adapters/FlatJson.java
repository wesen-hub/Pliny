package com.dcp.outlet.adapters;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 极简 JSON 读取(只支持平台快照接口用到的形状):
 * <pre>
 * { "rows": [ {"col":"v","id":1}, ... ], "nextPageToken": "abc" }
 * </pre>
 * 仅处理**扁平对象**(值只能是字符串/数字/布尔/null)。刻意不引 JSON 库,保持适配层可裁剪。
 */
public final class FlatJson {

    private FlatJson() { }

    /** 取顶层字符串字段。 */
    public static String string(String body, String field) {
        String key = "\"" + field + "\"";
        int i = body.indexOf(key);
        if (i < 0) return null;
        int colon = body.indexOf(':', i + key.length());
        if (colon < 0) return null;
        int j = colon + 1;
        while (j < body.length() && Character.isWhitespace(body.charAt(j))) j++;
        if (j >= body.length()) return null;
        if (body.charAt(j) == '"') {
            StringBuilder sb = new StringBuilder();
            for (int k = j + 1; k < body.length(); k++) {
                char c = body.charAt(k);
                if (c == '\\' && k + 1 < body.length()) { sb.append(body.charAt(++k)); continue; }
                if (c == '"') break;
                sb.append(c);
            }
            return sb.toString();
        }
        int end = j;
        while (end < body.length() && ",}]".indexOf(body.charAt(end)) < 0) end++;
        return body.substring(j, end).trim();
    }

    /** 解析 {@code "rows": [ {...}, ... ]} 为行列表。 */
    public static List<Map<String, String>> rows(String body) {
        List<Map<String, String>> out = new ArrayList<>();
        int i = body.indexOf("\"rows\"");
        if (i < 0) return out;
        int lb = body.indexOf('[', i);
        if (lb < 0) return out;
        int depth = 0, objStart = -1;
        boolean inStr = false;
        for (int k = lb; k < body.length(); k++) {
            char c = body.charAt(k);
            if (inStr) {
                if (c == '\\') { k++; continue; }
                if (c == '"') inStr = false;
                continue;
            }
            switch (c) {
                case '"' -> inStr = true;
                case '{' -> { if (depth++ == 0) objStart = k; }
                case '}' -> {
                    if (--depth == 0 && objStart >= 0) out.add(object(body.substring(objStart, k + 1)));
                }
                case ']' -> { if (depth == 0) return out; }
                default -> { }
            }
        }
        return out;
    }

    /** 解析单个扁平对象。 */
    public static Map<String, String> object(String obj) {
        Map<String, String> m = new LinkedHashMap<>();
        boolean inStr = false;
        StringBuilder key = new StringBuilder();
        int k = 1;
        while (k < obj.length() - 1) {
            char c = obj.charAt(k);
            if (c == '"') {                                  // 读到 key
                StringBuilder sb = new StringBuilder();
                for (int j = k + 1; j < obj.length(); j++) {
                    char d = obj.charAt(j);
                    if (d == '\\' && j + 1 < obj.length()) { sb.append(obj.charAt(++j)); continue; }
                    if (d == '"') { k = j; break; }
                    sb.append(d);
                }
                int colon = obj.indexOf(':', k);
                if (colon < 0) break;
                int v = colon + 1;
                while (v < obj.length() && Character.isWhitespace(obj.charAt(v))) v++;
                String value;
                if (v < obj.length() && obj.charAt(v) == '"') {
                    StringBuilder vb = new StringBuilder();
                    for (int j = v + 1; j < obj.length(); j++) {
                        char d = obj.charAt(j);
                        if (d == '\\' && j + 1 < obj.length()) { vb.append(obj.charAt(++j)); continue; }
                        if (d == '"') { k = j; break; }
                        vb.append(d);
                    }
                    value = vb.toString();
                } else {
                    int end = v;
                    while (end < obj.length() && ",}".indexOf(obj.charAt(end)) < 0) end++;
                    value = obj.substring(v, end).trim();
                    k = end - 1;
                }
                if (value != null && !"null".equals(value)) m.put(sb.toString(), value);
            }
            k++;
        }
        return m;
    }
}
