package com.dcp.subpush.match;

import com.fasterxml.jackson.databind.JsonNode;

import java.math.BigDecimal;
import java.util.Map;

/**
 * logical 视图 conds 内存求值器(语义对齐切流前 DataSubService.condSql 生成 SQL 的
 * 数据库执行结果; 信封流事件驱动下无法回表拼 SQL, 内存等价实现)。
 *
 * <p>视图形状: {@code groups:[{conds:[{field,op,val}...]}...]}, 组内 AND、组间 AND
 * (与 queryLogical 的 where 拼接同语义)。支持的 op 与 condSql 一致:
 * {@code = != > >= < <= in like null}。
 *
 * <p>值语义: 信封 fields 值经 JSON 反序列化后是字符串/数字; 等值比较两边字符串化,
 * 大小比较优先 {@link BigDecimal} 数值比较(失败回退字符串字典序), 与 DB 端
 * "列类型决定比较" 的语义在数字列上等价。
 */
public final class CondMatcher {

    private CondMatcher() {
    }

    /** 行是否命中视图条件(无 groups / 空组 = 全通过)。 */
    public static boolean match(JsonNode view, Map<String, Object> row) {
        JsonNode groups = view == null ? null : view.path("groups");
        if (!groups.isArray() || groups.isEmpty()) {
            return true;
        }
        for (JsonNode g : groups) {
            JsonNode conds = g.path("conds");
            if (!conds.isArray() || conds.isEmpty()) {
                continue;
            }
            for (JsonNode c : conds) {
                if (!condHolds(c, row)) {
                    return false;   // 组内 AND: 一条不满足整组失败
                }
            }
        }
        return true;   // 组间 AND: 全部组通过
    }

    private static boolean condHolds(JsonNode c, Map<String, Object> row) {
        String field = c.path("field").asText("");
        String op = c.path("op").asText("");
        String val = c.path("val").asText("");
        if (field.isBlank() || op.isBlank()) {
            return true;   // 空条件不参与过滤(与 condSql 跳过逻辑一致)
        }
        Object cell = row.get(field);
        switch (op) {
            case "=":
                return cell != null && stringOf(cell).equals(val);
            case "!=":
                return cell == null || !stringOf(cell).equals(val);
            case ">":
            case ">=":
            case "<":
            case "<=":
                return cmp(cell, val, op);
            case "in": {
                for (String p : val.split("[,，]")) {
                    if (!p.isBlank() && cell != null && stringOf(cell).equals(p.trim())) {
                        return true;
                    }
                }
                return false;
            }
            case "like":
                return cell != null && stringOf(cell).contains(val);
            case "null":
                return cell == null || stringOf(cell).isBlank();
            default:
                return true;   // 未知 op 不参与过滤
        }
    }

    private static boolean cmp(Object cell, String val, String op) {
        if (cell == null || val == null || val.isBlank()) {
            return false;
        }
        try {
            int r = new BigDecimal(stringOf(cell)).compareTo(new BigDecimal(val));
            switch (op) {
                case ">":  return r > 0;
                case ">=": return r >= 0;
                case "<":  return r < 0;
                default:   return r <= 0;
            }
        } catch (NumberFormatException e) {
            // 非数值列: 字符串字典序(与 DB 字符串列比较一致)
            int r = stringOf(cell).compareTo(val);
            switch (op) {
                case ">":  return r > 0;
                case ">=": return r >= 0;
                case "<":  return r < 0;
                default:   return r <= 0;
            }
        }
    }

    private static String stringOf(Object v) {
        return String.valueOf(v);
    }
}
