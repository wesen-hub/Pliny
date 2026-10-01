package com.dcp.filesync.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 文件同步策略工具(迁自 backend FileSyncTaskService 的策略/JSON 纯函数, 无框架依赖):
 * strategy_json 解析、模式三态、全量判定、引擎动作(engineOp)、布尔/数值归一。
 */
public final class Strategy {

    /** 同步模式三态 */
    public static final String MODE_FULL_ONCE = "full_once";
    public static final String MODE_INCR = "incr_scheduled";
    public static final String MODE_FULL_THEN_INCR = "full_then_incr";

    private static final ObjectMapper OM = new ObjectMapper();

    private Strategy() {
    }

    /** 本轮是否为全量: full_once 恒全量; incr_scheduled 恒增量; full_then_incr 看基线 */
    public static boolean isFullRound(String mode, boolean baselineDone) {
        if (MODE_FULL_ONCE.equals(mode)) return true;
        if (MODE_INCR.equals(mode)) return false;
        return !baselineDone;
    }

    /** 本轮实际引擎动作: 全量默认 sync(镜像删多余); deleteExtras=false 时全量退化为 copy; 增量恒 copy */
    public static String engineOp(boolean full, Map<String, Object> st) {
        boolean deleteExtras = st.get("deleteExtras") == null || truthy(st.get("deleteExtras"));
        return full && deleteExtras ? "sync" : "copy";
    }

    public static String normMode(Object v) {
        String s = v == null ? "" : String.valueOf(v);
        if (MODE_FULL_ONCE.equals(s) || MODE_INCR.equals(s) || MODE_FULL_THEN_INCR.equals(s)) return s;
        return MODE_FULL_THEN_INCR;   // 缺省/存量 → 最常用的"全量+定时增量"
    }

    public static Map<String, Object> parseJson(String s) {
        if (s == null || s.isBlank() || "null".equals(s)) return new LinkedHashMap<>();
        try {
            return OM.readValue(s, new TypeReference<LinkedHashMap<String, Object>>() {
            });
        } catch (Exception e) {
            return new LinkedHashMap<>();
        }
    }

    public static boolean truthy(Object v) {
        if (v == null) return false;
        if (v instanceof Boolean b) return b;
        if (v instanceof Number n) return n.intValue() != 0;
        String s = String.valueOf(v);
        return "1".equals(s) || "true".equalsIgnoreCase(s);
    }

    public static int intOr(Object v, int dft) {
        if (v instanceof Number n) return n.intValue();
        try {
            return v == null || String.valueOf(v).isBlank() ? dft : Integer.parseInt(String.valueOf(v).trim());
        } catch (Exception e) {
            return dft;
        }
    }

    public static List<Object> list(Object o) {
        if (o instanceof List<?> l) return new ArrayList<>(l);
        return List.of();
    }

    /** extra_params JSON → ObjectNode(坏值/空 → 空节点) */
    public static ObjectNode extraNode(Object extra) {
        try {
            if (extra == null) return OM.createObjectNode();
            String s = String.valueOf(extra);
            if (s.isBlank() || "null".equals(s)) return OM.createObjectNode();
            JsonNode n = OM.readTree(s);
            return n instanceof ObjectNode on ? on : OM.createObjectNode();
        } catch (Exception e) {
            return OM.createObjectNode();
        }
    }

    public static String strip(String p) {
        return p == null ? "" : p.replaceAll("^/+|/+$", "");
    }

    public static String truncate(String s, int n) {
        if (s == null) return null;
        return s.length() > n ? s.substring(0, n) : s;
    }

    /** 读 JSON 对象的 ObjectMapper(执行器共享解析用)。 */
    public static ObjectMapper mapper() {
        return OM;
    }
}
