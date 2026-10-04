package com.dcp.indexer.core;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * canal-json 轻量解析(适配层用):只取 **commitTs / watermarkTs / type / table**。
 *
 * <p>兼容 TiCDC canal-json 变体:TiCDC 的 DML/水印消息顶层用 {@code "ts"} 而非
 * {@code "commitTs"/"watermarkTs"}(OceanBase/Canal 官方协议键名), 故两套键名都识别、
 * 优先官方键、缺失时回退 {@code "ts"}(TiCDC 顶层 ts 即 commit/watermark 时间)。
 *
 * <p>索引器与起点短扫只需要这几个字段 —— 这是锚点方案成本可控的前提。
 * 生产可换成完整 parser;此处用正则避免额外依赖。
 */
public final class CanalJson {

    private static final Pattern COMMIT_TS = Pattern.compile("\"commitTs\"\\s*:\\s*(\\d+)");
    private static final Pattern WATERMARK_TS = Pattern.compile("\"watermarkTs\"\\s*:\\s*(\\d+)");
    private static final Pattern TYPE = Pattern.compile("\"type\"\\s*:\\s*\"([A-Z_]+)\"");
    private static final Pattern TABLE = Pattern.compile("\"table\"\\s*:\\s*\"([^\"]*)\"");

    private CanalJson() { }

    public static long commitTs(String json) {
        Matcher m = COMMIT_TS.matcher(json);
        if (m.find()) return Long.parseLong(m.group(1));
        return topLevelTs(json);           // TiCDC 变体: 顶层 "ts" 即 commit ts
    }

    public static long watermarkTs(String json) {
        Matcher m = WATERMARK_TS.matcher(json);
        if (m.find()) return Long.parseLong(m.group(1));
        return topLevelTs(json);           // TiCDC 变体: TIDB_WATERMARK 消息的 "ts"
    }

    /**
     * TiCDC canal-json 变体的顶层 {@code "ts"}(低危-4)。
     *
     * <p>修复前用非锚定正则 {@code "ts"\s*:\s*(\d+)} 全串搜索, 若 data 行内有 ts 列
     * (如 {@code "data":[{"ts":123}]}) 会误把行内值当 commit/watermark 时间。
     * 这里只认**首层**(depth==0)的 ts 键, 嵌套在 data/old 内的同名列被排除。
     */
    private static long topLevelTs(String json) {
        int depth = 0;
        boolean inStr = false, esc = false;
        for (int i = 0; i + 3 < json.length(); i++) {
            char c = json.charAt(i);
            if (inStr) {
                if (esc) { esc = false; }
                else if (c == '\\') { esc = true; }
                else if (c == '"') { inStr = false; }
                continue;
            }
            if (c == '"') {
                inStr = true;
                // depth==1: 顶层对象 { 内部的键; 嵌套进 data/old 数组(含其行对象)后 depth≥2 → 排除
                if (depth == 1 && json.startsWith("ts\"", i + 1)) {
                    int j = i + 4;
                    while (j < json.length() && Character.isWhitespace(json.charAt(j))) j++;
                    if (j < json.length() && json.charAt(j) == ':') {
                        j++;
                        while (j < json.length() && Character.isWhitespace(json.charAt(j))) j++;
                        int s = j;
                        while (j < json.length() && Character.isDigit(json.charAt(j))) j++;
                        if (j > s) return Long.parseLong(json.substring(s, j));
                    }
                }
                continue;
            }
            if (c == '{' || c == '[') { depth++; }
            else if (c == '}' || c == ']') { depth--; }
        }
        return -1L;
    }

    public static String type(String json) {
        Matcher m = TYPE.matcher(json);
        return m.find() ? m.group(1) : "UNKNOWN";
    }

    public static String table(String json) {
        Matcher m = TABLE.matcher(json);
        return m.find() ? m.group(1) : "";
    }

    public static boolean isWatermark(String json) {
        return "TIDB_WATERMARK".equals(type(json));
    }

    public static boolean isDml(String json) {
        String t = type(json);
        return "INSERT".equals(t) || "UPDATE".equals(t) || "DELETE".equals(t);
    }
}
