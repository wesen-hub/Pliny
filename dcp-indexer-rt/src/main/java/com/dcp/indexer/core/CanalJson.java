package com.dcp.indexer.core;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * canal-json 轻量解析(适配层用):只取 **commitTs / watermarkTs / type / table**。
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
        return m.find() ? Long.parseLong(m.group(1)) : -1L;
    }

    public static long watermarkTs(String json) {
        Matcher m = WATERMARK_TS.matcher(json);
        return m.find() ? Long.parseLong(m.group(1)) : -1L;
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
