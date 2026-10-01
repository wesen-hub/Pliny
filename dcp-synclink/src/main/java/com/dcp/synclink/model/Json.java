package com.dcp.synclink.model;

/**
 * 轻量 JSON 工具(业务 jar 自足): 复刻 backend SyncProducer.Json 的语义,
 * 不做 Spring 依赖, ObjectMapper 静态单例(线程安全)。
 */
public final class Json {

    private static final com.fasterxml.jackson.databind.ObjectMapper OM;

    static {
        OM = new com.fasterxml.jackson.databind.ObjectMapper();
        // JDBC 时间类型(LocalDateTime 等)需要 jsr310 模块
        OM.registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule());
        OM.disable(com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    private Json() {
    }

    public static String toJson(Object o) {
        try {
            return OM.writeValueAsString(o);
        } catch (Exception e) {
            throw new IllegalStateException("JSON 序列化失败", e);
        }
    }

    public static com.fasterxml.jackson.databind.JsonNode readTree(String s) {
        try {
            return OM.readTree(s);
        } catch (Exception e) {
            throw new IllegalStateException("JSON 解析失败", e);
        }
    }

    public static <T> T fromJson(String s, Class<T> type) {
        try {
            return OM.readValue(s, type);
        } catch (Exception e) {
            throw new IllegalStateException("JSON 反序列化失败", e);
        }
    }
}
