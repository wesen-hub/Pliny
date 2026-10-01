package com.dcp.ingest.model;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/** JSON 工具(信封序列化 / 入站消息解析; 入站值均为字符串, 无需 jsr310 模块)。 */
public final class Json {

    private static final ObjectMapper OM = new ObjectMapper();

    private Json() {
    }

    public static String toJson(Object o) {
        try {
            return OM.writeValueAsString(o);
        } catch (Exception e) {
            throw new IllegalStateException("JSON 序列化失败", e);
        }
    }

    public static JsonNode readTree(String s) {
        try {
            return OM.readTree(s);
        } catch (Exception e) {
            throw new IllegalStateException("JSON 解析失败", e);
        }
    }
}
