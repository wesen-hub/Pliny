package com.dcp.scheduler.store;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

/** JSON 工具(etcd value 序列化)。单例 ObjectMapper。 */
public final class Json {

    public static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private Json() {}

    public static byte[] toBytes(Object o) {
        try {
            return MAPPER.writeValueAsBytes(o);
        } catch (Exception e) {
            throw new IllegalStateException("JSON 序列化失败: " + o, e);
        }
    }

    public static String toStr(Object o) {
        return new String(toBytes(o), java.nio.charset.StandardCharsets.UTF_8);
    }

    public static <T> T fromBytes(byte[] b, Class<T> type) {
        try {
            return MAPPER.readValue(b, type);
        } catch (Exception e) {
            throw new IllegalStateException("JSON 反序列化失败: " + type.getSimpleName(), e);
        }
    }
}
